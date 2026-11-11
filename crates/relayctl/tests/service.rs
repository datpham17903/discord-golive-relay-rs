//! Port of `tests/test_service.py`: service wiring with a fake adapter, fake output accounts and a
//! fake worker (no Discord, no worker thread).
//!
//! Python called `_handle` synchronously from the fake adapter; here adapter events go through the
//! service's event queue, so the tests `flush()` it where Python asserted right after an event.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use parking_lot::Mutex;
use relay_core::clock::monotonic;
use relay_core::config::{AccountRole, OutputAccount, OutputCodec, RelayConfig, load_config};
use relay_core::control::commands::ChatCommand;
use relay_core::control::state::{
    JsonObject, PersistedRouting, PersistedState, StateStore, set_account,
};
use relay_core::supervisor::codec::{CodecEvent, OutputId};
use relay_core::supervisor::model::{
    Action, Event, GatewayState, HoldReason, KeyframeMethod, RelayState, SourceStream, StopReason,
    TargetVoice,
};
use relay_core::supervisor::outputs_model::{PoolEvent, account_view};
use relay_core::worker::messages::{
    Egress, EgressVideo, Ingress, OutputProfile, WorkerEvent, WorkerEventKind,
};
use relay_discord::{
    Adapter, AdapterError, AdapterEvent, ApiFuture, BoxFuture, EventSink, LogFn, OutputAdapter,
    OutputSource, PoolEventSink, SourceFactory, Token, VoiceStateCallback,
};
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;
use relay_media::h264::is_idr;
use relay_media::slate::{SlateGop, build_slate};
use relay_voice::VoiceError;
use relay_voice::publish::VideoParams;
use relayctl::service::{
    Clock, OperatorCheck, Service, ServiceError, ServiceOptions, SlateBuilder, SlateKey, Worker,
    watch_retryable,
};
use serde_json::{Map, Value, json};
use tokio::task::JoinHandle;

const CANVAS: (u32, u32) = (64, 64);
const OPERATOR: u64 = 50;
const CMD_CHAN: u64 = 60;
const RELAY_ID: u64 = 100_000_000_000_000_001;
const VIEWERS: [u64; 3] = [5001, 5002, 5003];

static SOURCE_GOP: LazyLock<Vec<Vec<u8>>> = LazyLock::new(|| {
    build_slate(CANVAS.0, CANVAS.1, 10, 1.0, 100)
        .unwrap()
        .variants[0]
        .clone()
});
/// A different size, so its bytes differ from the source.
static SLATE: LazyLock<Arc<SlateGop>> =
    LazyLock::new(|| Arc::new(build_slate(32, 32, 10, 1.0, 100).unwrap()));

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

// ------------------------------------------------------------------ fake worker

struct FakeWorker {
    /// Announces an eligible H.265 source before the first frame, like the real worker.
    adaptive: bool,
    gop: Mutex<VecDeque<Vec<u8>>>,
    sent: Mutex<Vec<Ingress>>,
    alive: AtomicBool,
    stopped: AtomicBool,
    events: Mutex<Vec<WorkerEvent>>,
    enabled: AtomicBool,
    cfg: Mutex<Option<(u64, u64)>>,
}

impl FakeWorker {
    fn new(adaptive: bool) -> Arc<Self> {
        Arc::new(Self {
            adaptive,
            gop: Mutex::new(SOURCE_GOP.iter().cloned().collect()),
            sent: Mutex::new(Vec::new()),
            alive: AtomicBool::new(true),
            stopped: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
            enabled: AtomicBool::new(false),
            cfg: Mutex::new(None),
        })
    }

    fn push_event(&self, g: u64, e: u64, kind: WorkerEventKind, data: Value) {
        self.events.lock().push(WorkerEvent {
            generation: g,
            epoch: e,
            kind,
            data: obj(data),
        });
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

impl Worker for FakeWorker {
    fn send(&self, msg: Ingress) -> bool {
        match &msg {
            Ingress::Configure(c) => {
                if self.adaptive {
                    self.push_event(
                        c.generation,
                        c.epoch,
                        WorkerEventKind::SourceCodec,
                        json!({"codec": "H265", "width": CANVAS.0, "height": CANVAS.1,
                               "eligible": true}),
                    );
                }
                *self.cfg.lock() = Some((c.generation, c.epoch));
                self.push_event(
                    c.generation,
                    c.epoch,
                    WorkerEventKind::FirstFrame,
                    json!({"width": CANVAS.0, "height": CANVAS.1}),
                );
            }
            Ingress::SetOutput(s) => self.enabled.store(s.enabled, Ordering::SeqCst),
            _ => {}
        }
        self.sent.lock().push(msg);
        true
    }

    fn poll_egress(&self, _max_items: usize) -> Vec<Egress> {
        let Some((g, e)) = *self.cfg.lock() else {
            return Vec::new();
        };
        if !self.enabled.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let Some(au) = self.gop.lock().pop_front() else {
            return Vec::new();
        };
        vec![Egress::Video(EgressVideo {
            generation: g,
            epoch: e,
            is_idr: is_idr(&au),
            data: au,
            frame_time_ms: 100.0,
            deadline: monotonic(),
            source_frame_at: None,
            repeated: false,
            codec: "H264".into(),
        })]
    }

    fn poll_events(&self) -> Vec<WorkerEvent> {
        std::mem::take(&mut *self.events.lock())
    }

    fn alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

// ------------------------------------------------------------------ fake adapter

type JoinHook =
    Arc<dyn Fn(Option<VoiceStateCallback>) -> BoxFut<Result<String, AdapterError>> + Send + Sync>;
type WatchHook = Arc<dyn Fn() -> BoxFut<Result<(), AdapterError>> + Send + Sync>;
type CreateHook = Arc<dyn Fn() -> BoxFut<Result<Option<String>, AdapterError>> + Send + Sync>;
type Hook = Arc<dyn Fn() + Send + Sync>;

struct FakeAdapter {
    events: EventSink,
    order: Arc<Mutex<Vec<String>>>,
    started: AtomicBool,
    calls: Mutex<Vec<String>>,
    replies: Mutex<Vec<(u64, u64, String)>>,
    snap: Mutex<Option<Event>>,
    voice_ids: Mutex<HashSet<u64>>,
    alive: AtomicBool,
    foreign: AtomicBool,
    last_vsu: Mutex<Option<String>>,
    routing: Mutex<(u64, u64)>,
    // adaptive (Python `AdaptiveAdapter`)
    negotiated: Mutex<String>,
    watch_lists: Mutex<Vec<Vec<String>>>,
    swaps: Mutex<Vec<String>>,
    network: Mutex<Value>,
    audio: Mutex<Value>,
    // monkeypatched methods
    fail_start: AtomicBool,
    join_hook: Mutex<Option<JoinHook>>,
    watch_hook: Mutex<Option<WatchHook>>,
    create_adaptive_hook: Mutex<Option<CreateHook>>,
    before_end_output: Mutex<Option<Hook>>,
    log_leave_call: AtomicBool,
}

impl FakeAdapter {
    fn new(events: EventSink, order: Arc<Mutex<Vec<String>>>) -> Arc<Self> {
        Arc::new(Self {
            events,
            order,
            started: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
            replies: Mutex::new(Vec::new()),
            snap: Mutex::new(None),
            voice_ids: Mutex::new(HashSet::new()),
            alive: AtomicBool::new(true),
            foreign: AtomicBool::new(false),
            last_vsu: Mutex::new(None),
            routing: Mutex::new((0, 0)),
            negotiated: Mutex::new("H265".into()),
            watch_lists: Mutex::new(Vec::new()),
            swaps: Mutex::new(Vec::new()),
            network: Mutex::new(json!({})),
            audio: Mutex::new(json!({"packets": 0, "silence": 0})),
            fail_start: AtomicBool::new(false),
            join_hook: Mutex::new(None),
            watch_hook: Mutex::new(None),
            create_adaptive_hook: Mutex::new(None),
            before_end_output: Mutex::new(None),
            log_leave_call: AtomicBool::new(false),
        })
    }

    fn detached() -> Arc<Self> {
        Self::new(Arc::new(|_| {}), Arc::new(Mutex::new(Vec::new())))
    }

    fn emit(&self, ev: Event) {
        (self.events)(AdapterEvent::Model(ev));
    }

    fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn push(&self, call: &str) {
        self.calls.lock().push(call.to_string());
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    fn count(&self, call: &str) -> usize {
        self.calls.lock().iter().filter(|c| *c == call).count()
    }

    fn has(&self, call: &str) -> bool {
        self.count(call) > 0
    }

    fn replies(&self) -> Vec<(u64, u64, String)> {
        self.replies.lock().clone()
    }

    fn texts(&self) -> Vec<String> {
        self.replies.lock().iter().map(|r| r.2.clone()).collect()
    }

    fn set_voice_ids(&self, ids: &[u64]) {
        *self.voice_ids.lock() = ids.iter().copied().collect();
    }

    /// Python `FakeAdapter.join`: the gateway echoed our voice state, then the join completes.
    fn default_join(
        &self,
        on_voice_state: Option<VoiceStateCallback>,
    ) -> Result<String, AdapterError> {
        self.push("join");
        if let Some(cb) = on_voice_state {
            cb();
        }
        Ok("sess-1".into())
    }

    fn set_join(&self, hook: JoinHook) {
        *self.join_hook.lock() = Some(hook);
    }
}

impl Adapter for FakeAdapter {
    fn start(&self, _token: Token) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if self.fail_start.load(Ordering::SeqCst) {
                return Err(AdapterError::failed("login boom"));
            }
            self.started.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.push("close") })
    }

    fn set_epoch(&self, _epoch: u64) {}

    fn set_routing(&self, target_user_id: u64, channel_id: u64) {
        *self.routing.lock() = (target_user_id, channel_id);
    }

    fn target_id(&self) -> u64 {
        self.routing.lock().0
    }

    fn channel_id(&self) -> u64 {
        self.routing.lock().1
    }

    fn snapshot(&self, new_session: bool) -> Event {
        self.push("snapshot");
        self.snap.lock().clone().unwrap_or(Event::GatewaySnapshot {
            target: None,
            stream: None,
            participants: Some(HashSet::new()),
            new_session,
        })
    }

    fn reply(&self, channel_id: u64, message_id: u64, text: String) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.replies.lock().push((channel_id, message_id, text));
            Ok(())
        })
    }

    fn voice_channel_ids(&self) -> HashSet<u64> {
        self.voice_ids.lock().clone()
    }

    fn display_name(&self, _user_id: u64) -> Option<String> {
        None
    }

    fn is_bot(&self, _user_id: u64) -> bool {
        false
    }

    fn role_name(&self, _role_id: u64) -> Option<String> {
        None
    }

    fn join(&self, on_voice_state: Option<VoiceStateCallback>) -> ApiFuture<'_, String> {
        let hook = self.join_hook.lock().clone();
        match hook {
            Some(h) => h(on_voice_state),
            None => {
                let r = self.default_join(on_voice_state);
                Box::pin(async move { r })
            }
        }
    }

    fn voice_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    fn last_vsu_session(&self) -> Option<String> {
        self.last_vsu.lock().clone()
    }

    fn foreign_voice_session(&self) -> bool {
        self.foreign.load(Ordering::SeqCst)
    }

    fn leave(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            if self.log_leave_call.load(Ordering::SeqCst) {
                self.push("leave");
            }
            self.order.lock().push("leave".into());
            Ok(())
        })
    }

    fn watch(&self, _generation: u64, _epoch: u64, _stream: SourceStream) -> ApiFuture<'_, ()> {
        let hook = self.watch_hook.lock().clone();
        match hook {
            Some(h) => h(),
            None => Box::pin(async move {
                self.push("watch");
                Ok(())
            }),
        }
    }

    fn update_watch_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move {
            self.watch_lists
                .lock()
                .push(names.iter().map(|n| n.to_string()).collect());
            Ok(true)
        })
    }

    fn request_pli(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.push("pli");
            Ok(())
        })
    }

    fn rewatch(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.push("rewatch");
            Ok(())
        })
    }

    fn stop_watch(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.order.lock().push("stop_watch".into());
            Ok(())
        })
    }

    fn create_output(&self, _generation: u64, _fps: u32) -> ApiFuture<'_, Option<String>> {
        Box::pin(async move {
            self.push("create_output");
            Ok(Some("H264".into()))
        })
    }

    fn create_output_adaptive(&self, _generation: u64, _fps: u32) -> ApiFuture<'_, Option<String>> {
        let hook = self.create_adaptive_hook.lock().clone();
        match hook {
            Some(h) => h(),
            None => Box::pin(async move {
                self.push("create_output_adaptive");
                Ok(Some("H265".into()))
            }),
        }
    }

    fn output_codec(&self) -> Option<String> {
        Some(self.negotiated.lock().clone())
    }

    fn update_output_codecs<'a>(&'a self, _names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move { Ok(true) })
    }

    fn swap_output_source(&self, make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>> {
        Box::pin(async move {
            // the codec the server negotiated by now
            let codec: VoiceCodec = self.negotiated.lock().parse().ok()?;
            let _source: Box<dyn FrameSource> = make_source(codec);
            self.swaps.lock().push(codec.as_str().to_string());
            Some(codec)
        })
    }

    fn play(
        &self,
        _source: Box<dyn FrameSource>,
        _params: VideoParams,
    ) -> Result<(), AdapterError> {
        self.push("play");
        Ok(())
    }

    fn end_output(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            let hook = self.before_end_output.lock().clone();
            if let Some(h) = hook {
                h();
            }
            self.order.lock().push("end_output".into());
            Ok(())
        })
    }

    fn take_audio_window(&self) -> Value {
        self.audio.lock().clone()
    }

    fn network_stats(&self) -> Value {
        self.network.lock().clone()
    }
}

// ------------------------------------------------------------------ fake output account

type OutStartHook = Arc<dyn Fn() -> Result<(), AdapterError> + Send + Sync>;
type Outs = Arc<Mutex<Vec<Arc<FakeOutAdapter>>>>;
/// Builds the start behaviour of each new output adapter from the ones created so far.
type OutStartFactory = Arc<dyn Fn(&Outs) -> Option<OutStartHook> + Send + Sync>;

struct FakeOutAdapter {
    uid: u64,
    events: PoolEventSink,
    order: Arc<Mutex<Vec<String>>>,
    calls: Mutex<Vec<String>>,
    channels: Mutex<Vec<u64>>,
    codec: String,
    attempt: AtomicU64,
    start_hook: Option<OutStartHook>,
    fail_publish: bool,
}

impl FakeOutAdapter {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    fn published_to(&self, channel: u64) -> bool {
        self.channels.lock().contains(&channel)
    }

    fn emit(&self, ev: PoolEvent) {
        (self.events)(ev);
    }
}

impl OutputAdapter for FakeOutAdapter {
    fn user_id(&self) -> u64 {
        self.uid
    }

    fn start(&self, _token: Token) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.calls.lock().push("start".into());
            match &self.start_hook {
                Some(h) => h(),
                None => Ok(()),
            }
        })
    }

    fn publish(
        &self,
        generation: u64,
        attempt: u64,
        _source: OutputSource,
        _params: VideoParams,
        channel_id: u64,
        _adaptive: bool,
    ) -> ApiFuture<'_, Option<String>> {
        Box::pin(async move {
            if self.fail_publish {
                return Err(AdapterError::failed("boom"));
            }
            self.calls.lock().push("publish".into());
            self.channels.lock().push(channel_id);
            self.attempt.store(attempt, Ordering::SeqCst);
            self.emit(PoolEvent::OutJoined {
                account: self.uid,
                generation,
                attempt,
                voice_session_id: Some("out-sess".into()),
            });
            self.emit(PoolEvent::OutReady {
                account: self.uid,
                generation,
                attempt,
                negotiated_video_codec: Some(self.codec.clone()),
            });
            Ok(Some(self.codec.clone()))
        })
    }

    fn update_codecs<'a>(&'a self, _names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move { Ok(true) })
    }

    fn swap_source(&self, _make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>> {
        Box::pin(async move { None })
    }

    fn end(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move {
            self.order.lock().push("out_end".into());
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.calls.lock().push("close".into()) })
    }

    fn network_stats(&self) -> Value {
        json!({})
    }
}

// ------------------------------------------------------------------ environment

type CfgEdit = Box<dyn FnOnce(&mut RelayConfig)>;
type StateEdit = Box<dyn FnOnce(PersistedState) -> PersistedState>;

struct Setup {
    enabled: bool,
    latch: Option<JsonObject>,
    adaptive: bool,
    ingest_publishes: bool,
    accounts: Vec<OutputAccount>,
    cfg: Option<CfgEdit>,
    state: Option<StateEdit>,
    out_start: Option<OutStartFactory>,
    out_fail_publish: bool,
    out_codec: &'static str,
    slate_builder: Option<SlateBuilder>,
    worker_hook: Option<Hook>,
    pli_min_interval_s: Option<f64>,
    voice_lost_grace_s: Option<f64>,
    peer_is_operator: Option<OperatorCheck>,
    adapter_fail_start: bool,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            enabled: true,
            latch: None,
            adaptive: false,
            ingest_publishes: true,
            accounts: Vec::new(),
            cfg: None,
            state: None,
            out_start: None,
            out_fail_publish: false,
            out_codec: "H264",
            slate_builder: None,
            worker_hook: None,
            pli_min_interval_s: None,
            voice_lost_grace_s: None,
            peer_is_operator: None,
            adapter_fail_start: false,
        }
    }
}

fn cfg(edit: impl FnOnce(&mut RelayConfig) + 'static) -> Option<CfgEdit> {
    Some(Box::new(edit))
}

fn state(edit: impl FnOnce(PersistedState) -> PersistedState + 'static) -> Option<StateEdit> {
    Some(Box::new(edit))
}

fn account(uid: u64, role: AccountRole, channel_id: Option<u64>) -> OutputAccount {
    OutputAccount {
        user_id: uid,
        credential: format!("relay-token-{uid}"),
        role,
        channel_id,
    }
}

/// Python `_multi_env`: one extra account, fast retries, every relay always publishing.
fn multi(uid: u64, role: AccountRole) -> Setup {
    Setup {
        accounts: vec![account(uid, role, None)],
        cfg: cfg(|c| c.runtime.relay_autoscale = false),
        ..Setup::default()
    }
}

struct Env {
    _dir: tempfile::TempDir,
    dir: PathBuf,
    cfg: RelayConfig,
    store: StateStore,
    order: Arc<Mutex<Vec<String>>>,
    adapters: Arc<Mutex<Vec<Arc<FakeAdapter>>>>,
    workers: Arc<Mutex<Vec<Arc<FakeWorker>>>>,
    outs: Arc<Mutex<Vec<Arc<FakeOutAdapter>>>>,
    logs: Arc<Mutex<Vec<(String, Value)>>>,
    svc: Arc<Service>,
    chan: u64,
    target: u64,
}

fn base_config(dir: &Path) -> RelayConfig {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml");
    let mut c = load_config(&root).unwrap();
    // resolved at startup by relayctl run
    c.discord.expected_relay_user_id = RELAY_ID;
    c.runtime.start_debounce_ms = 0;
    c.liveness.source_dead_timeout_seconds = Some(30.0);
    c.storage.state_directory = dir.to_path_buf();
    // tests opt in
    c.control.command_cooldown_seconds = 0;
    c.control.change_cooldown_seconds = 0;
    c.control.operator_user_ids = BTreeSet::from([OPERATOR]);
    c
}

fn try_env(setup: Setup) -> Result<Env, ServiceError> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut c = base_config(&path);
    c.codecs.output_codec = if setup.adaptive {
        OutputCodec::Adaptive
    } else {
        OutputCodec::H264
    };
    c.discord.ingest_publishes = setup.ingest_publishes;
    if !setup.accounts.is_empty() {
        c.discord.output_accounts = setup.accounts.clone();
        c.runtime.retry_delays_seconds = vec![0.01];
    }
    if let Some(edit) = setup.cfg {
        edit(&mut c);
    }
    let store = StateStore::new(&path);
    let (enabled, latch) = (setup.enabled, setup.latch.clone());
    store
        .update(|mut s| {
            s.desired_enabled = enabled;
            s.fatal_latch = latch;
            s
        })
        .unwrap();
    if let Some(edit) = setup.state {
        store.update(edit).unwrap();
    }
    let order = Arc::new(Mutex::new(Vec::new()));
    let adapters: Arc<Mutex<Vec<Arc<FakeAdapter>>>> = Arc::new(Mutex::new(Vec::new()));
    let workers: Arc<Mutex<Vec<Arc<FakeWorker>>>> = Arc::new(Mutex::new(Vec::new()));
    let outs: Arc<Mutex<Vec<Arc<FakeOutAdapter>>>> = Arc::new(Mutex::new(Vec::new()));
    let logs: Arc<Mutex<Vec<(String, Value)>>> = Arc::new(Mutex::new(Vec::new()));

    let adapter_factory = {
        let (order, adapters) = (Arc::clone(&order), Arc::clone(&adapters));
        let fail_start = setup.adapter_fail_start;
        Box::new(move |events: EventSink, _ingress, _log| {
            let a = FakeAdapter::new(events, Arc::clone(&order));
            a.fail_start.store(fail_start, Ordering::SeqCst);
            adapters.lock().push(Arc::clone(&a));
            a as Arc<dyn Adapter>
        })
    };
    let worker_factory = {
        let workers = Arc::clone(&workers);
        let adaptive = setup.adaptive;
        let hook = setup.worker_hook.clone();
        Arc::new(move |_profile: OutputProfile| {
            if let Some(h) = &hook {
                h();
            }
            let w = FakeWorker::new(adaptive);
            workers.lock().push(Arc::clone(&w));
            Ok(w as Arc<dyn Worker>)
        })
    };
    let log: LogFn = {
        let logs = Arc::clone(&logs);
        Arc::new(move |name: &str, fields: Value| logs.lock().push((name.to_string(), fields)))
    };
    let mut opts = ServiceOptions {
        slate_builder: setup
            .slate_builder
            .unwrap_or_else(|| Arc::new(|_: &SlateKey| Ok(Arc::clone(&SLATE)))),
        install_signals: false,
        tick_s: 0.02,
        ..ServiceOptions::default()
    };
    if let Some(v) = setup.pli_min_interval_s {
        opts.pli_min_interval_s = v;
    }
    if let Some(v) = setup.voice_lost_grace_s {
        opts.voice_lost_grace_s = v;
    }
    if let Some(v) = setup.peer_is_operator {
        opts.peer_is_operator = v;
    }
    if !setup.accounts.is_empty() {
        let (order, outs2) = (Arc::clone(&order), Arc::clone(&outs));
        let out_start = setup.out_start.clone();
        let (fail_publish, codec) = (setup.out_fail_publish, setup.out_codec);
        opts.output_adapter_factory = Some(Box::new(move |acct: &OutputAccount, events, _log| {
            let a = Arc::new(FakeOutAdapter {
                uid: acct.user_id,
                events,
                order: Arc::clone(&order),
                calls: Mutex::new(Vec::new()),
                channels: Mutex::new(Vec::new()),
                codec: codec.to_string(),
                attempt: AtomicU64::new(0),
                start_hook: out_start.as_ref().and_then(|f| f(&outs2)),
                fail_publish,
            });
            outs2.lock().push(Arc::clone(&a));
            a as Arc<dyn OutputAdapter>
        }));
        opts.output_tokens = setup
            .accounts
            .iter()
            .map(|a| (a.user_id, Token::new("tok2")))
            .collect();
    }
    let svc = Service::new(
        c.clone(),
        StateStore::new(&path),
        adapter_factory,
        worker_factory,
        log,
        opts,
    )?;
    Ok(Env {
        chan: c.discord.allowed_voice_channel_id,
        target: c.discord.target_user_id,
        _dir: dir,
        dir: path,
        cfg: c,
        store,
        order,
        adapters,
        workers,
        outs,
        logs,
        svc,
    })
}

fn env(setup: Setup) -> Env {
    try_env(setup).unwrap_or_else(|e| panic!("service: {e}"))
}

impl Env {
    fn snapshot(&self) -> Event {
        Event::GatewaySnapshot {
            target: Some(TargetVoice {
                channel_id: Some(self.chan),
                self_stream: true,
            }),
            stream: Some(stream("key-1", self.target, self.chan, 1)),
            participants: Some(HashSet::from([self.target])),
            new_session: true,
        }
    }

    /// The target is in the channel but not streaming.
    fn idle_snapshot(&self) -> Event {
        Event::GatewaySnapshot {
            target: Some(TargetVoice {
                channel_id: Some(self.chan),
                self_stream: false,
            }),
            stream: None,
            participants: Some(HashSet::from([self.target])),
            new_session: true,
        }
    }

    fn start(&self) -> JoinHandle<i32> {
        let s = Arc::clone(&self.svc);
        tokio::spawn(async move { s.run(Token::new("tok")).await })
    }

    fn adapter(&self, i: usize) -> Arc<FakeAdapter> {
        Arc::clone(&self.adapters.lock()[i])
    }

    fn last_adapter(&self) -> Arc<FakeAdapter> {
        Arc::clone(self.adapters.lock().last().expect("an adapter"))
    }

    fn adapter_started(&self, i: usize) -> bool {
        self.adapters.lock().get(i).is_some_and(|a| a.started())
    }

    fn worker(&self, i: usize) -> Arc<FakeWorker> {
        Arc::clone(&self.workers.lock()[i])
    }

    fn out(&self, uid: u64) -> Option<Arc<FakeOutAdapter>> {
        self.outs.lock().iter().find(|o| o.uid == uid).cloned()
    }

    fn published(&self, uid: u64) -> bool {
        self.outs
            .lock()
            .iter()
            .any(|o| o.uid == uid && !o.channels.lock().is_empty())
    }

    fn state(&self) -> RelayState {
        self.svc.with_sup(|s| s.state())
    }

    fn generation(&self) -> u64 {
        self.svc.with_sup(|s| s.generation())
    }

    fn live(&self, uid: u64) -> bool {
        let (g, e) = self.svc.current_ids();
        self.svc.with_pool(|p| p.account_live(uid, g, e))
    }

    fn wanted(&self) -> usize {
        self.svc.with_pool(|p| p.relay_summary().wanted)
    }

    fn running(&self) -> usize {
        self.svc.with_pool(|p| p.relay_summary().running)
    }

    fn has_log(&self, name: &str) -> bool {
        self.logs.lock().iter().any(|(n, _)| n == name)
    }

    fn log_count(&self, name: &str) -> usize {
        self.logs.lock().iter().filter(|(n, _)| n == name).count()
    }

    fn logged(&self, name: &str) -> Vec<Value> {
        self.logs
            .lock()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
            .collect()
    }

    fn timings(&self) -> Vec<Value> {
        self.logged("session_timing")
    }

    fn order(&self) -> Vec<String> {
        self.order.lock().clone()
    }

    fn load(&self) -> PersistedState {
        self.store.load().unwrap()
    }

    fn update(&self, f: impl FnOnce(PersistedState) -> PersistedState) {
        self.store.update(f).unwrap();
    }

    async fn chat(&self, text: &str, msg_id: u64, voice: Option<u64>, author: u64) {
        (self.last_adapter().events)(AdapterEvent::Chat(ChatCommand {
            author_id: author,
            channel_id: CMD_CHAN,
            message_id: msg_id,
            text: text.to_string(),
            author_role_ids: BTreeSet::new(),
            author_voice_channel_id: voice,
        }));
        self.svc.flush().await;
    }

    async fn op(&self, text: &str, msg_id: u64) {
        self.chat(text, msg_id, None, OPERATOR).await;
    }

    async fn emit(&self, ev: Event) {
        self.last_adapter().emit(ev);
        self.svc.flush().await;
    }

    async fn reach_relaying(&self) -> JoinHandle<i32> {
        let task = self.start();
        wait_until(|| self.adapter_started(0)).await;
        let a = self.adapter(0);
        a.emit(self.snapshot());
        wait_until(|| a.has("watch")).await;
        a.emit(Event::SourceProgress {
            generation: self.generation(),
            at: monotonic(),
            video: true,
        });
        wait_until(|| self.state() == RelayState::Relaying).await;
        task
    }

    async fn shutdown(&self, task: JoinHandle<i32>) -> i32 {
        self.svc.request_shutdown();
        tokio::time::timeout(Duration::from_secs(40), task)
            .await
            .expect("service stopped in time")
            .expect("run task")
    }
}

fn stream(key: &str, owner: u64, chan: u64, incarnation: i64) -> SourceStream {
    SourceStream {
        key: key.into(),
        owner_id: owner,
        channel_id: chan,
        incarnation,
        paused: None,
        unavailable: false,
    }
}

fn routing(target: u64, chan: u64, mirrors: &[(&str, u64)]) -> PersistedRouting {
    PersistedRouting {
        target_user_id: target,
        channel_id: chan,
        mirrors: mirrors
            .iter()
            .map(|(ch, acct)| (ch.to_string(), *acct))
            .collect::<IndexMap<_, _>>(),
    }
}

async fn wait_until(pred: impl FnMut() -> bool) {
    wait_until_t(pred, 8.0).await;
}

async fn wait_until_t(mut pred: impl FnMut() -> bool, timeout_s: f64) {
    let end = Instant::now() + Duration::from_secs_f64(timeout_s);
    while Instant::now() < end {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached in time");
}

async fn sleep(s: f64) {
    tokio::time::sleep(Duration::from_secs_f64(s)).await;
}

fn closed_4006() -> AdapterError {
    AdapterError::Voice(VoiceError::Closed {
        code: Some(4006),
        reason: "4006".into(),
    })
}

/// A blocking one-shot release for slate builders and worker factories (Python
/// `threading.Event`).
#[derive(Clone, Default)]
struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl Release {
    fn set(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }

    fn wait(&self, timeout_s: f64) -> bool {
        let guard = self.0.0.lock().unwrap();
        let (guard, _) = self
            .0
            .1
            .wait_timeout_while(guard, Duration::from_secs_f64(timeout_s), |set| !*set)
            .unwrap();
        *guard
    }
}

fn manual_clock() -> (Arc<Mutex<f64>>, Clock) {
    let t = Arc::new(Mutex::new(1000.0));
    let read = Arc::clone(&t);
    (t, Arc::new(move || *read.lock()))
}

fn advance(t: &Arc<Mutex<f64>>, by: f64) {
    *t.lock() += by;
}

macro_rules! service_test {
    ($name:ident, $body:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            $body.await
        }
    };
}

// ------------------------------------------------------------------ session lifecycle

service_test!(test_flow_a01_opens_gate_and_publishes_source, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    let publisher = env.svc.publisher();
    let mut seen: Vec<Vec<u8>> = Vec::new();
    wait_until(|| {
        if let Some(f) = publisher.next_video() {
            seen.push(f.data);
        }
        seen.contains(&SOURCE_GOP[0])
    })
    .await;
    let a = env.adapter(0);
    assert!(a.has("create_output") && a.has("play"));
    let status = env.svc.control(&json!({"cmd": "status"}));
    assert_eq!(status["ok"], json!(true));
    assert!(serde_json::to_string(&status).is_ok());
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(test_shutdown_closes_gate_before_teardown, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    let (publisher, order) = (env.svc.publisher(), Arc::clone(&env.order));
    *a.before_end_output.lock() = Some(Arc::new(move || {
        if !publisher.has_source() {
            order.lock().push("gate_closed".into());
        }
    }));
    assert_eq!(env.shutdown(task).await, 0);
    let o = env.order();
    let at = |s: &str| o.iter().position(|x| x == s).unwrap_or(usize::MAX);
    assert!(
        at("gate_closed") < at("end_output") && at("end_output") < at("leave"),
        "{o:?}"
    );
    assert!(env.worker(0).stopped());
});

service_test!(test_fatal_latch_never_starts_adapter, async {
    let env = env(Setup {
        latch: Some(obj(
            json!({"reason": "AUTH_REJECTED", "detail": "", "at": 0.0}),
        )),
        ..Setup::default()
    });
    let task = env.start();
    sleep(0.3).await;
    assert_eq!(env.state(), RelayState::Fatal);
    assert_eq!(env.shutdown(task).await, 0);
    assert!(env.adapters.lock().is_empty());
});

service_test!(test_offline_disable_is_picked_up, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    env.update(|mut s| {
        s.desired_enabled = false;
        s
    });
    wait_until(|| env.state() == RelayState::Disabled).await;
    let (g, e) = env.svc.with_sup(|s| (s.generation(), s.epoch()));
    assert!(
        !env.svc
            .with_sup(|s| s.gate_allows(g, e, relay_core::clock::wall_clock()))
    );
    // gate closed and slate cleared: nothing is sent
    assert!(env.svc.publisher().next_video().is_none());
    env.shutdown(task).await;
});

service_test!(test_enable_and_reset_fatal_are_operator_only, async {
    let env = env(Setup::default());
    for cmd in ["enable", "reset-fatal"] {
        for uid in [Value::Null, json!(4242)] {
            let resp = env.svc.control(&json!({"cmd": cmd, "_peer_uid": uid}));
            assert_eq!(resp["ok"], json!(false));
            assert!(resp["error"].as_str().unwrap().contains("operator-only"));
        }
    }
});

// ------------------------------------------------------------------ output accounts

service_test!(
    test_output_account_fans_out_and_stops_with_gate_closed,
    async {
        let uid = 424242;
        // the relay publishes with any viewer count
        let env = env(Setup {
            accounts: vec![account(uid, AccountRole::Relay, None)],
            cfg: cfg(|c| {
                c.runtime.relay_autoscale = false;
                c.runtime.retry_delays_seconds = vec![1.0, 2.0, 4.0, 8.0, 15.0];
            }),
            ..Setup::default()
        });
        let task = env.reach_relaying().await;
        wait_until(|| {
            env.out(uid)
                .is_some_and(|o| o.calls().contains(&"publish".into()))
        })
        .await;
        wait_until(|| env.live(uid)).await;
        let publisher = env.svc.out_publisher(uid).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        wait_until(|| {
            if let Some(f) = publisher.next_video() {
                seen.push(f.data);
            }
            seen.contains(&SOURCE_GOP[0])
        })
        .await;
        assert_eq!(env.shutdown(task).await, 0);
        assert!(env.order().contains(&"out_end".into()));
        assert!(!env.live(uid));
    }
);

service_test!(
    test_output_login_is_retried_after_transient_failure,
    async {
        let uid = 424242;
        let env = env(Setup {
            out_start: Some(Arc::new(|outs: &Arc<Mutex<Vec<Arc<FakeOutAdapter>>>>| {
                let outs = Arc::clone(outs);
                Some(Arc::new(move || {
                    if outs.lock().len() < 3 {
                        Err(AdapterError::failed("transient"))
                    } else {
                        Ok(())
                    }
                }) as OutStartHook)
            })),
            ..multi(uid, AccountRole::Relay)
        });
        let task = env.reach_relaying().await;
        wait_until(|| env.live(uid)).await;
        let outs = env.outs.lock().clone();
        assert_eq!(outs.len(), 3);
        assert!(outs[2].calls().contains(&"publish".into()));
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(
    test_reset_fatal_account_zeroes_rejections_and_reports_unlatched,
    async {
        let uid = 424242;
        let latch = obj(json!({"reason": "RELAY_REJECTED_BY_SERVER", "detail": "", "at": 0.0}));
        let env = env(Setup {
            state: state(move |s| {
                set_account(s, uid, |r| {
                    r.fatal_latch = Some(latch);
                    r.consecutive_rejections = 3;
                    r.quarantine_incarnation = Some(5);
                })
            }),
            peer_is_operator: Some(Arc::new(|_| true)),
            ..multi(uid, AccountRole::Relay)
        });
        let resp = env
            .svc
            .control(&json!({"cmd": "reset-fatal", "account": uid.to_string(), "_peer_uid": 0}));
        assert_eq!(resp["ok"], json!(true));
        let v = account_view(&env.load(), uid);
        assert!(v.fatal_reason.is_none());
        assert_eq!(v.consecutive_rejections, 0);
        assert_eq!(v.quarantine_incarnation, Some(5));
        let again = env
            .svc
            .control(&json!({"cmd": "reset-fatal", "account": uid.to_string(), "_peer_uid": 0}));
        assert_eq!(again["message"], json!("no fatal latch set"));
    }
);

service_test!(
    test_output_account_mismatch_is_not_retried_and_ingest_keeps_relaying,
    async {
        let uid = 424242;
        let env = env(Setup {
            out_start: Some(Arc::new(|_: &Arc<Mutex<Vec<Arc<FakeOutAdapter>>>>| {
                Some(Arc::new(|| Err(AdapterError::AccountMismatch)) as OutStartHook)
            })),
            cfg: cfg(|c| {
                c.runtime.relay_autoscale = false;
                c.runtime.retry_delays_seconds = vec![1.0, 2.0];
            }),
            ..multi(uid, AccountRole::Relay)
        });
        let task = env.reach_relaying().await;
        sleep(2.5).await; // past the first retry delays (1 s, 2 s)
        let outs = env.outs.lock().clone();
        assert_eq!(outs.len(), 1); // a config error is not retried
        assert!(outs.iter().all(|o| !o.calls().contains(&"publish".into())));
        assert!(!env.live(uid));
        assert_eq!(env.state(), RelayState::Relaying);
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(test_persist_failure_closes_gates_and_exits_nonzero, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    // every later read and write of state.json fails (Python: store.update raises OSError)
    std::fs::write(env.dir.join("state.json"), b"{not json").unwrap();
    env.svc.execute(vec![Action::SetRejections { count: 1 }]);
    // gate closed, slate cleared
    assert!(env.svc.publisher().next_video().is_none());
    let code = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code, 1);
    assert!(env.has_log("persist_failed"));
});

service_test!(
    test_adapter_start_failure_returns_nonzero_and_closes,
    async {
        let env = env(Setup {
            adapter_fail_start: true,
            ..Setup::default()
        });
        let svc = Arc::clone(&env.svc);
        let code = tokio::time::timeout(Duration::from_secs(10), svc.run(Token::new("tok")))
            .await
            .unwrap();
        assert_eq!(code, 1);
        assert!(env.has_log("adapter_start_failed"));
        assert!(env.adapter(0).has("close"));
    }
);

service_test!(test_pli_inside_rate_limit_is_deferred_once, async {
    let env = env(Setup {
        pli_min_interval_s: Some(0.05),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    env.svc.request_pli();
    env.svc.request_pli();
    env.svc.request_pli();
    assert!(env.svc.pli_timer_pending());
    wait_until_t(|| a.count("pli") == 1, 1.0).await;
    sleep(0.15).await;
    assert_eq!(a.count("pli"), 2);
    assert!(!env.svc.pli_timer_pending());
    env.svc.request_pli();
    env.svc.request_pli();
    env.svc.cancel_pli_timer_hook();
    sleep(0.1).await;
    assert_eq!(a.count("pli"), 3);
    env.shutdown(task).await;
});

service_test!(test_out_pli_routes_to_need_idr, async {
    let env = env(Setup::default());
    let w = FakeWorker::new(false);
    env.svc.set_worker(Some(Arc::clone(&w) as Arc<dyn Worker>));
    env.svc
        .dispatch(AdapterEvent::Pool(PoolEvent::OutPli { account: None }));
    let force = w
        .sent
        .lock()
        .iter()
        .filter(|m| matches!(m, Ingress::ForceIdr(_)))
        .count();
    assert_eq!(force, 1);
});

service_test!(
    test_viewer_pli_requests_a_source_keyframe_rate_limited,
    async {
        let env = env(Setup::default());
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        let w = env.worker(0);
        let force = || {
            w.sent
                .lock()
                .iter()
                .filter(|m| matches!(m, Ingress::ForceIdr(_)))
                .count()
        };
        let before = a.count("pli");
        env.svc
            .dispatch(AdapterEvent::Pool(PoolEvent::OutPli { account: None }));
        assert_eq!(force(), 1);
        // a passthrough worker answers ForceIdr with need_keyframe (worker/process.rs)
        let (g, e) = env.svc.with_sup(|s| (s.generation(), s.epoch()));
        w.push_event(g, e, WorkerEventKind::NeedKeyframe, json!({}));
        wait_until_t(|| a.count("pli") == before + 1, 1.0).await;
        // a second viewer PLI inside feedback_idr_min_interval_ms asks for nothing more
        env.svc
            .dispatch(AdapterEvent::Pool(PoolEvent::OutPli { account: None }));
        assert_eq!(force(), 1);
        env.shutdown(task).await;
    }
);

service_test!(test_mandatory_idr_not_rate_limited_by_feedback, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    env.svc.need_idr();
    env.svc.set_last_idr_req(monotonic());
    let (g, e) = env.svc.with_sup(|s| (s.generation(), s.epoch()));
    env.svc.execute(vec![Action::OpenGate {
        generation: g,
        epoch: e,
    }]);
    assert_eq!(env.svc.last_idr_req(), f64::NEG_INFINITY);
    env.shutdown(task).await;
});

service_test!(test_status_network_only_when_enabled, async {
    let env = env(Setup::default());
    env.svc.set_network_stats(false);
    assert!(env.svc.status_dict().get("network").is_none());
    env.svc.set_network_stats(true);
    assert_eq!(env.svc.status_dict()["network"], json!({}));
    let a = FakeAdapter::detached();
    *a.network.lock() = json!({"watch": {"video_sequence_gaps": 3}});
    env.svc.set_adapter(Some(a as Arc<dyn Adapter>));
    assert_eq!(
        env.svc.status_dict()["network"],
        json!({"watch": {"video_sequence_gaps": 3}})
    );
});

service_test!(test_status_exposes_audio_counters, async {
    let env = env(Setup::default());
    assert_eq!(env.svc.status_dict()["ingress_audio"], json!({}));
    let a = FakeAdapter::detached();
    *a.audio.lock() = json!({"packets": 50, "silence": 50});
    env.svc.set_adapter(Some(a as Arc<dyn Adapter>));
    let st = env.svc.status_dict();
    assert_eq!(st["ingress_audio"], json!({"packets": 50, "silence": 50}));
    for k in [
        "audio_played",
        "audio_held_reads",
        "audio_late_dropped",
        "audio_evicted",
    ] {
        assert!(st["ingest_send"].get(k).is_some(), "{k}");
    }
});

// ------------------------------------------------------------------ chat

service_test!(test_chat_target_command_persists_and_restarts, async {
    let env = env(Setup::default());
    let new_target = 777;
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    let gen0 = env.generation();
    *a.snap.lock() = Some(Event::GatewaySnapshot {
        target: Some(TargetVoice {
            channel_id: Some(env.chan),
            self_stream: true,
        }),
        stream: Some(stream("key-2", new_target, env.chan, 2)),
        participants: Some(HashSet::from([new_target])),
        new_session: false,
    });
    env.op(&format!("!relay target {new_target}"), 900).await;
    assert_eq!(env.load().routing, Some(routing(new_target, env.chan, &[])));
    assert_eq!(env.svc.with_sup(|s| s.routing().target_user_id), new_target);
    assert_eq!((a.target_id(), a.channel_id()), (new_target, env.chan));
    // the new target is already streaming: the re-snapshot starts a new session without any
    // gateway event
    wait_until(|| env.generation() > gen0 && a.count("watch") == 2).await;
    wait_until(|| !a.replies().is_empty()).await;
    let replies = a.replies();
    assert_eq!(replies.len(), 1);
    let (ch, mid, text) = &replies[0];
    assert_eq!((*ch, *mid), (CMD_CHAN, 900));
    assert!(text.starts_with('✅') && text.contains(&new_target.to_string()));
    // a recreated adapter (reconnect / fatal reset) keeps the new routing
    env.svc.drop_adapter().await;
    env.svc.start_adapter().await.unwrap();
    assert_eq!(env.last_adapter().target_id(), new_target);
    assert_eq!(env.shutdown(task).await, 0);
    assert!(env.has_log("chat_command"));
});

service_test!(
    test_target_change_during_resume_resnapshots_when_fresh,
    async {
        let env = env(Setup::default());
        let new_target = 777;
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        env.emit(Event::GatewayChanged {
            state: GatewayState::Resuming,
        })
        .await;
        wait_until(|| env.svc.with_sup(|s| s.gateway()) == GatewayState::Resuming).await;
        *a.snap.lock() = Some(Event::GatewaySnapshot {
            target: Some(TargetVoice {
                channel_id: Some(env.chan),
                self_stream: true,
            }),
            stream: Some(stream("key-2", new_target, env.chan, 2)),
            participants: Some(HashSet::from([new_target])),
            new_session: false,
        });
        let snaps = a.count("snapshot");
        env.op(&format!("!relay target {new_target}"), 900).await;
        wait_until(|| env.svc.with_sup(|s| s.routing().target_user_id) == new_target).await;
        sleep(0.1).await;
        assert_eq!(a.count("snapshot"), snaps); // the cache is not trusted while resuming
        env.emit(Event::GatewayChanged {
            state: GatewayState::Fresh,
        })
        .await;
        wait_until(|| a.count("snapshot") == snaps + 1).await;
        assert_eq!(
            env.svc.with_sup(|s| s.stream().map(|st| st.owner_id)),
            Some(new_target)
        );
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(
    test_chat_rejection_and_status_reply_without_state_change,
    async {
        let env = env(Setup::default());
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        let rev = env.load().revision;
        env.op(&format!("!relay target {RELAY_ID}"), 1).await;
        env.op("!relay target <@123>", 2).await;
        env.op("!relay status", 3).await;
        env.op("just talking", 4).await;
        wait_until(|| a.replies().len() == 3).await;
        let replies = a.replies();
        assert_eq!(replies.iter().map(|r| r.1).collect::<Vec<_>>(), [1, 2, 3]);
        assert!(replies[0].2.starts_with("⚠️") && replies[1].2.starts_with('❌'));
        assert!(replies[2].2.starts_with("# 📡"));
        assert!(replies.iter().all(|r| !r.2.contains("<@")));
        assert_eq!(env.load().revision, rev);
        env.shutdown(task).await;
    }
);

service_test!(
    test_chat_status_verbose_adds_technical_section_without_consuming_window,
    async {
        let env = env(Setup::default());
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        env.op("!relay status -v", 1).await;
        wait_until(|| a.replies().len() == 1).await;
        let text = &a.replies()[0].2;
        assert!(text.starts_with("# 📡"));
        assert!(
            text.contains("### 📊 Kỹ thuật") && text.contains("> Nguồn: mất"),
            "{text}"
        );
        assert!(env.svc.status_details(0.0, false).tech.is_none());
        env.shutdown(task).await;
    }
);

service_test!(test_chat_off_reuses_enable_path, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    env.op("!relay off", 900).await;
    assert!(!env.load().desired_enabled);
    wait_until(|| env.state() == RelayState::Disabled).await;
    wait_until(|| !env.adapter(0).replies().is_empty()).await;
    assert!(env.adapter(0).texts()[0].starts_with('✅'));
    env.shutdown(task).await;
});

service_test!(
    test_state_file_routing_change_applies_without_reply,
    async {
        let env = env(Setup::default());
        let task = env.reach_relaying().await;
        let r = routing(778, env.chan, &[]);
        env.update(move |mut s| {
            s.routing = Some(r);
            s
        });
        wait_until(|| env.svc.with_sup(|s| s.routing().target_user_id) == 778).await;
        assert_eq!(env.adapter(0).target_id(), 778);
        sleep(0.1).await;
        assert!(env.adapter(0).replies().is_empty());
        env.shutdown(task).await;
    }
);

service_test!(test_invalid_persisted_routing_fails_closed, async {
    let env = env(Setup::default());
    // no routing persisted: config values
    assert_eq!(env.svc.with_sup(|s| s.routing().target_user_id), env.target);
    let bad = [
        routing(RELAY_ID, env.chan, &[]),
        // not an output account
        routing(777, env.chan, &[("999", 31337)]),
    ];
    for r in bad {
        env.update(move |mut s| {
            s.routing = Some(r);
            s
        });
        let res = Service::new(
            env.cfg.clone(),
            StateStore::new(&env.dir),
            Box::new(|_, _, _| FakeAdapter::detached() as Arc<dyn Adapter>),
            Arc::new(|_| Ok(FakeWorker::new(false) as Arc<dyn Worker>)),
            Arc::new(|_: &str, _: Value| {}),
            ServiceOptions {
                install_signals: false,
                ..ServiceOptions::default()
            },
        );
        assert!(
            matches!(res, Err(ServiceError::StateCorrupt(m)) if m.contains("state.json routing"))
        );
    }
});

service_test!(
    test_mirror_command_starts_account_in_mirror_channel,
    async {
        let (uid, mirror_chan) = (424242, 999);
        let env = env(multi(uid, AccountRole::Mirror));
        let task = env.reach_relaying().await;
        // unassigned mirror account stays off
        assert_eq!(
            env.svc
                .with_pool(|p| p.status(monotonic())[0].state.as_str()),
            "off"
        );
        let a = env.adapter(0);
        a.set_voice_ids(&[env.chan, mirror_chan]);
        env.op(&format!("!relay mirror {mirror_chan}"), 900).await;
        assert_eq!(
            env.load().routing.unwrap().mirrors,
            IndexMap::from([(mirror_chan.to_string(), uid)])
        );
        wait_until(|| env.out(uid).is_some_and(|o| o.published_to(mirror_chan))).await;
        wait_until(|| !a.replies().is_empty()).await;
        assert!(a.texts()[0].starts_with('✅'));
        let st = env.svc.status_dict();
        assert_eq!(st["routing"]["mirrors"], json!({"999": uid}));
        assert_eq!(st["accounts"][0]["channel_id"], json!(mirror_chan));
        // the main session is not interrupted
        assert_eq!(env.state(), RelayState::Relaying);
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(test_bare_mirror_uses_operator_voice_channel, async {
    let (uid, mirror_chan) = (424242, 999);
    let env = env(multi(uid, AccountRole::Mirror));
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    a.set_voice_ids(&[env.chan, mirror_chan]);
    env.chat("!relay mirror", 900, Some(mirror_chan), OPERATOR)
        .await;
    assert_eq!(
        env.load().routing.unwrap().mirrors,
        IndexMap::from([(mirror_chan.to_string(), uid)])
    );
    wait_until(|| env.out(uid).is_some_and(|o| o.published_to(mirror_chan))).await;
    wait_until(|| !a.replies().is_empty()).await;
    assert!(a.texts()[0].starts_with('✅'));
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(
    test_bare_mirror_without_voice_or_from_main_channel_is_refused,
    async {
        let (uid, mirror_chan) = (424242, 999);
        let env = env(multi(uid, AccountRole::Mirror));
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        a.set_voice_ids(&[env.chan, mirror_chan]);
        env.op("!relay mirror", 1).await;
        env.chat("!relay mirror", 2, Some(env.chan), OPERATOR).await;
        wait_until(|| a.replies().len() == 2).await;
        assert!(a.texts().iter().all(|t| t.starts_with("⚠️")));
        assert!(env.svc.with_sup(|s| s.routing().mirrors.is_empty()));
        assert!(env.load().routing.is_none_or(|r| r.mirrors.is_empty()));
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(test_non_operator_may_use_public_commands_only, async {
    let (uid, mirror_chan, guest) = (424242, 999, 51);
    let env = env(multi(uid, AccountRole::Mirror));
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    a.set_voice_ids(&[env.chan, mirror_chan, 998]);
    let rev = env.load().revision;
    let texts = [
        "!relay target 777".to_string(),
        format!("!relay channel {mirror_chan}"),
        "!relay off".into(),
        "!relay on".into(),
        "!relay prejoin off".into(),
        "!relay autoscale on".into(),
        "!relay".into(),
        "!relay wat".into(),
        "!relay mirror 1 2 3".into(),
        "!relay mirror <@1>".into(),
    ];
    for (i, text) in texts.iter().enumerate() {
        env.chat(text, 10 + i as u64, Some(mirror_chan), guest)
            .await;
    }
    sleep(0.1).await;
    assert!(a.replies().is_empty());
    assert_eq!(env.load().revision, rev);
    assert!(env.svc.with_sup(|s| s.desired_enabled()));
    env.chat("!relay status", 1, None, guest).await;
    env.chat("!relay mirror", 2, Some(mirror_chan), guest).await;
    wait_until(|| a.replies().len() == 2).await;
    let t = a.texts();
    assert!(t[0].starts_with("# 📡") && t[1].starts_with('✅'));
    assert_eq!(
        env.svc.with_sup(|s| s.routing().mirrors),
        IndexMap::from([(mirror_chan, uid)])
    );
    // unmirror is public by default
    env.chat("!relay unmirror", 3, Some(mirror_chan), guest)
        .await;
    wait_until(|| a.replies().len() == 3).await;
    assert!(a.texts()[2].starts_with('✅'));
    assert!(env.svc.with_sup(|s| s.routing().mirrors.is_empty()));
    env.chat(&format!("!relay mirror {mirror_chan}"), 4, None, guest)
        .await;
    env.chat(&format!("!relay unmirror {mirror_chan}"), 5, None, guest)
        .await;
    wait_until(|| a.replies().len() == 5).await;
    assert!(a.texts()[3..].iter().all(|t| t.starts_with('✅')));
    assert!(env.svc.with_sup(|s| s.routing().mirrors.is_empty()));
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(
    test_per_author_cooldown_one_notice_then_silence_operators_exempt,
    async {
        let env = env(Setup::default());
        let (clk, clock) = manual_clock();
        env.svc.set_chat_cooldowns(5.0, 3.0, clock);
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        env.chat("!relay status", 1, None, 51).await;
        advance(&clk, 1.0);
        env.chat("!relay status", 2, None, 51).await;
        env.chat("!relay status", 3, None, 51).await;
        env.chat("!relay status", 4, None, 52).await; // another author is not limited
        for mid in [5, 6, 7] {
            env.op("!relay status", mid).await; // operator: exempt
        }
        advance(&clk, 4.0);
        env.chat("!relay status", 8, None, 51).await;
        wait_until(|| a.replies().len() == 7).await;
        let replies = a.replies();
        assert_eq!(
            replies.iter().map(|r| r.1).collect::<Vec<_>>(),
            [1, 2, 4, 5, 6, 7, 8]
        );
        assert_eq!(replies[1].2, "⏳ chậm lại chút, thử lại sau 4s");
        assert!(
            replies
                .iter()
                .enumerate()
                .all(|(i, r)| i == 1 || r.2.starts_with("# 📡"))
        );
        env.shutdown(task).await;
        let cooldowns = env
            .logged("chat_command")
            .iter()
            .filter(|kw| kw["outcome"] == json!("cooldown"))
            .count();
        assert_eq!(cooldowns, 2);
    }
);

service_test!(
    test_global_change_cooldown_blocks_everyone_but_not_status,
    async {
        let env = env(Setup::default());
        let (clk, clock) = manual_clock();
        env.svc.set_chat_cooldowns(5.0, 3.0, clock);
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        env.op("!relay prejoin off", 1).await;
        assert!(!env.svc.with_sup(|s| s.prejoin_voice()));
        advance(&clk, 1.0);
        env.op("!relay prejoin on", 2).await; // operators are not exempt
        env.chat("!relay mirror", 3, None, 51).await;
        env.op("!relay status", 4).await;
        env.chat("!relay status", 5, None, 52).await;
        assert!(!env.svc.with_sup(|s| s.prejoin_voice()));
        assert_eq!(env.load().prejoin_voice, Some(false));
        advance(&clk, 2.0);
        env.op("!relay prejoin on", 6).await;
        assert!(env.svc.with_sup(|s| s.prejoin_voice()));
        wait_until(|| a.replies().len() == 6).await;
        let t = a.texts();
        assert!(t[0].starts_with('✅') && t[5].starts_with('✅'));
        assert_eq!(t[1], "⏳ vừa có thay đổi, thử lại sau 2s");
        assert_eq!(t[2], t[1]);
        assert!(t[3].starts_with("# 📡") && t[4].starts_with("# 📡"));
        env.shutdown(task).await;
        let cooled: Vec<Value> = env
            .logged("chat_command")
            .into_iter()
            .filter(|kw| kw["outcome"] == json!("cooldown"))
            .collect();
        assert_eq!(
            cooled
                .iter()
                .map(|kw| kw["command"].clone())
                .collect::<Vec<_>>(),
            [json!("SetPrejoin"), json!("AddMirror")]
        );
        assert!(cooled.iter().all(|kw| kw.get("text").is_none()));
    }
);

service_test!(
    test_global_change_cooldown_notifies_a_guest_once_per_window,
    async {
        let (uid, mirror_chan) = (424242, 999);
        let env = env(multi(uid, AccountRole::Mirror));
        let (clk, clock) = manual_clock();
        env.svc.set_chat_cooldowns(5.0, 3.0, clock);
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        a.set_voice_ids(&[env.chan, mirror_chan]);
        env.op("!relay prejoin off", 1).await;
        advance(&clk, 1.0);
        for i in [2, 3, 4] {
            env.chat(&format!("!relay mirror {mirror_chan}"), i, None, 51)
                .await;
        }
        advance(&clk, 2.0);
        env.chat(&format!("!relay mirror {mirror_chan}"), 5, None, 51)
            .await;
        wait_until(|| a.replies().len() == 3).await;
        sleep(0.1).await;
        let t = a.texts();
        assert_eq!(t.len(), 3);
        assert_eq!(t[1], "⏳ vừa có thay đổi, thử lại sau 2s");
        assert!(t[2].starts_with('✅'));
        assert_eq!(
            env.svc.with_sup(|s| s.routing().mirrors),
            IndexMap::from([(mirror_chan, uid)])
        );
        env.shutdown(task).await;
    }
);

service_test!(
    test_global_change_cooldown_does_not_start_the_author_window,
    async {
        let (uid, mirror_chan) = (424242, 999);
        let env = env(multi(uid, AccountRole::Mirror));
        let (clk, clock) = manual_clock();
        env.svc.set_chat_cooldowns(5.0, 3.0, clock);
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        a.set_voice_ids(&[env.chan, mirror_chan]);
        env.op("!relay prejoin off", 1).await;
        advance(&clk, 1.0);
        env.chat(&format!("!relay mirror {mirror_chan}"), 2, None, 51)
            .await;
        advance(&clk, 2.0); // the stated wait
        env.chat(&format!("!relay mirror {mirror_chan}"), 3, None, 51)
            .await;
        wait_until(|| a.replies().len() == 3).await;
        let t = a.texts();
        assert_eq!(t[1], "⏳ vừa có thay đổi, thử lại sau 2s");
        assert!(t[2].starts_with('✅'));
        assert_eq!(
            env.svc.with_sup(|s| s.routing().mirrors),
            IndexMap::from([(mirror_chan, uid)])
        );
        env.shutdown(task).await;
    }
);

service_test!(test_failed_mirror_publish_logs_output_failed, async {
    let (uid, mirror_chan) = (424242, 999);
    let env = env(Setup {
        out_fail_publish: true,
        ..multi(uid, AccountRole::Mirror)
    });
    let task = env.reach_relaying().await;
    env.adapter(0).set_voice_ids(&[env.chan, mirror_chan]);
    env.op(&format!("!relay mirror {mirror_chan}"), 900).await;
    wait_until(|| env.has_log("output_failed")).await;
    let kw = &env.logged("output_failed")[0];
    assert_eq!(kw["account"], json!(uid));
    assert_eq!(kw["role"], json!("mirror"));
    assert_eq!(kw["channel"], json!(mirror_chan));
    assert_eq!(kw["reason"], json!("connect_failed"));
    assert_eq!(kw["retryable"], json!(true));
    assert!(kw["detail"].as_str().unwrap().contains("boom"));
    assert_eq!(env.shutdown(task).await, 0);
});

// ------------------------------------------------------- start latency / slot race

service_test!(
    test_output_plays_before_slate_finishes_and_late_slate_is_applied,
    async {
        let release = Release::default();
        let r = release.clone();
        let env = env(Setup {
            // the slate build outlasts the whole output creation
            slate_builder: Some(Arc::new(move |_: &SlateKey| {
                r.wait(5.0);
                Ok(Arc::clone(&SLATE))
            })),
            ..Setup::default()
        });
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        assert!(a.has("play") && env.svc.publisher().slate().is_none());
        release.set();
        wait_until(|| {
            env.svc
                .publisher()
                .slate()
                .is_some_and(|s| Arc::ptr_eq(&s, &SLATE))
        })
        .await;
        assert!(env.svc.slate().is_some_and(|s| Arc::ptr_eq(&s, &SLATE)));
        env.shutdown(task).await;
    }
);

service_test!(test_stale_slate_is_ignored_after_teardown, async {
    let release = Release::default();
    let r = release.clone();
    let env = env(Setup {
        slate_builder: Some(Arc::new(move |_: &SlateKey| {
            if r.wait(5.0) {
                Ok(Arc::clone(&SLATE))
            } else {
                Err("not released".into())
            }
        })),
        ..Setup::default()
    });
    let task = env.reach_relaying().await;
    env.emit(Event::StreamEnded {
        key: "key-1".into(),
        incarnation: 1,
    })
    .await;
    wait_until(|| env.state() == RelayState::Idle).await;
    wait_until(|| env.order().contains(&"end_output".into())).await;
    release.set();
    wait_until(|| env.svc.slate_tasks() == 0).await;
    assert!(env.svc.slate().is_none() && env.svc.publisher().slate().is_none());
    env.shutdown(task).await;
});

service_test!(test_slate_is_cached_across_sessions, async {
    let built = Arc::new(AtomicUsize::new(0));
    let b = Arc::clone(&built);
    let env = env(Setup {
        slate_builder: Some(Arc::new(move |_: &SlateKey| {
            b.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&SLATE))
        })),
        ..Setup::default()
    });
    let task = env.reach_relaying().await;
    wait_until(|| env.svc.slate_tasks() == 0).await;
    let a = env.adapter(0);
    env.emit(Event::StreamEnded {
        key: "key-1".into(),
        incarnation: 1,
    })
    .await;
    wait_until(|| env.state() == RelayState::Idle).await;
    env.emit(Event::StreamStarted {
        stream: stream("key-1", env.target, env.chan, 2),
    })
    .await;
    wait_until(|| a.count("watch") == 2).await;
    env.emit(Event::SourceProgress {
        generation: env.generation(),
        at: monotonic(),
        video: true,
    })
    .await;
    wait_until(|| env.state() == RelayState::Relaying).await;
    env.shutdown(task).await;
    assert_eq!(built.load(Ordering::SeqCst), 1);
});

service_test!(
    test_slate_failure_is_logged_and_does_not_fail_the_session,
    async {
        let env = env(Setup {
            slate_builder: Some(Arc::new(|_: &SlateKey| Err("slate broke".into()))),
            ..Setup::default()
        });
        let task = env.reach_relaying().await;
        wait_until(|| env.has_log("slate_failed")).await;
        assert!(env.adapter(0).has("play") && env.state() == RelayState::Relaying);
        env.shutdown(task).await;
    }
);

service_test!(test_slate_skipped_when_nothing_publishes, async {
    let built = Arc::new(AtomicUsize::new(0));
    let b = Arc::clone(&built);
    let env = env(Setup {
        ingest_publishes: false,
        slate_builder: Some(Arc::new(move |_: &SlateKey| {
            b.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&SLATE))
        })),
        ..Setup::default()
    });
    let task = env.reach_relaying().await;
    env.shutdown(task).await;
    assert_eq!(built.load(Ordering::SeqCst), 0);
});

service_test!(test_pli_does_not_wait_behind_ops_queue, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    let release = Arc::new(tokio::sync::Notify::new());
    let r = Arc::clone(&release);
    env.svc.queue_op(async move { r.notified().await });
    let (g, e) = env.svc.with_sup(|s| (s.generation(), s.epoch()));
    env.svc.on_worker_event(WorkerEvent {
        generation: g,
        epoch: e,
        kind: WorkerEventKind::NeedKeyframe,
        data: Map::new(),
    });
    wait_until_t(|| a.count("pli") == 1, 1.0).await;
    env.svc.execute(vec![Action::RequestInputKeyframe {
        generation: g,
        method: KeyframeMethod::Pli,
    }]);
    wait_until_t(|| a.count("pli") == 2, 1.0).await;
    release.notify_one();
    env.shutdown(task).await;
});

service_test!(test_session_timing_logged_once_at_open, async {
    let env = env(Setup::default());
    let task = env.reach_relaying().await;
    env.shutdown(task).await;
    let t = env.timings();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0]["outcome"], json!("open"));
    assert_eq!(t[0]["prejoined"], json!(false));
    for k in [
        "start_from_seen_ms",
        "watch_sent_from_seen_ms",
        "watching_from_seen_ms",
        "voice_connected_ms",
        "voice_state_confirmed_ms",
        "voice_state_confirmed_from_seen_ms",
        "voice_connected_from_seen_ms",
        "start_from_gate_ready_ms",
        "gate_ready_from_seen_ms",
        "worker_started_ms",
        "watch_sent_ms",
        "watching_ms",
        "first_video_packet_ms",
        "first_frame_ms",
        "output_created_ms",
        "output_create_ms",
        "open_gate_ms",
    ] {
        assert!(t[0][k].as_i64().is_some_and(|v| v >= 0), "{k}: {}", t[0]);
    }
});

service_test!(test_prejoin_joins_before_stream_then_only_watches, async {
    let env = env(Setup {
        cfg: cfg(|c| c.runtime.prejoin_voice = true),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.emit(env.idle_snapshot());
    wait_until(|| a.has("join")).await;
    a.emit(env.snapshot());
    wait_until(|| a.has("watch")).await;
    a.emit(Event::SourceProgress {
        generation: env.generation(),
        at: monotonic(),
        video: true,
    });
    wait_until(|| env.state() == RelayState::Relaying).await;
    env.shutdown(task).await;
    assert_eq!(a.count("join"), 1);
    assert_eq!(env.timings()[0]["prejoined"], json!(true));
});

service_test!(
    test_full_stream_watch_retries_fast_without_rejoining,
    async {
        let env = env(Setup::default());
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        let a2 = Arc::clone(&a);
        *a.watch_hook.lock() = Some(Arc::new(move || {
            a2.push("watch");
            let first = a2.count("watch") == 1;
            Box::pin(async move {
                if first {
                    Err(AdapterError::Voice(VoiceError::StreamRejected(
                        "stream_full".into(),
                    )))
                } else {
                    Ok(())
                }
            })
        }));
        a.emit(env.snapshot());
        wait_until_t(|| a.count("watch") == 2, 3.0).await;
        assert_eq!(a.count("join"), 1);
        assert!(!env.order().contains(&"leave".into()));
        assert_eq!(
            env.svc.with_sup(|s| s
                .status(relay_core::clock::wall_clock())
                .consecutive_rejections),
            0
        );
        env.shutdown(task).await;
        assert_eq!(env.timings()[0]["outcome"], json!("connect_failed"));
    }
);

service_test!(
    test_watch_is_sent_on_voice_state_echo_before_voice_connects,
    async {
        let env = env(Setup::default());
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        let (watched_tx, watched_rx) = tokio::sync::watch::channel(false);
        let a2 = Arc::clone(&a);
        a.set_join(Arc::new(move |cb| {
            a2.push("join");
            if let Some(cb) = cb {
                cb();
            }
            let (a3, mut rx) = (Arc::clone(&a2), watched_rx.clone());
            Box::pin(async move {
                // sequential join-then-watch times out here
                tokio::time::timeout(Duration::from_secs(2), rx.wait_for(|w| *w))
                    .await
                    .map_err(|_| AdapterError::failed("timeout"))?
                    .map_err(|_| AdapterError::failed("closed"))?;
                a3.push("voice_connected");
                Ok("sess-1".to_string())
            })
        }));
        let a2 = Arc::clone(&a);
        *a.watch_hook.lock() = Some(Arc::new(move || {
            a2.push("watch");
            watched_tx.send_replace(true);
            Box::pin(async { Ok(()) })
        }));
        a.emit(env.snapshot());
        wait_until(|| a.has("voice_connected")).await;
        a.emit(Event::SourceProgress {
            generation: env.generation(),
            at: monotonic(),
            video: true,
        });
        wait_until(|| env.state() == RelayState::Relaying).await;
        env.shutdown(task).await;
        let calls = a.calls();
        let at = |s: &str| calls.iter().position(|c| c == s).unwrap();
        assert!(at("join") < at("watch") && at("watch") < at("voice_connected"));
        let t = &env.timings()[0];
        let ms = |k: &str| t[k].as_i64().unwrap();
        assert!(ms("voice_state_confirmed_ms") <= ms("watch_sent_ms"));
        assert!(ms("watch_sent_ms") <= ms("voice_connected_ms"));
    }
);

service_test!(
    test_join_failure_after_early_watch_fails_the_session,
    async {
        let env = env(Setup::default());
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        let a2 = Arc::clone(&a);
        a.set_join(Arc::new(move |cb| {
            a2.push("join");
            if let Some(cb) = cb {
                cb();
            }
            Box::pin(async {
                sleep(0.05).await;
                Err(AdapterError::failed("voice handshake failed"))
            })
        }));
        a.emit(env.snapshot());
        wait_until(|| env.state() == RelayState::Backoff).await;
        wait_until(|| env.order().contains(&"leave".into())).await;
        assert!(a.has("watch") && !env.svc.in_voice());
        env.shutdown(task).await;
        assert_eq!(env.timings()[0]["outcome"], json!("connect_failed"));
    }
);

service_test!(test_worker_spawns_concurrently_with_join, async {
    let spawning = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&spawning);
    let env = env(Setup {
        worker_hook: Some(Arc::new(move || s.store(true, Ordering::SeqCst))),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    let (a2, s2) = (Arc::clone(&a), Arc::clone(&spawning));
    a.set_join(Arc::new(move |cb| {
        a2.push("join");
        if let Some(cb) = cb {
            cb();
        }
        let s3 = Arc::clone(&s2);
        Box::pin(async move {
            // join-then-spawn would time out here
            wait_until_t(|| s3.load(Ordering::SeqCst), 2.0).await;
            Ok("sess-1".to_string())
        })
    }));
    a.emit(env.snapshot());
    wait_until(|| a.has("watch")).await;
    a.emit(Event::SourceProgress {
        generation: env.generation(),
        at: monotonic(),
        video: true,
    });
    wait_until(|| env.state() == RelayState::Relaying).await;
    env.shutdown(task).await;
    let workers = env.workers.lock().clone();
    assert_eq!(workers.len(), 1);
    assert!(workers[0].stopped());
    assert!(matches!(workers[0].sent.lock()[0], Ingress::Configure(_)));
});

service_test!(test_worker_spawned_after_teardown_is_stopped, async {
    let release = Release::default();
    let r = release.clone();
    let env = env(Setup {
        worker_hook: Some(Arc::new(move || {
            r.wait(3.0);
        })),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.emit(env.snapshot());
    wait_until(|| a.has("watch")).await;
    // stream gone while the worker is spawning
    a.emit(Event::TargetVoiceChanged {
        target: Some(TargetVoice {
            channel_id: Some(env.chan),
            self_stream: false,
        }),
    });
    wait_until(|| env.state() == RelayState::Idle).await;
    release.set();
    wait_until(|| env.workers.lock().first().is_some_and(|w| w.stopped())).await;
    assert!(!env.svc.has_worker());
    env.shutdown(task).await;
});

#[test]
fn test_watch_retryable_only_for_stream_full_or_timeout() {
    assert!(watch_retryable(&AdapterError::Voice(
        VoiceError::StreamRejected("stream_full".into())
    )));
    assert!(watch_retryable(&AdapterError::Voice(VoiceError::Timeout(
        "watching the stream"
    ))));
    assert!(!watch_retryable(&AdapterError::Voice(
        VoiceError::StreamRejected("unauthorized".into())
    )));
    assert!(!watch_retryable(&AdapterError::failed(
        "target stream changed before watch"
    )));
}

// ------------------------------------------------------------------ prejoin / voice loss

service_test!(test_chat_prejoin_toggle_persists_joins_and_leaves, async {
    let env = env(Setup {
        cfg: cfg(|c| c.runtime.prejoin_voice = false),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    env.emit(env.idle_snapshot()).await;
    sleep(0.1).await;
    assert!(!a.has("join")); // config: off
    env.op("!relay prejoin on", 1).await;
    assert_eq!(env.load().prejoin_voice, Some(true));
    assert!(env.svc.with_sup(|s| s.prejoin_voice()));
    wait_until(|| a.has("join")).await;
    env.op("!relay status", 2).await;
    env.op("!relay prejoin off", 3).await;
    assert_eq!(env.load().prejoin_voice, Some(false));
    wait_until(|| env.order().contains(&"leave".into())).await;
    wait_until(|| a.replies().len() == 3).await;
    let t = a.texts();
    assert!(t[0].starts_with('✅') && t[1].contains("prejoin bật"));
    assert!(t[2].starts_with('✅'));
    env.shutdown(task).await;
});

service_test!(test_persisted_prejoin_overrides_config, async {
    let env = env(Setup {
        cfg: cfg(|c| c.runtime.prejoin_voice = true),
        state: state(|mut s| {
            s.prejoin_voice = Some(false);
            s
        }),
        ..Setup::default()
    });
    assert!(!env.svc.with_sup(|s| s.prejoin_voice()));
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    // offline edit is picked up
    env.update(|mut s| {
        s.prejoin_voice = Some(true);
        s
    });
    wait_until(|| env.svc.with_sup(|s| s.prejoin_voice())).await;
    env.shutdown(task).await;
});

service_test!(test_prejoin_failure_is_reported_and_retried, async {
    let env = env(Setup {
        cfg: cfg(|c| {
            c.runtime.prejoin_voice = true;
            c.runtime.retry_delays_seconds = vec![0.2];
        }),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    let a2 = Arc::clone(&a);
    a.set_join(Arc::new(move |cb| {
        let r = if a2.count("join") == 0 {
            a2.push("join");
            Err(AdapterError::failed("no voice"))
        } else {
            a2.default_join(cb)
        };
        Box::pin(async move { r })
    }));
    a.emit(env.idle_snapshot());
    wait_until_t(|| a.count("join") == 2, 3.0).await;
    wait_until(|| env.svc.in_voice()).await;
    assert!(env.has_log("prejoin_failed"));
    env.shutdown(task).await;
});

service_test!(
    test_prejoined_dead_voice_leaves_and_rejoins_before_watch,
    async {
        let env = env(Setup {
            cfg: cfg(|c| c.runtime.prejoin_voice = true),
            ..Setup::default()
        });
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        a.emit(env.idle_snapshot());
        wait_until(|| env.svc.in_voice()).await;
        // the pre-joined parent died (e.g. its voice poller crashed)
        a.alive.store(false, Ordering::SeqCst);
        a.log_leave_call.store(true, Ordering::SeqCst);
        a.emit(env.snapshot());
        wait_until(|| a.has("watch")).await;
        let seq: Vec<String> = a
            .calls()
            .into_iter()
            .filter(|c| ["join", "leave", "watch"].contains(&c.as_str()))
            .collect();
        assert_eq!(seq, ["join", "leave", "join", "watch"]);
        assert!(env.has_log("prejoined_voice_dead"));
        env.shutdown(task).await;
    }
);

service_test!(test_ticker_reports_voice_lost_after_grace_only, async {
    let env = env(Setup {
        voice_lost_grace_s: Some(0.5),
        cfg: cfg(|c| {
            c.runtime.prejoin_voice = true;
            c.runtime.retry_delays_seconds = vec![30.0];
        }),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.emit(env.idle_snapshot());
    wait_until(|| env.svc.in_voice()).await;
    a.alive.store(false, Ordering::SeqCst);
    sleep(0.25).await;
    assert!(!env.has_log("voice_lost") && !env.order().contains(&"leave".into()));
    wait_until_t(|| env.has_log("voice_lost"), 3.0).await;
    wait_until(|| env.order().contains(&"leave".into())).await;
    sleep(0.6).await;
    assert_eq!(env.log_count("voice_lost"), 1);
    assert_eq!(env.order().iter().filter(|o| *o == "leave").count(), 1);
    assert_eq!(env.svc.metrics()["counters"]["event.VoiceLost"], json!(1));
    assert!(!env.svc.in_voice() && a.count("join") == 1);
    env.shutdown(task).await;
});

/// Python `_failing_join`: joins while `fails()` is false, else a voice close 4006.
fn failing_join(
    a: &Arc<FakeAdapter>,
    fails: impl Fn(&FakeAdapter) -> bool + Send + Sync + 'static,
) -> JoinHook {
    let a = Arc::clone(a);
    Arc::new(move |cb| {
        let r = if fails(&a) {
            a.push("join");
            Err(closed_4006())
        } else {
            a.default_join(cb)
        };
        Box::pin(async move { r })
    })
}

service_test!(test_repeated_4006_prejoin_restarts_adapter_once, async {
    let env = env(Setup {
        cfg: cfg(|c| {
            c.runtime.prejoin_voice = true;
            c.runtime.retry_delays_seconds = vec![0.1];
        }),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.set_join(failing_join(&a, |_| true));
    a.emit(env.idle_snapshot());
    wait_until_t(
        || env.adapters.lock().len() == 2 && env.adapter_started(1),
        3.0,
    )
    .await;
    assert!(a.count("join") == 2 && a.has("close"));
    let b = env.adapter(1);
    b.set_join(failing_join(&b, |_| true));
    b.emit(env.idle_snapshot());
    wait_until_t(|| b.count("join") >= 3, 3.0).await;
    assert_eq!(env.adapters.lock().len(), 2); // at most one restart per 5 minutes
    let codes: Vec<Value> = env
        .logged("prejoin_failed")
        .iter()
        .take(2)
        .map(|kw| kw["code"].clone())
        .collect();
    assert_eq!(codes, [json!(4006), json!(4006)]);
    env.shutdown(task).await;
});

service_test!(
    test_4006_with_foreign_session_does_not_restart_adapter,
    async {
        let env = env(Setup {
            cfg: cfg(|c| {
                c.runtime.prejoin_voice = true;
                c.runtime.retry_delays_seconds = vec![0.1];
                c.runtime.retry_cooldown_seconds = 0.6;
            }),
            ..Setup::default()
        });
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        a.foreign.store(true, Ordering::SeqCst);
        *a.last_vsu.lock() = Some("other-session".into());
        let stamps = Arc::new(Mutex::new(Vec::<f64>::new()));
        let failing = failing_join(&a, |_| true);
        let st = Arc::clone(&stamps);
        a.set_join(Arc::new(move |cb| {
            st.lock().push(monotonic());
            failing(cb)
        }));
        a.emit(env.idle_snapshot());
        wait_until_t(|| stamps.lock().len() >= 2, 5.0).await;
        let s = stamps.lock().clone();
        assert!(s[1] - s[0] >= 0.5, "{s:?}"); // retry_cooldown (0.6), not retry_delays (0.1)
        assert_eq!(env.adapters.lock().len(), 1);
        assert!(!a.has("close"));
        assert!(env.has_log("foreign_voice_session"));
        assert_eq!(
            env.logged("prejoin_failed")[0]["vsu_session"],
            json!("other-session")
        );
        env.shutdown(task).await;
    }
);

service_test!(
    test_late_foreign_prejoin_failure_does_not_tear_down_connecting_session,
    async {
        let env = env(Setup {
            cfg: cfg(|c| {
                c.runtime.prejoin_voice = true;
                c.runtime.retry_delays_seconds = vec![0.1];
            }),
            ..Setup::default()
        });
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let a = env.adapter(0);
        a.foreign.store(true, Ordering::SeqCst);
        *a.last_vsu.lock() = Some("other-session".into());
        let block = Release::default();
        let (a2, b2) = (Arc::clone(&a), block.clone());
        a.set_join(Arc::new(move |_cb| {
            a2.push("join");
            let first = a2.count("join") == 1;
            let b3 = b2.clone();
            Box::pin(async move {
                if first {
                    // the session's own join stays pending
                    tokio::task::spawn_blocking(move || b3.wait(10.0))
                        .await
                        .unwrap();
                    Ok("sess-1".to_string())
                } else {
                    Err(closed_4006())
                }
            })
        }));
        a.emit(env.snapshot());
        wait_until(|| env.state() == RelayState::Connecting && a.has("join")).await;
        env.svc.set_in_voice(false);
        // a pre-join queued before the session started fails late
        env.svc.prejoin().await;
        assert!(env.has_log("prejoin_failed"));
        assert_eq!(env.state(), RelayState::Connecting);
        block.set();
        wait_until(|| env.svc.in_voice()).await;
        tokio::time::timeout(Duration::from_secs(10), env.shutdown(task))
            .await
            .unwrap();
    }
);

service_test!(test_4006_on_connect_path_restarts_adapter, async {
    let env = env(Setup {
        cfg: cfg(|c| c.runtime.retry_delays_seconds = vec![0.1]),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.set_join(failing_join(&a, |_| true));
    a.emit(env.snapshot());
    wait_until_t(
        || env.adapters.lock().len() == 2 && env.adapter_started(1),
        5.0,
    )
    .await;
    assert!(a.count("join") == 2 && a.has("close"), "{:?}", a.calls());
    assert!(env.has_log("voice_4006_restart_adapter"));
    env.shutdown(task).await;
});

service_test!(test_4006_counter_resets_after_successful_join, async {
    let env = env(Setup {
        cfg: cfg(|c| {
            c.runtime.prejoin_voice = true;
            c.runtime.retry_delays_seconds = vec![0.1];
        }),
        ..Setup::default()
    });
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    let a = env.adapter(0);
    a.set_join(failing_join(&a, |a| a.count("join") == 0));
    a.emit(env.idle_snapshot());
    wait_until(|| env.svc.voice_4006() == 1).await;
    wait_until_t(|| env.svc.in_voice(), 3.0).await;
    assert_eq!(env.svc.voice_4006(), 0);
    assert_eq!(env.adapters.lock().len(), 1);
    env.shutdown(task).await;
});

// ------------------------------------------------------------------ routing in state.json

service_test!(test_runtime_invalid_routing_is_ignored_but_visible, async {
    let env = env(Setup::default());
    let bad = routing(RELAY_ID, env.chan, &[]);
    let rejected = || env.logged("state_routing_rejected");
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    let b = bad.clone();
    env.update(move |mut s| {
        s.routing = Some(b);
        s
    });
    wait_until(|| !rejected().is_empty()).await;
    assert_eq!(rejected()[0]["level"], json!("error"));
    assert!(
        rejected()[0]["error"]
            .as_str()
            .unwrap()
            .contains("relay account")
    );
    // still ignored at runtime
    assert_eq!(env.svc.with_sup(|s| s.routing().target_user_id), env.target);
    assert!(
        env.svc.status_dict()["routing_error"]
            .as_str()
            .unwrap()
            .contains("relay account")
    );
    env.op("!relay status", 1).await;
    wait_until(|| !a.replies().is_empty()).await;
    assert!(a.texts()[0].contains("⚠️") && a.texts()[0].contains("state.json"));
    // same bad routing: no second log
    env.update(|mut s| {
        s.desired_enabled = true;
        s
    });
    sleep(0.2).await;
    assert_eq!(rejected().len(), 1);
    env.update(|mut s| {
        s.routing = None;
        s
    }); // valid again
    wait_until(|| env.svc.status_dict()["routing_error"].is_null()).await;
    let b = bad.clone();
    env.update(move |mut s| {
        s.routing = Some(b);
        s
    }); // the same bad value is reported again
    wait_until(|| rejected().len() == 2).await;
    // a chat command may replace the reported bad routing
    env.op("!relay target 779", 2).await;
    assert_eq!(env.load().routing.unwrap().target_user_id, 779);
    wait_until(|| env.svc.status_dict()["routing_error"].is_null()).await;
    env.shutdown(task).await;
});

service_test!(
    test_chat_routing_write_rejects_concurrent_hand_edit,
    async {
        let env = env(Setup::default());
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        let hand = routing(778, env.chan, &[]);
        let h = hand.clone();
        env.update(move |mut s| {
            s.routing = Some(h);
            s
        }); // not yet polled
        env.op("!relay target 779", 1).await;
        assert_eq!(env.load().routing, Some(hand));
        assert_ne!(env.svc.with_sup(|s| s.routing().target_user_id), 779);
        wait_until(|| !a.replies().is_empty()).await;
        let t = &a.texts()[0];
        assert!(t.starts_with("⚠️") && t.contains("state.json vừa thay đổi, thử lại lệnh"));
        wait_until(|| env.svc.with_sup(|s| s.routing().target_user_id) == 778).await;
        env.op("!relay target 779", 2).await; // retried after the poll: applies
        assert_eq!(env.load().routing.unwrap().target_user_id, 779);
        env.shutdown(task).await;
    }
);

service_test!(test_chat_op_commands_owner_only_persist_and_poll, async {
    let env = env(Setup::default());
    let (helper, guest) = (51, 52);
    let task = env.reach_relaying().await;
    let a = env.adapter(0);
    env.chat(&format!("!relay op add {helper}"), 1, None, guest)
        .await; // guests: silently ignored
    env.chat("!relay op list", 2, None, guest).await;
    env.chat("!relay prejoin off", 3, None, helper).await; // not yet an operator
    sleep(0.1).await;
    assert!(a.replies().is_empty());
    assert!(env.load().chat_operator_ids.is_empty());
    assert!(env.svc.with_sup(|s| s.prejoin_voice()));
    env.op(&format!("!relay op add {helper}"), 4).await;
    assert_eq!(env.load().chat_operator_ids, [helper]);
    // chat-added operator: operator commands
    env.chat("!relay prejoin off", 5, None, helper).await;
    assert!(!env.svc.with_sup(|s| s.prejoin_voice()));
    let rev = env.load().revision;
    // but never op add/remove
    env.chat(&format!("!relay op add {guest}"), 6, None, helper)
        .await;
    env.chat(&format!("!relay op remove {helper}"), 7, None, helper)
        .await;
    assert_eq!(env.load().revision, rev);
    env.chat("!relay op list", 8, None, helper).await;
    env.op(&format!("!relay op remove {OPERATOR}"), 9).await; // config-defined: not removable
    env.op(&format!("!relay op add {RELAY_ID}"), 11).await;
    env.op("!relay op add <@52>", 12).await;
    assert_eq!(env.load().revision, rev);
    wait_until(|| a.replies().len() == 8).await;
    let t = a.texts();
    assert!(t[0].starts_with('✅') && t[1].starts_with('✅'));
    assert!(t[2].starts_with("⚠️") && t[2].contains("chủ sở hữu") && t[3] == t[2]);
    assert!(t[4].starts_with('👥') && t[4].contains(&helper.to_string()));
    assert!(t[5].starts_with("⚠️") && t[5].contains("sửa config để gỡ"));
    assert!(t[6].starts_with("⚠️") && t[6].contains("bot"));
    assert!(t[7].starts_with('❌'));
    assert!(t.iter().all(|x| !x.contains("<@")));
    // hand edit: picked up by the poll
    env.update(move |mut s| {
        s.chat_operator_ids = vec![guest];
        s
    });
    let ctl = env.cfg.control.clone();
    wait_until(|| ctl.is_operator(guest, &BTreeSet::new(), &env.svc.chat_ops())).await;
    assert!(!ctl.is_operator(helper, &BTreeSet::new(), &env.svc.chat_ops()));
    env.op(&format!("!relay op remove {guest}"), 13).await;
    assert!(env.load().chat_operator_ids.is_empty());
    assert_eq!(env.shutdown(task).await, 0);
    let ops: Vec<Value> = env
        .logged("chat_command")
        .into_iter()
        .filter(|kw| kw["command"] == json!("OpAdd") || kw["command"] == json!("OpRemove"))
        .collect();
    assert!(ops.contains(
        &json!({"command": "OpAdd", "author": OPERATOR, "channel": CMD_CHAN,
                                 "user": helper, "outcome": "applied"})
    ));
    assert!(ops.contains(
        &json!({"command": "OpAdd", "author": helper, "channel": CMD_CHAN,
                                 "user": guest, "outcome": "denied"})
    ));
    assert!(
        env.logs
            .lock()
            .iter()
            .all(|(_, kw)| kw.get("text").is_none())
    );
});

service_test!(test_chat_operators_survive_restart, async {
    let env = env(Setup {
        state: state(|mut s| {
            s.chat_operator_ids = vec![51];
            s
        }),
        ..Setup::default()
    });
    assert_eq!(env.svc.chat_ops(), BTreeSet::from([51]));
});

service_test!(
    test_op_add_respects_the_global_change_cooldown_op_list_does_not,
    async {
        let env = env(Setup::default());
        let (_clk, clock) = manual_clock();
        env.svc.set_chat_cooldowns(5.0, 3.0, clock);
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        env.op("!relay op add 51", 1).await;
        env.op("!relay op add 52", 2).await;
        env.op("!relay op list", 3).await;
        wait_until(|| a.replies().len() == 3).await;
        assert_eq!(env.load().chat_operator_ids, [51]);
        let t = a.texts();
        assert!(t[0].starts_with('✅') && t[1].starts_with('⏳') && t[2].starts_with('👥'));
        env.shutdown(task).await;
    }
);

service_test!(test_reply_without_adapter_is_logged, async {
    let env = env(Setup::default());
    env.svc.dispatch(AdapterEvent::Chat(ChatCommand {
        author_id: OPERATOR,
        channel_id: CMD_CHAN,
        message_id: 1,
        text: "!relay status".into(),
        author_role_ids: BTreeSet::new(),
        author_voice_channel_id: None,
    }));
    assert!(
        env.logged("chat_reply_failed")
            .contains(&json!({"channel": CMD_CHAN, "error": "no adapter"}))
    );
});

// ------------------------------------------------------- relay autoscale / mirror pool

/// Python `_pool_env`: the given accounts, fast retries, autoscale as configured.
fn pool_setup(
    accounts: Vec<OutputAccount>,
    edit: impl FnOnce(&mut RelayConfig) + 'static,
) -> Setup {
    Setup {
        accounts,
        cfg: cfg(edit),
        ..Setup::default()
    }
}

fn relay(uid: u64) -> OutputAccount {
    account(uid, AccountRole::Relay, None)
}

service_test!(test_participants_drive_relay_count, async {
    let (r1, r2) = (424242, 424243);
    // cap 1 with ingest publishing: the target stream holds 0 extra viewers, the ingest Go Live 1,
    // each relay 1
    let env = env(pool_setup(vec![relay(r1), relay(r2)], |c| {
        c.runtime.stream_viewer_cap = 1
    }));
    let bots = [env.target, RELAY_ID, r1, r2];
    let task = env.reach_relaying().await;
    sleep(0.2).await;
    assert_eq!(env.wanted(), 0); // only the target is in the channel
    assert!(!env.published(r1) && !env.published(r2));
    let mut p: HashSet<u64> = bots.into_iter().collect();
    p.extend(&VIEWERS[..2]);
    env.emit(Event::ParticipantsChanged {
        participants: Some(p.clone()),
    })
    .await; // N = 2 -> 1 relay
    wait_until(|| env.published(r1)).await;
    assert_eq!(env.wanted(), 1);
    assert!(!env.published(r2));
    p.extend(VIEWERS);
    env.emit(Event::ParticipantsChanged {
        participants: Some(p),
    })
    .await; // N = 3 -> 2 relays at once
    wait_until(|| env.wanted() == 2).await;
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(
    test_extra_relay_stays_while_watched_then_stops_and_leaves,
    async {
        let r1 = 424242;
        let env = env(pool_setup(vec![relay(r1)], |c| {
            c.runtime.stream_viewer_cap = 1;
            c.runtime.relay_idle_stop_seconds = 0.4;
        }));
        let task = env.reach_relaying().await;
        env.emit(Event::ParticipantsChanged {
            participants: Some(HashSet::from([env.target, VIEWERS[0], VIEWERS[1]])),
        })
        .await;
        wait_until(|| env.live(r1)).await;
        let out = env.out(r1).unwrap();
        out.emit(PoolEvent::OutViewers {
            account: r1,
            viewers: HashSet::from([VIEWERS[0], env.target]),
        });
        // last event: only ticks follow
        env.emit(Event::ParticipantsChanged {
            participants: Some(HashSet::from([env.target])),
        })
        .await;
        assert_eq!(env.wanted(), 0); // lowered at once
        let st = env.svc.status_dict();
        assert_eq!(st["details"]["relay_kept"], json!(1));
        assert_eq!(st["details"]["relay_kept_ids"], json!([r1]));
        let row = st["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["account"] == json!(r1))
            .unwrap()
            .clone();
        assert_eq!(row["viewers"], json!(1)); // the target is not counted
        sleep(0.8).await; // well past the idle time: someone still watches
        assert!(env.live(r1) && !env.order().contains(&"out_end".into()));
        out.emit(PoolEvent::OutViewers {
            account: r1,
            viewers: HashSet::new(),
        });
        env.svc.flush().await;
        assert_eq!(env.svc.status_dict()["details"]["relay_kept"], json!(0));
        wait_until(|| env.order().contains(&"out_end".into())).await;
        assert_eq!(env.running(), 0);
        // the main session keeps running
        assert_eq!(env.state(), RelayState::Relaying);
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(
    test_chat_autoscale_toggle_persists_and_applies_live,
    async {
        let (r1, r2) = (424242, 424243);
        // idle stop > start stagger / 4
        let env = env(pool_setup(vec![relay(r1), relay(r2)], |c| {
            c.runtime.stream_viewer_cap = 1;
            c.runtime.relay_idle_stop_seconds = 1.0;
        }));
        let task = env.reach_relaying().await;
        let a = env.adapter(0);
        sleep(0.1).await;
        assert_eq!(env.wanted(), 0); // config: autoscale, nobody watching
        env.chat("!relay autoscale off", 1, None, 51).await; // operators only: ignored
        sleep(0.1).await;
        assert!(a.replies().is_empty() && env.load().relay_autoscale.is_none());
        env.op("!relay autoscale off", 2).await;
        assert_eq!(env.load().relay_autoscale, Some(false));
        assert_eq!(env.wanted(), 2);
        wait_until(|| env.published(r1) && env.published(r2)).await;
        env.op("!relay status", 3).await;
        env.op("!relay autoscale on", 4).await;
        assert_eq!(env.load().relay_autoscale, Some(true));
        assert_eq!(env.wanted(), 0);
        // no instant mass stop: without viewer data both stay for four idle periods after going live
        assert_eq!(env.running(), 2);
        wait_until(|| env.running() == 0).await;
        assert_eq!(env.state(), RelayState::Relaying);
        wait_until(|| a.replies().len() == 3).await;
        let t = a.texts();
        assert!(t[0].starts_with('✅') && t[1].contains("autoscale tắt") && t[2].starts_with('✅'));
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(
    test_repeated_autoscale_on_keeps_the_idle_stop_countdown,
    async {
        let r1 = 424242;
        let env = env(pool_setup(vec![relay(r1)], |c| {
            c.runtime.stream_viewer_cap = 1;
            c.runtime.relay_idle_stop_seconds = 1.5;
        }));
        let stop_in = || {
            env.svc
                .status_details(monotonic(), false)
                .accounts
                .iter()
                .find(|a| a.uid == r1)
                .and_then(|a| a.stop_in)
        };
        let task = env.reach_relaying().await;
        env.op("!relay autoscale off", 1).await;
        wait_until(|| env.published(r1)).await;
        env.op("!relay autoscale on", 2).await;
        env.out(r1).unwrap().emit(PoolEvent::OutViewers {
            account: r1,
            viewers: HashSet::new(),
        });
        wait_until(|| stop_in().is_some()).await;
        let first = stop_in().unwrap();
        sleep(0.3).await;
        env.op("!relay autoscale on", 3).await;
        assert!(stop_in().unwrap() < first - 0.2);
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(test_persisted_autoscale_overrides_config, async {
    let (r1, r2) = (424242, 424243);
    let env = env(Setup {
        state: state(|mut s| {
            s.relay_autoscale = Some(false);
            s
        }),
        ..pool_setup(vec![relay(r1), relay(r2)], |c| {
            c.runtime.stream_viewer_cap = 1
        })
    });
    assert!(env.cfg.runtime.relay_autoscale);
    assert_eq!(env.wanted(), 2);
    assert_eq!(env.svc.status_dict()["details"]["autoscale"], json!(false));
    let task = env.start();
    wait_until(|| env.adapter_started(0)).await;
    // offline edit is picked up
    env.update(|mut s| {
        s.relay_autoscale = Some(true);
        s
    });
    wait_until(|| env.wanted() == 0).await; // nothing running: no delay needed
    assert_eq!(env.svc.status_dict()["details"]["autoscale"], json!(true));
    env.shutdown(task).await;
});

service_test!(test_fixed_mirrors_active_without_persisted_routing, async {
    let (m1, mirror_chan) = (424244, 999);
    let env = env(pool_setup(
        vec![account(m1, AccountRole::Mirror, Some(mirror_chan))],
        |_| {},
    ));
    assert!(env.load().routing.is_none());
    assert_eq!(
        env.svc.with_sup(|s| s.routing().mirrors),
        IndexMap::from([(mirror_chan, m1)])
    );
    let task = env.reach_relaying().await;
    wait_until(|| env.outs.lock().iter().any(|o| o.published_to(mirror_chan))).await;
    assert_eq!(
        env.svc.status_dict()["routing"]["mirrors"],
        json!({"999": m1})
    );
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(
    test_persisted_mirror_with_relay_account_fails_closed,
    async {
        let (r1, m1) = (424242, 424244);
        let chan = base_config(Path::new(".")).discord.allowed_voice_channel_id;
        // a relay account; a fixed mirror in another channel
        for acct in [r1, m1] {
            let r = routing(777, chan, &[("998", acct)]);
            let res = try_env(Setup {
                state: state(move |mut s| {
                    s.routing = Some(r);
                    s
                }),
                ..pool_setup(
                    vec![relay(r1), account(m1, AccountRole::Mirror, Some(999))],
                    |_| {},
                )
            });
            assert!(
                matches!(&res, Err(ServiceError::StateCorrupt(m)) if m.contains("state.json routing")),
                "{acct}"
            );
        }
    }
);

service_test!(test_status_details_from_service, async {
    let (r1, r2, m1, mirror_chan) = (424242, 424243, 424244, 999);
    // mirror first: it logs in first, so no publish waits on a login
    let env = env(pool_setup(
        vec![
            account(m1, AccountRole::Mirror, Some(mirror_chan)),
            relay(r1),
            relay(r2),
        ],
        |c| c.runtime.stream_viewer_cap = 1,
    ));
    let task = env.reach_relaying().await;
    env.emit(Event::ParticipantsChanged {
        participants: Some(HashSet::from([env.target, m1, VIEWERS[0], VIEWERS[1]])),
    })
    .await;
    wait_until(|| env.live(r1)).await;
    wait_until(|| env.live(m1)).await;
    let d = env.svc.status_details(monotonic(), false);
    assert_eq!(d.session_state, "relaying");
    assert!(d.session_uptime_s.is_some());
    assert_eq!(d.viewers, 2);
    assert_eq!(
        (
            d.relay_wanted,
            d.relay_running,
            d.relay_available,
            d.relay_short_by
        ),
        (1, 1, 2, 0)
    );
    assert_eq!(d.relay_kept, 0);
    assert_eq!(d.prejoin, env.svc.with_sup(|s| s.prejoin_voice()));
    assert_eq!(d.debounce_ms, 0);
    let (w, h, fps) = env.cfg.profile_key();
    assert_eq!(d.output_size, format!("{w}x{h}@{fps}"));
    assert_eq!(
        env.svc.status_dict()["details"]["video_mode"],
        json!(env.cfg.codecs.video_mode.as_str())
    );
    assert!(d.last_watch_ms.is_some());
    // a later session that never sent a watch keeps the last measurement
    env.svc.log_unwatched_timing(u64::MAX);
    assert_eq!(
        env.svc.status_details(monotonic(), false).last_watch_ms,
        d.last_watch_ms
    );
    let row = |uid: u64| d.accounts.iter().find(|a| a.uid == uid).unwrap().clone();
    let (a1, a2, am) = (row(r1), row(r2), row(m1));
    assert_eq!(
        (a1.role.as_str(), a1.channel_id, a1.state.as_str()),
        ("relay", Some(env.chan), "live")
    );
    assert!(a1.uptime_s.is_some());
    // no viewer data yet, not an extra relay
    assert_eq!((a1.viewers, a1.stop_in), (None, None));
    assert_eq!(
        (a2.role.as_str(), a2.channel_id, a2.state.as_str()),
        ("relay", Some(env.chan), "off")
    );
    assert!(a2.uptime_s.is_none());
    assert_eq!(
        (am.role.as_str(), am.channel_id, am.state.as_str()),
        ("mirror", Some(mirror_chan), "live")
    );
    env.op("!relay status", 900).await;
    wait_until(|| !env.adapter(0).replies().is_empty()).await;
    let text = &env.adapter(0).texts()[0];
    assert!(text.starts_with("# \u{1f4e1}"), "{text}");
    assert!(
        text.contains("**2** người") && text.contains("Relay · 1/2 chạy"),
        "{text}"
    );
    assert!(text.contains("<#999>"), "{text}");
    assert_eq!(env.shutdown(task).await, 0);
});

// Python parametrizes passthrough and transcode; the Rust publisher is Opus-only, so only the
// passthrough case remains: one publisher per output account.
service_test!(test_audio_mode_selects_opus_publishers, async {
    let env = env(Setup {
        accounts: vec![relay(1), relay(2)],
        ..Setup::default()
    });
    let ids: HashSet<u64> = env.svc.out_publisher_ids().into_iter().collect();
    assert_eq!(ids, HashSet::from([1, 2]));
});

// ------------------------------------------------------------------ adaptive codec

fn adaptive(setup: Setup) -> Setup {
    Setup {
        adaptive: true,
        ..setup
    }
}

service_test!(test_service_routes_op14_to_controller_and_swaps, async {
    let env = env(adaptive(Setup::default()));
    let task = env.reach_relaying().await;
    let ad = env.adapter(0);
    wait_until(|| {
        ad.has("create_output_adaptive") && env.svc.with_codec(|c| c.effective() == Some("H265"))
    })
    .await;
    assert_eq!(env.svc.publisher().codec(), "H265");
    assert_eq!(
        env.svc.status_details(monotonic(), false).codec.as_deref(),
        Some("H265")
    );
    *ad.negotiated.lock() = "H264".into();
    // a weak viewer joined the ingest output
    env.emit(Event::CodecChanged {
        generation: env.svc.current_ids().0,
        negotiated_video_codec: Some("H264".into()),
    })
    .await;
    wait_until(|| {
        ad.watch_lists
            .lock()
            .last()
            .is_some_and(|l| l == &["H264", "VP8", "VP9"])
    })
    .await;
    assert_eq!(
        env.svc.status_details(monotonic(), false).codec_switch,
        Some(("H265".to_string(), "H264".to_string()))
    );
    let (g, e) = env.svc.current_ids();
    env.worker(0).push_event(
        g,
        e,
        WorkerEventKind::SourceCodec,
        json!({"codec": "H264", "width": CANVAS.0, "height": CANVAS.1, "eligible": true}),
    );
    wait_until(|| ad.swaps.lock().last().is_some_and(|c| c == "H264")).await;
    assert_eq!(env.svc.publisher().codec(), "H264");
    assert_eq!(
        env.svc.with_codec(|c| c.effective().map(str::to_owned)),
        Some("H264".into())
    );
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(
    test_adaptive_play_uses_codec_negotiated_after_create,
    async {
        let env = env(adaptive(Setup::default()));
        let task = env.start();
        wait_until(|| env.adapter_started(0)).await;
        let ad = env.adapter(0);
        let a2 = Arc::clone(&ad);
        *ad.create_adaptive_hook.lock() = Some(Arc::new(move || {
            a2.push("create_output_adaptive");
            // op 14 lands while the slate is still building
            *a2.negotiated.lock() = "H264".into();
            Box::pin(async { Ok(Some("AV1".to_string())) })
        }));
        ad.emit(env.snapshot());
        wait_until(|| ad.has("watch")).await;
        ad.emit(Event::SourceProgress {
            generation: env.generation(),
            at: monotonic(),
            video: true,
        });
        wait_until(|| env.state() == RelayState::Relaying).await;
        wait_until(|| ad.has("play")).await;
        // `play` takes no codec in Rust: the publisher it plays is set to the codec negotiated now
        assert_eq!(env.svc.publisher().codec(), "H264");
        assert_eq!(env.shutdown(task).await, 0);
    }
);

service_test!(test_service_hold_debounced, async {
    let env = env(adaptive(Setup::default()));
    let task = env.reach_relaying().await;
    wait_until(|| env.svc.with_codec(|c| c.effective() == Some("H265"))).await;
    // The service's own ticker re-evaluates the hold on the real clock, so each check is a tick at a
    // chosen time whose result is read under the same lock (the hold starts within [held_at, now]).
    let held_at = monotonic();
    env.svc.execute(vec![Action::ShowSlate {
        generation: env.svc.current_ids().0,
        reason: HoldReason::NoMedia,
    }]);
    // a short stall changes nothing: at most 1 s of hold by then
    assert_eq!(
        env.svc.codec_event_hook(CodecEvent::Tick, held_at + 1.0),
        Some("H265".into())
    );
    // Python moves `held_since` back 3 s; the same as a tick 3 s later
    assert_eq!(
        env.svc
            .codec_event_hook(CodecEvent::Tick, monotonic() + 3.0),
        Some("H264".into())
    );
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(test_ingest_not_registered_without_ingest_publish, async {
    let env = env(adaptive(Setup {
        ingest_publishes: false,
        ..Setup::default()
    }));
    let task = env.reach_relaying().await;
    wait_until(|| env.svc.with_codec(|c| c.effective() == Some("H265"))).await;
    assert!(
        env.svc
            .with_codec(|c| c.reported(OutputId::Ingest).is_none())
    );
    assert!(!env.adapter(0).has("create_output_adaptive"));
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(test_stale_attempt_codec_event_dropped, async {
    let uid = 424242;
    let env = env(adaptive(Setup {
        out_codec: "H265",
        ..multi(uid, AccountRole::Relay)
    }));
    let task = env.reach_relaying().await;
    let id = OutputId::Account(uid);
    wait_until(|| env.svc.with_codec(|c| c.reported(id).is_some())).await;
    let (g, n) = (
        env.svc.current_ids().0,
        env.out(uid).unwrap().attempt.load(Ordering::SeqCst),
    );
    let changed = |attempt| PoolEvent::OutCodecChanged {
        account: uid,
        generation: g,
        attempt,
        codec: "H264".into(),
    };
    env.svc.pool_event_hook(changed(n + 1)); // an old attempt
    assert_eq!(
        env.svc.with_codec(|c| c.reported(id).map(str::to_owned)),
        Some("H265".into())
    );
    env.svc.pool_event_hook(changed(n));
    assert_eq!(
        env.svc.with_codec(|c| c.reported(id).map(str::to_owned)),
        Some("H264".into())
    );
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(test_stale_attempt_failure_keeps_codec_output, async {
    let uid = 424242;
    let env = env(adaptive(Setup {
        out_codec: "H265",
        ..multi(uid, AccountRole::Relay)
    }));
    let task = env.reach_relaying().await;
    let id = OutputId::Account(uid);
    wait_until(|| env.svc.with_codec(|c| c.reported(id).is_some())).await;
    let (g, n) = (
        env.svc.current_ids().0,
        env.out(uid).unwrap().attempt.load(Ordering::SeqCst),
    );
    env.svc.pool_event_hook(PoolEvent::OutFailed {
        account: uid,
        generation: g,
        attempt: n + 1,
        reason: StopReason::ConnectFailed,
        retryable: true,
        detail: "stale".into(),
    });
    assert!(env.svc.with_codec(|c| c.reported(id).is_some()));
    assert_eq!(env.shutdown(task).await, 0);
});

service_test!(test_codec_stall_watchdog_reswaps_silent_output, async {
    use relayctl::service::CODEC_STALL_S;

    let env = env(adaptive(Setup::default()));
    let task = env.reach_relaying().await;
    let ad = env.adapter(0);
    wait_until(|| {
        env.svc
            .with_codec(|c| c.bound_outputs() == [OutputId::Ingest])
    })
    .await;
    // started with the negotiated codec's source: no redundant swap
    assert!(ad.swaps.lock().is_empty());
    let now = monotonic();
    env.svc.set_last_egress_at(now);
    env.svc.clear_stall_kick();
    env.svc.check_codec_stall(now); // first sight starts the grace period
    assert_eq!(
        env.svc.with_codec(|c| c.bound_outputs()),
        [OutputId::Ingest]
    );
    // the fake adapter's `play` never pulls, so the publisher has sent nothing
    assert!(env.svc.publisher().last_send_at().is_none());
    env.svc.set_last_egress_at(now + CODEC_STALL_S); // the source is still flowing
    env.svc.check_codec_stall(now + CODEC_STALL_S + 0.1);
    // unbound: the controller swaps it again
    assert!(env.svc.with_codec(|c| c.bound_outputs().is_empty()));
    assert_eq!(env.shutdown(task).await, 0);
});
