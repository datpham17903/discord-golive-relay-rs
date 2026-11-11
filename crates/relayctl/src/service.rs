//! Service wiring: Supervisor actions -> adapter, worker, publisher (spec §7, 9, 13, 16). Port of
//! `service.py`.
//!
//! Everything that decides lives in the Supervisor and the output pool; this module only executes
//! their actions in order. Concurrency (replacing the single asyncio loop of Python):
//!
//! - All decision state is in [`Core`], behind one `parking_lot` mutex that is never held across an
//!   `.await`. Every Python method that ran on the loop thread is a method taking `&mut Core`, so
//!   nested calls (`_handle` inside `_execute`, ...) stay plain calls on the held lock.
//! - The Supervisor and the output pool have their own mutexes because the publishers' gates read
//!   them from the voice sender thread while holding the publisher's lock. Lock order: `core` ->
//!   `sup` / `pool` / publisher -> `hot`; `sup` and `pool` are only held for one call and never while
//!   calling a publisher.
//! - Adapter callbacks (gateway task, UDP dispatcher thread) never touch `core`: events go through an
//!   unbounded channel to one events task (Python `call_soon_threadsafe`), ingress goes straight to
//!   the worker under the small `hot` lock (worker, generation, epoch, feedback IDR rate limit), which
//!   is also all the publisher's `on_need_idr` callback needs.
//! - The worker's blocking calls (spawn, `stop`) and the slate build run in `spawn_blocking`; its
//!   non-blocking `send`/`poll_*` are called directly. The ops queue, the per-account output queues
//!   and the reply queue are serial tasks fed by channels.
//!
//! Clocks: the Supervisor runs on [`wall_clock`] (its `now`, gate checks, session timing);
//! `SourceProgress.at` (monotonic, from the adapter) is moved onto that clock in [`Service::handle`].
//! The worker, the publishers, the pool, the codec controller, chat cooldowns and the service's own
//! intervals use [`monotonic`].

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use indexmap::IndexMap;
use parking_lot::Mutex;
use relay_core::clock::{monotonic, wall_clock};
use relay_core::config::{COMMAND_NAMES, OutputAccount, OutputCodec, RelayConfig};
use relay_core::control::commands::{
    AccountStatus, ChatCommand, Command, OperatorContext, ParseError, RoutingContext,
    StatusDetails, TechRow, command_name, describe_change, describe_op, format_operators,
    format_status, parse, plan_change, plan_op, resolve_here,
};
use relay_core::control::cooldown::ChatCooldowns;
use relay_core::control::lock::InstanceLocks;
use relay_core::control::server::{ControlServer, Handler, own_uid};
use relay_core::control::state::{
    JsonObject, PersistedRouting, PersistedState, StateError, StateStore, set_account,
};
use relay_core::obs::{JsonLogger, Metrics, write_status};
use relay_core::publisher::{Publisher, PublisherSource, SendSnapshot, freezes_since, rates};
use relay_core::supervisor::codec::{CodecAction, CodecController, CodecEvent, OutputId};
use relay_core::supervisor::machine::{PersistedStateView, Supervisor, SupervisorConfig};
use relay_core::supervisor::model::{
    Action, Event, FatalReason, GatewayState, KeyframeMethod, QuarantineRecord, RelayState,
    SourceStream, StatusSnapshot, StopReason,
};
use relay_core::supervisor::outputs::{OutputPool, PoolConfig};
use relay_core::supervisor::outputs_model::{
    AccountPersisted, OutputState, PoolAction, PoolEvent, account_view,
};
use relay_core::supervisor::retry::RetryPolicy;
use relay_core::supervisor::routing::Routing;
use relay_core::supervisor::scaling::relays_needed;
use relay_core::worker::messages::{
    Configure, Egress, ForceIdr, Ingress, OutputProfile, ResetPipeline, SetOutput, WorkerEvent,
    WorkerEventKind,
};
use relay_core::worker::process::WorkerHandle;
use relay_discord::{
    Adapter, AdapterError, AdapterEvent, AdapterOptions, DiscordAdapter, EventSink, IngressSink,
    LogFn, OutputAccountAdapter, OutputAdapter, OutputOptions, OutputSource, PoolEventSink,
    SourceFactory, Token, VoiceStateCallback,
};
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;
use relay_media::slate::{SlateGop, build_slate};
use relay_voice::VoiceError;
use relay_voice::publish::VideoParams;
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::metrics::{self, Endpoints, HEALTH_MAX_AGE_S};
use crate::tokens::load_token;

pub const TICK_S: f64 = 0.1;
pub const PUMP_S: f64 = 0.01;
pub const STATE_POLL_S: f64 = 0.25;
pub const STATUS_S: f64 = 1.0;
/// `status -v` send-counter sampling.
pub const SEND_SAMPLE_S: f64 = 1.0;
/// ~6 min of samples.
pub const SEND_HISTORY: usize = 400;
pub const RATE_WINDOW_S: f64 = 10.0;
pub const FREEZE_WINDOW_S: f64 = 300.0;
pub const DRAIN_TIMEOUT_S: f64 = 15.0;
pub use relay_core::ingress::{OVERFLOW_MIN_INTERVAL_S, PLI_MIN_INTERVAL_S};
pub const LOGIN_STAGGER_S: f64 = 2.0;
pub const OUT_LOGIN_WAIT_S: f64 = 30.0;
pub const SLATE_CACHE_SIZE: usize = 4;
/// Adaptive: a bound output silent this long while the source flows is re-swapped.
pub const CODEC_STALL_S: f64 = 5.0;
/// > `CONNECT_TIMEOUT_S`: the library may still recover a 4014 or resume a 4015.
pub const VOICE_LOST_GRACE_S: f64 = 20.0;
/// At most one adapter restart for repeated voice close 4006 per 5 minutes.
pub const VOICE_4006_RESTART_MIN_S: f64 = 300.0;
/// `publisher.py` default `max_late_s`.
pub const MAX_LATE_S: f64 = 0.2;
const MAX_EGRESS_POLL: usize = 16;
/// session_timing milestones also reported relative to stream_seen.
const FROM_SEEN: [&str; 4] = [
    "voice_state_confirmed",
    "watch_sent",
    "watching",
    "voice_connected",
];

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

// ------------------------------------------------------------------ pure helpers

/// The Supervisor configuration of `cfg`; `t_dead` is required to run.
pub fn supervisor_config(cfg: &RelayConfig) -> Result<SupervisorConfig, String> {
    let t_dead = cfg
        .liveness
        .source_dead_timeout_seconds
        .ok_or("liveness.source_dead_timeout_seconds is required to run")?;
    let r = &cfg.runtime;
    let d = &cfg.discord;
    let mut s = SupervisorConfig::new(
        d.target_user_id,
        d.guild_id,
        d.allowed_voice_channel_id,
        t_dead,
    );
    s.excluded_viewer_ids = cfg
        .authorization
        .excluded_viewer_ids
        .iter()
        .copied()
        .collect();
    s.debounce = r.start_debounce_ms as f64 / 1000.0;
    s.hold_timeout = r.hold_recovery_timeout_seconds;
    s.idle_grace = r.idle_voice_grace_seconds;
    s.connect_timeout = r.connect_timeout_seconds;
    s.first_media_timeout = r.first_media_timeout_seconds;
    s.retry_delays = r.retry_delays_seconds.clone();
    s.retry_cooldown = r.retry_cooldown_seconds;
    s.rejection_limit = u32::try_from(r.repeated_rejection_limit).unwrap_or(u32::MAX);
    s.prejoin_voice = r.prejoin_voice;
    s.adaptive = cfg.codecs.output_codec == OutputCodec::Adaptive;
    Ok(s)
}

/// Mirror accounts with a fixed channel_id start mirrored there while state.json has no routing.
pub fn config_routing(cfg: &RelayConfig) -> Routing {
    let mirrors = cfg
        .discord
        .mirror_accounts()
        .iter()
        .filter_map(|a| a.channel_id.map(|ch| (ch, a.user_id)))
        .collect();
    Routing {
        target_user_id: cfg.discord.target_user_id,
        channel_id: cfg.discord.allowed_voice_channel_id,
        mirrors,
    }
}

/// Checks a routing that did not come from a chat command (state.json); channels are not checked
/// offline.
pub fn routing_error(r: &Routing, cfg: &RelayConfig) -> Option<String> {
    let mut relays: HashSet<u64> = cfg.output_accounts().iter().map(|a| a.user_id).collect();
    relays.insert(cfg.discord.expected_relay_user_id);
    if relays.contains(&r.target_user_id) {
        return Some(format!(
            "target_user_id {} is a relay account (ingest or output)",
            r.target_user_id
        ));
    }
    let fixed: HashMap<u64, Option<u64>> = cfg
        .discord
        .mirror_accounts()
        .iter()
        .map(|a| (a.user_id, a.channel_id))
        .collect();
    for (ch, acct) in &r.mirrors {
        let Some(fixed_ch) = fixed.get(acct) else {
            return Some(format!(
                "mirror {ch} uses {acct}, which is not a configured mirror account"
            ));
        };
        if let Some(f) = fixed_ch
            && f != ch
        {
            return Some(format!(
                "mirror {ch} uses {acct}, which is fixed to channel {f}"
            ));
        }
        if *ch == r.channel_id {
            return Some(format!("mirror {ch} is the main channel"));
        }
    }
    None
}

/// What the Supervisor needs from state.json; an unknown latch reason stays fatal.
pub fn persisted_view(st: &PersistedState) -> PersistedStateView {
    let quarantine = st.quarantine.as_ref().map(|q| QuarantineRecord {
        owner_id: q.get("owner_id").and_then(Value::as_u64).unwrap_or(0),
        stream_key: q
            .get("stream_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        incarnation: q.get("incarnation").and_then(Value::as_i64).unwrap_or(0),
        since: q.get("since").and_then(Value::as_f64).unwrap_or(0.0),
    });
    PersistedStateView {
        desired_enabled: st.desired_enabled,
        fatal_reason: FatalReason::from_latch(st.fatal_latch.as_ref()),
        quarantine,
        consecutive_rejections: u32::try_from(st.consecutive_rejections.max(0)).unwrap_or(u32::MAX),
    }
}

/// Fast watch retry only for a slot race: a stream_full refusal or a watch timeout. Any other
/// refusal (unauthorized, invalid_channel, ...) or error keeps normal backoff.
pub fn watch_retryable(exc: &AdapterError) -> bool {
    matches!(exc, AdapterError::Voice(VoiceError::Timeout(_)))
        || exc.to_string().contains("stream_full")
}

fn quarantine_json(rec: &QuarantineRecord) -> JsonObject {
    let mut m = Map::new();
    m.insert("owner_id".into(), json!(rec.owner_id));
    m.insert("stream_key".into(), json!(rec.stream_key));
    m.insert("incarnation".into(), json!(rec.incarnation));
    m.insert("since".into(), json!(rec.since));
    m
}

fn latch(reason: FatalReason, detail: &str) -> JsonObject {
    let mut m = Map::new();
    m.insert("reason".into(), json!(reason.as_str()));
    m.insert("detail".into(), json!(detail));
    m.insert("at".into(), json!(wall_clock()));
    m
}

fn routing_json(r: &PersistedRouting) -> Value {
    let mirrors: Map<String, Value> = r
        .mirrors
        .iter()
        .map(|(ch, acct)| (ch.clone(), json!(acct)))
        .collect();
    json!({"target_user_id": r.target_user_id, "channel_id": r.channel_id, "mirrors": mirrors})
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

fn ms(seconds: f64) -> i64 {
    (seconds * 1000.0).round() as i64
}

/// A monotonic timestamp on the Supervisor's wall clock.
fn mono_to_wall(at: f64) -> f64 {
    at + (wall_clock() - monotonic())
}

fn output_id_str(oid: OutputId) -> String {
    match oid {
        OutputId::Ingest => "ingest".to_string(),
        OutputId::Account(uid) => uid.to_string(),
    }
}

fn same_worker(a: Option<&Arc<dyn Worker>>, b: Option<&Arc<dyn Worker>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

/// Python `type(event).__name__` for the `event.*` counters.
fn event_name(e: &Event) -> &'static str {
    match e {
        Event::Tick => "Tick",
        Event::EnableChanged { .. } => "EnableChanged",
        Event::RoutingChanged { .. } => "RoutingChanged",
        Event::PrejoinChanged { .. } => "PrejoinChanged",
        Event::PrejoinFailed { .. } => "PrejoinFailed",
        Event::VoiceLost { .. } => "VoiceLost",
        Event::ResetFatal => "ResetFatal",
        Event::GatewayChanged { .. } => "GatewayChanged",
        Event::GatewaySnapshot { .. } => "GatewaySnapshot",
        Event::TargetVoiceChanged { .. } => "TargetVoiceChanged",
        Event::StreamStarted { .. } => "StreamStarted",
        Event::StreamUpdated { .. } => "StreamUpdated",
        Event::StreamEnded { .. } => "StreamEnded",
        Event::ParticipantsChanged { .. } => "ParticipantsChanged",
        Event::RelayVoiceObserved(_) => "RelayVoiceObserved",
        Event::Joined { .. } => "Joined",
        Event::Watching { .. } => "Watching",
        Event::FirstFrame { .. } => "FirstFrame",
        Event::OutputReady { .. } => "OutputReady",
        Event::ConnectFailed { .. } => "ConnectFailed",
        Event::InboundHealth { .. } => "InboundHealth",
        Event::OutputHealth { .. } => "OutputHealth",
        Event::SourceProgress { .. } => "SourceProgress",
        Event::FreshFrame { .. } => "FreshFrame",
        Event::WorkerFailed { .. } => "WorkerFailed",
        Event::CodecChanged { .. } => "CodecChanged",
        Event::AuthRejected => "AuthRejected",
        Event::ConfigInvalid { .. } => "ConfigInvalid",
        Event::Shutdown => "Shutdown",
    }
}

fn action_name(a: &Action) -> &'static str {
    match a {
        Action::CloseGate { .. } => "CloseGate",
        Action::OpenGate { .. } => "OpenGate",
        Action::ShowSlate { .. } => "ShowSlate",
        Action::StartConnect { .. } => "StartConnect",
        Action::CreateOutput { .. } => "CreateOutput",
        Action::RebuildPipeline { .. } => "RebuildPipeline",
        Action::RequestInputKeyframe { .. } => "RequestInputKeyframe",
        Action::Teardown { .. } => "Teardown",
        Action::LeaveVoice { .. } => "LeaveVoice",
        Action::JoinVoice => "JoinVoice",
        Action::SetFatalLatch { .. } => "SetFatalLatch",
        Action::ClearFatalLatch { .. } => "ClearFatalLatch",
        Action::SetQuarantine { .. } => "SetQuarantine",
        Action::SetRejections { .. } => "SetRejections",
        Action::Exit { .. } => "Exit",
        Action::ApplyRouting { .. } => "ApplyRouting",
    }
}

fn pool_action_name(a: &PoolAction) -> &'static str {
    match a {
        PoolAction::Start { .. } => "OutStart",
        PoolAction::SetLive { .. } => "OutSetLive",
        PoolAction::Slate { .. } => "OutSlate",
        PoolAction::CloseGate { .. } => "OutCloseGate",
        PoolAction::Teardown { .. } => "OutTeardown",
        PoolAction::SetFatal { .. } => "OutSetFatal",
        PoolAction::SetQuarantine { .. } => "OutSetQuarantine",
        PoolAction::SetRejections { .. } => "OutSetRejections",
    }
}

/// Python `type(cmd).__name__` for the `chat_command` log.
fn command_type_name(cmd: &Result<Command, ParseError>) -> &'static str {
    match cmd {
        Err(_) => "ParseError",
        Ok(Command::Status { .. }) => "Status",
        Ok(Command::SetEnabled(_)) => "SetEnabled",
        Ok(Command::SetPrejoin(_)) => "SetPrejoin",
        Ok(Command::SetAutoscale(_)) => "SetAutoscale",
        Ok(Command::SetTarget(_)) => "SetTarget",
        Ok(Command::SetChannel(_)) => "SetChannel",
        Ok(Command::AddMirror { .. }) => "AddMirror",
        Ok(Command::RemoveMirror { .. }) => "RemoveMirror",
        Ok(Command::OpList) => "OpList",
        Ok(Command::OpAdd(_)) => "OpAdd",
        Ok(Command::OpRemove(_)) => "OpRemove",
    }
}

// ------------------------------------------------------------------ injected parts

/// The passthrough worker as the service drives it (`WorkerHandle` in production, fakes in tests).
pub trait Worker: Send + Sync {
    /// Non-blocking; `false` when the bounded ingress is full.
    fn send(&self, msg: Ingress) -> bool;
    fn poll_egress(&self, max_items: usize) -> Vec<Egress>;
    fn poll_events(&self) -> Vec<WorkerEvent>;
    fn alive(&self) -> bool;
    /// Blocking (up to a few seconds): the service calls it from `spawn_blocking`.
    fn stop(&self);
}

/// The worker thread of `relay-core`.
pub struct ThreadWorker(Mutex<WorkerHandle>);

impl ThreadWorker {
    pub fn new(handle: WorkerHandle) -> Self {
        Self(Mutex::new(handle))
    }
}

impl Worker for ThreadWorker {
    fn send(&self, msg: Ingress) -> bool {
        self.0.lock().send(msg)
    }

    fn poll_egress(&self, max_items: usize) -> Vec<Egress> {
        self.0.lock().poll_egress(max_items)
    }

    fn poll_events(&self) -> Vec<WorkerEvent> {
        self.0.lock().poll_events()
    }

    fn alive(&self) -> bool {
        self.0.lock().alive()
    }

    fn stop(&self) {
        // Only called once the worker is detached from ingress and pump, so holding the lock while
        // it stops blocks nobody.
        self.0.lock().stop();
    }
}

pub type AdapterFactory =
    Box<dyn Fn(EventSink, IngressSink, LogFn) -> Arc<dyn Adapter> + Send + Sync>;
pub type OutputAdapterFactory =
    Box<dyn Fn(&OutputAccount, PoolEventSink, LogFn) -> Arc<dyn OutputAdapter> + Send + Sync>;
/// Called from `spawn_blocking`.
pub type WorkerFactory =
    Arc<dyn Fn(OutputProfile) -> Result<Arc<dyn Worker>, String> + Send + Sync>;
/// Called from `spawn_blocking`.
pub type SlateBuilder = Arc<dyn Fn(&SlateKey) -> Result<Arc<SlateGop>, String> + Send + Sync>;
pub type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;
/// Whether a control-socket peer (its uid, if known) may use operator-only commands.
pub type OperatorCheck = Arc<dyn Fn(Option<u64>) -> bool + Send + Sync>;

/// Every `build_slate` input; built slates are reused by later sessions on the same canvas.
#[derive(Debug, Clone, Copy)]
pub struct SlateKey {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub keyframe_interval_s: f64,
    pub bitrate_kbps: u32,
}

impl PartialEq for SlateKey {
    fn eq(&self, other: &Self) -> bool {
        (self.width, self.height, self.fps, self.bitrate_kbps)
            == (other.width, other.height, other.fps, other.bitrate_kbps)
            && self.keyframe_interval_s.to_bits() == other.keyframe_interval_s.to_bits()
    }
}

impl Eq for SlateKey {}

impl Hash for SlateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.width, self.height, self.fps, self.bitrate_kbps).hash(state);
        self.keyframe_interval_s.to_bits().hash(state);
    }
}

pub fn default_slate_builder() -> SlateBuilder {
    Arc::new(|k: &SlateKey| {
        build_slate(
            k.width,
            k.height,
            k.fps,
            k.keyframe_interval_s,
            k.bitrate_kbps,
        )
        .map(Arc::new)
        .map_err(|e| e.to_string())
    })
}

/// Python `_is_operator`: the peer is root or this process's own user; unknown is refused.
pub fn root_is_operator() -> OperatorCheck {
    let own = own_uid();
    Arc::new(move |uid: Option<u64>| uid.is_some() && (uid == Some(0) || uid == own))
}

/// Keyword arguments of the Python constructor plus the module constants tests patched.
pub struct ServiceOptions {
    pub slate_builder: SlateBuilder,
    pub install_signals: bool,
    pub tick_s: f64,
    pub output_adapter_factory: Option<OutputAdapterFactory>,
    pub output_tokens: HashMap<u64, Token>,
    pub pli_min_interval_s: f64,
    pub voice_lost_grace_s: f64,
    pub peer_is_operator: OperatorCheck,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            slate_builder: default_slate_builder(),
            install_signals: true,
            tick_s: TICK_S,
            output_adapter_factory: None,
            output_tokens: HashMap::new(),
            pli_min_interval_s: PLI_MIN_INTERVAL_S,
            voice_lost_grace_s: VOICE_LOST_GRACE_S,
            peer_is_operator: root_is_operator(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Python `StateCorrupt` raised by the constructor (an invalid persisted routing).
    #[error("{0}")]
    StateCorrupt(String),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("{0}")]
    Config(String),
}

// ------------------------------------------------------------------ task plumbing

/// Counts running tasks (Python's `_out_tasks` / `_slate_tasks` sets).
#[derive(Default)]
struct Tracker {
    count: AtomicUsize,
    idle: Notify,
}

struct TrackGuard(Arc<Tracker>);

impl Drop for TrackGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

fn track(tracker: &Arc<Tracker>) -> TrackGuard {
    tracker.count.fetch_add(1, Ordering::AcqRel);
    TrackGuard(Arc::clone(tracker))
}

/// `true` once nothing is running, `false` after `timeout`.
async fn wait_idle(tracker: &Tracker, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            let idle = tracker.idle.notified();
            if tracker.count.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    })
    .await
    .is_ok()
}

/// Runs pushed futures one at a time, in order (an asyncio queue drained by one task, or Python's
/// chained `prev` tasks). The consumer task starts with the first push.
#[derive(Clone, Default)]
struct SerialQueue(Arc<SerialInner>);

#[derive(Default)]
struct SerialInner {
    tx: Mutex<Option<mpsc::UnboundedSender<BoxFuture<()>>>>,
    task: Mutex<Option<AbortHandle>>,
    pending: Arc<Tracker>,
}

impl SerialQueue {
    fn push(&self, fut: BoxFuture<()>) {
        let guard = track(&self.0.pending);
        let fut: BoxFuture<()> = Box::pin(async move {
            let _guard = guard;
            fut.await;
        });
        let mut tx = self.0.tx.lock();
        let sender = tx.get_or_insert_with(|| {
            let (tx, mut rx) = mpsc::unbounded_channel::<BoxFuture<()>>();
            let task = tokio::spawn(async move {
                while let Some(f) = rx.recv().await {
                    f.await;
                }
            });
            *self.0.task.lock() = Some(task.abort_handle());
            tx
        });
        let _ = sender.send(fut);
    }

    async fn drain(&self, timeout: Duration) -> bool {
        wait_idle(&self.0.pending, timeout).await
    }

    fn abort(&self) {
        if let Some(task) = self.0.task.lock().take() {
            task.abort();
        }
        *self.0.tx.lock() = None;
    }
}

enum Msg {
    Adapter(AdapterEvent),
    Pool(PoolEvent),
    Resnapshot(Arc<dyn Adapter>),
    IngressOverflow(u64, u64),
    Flush(oneshot::Sender<()>),
}

/// What the UDP thread and the publishers' `on_need_idr` read: the current worker and its ids.
struct Hot {
    worker: Option<Arc<dyn Worker>>,
    generation: u64,
    epoch: u64,
    last_idr_req: f64,
}

struct Timing {
    generation: u64,
    start: f64,
    seen: Option<f64>,
    gate_ready: Option<f64>,
    prejoined: bool,
    marks: IndexMap<String, f64>,
    extra: Map<String, Value>,
}

/// `None` = the ingest publisher.
type SendKey = Option<u64>;

/// Everything Python kept on `self` and only touched from the loop thread.
struct Core {
    adapter: Option<Arc<dyn Adapter>>,
    token: Option<Token>,
    codec_ctl: CodecController,
    stall_kick: HashMap<OutputId, f64>,
    last_egress_at: f64,
    /// Source size while it is passthrough-eligible.
    src_dims: Option<(u32, u32)>,
    autoscale: bool,
    /// Added by `op add`; config owners are not in here.
    chat_ops: BTreeSet<u64>,
    /// Gate first opened in the current session (monotonic).
    session_since: Option<f64>,
    send_hist: HashMap<SendKey, VecDeque<(f64, SendSnapshot)>>,
    send_base: HashMap<SendKey, u64>,
    next_send_sample: f64,
    /// Account -> first OutSetLive of its current run (monotonic).
    out_since: HashMap<u64, f64>,
    /// watch_sent_from_seen_ms of the last logged session.
    last_watch_ms: Option<f64>,
    /// Non-silent target audio in the last status window.
    audio_ok: bool,
    out_adapters: HashMap<u64, Arc<dyn OutputAdapter>>,
    out_misconfigured: HashSet<u64>,
    cooldowns: ChatCooldowns,
    chat_clock: Clock,
    /// Routing changed while the gateway was not FRESH.
    resnap_pending: bool,
    profile: Option<OutputProfile>,
    canvas: Option<(u32, u32)>,
    slate: Option<Arc<SlateGop>>,
    slates: IndexMap<SlateKey, Arc<SlateGop>>,
    /// (key, incarnation) last seen by the gate.
    seen_stream: Option<(String, i64)>,
    seen_at: f64,
    /// Start milestones of the current generation.
    timing: Option<Timing>,
    /// Generation of the running session; `None` after its Teardown.
    session_gen: Option<u64>,
    in_voice: bool,
    session_id: Option<String>,
    /// In voice per `in_voice`, but the adapter's voice is not connected.
    voice_dead_since: Option<f64>,
    /// Join failures with voice close 4006 since the last successful join.
    voice_4006: u32,
    last_4006_restart: Option<f64>,
    exit_code: i32,
    shutdown_requested: bool,
    persist_failed: bool,
    last_pli: f64,
    pli_timer: Option<AbortHandle>,
    last_overflow: f64,
    worker_dead_reported: bool,
    metrics: Metrics,
    network_stats: bool,
    rejected_routing: Option<PersistedRouting>,
    /// Why the routing in state.json is being ignored.
    routing_error: Option<String>,
}

pub struct Service {
    me: Weak<Service>,
    cfg: RelayConfig,
    store: StateStore,
    log: LogFn,
    adaptive: bool,
    adapter_factory: AdapterFactory,
    worker_factory: WorkerFactory,
    slate_builder: SlateBuilder,
    out_factory: Option<OutputAdapterFactory>,
    out_tokens: HashMap<u64, Token>,
    install_signals: bool,
    tick_s: f64,
    pli_min_interval: f64,
    voice_lost_grace: f64,
    peer_is_operator: OperatorCheck,
    feedback_min: f64,
    sup: Mutex<Supervisor>,
    pool: Mutex<OutputPool>,
    publisher: Arc<Publisher>,
    out_pubs: IndexMap<u64, Arc<Publisher>>,
    out_logged_in: HashMap<u64, watch::Sender<bool>>,
    hot: Mutex<Hot>,
    core: Mutex<Core>,
    events_tx: mpsc::UnboundedSender<Msg>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<Msg>>>,
    ops: SerialQueue,
    replies: SerialQueue,
    out_queues: Mutex<HashMap<u64, SerialQueue>>,
    tasks: Arc<Tracker>,
    slate_tasks: Arc<Tracker>,
    ingest_lock: tokio::sync::Mutex<()>,
    done: watch::Sender<bool>,
    /// The last status.json written and when (monotonic): what `--metrics-listen` serves.
    status_written: Mutex<Option<(f64, Value)>>,
}

enum ChatFail {
    /// state.json routing changed after the chat command was planned; nothing was written.
    Moved,
    Write(String),
}

impl From<StateError> for ChatFail {
    fn from(e: StateError) -> Self {
        ChatFail::Write(e.to_string())
    }
}

enum Created {
    Stale,
    Ready(Option<String>),
}

impl Service {
    pub fn new(
        cfg: RelayConfig,
        store: StateStore,
        adapter_factory: AdapterFactory,
        worker_factory: WorkerFactory,
        log: LogFn,
        opts: ServiceOptions,
    ) -> Result<Arc<Service>, ServiceError> {
        let st = store.load()?;
        let routing = Routing::from_persisted(st.routing.as_ref(), &config_routing(&cfg));
        if let Some(err) = routing_error(&routing, &cfg) {
            return Err(ServiceError::StateCorrupt(format!(
                "state.json routing is invalid: {err}; fix or remove the routing entry"
            )));
        }
        let mut sup_cfg = supervisor_config(&cfg).map_err(ServiceError::Config)?;
        sup_cfg.target_user_id = routing.target_user_id;
        sup_cfg.allowed_channel_id = routing.channel_id;
        if let Some(prejoin) = st.prejoin_voice {
            // chat override persisted in state.json; config is only the default
            sup_cfg.prejoin_voice = prejoin;
        }
        let adaptive = cfg.codecs.output_codec == OutputCodec::Adaptive;
        let sup = Supervisor::new(sup_cfg, persisted_view(&st), routing.mirrors.clone());
        let r = &cfg.runtime;
        let accounts = cfg.output_accounts();
        let persisted: HashMap<u64, AccountPersisted> = accounts
            .iter()
            .map(|a| (a.user_id, account_view(&st, a.user_id)))
            .collect();
        let delays = r.retry_delays_seconds.clone();
        let cooldown = r.retry_cooldown_seconds;
        let mut pool_cfg = PoolConfig::new(routing);
        pool_cfg.rejection_limit = i64::try_from(r.repeated_rejection_limit).unwrap_or(i64::MAX);
        pool_cfg.excluded_viewer_ids = cfg
            .authorization
            .excluded_viewer_ids
            .iter()
            .copied()
            .collect();
        pool_cfg.relay_idle_stop_s = r.relay_idle_stop_seconds;
        pool_cfg.adaptive = adaptive;
        let mut pool = OutputPool::new(
            &accounts,
            &persisted,
            move || RetryPolicy::new(&delays, cooldown),
            pool_cfg,
        );
        // chat override persisted in state.json; config is only the default
        let autoscale = st.relay_autoscale.unwrap_or(r.relay_autoscale);
        if autoscale {
            // no session yet: nothing to start or stop
            pool.set_relay_target(0, monotonic());
        }
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let core = Core {
            adapter: None,
            token: None,
            codec_ctl: CodecController::new(),
            stall_kick: HashMap::new(),
            last_egress_at: f64::NEG_INFINITY,
            src_dims: None,
            autoscale,
            chat_ops: st.chat_operator_ids.iter().copied().collect(),
            session_since: None,
            send_hist: HashMap::new(),
            send_base: HashMap::new(),
            next_send_sample: f64::NEG_INFINITY,
            out_since: HashMap::new(),
            last_watch_ms: None,
            audio_ok: false,
            out_adapters: HashMap::new(),
            out_misconfigured: HashSet::new(),
            cooldowns: ChatCooldowns::new(
                cfg.control.command_cooldown_seconds as f64,
                cfg.control.change_cooldown_seconds as f64,
            ),
            chat_clock: Arc::new(monotonic),
            resnap_pending: false,
            profile: None,
            canvas: None,
            slate: None,
            slates: IndexMap::new(),
            seen_stream: None,
            seen_at: 0.0,
            timing: None,
            session_gen: None,
            in_voice: false,
            session_id: None,
            voice_dead_since: None,
            voice_4006: 0,
            last_4006_restart: None,
            exit_code: 0,
            shutdown_requested: false,
            persist_failed: false,
            last_pli: f64::NEG_INFINITY,
            pli_timer: None,
            last_overflow: f64::NEG_INFINITY,
            worker_dead_reported: false,
            metrics: Metrics::new(),
            network_stats: cfg.logging.network_stats,
            rejected_routing: None,
            routing_error: None,
        };
        let feedback_min = cfg.runtime.feedback_idr_min_interval_ms as f64 / 1000.0;
        let svc = Arc::new_cyclic(|me: &Weak<Service>| {
            let need_idr = |me: Weak<Service>| -> Box<dyn Fn() + Send + Sync> {
                Box::new(move || {
                    if let Some(s) = me.upgrade() {
                        s.need_idr();
                    }
                })
            };
            let gate_me = me.clone();
            let publisher = Arc::new(Publisher::new(
                Box::new(move |g: u64, e: u64, _now: f64| {
                    gate_me
                        .upgrade()
                        .is_some_and(|s| s.sup.lock().gate_allows(g, e, wall_clock()))
                }),
                Box::new(monotonic),
                MAX_LATE_S,
                need_idr(me.clone()),
                "H264",
            ));
            let out_pubs = accounts
                .iter()
                .map(|a| {
                    let uid = a.user_id;
                    let gate_me = me.clone();
                    let gate = Box::new(move |g: u64, e: u64, _now: f64| {
                        let Some(s) = gate_me.upgrade() else {
                            return false;
                        };
                        let open = s.sup.lock().gate_allows(g, e, wall_clock());
                        open && s.pool.lock().account_live(uid, g, e)
                    });
                    let p = Publisher::new(
                        gate,
                        Box::new(monotonic),
                        MAX_LATE_S,
                        need_idr(me.clone()),
                        "H264",
                    );
                    (uid, Arc::new(p))
                })
                .collect();
            let out_logged_in = accounts
                .iter()
                .map(|a| (a.user_id, watch::Sender::new(false)))
                .collect();
            Service {
                me: me.clone(),
                store,
                log,
                adaptive,
                adapter_factory,
                worker_factory,
                slate_builder: opts.slate_builder,
                out_factory: opts.output_adapter_factory,
                out_tokens: opts.output_tokens,
                install_signals: opts.install_signals,
                tick_s: opts.tick_s,
                pli_min_interval: opts.pli_min_interval_s,
                voice_lost_grace: opts.voice_lost_grace_s,
                peer_is_operator: opts.peer_is_operator,
                feedback_min,
                sup: Mutex::new(sup),
                pool: Mutex::new(pool),
                publisher,
                out_pubs,
                out_logged_in,
                hot: Mutex::new(Hot {
                    worker: None,
                    generation: 0,
                    epoch: 0,
                    last_idr_req: f64::NEG_INFINITY,
                }),
                core: Mutex::new(core),
                events_tx,
                events_rx: Mutex::new(Some(events_rx)),
                ops: SerialQueue::default(),
                replies: SerialQueue::default(),
                out_queues: Mutex::new(HashMap::new()),
                tasks: Arc::new(Tracker::default()),
                slate_tasks: Arc::new(Tracker::default()),
                ingest_lock: tokio::sync::Mutex::new(()),
                done: watch::Sender::new(false),
                status_written: Mutex::new(None),
                cfg,
            }
        });
        Ok(svc)
    }

    fn arc(&self) -> Arc<Service> {
        self.me
            .upgrade()
            .expect("the service is alive while it runs")
    }

    fn log(&self, name: &str, fields: Value) {
        (self.log)(name, fields);
    }

    fn set_done(&self) {
        self.done.send_replace(true);
    }

    fn gen_epoch(&self) -> (u64, u64) {
        let h = self.hot.lock();
        (h.generation, h.epoch)
    }

    fn worker(&self) -> Option<Arc<dyn Worker>> {
        self.hot.lock().worker.clone()
    }

    // ------------------------------------------------------------------ run

    /// Runs until shutdown; the exit code (0, 1 or 78).
    pub async fn run(self: Arc<Self>, token: Token) -> i32 {
        self.core.lock().token = Some(token);
        let handler: Handler = {
            let svc = Arc::clone(&self);
            Arc::new(move |req| svc.control(req))
        };
        let mut server = ControlServer::create(
            self.cfg.storage.state_directory.join("control.sock"),
            handler,
        );
        if let Some(server) = server.as_mut()
            && let Err(e) = server.start().await
        {
            self.log("control_socket_failed", json!({"error": e.to_string()}));
            return 1;
        }
        let signals = self
            .install_signals
            .then(|| tokio::spawn(wait_signals(Arc::downgrade(&self))));
        let events = tokio::spawn(Arc::clone(&self).events_loop());
        // The latch is read (in `new`) before any login; a latched account never logs in.
        if self.sup.lock().state() != RelayState::Fatal
            && let Err(e) = self.start_adapter().await
        {
            self.log("adapter_start_failed", json!({"error": e}));
            self.drop_adapter().await;
            events.abort();
            if let Some(s) = signals {
                s.abort();
            }
            if let Some(server) = server.as_mut() {
                server.close();
            }
            return 1;
        }
        let login = tokio::spawn(Arc::clone(&self).login_outputs());
        let mut ticker = tokio::spawn(Arc::clone(&self).ticker());
        let mut pump = tokio::spawn(Arc::clone(&self).pump());
        let mut done = self.done.subscribe();
        let failed = tokio::select! {
            _ = done.wait_for(|d| *d) => None,
            r = &mut ticker => Some(r),
            r = &mut pump => Some(r),
        };
        if let Some(r) = failed {
            let error = match r {
                Ok(()) => "task ended".to_string(),
                Err(e) => e.to_string(),
            };
            self.log("service_task_failed", json!({"error": error}));
            self.publisher.close();
            self.core.lock().exit_code = 1;
        }
        ticker.abort();
        pump.abort();
        if !self
            .ops
            .drain(Duration::from_secs_f64(DRAIN_TIMEOUT_S))
            .await
        {
            self.log("drain_timeout", json!({}));
        }
        self.ops.abort();
        login.abort();
        let _ = login.await;
        self.finish().await;
        if let Some(server) = server.as_mut() {
            server.close();
        }
        events.abort();
        if let Some(s) = signals {
            s.abort();
        }
        self.core.lock().exit_code
    }

    async fn finish(&self) {
        self.publisher.close();
        for p in self.out_pubs.values() {
            p.close();
        }
        wait_idle(&self.tasks, Duration::from_secs_f64(DRAIN_TIMEOUT_S)).await;
        let uids: Vec<u64> = self.core.lock().out_adapters.keys().copied().collect();
        for uid in uids {
            self.drop_output(uid).await;
        }
        let worker = self.hot.lock().worker.take();
        if let Some(w) = worker {
            let _ = tokio::task::spawn_blocking(move || w.stop()).await;
        }
        let adapter = {
            let mut c = self.core.lock();
            Self::cancel_pli_timer(&mut c);
            c.adapter.take()
        };
        if let Some(a) = adapter {
            a.close().await;
        }
        let mut c = self.core.lock();
        self.write_status(&mut c);
    }

    /// SIGTERM/SIGINT path: the Supervisor closes the gate first, then tears down.
    pub fn request_shutdown(&self) {
        let mut c = self.core.lock();
        self.request_shutdown_locked(&mut c);
    }

    fn request_shutdown_locked(&self, c: &mut Core) {
        if c.shutdown_requested {
            return;
        }
        c.shutdown_requested = true;
        self.handle(c, Event::Shutdown);
    }

    async fn events_loop(self: Arc<Self>) {
        let Some(mut rx) = self.events_rx.lock().take() else {
            return;
        };
        while let Some(msg) = rx.recv().await {
            self.on_msg(msg);
        }
    }

    fn on_msg(&self, msg: Msg) {
        if let Msg::Flush(tx) = msg {
            let _ = tx.send(());
            return;
        }
        let mut c = self.core.lock();
        match msg {
            Msg::Adapter(ev) => self.dispatch_locked(&mut c, ev),
            Msg::Pool(ev) => self.pool_event(&mut c, ev),
            Msg::Resnapshot(adapter) => self.resnapshot(&mut c, &adapter),
            Msg::IngressOverflow(g, e) => self.ingress_overflow(&mut c, g, e),
            Msg::Flush(_) => {}
        }
    }

    fn dispatch_locked(&self, c: &mut Core, ev: AdapterEvent) {
        match ev {
            AdapterEvent::Model(e) => self.handle(c, e),
            AdapterEvent::Chat(cmd) => self.on_chat(c, cmd),
            // a viewer PLI: the worker answers it
            AdapterEvent::Pool(PoolEvent::OutPli { .. }) => self.need_idr(),
            AdapterEvent::Pool(e) => self.pool_event(c, e),
        }
    }

    async fn ticker(self: Arc<Self>) {
        let mut next_poll = f64::NEG_INFINITY;
        let mut next_status = f64::NEG_INFINITY;
        loop {
            let now = monotonic();
            {
                let mut c = self.core.lock();
                self.handle(&mut c, Event::Tick);
                self.pool_event(&mut c, PoolEvent::OutTick);
                self.codec_event(&mut c, CodecEvent::Tick);
                self.check_codec_stall_locked(&mut c, now);
                self.sample_sends(&mut c, now);
                // OutTick above stops extra relays whose Go Live stayed without viewers
                self.scale_relays(&mut c, now);
                self.check_voice(&mut c, now);
                if now >= next_poll {
                    self.poll_state(&mut c);
                    next_poll = now + STATE_POLL_S;
                }
                if now >= next_status {
                    self.write_status(&mut c);
                    next_status = now + STATUS_S;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(self.tick_s)).await;
        }
    }

    async fn pump(self: Arc<Self>) {
        loop {
            if let Some(worker) = self.worker() {
                let mut c = self.core.lock();
                self.pump_worker(&mut c, &worker);
            }
            tokio::time::sleep(Duration::from_secs_f64(PUMP_S)).await;
        }
    }

    fn queue<F>(&self, name: &'static str, fut: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let log = Arc::clone(&self.log);
        self.ops.push(Box::pin(async move {
            if let Err(e) = fut.await {
                log("op_error", json!({"op": name, "error": e}));
            }
        }));
    }

    fn spawn_tracked<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let guard = track(&self.tasks);
        tokio::spawn(async move {
            let _guard = guard;
            fut.await;
        });
    }

    fn check_voice(&self, c: &mut Core, now: f64) {
        let Some(adapter) = c.adapter.clone() else {
            c.voice_dead_since = None;
            return;
        };
        if !c.in_voice || adapter.voice_alive() {
            c.voice_dead_since = None;
            return;
        }
        let since = *c.voice_dead_since.get_or_insert(now);
        if now - since < self.voice_lost_grace {
            return;
        }
        c.voice_dead_since = None;
        let foreign = adapter.foreign_voice_session();
        self.log(
            "voice_lost",
            json!({"vsu_session": adapter.last_vsu_session(), "foreign_session": foreign}),
        );
        c.in_voice = false;
        let s = self.arc();
        self.queue("_leave", async move {
            s.leave().await;
            Ok(())
        });
        self.handle(
            c,
            Event::VoiceLost {
                detail: "voice not connected".into(),
                foreign_session: foreign,
                from_prejoin: false,
            },
        );
    }

    /// Counts voice close 4006 and escalates; true when the failure came with a foreign voice
    /// session.
    fn voice_join_failed(
        &self,
        c: &mut Core,
        adapter: &Arc<dyn Adapter>,
        code: Option<u16>,
    ) -> bool {
        if code != Some(4006) {
            return false;
        }
        if adapter.foreign_voice_session() {
            self.log(
                "foreign_voice_session",
                json!({"vsu_session": adapter.last_vsu_session()}),
            );
            // another session of this account holds the voice state: a new gateway would not help
            return true;
        }
        c.voice_4006 += 1;
        let now = monotonic();
        if c.voice_4006 >= 2
            && c.session_gen.is_none()
            && c.last_4006_restart
                .is_none_or(|t| now - t >= VOICE_4006_RESTART_MIN_S)
        {
            self.log("voice_4006_restart_adapter", json!({"count": c.voice_4006}));
            c.voice_4006 = 0;
            c.last_4006_restart = Some(now);
            let s = self.arc();
            self.queue("_drop_adapter", async move {
                s.drop_adapter().await;
                Ok(())
            });
            // fresh IDENTIFY: its snapshot re-evaluates every gate
            let s = self.arc();
            self.queue("_ensure_adapter", async move { s.ensure_adapter().await });
        }
        false
    }

    fn connect_join_failed(
        &self,
        c: &mut Core,
        g: u64,
        adapter: &Arc<dyn Adapter>,
        exc: &AdapterError,
    ) {
        self.log(
            "join_failed",
            json!({"error": exc.to_string(), "code": exc.close_code(),
                   "vsu_session": adapter.last_vsu_session()}),
        );
        // order matters: ConnectFailed must run first so session_gen is cleared before
        // voice_join_failed may restart the adapter
        self.handle(
            c,
            Event::ConnectFailed {
                generation: g,
                reason: StopReason::ConnectFailed,
                retryable: true,
                detail: exc.to_string(),
                watch_retry: false,
            },
        );
        self.voice_join_failed(c, adapter, exc.close_code());
    }

    // ---------------------------------------------------------------- events

    fn handle(&self, c: &mut Core, event: Event) {
        let event = match event {
            Event::SourceProgress {
                generation,
                at,
                video,
            } => Event::SourceProgress {
                generation,
                at: mono_to_wall(at),
                video,
            },
            other => other,
        };
        let now = wall_clock();
        if let Event::ConnectFailed {
            generation, detail, ..
        } = &event
            && !detail.is_empty()
        {
            self.log(
                "connect_failed",
                json!({"generation": generation, "detail": truncate(detail, 300)}),
            );
        }
        let (before, acts, after, stream) = {
            let mut sup = self.sup.lock();
            let before = sup.state();
            let acts = sup.handle(event.clone(), now);
            let stream = sup.stream().map(|s| (s.key.clone(), s.incarnation));
            (before, acts, sup.state(), stream)
        };
        if let Some(s) = stream
            && c.seen_stream.as_ref() != Some(&s)
        {
            c.seen_stream = Some(s);
            c.seen_at = now;
        }
        if let Event::SourceProgress {
            generation,
            video: true,
            ..
        } = &event
        {
            Self::mark(c, *generation, "first_video_packet", Map::new());
        }
        if !matches!(event, Event::Tick) {
            c.metrics.inc(&format!("event.{}", event_name(&event)), 1);
        }
        if before != after {
            self.log(
                "state",
                json!({"old": before.as_str(), "new": after.as_str()}),
            );
        }
        self.execute_locked(c, acts);
        if self.adaptive {
            self.codec_from_event(c, &event);
        }
        if matches!(
            event,
            Event::ParticipantsChanged { .. }
                | Event::GatewaySnapshot { .. }
                | Event::RoutingChanged { .. }
        ) {
            self.scale_relays(c, monotonic());
        }
        match &event {
            Event::GatewaySnapshot { .. } => c.resnap_pending = false,
            Event::GatewayChanged {
                state: GatewayState::Fresh,
            } if c.resnap_pending => {
                // the resumed cache now holds the new target's voice, stream and participants
                c.resnap_pending = false;
                if let Some(a) = c.adapter.clone() {
                    let snap = a.snapshot(false);
                    self.handle(c, snap);
                }
            }
            _ => {}
        }
        if matches!(event, Event::ConfigInvalid { .. }) {
            c.exit_code = 78;
            self.set_done();
        }
    }

    /// Offline `relayctl` writes reach the running service through state.json.
    fn poll_state(&self, c: &mut Core) {
        let st = match self.store.load() {
            Ok(st) => st,
            Err(e) => {
                self.log("state_poll_failed", json!({"error": e.to_string()}));
                return;
            }
        };
        let (desired, state, fatal, prejoin_now, current_routing) = {
            let sup = self.sup.lock();
            (
                sup.desired_enabled(),
                sup.state(),
                sup.fatal(),
                sup.prejoin_voice(),
                sup.routing(),
            )
        };
        if st.desired_enabled != desired {
            self.handle(
                c,
                Event::EnableChanged {
                    enabled: st.desired_enabled,
                },
            );
        }
        if st.fatal_latch.is_none()
            && state == RelayState::Fatal
            && fatal.is_some_and(FatalReason::is_durable)
        {
            self.handle(c, Event::ResetFatal);
        }
        let prejoin = st.prejoin_voice.unwrap_or(self.cfg.runtime.prejoin_voice);
        if prejoin != prejoin_now {
            self.handle(c, Event::PrejoinChanged { on: prejoin });
        }
        let autoscale = st
            .relay_autoscale
            .unwrap_or(self.cfg.runtime.relay_autoscale);
        if autoscale != c.autoscale {
            self.set_autoscale(c, autoscale);
        }
        c.chat_ops = st.chat_operator_ids.iter().copied().collect();
        let routing = Routing::from_persisted(st.routing.as_ref(), &config_routing(&self.cfg));
        match routing_error(&routing, &self.cfg) {
            None => {
                c.rejected_routing = None;
                c.routing_error = None;
                if routing != current_routing {
                    self.handle(c, Event::RoutingChanged { routing });
                }
            }
            Some(err) => {
                // ignored while running; start-time check stays fail-closed, so make it visible now
                c.routing_error = Some(err.clone());
                if st.routing != c.rejected_routing {
                    // log once per distinct bad entry
                    c.rejected_routing = st.routing.clone();
                    self.log(
                        "state_routing_rejected",
                        json!({"level": "error", "error": err}),
                    );
                }
            }
        }
    }

    // ------------------------------------------------------------------ chat

    /// parse -> plan -> persist -> RoutingChanged (which re-routes the pool) -> reply. Content is
    /// never logged.
    fn on_chat(&self, c: &mut Core, ev: ChatCommand) {
        let ctl = &self.cfg.control;
        let Some(parsed) = parse(&ev.text, &ctl.command_prefix, &ctl.public_commands) else {
            return;
        };
        let name = command_type_name(&parsed);
        let changing = !matches!(
            parsed,
            Err(_) | Ok(Command::Status { .. }) | Ok(Command::OpList)
        );
        let operator = ctl.is_operator(ev.author_id, &ev.author_role_ids, &c.chat_ops);
        let now = (c.chat_clock)();
        if !operator {
            let public = match &parsed {
                Err(_) => false,
                Ok(cmd) => ctl.public_commands.contains(command_name(cmd)),
            };
            if !public {
                // no reply to non-operators for invalid or non-public commands
                return;
            }
            // a change the global cooldown rejects must not start the author window, or its
            // stated wait would lie
            if !(changing && c.cooldowns.change_wait(now) > 0)
                && let Some(wait) = c.cooldowns.author(ev.author_id, now)
            {
                let text = (wait > 0).then(|| format!("⏳ chậm lại chút, thử lại sau {wait}s"));
                self.chat_cooldown(c, &ev, name, text);
                return;
            }
        }
        let op_user = match &parsed {
            Ok(Command::OpAdd(u) | Command::OpRemove(u)) => Some(*u),
            _ => None,
        };
        let chat_log = |outcome: &str| {
            let mut m = Map::new();
            m.insert("command".into(), json!(name));
            m.insert("author".into(), json!(ev.author_id));
            m.insert("channel".into(), json!(ev.channel_id));
            m.insert("outcome".into(), json!(outcome));
            if let Some(u) = op_user {
                m.insert("user".into(), json!(u));
            }
            Value::Object(m)
        };
        if op_user.is_some() && !ctl.is_owner(ev.author_id, &ev.author_role_ids) {
            // chat-added operators may not add others: only config owners manage the list
            self.log("chat_command", chat_log("denied"));
            self.reply(
                c,
                &ev,
                "⚠️ **Chỉ chủ sở hữu** (khai báo trong config) mới thêm hoặc gỡ operator.".into(),
            );
            return;
        }
        if changing {
            let wait = c.cooldowns.change_wait(now);
            if wait > 0 {
                let notice = operator || c.cooldowns.change_notice(ev.author_id);
                let text = notice.then(|| format!("⏳ vừa có thay đổi, thử lại sau {wait}s"));
                self.chat_cooldown(c, &ev, name, text);
                return;
            }
        }
        let ctx = self.routing_context(c);
        let old = self.sup.lock().routing();
        let result: Result<(&str, String), ChatFail> = (|| match parsed {
            Err(ParseError(text)) => Ok(("parse_error", text)),
            Ok(Command::Status { verbose }) => {
                let details = self.status_details_locked(c, monotonic(), verbose);
                let text = format_status(&old, &ctx, &details, c.routing_error.as_deref());
                Ok(("status", text))
            }
            Ok(cmd @ Command::SetEnabled(on)) => {
                self.set_enabled(c, on)?;
                Ok(("applied", describe_change(&cmd, &old, &old, &ctx)))
            }
            Ok(cmd @ Command::SetPrejoin(on)) => {
                self.store.update(|mut s| {
                    s.prejoin_voice = Some(on);
                    s
                })?;
                self.handle(c, Event::PrejoinChanged { on });
                Ok(("applied", describe_change(&cmd, &old, &old, &ctx)))
            }
            Ok(cmd @ Command::SetAutoscale(on)) => {
                if on != c.autoscale {
                    self.store.update(|mut s| {
                        s.relay_autoscale = Some(on);
                        s
                    })?;
                    self.set_autoscale(c, on);
                }
                Ok(("applied", describe_change(&cmd, &old, &old, &ctx)))
            }
            Ok(Command::OpList) => {
                Ok(("listed", format_operators(&self.operator_context(c), &ctx)))
            }
            Ok(cmd @ (Command::OpAdd(_) | Command::OpRemove(_))) => {
                match plan_op(&cmd, &self.operator_context(c), &ctx) {
                    Err(text) => Ok(("rejected", text)),
                    Ok(_) => {
                        let (add, uid) = match cmd {
                            Command::OpAdd(u) => (true, u),
                            Command::OpRemove(u) => (false, u),
                            _ => unreachable!("matched above"),
                        };
                        // applies only this change, so a concurrent hand edit is kept
                        let st = self.store.update(|mut s| {
                            s.chat_operator_ids.retain(|u| *u != uid);
                            if add {
                                s.chat_operator_ids.push(uid);
                            }
                            s
                        })?;
                        c.chat_ops = st.chat_operator_ids.iter().copied().collect();
                        Ok(("applied", describe_op(&cmd, &ctx)))
                    }
                }
            }
            Ok(cmd) => {
                let resolved = match resolve_here(&cmd, &old, ev.author_voice_channel_id) {
                    Err(text) => return Ok(("rejected", text)),
                    Ok(r) => r,
                };
                match plan_change(&resolved, &old, &ctx) {
                    Err(text) => Ok(("rejected", text)),
                    Ok(new) => {
                        self.routing_write(c, &old, &new)?;
                        self.handle(
                            c,
                            Event::RoutingChanged {
                                routing: new.clone(),
                            },
                        );
                        Ok(("applied", describe_change(&resolved, &old, &new, &ctx)))
                    }
                }
            }
        })();
        let (outcome, text) = match result {
            Ok((o, t)) => (o, t),
            Err(ChatFail::Moved) => ("stale", "⚠️ state.json vừa thay đổi, thử lại lệnh.".into()),
            Err(ChatFail::Write(e)) => {
                self.log("chat_state_write_failed", json!({"error": e}));
                (
                    "write_failed",
                    "❌ **Không ghi được state.json**; không có gì thay đổi.".into(),
                )
            }
        };
        if outcome == "applied" {
            c.cooldowns.changed(now);
        }
        self.log("chat_command", chat_log(outcome));
        self.reply(c, &ev, text);
    }

    /// A rate-limited command changes nothing; `text` None = the author was already told this
    /// window.
    fn chat_cooldown(&self, c: &mut Core, ev: &ChatCommand, name: &str, text: Option<String>) {
        self.log(
            "chat_command",
            json!({"command": name, "author": ev.author_id, "channel": ev.channel_id,
                   "outcome": "cooldown"}),
        );
        if let Some(text) = text {
            self.reply(c, ev, text);
        }
    }

    /// Writes under the state lock (re-checked by revision): a routing stored since the last poll
    /// must not be overwritten.
    fn routing_write(&self, c: &Core, old: &Routing, new: &Routing) -> Result<(), ChatFail> {
        let planned = old.to_persisted();
        let default = config_routing(&self.cfg).to_persisted();
        for _ in 0..8 {
            let s = self.store.load()?;
            let stored = s.routing.clone().unwrap_or_else(|| default.clone());
            // an already-reported invalid routing is never applied; a chat command may replace it
            if stored != planned && (s.routing.is_none() || s.routing != c.rejected_routing) {
                return Err(ChatFail::Moved);
            }
            match self.store.update_expecting(s.revision, |mut st| {
                st.routing = Some(new.to_persisted());
                st
            }) {
                Ok(_) => return Ok(()),
                Err(StateError::Conflict(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(ChatFail::Moved)
    }

    fn operator_context(&self, c: &Core) -> OperatorContext {
        let ctl = &self.cfg.control;
        OperatorContext {
            config_user_ids: ctl.operator_user_ids.clone(),
            role_ids: ctl.operator_role_ids.clone(),
            added: c.chat_ops.clone(),
            target_user_id: self.sup.lock().routing().target_user_id,
        }
    }

    fn routing_context(&self, c: &Core) -> RoutingContext {
        let accounts = self.cfg.output_accounts();
        let mirrors = self.cfg.discord.mirror_accounts();
        let mut relay_user_ids: BTreeSet<u64> = accounts.iter().map(|a| a.user_id).collect();
        relay_user_ids.insert(self.cfg.discord.expected_relay_user_id);
        let unavailable = self
            .pool
            .lock()
            .status(monotonic())
            .iter()
            .filter(|r| matches!(r.state, OutputState::Fatal | OutputState::Quarantine))
            .map(|r| r.account)
            .collect();
        let voice_channel_ids = c
            .adapter
            .as_ref()
            .map(|a| a.voice_channel_ids().into_iter().collect())
            .unwrap_or_default();
        let mut ctx = RoutingContext::new(
            relay_user_ids,
            mirrors.iter().map(|a| a.user_id).collect(),
            unavailable,
            voice_channel_ids,
        );
        ctx.mirror_fixed = mirrors
            .iter()
            .filter_map(|a| a.channel_id.map(|ch| (a.user_id, ch)))
            .collect();
        if let Some(a) = &c.adapter {
            let (n, b, r) = (Arc::clone(a), Arc::clone(a), Arc::clone(a));
            ctx.user_name = Box::new(move |uid| n.display_name(uid));
            ctx.is_bot = Box::new(move |uid| b.is_bot(uid));
            ctx.role_name = Box::new(move |rid| r.role_name(rid));
        }
        ctx
    }

    /// Replies go out in order, off the ops queue so a slow voice join never delays them.
    fn reply(&self, c: &Core, ev: &ChatCommand, text: String) {
        let Some(adapter) = c.adapter.clone() else {
            self.log(
                "chat_reply_failed",
                json!({"channel": ev.channel_id, "error": "no adapter"}),
            );
            return;
        };
        let (channel, message) = (ev.channel_id, ev.message_id);
        let log = Arc::clone(&self.log);
        let guard = track(&self.tasks);
        self.replies.push(Box::pin(async move {
            let _guard = guard;
            if let Err(e) = adapter.reply(channel, message, text).await {
                log(
                    "chat_reply_failed",
                    json!({"channel": channel, "error": e.to_string()}),
                );
            }
        }));
    }

    /// After a target/channel change the new target may already be streaming: read the cache now.
    fn resnapshot(&self, c: &mut Core, adapter: &Arc<dyn Adapter>) {
        if !c.adapter.as_ref().is_some_and(|a| Arc::ptr_eq(a, adapter)) {
            return;
        }
        if self.sup.lock().gateway() == GatewayState::Fresh {
            let snap = adapter.snapshot(false);
            self.handle(c, snap);
        } else {
            c.resnap_pending = true;
        }
    }

    // --------------------------------------------------------------- actions

    fn execute_locked(&self, c: &mut Core, acts: Vec<Action>) {
        for a in acts {
            c.metrics.inc(&format!("action.{}", action_name(&a)), 1);
            match a {
                Action::CloseGate { .. } => {
                    // synchronous and first on every stop/hold path
                    self.publisher.close();
                    for p in self.out_pubs.values() {
                        p.close();
                    }
                }
                Action::ShowSlate { generation, reason } => {
                    self.publisher.set_slate(c.slate.clone());
                    self.pool_event(c, PoolEvent::SessionHold { generation, reason });
                    self.codec_event(c, CodecEvent::Hold);
                    let epoch = self.gen_epoch().1;
                    self.send(SetOutput {
                        generation,
                        epoch,
                        enabled: false,
                    });
                }
                Action::OpenGate { generation, epoch } => {
                    self.hot.lock().last_idr_req = f64::NEG_INFINITY;
                    self.publisher.set_source(generation, epoch);
                    self.publisher.set_slate(c.slate.clone());
                    self.send(SetOutput {
                        generation,
                        epoch,
                        enabled: true,
                    });
                    let inc = self.sup.lock().status(wall_clock()).source_incarnation;
                    self.pool_event(
                        c,
                        PoolEvent::SessionLive {
                            generation,
                            epoch,
                            incarnation: inc.unwrap_or(0),
                        },
                    );
                    self.codec_event(c, CodecEvent::Live);
                    if c.session_since.is_none() {
                        c.session_since = Some(monotonic());
                        c.send_base = self
                            .send_pubs()
                            .iter()
                            .map(|(k, p)| (*k, p.snapshot().freezes))
                            .collect();
                    }
                    Self::mark(c, generation, "open_gate", Map::new());
                    self.log_timing(c, generation, "open");
                }
                Action::ApplyRouting { routing } => {
                    if let Some(adapter) = c.adapter.clone()
                        && (adapter.target_id(), adapter.channel_id())
                            != (routing.target_user_id, routing.channel_id)
                    {
                        adapter.set_routing(routing.target_user_id, routing.channel_id);
                        let _ = self.events_tx.send(Msg::Resnapshot(adapter));
                    }
                    let acts = self.pool.lock().set_routing(routing, monotonic());
                    for pa in acts {
                        self.pool_action(c, pa);
                    }
                }
                Action::StartConnect {
                    generation,
                    epoch,
                    stream,
                } => self.start_connect(c, generation, epoch, stream),
                Action::CreateOutput { generation } => {
                    let s = self.arc();
                    let worker = self.worker();
                    self.queue("_create_output", async move {
                        s.create_output(generation, worker).await;
                        Ok(())
                    });
                }
                Action::RebuildPipeline { generation, epoch } => {
                    {
                        let mut h = self.hot.lock();
                        h.epoch = epoch;
                        // a mandatory IDR is never delayed by earlier feedback requests
                        h.last_idr_req = f64::NEG_INFINITY;
                    }
                    if let Some(a) = &c.adapter {
                        a.set_epoch(epoch);
                    }
                    self.send(ResetPipeline { generation, epoch });
                }
                Action::RequestInputKeyframe { method, .. } => match method {
                    KeyframeMethod::Pli => self.pli(c),
                    KeyframeMethod::Rewatch => {
                        let s = self.arc();
                        self.queue("_keyframe", async move {
                            s.keyframe(KeyframeMethod::Rewatch).await;
                            Ok(())
                        });
                    }
                },
                Action::Teardown {
                    generation,
                    leave_voice,
                    reason,
                } => {
                    // a session that never opened is still reported
                    self.log_timing(c, generation, reason.as_str());
                    self.pool_event(
                        c,
                        PoolEvent::SessionEnded {
                            generation,
                            reason,
                            incarnation_ended: matches!(
                                reason,
                                StopReason::SourceStopped | StopReason::SourceLeftWhitelist
                            ),
                        },
                    );
                    c.session_gen = None;
                    c.session_since = None;
                    c.send_base.clear();
                    // cumulative per worker: the next session starts at 0
                    c.metrics.gauge_int("worker.gap_frames", 0);
                    // the ingest output is only ever ended by this Teardown, so the fresh
                    // controller drops "ingest"
                    Self::reset_codec_ctl(c);
                    c.src_dims = None;
                    let worker = self.hot.lock().worker.take();
                    c.slate = None;
                    c.canvas = None;
                    let s = self.arc();
                    self.queue("_teardown", async move {
                        s.teardown(worker, leave_voice).await;
                        Ok(())
                    });
                }
                Action::LeaveVoice { .. } => {
                    c.in_voice = false;
                    let s = self.arc();
                    self.queue("_leave", async move {
                        s.leave().await;
                        Ok(())
                    });
                }
                Action::JoinVoice => {
                    let s = self.arc();
                    self.queue("_prejoin", async move {
                        s.prejoin().await;
                        Ok(())
                    });
                }
                Action::SetFatalLatch { reason, detail } => {
                    self.persist(c, move |mut s| {
                        s.fatal_latch = Some(latch(reason, &detail));
                        s
                    });
                    let s = self.arc();
                    self.queue("_drop_adapter", async move {
                        s.drop_adapter().await;
                        Ok(())
                    });
                }
                Action::ClearFatalLatch { reset_rejections } => {
                    self.persist(c, move |mut s| {
                        s.fatal_latch = None;
                        if reset_rejections {
                            s.consecutive_rejections = 0;
                        }
                        s
                    });
                    let s = self.arc();
                    self.queue("_ensure_adapter", async move { s.ensure_adapter().await });
                }
                Action::SetQuarantine { record } => {
                    self.persist(c, move |mut s| {
                        s.quarantine = record.as_ref().map(quarantine_json);
                        s
                    });
                }
                Action::SetRejections { count } => {
                    self.persist(c, move |mut s| {
                        s.consecutive_rejections = i64::from(count);
                        s
                    });
                }
                Action::Exit { code } => {
                    c.exit_code = if c.persist_failed { 1 } else { code };
                    self.set_done();
                }
            }
        }
    }

    // ------------------------------------------------------- output accounts

    fn pool_event(&self, c: &mut Core, event: PoolEvent) {
        match &event {
            PoolEvent::OutFailed {
                account,
                reason,
                retryable,
                detail,
                ..
            } => {
                let role = self
                    .cfg
                    .output_accounts()
                    .iter()
                    .find(|a| a.user_id == *account)
                    .map(|a| a.role.as_str());
                let channel = self.pool.lock().channel_of(*account);
                self.log(
                    "output_failed",
                    json!({"account": account, "role": role, "channel": channel,
                           "reason": reason.as_str(), "retryable": retryable,
                           "detail": truncate(detail, 300)}),
                );
            }
            PoolEvent::OutHealth {
                account, ok: false, ..
            } => {
                let channel = self.pool.lock().channel_of(*account);
                self.log(
                    "output_unhealthy",
                    json!({"account": account, "channel": channel}),
                );
            }
            PoolEvent::OutPli { .. } => {
                self.need_idr();
                return;
            }
            PoolEvent::OutCodecChanged {
                account,
                generation,
                attempt,
                codec,
            } => {
                let current = self.pool.lock().is_current(*account, *generation, *attempt);
                if current {
                    self.codec_event(
                        c,
                        CodecEvent::OutputCodec {
                            output_id: OutputId::Account(*account),
                            codec: codec.clone(),
                        },
                    );
                }
                return;
            }
            _ => {}
        }
        // a stale failure is ignored by the pool and must not drop the live output from the codec
        // controller
        let failed_current = match &event {
            PoolEvent::OutFailed {
                account,
                generation,
                attempt,
                ..
            } => self.pool.lock().is_current(*account, *generation, *attempt),
            _ => false,
        };
        let acts = self.pool.lock().handle(event.clone(), monotonic());
        for a in acts {
            self.pool_action(c, a);
        }
        if failed_current {
            if let PoolEvent::OutFailed { account, .. } = event {
                self.codec_event(
                    c,
                    CodecEvent::OutputGone {
                        output_id: OutputId::Account(account),
                    },
                );
            }
        } else if let PoolEvent::OutReady {
            account,
            generation,
            attempt,
            negotiated_video_codec: Some(codec),
        } = event
            && self.adaptive
        {
            let ready = {
                let pool = self.pool.lock();
                pool.is_current(account, generation, attempt)
                    && pool.status(monotonic()).iter().any(|r| {
                        r.account == account
                            && matches!(r.state, OutputState::Ready | OutputState::Live)
                    })
            };
            if ready && let Some(p) = self.out_pubs.get(&account) {
                p.set_codec(&codec);
                self.codec_event(
                    c,
                    CodecEvent::OutputAdded {
                        output_id: OutputId::Account(account),
                        codec: codec.clone(),
                        offer_top: "AV1".into(),
                        bound: Some(codec),
                    },
                );
            }
        }
    }

    fn pool_action(&self, c: &mut Core, a: PoolAction) {
        c.metrics
            .inc(&format!("action.{}", pool_action_name(&a)), 1);
        match a {
            PoolAction::Start {
                account,
                generation,
                attempt,
                channel_id,
            } => {
                let (Some(_), Some(p)) = (c.canvas, c.profile.clone()) else {
                    self.pool_event(
                        c,
                        PoolEvent::OutFailed {
                            account,
                            generation,
                            attempt,
                            reason: StopReason::ConnectFailed,
                            retryable: true,
                            detail: "no canvas".into(),
                        },
                    );
                    return;
                };
                let Some((w, h)) = Self::video_dims(c) else {
                    return;
                };
                let Some(publisher) = self.out_pubs.get(&account).cloned() else {
                    return;
                };
                let params = VideoParams {
                    width: w,
                    height: h,
                    fps: u32::try_from(p.fps).unwrap_or(u32::MAX),
                    bitrate: u32::try_from(p.video_bitrate_kbps * 1000).unwrap_or(u32::MAX),
                };
                // adaptive: the codec is only known once the Go Live negotiated it
                let source = if self.adaptive {
                    let factory: SourceFactory = Box::new(move |_codec: VoiceCodec| {
                        Box::new(PublisherSource(publisher)) as Box<dyn FrameSource>
                    });
                    OutputSource::ForCodec(factory)
                } else {
                    OutputSource::Ready(Box::new(PublisherSource(publisher)))
                };
                let s = self.arc();
                self.out_queue(account, "_out_publish", async move {
                    s.out_publish(account, generation, attempt, source, params, channel_id)
                        .await;
                    Ok(())
                });
            }
            PoolAction::SetLive {
                account,
                generation,
                epoch,
            } => {
                c.out_since.entry(account).or_insert_with(monotonic);
                if let Some(p) = self.out_pubs.get(&account) {
                    p.set_slate(c.slate.clone());
                    p.set_source(generation, epoch);
                }
            }
            PoolAction::Slate { account, .. } => {
                if let Some(p) = self.out_pubs.get(&account) {
                    p.set_slate(c.slate.clone());
                }
            }
            PoolAction::CloseGate { account, .. } => {
                // purges, so a closed publisher holds no frames
                if let Some(p) = self.out_pubs.get(&account) {
                    p.close();
                }
            }
            PoolAction::Teardown { account, .. } => {
                c.out_since.remove(&account);
                self.codec_event(
                    c,
                    CodecEvent::OutputGone {
                        output_id: OutputId::Account(account),
                    },
                );
                let s = self.arc();
                self.out_queue(account, "_out_end", async move {
                    s.out_end(account).await;
                    Ok(())
                });
            }
            PoolAction::SetFatal {
                account,
                reason,
                detail,
            } => {
                c.out_since.remove(&account);
                self.codec_event(
                    c,
                    CodecEvent::OutputGone {
                        output_id: OutputId::Account(account),
                    },
                );
                self.persist(c, move |s| {
                    set_account(s, account, |r| r.fatal_latch = Some(latch(reason, &detail)))
                });
                let s = self.arc();
                self.out_queue(account, "_drop_output", async move {
                    s.drop_output(account).await;
                    Ok(())
                });
            }
            PoolAction::SetQuarantine {
                account,
                incarnation,
            } => self.persist(c, move |s| {
                set_account(s, account, |r| r.quarantine_incarnation = incarnation)
            }),
            PoolAction::SetRejections { account, count } => self.persist(c, move |s| {
                set_account(s, account, |r| r.consecutive_rejections = count)
            }),
        }
    }

    /// Per-account serial ops: one account's slow join never delays another account or the ingest.
    fn out_queue<F>(&self, uid: u64, name: &'static str, fut: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let queue = self.out_queues.lock().entry(uid).or_default().clone();
        let log = Arc::clone(&self.log);
        let guard = track(&self.tasks);
        queue.push(Box::pin(async move {
            let _guard = guard;
            if let Err(e) = fut.await {
                log(
                    "output_op_error",
                    json!({"account": uid, "op": name, "error": e}),
                );
            }
        }));
    }

    fn pool_event_now(&self, event: PoolEvent) {
        let mut c = self.core.lock();
        self.pool_event(&mut c, event);
    }

    async fn out_publish(
        self: Arc<Self>,
        uid: u64,
        g: u64,
        n: u64,
        source: OutputSource,
        params: VideoParams,
        channel_id: u64,
    ) {
        if self.core.lock().out_misconfigured.contains(&uid) {
            self.pool_event_now(PoolEvent::OutFailed {
                account: uid,
                generation: g,
                attempt: n,
                reason: StopReason::ConfigInvalid,
                retryable: false,
                detail: "token belongs to another account".into(),
            });
            return;
        }
        let result: Result<(), String> = async {
            let logged_in = self.core.lock().out_adapters.contains_key(&uid);
            if !logged_in
                && self.out_factory.is_some()
                && self.out_tokens.contains_key(&uid)
                && let Some(ready) = self.out_logged_in.get(&uid)
            {
                // a session can go live before the staggered/retried login finishes
                let mut rx = ready.subscribe();
                tokio::time::timeout(
                    Duration::from_secs_f64(OUT_LOGIN_WAIT_S),
                    rx.wait_for(|v| *v),
                )
                .await
                .map_err(|_| "TimeoutError()".to_string())?
                .map_err(|e| e.to_string())?;
            }
            let adapter = self
                .core
                .lock()
                .out_adapters
                .get(&uid)
                .cloned()
                .ok_or("output account not logged in")?;
            adapter
                .publish(g, n, source, params, channel_id, self.adaptive)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
        .await;
        if let Err(detail) = result {
            self.pool_event_now(PoolEvent::OutFailed {
                account: uid,
                generation: g,
                attempt: n,
                reason: StopReason::ConnectFailed,
                retryable: true,
                detail,
            });
        }
    }

    async fn out_end(&self, uid: u64) {
        let adapter = self.core.lock().out_adapters.get(&uid).cloned();
        if let Some(a) = adapter
            && let Err(e) = a.end().await
        {
            self.step_error("end", &e);
        }
    }

    async fn drop_output(&self, uid: u64) {
        let adapter = self.core.lock().out_adapters.remove(&uid);
        if let Some(a) = adapter {
            a.close().await;
        }
    }

    fn out_fatal(&self, uid: u64) -> bool {
        self.pool
            .lock()
            .status(monotonic())
            .iter()
            .any(|r| r.account == uid && r.fatal.is_some())
    }

    async fn login_outputs(self: Arc<Self>) {
        if self.out_factory.is_none() {
            return;
        }
        // dropping the set (this task cancelled) cancels every login
        let mut tasks = JoinSet::new();
        let mut first = true;
        for acct in self.cfg.output_accounts() {
            if self.out_fatal(acct.user_id) || !self.out_tokens.contains_key(&acct.user_id) {
                continue;
            }
            if !first {
                tokio::time::sleep(Duration::from_secs_f64(LOGIN_STAGGER_S)).await;
            }
            first = false;
            tasks.spawn(Arc::clone(&self).login_one(acct));
        }
        while tasks.join_next().await.is_some() {}
    }

    /// Retry login with backoff until it succeeds or the account is latched.
    async fn login_one(self: Arc<Self>, acct: OutputAccount) {
        let uid = acct.user_id;
        let Some(factory) = &self.out_factory else {
            return;
        };
        let Some(token) = self.out_tokens.get(&uid).cloned() else {
            return;
        };
        let r = &self.cfg.runtime;
        let mut policy = RetryPolicy::new(&r.retry_delays_seconds, r.retry_cooldown_seconds);
        while !self.out_fatal(uid) {
            let tx = self.events_tx.clone();
            let sink: PoolEventSink = Arc::new(move |ev| {
                let _ = tx.send(Msg::Pool(ev));
            });
            let adapter = factory(&acct, sink, Arc::clone(&self.log));
            match adapter.start(token.clone()).await {
                Err(AdapterError::AccountMismatch) => {
                    // the token belongs to another account: a config error, not transient (spec §15)
                    self.core.lock().out_misconfigured.insert(uid);
                    self.log(
                        "output_account_mismatch",
                        json!({"account": uid,
                               "error": AdapterError::AccountMismatch.to_string()}),
                    );
                    adapter.close().await;
                    return;
                }
                Err(e) => {
                    self.log(
                        "output_login_failed",
                        json!({"account": uid, "error": e.to_string()}),
                    );
                    adapter.close().await;
                    tokio::time::sleep(Duration::from_secs_f64(policy.next_delay())).await;
                    continue;
                }
                Ok(()) => {}
            }
            // OutAuthRejected already latched it
            if !self.out_fatal(uid) {
                self.core.lock().out_adapters.insert(uid, adapter);
                if let Some(ready) = self.out_logged_in.get(&uid) {
                    ready.send_replace(true);
                }
            }
            return;
        }
    }

    fn persist(&self, c: &mut Core, f: impl FnOnce(PersistedState) -> PersistedState) {
        if let Err(e) = self.store.update(f) {
            // fail closed: the latch/quarantine may not be durable, so stop rather than keep
            // publishing
            self.log("persist_failed", json!({"error": e.to_string()}));
            c.persist_failed = true;
            c.exit_code = 1;
            self.publisher.close();
            for p in self.out_pubs.values() {
                p.close();
            }
            self.request_shutdown_locked(c);
        }
    }

    fn send(&self, msg: impl Into<Ingress>) {
        if let Some(w) = self.worker() {
            w.send(msg.into());
        }
    }

    fn start_connect(&self, c: &mut Core, generation: u64, epoch: u64, stream: SourceStream) {
        let seen = (c.seen_stream.as_ref() == Some(&(stream.key.clone(), stream.incarnation)))
            .then_some(c.seen_at);
        c.timing = Some(Timing {
            generation,
            start: wall_clock(),
            seen,
            gate_ready: self.sup.lock().gate_ready_at(),
            prejoined: c.in_voice,
            marks: IndexMap::new(),
            extra: Map::new(),
        });
        {
            let mut h = self.hot.lock();
            h.generation = generation;
            h.epoch = epoch;
        }
        c.canvas = None;
        Self::reset_codec_ctl(c);
        c.src_dims = None;
        c.worker_dead_reported = false;
        c.profile = Some(self.cfg.output_profile());
        c.session_gen = Some(generation);
        let s = self.arc();
        self.queue("_connect", async move {
            s.connect(generation, epoch, stream).await;
            Ok(())
        });
    }

    /// Records the first time `name` happened in generation `g` (start milestones, never per
    /// packet).
    fn mark(c: &mut Core, g: u64, name: &str, extra: Map<String, Value>) {
        if let Some(t) = c.timing.as_mut()
            && t.generation == g
        {
            t.marks.entry(name.to_string()).or_insert_with(wall_clock);
            t.extra.extend(extra);
        }
    }

    fn mark_now(&self, g: u64, name: &str, extra: Map<String, Value>) {
        Self::mark(&mut self.core.lock(), g, name, extra);
    }

    /// One session_timing event per generation: at OpenGate, or at teardown when it never opened.
    fn log_timing(&self, c: &mut Core, g: u64, outcome: &str) {
        if !c.timing.as_ref().is_some_and(|t| t.generation == g) {
            return;
        }
        let t = c.timing.take().expect("checked above");
        let mut f = Map::new();
        f.insert("generation".into(), json!(g));
        f.insert("outcome".into(), json!(outcome));
        f.insert("prejoined".into(), json!(t.prejoined));
        f.extend(t.extra);
        if let Some(seen) = t.seen {
            f.insert("start_from_seen_ms".into(), json!(ms(t.start - seen)));
        }
        if let Some(ready) = t.gate_ready {
            f.insert(
                "start_from_gate_ready_ms".into(),
                json!(ms(t.start - ready)),
            );
            if let Some(seen) = t.seen {
                f.insert("gate_ready_from_seen_ms".into(), json!(ms(ready - seen)));
            }
        }
        for (name, at) in &t.marks {
            f.insert(format!("{name}_ms"), json!(ms(*at - t.start)));
            if let Some(seen) = t.seen
                && FROM_SEEN.contains(&name.as_str())
            {
                f.insert(format!("{name}_from_seen_ms"), json!(ms(*at - seen)));
            }
        }
        // sessions that never sent a watch keep the last measurement
        if let Some(v) = f.get("watch_sent_from_seen_ms").and_then(Value::as_f64) {
            c.last_watch_ms = Some(v);
        }
        self.log("session_timing", Value::Object(f));
    }

    // ----------------------------------------------------------- async steps

    fn handle_now(&self, event: Event) {
        let mut c = self.core.lock();
        self.handle(&mut c, event);
    }

    /// Join and watch back to back like the official client: the watch (gateway op 20) is requested
    /// as soon as the gateway echoes our voice state, while the voice media session connects and
    /// the worker spawns. Returns only after join, watch and spawn have all settled, so a queued
    /// Teardown cleans up all of them.
    async fn connect(self: Arc<Self>, g: u64, e: u64, stream: SourceStream) {
        let adapter = {
            let c = self.core.lock();
            if c.session_gen != Some(g) {
                return;
            }
            c.adapter.clone()
        };
        let Some(adapter) = adapter else {
            self.handle_now(Event::ConnectFailed {
                generation: g,
                reason: StopReason::ConnectFailed,
                retryable: true,
                detail: "adapter not running".into(),
                watch_retry: false,
            });
            return;
        };
        adapter.set_epoch(e);
        if self.core.lock().in_voice && !adapter.voice_alive() {
            self.log(
                "prejoined_voice_dead",
                json!({"vsu_session": adapter.last_vsu_session()}),
            );
            if let Err(err) = adapter.leave().await {
                self.step_error("leave", &err);
            }
            self.core.lock().in_voice = false;
        }
        let mut join: Option<JoinHandle<Result<(), AdapterError>>> = None;
        let mut echo: Option<oneshot::Receiver<()>> = None;
        if !self.core.lock().in_voice {
            let (tx, rx) = oneshot::channel();
            let cb: VoiceStateCallback = Box::new(move || {
                let _ = tx.send(());
            });
            join = Some(tokio::spawn(
                Arc::clone(&self).join(Arc::clone(&adapter), Some(cb)),
            ));
            echo = Some(rx);
        }
        let spawn = tokio::spawn(Arc::clone(&self).spawn_worker(g));
        'body: {
            let mut join_result: Option<Result<(), AdapterError>> = None;
            match (join.as_mut(), echo.as_mut()) {
                (Some(j), Some(rx)) => {
                    let confirmed = tokio::select! {
                        biased;
                        r = &mut *rx => r.is_ok(),
                        r = &mut *j => {
                            join_result = Some(joined(r));
                            rx.try_recv().is_ok()
                        }
                    };
                    if !confirmed && join_result.is_none() {
                        // the callback was dropped unused: the join has the answer
                        join_result = Some(joined((&mut *j).await));
                    }
                    if join_result.is_some() {
                        join = None;
                    }
                    if confirmed {
                        self.mark_now(g, "voice_state_confirmed", Map::new());
                    } else if let Some(Err(exc)) = &join_result {
                        let mut c = self.core.lock();
                        self.connect_join_failed(&mut c, g, &adapter, exc);
                        break 'body;
                    }
                }
                _ => {
                    let mut c = self.core.lock();
                    Self::mark(&mut c, g, "voice_connected", Map::new());
                    let sid = c.session_id.clone();
                    self.handle(
                        &mut c,
                        Event::Joined {
                            generation: g,
                            voice_session_id: sid,
                        },
                    );
                }
            }
            if self.core.lock().session_gen != Some(g) {
                break 'body;
            }
            // never cancelled: a stream client cancelled mid-connect would stay registered
            let watch = tokio::spawn(Arc::clone(&self).watch(Arc::clone(&adapter), g, e, stream));
            if echo.is_some() {
                let r = match join_result.take() {
                    Some(r) => r,
                    None => match join.take() {
                        Some(j) => joined(j.await),
                        None => Ok(()),
                    },
                };
                let mut c = self.core.lock();
                match r {
                    Err(exc) => self.connect_join_failed(&mut c, g, &adapter, &exc),
                    Ok(()) => {
                        Self::mark(&mut c, g, "voice_connected", Map::new());
                        let sid = c.session_id.clone();
                        self.handle(
                            &mut c,
                            Event::Joined {
                                generation: g,
                                voice_session_id: sid,
                            },
                        );
                    }
                }
            }
            let _ = watch.await;
        }
        if let Some(j) = join {
            let _ = j.await;
        }
        let _ = spawn.await;
    }

    async fn watch(
        self: Arc<Self>,
        adapter: Arc<dyn Adapter>,
        g: u64,
        e: u64,
        stream: SourceStream,
    ) {
        self.mark_now(g, "watch_sent", Map::new());
        if let Err(exc) = adapter.watch(g, e, stream).await {
            self.handle_now(Event::ConnectFailed {
                generation: g,
                reason: StopReason::ConnectFailed,
                retryable: true,
                detail: exc.to_string(),
                watch_retry: watch_retryable(&exc),
            });
            return;
        }
        let mut c = self.core.lock();
        Self::mark(&mut c, g, "watching", Map::new());
        self.handle(&mut c, Event::Watching { generation: g });
    }

    /// The worker spawn runs off the runtime, concurrently with join and watch.
    async fn spawn_worker(self: Arc<Self>, g: u64) {
        let Some(profile) = self.core.lock().profile.clone() else {
            return;
        };
        let factory = Arc::clone(&self.worker_factory);
        let spawned = {
            let profile = profile.clone();
            tokio::task::spawn_blocking(move || factory(profile))
                .await
                .unwrap_or_else(|e| Err(e.to_string()))
        };
        let worker = match spawned {
            Ok(w) => w,
            Err(err) => {
                let mut c = self.core.lock();
                if c.session_gen == Some(g) {
                    self.handle(
                        &mut c,
                        Event::ConnectFailed {
                            generation: g,
                            reason: StopReason::ConnectFailed,
                            retryable: true,
                            detail: format!("worker: {err}"),
                            watch_retry: false,
                        },
                    );
                }
                return;
            }
        };
        // checked and installed under one lock, so a Teardown cannot slip in between
        let installed = {
            let mut c = self.core.lock();
            if c.session_gen == Some(g) {
                // a RebuildPipeline may have advanced the epoch while spawning
                let e = {
                    let mut h = self.hot.lock();
                    h.worker = Some(Arc::clone(&worker));
                    h.epoch
                };
                worker.send(
                    Configure {
                        generation: g,
                        epoch: e,
                        profile,
                    }
                    .into(),
                );
                // nothing is emitted until the gate opens
                worker.send(
                    SetOutput {
                        generation: g,
                        epoch: e,
                        enabled: false,
                    }
                    .into(),
                );
                Self::mark(&mut c, g, "worker_started", Map::new());
                true
            } else {
                false
            }
        };
        if !installed {
            // torn down while spawning: no Teardown will stop this worker
            let _ = tokio::task::spawn_blocking(move || worker.stop()).await;
        }
    }

    async fn join(
        self: Arc<Self>,
        adapter: Arc<dyn Adapter>,
        on_voice_state: Option<VoiceStateCallback>,
    ) -> Result<(), AdapterError> {
        let sid = adapter.join(on_voice_state).await?;
        let mut c = self.core.lock();
        c.session_id = Some(sid);
        c.in_voice = true;
        c.voice_4006 = 0;
        Ok(())
    }

    /// JoinVoice: the same join a session would do, done before the target streams.
    pub async fn prejoin(&self) {
        let adapter = {
            let c = self.core.lock();
            if c.in_voice {
                return;
            }
            c.adapter.clone()
        };
        let Some(adapter) = adapter else {
            return;
        };
        if let Err(exc) = self.arc().join(Arc::clone(&adapter), None).await {
            // the next StartConnect joins as usual
            self.log(
                "prejoin_failed",
                json!({"error": exc.to_string(), "code": exc.close_code(),
                       "vsu_session": adapter.last_vsu_session()}),
            );
            let mut c = self.core.lock();
            let event = if self.voice_join_failed(&mut c, &adapter, exc.close_code()) {
                // retried after the retry cooldown
                Event::VoiceLost {
                    detail: exc.to_string(),
                    foreign_session: true,
                    from_prejoin: true,
                }
            } else {
                // the Supervisor retries the pre-join after a retry delay
                Event::PrejoinFailed {
                    detail: exc.to_string(),
                }
            };
            self.handle(&mut c, event);
        }
    }

    async fn create_output(self: Arc<Self>, g: u64, worker: Option<Arc<dyn Worker>>) {
        if !same_worker(self.worker().as_ref(), worker.as_ref()) {
            return;
        }
        let result: Result<Created, String> = async {
            let (adapter, p, canvas) = {
                let c = self.core.lock();
                (c.adapter.clone(), c.profile.clone(), c.canvas)
            };
            let adapter = adapter.ok_or("adapter not running")?;
            let p = p.ok_or("no output profile")?;
            let (mut w, mut h) = canvas.ok_or("canvas not locked")?;
            let publish = self.cfg.discord.ingest_publishes;
            let fps = u32::try_from(p.fps).unwrap_or(u32::MAX);
            let kbps = u32::try_from(p.video_bitrate_kbps).unwrap_or(u32::MAX);
            // The slate builds in the background: the output plays without it (a hold freezes
            // until it lands).
            self.spawn_slate(
                g,
                worker.clone(),
                SlateKey {
                    width: w,
                    height: h,
                    fps,
                    keyframe_interval_s: p.keyframe_interval_s,
                    bitrate_kbps: kbps,
                },
            );
            let created = if publish {
                Some(self.create_ingest_output(g, &adapter, fps).await?)
            } else {
                None
            };
            if !same_worker(self.worker().as_ref(), worker.as_ref()) {
                // the Teardown already queued ends the output
                return Ok(Created::Stale);
            }
            // the ingest account sends nothing itself when ingest_publishes is false
            let mut codec = Some("H264".to_string());
            if let Some(created) = created {
                codec = created;
                if self.adaptive {
                    // the publisher drops other codecs until the controller swaps it; an op 14
                    // renegotiation can land during the create: play() needs the codec now
                    codec = adapter.output_codec().or(codec);
                    if let Some(cd) = &codec {
                        self.publisher.set_codec(cd);
                    }
                    (w, h) = Self::video_dims(&self.core.lock()).ok_or("canvas not locked")?;
                }
                adapter
                    .play(
                        Box::new(PublisherSource(Arc::clone(&self.publisher))),
                        VideoParams {
                            width: w,
                            height: h,
                            fps,
                            bitrate: kbps.saturating_mul(1000),
                        },
                    )
                    .map_err(|e| e.to_string())?;
            }
            Ok(Created::Ready(codec))
        }
        .await;
        match result {
            Ok(Created::Stale) => {}
            Ok(Created::Ready(codec)) => self.handle_now(Event::OutputReady {
                generation: g,
                negotiated_video_codec: codec,
            }),
            Err(detail) => self.handle_now(Event::ConnectFailed {
                generation: g,
                reason: StopReason::ConnectFailed,
                retryable: true,
                detail,
                watch_retry: false,
            }),
        }
    }

    fn spawn_slate(&self, g: u64, worker: Option<Arc<dyn Worker>>, key: SlateKey) {
        let guard = track(&self.slate_tasks);
        let s = self.arc();
        tokio::spawn(async move {
            let _guard = guard;
            s.apply_slate(g, worker, key).await;
        });
    }

    async fn apply_slate(&self, g: u64, worker: Option<Arc<dyn Worker>>, key: SlateKey) {
        let slate = match self.get_slate(g, key).await {
            Ok(Some(slate)) => slate,
            Ok(None) => return, // nothing publishes
            Err(e) => {
                self.log("slate_failed", json!({"generation": g, "error": e}));
                return;
            }
        };
        let mut c = self.core.lock();
        if !same_worker(self.worker().as_ref(), worker.as_ref()) {
            return; // the session ended while it was building
        }
        c.slate = Some(Arc::clone(&slate));
        self.publisher.set_slate(Some(Arc::clone(&slate)));
        for p in self.out_pubs.values() {
            // a closed publisher must not send the slate
            if p.has_source() {
                p.set_slate(Some(Arc::clone(&slate)));
            }
        }
    }

    async fn get_slate(&self, g: u64, key: SlateKey) -> Result<Option<Arc<SlateGop>>, String> {
        if !self.cfg.discord.ingest_publishes && self.out_pubs.is_empty() {
            return Ok(None); // no publisher exists that could ever send it
        }
        let t0 = monotonic();
        let cached = {
            let mut c = self.core.lock();
            let hit = c.slates.shift_remove(&key);
            if let Some(slate) = &hit {
                c.slates.insert(key, Arc::clone(slate));
            }
            hit
        };
        let was_cached = cached.is_some();
        let slate = match cached {
            Some(slate) => slate,
            None => {
                let builder = Arc::clone(&self.slate_builder);
                let slate = tokio::task::spawn_blocking(move || builder(&key))
                    .await
                    .map_err(|e| e.to_string())??;
                let mut c = self.core.lock();
                c.slates.insert(key, Arc::clone(&slate));
                if c.slates.len() > SLATE_CACHE_SIZE {
                    c.slates.shift_remove_index(0);
                }
                slate
            }
        };
        let mut extra = Map::new();
        extra.insert("slate_ms".into(), json!(ms(monotonic() - t0)));
        extra.insert("slate_cached".into(), json!(was_cached));
        self.mark_now(g, "slate_ready", extra);
        Ok(Some(slate))
    }

    async fn create_ingest_output(
        &self,
        g: u64,
        adapter: &Arc<dyn Adapter>,
        fps: u32,
    ) -> Result<Option<String>, String> {
        let t0 = monotonic();
        let codec = if self.adaptive {
            adapter.create_output_adaptive(g, fps).await
        } else {
            adapter.create_output(g, fps).await
        }
        .map_err(|e| e.to_string())?;
        let mut extra = Map::new();
        extra.insert("output_create_ms".into(), json!(ms(monotonic() - t0)));
        self.mark_now(g, "output_created", extra);
        Ok(codec)
    }

    /// spec §7 order: end output, stop worker, stop receiving, leave voice.
    async fn teardown(&self, worker: Option<Arc<dyn Worker>>, leave_voice: bool) {
        let adapter = self.core.lock().adapter.clone();
        if let Some(a) = &adapter
            && let Err(e) = a.end_output().await
        {
            self.step_error("end_output", &e);
        }
        if let Some(w) = worker {
            let _ = tokio::task::spawn_blocking(move || w.stop()).await;
        }
        if let Some(a) = &adapter {
            if let Err(e) = a.stop_watch().await {
                self.step_error("stop_watch", &e);
            }
            if leave_voice {
                self.core.lock().in_voice = false;
                if let Err(e) = a.leave().await {
                    self.step_error("leave", &e);
                }
            }
        }
    }

    fn step_error(&self, step: &str, e: &AdapterError) {
        self.log(
            "teardown_step_error",
            json!({"step": step, "error": e.to_string()}),
        );
    }

    async fn leave(&self) {
        // a pre-join queued before this LeaveVoice may have set it again
        let adapter = {
            let mut c = self.core.lock();
            c.in_voice = false;
            c.adapter.clone()
        };
        if let Some(a) = adapter
            && let Err(e) = a.leave().await
        {
            self.step_error("leave", &e);
        }
    }

    async fn keyframe(&self, method: KeyframeMethod) {
        let Some(adapter) = self.core.lock().adapter.clone() else {
            return;
        };
        let (step, result) = match method {
            KeyframeMethod::Pli => ("request_pli", adapter.request_pli().await),
            KeyframeMethod::Rewatch => ("rewatch", adapter.rewatch().await),
        };
        if let Err(e) = result {
            self.step_error(step, &e);
        }
    }

    /// request_pli only sends one RTCP packet, so it never waits behind a slow queued op.
    fn pli(&self, c: &Core) {
        if c.adapter.is_none() {
            return;
        }
        let s = self.arc();
        self.spawn_tracked(async move { s.keyframe(KeyframeMethod::Pli).await });
    }

    fn request_pli_locked(&self, c: &mut Core) {
        let now = monotonic();
        if now - c.last_pli >= self.pli_min_interval {
            c.last_pli = now;
            self.pli(c);
        } else if c.pli_timer.is_none() {
            // inside the rate limit: defer one trailing PLI instead of dropping it
            let delay = (c.last_pli + self.pli_min_interval - now).max(0.0);
            let s = self.arc();
            let timer = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                let mut c = s.core.lock();
                c.pli_timer = None;
                c.last_pli = monotonic();
                s.pli(&c);
            });
            c.pli_timer = Some(timer.abort_handle());
        }
    }

    fn cancel_pli_timer(c: &mut Core) {
        if let Some(t) = c.pli_timer.take() {
            t.abort();
        }
    }

    // ------------------------------------------------------- adaptive codec

    fn reset_codec_ctl(c: &mut Core) {
        c.stall_kick.clear();
        c.codec_ctl = CodecController::new();
    }

    /// Size of the video the outputs send: the source's own while it is passed through, else the
    /// canvas.
    fn video_dims(c: &Core) -> Option<(u32, u32)> {
        c.src_dims.or(c.canvas)
    }

    /// Supervisor events that concern the ingest output: its negotiation and server op 14.
    fn codec_from_event(&self, c: &mut Core, event: &Event) {
        if !self.cfg.discord.ingest_publishes {
            return;
        }
        let current = self.gen_epoch().0;
        match event {
            Event::OutputReady {
                generation,
                negotiated_video_codec: Some(codec),
            } if *generation == current => self.codec_event(
                c,
                CodecEvent::OutputAdded {
                    output_id: OutputId::Ingest,
                    codec: codec.clone(),
                    offer_top: "AV1".into(),
                    bound: Some(codec.clone()),
                },
            ),
            Event::CodecChanged {
                generation,
                negotiated_video_codec: Some(codec),
            } if *generation == current => self.codec_event(
                c,
                CodecEvent::OutputCodec {
                    output_id: OutputId::Ingest,
                    codec: codec.clone(),
                },
            ),
            _ => {}
        }
    }

    fn codec_event(&self, c: &mut Core, event: CodecEvent) {
        self.codec_event_at(c, event, monotonic());
    }

    fn codec_event_at(&self, c: &mut Core, event: CodecEvent, now: f64) {
        if !self.adaptive {
            return;
        }
        // actions queued now are void once the session changes
        let g = c.session_gen;
        let before = c.codec_ctl.effective().map(str::to_owned);
        let actions = match c.codec_ctl.handle(event, now) {
            Ok(actions) => actions,
            Err(e) => {
                self.log("codec_error", json!({"error": e.to_string()}));
                return;
            }
        };
        let after = c.codec_ctl.effective().map(str::to_owned);
        if before != after {
            self.log("codec_change", json!({"old": before, "new": after}));
        }
        for a in actions {
            let s = self.arc();
            match a.clone() {
                CodecAction::UpdateWatch { codecs } => {
                    self.queue("_codec_run", async move {
                        let s2 = Arc::clone(&s);
                        s.codec_run(a, g, async move { s2.codec_update_watch(codecs).await })
                            .await
                    });
                }
                CodecAction::UpdateOutput {
                    output_id: OutputId::Ingest,
                    codecs,
                } => self.ingest_task("_codec_run", async move {
                    let s2 = Arc::clone(&s);
                    s.codec_run(a, g, async move { s2.codec_update_ingest(codecs).await })
                        .await
                }),
                CodecAction::UpdateOutput {
                    output_id: OutputId::Account(uid),
                    codecs,
                } => self.out_queue(uid, "_codec_run", async move {
                    let s2 = Arc::clone(&s);
                    s.codec_run(a, g, async move { s2.codec_update_out(uid, codecs).await })
                        .await
                }),
                CodecAction::SwapOutput {
                    output_id: OutputId::Ingest,
                    codec,
                } => self.ingest_task("_codec_swap", async move {
                    s.codec_swap(OutputId::Ingest, codec, g).await;
                    Ok(())
                }),
                CodecAction::SwapOutput {
                    output_id: OutputId::Account(uid),
                    codec,
                } => self.out_queue(uid, "_codec_swap", async move {
                    s.codec_swap(OutputId::Account(uid), codec, g).await;
                    Ok(())
                }),
            }
        }
    }

    /// An output bound to the current codec that sends nothing while the source flows is
    /// re-swapped (the swap resets the publisher and asks the target for a keyframe).
    fn check_codec_stall_locked(&self, c: &mut Core, now: f64) {
        if !self.adaptive || now - c.last_egress_at > 1.0 {
            return;
        }
        for oid in c.codec_ctl.bound_outputs() {
            let publisher = match oid {
                OutputId::Ingest => Some(&self.publisher),
                OutputId::Account(uid) => self.out_pubs.get(&uid),
            };
            let Some(publisher) = publisher else {
                continue;
            };
            let Some(&kick) = c.stall_kick.get(&oid) else {
                // grace period from when the output is first seen bound
                c.stall_kick.insert(oid, now);
                continue;
            };
            let since = publisher
                .last_send_at()
                .unwrap_or(f64::NEG_INFINITY)
                .max(kick);
            if now - since > CODEC_STALL_S {
                c.stall_kick.insert(oid, now);
                let effective = c.codec_ctl.effective().map(str::to_owned);
                self.log(
                    "codec_stall",
                    json!({"output": output_id_str(oid), "codec": effective}),
                );
                self.codec_event(
                    c,
                    CodecEvent::OutputBound {
                        output_id: oid,
                        codec: None,
                    },
                );
            }
        }
    }

    fn codec_feed(&self, g: Option<u64>, event: CodecEvent) {
        let mut c = self.core.lock();
        if c.session_gen == g {
            self.codec_event(&mut c, event);
        }
    }

    /// Runs one controller action; a no-op or a failure is fed back so the controller emits it
    /// again.
    async fn codec_run<F>(&self, a: CodecAction, g: Option<u64>, fut: F) -> Result<(), String>
    where
        F: Future<Output = Result<bool, String>>,
    {
        if self.core.lock().session_gen != g {
            return Ok(());
        }
        match fut.await {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.codec_feed(g, CodecEvent::ActionFailed(a));
                Ok(())
            }
            Err(e) => {
                self.codec_feed(g, CodecEvent::ActionFailed(a));
                Err(e)
            }
        }
    }

    /// Ingest output ops run one at a time (the pool outputs have their own per-account queue).
    fn ingest_task<F>(&self, name: &'static str, fut: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let s = self.arc();
        self.spawn_tracked(async move {
            let _lock = s.ingest_lock.lock().await;
            if let Err(e) = fut.await {
                s.log("ingest_codec_op_error", json!({"op": name, "error": e}));
            }
        });
    }

    async fn codec_update_watch(&self, names: &'static [&'static str]) -> Result<bool, String> {
        let Some(a) = self.core.lock().adapter.clone() else {
            return Ok(false);
        };
        a.update_watch_codecs(names)
            .await
            .map_err(|e| e.to_string())
    }

    async fn codec_update_ingest(&self, names: &'static [&'static str]) -> Result<bool, String> {
        let Some(a) = self.core.lock().adapter.clone() else {
            return Ok(false);
        };
        a.update_output_codecs(names)
            .await
            .map_err(|e| e.to_string())
    }

    async fn codec_update_out(
        &self,
        uid: u64,
        names: &'static [&'static str],
    ) -> Result<bool, String> {
        let Some(a) = self.core.lock().out_adapters.get(&uid).cloned() else {
            return Ok(false);
        };
        a.update_codecs(names).await.map_err(|e| e.to_string())
    }

    /// Rebinds one output to `codec` in place. The publisher follows inside the adapter's swap, once
    /// the negotiated codec is re-checked under its lock.
    async fn codec_swap(&self, oid: OutputId, codec: String, g: Option<u64>) {
        enum Swap {
            Ingest(Arc<dyn Adapter>),
            Out(Arc<dyn OutputAdapter>),
        }
        let (publisher, swap, ready) = {
            let c = self.core.lock();
            if c.session_gen != g {
                return;
            }
            let (publisher, swap) = match oid {
                OutputId::Ingest => (
                    Arc::clone(&self.publisher),
                    c.adapter.clone().map(Swap::Ingest),
                ),
                OutputId::Account(uid) => {
                    let Some(p) = self.out_pubs.get(&uid) else {
                        return;
                    };
                    (
                        Arc::clone(p),
                        c.out_adapters.get(&uid).cloned().map(Swap::Out),
                    )
                }
            };
            let ready = c.profile.is_some() && Self::video_dims(&c).is_some();
            (publisher, swap, ready)
        };
        let Some(swap) = swap.filter(|_| ready) else {
            self.codec_feed(
                g,
                CodecEvent::OutputBound {
                    output_id: oid,
                    codec: None,
                },
            );
            return;
        };
        let prev = publisher.codec();
        let source_pub = Arc::clone(&publisher);
        let make_source: SourceFactory = Box::new(move |c: VoiceCodec| {
            source_pub.set_codec(c.as_str());
            Box::new(PublisherSource(source_pub)) as Box<dyn FrameSource>
        });
        let bound = match &swap {
            Swap::Ingest(a) => a.swap_output_source(make_source).await,
            Swap::Out(a) => a.swap_source(make_source).await,
        };
        let output = output_id_str(oid);
        let Some(bound) = bound.map(|b| b.as_str().to_string()) else {
            self.log(
                "codec_swap",
                json!({"output": output, "codec": codec, "bound": null}),
            );
            if publisher.codec() != prev {
                publisher.set_codec(&prev);
            }
            self.codec_feed(
                g,
                CodecEvent::OutputBound {
                    output_id: oid,
                    codec: None,
                },
            );
            return;
        };
        self.log(
            "codec_swap",
            json!({"output": output, "codec": codec, "bound": bound}),
        );
        self.codec_feed(
            g,
            CodecEvent::OutputBound {
                output_id: oid,
                codec: Some(bound.clone()),
            },
        );
        if bound != codec {
            // the server renegotiated while we swapped
            self.codec_feed(
                g,
                CodecEvent::OutputCodec {
                    output_id: oid,
                    codec: bound,
                },
            );
        }
    }

    /// Creates the ingest adapter (following the current routing, not the static config) and
    /// logs in.
    pub async fn start_adapter(&self) -> Result<(), String> {
        let (adapter, token) = {
            let mut c = self.core.lock();
            if c.adapter.is_some() || c.shutdown_requested {
                return Ok(());
            }
            let tx = self.events_tx.clone();
            let events: EventSink = Arc::new(move |ev| {
                let _ = tx.send(Msg::Adapter(ev));
            });
            let me = self.me.clone();
            let ingress: IngressSink = Arc::new(move |msg| {
                if let Some(s) = me.upgrade() {
                    s.ingress(msg);
                }
            });
            let adapter = (self.adapter_factory)(events, ingress, Arc::clone(&self.log));
            let r = self.sup.lock().routing();
            adapter.set_routing(r.target_user_id, r.channel_id);
            c.adapter = Some(Arc::clone(&adapter));
            let token = c.token.clone().unwrap_or_else(|| Token::new(""));
            (adapter, token)
        };
        adapter.start(token).await.map_err(|e| e.to_string())
    }

    async fn ensure_adapter(&self) -> Result<(), String> {
        if self.sup.lock().state() != RelayState::Fatal {
            self.start_adapter().await?;
        }
        Ok(())
    }

    pub async fn drop_adapter(&self) {
        let adapter = {
            let mut c = self.core.lock();
            Self::cancel_pli_timer(&mut c);
            c.in_voice = false;
            c.adapter.take()
        };
        if let Some(a) = adapter {
            a.close().await;
        }
    }

    // ------------------------------------------------------- threads / worker

    /// UDP dispatcher thread: forward to the worker for the current (generation, epoch) only.
    fn ingress(&self, msg: Ingress) {
        let (g, e) = match &msg {
            Ingress::Video(m) => (m.generation, m.epoch),
            Ingress::Audio(m) => (m.generation, m.epoch),
            _ => return,
        };
        let worker = {
            let h = self.hot.lock();
            match &h.worker {
                Some(w) if h.generation == g && h.epoch == e => Arc::clone(w),
                _ => return,
            }
        };
        if !worker.send(msg) {
            let _ = self.events_tx.send(Msg::IngressOverflow(g, e));
        }
    }

    fn ingress_overflow(&self, c: &mut Core, g: u64, e: u64) {
        let now = monotonic();
        if (g, e) != self.gen_epoch() || now - c.last_overflow < OVERFLOW_MIN_INTERVAL_S {
            return;
        }
        c.last_overflow = now;
        c.metrics.inc("ingress_overflow", 1);
        // new epoch drops the buffered backlog
        self.handle(
            c,
            Event::WorkerFailed {
                generation: g,
                epoch: e,
                detail: "ingress overflow".into(),
            },
        );
    }

    /// Publisher callback (any thread): feedback IDR, at most one per min interval.
    pub fn need_idr(&self) {
        let now = monotonic();
        let mut h = self.hot.lock();
        let Some(worker) = h.worker.clone() else {
            return;
        };
        if now - h.last_idr_req < self.feedback_min {
            return;
        }
        h.last_idr_req = now;
        worker.send(
            ForceIdr {
                generation: h.generation,
                epoch: h.epoch,
            }
            .into(),
        );
    }

    fn pump_worker(&self, c: &mut Core, worker: &Arc<dyn Worker>) {
        let publishers = self.publishers();
        for item in worker.poll_egress(MAX_EGRESS_POLL) {
            match item {
                Egress::Video(au) => {
                    c.last_egress_at = monotonic();
                    for p in &publishers {
                        p.offer_video(au.clone());
                    }
                }
                Egress::Audio(au) => {
                    for p in &publishers {
                        p.offer_audio(au.clone());
                    }
                }
            }
        }
        for ev in worker.poll_events() {
            self.on_worker_event_locked(c, ev);
        }
        let current = self.worker().is_some_and(|w| Arc::ptr_eq(&w, worker));
        if !worker.alive() && current && !c.worker_dead_reported {
            c.worker_dead_reported = true;
            let (g, e) = self.gen_epoch();
            self.handle(
                c,
                Event::WorkerFailed {
                    generation: g,
                    epoch: e,
                    detail: "worker exited".into(),
                },
            );
        }
    }

    fn publishers(&self) -> Vec<Arc<Publisher>> {
        let mut pubs = Vec::with_capacity(self.out_pubs.len() + 1);
        if self.cfg.discord.ingest_publishes {
            pubs.push(Arc::clone(&self.publisher));
        }
        pubs.extend(self.out_pubs.values().cloned());
        pubs
    }

    fn on_worker_event_locked(&self, c: &mut Core, ev: WorkerEvent) {
        if ev.generation != self.gen_epoch().0 {
            return;
        }
        let dims = |data: &Map<String, Value>| -> Option<(u32, u32)> {
            Some((
                u32::try_from(data.get("width")?.as_u64()?).ok()?,
                u32::try_from(data.get("height")?.as_u64()?).ok()?,
            ))
        };
        match ev.kind {
            WorkerEventKind::FirstFrame => {
                let Some((w, h)) = dims(&ev.data) else {
                    return;
                };
                c.canvas = Some((w, h));
                Self::mark(c, ev.generation, "first_frame", Map::new());
                self.handle(
                    c,
                    Event::FirstFrame {
                        generation: ev.generation,
                        epoch: ev.epoch,
                        width: w,
                        height: h,
                    },
                );
            }
            WorkerEventKind::SourceCodec => {
                let eligible = ev
                    .data
                    .get("eligible")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                c.src_dims = if eligible { dims(&ev.data) } else { None };
                let codec = match ev.data.get("codec") {
                    Some(Value::String(s)) => s.to_uppercase(),
                    Some(v) => v.to_string().to_uppercase(),
                    None => "NONE".into(),
                };
                self.codec_event(c, CodecEvent::SourceCodec { codec, eligible });
            }
            WorkerEventKind::FreshFrame => self.handle(
                c,
                Event::FreshFrame {
                    generation: ev.generation,
                    epoch: ev.epoch,
                },
            ),
            WorkerEventKind::NeedKeyframe => self.request_pli_locked(c),
            WorkerEventKind::Fatal => {
                let reason = match ev.data.get("reason") {
                    Some(Value::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => String::new(),
                };
                self.log("worker_fatal", Value::Object(ev.data));
                self.handle(
                    c,
                    Event::WorkerFailed {
                        generation: ev.generation,
                        epoch: ev.epoch,
                        detail: reason,
                    },
                );
            }
            WorkerEventKind::Stats => {
                for (k, v) in &ev.data {
                    let name = format!("worker.{k}");
                    if let Some(i) = v.as_i64() {
                        c.metrics.gauge_int(&name, i);
                    } else if let Some(f) = v.as_f64() {
                        c.metrics.gauge(&name, f);
                    }
                }
            }
            // decode_error, overflow, source_ineligible
            kind => {
                c.metrics.inc(&format!("worker.{}", kind.as_str()), 1);
                self.log(&format!("worker_{}", kind.as_str()), Value::Object(ev.data));
            }
        }
    }

    // --------------------------------------------------------- status/control

    fn status_dict_locked(&self, c: &mut Core) -> Value {
        let now = monotonic();
        let snap = self.sup.lock().status(wall_clock());
        let mut d = snapshot_json(&snap);
        d.insert("adapter_running".into(), json!(c.adapter.is_some()));
        d.insert("metrics".into(), c.metrics.snapshot());
        let w = self.publisher.take_window();
        d.insert(
            "ingest_send".into(),
            json!({"sent": w.sent, "stalls": w.stalls, "max_send_gap_ms": w.max_send_gap_ms,
                   "audio_played": w.audio_played, "audio_held_reads": w.audio_held_reads,
                   "audio_late_dropped": w.audio_late_dropped, "audio_evicted": w.audio_evicted,
                   "codec": self.publisher.codec()}),
        );
        let audio = c
            .adapter
            .as_ref()
            .map_or_else(|| json!({}), |a| a.take_audio_window());
        let count = |k: &str| audio.get(k).and_then(Value::as_i64).unwrap_or(0);
        c.audio_ok = count("packets") > count("silence");
        d.insert("ingress_audio".into(), audio);
        let routing = self.sup.lock().routing();
        d.insert("routing".into(), routing_json(&routing.to_persisted()));
        d.insert("routing_error".into(), json!(c.routing_error));
        let rows = self.pool.lock().status(now);
        let mut accounts = Vec::new();
        for row in rows {
            let channel = self.pool.lock().channel_of(row.account);
            let mut m = Map::new();
            m.insert("account".into(), json!(row.account));
            m.insert("state".into(), json!(row.state.as_str()));
            m.insert("generation".into(), json!(row.generation));
            m.insert("backoff_in".into(), json!(row.backoff_in));
            m.insert("fatal".into(), json!(row.fatal.map(FatalReason::as_str)));
            m.insert(
                "quarantine_incarnation".into(),
                json!(row.quarantine_incarnation),
            );
            m.insert(
                "consecutive_rejections".into(),
                json!(row.consecutive_rejections),
            );
            m.insert("viewers".into(), json!(row.viewers));
            m.insert("stop_in".into(), json!(row.stop_in));
            m.insert("channel_id".into(), json!(channel));
            if let Some(p) = self.out_pubs.get(&row.account) {
                let w = p.take_window();
                m.insert("sent".into(), json!(w.sent));
                m.insert("stalls".into(), json!(w.stalls));
                m.insert("max_send_gap_ms".into(), json!(w.max_send_gap_ms));
                m.insert("audio_played".into(), json!(w.audio_played));
                m.insert("audio_held_reads".into(), json!(w.audio_held_reads));
                m.insert("audio_late_dropped".into(), json!(w.audio_late_dropped));
                m.insert("audio_evicted".into(), json!(w.audio_evicted));
            }
            if c.network_stats {
                let net = c
                    .out_adapters
                    .get(&row.account)
                    .map_or_else(|| json!({}), |a| a.network_stats());
                m.insert("network".into(), net);
            }
            accounts.push(Value::Object(m));
        }
        if c.network_stats {
            let net = c
                .adapter
                .as_ref()
                .map_or_else(|| json!({}), |a| a.network_stats());
            d.insert("network".into(), net);
        }
        d.insert("accounts".into(), Value::Array(accounts));
        let details = self.status_details_locked(c, now, false);
        let details = details_json(&details, self.cfg.codecs.video_mode.as_str());
        d.insert("codec".into(), details["codec"].clone());
        d.insert("details".into(), details);
        Value::Object(d)
    }

    /// Main-channel members other than the target and the configured bot accounts.
    fn viewers(&self) -> usize {
        let sup = self.sup.lock();
        let mut bots: HashSet<u64> = self
            .cfg
            .output_accounts()
            .iter()
            .map(|a| a.user_id)
            .collect();
        bots.insert(sup.routing().target_user_id);
        bots.insert(self.cfg.discord.expected_relay_user_id);
        sup.participants()
            .map_or(0, |p| p.iter().filter(|u| !bots.contains(u)).count())
    }

    /// Off: every relay account may run. On: the target follows the viewers; extra running relays
    /// stop only once their Go Live has no viewer (see `OutputPool::set_relay_target`).
    fn set_autoscale(&self, c: &mut Core, on: bool) {
        let now = monotonic();
        c.autoscale = on;
        if on {
            self.scale_relays(c, now);
        } else {
            let n = self.cfg.discord.relay_accounts().len();
            let acts = self.pool.lock().set_relay_target(n, now);
            for a in acts {
                self.pool_action(c, a);
            }
        }
    }

    fn scale_relays(&self, c: &mut Core, now: f64) {
        if !c.autoscale {
            return; // the pool keeps its target: every relay account
        }
        let cap = usize::try_from(self.cfg.runtime.stream_viewer_cap).unwrap_or(usize::MAX);
        let want = relays_needed(self.viewers(), cap, self.cfg.discord.ingest_publishes);
        let wanted = self.pool.lock().relay_summary().wanted;
        if want != wanted {
            let acts = self.pool.lock().set_relay_target(want, now);
            for a in acts {
                self.pool_action(c, a);
            }
        }
    }

    fn send_pubs(&self) -> Vec<(SendKey, Arc<Publisher>)> {
        let mut pubs: Vec<(SendKey, Arc<Publisher>)> = self
            .out_pubs
            .iter()
            .map(|(uid, p)| (Some(*uid), Arc::clone(p)))
            .collect();
        if self.cfg.discord.ingest_publishes {
            pubs.push((None, Arc::clone(&self.publisher)));
        }
        pubs
    }

    fn sample_sends(&self, c: &mut Core, now: f64) {
        if now < c.next_send_sample {
            return;
        }
        c.next_send_sample = now + SEND_SAMPLE_S;
        for (key, p) in self.send_pubs() {
            let hist = c.send_hist.entry(key).or_default();
            hist.push_back((now, p.snapshot()));
            while hist.len() > SEND_HISTORY {
                hist.pop_front();
            }
        }
    }

    fn tech_rows(&self, c: &Core, now: f64) -> Vec<TechRow> {
        let live: HashSet<u64> = self
            .pool
            .lock()
            .status(now)
            .iter()
            .filter(|r| r.state == OutputState::Live)
            .map(|r| r.account)
            .collect();
        let mut rows = Vec::new();
        for (key, p) in self.send_pubs() {
            if let Some(uid) = key
                && !live.contains(&uid)
            {
                rows.push(TechRow {
                    uid: Some(uid),
                    rate: None,
                    freezes_recent: 0,
                    freezes_session: 0,
                });
                continue;
            }
            let cur = p.snapshot();
            let hist: Vec<(f64, SendSnapshot)> = c
                .send_hist
                .get(&key)
                .map(|h| h.iter().copied().collect())
                .unwrap_or_default();
            let rate = rates(&hist, now, &cur, RATE_WINDOW_S);
            let session = cur.freezes - c.send_base.get(&key).copied().unwrap_or(cur.freezes);
            let mut recent = freezes_since(&hist, now, &cur, FREEZE_WINDOW_S);
            if c.session_since.is_some() {
                // the window must not reach back into an earlier session
                recent = recent.min(session);
            }
            rows.push(TechRow {
                uid: key,
                rate,
                freezes_recent: u32::try_from(recent).unwrap_or(u32::MAX),
                freezes_session: u32::try_from(session).unwrap_or(u32::MAX),
            });
        }
        rows
    }

    fn status_details_locked(&self, c: &Core, now: f64, verbose: bool) -> StatusDetails {
        let (relay, rows) = {
            let pool = self.pool.lock();
            (pool.relay_summary(), pool.status(now))
        };
        let roles: HashMap<u64, &'static str> = self
            .cfg
            .output_accounts()
            .iter()
            .map(|a| (a.user_id, a.role.as_str()))
            .collect();
        let (w, h, fps) = self.cfg.profile_key();
        let accounts = rows
            .iter()
            .map(|row| {
                let since = (row.state == OutputState::Live)
                    .then(|| c.out_since.get(&row.account).copied())
                    .flatten();
                AccountStatus {
                    uid: row.account,
                    role: roles.get(&row.account).copied().unwrap_or("relay").into(),
                    channel_id: self.pool.lock().channel_of(row.account),
                    state: row.state.as_str().into(),
                    uptime_s: since.map(|s| now - s),
                    viewers: row.viewers.map(|v| u32::try_from(v).unwrap_or(u32::MAX)),
                    stop_in: row.stop_in,
                }
            })
            .collect();
        let (state, prejoin, fatal) = {
            let sup = self.sup.lock();
            (sup.state(), sup.prejoin_voice(), sup.fatal())
        };
        let to32 = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        let ctl = &self.cfg.control;
        let gap_frames = (verbose && c.session_since.is_some()).then(|| {
            let snap = c.metrics.snapshot();
            snap["gauges"]
                .get("worker.gap_frames")
                .and_then(Value::as_f64)
                .map_or(0, |v| v as u32)
        });
        StatusDetails {
            session_state: state.as_str().into(),
            session_uptime_s: c.session_since.map(|s| now - s),
            viewers: to32(self.viewers()),
            relay_wanted: to32(relay.wanted),
            relay_running: to32(relay.running),
            relay_available: to32(relay.available),
            relay_short_by: to32(relay.short_by),
            relay_kept: to32(relay.kept),
            prejoin,
            autoscale: c.autoscale,
            debounce_ms: u32::try_from(self.cfg.runtime.start_debounce_ms).unwrap_or(u32::MAX),
            audio_mode: self.cfg.codecs.audio_mode.as_str().into(),
            output_size: format!("{w}x{h}@{fps}"),
            last_watch_ms: c.last_watch_ms,
            audio_ok: c.audio_ok,
            accounts,
            public_commands: COMMAND_NAMES
                .iter()
                .filter(|n| ctl.public_commands.contains(**n))
                .map(|n| n.to_string())
                .collect(),
            command_cooldown_s: ctl.command_cooldown_seconds,
            change_cooldown_s: ctl.change_cooldown_seconds,
            viewer_cap: u32::try_from(self.cfg.runtime.stream_viewer_cap).unwrap_or(u32::MAX),
            ingest_publishes: self.cfg.discord.ingest_publishes,
            fatal_reason: fatal.map(|f| f.as_str().into()),
            relay_kept_ids: relay.kept_ids,
            codec: if self.adaptive {
                c.codec_ctl.effective().map(str::to_owned)
            } else {
                Some("H264".into())
            },
            codec_switch: if self.adaptive {
                c.codec_ctl
                    .switching()
                    .map(|(a, b)| (a.to_owned(), b.to_owned()))
            } else {
                None
            },
            tech: verbose.then(|| self.tech_rows(c, now)),
            gap_frames,
        }
    }

    fn write_status(&self, c: &mut Core) {
        let status = self.status_dict_locked(c);
        match write_status(&self.cfg.storage.state_directory, &status) {
            Ok(()) => *self.status_written.lock() = Some((monotonic(), status)),
            Err(e) => self.log("status_write_failed", json!({"error": e.to_string()})),
        }
    }

    /// `/healthz`: the service has not finished and wrote status.json in the last
    /// [`HEALTH_MAX_AGE_S`] (the ticker writes it every [`STATUS_S`]).
    pub fn healthy(&self, now: f64) -> bool {
        !*self.done.borrow()
            && self
                .status_written
                .lock()
                .as_ref()
                .is_some_and(|(at, _)| now - at <= HEALTH_MAX_AGE_S)
    }

    /// `/metrics`: the last status.json as Prometheus text.
    pub fn metrics_text(&self, now: f64) -> String {
        let up = self.healthy(now);
        let last = self.status_written.lock();
        metrics::render(
            last.as_ref().map(|(_, status)| status),
            last.as_ref().map(|(at, _)| now - at),
            up,
        )
    }

    /// The `--metrics-listen` endpoints; they hold the service weakly.
    pub fn metrics_endpoints(self: &Arc<Self>) -> Endpoints {
        let (h, m) = (Arc::downgrade(self), Arc::downgrade(self));
        Endpoints {
            healthy: Box::new(move || h.upgrade().is_some_and(|s| s.healthy(monotonic()))),
            metrics: Box::new(move || {
                m.upgrade().map_or_else(
                    || metrics::render(None, None, false),
                    |s| s.metrics_text(monotonic()),
                )
            }),
        }
    }

    /// One control-socket request (`{"cmd": ..., "_peer_uid": ...}`) and its reply.
    pub fn control(&self, req: &Value) -> Value {
        let cmd = req.get("cmd").and_then(Value::as_str);
        let shown = cmd.map_or_else(|| "None".to_string(), |c| format!("'{c}'"));
        let peer = req.get("_peer_uid").and_then(Value::as_u64);
        match cmd {
            Some("status") => json!({"ok": true, "status": self.status_dict()}),
            Some(c @ ("enable" | "reset-fatal")) if !(self.peer_is_operator)(peer) => {
                json!({"ok": false, "error": format!("{c} is operator-only")})
            }
            Some(c @ ("enable" | "disable")) => {
                let enabled = c == "enable";
                let mut core = self.core.lock();
                match self.set_enabled(&mut core, enabled) {
                    Ok(st) => {
                        json!({"ok": true, "revision": st.revision, "desired_enabled": enabled})
                    }
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            }
            Some("reset-fatal") if req.get("account").is_some_and(|a| !a.is_null()) => {
                let account = req.get("account").expect("checked");
                let uid = account
                    .as_u64()
                    .or_else(|| account.as_str().and_then(|s| s.trim().parse().ok()));
                let Some(uid) = uid.filter(|u| self.out_pubs.contains_key(u)) else {
                    return json!({"ok": false,
                                  "error": format!("unknown output account: {}",
                                                   account.as_str().map_or_else(|| account.to_string(), str::to_owned))});
                };
                let latched = match self.store.load() {
                    Ok(st) => account_view(&st, uid).fatal_reason.is_some(),
                    Err(e) => return json!({"ok": false, "error": e.to_string()}),
                };
                if !latched {
                    return json!({"ok": true, "message": "no fatal latch set"});
                }
                let cleared = self.store.update(|s| {
                    let Some(rec) = s.accounts.get(&uid.to_string()) else {
                        return s;
                    };
                    let Some(latch) = &rec.fatal_latch else {
                        return s;
                    };
                    let zero = latch.get("reason").and_then(Value::as_str)
                        == Some(FatalReason::RelayRejectedByServer.as_str());
                    set_account(s, uid, |r| {
                        r.fatal_latch = None;
                        if zero {
                            r.consecutive_rejections = 0;
                        }
                    })
                });
                match cleared {
                    Ok(_) => json!({"ok": true,
                        "message": "latch cleared; the account logs in again after a service restart"}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            }
            Some("reset-fatal") => {
                if !self.sup.lock().fatal().is_some_and(FatalReason::is_durable) {
                    return json!({"ok": true, "message": "no fatal latch set"});
                }
                self.handle_now(Event::ResetFatal);
                json!({"ok": true})
            }
            _ => json!({"ok": false, "error": format!("unknown command: {shown}")}),
        }
    }

    fn set_enabled(&self, c: &mut Core, enabled: bool) -> Result<PersistedState, StateError> {
        let st = self.store.update(|mut s| {
            s.desired_enabled = enabled;
            s
        })?;
        self.handle(c, Event::EnableChanged { enabled });
        Ok(st)
    }

    // ------------------------------------------------------------- test hooks
    //
    // White-box access the Python tests had through attributes; not a supported interface.

    #[doc(hidden)]
    pub fn status_dict(&self) -> Value {
        let mut c = self.core.lock();
        self.status_dict_locked(&mut c)
    }

    #[doc(hidden)]
    pub fn status_details(&self, now: f64, verbose: bool) -> StatusDetails {
        let c = self.core.lock();
        self.status_details_locked(&c, now, verbose)
    }

    #[doc(hidden)]
    pub fn with_sup<R>(&self, f: impl FnOnce(&Supervisor) -> R) -> R {
        f(&self.sup.lock())
    }

    #[doc(hidden)]
    pub fn with_pool<R>(&self, f: impl FnOnce(&OutputPool) -> R) -> R {
        f(&self.pool.lock())
    }

    #[doc(hidden)]
    pub fn with_codec<R>(&self, f: impl FnOnce(&CodecController) -> R) -> R {
        f(&self.core.lock().codec_ctl)
    }

    #[doc(hidden)]
    pub fn current_ids(&self) -> (u64, u64) {
        self.gen_epoch()
    }

    #[doc(hidden)]
    pub fn publisher(&self) -> Arc<Publisher> {
        Arc::clone(&self.publisher)
    }

    #[doc(hidden)]
    pub fn out_publisher(&self, uid: u64) -> Option<Arc<Publisher>> {
        self.out_pubs.get(&uid).cloned()
    }

    #[doc(hidden)]
    pub fn out_publisher_ids(&self) -> Vec<u64> {
        self.out_pubs.keys().copied().collect()
    }

    #[doc(hidden)]
    pub fn in_voice(&self) -> bool {
        self.core.lock().in_voice
    }

    #[doc(hidden)]
    pub fn set_in_voice(&self, on: bool) {
        self.core.lock().in_voice = on;
    }

    #[doc(hidden)]
    pub fn voice_4006(&self) -> u32 {
        self.core.lock().voice_4006
    }

    #[doc(hidden)]
    pub fn chat_ops(&self) -> BTreeSet<u64> {
        self.core.lock().chat_ops.clone()
    }

    #[doc(hidden)]
    pub fn slate(&self) -> Option<Arc<SlateGop>> {
        self.core.lock().slate.clone()
    }

    #[doc(hidden)]
    pub fn slate_tasks(&self) -> usize {
        self.slate_tasks.count.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn metrics(&self) -> Value {
        self.core.lock().metrics.snapshot()
    }

    #[doc(hidden)]
    pub fn set_network_stats(&self, on: bool) {
        self.core.lock().network_stats = on;
    }

    #[doc(hidden)]
    pub fn set_adapter(&self, adapter: Option<Arc<dyn Adapter>>) {
        self.core.lock().adapter = adapter;
    }

    #[doc(hidden)]
    pub fn has_worker(&self) -> bool {
        self.hot.lock().worker.is_some()
    }

    #[doc(hidden)]
    pub fn set_worker(&self, worker: Option<Arc<dyn Worker>>) {
        self.hot.lock().worker = worker;
    }

    #[doc(hidden)]
    pub fn set_chat_cooldowns(&self, author_s: f64, change_s: f64, clock: Clock) {
        let mut c = self.core.lock();
        c.cooldowns = ChatCooldowns::new(author_s, change_s);
        c.chat_clock = clock;
    }

    /// Waits until every event queued before this call was handled (needs `run`).
    #[doc(hidden)]
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.events_tx.send(Msg::Flush(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    #[doc(hidden)]
    pub fn dispatch(&self, ev: AdapterEvent) {
        let mut c = self.core.lock();
        self.dispatch_locked(&mut c, ev);
    }

    #[doc(hidden)]
    pub fn execute(&self, acts: Vec<Action>) {
        let mut c = self.core.lock();
        self.execute_locked(&mut c, acts);
    }

    #[doc(hidden)]
    pub fn pool_event_hook(&self, ev: PoolEvent) {
        self.pool_event_now(ev);
    }

    #[doc(hidden)]
    pub fn on_worker_event(&self, ev: WorkerEvent) {
        let mut c = self.core.lock();
        self.on_worker_event_locked(&mut c, ev);
    }

    #[doc(hidden)]
    pub fn request_pli(&self) {
        let mut c = self.core.lock();
        self.request_pli_locked(&mut c);
    }

    #[doc(hidden)]
    pub fn pli_timer_pending(&self) -> bool {
        self.core.lock().pli_timer.is_some()
    }

    #[doc(hidden)]
    pub fn cancel_pli_timer_hook(&self) {
        Self::cancel_pli_timer(&mut self.core.lock());
    }

    #[doc(hidden)]
    pub fn queue_op<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.queue("test_op", async move {
            fut.await;
            Ok(())
        });
    }

    #[doc(hidden)]
    pub fn last_idr_req(&self) -> f64 {
        self.hot.lock().last_idr_req
    }

    #[doc(hidden)]
    pub fn set_last_idr_req(&self, t: f64) {
        self.hot.lock().last_idr_req = t;
    }

    /// The effective codec right after the event, read under the same lock: the service's own
    /// `Tick` (real clock) may change it again at any time.
    #[doc(hidden)]
    pub fn codec_event_hook(&self, ev: CodecEvent, now: f64) -> Option<String> {
        let mut c = self.core.lock();
        self.codec_event_at(&mut c, ev, now);
        c.codec_ctl.effective().map(str::to_owned)
    }

    #[doc(hidden)]
    pub fn check_codec_stall(&self, now: f64) {
        let mut c = self.core.lock();
        self.check_codec_stall_locked(&mut c, now);
    }

    #[doc(hidden)]
    pub fn set_last_egress_at(&self, t: f64) {
        self.core.lock().last_egress_at = t;
    }

    #[doc(hidden)]
    pub fn clear_stall_kick(&self) {
        self.core.lock().stall_kick.clear();
    }

    /// Logs a session_timing for a generation that never sent a watch, keeping the current timing.
    #[doc(hidden)]
    pub fn log_unwatched_timing(&self, g: u64) {
        let mut c = self.core.lock();
        let saved = c.timing.replace(Timing {
            generation: g,
            start: 0.0,
            seen: None,
            gate_ready: None,
            prejoined: false,
            marks: IndexMap::new(),
            extra: Map::new(),
        });
        self.log_timing(&mut c, g, "test");
        c.timing = saved;
    }
}

/// Moves a `JoinHandle` result of the join task into the adapter's error type.
fn joined(r: Result<Result<(), AdapterError>, tokio::task::JoinError>) -> Result<(), AdapterError> {
    r.unwrap_or_else(|e| Err(AdapterError::failed(format!("join task failed: {e}"))))
}

fn snapshot_json(s: &StatusSnapshot) -> Map<String, Value> {
    let mut d = Map::new();
    d.insert("state".into(), json!(s.state.as_str()));
    d.insert("desired_enabled".into(), json!(s.desired_enabled));
    d.insert("fatal".into(), json!(s.fatal));
    d.insert("hold_reason".into(), json!(s.hold_reason));
    d.insert("hold_deadline".into(), json!(s.hold_deadline));
    d.insert("backoff_until".into(), json!(s.backoff_until));
    d.insert("generation".into(), json!(s.generation));
    d.insert("epoch".into(), json!(s.epoch));
    d.insert("source_incarnation".into(), json!(s.source_incarnation));
    d.insert("gateway".into(), json!(s.gateway));
    d.insert(
        "quarantine".into(),
        s.quarantine
            .as_ref()
            .map_or(Value::Null, |q| Value::Object(quarantine_json(q))),
    );
    d.insert(
        "consecutive_rejections".into(),
        json!(s.consecutive_rejections),
    );
    d.insert(
        "voice_session_baseline".into(),
        json!(s.voice_session_baseline),
    );
    d.insert(
        "last_source_progress_age".into(),
        json!(s.last_source_progress_age),
    );
    d.insert("gate_failed".into(), json!(s.gate_failed));
    d
}

/// Python `asdict(StatusDetails)`: tuples become lists. `video_mode` is no longer a field of the
/// Rust struct but stays in status.json.
fn details_json(d: &StatusDetails, video_mode: &str) -> Value {
    let accounts: Vec<Value> = d
        .accounts
        .iter()
        .map(|a| {
            json!([
                a.uid,
                a.role,
                a.channel_id,
                a.state,
                a.uptime_s,
                a.viewers,
                a.stop_in
            ])
        })
        .collect();
    let tech = d.tech.as_ref().map(|rows| {
        rows.iter()
            .map(|r| {
                let (kbps, fps) = r.rate.map_or((None, None), |(k, f)| (Some(k), Some(f)));
                json!([r.uid, kbps, fps, r.freezes_recent, r.freezes_session])
            })
            .collect::<Vec<_>>()
    });
    json!({
        "session_state": d.session_state,
        "session_uptime_s": d.session_uptime_s,
        "viewers": d.viewers,
        "relay_wanted": d.relay_wanted,
        "relay_running": d.relay_running,
        "relay_available": d.relay_available,
        "relay_short_by": d.relay_short_by,
        "relay_kept": d.relay_kept,
        "prejoin": d.prejoin,
        "autoscale": d.autoscale,
        "debounce_ms": d.debounce_ms,
        "video_mode": video_mode,
        "audio_mode": d.audio_mode,
        "output_size": d.output_size,
        "last_watch_ms": d.last_watch_ms,
        "audio_ok": d.audio_ok,
        "accounts": accounts,
        "public_commands": d.public_commands,
        "command_cooldown_s": d.command_cooldown_s,
        "change_cooldown_s": d.change_cooldown_s,
        "viewer_cap": d.viewer_cap,
        "ingest_publishes": d.ingest_publishes,
        "fatal_reason": d.fatal_reason,
        "relay_kept_ids": d.relay_kept_ids,
        "codec": d.codec,
        "codec_switch": d.codec_switch.as_ref().map(|(a, b)| json!([a, b])),
        "tech": tech,
        "gap_frames": d.gap_frames,
    })
}

async fn wait_signals(svc: Weak<Service>) {
    loop {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    if let Some(s) = svc.upgrade() {
                        s.request_shutdown();
                    }
                    continue;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(windows)]
        {
            // Ctrl+Break and console close are how a Windows supervisor stops a console process
            // (Ctrl+C cannot be sent to another process group).
            use tokio::signal::windows::{ctrl_break, ctrl_close};
            let (Ok(mut brk), Ok(mut close)) = (ctrl_break(), ctrl_close()) else {
                return;
            };
            tokio::select! {
                r = tokio::signal::ctrl_c() => if r.is_err() { return; },
                _ = brk.recv() => {}
                _ = close.recv() => {}
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
        }
        match svc.upgrade() {
            Some(s) => s.request_shutdown(),
            None => return,
        }
    }
}

/// `relayctl` exit code for an unusable config, credential or state (`cli.py::EXIT_CONFIG`).
pub const EXIT_CONFIG: i32 = 78;

/// Entry used by `relayctl run`: one instance per relay account. Core dumps are left to the unit
/// (`LimitCORE=0`): `setrlimit` needs `unsafe` libc calls, which the workspace forbids.
///
/// Exit codes follow `cli.py::_run`: a held instance lock is 1; an unreadable output token or a
/// state the service cannot start from is 78. `metrics_listen` serves `/healthz` and `/metrics`
/// there; an address that cannot be bound is 1.
pub async fn run_service(
    cfg: RelayConfig,
    token: Token,
    metrics_listen: Option<SocketAddr>,
) -> i32 {
    let accounts = cfg.output_accounts();
    let mut out_tokens = HashMap::new();
    let mut secrets = vec![token.expose().to_string()];
    for a in &accounts {
        match load_token(&a.credential) {
            Ok(t) => {
                secrets.push(t.clone());
                out_tokens.insert(a.user_id, Token::new(t));
            }
            Err(e) => {
                let logger = stderr_logger(&secrets);
                logger.event(
                    "output_token_failed",
                    [
                        ("account", json!(a.user_id)),
                        ("error", json!(e.to_string())),
                    ],
                );
                return EXIT_CONFIG;
            }
        }
    }
    let logger = stderr_logger(&secrets);
    let log: LogFn = Arc::new(move |name: &str, fields: Value| match fields {
        Value::Object(m) => logger.event(name, m),
        other => logger.event(name, [("value", other)]),
    });
    let store = StateStore::new(&cfg.storage.state_directory);
    let adaptive = cfg.codecs.output_codec == OutputCodec::Adaptive;
    if adaptive {
        log(
            "adaptive_codec",
            json!({"note": "codecs.watch_codecs and codecs.parent_codecs are ignored"}),
        );
    }
    let ids = std::iter::once(cfg.discord.expected_relay_user_id)
        .chain(accounts.iter().map(|a| a.user_id));
    let _locks = match InstanceLocks::acquire(ids, None) {
        Ok(l) => l,
        Err(e) => {
            log("instance_locked", json!({"error": e.to_string()}));
            return 1;
        }
    };
    let adapter_cfg = cfg.clone();
    let adapter_factory: AdapterFactory = Box::new(move |events, ingress, lg| {
        Arc::new(DiscordAdapter::new(
            AdapterOptions::from_config(&adapter_cfg),
            events,
            ingress,
            lg,
        )) as Arc<dyn Adapter>
    });
    let out_cfg = cfg.clone();
    let out_factory: OutputAdapterFactory = Box::new(move |acct, events, lg| {
        Arc::new(OutputAccountAdapter::new(
            OutputOptions::from_config(&out_cfg, acct),
            events,
            lg,
        )) as Arc<dyn OutputAdapter>
    });
    let worker_factory: WorkerFactory = Arc::new(|profile| {
        WorkerHandle::start(profile)
            .map(|h| Arc::new(ThreadWorker::new(h)) as Arc<dyn Worker>)
            .map_err(|e| e.to_string())
    });
    let opts = ServiceOptions {
        output_adapter_factory: Some(out_factory),
        output_tokens: out_tokens,
        ..ServiceOptions::default()
    };
    let svc = match Service::new(
        cfg,
        store,
        adapter_factory,
        worker_factory,
        Arc::clone(&log),
        opts,
    ) {
        Ok(s) => s,
        Err(e) => {
            log("service_start_failed", json!({"error": e.to_string()}));
            return EXIT_CONFIG;
        }
    };
    let mut metrics_task = None;
    if let Some(addr) = metrics_listen {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                let address = listener.local_addr().unwrap_or(addr).to_string();
                log("metrics_listening", json!({"address": address}));
                let endpoints = Arc::new(svc.metrics_endpoints());
                metrics_task = Some(tokio::spawn(metrics::serve(listener, endpoints)));
            }
            Err(e) => {
                log(
                    "metrics_listen_failed",
                    json!({"address": addr.to_string(), "error": e.to_string()}),
                );
                return 1;
            }
        }
    }
    let code = svc.run(token).await;
    if let Some(task) = metrics_task {
        task.abort();
    }
    code
}

fn stderr_logger(secrets: &[String]) -> JsonLogger {
    let writer: Arc<std::sync::Mutex<dyn std::io::Write + Send>> =
        Arc::new(std::sync::Mutex::new(std::io::stderr()));
    JsonLogger::new(writer, secrets)
}
