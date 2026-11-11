//! Discord adapter: gateway events to model events, voice/watch/output calls. Port of
//! `adapter/discord_adapter.py`.
//!
//! `handle_dispatch` is synchronous and runs in gateway delivery order (the gateway task applies a
//! dispatch to the cache and then delivers it), so one ordered event stream comes out;
//! `RESUMED` -> `FRESH` is therefore emitted after every earlier event (the replay fence, spec
//! section 13). Voice and stream clients never reconnect on their own (the Python code passes
//! `reconnect=False`); the gateway RESUME stays with `Gateway`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use relay_core::clock::{monotonic, wall_clock};
use relay_core::config::{CodecPolicy, ControlConfig, OutputCodec, RelayConfig};
use relay_core::control::commands::ChatCommand;
use relay_core::ingress::{GapTracker, rtcp_to_mono};
use relay_core::supervisor::model::{
    Event, GatewayState, RelayVoiceObserved, SourceStream, TargetVoice,
};
use relay_core::supervisor::outputs_model::PoolEvent;
use relay_core::worker::messages::Ingress;
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;
use relay_voice::connection::RtcSettings;
use relay_voice::publish::VideoParams;
use relay_voice::receive::{MediaType, Packet, RtpObservation};
use relay_voice::voice_ws::{StreamSpec, video_codec_list};
use relay_voice::{ClientEvent, JoinOptions, StreamClient, VoiceClient, VoiceClientConfig};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use crate::api::{
    Adapter, AdapterError, AdapterEvent, ApiFuture, BoxFuture, EventSink, IngressSink, LogFn,
    SourceFactory, VoiceStateCallback,
};
use crate::cache::{Cache, CacheChange, SharedCache, StreamKey, StreamState, VoiceStateEntry};
use crate::codecs::{
    ALL_VIDEO_CODECS, adaptive_decode_codecs, decode_codecs, output_settings, parse_names,
};
use crate::ingress::{AudioStats, ingress_audio, ingress_video};
use crate::link::{link_event, snowflake};
use crate::net::Net;
use crate::twcc::{TWCC_INTERVAL_S, TwccFeedback, TwccSender, transport_seq};
use crate::{Dispatch, GatewayEvent, RestClient, Token, Urls};

/// 4 per second, per media type.
pub const PROGRESS_MIN_INTERVAL_S: f64 = 0.25;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_CLOSE_WAIT: Duration = Duration::from_secs(5);
/// `request_video(ssrc, quality=100)` for the target's first video SSRC.
const REQUEST_QUALITY: u32 = 100;

/// What the adapter needs from the config (`cfg.discord`, `control`, `codecs`, `runtime`,
/// `logging`) plus the knobs the tests turn.
#[derive(Clone)]
pub struct AdapterOptions {
    pub guild_id: u64,
    pub target_id: u64,
    pub channel_id: u64,
    pub relay_id: u64,
    /// Ids of the extra output accounts: their messages are never commands.
    pub output_account_ids: Vec<u64>,
    pub control: ControlConfig,
    /// `false` reports `SourceStream.paused` as `None` (the capability is not verified).
    pub paused_verified: bool,
    /// `codecs.output_codec: adaptive`: every codec offered, the codec controller narrows it.
    pub adaptive: bool,
    /// Decode lists of the parent voice connection and of the watch (`parent_codecs`,
    /// `watch_codecs`); ignored when `adaptive`.
    pub parent_codecs: Vec<VoiceCodec>,
    pub watch_codecs: Vec<VoiceCodec>,
    pub network_stats: bool,
    pub self_deaf: bool,
    pub urls: Urls,
    pub connect_timeout: Duration,
}

impl AdapterOptions {
    /// Adaptive mode offers every codec and ignores `parent_codecs` / `watch_codecs`; with
    /// `codecs.output_codec: H264` the policies are always `h264_only` (see
    /// `CodecsConfig::effective_*`). The ingest account's `self_deaf` is `runtime.self_deaf_ingest`.
    pub fn from_config(cfg: &RelayConfig) -> Self {
        let adaptive = cfg.codecs.output_codec == OutputCodec::Adaptive;
        let list = |policy: CodecPolicy| match policy {
            CodecPolicy::AutoDecode => ALL_VIDEO_CODECS.to_vec(),
            CodecPolicy::H264Only if !adaptive => vec![VoiceCodec::H264],
            CodecPolicy::H264Only => ALL_VIDEO_CODECS.to_vec(),
        };
        let d = &cfg.discord;
        Self {
            guild_id: d.guild_id,
            target_id: d.target_user_id,
            channel_id: d.allowed_voice_channel_id,
            relay_id: d.expected_relay_user_id,
            output_account_ids: d.output_accounts.iter().map(|a| a.user_id).collect(),
            control: cfg.control.clone(),
            paused_verified: false,
            adaptive,
            parent_codecs: list(cfg.codecs.effective_parent_codecs()),
            watch_codecs: list(cfg.codecs.effective_watch_codecs()),
            network_stats: cfg.logging.network_stats,
            self_deaf: cfg.runtime.self_deaf_ingest,
            urls: Urls::default(),
            connect_timeout: CONNECT_TIMEOUT,
        }
    }
}

#[derive(Default)]
struct State {
    incarnation: i64,
    stream: Option<SourceStream>,
    watch_args: Option<(u64, SourceStream)>,
    vc: Option<VoiceClient>,
    /// The client being disconnected: it is no longer `vc` (nothing observes it any more) but still
    /// needs the gateway's voice-state echo that completes its leave.
    leaving: Option<VoiceClient>,
    vc_pump: Option<JoinHandle<()>>,
    sc: Option<StreamClient>,
    twcc_task: Option<JoinHandle<()>>,
    out: Option<StreamClient>,
    /// Stream key and generation of an adaptive output: op 14 of that stream becomes `CodecChanged`.
    codec_hook: Option<(String, u64)>,
    last_vsu_session: Option<String>,
    pump: Option<JoinHandle<()>>,
}

/// The receive path's state, touched on the UDP dispatcher thread for every packet.
struct Hot {
    generation: u64,
    epoch: u64,
    video_ssrc: Option<u32>,
    last_seq: HashMap<u32, u16>,
    gaps: GapTracker,
    /// Per media type: audio (0), video (1).
    last_progress: [f64; 2],
    /// The watch connection (video requests and receive-state cleanup on a codec switch).
    sc: Option<StreamClient>,
}

struct Audio {
    stats: AudioStats,
    window_start: f64,
}

struct Inner {
    opts: AdapterOptions,
    events: EventSink,
    ingress: IngressSink,
    log: LogFn,
    cache: SharedCache,
    relay_ids: HashSet<u64>,
    target: AtomicU64,
    channel: AtomicU64,
    net: Mutex<Option<Net>>,
    st: Mutex<State>,
    hot: Mutex<Hot>,
    audio: Mutex<Audio>,
    twcc: TwccSender,
}

pub struct DiscordAdapter {
    inner: Arc<Inner>,
}

fn model(event: Event) -> AdapterEvent {
    AdapterEvent::Model(event)
}

impl DiscordAdapter {
    pub fn new(opts: AdapterOptions, events: EventSink, ingress: IngressSink, log: LogFn) -> Self {
        Self::with_cache(opts, Cache::shared(), events, ingress, log)
    }

    /// Reads `cache` instead of a fresh one; the gateway started by `start` applies to it.
    pub fn with_cache(
        opts: AdapterOptions,
        cache: SharedCache,
        events: EventSink,
        ingress: IngressSink,
        log: LogFn,
    ) -> Self {
        let relay_ids = std::iter::once(opts.relay_id)
            .chain(opts.output_account_ids.iter().copied())
            .collect();
        let inner = Inner {
            target: AtomicU64::new(opts.target_id),
            channel: AtomicU64::new(opts.channel_id),
            opts,
            events,
            ingress,
            log,
            cache,
            relay_ids,
            net: Mutex::new(None),
            st: Mutex::new(State::default()),
            hot: Mutex::new(Hot {
                generation: 0,
                epoch: 0,
                video_ssrc: None,
                last_seq: HashMap::new(),
                gaps: GapTracker::default(),
                last_progress: [f64::NEG_INFINITY; 2],
                sc: None,
            }),
            audio: Mutex::new(Audio {
                stats: AudioStats::default(),
                window_start: monotonic(),
            }),
            twcc: TwccSender::new(),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Translates one gateway dispatch into zero or more model events, in order (the cache has
    /// already applied it).
    pub fn handle_dispatch(&self, dispatch: &Dispatch) {
        self.inner.handle_dispatch(dispatch);
    }

    /// The gateway task ended for good (`_run_gateway` after `connect` raised).
    pub fn handle_gateway_closed(&self, error: &crate::Error) {
        self.inner.on_gateway_closed(error);
    }

    #[cfg(test)]
    fn attach_detached(&self, net: Net) {
        *self.inner.net.lock() = Some(net);
    }
}

impl Inner {
    fn emit(&self, event: AdapterEvent) {
        (self.events)(event);
    }

    fn emit_all(&self, events: Vec<AdapterEvent>) {
        for event in events {
            self.emit(event);
        }
    }

    fn log(&self, name: &str, fields: Value) {
        (self.log)(name, fields);
    }

    fn target_id(&self) -> u64 {
        self.target.load(Ordering::Relaxed)
    }

    fn channel_id(&self) -> u64 {
        self.channel.load(Ordering::Relaxed)
    }

    fn net(&self) -> Option<Net> {
        self.net.lock().clone()
    }

    fn need_net(&self) -> Result<Net, AdapterError> {
        self.net()
            .ok_or_else(|| AdapterError::failed("adapter not started"))
    }

    // ------------------------------------------------------------ lifecycle

    async fn start(self: &Arc<Self>, token: Token) -> Result<(), AdapterError> {
        let rest = RestClient::new(self.opts.urls.clone(), token).await?;
        match rest.login().await {
            Ok(_) => {}
            Err(crate::Error::LoginFailed) => {
                self.emit(model(Event::AuthRejected));
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
        let (net, events) = Net::attach(rest, Arc::clone(&self.cache));
        *self.net.lock() = Some(net);
        let pump = tokio::spawn(run_gateway(Arc::downgrade(self), events));
        self.st.lock().pump = Some(pump);
        Ok(())
    }

    fn on_gateway_event(&self, event: GatewayEvent) {
        match event {
            GatewayEvent::Dispatch(dispatch) => self.handle_dispatch(&dispatch),
            GatewayEvent::Disconnected { .. } => self.emit(model(Event::GatewayChanged {
                state: GatewayState::Resuming,
            })),
            GatewayEvent::Closed(error) => self.on_gateway_closed(&error),
        }
    }

    fn on_gateway_closed(&self, error: &crate::Error) {
        self.log("gateway_ended", json!({"error": error.to_string()}));
        if matches!(error, crate::Error::LoginFailed) {
            self.emit(model(Event::AuthRejected));
        } else {
            self.emit(model(Event::GatewayChanged {
                state: GatewayState::Invalid,
            }));
        }
    }

    async fn close(self: &Arc<Self>) {
        let steps: [(&str, Result<(), AdapterError>); 3] = [
            ("end_output", self.end_output().await),
            ("stop_watch", self.stop_watch().await),
            ("leave", self.leave().await),
        ];
        for (step, result) in steps {
            if let Err(e) = result {
                self.log(
                    "close_step_error",
                    json!({"step": step, "error": e.to_string()}),
                );
            }
        }
        if let Some(net) = self.net() {
            net.close();
        }
        let pump = self.st.lock().pump.take();
        if let Some(pump) = pump {
            let abort = pump.abort_handle();
            if tokio::time::timeout(GATEWAY_CLOSE_WAIT, pump)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
    }

    // ------------------------------------------------------ event translation

    fn handle_dispatch(&self, d: &Dispatch) {
        let mut out = Vec::new();
        let mut own_observed = None;
        match d.name.as_str() {
            "READY_SUPPLEMENTAL" => self.on_ready(&mut out),
            "RESUMED" => out.push(model(Event::GatewayChanged {
                state: GatewayState::Fresh,
            })),
            "STREAM_CREATE" => {
                if let Some(CacheChange::Stream {
                    after: Some(stream),
                    ..
                }) = &d.change
                {
                    self.on_stream_create(stream, &mut out);
                }
            }
            "STREAM_UPDATE" => {
                if let Some(CacheChange::Stream {
                    after: Some(stream),
                    ..
                }) = &d.change
                {
                    self.on_stream_update(stream, &mut out);
                }
            }
            "STREAM_DELETE" => match &d.change {
                // `unavailable`: the stream is kept, flagged (`stream_unavailable`)
                Some(CacheChange::Stream {
                    after: Some(stream),
                    ..
                }) => self.on_stream_update(stream, &mut out),
                Some(CacheChange::Stream {
                    before: Some(stream),
                    after: None,
                    ..
                }) => {
                    let reason = d.data.get("reason").and_then(Value::as_str);
                    self.on_stream_delete(stream, reason, &mut out);
                }
                _ => {}
            },
            "VOICE_STATE_UPDATE" => {
                own_observed = self.on_own_voice_state(d);
                if let Some(CacheChange::Voice {
                    user_id,
                    before,
                    after,
                    ..
                }) = &d.change
                {
                    self.on_voice_state(*user_id, *before, *after, &mut out);
                }
            }
            "MESSAGE_CREATE" => self.on_message(&d.data, &mut out),
            _ => {}
        }
        // Python order: the dispatch's model events, then the relay's own voice observation, and
        // only then does the voice client apply the dispatch (spec section 12).
        self.emit_all(out);
        if let Some(event) = own_observed {
            self.emit(event);
        }
        if let Some(event) = link_event(&d.name, &d.data, self.opts.relay_id) {
            let vc = {
                let st = self.st.lock();
                st.vc.clone().or_else(|| st.leaving.clone())
            };
            if let Some(vc) = vc {
                vc.handle_event(event);
            }
        }
    }

    /// The relay's own voice state, observed before the voice client applies it (spec section 12).
    fn on_own_voice_state(&self, d: &Dispatch) -> Option<AdapterEvent> {
        let data = &d.data;
        if data.get("user_id").and_then(snowflake) != Some(self.opts.relay_id) {
            return None;
        }
        let net = self.net();
        let mut st = self.st.lock();
        // only a registered voice client observes it
        let vc = st.vc.clone()?;
        let session = data
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let channel_id = data.get("channel_id").and_then(snowflake);
        st.last_vsu_session.clone_from(&session);
        drop(st);
        self.log(
            "relay_voice_state",
            json!({
                "vsu_session": session,
                "gw_session": net.and_then(|n| n.gateway_session()),
                "channel_id": channel_id,
                "connected": vc.is_connected(),
            }),
        );
        Some(model(Event::RelayVoiceObserved(RelayVoiceObserved {
            session_id: session,
            channel_id,
        })))
    }

    /// Messages in the configured guild (and command channel) only; the rest is ignored silently.
    /// Only prefixed messages are passed on; the service decides who may use what (chat-added
    /// operators live there).
    fn on_message(&self, data: &Value, out: &mut Vec<AdapterEvent>) {
        let c = &self.opts.control;
        let guild = data.get("guild_id").and_then(snowflake);
        if !c.chat_commands || guild != Some(self.opts.guild_id) {
            return;
        }
        let Some(author) = data
            .get("author")
            .and_then(|a| a.get("id"))
            .and_then(snowflake)
        else {
            return;
        };
        if self.relay_ids.contains(&author) {
            return;
        }
        let Some(channel_id) = data.get("channel_id").and_then(snowflake) else {
            return;
        };
        if c.command_channel_id.is_some_and(|only| only != channel_id) {
            return;
        }
        let content = data.get("content").and_then(Value::as_str).unwrap_or("");
        let Some(head) = content.split_whitespace().next() else {
            return;
        };
        if head.to_lowercase() != c.command_prefix.to_lowercase() {
            return;
        }
        // Raw ids from the message's member object: the guild role cache may lack some.
        let role_ids = data
            .get("member")
            .and_then(|m| m.get("roles"))
            .and_then(Value::as_array)
            .map(|roles| roles.iter().filter_map(snowflake).collect())
            .unwrap_or_default();
        let author_voice_channel_id = self
            .cache
            .read()
            .voice_state(self.opts.guild_id, author)
            .and_then(|vs| vs.channel_id);
        let Some(message_id) = data.get("id").and_then(snowflake) else {
            return;
        };
        out.push(AdapterEvent::Chat(ChatCommand {
            author_id: author,
            channel_id,
            message_id,
            text: content.to_owned(),
            author_role_ids: role_ids,
            author_voice_channel_id,
        }));
    }

    fn is_target_stream(&self, key: &StreamKey) -> bool {
        key.owner_id() == self.target_id() && key.guild_id() == Some(self.opts.guild_id)
    }

    fn to_source(&self, stream: &StreamState, incarnation: i64) -> SourceStream {
        SourceStream {
            key: stream.key.to_string(),
            owner_id: stream.key.owner_id(),
            channel_id: stream.key.channel_id().unwrap_or(0),
            incarnation,
            paused: self.opts.paused_verified.then_some(stream.paused),
            unavailable: stream.unavailable,
        }
    }

    fn on_stream_create(&self, stream: &StreamState, out: &mut Vec<AdapterEvent>) {
        if !self.is_target_stream(&stream.key) {
            return;
        }
        let mut st = self.st.lock();
        // same key while an incarnation is active (incl. one synthesized from voice state or
        // stream_available) is an update, not a new share
        let key = stream.key.to_string();
        let new = st.stream.as_ref().is_none_or(|s| s.key != key);
        if new {
            end_stream(&mut st, out);
            st.incarnation += 1;
        }
        let source = self.to_source(stream, st.incarnation);
        st.stream = Some(source.clone());
        out.push(model(if new {
            Event::StreamStarted { stream: source }
        } else {
            Event::StreamUpdated { stream: source }
        }));
    }

    fn on_stream_update(&self, stream: &StreamState, out: &mut Vec<AdapterEvent>) {
        let mut st = self.st.lock();
        let key = stream.key.to_string();
        if !self.is_target_stream(&stream.key) || st.stream.as_ref().is_none_or(|s| s.key != key) {
            return;
        }
        let source = self.to_source(stream, st.incarnation);
        st.stream = Some(source.clone());
        out.push(model(Event::StreamUpdated { stream: source }));
    }

    fn target_streaming(&self) -> bool {
        self.cache
            .read()
            .voice_state(self.opts.guild_id, self.target_id())
            .is_some_and(|vs| vs.channel_id.is_some() && vs.self_stream)
    }

    fn on_stream_delete(
        &self,
        stream: &StreamState,
        reason: Option<&str>,
        out: &mut Vec<AdapterEvent>,
    ) {
        let mut st = self.st.lock();
        let key = stream.key.to_string();
        if !self.is_target_stream(&stream.key) || st.stream.as_ref().is_none_or(|s| s.key != key) {
            return;
        }
        // STREAM_DELETE also reflects our own watch ending; the target's real end is a voice state update
        if self.target_streaming() {
            drop(st);
            self.log(
                "stream_delete_ignored",
                json!({"reason": reason.unwrap_or("None")}),
            );
            return;
        }
        end_stream(&mut st, out);
    }

    /// Stream synthesized from voice state (STREAM_CREATE is not delivered outside voice).
    fn voice_source(&self, channel_id: u64, incarnation: i64) -> SourceStream {
        let key = StreamKey::Guild {
            guild_id: self.opts.guild_id,
            channel_id,
            owner_id: self.target_id(),
        };
        SourceStream {
            key: key.to_string(),
            owner_id: self.target_id(),
            channel_id,
            incarnation,
            paused: None,
            unavailable: false,
        }
    }

    fn on_target_stream_flag(
        &self,
        b: Option<u64>,
        a: Option<u64>,
        before: Option<VoiceStateEntry>,
        after: VoiceStateEntry,
        out: &mut Vec<AdapterEvent>,
    ) {
        let mut st = self.st.lock();
        // self_stream is stale-true when the channel is None, so require a channel
        let Some(a) = a.filter(|_| after.self_stream) else {
            end_stream(&mut st, out);
            return;
        };
        let was_streaming = b.is_some() && before.is_some_and(|v| v.self_stream);
        if was_streaming && b == Some(a) {
            return;
        }
        let key = self.voice_source(a, 0).key;
        if st.stream.as_ref().is_some_and(|s| s.key == key) {
            return;
        }
        end_stream(&mut st, out);
        st.incarnation += 1;
        let source = self.voice_source(a, st.incarnation);
        st.stream = Some(source.clone());
        out.push(model(Event::StreamStarted { stream: source }));
    }

    fn on_voice_state(
        &self,
        user_id: u64,
        before: Option<VoiceStateEntry>,
        after: VoiceStateEntry,
        out: &mut Vec<AdapterEvent>,
    ) {
        let b = before.and_then(|v| v.channel_id);
        let a = after.channel_id;
        if user_id == self.target_id() {
            out.push(model(Event::TargetVoiceChanged {
                target: a.map(|channel_id| TargetVoice {
                    channel_id: Some(channel_id),
                    self_stream: after.self_stream,
                }),
            }));
            self.on_target_stream_flag(b, a, before, after, out);
        }
        let channel = self.channel_id();
        if a == Some(channel) || b == Some(channel) {
            let mut ids: HashSet<u64> = self
                .cache
                .read()
                .voice_states(self.opts.guild_id)
                .filter(|(_, vs)| vs.channel_id == Some(channel))
                .map(|(id, _)| id)
                .collect();
            if a == Some(channel) {
                ids.insert(user_id);
            } else {
                ids.remove(&user_id);
            }
            ids.remove(&self.opts.relay_id);
            out.push(model(Event::ParticipantsChanged {
                participants: Some(ids),
            }));
        }
    }

    fn on_ready(&self, out: &mut Vec<AdapterEvent>) {
        if self.cache.read().self_id() != Some(self.opts.relay_id) {
            out.push(model(Event::ConfigInvalid {
                detail: "logged-in account is not discord.expected_relay_user_id".into(),
            }));
            return;
        }
        if !self
            .cache
            .read()
            .is_voice_channel(self.opts.guild_id, self.channel_id())
        {
            out.push(model(Event::ConfigInvalid {
                detail: "allowed_voice_channel_id is not a voice channel of the configured guild"
                    .into(),
            }));
            return;
        }
        out.push(model(self.snapshot(true)));
    }

    /// Synced view of target voice, target stream and participants (`Event::GatewaySnapshot`).
    fn snapshot(&self, new_session: bool) -> Event {
        let channel = self.channel_id();
        let mut target = None;
        let mut participants = None;
        {
            let cache = self.cache.read();
            if cache.is_voice_channel(self.opts.guild_id, channel) {
                let mut ids = HashSet::new();
                for (id, vs) in cache.voice_states(self.opts.guild_id) {
                    if vs.channel_id != Some(channel) {
                        continue;
                    }
                    if id == self.target_id() {
                        target = Some(TargetVoice {
                            channel_id: Some(channel),
                            self_stream: vs.self_stream,
                        });
                    }
                    if id != self.opts.relay_id {
                        ids.insert(id);
                    }
                }
                participants = Some(ids);
            }
        }
        let mut st = self.st.lock();
        let prev = st.stream.clone();
        self.sync_stream(&mut st);
        if st.stream.is_none()
            && let Some(TargetVoice {
                channel_id: Some(channel_id),
                self_stream: true,
            }) = target
        {
            let mut src = self.voice_source(channel_id, st.incarnation);
            if prev.as_ref().is_none_or(|p| p.key != src.key) {
                st.incarnation += 1;
                src = self.voice_source(channel_id, st.incarnation);
            } else if let Some(prev) = prev {
                src = prev;
            }
            st.stream = Some(src);
        }
        Event::GatewaySnapshot {
            target,
            stream: st.stream.clone(),
            participants,
            new_session,
        }
    }

    /// Reconciles the tracked stream with the gateway cache (restart / new session).
    fn sync_stream(&self, st: &mut State) {
        let live = {
            let cache = self.cache.read();
            cache
                .streams()
                .find(|s| self.is_target_stream(&s.key))
                .cloned()
        };
        let Some(live) = live else {
            st.stream = None;
            return;
        };
        if st
            .stream
            .as_ref()
            .is_none_or(|s| s.key != live.key.to_string())
        {
            st.incarnation += 1;
        }
        st.stream = Some(self.to_source(&live, st.incarnation));
    }

    // ------------------------------------------------------------------- chat

    async fn reply(
        &self,
        channel_id: u64,
        message_id: u64,
        text: &str,
    ) -> Result<(), AdapterError> {
        let rest = self
            .need_net()?
            .rest
            .ok_or_else(|| AdapterError::failed("adapter not started"))?;
        rest.reply(Some(self.opts.guild_id), channel_id, message_id, text)
            .await?;
        Ok(())
    }

    // ------------------------------------------------------------------ voice

    fn parent_codec_specs(&self) -> Vec<relay_voice::voice_ws::CodecSpec> {
        if self.opts.adaptive {
            adaptive_decode_codecs()
        } else {
            decode_codecs(&self.opts.parent_codecs)
        }
    }

    fn watch_codec_specs(&self) -> Vec<relay_voice::voice_ws::CodecSpec> {
        if self.opts.adaptive {
            adaptive_decode_codecs()
        } else {
            decode_codecs(&self.opts.watch_codecs)
        }
    }

    fn foreign_voice_session(&self) -> bool {
        let gateway = self.net().and_then(|n| n.gateway_session());
        let last = self.st.lock().last_vsu_session.clone();
        matches!((last, gateway), (Some(l), Some(g)) if !l.is_empty() && !g.is_empty() && l != g)
    }

    async fn join(
        self: &Arc<Self>,
        on_voice_state: Option<VoiceStateCallback>,
    ) -> Result<String, AdapterError> {
        let net = self.need_net()?;
        let channel_id = self.channel_id();
        if !self
            .cache
            .read()
            .is_voice_channel(self.opts.guild_id, channel_id)
        {
            return Err(AdapterError::failed("allowed voice channel not available"));
        }
        // one voice client per account: a dead parent still registered goes first
        let stale = {
            let mut st = self.st.lock();
            if let Some(pump) = st.vc_pump.take() {
                pump.abort();
            }
            st.vc.take()
        };
        if let Some(stale) = stale {
            self.disconnect_voice(stale).await;
        }
        let mut rtc = RtcSettings::new(
            self.parent_codec_specs(),
            vec![StreamSpec::video(REQUEST_QUALITY)],
            true,
        );
        rtc.user_agent.clone_from(&net.user_agent);
        let (vc, events) = VoiceClient::new(VoiceClientConfig {
            user_id: self.opts.relay_id,
            guild_id: Some(self.opts.guild_id),
            channel_id,
            gateway: Arc::clone(&net.link),
            http: net.http.clone(),
            rtc,
        });
        {
            let mut st = self.st.lock();
            st.vc = Some(vc.clone());
            st.vc_pump = Some(spawn_client_events(Arc::downgrade(self), events));
        }
        let joined = vc
            .connect(JoinOptions {
                timeout: self.opts.connect_timeout,
                self_mute: true,
                self_deaf: self.opts.self_deaf,
                on_voice_state: on_voice_state
                    .map(|cb| Box::new(move |_: &str| cb()) as Box<dyn FnOnce(&str) + Send>),
            })
            .await;
        match joined {
            Ok(session_id) => {
                net.set_rtc_connected(true);
                Ok(session_id)
            }
            Err(e) => {
                // the voice-state echo may already have registered it
                let mut st = self.st.lock();
                st.vc = None;
                if let Some(pump) = st.vc_pump.take() {
                    pump.abort();
                }
                Err(e.into())
            }
        }
    }

    fn voice_alive(&self) -> bool {
        let vc = self.st.lock().vc.clone();
        vc.is_some_and(|vc| vc.is_connected())
    }

    /// Disconnects `vc` (already unregistered) while it still receives the dispatches it waits for.
    async fn disconnect_voice(&self, vc: VoiceClient) {
        self.st.lock().leaving = Some(vc.clone());
        vc.disconnect(LEAVE_TIMEOUT).await;
        self.st.lock().leaving = None;
    }

    async fn leave(&self) -> Result<(), AdapterError> {
        let (vc, pump) = {
            let mut st = self.st.lock();
            (st.vc.take(), st.vc_pump.take())
        };
        if let Some(vc) = vc {
            self.disconnect_voice(vc).await;
        }
        if let Some(net) = self.net() {
            net.set_rtc_connected(false);
        }
        if let Some(pump) = pump {
            pump.abort();
        }
        Ok(())
    }

    /// Watches exactly `stream` (key and incarnation must match the tracked one).
    async fn watch(
        self: &Arc<Self>,
        generation: u64,
        epoch: u64,
        stream: SourceStream,
    ) -> Result<(), AdapterError> {
        {
            let st = self.st.lock();
            match &st.stream {
                Some(cur) if cur.key == stream.key && cur.incarnation == stream.incarnation => {}
                _ => return Err(AdapterError::failed("target stream changed before watch")),
            }
        }
        let key = StreamKey::Guild {
            guild_id: self.opts.guild_id,
            channel_id: stream.channel_id,
            owner_id: self.target_id(),
        };
        if key.to_string() != stream.key {
            return Err(AdapterError::failed(
                "target stream key does not match the tracked stream",
            ));
        }
        let net = self.need_net()?;
        let vc = self
            .st
            .lock()
            .vc
            .clone()
            .ok_or_else(|| AdapterError::failed("not in voice"))?;
        {
            let mut hot = self.hot.lock();
            hot.generation = generation;
            hot.epoch = epoch;
            hot.video_ssrc = None;
            hot.last_seq.clear();
            hot.gaps.clear();
            hot.sc = None;
        }
        self.st.lock().watch_args = Some((generation, stream));
        // fresh transport-wide sequence state, in place before connecting so no packet is missed
        let twcc = Arc::new(TwccFeedback::new());
        self.twcc.reset_sent();

        let mut settings = RtcSettings::watch(self.watch_codec_specs(), true);
        settings.user_agent.clone_from(&net.user_agent);
        settings.receive.debug_stats = self.opts.network_stats;
        let record = Arc::clone(&twcc);
        settings.receive.rtp_tap = Some(Arc::new(move |obs: &RtpObservation<'_>| {
            // UDP dispatcher thread: note each packet's transport-wide sequence number
            if let Some(seq) = transport_seq(obs.rtp_extensions) {
                record.record(seq, arrival_seconds(obs.received_at));
            }
        }));
        // Live G2a: STREAM_CREATE is not sent to a non-watcher even inside voice, so watch by key.
        let sc = vc
            .watch_stream(&key.to_string(), settings, self.opts.connect_timeout)
            .await?;
        // in place before the first packet, which may ask for the video SSRC
        self.hot.lock().sc = Some(sc.clone());
        let me = Arc::downgrade(self);
        let listening = sc.listen(move |packet| {
            if let Some(inner) = me.upgrade() {
                inner.on_packet(&packet);
            }
        });
        if let Err(e) = listening {
            // the stream is connected but never tracked in `st.sc`: nothing else would close it
            self.hot.lock().sc = None;
            sc.disconnect().await;
            return Err(e.into());
        }
        let task = tokio::spawn(twcc_loop(Arc::downgrade(self), sc.clone(), twcc));
        let mut st = self.st.lock();
        st.sc = Some(sc);
        if let Some(old) = st.twcc_task.replace(task) {
            old.abort();
        }
        Ok(())
    }

    async fn stop_watch(&self) -> Result<(), AdapterError> {
        let (sc, task) = {
            let mut st = self.st.lock();
            (st.sc.take(), st.twcc_task.take())
        };
        if let Some(task) = task {
            task.abort();
        }
        {
            let mut hot = self.hot.lock();
            hot.gaps.clear();
            hot.sc = None;
        }
        if let Some(sc) = sc {
            sc.stop_listening();
            sc.disconnect().await;
        }
        Ok(())
    }

    async fn rewatch(self: &Arc<Self>) -> Result<(), AdapterError> {
        let args = self.st.lock().watch_args.clone();
        let Some((generation, stream)) = args else {
            return Err(AdapterError::failed("no watch to renew"));
        };
        self.stop_watch().await?;
        let epoch = self.hot.lock().epoch;
        self.watch(generation, epoch, stream).await
    }

    async fn request_pli(&self) -> Result<(), AdapterError> {
        let ssrc = self.hot.lock().video_ssrc;
        let sc = self.st.lock().sc.clone();
        if let (Some(sc), Some(ssrc)) = (sc, ssrc) {
            sc.send_pli(ssrc)?;
        }
        Ok(())
    }

    async fn update_watch_codecs(&self, names: &[&str]) -> Result<bool, AdapterError> {
        let sc = self.st.lock().sc.clone();
        let Some(sc) = sc else { return Ok(false) };
        let codecs = parse_names(names).map_err(|e| AdapterError::failed(e.to_string()))?;
        sc.session_update(&video_codec_list(&codecs, false))?;
        Ok(true)
    }

    // ----------------------------------------------------------------- output

    async fn create_output(
        self: &Arc<Self>,
        generation: u64,
        fps: u32,
        adaptive: bool,
    ) -> Result<Option<String>, AdapterError> {
        let net = self.need_net()?;
        let vc = self
            .st
            .lock()
            .vc
            .clone()
            .ok_or_else(|| AdapterError::failed("not in voice"))?;
        let settings = output_settings(adaptive, fps, self.opts.network_stats, &net.user_agent);
        if adaptive {
            // op 14 can land before create_stream returns: the hook must already be in place
            let key = StreamKey::Guild {
                guild_id: self.opts.guild_id,
                channel_id: self.channel_id(),
                owner_id: self.opts.relay_id,
            };
            self.st.lock().codec_hook = Some((key.to_string(), generation));
        }
        let out = vc
            .create_stream(settings, self.opts.connect_timeout)
            .await?;
        // the sender never replays a keyframe, fixed codec or adaptive: viewer PLIs go to the source
        let me = Arc::downgrade(self);
        out.on_keyframe_request(move || {
            if let Some(inner) = me.upgrade() {
                inner.on_output_pli();
            }
        });
        let codec = negotiated_name(&out)?;
        self.st.lock().out = Some(out);
        Ok(codec)
    }

    /// A viewer of the ingest output asked for a keyframe the library cannot answer.
    fn on_output_pli(&self) {
        self.emit(AdapterEvent::Pool(PoolEvent::OutPli { account: None }));
    }

    fn output_codec(&self) -> Option<String> {
        let out = self.st.lock().out.clone()?;
        negotiated_name(&out).ok().flatten()
    }

    async fn update_output_codecs(&self, names: &[&str]) -> Result<bool, AdapterError> {
        let out = self.st.lock().out.clone();
        let Some(out) = out else { return Ok(false) };
        let codecs = parse_names(names).map_err(|e| AdapterError::failed(e.to_string()))?;
        out.session_update(&video_codec_list(&codecs, true))?;
        Ok(true)
    }

    async fn swap_output_source(&self, make_source: SourceFactory) -> Option<VoiceCodec> {
        let out = self.st.lock().out.clone();
        swap_source(out.as_ref(), make_source, &self.log)
    }

    fn play(&self, source: Box<dyn FrameSource>, params: VideoParams) -> Result<(), AdapterError> {
        let out = self
            .st
            .lock()
            .out
            .clone()
            .ok_or_else(|| AdapterError::failed("no output to play on"))?;
        out.play(source, params)?;
        Ok(())
    }

    async fn end_output(&self) -> Result<(), AdapterError> {
        let out = {
            let mut st = self.st.lock();
            st.codec_hook = None;
            st.out.take()
        };
        if let Some(out) = out {
            out.disconnect().await;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ stats

    fn take_audio_window(&self) -> Value {
        let now = monotonic();
        let mut audio = self.audio.lock();
        let elapsed = now - std::mem::replace(&mut audio.window_start, now);
        audio.stats.take(elapsed)
    }

    fn network_stats(&self) -> Value {
        let (watch, out) = {
            let st = self.st.lock();
            (st.sc.clone(), st.out.clone())
        };
        let stats_of = |c: &StreamClient| {
            c.receiver()
                .map(|r| json!(r.stats()))
                .unwrap_or_else(|_| json!({}))
        };
        let mut stats = serde_json::Map::new();
        if let Some(sc) = watch {
            let mut watch = stats_of(&sc);
            watch["twcc_sent"] = json!(self.twcc.sent());
            stats.insert("watch".into(), watch);
        }
        if let Some(out) = out {
            let o = stats_of(&out);
            if o.as_object().is_some_and(|m| !m.is_empty()) {
                stats.insert("output".into(), o);
            }
        }
        Value::Object(stats)
    }

    // ---------------------------------------------------------- receive path

    /// UDP dispatcher thread: filter, forward to ingress, rate-limit `SourceProgress`.
    fn on_packet(&self, packet: &Packet) {
        if packet.user_id != Some(self.target_id()) {
            return;
        }
        let mono_now = monotonic();
        let received = mono_now - packet.received_at.elapsed().as_secs_f64();
        let rtcp = rtcp_to_mono(packet.rtcp_time, wall_clock(), mono_now);
        let video = packet.media_type == MediaType::Video;
        let message;
        let mut request = None;
        let progress;
        {
            let mut hot = self.hot.lock();
            let ids = (hot.generation, hot.epoch);
            if video {
                let Some(codec) = packet.codec.video() else {
                    return;
                };
                if hot
                    .gaps
                    .payload_type_switched(packet.ssrc, codec, packet.payload_type)
                {
                    // Drop the library's receive state of the codec left behind: RTP sequence
                    // numbers run on across payload types, so a later return to it would make its
                    // reorder buffer wait out every number sent meanwhile (0.5 s), then skip them,
                    // and the held frames arrive stale.
                    if let Some(receiver) = hot.sc.as_ref().and_then(|s| s.receiver().ok()) {
                        receiver.forget_other_payload_types(packet.ssrc, packet.payload_type);
                    }
                }
                let gap = hot.gaps.gap(
                    packet.ssrc,
                    codec,
                    packet.sequence,
                    packet.rtp_packets.iter().map(|p| p.sequence),
                );
                message = Ingress::Video(ingress_video(packet, ids, received, rtcp, gap));
                if hot.video_ssrc != Some(packet.ssrc) {
                    hot.video_ssrc = Some(packet.ssrc);
                    request = hot.sc.clone().map(|sc| (packet.ssrc, sc));
                }
            } else {
                message = Ingress::Audio(ingress_audio(packet, ids, received, rtcp));
                self.audio
                    .lock()
                    .stats
                    .count(packet.ssrc, packet.sequence, &packet.payload);
            }
            let fresh = hot.last_seq.get(&packet.ssrc) != Some(&packet.sequence);
            hot.last_seq.insert(packet.ssrc, packet.sequence);
            let slot = usize::from(video);
            progress = (fresh && mono_now - hot.last_progress[slot] >= PROGRESS_MIN_INTERVAL_S)
                .then(|| {
                    hot.last_progress[slot] = mono_now;
                    Event::SourceProgress {
                        generation: ids.0,
                        at: mono_now,
                        video,
                    }
                });
        }
        if let Some((ssrc, sc)) = request {
            let result = sc
                .request_video(ssrc, REQUEST_QUALITY)
                .and_then(|()| sc.send_pli(ssrc));
            if let Err(e) = result {
                self.log("request_video_failed", json!({"error": e.to_string()}));
            }
        }
        (self.ingress)(message);
        if let Some(progress) = progress {
            self.emit(model(progress));
        }
    }

    // ------------------------------------------------------- voice events

    /// A voice or stream client event of the parent connection (and its streams).
    fn on_client_event(&self, event: ClientEvent) {
        match event {
            ClientEvent::SessionUpdate { stream_key, info } => {
                let hook = self.st.lock().codec_hook.clone();
                if let (Some(key), Some((hook_key, generation)), Some(codec)) =
                    (stream_key, hook, info.video_codec)
                    && key == hook_key
                {
                    self.emit(model(Event::CodecChanged {
                        generation,
                        negotiated_video_codec: Some(codec.to_ascii_uppercase()),
                    }));
                }
            }
            ClientEvent::Disconnected {
                stream_key: Some(key),
                ..
            } => {
                // the watch connection ended by itself: its feedback loop has nothing left to send on
                let mut st = self.st.lock();
                if st.sc.as_ref().is_some_and(|sc| sc.key() == key)
                    && let Some(task) = st.twcc_task.take()
                {
                    task.abort();
                }
                drop(st);
                self.log(
                    "voice_event",
                    json!({"event": "Disconnected", "stream_key": key}),
                );
            }
            other => self.log("voice_event", json!({"event": format!("{other:?}")})),
        }
    }
}

fn end_stream(st: &mut State, out: &mut Vec<AdapterEvent>) {
    if let Some(ended) = st.stream.take() {
        out.push(model(Event::StreamEnded {
            key: ended.key,
            incarnation: ended.incarnation,
        }));
    }
}

/// Seconds on the relay's monotonic clock at which a datagram arrived.
pub(crate) fn arrival_seconds(received_at: std::time::Instant) -> f64 {
    monotonic() - received_at.elapsed().as_secs_f64()
}

/// The video codec the server negotiated for `stream`, as the name the supervisor uses.
fn negotiated_name(stream: &StreamClient) -> Result<Option<String>, AdapterError> {
    Ok(stream
        .negotiated_video_codec()?
        .map(|c| c.as_str().to_owned()))
}

/// `codecs.swap_output_source`: replaces the running output's source in place for the codec the
/// server negotiated now, without reconnecting. Failures are logged and reported as `None`.
pub(crate) fn swap_source(
    out: Option<&StreamClient>,
    make_source: SourceFactory,
    log: &LogFn,
) -> Option<VoiceCodec> {
    let Some(out) = out else {
        log(
            "codec_swap_skipped",
            json!({"reason": "no output or codec"}),
        );
        return None;
    };
    match out.swap_output_source(make_source) {
        Ok(Some(codec)) => Some(codec),
        Ok(None) => {
            log(
                "codec_swap_skipped",
                json!({"reason": "no output or codec"}),
            );
            None
        }
        Err(e) => {
            log("codec_swap_failed", json!({"error": e.to_string()}));
            None
        }
    }
}

/// The gateway events of the main account: dispatches into the adapter, disconnects into
/// `RESUMING`, the end of the task into `INVALID` (or `AuthRejected`).
async fn run_gateway(adapter: Weak<Inner>, mut events: UnboundedReceiver<GatewayEvent>) {
    while let Some(event) = events.recv().await {
        let Some(inner) = adapter.upgrade() else {
            return;
        };
        inner.on_gateway_event(event);
    }
}

/// Pumps the voice client's events into the adapter until the client or the adapter is gone.
fn spawn_client_events(
    adapter: Weak<Inner>,
    mut events: UnboundedReceiver<ClientEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let Some(inner) = adapter.upgrade() else {
                return;
            };
            inner.on_client_event(event);
        }
    })
}

/// Sends transport-cc feedback for the watch every [`TWCC_INTERVAL_S`] while it lives.
async fn twcc_loop(adapter: Weak<Inner>, sc: StreamClient, twcc: Arc<TwccFeedback>) {
    let mut tick = tokio::time::interval(Duration::from_secs_f64(TWCC_INTERVAL_S));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tick.tick().await;
        let Some(inner) = adapter.upgrade() else {
            return;
        };
        let ssrc = inner.hot.lock().video_ssrc;
        let failure = inner.twcc.tick(
            &twcc,
            sc.own_ssrc().map_err(|e| e.to_string()),
            ssrc,
            |header, body| sc.send_rtcp(header, body).map_err(|e| e.to_string()),
        );
        if let Some((step, error)) = failure {
            inner.log("twcc_failed", json!({"step": step, "error": error}));
        }
    }
}

impl Adapter for DiscordAdapter {
    fn start(&self, token: Token) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.start(token).await })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.inner.close().await })
    }

    fn set_epoch(&self, epoch: u64) {
        self.inner.hot.lock().epoch = epoch;
    }

    fn set_routing(&self, target_user_id: u64, channel_id: u64) {
        self.inner.target.store(target_user_id, Ordering::Relaxed);
        self.inner.channel.store(channel_id, Ordering::Relaxed);
    }

    fn target_id(&self) -> u64 {
        self.inner.target_id()
    }

    fn channel_id(&self) -> u64 {
        self.inner.channel_id()
    }

    fn snapshot(&self, new_session: bool) -> Event {
        self.inner.snapshot(new_session)
    }

    fn reply(&self, channel_id: u64, message_id: u64, text: String) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.reply(channel_id, message_id, &text).await })
    }

    fn voice_channel_ids(&self) -> HashSet<u64> {
        self.inner
            .cache
            .read()
            .voice_channel_ids(self.inner.opts.guild_id)
            .into_iter()
            .collect()
    }

    fn display_name(&self, user_id: u64) -> Option<String> {
        self.inner
            .cache
            .read()
            .display_name(self.inner.opts.guild_id, user_id)
    }

    fn is_bot(&self, user_id: u64) -> bool {
        self.inner.cache.read().is_bot(user_id)
    }

    fn role_name(&self, role_id: u64) -> Option<String> {
        self.inner
            .cache
            .read()
            .role_name(self.inner.opts.guild_id, role_id)
            .map(str::to_owned)
    }

    fn join(&self, on_voice_state: Option<VoiceStateCallback>) -> ApiFuture<'_, String> {
        Box::pin(async move { self.inner.join(on_voice_state).await })
    }

    fn voice_alive(&self) -> bool {
        self.inner.voice_alive()
    }

    fn last_vsu_session(&self) -> Option<String> {
        self.inner.st.lock().last_vsu_session.clone()
    }

    fn foreign_voice_session(&self) -> bool {
        self.inner.foreign_voice_session()
    }

    fn leave(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.leave().await })
    }

    fn watch(&self, generation: u64, epoch: u64, stream: SourceStream) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.watch(generation, epoch, stream).await })
    }

    fn update_watch_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move { self.inner.update_watch_codecs(names).await })
    }

    fn request_pli(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.request_pli().await })
    }

    fn rewatch(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.rewatch().await })
    }

    fn stop_watch(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.stop_watch().await })
    }

    fn create_output(&self, generation: u64, fps: u32) -> ApiFuture<'_, Option<String>> {
        Box::pin(async move { self.inner.create_output(generation, fps, false).await })
    }

    fn create_output_adaptive(&self, generation: u64, fps: u32) -> ApiFuture<'_, Option<String>> {
        Box::pin(async move { self.inner.create_output(generation, fps, true).await })
    }

    fn output_codec(&self) -> Option<String> {
        self.inner.output_codec()
    }

    fn update_output_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move { self.inner.update_output_codecs(names).await })
    }

    fn swap_output_source(&self, make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>> {
        Box::pin(async move { self.inner.swap_output_source(make_source).await })
    }

    fn play(&self, source: Box<dyn FrameSource>, params: VideoParams) -> Result<(), AdapterError> {
        self.inner.play(source, params)
    }

    fn end_output(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.end_output().await })
    }

    fn take_audio_window(&self) -> Value {
        self.inner.take_audio_window()
    }

    fn network_stats(&self) -> Value {
        self.inner.network_stats()
    }
}

#[cfg(test)]
mod tests;
