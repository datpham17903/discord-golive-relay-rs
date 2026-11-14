//! Output-only Discord account (multi-account fan-out). Port of `adapter/output_account.py`.
//!
//! Logs in, joins the allowed voice channel muted and publishes its own Go Live. It never watches
//! and never listens. Every call that can fail returns an error, which the service turns into
//! `OutFailed`. `end` is safe to call after a partial `publish`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use relay_core::config::{CodecPolicy, OutputAccount, OutputCodec, RelayConfig};
use relay_core::supervisor::outputs_model::PoolEvent;
use relay_media::codec::VoiceCodec;
use relay_voice::connection::{RECONNECT_TIMEOUT, RtcSettings};
use relay_voice::voice_ws::{StreamSpec, video_codec_list};
use relay_voice::{ClientEvent, JoinOptions, StreamClient, VoiceClient, VoiceClientConfig};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::adapter::{CONNECT_TIMEOUT, swap_source};
use crate::api::{
    AdapterError, ApiFuture, BoxFuture, LogFn, OutputAdapter, OutputSource, PoolEventSink,
    SourceFactory,
};
use crate::cache::{Cache, CacheChange, SharedCache, StreamKey, StreamState};
use crate::codecs::{
    ALL_VIDEO_CODECS, adaptive_decode_codecs, decode_codecs, output_settings, parse_names,
};
use crate::link::{link_event, snowflake};
use crate::net::Net;
use crate::{Dispatch, GatewayEvent, RestClient, Token, Urls};

/// `HEALTH_POLL_S`.
pub const HEALTH_POLL: Duration = Duration::from_secs(1);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_CLOSE_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct OutputOptions {
    pub user_id: u64,
    pub guild_id: u64,
    /// The channel the account is assigned to until `publish` names another.
    pub channel_id: u64,
    /// Decode list of the voice connection; ignored when `adaptive`.
    pub parent_codecs: Vec<VoiceCodec>,
    pub adaptive: bool,
    pub network_stats: bool,
    /// Never join while one of these is in the channel.
    pub excluded_viewer_ids: HashSet<u64>,
    /// Never counted as viewers of this account's Go Live (the account itself is added).
    pub bot_user_ids: HashSet<u64>,
    pub self_deaf: bool,
    pub urls: Urls,
    pub connect_timeout: Duration,
    pub health_poll: Duration,
}

impl OutputOptions {
    /// Options of `account` as `service.run_service` builds them.
    pub fn from_config(cfg: &RelayConfig, account: &OutputAccount) -> Self {
        let d = &cfg.discord;
        let adaptive = cfg.codecs.output_codec == OutputCodec::Adaptive;
        let codecs = match cfg.codecs.effective_parent_codecs() {
            CodecPolicy::H264Only if !adaptive => vec![VoiceCodec::H264],
            _ => ALL_VIDEO_CODECS.to_vec(),
        };
        let mut bots: HashSet<u64> = d.output_accounts.iter().map(|a| a.user_id).collect();
        bots.insert(d.expected_relay_user_id);
        Self {
            user_id: account.user_id,
            guild_id: d.guild_id,
            channel_id: d.allowed_voice_channel_id,
            parent_codecs: codecs,
            adaptive,
            network_stats: cfg.logging.network_stats,
            excluded_viewer_ids: cfg
                .authorization
                .excluded_viewer_ids
                .iter()
                .copied()
                .collect(),
            bot_user_ids: bots,
            self_deaf: cfg.runtime.self_deaf_outputs,
            urls: Urls::default(),
            connect_timeout: CONNECT_TIMEOUT,
            health_poll: HEALTH_POLL,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    Connecting,
    Ready,
    Closed(String),
}

/// Upper bound for login plus READY in `start`.
const START_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
struct State {
    vc: Option<VoiceClient>,
    /// The client being disconnected: no longer `vc`, but it still needs the gateway's voice-state echo
    /// that completes its leave.
    leaving: Option<VoiceClient>,
    vc_pump: Option<JoinHandle<()>>,
    out: Option<StreamClient>,
    health: Option<JoinHandle<()>>,
    /// Last viewers reported; `None` while not live.
    viewers: Option<HashSet<u64>>,
    /// Generation, attempt and key of an adaptive output: op 14 of that stream becomes
    /// `OutCodecChanged`.
    codec_hook: Option<(String, u64, u64)>,
    pump: Option<JoinHandle<()>>,
}

struct Inner {
    opts: OutputOptions,
    bots: HashSet<u64>,
    events: PoolEventSink,
    log: LogFn,
    cache: SharedCache,
    channel: AtomicU64,
    net: Mutex<Option<Net>>,
    st: Mutex<State>,
    phase: watch::Sender<Phase>,
}

pub struct OutputAccountAdapter {
    inner: Arc<Inner>,
}

impl OutputAccountAdapter {
    pub fn new(opts: OutputOptions, events: PoolEventSink, log: LogFn) -> Self {
        Self::with_cache(opts, Cache::shared(), events, log)
    }

    pub fn with_cache(
        opts: OutputOptions,
        cache: SharedCache,
        events: PoolEventSink,
        log: LogFn,
    ) -> Self {
        let mut bots = opts.bot_user_ids.clone();
        bots.insert(opts.user_id);
        let inner = Inner {
            channel: AtomicU64::new(opts.channel_id),
            bots,
            opts,
            events,
            log,
            cache,
            net: Mutex::new(None),
            st: Mutex::new(State::default()),
            phase: watch::channel(Phase::Connecting).0,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Applies one gateway dispatch (the cache has already seen it).
    pub fn handle_dispatch(&self, dispatch: &Dispatch) {
        self.inner.handle_dispatch(dispatch);
    }

    #[cfg(test)]
    fn attach_detached(&self, net: Net) {
        *self.inner.net.lock() = Some(net);
    }
}

impl Inner {
    fn emit(&self, event: PoolEvent) {
        (self.events)(event);
    }

    fn log(&self, name: &str, fields: Value) {
        (self.log)(name, fields);
    }

    fn net(&self) -> Option<Net> {
        self.net.lock().clone()
    }

    fn channel_id(&self) -> u64 {
        self.channel.load(Ordering::Relaxed)
    }

    /// Logs in and waits for READY. `LoginFailure` -> `OutAuthRejected` (nothing is left running).
    async fn start(self: &Arc<Self>, token: Token) -> Result<(), AdapterError> {
        let rest = RestClient::new(self.opts.urls.clone(), token).await?;
        match rest.login().await {
            Ok(_) => {}
            Err(crate::Error::LoginFailed) => {
                self.emit(PoolEvent::OutAuthRejected {
                    account: self.opts.user_id,
                });
                self.close().await;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
        let (net, events) = Net::attach(rest, Arc::clone(&self.cache));
        *self.net.lock() = Some(net);
        let pump = tokio::spawn(run_gateway(Arc::downgrade(self), events));
        self.st.lock().pump = Some(pump);

        let mut phase = self.phase.subscribe();
        let waited = tokio::time::timeout(START_TIMEOUT, async {
            phase
                .wait_for(|p| *p != Phase::Connecting)
                .await
                .map(|p| p.clone())
                .unwrap_or(Phase::Closed("gateway ended before READY".into()))
        })
        .await;
        let Ok(phase) = waited else {
            self.close().await;
            return Err(AdapterError::failed("timed out waiting for READY"));
        };
        if let Phase::Closed(error) = phase {
            return Err(AdapterError::failed(error));
        }
        if self.cache.read().self_id() != Some(self.opts.user_id) {
            self.close().await;
            return Err(AdapterError::AccountMismatch);
        }
        Ok(())
    }

    /// The gateway gave up after READY (fatal close code): tell the pool, which would otherwise keep
    /// a mirror whose account can no longer see voice.
    fn on_gateway_closed(&self, _error: &crate::Error) {
        // The gateway task only ends on a non-resumable close (1000, 4004, 4010-4014): the account is
        // unusable until a restart, published or idle, so the pool must stop using it.
        self.emit(PoolEvent::OutAuthRejected {
            account: self.opts.user_id,
        });
    }

    fn parent_codec_specs(&self) -> Vec<relay_voice::voice_ws::CodecSpec> {
        if self.opts.adaptive {
            adaptive_decode_codecs()
        } else {
            decode_codecs(&self.opts.parent_codecs)
        }
    }

    // ------------------------------------------------------------- dispatches

    fn participants_if_changed(&self, change: &CacheChange) {
        match change {
            CacheChange::Voice { before, after, .. } => {
                let channel = Some(self.channel_id());
                if before.and_then(|v| v.channel_id) == channel || after.channel_id == channel {
                    self.emit_participants();
                }
            }
            CacheChange::Batch(changes) => {
                for change in changes {
                    self.participants_if_changed(change);
                }
            }
            CacheChange::Stream { .. } => {}
        }
    }

    fn handle_dispatch(&self, d: &Dispatch) {
        let uid = self.opts.user_id;
        match d.name.as_str() {
            "READY_SUPPLEMENTAL" => {
                self.phase.send_replace(Phase::Ready);
            }
            "VOICE_STATE_UPDATE" => {
                if d.data.get("user_id").and_then(snowflake) == Some(uid) {
                    self.observe_own_voice_state(&d.data);
                }
                if let Some(change) = &d.change {
                    self.participants_if_changed(change);
                }
            }
            "PASSIVE_UPDATE_V2" => {
                if let Some(change) = &d.change {
                    self.participants_if_changed(change);
                }
            }
            "STREAM_CREATE" | "STREAM_UPDATE" => {
                if let Some(CacheChange::Stream {
                    after: Some(stream),
                    ..
                }) = &d.change
                {
                    self.own_stream(stream);
                }
            }
            _ => {}
        }
        if let Some(event) = link_event(&d.name, &d.data, uid) {
            let vc = {
                let st = self.st.lock();
                st.vc.clone().or_else(|| st.leaving.clone())
            };
            if let Some(vc) = vc {
                vc.handle_event(event);
            }
        }
    }

    /// The account's own voice state, observed before the voice client applies it.
    fn observe_own_voice_state(&self, data: &Value) {
        if self.st.lock().vc.is_none() {
            return; // only a registered voice client observes it
        }
        self.emit(PoolEvent::OutVoiceObserved {
            account: self.opts.user_id,
            session_id: data
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            channel_id: data.get("channel_id").and_then(snowflake),
        });
    }

    /// Reports who watches this account's own Go Live in its channel, once per change.
    fn own_stream(&self, stream: &StreamState) {
        let key = stream.key;
        let mut st = self.st.lock();
        if st.vc.is_none()
            || key.owner_id() != self.opts.user_id
            || key.channel_id() != Some(self.channel_id())
        {
            return;
        }
        let viewers: HashSet<u64> = stream
            .viewer_ids
            .iter()
            .copied()
            .filter(|id| !self.bots.contains(id))
            .collect();
        if st.viewers.as_ref() != Some(&viewers) {
            st.viewers = Some(viewers.clone());
            drop(st);
            self.emit(PoolEvent::OutViewers {
                account: self.opts.user_id,
                viewers,
            });
        }
    }

    fn channel_participants(&self, channel_id: u64) -> Option<HashSet<u64>> {
        let cache = self.cache.read();
        cache
            .is_voice_channel(self.opts.guild_id, channel_id)
            .then(|| {
                cache
                    .voice_states(self.opts.guild_id)
                    .filter(|(_, vs)| vs.channel_id == Some(channel_id))
                    .map(|(id, _)| id)
                    .collect()
            })
    }

    fn emit_participants(&self) {
        let channel_id = self.channel_id();
        if let Some(participants) = self.channel_participants(channel_id) {
            self.emit(PoolEvent::MirrorParticipants {
                account: self.opts.user_id,
                participants,
                channel_id,
            });
        }
    }

    // ---------------------------------------------------------------- publish

    /// Joins `channel_id`, creates the Go Live and plays `source`; returns the negotiated codec.
    async fn publish(
        self: &Arc<Self>,
        generation: u64,
        attempt: u64,
        source: OutputSource,
        params: relay_voice::publish::VideoParams,
        channel_id: u64,
        adaptive: bool,
    ) -> Result<Option<String>, AdapterError> {
        let uid = self.opts.user_id;
        self.channel.store(channel_id, Ordering::Relaxed);
        let net = self
            .net()
            .ok_or_else(|| AdapterError::failed("adapter not started"))?;
        let Some(present) = self.channel_participants(channel_id) else {
            return Err(AdapterError::failed("allowed voice channel not available"));
        };
        if !self.opts.excluded_viewer_ids.is_disjoint(&present) {
            // same as the main channel's preflight: never join while an excluded viewer is present
            self.emit_participants();
            return Err(AdapterError::failed("excluded viewer in voice channel"));
        }

        let mut rtc = RtcSettings::new(
            self.parent_codec_specs(),
            vec![StreamSpec::video(100)],
            true,
        );
        rtc.user_agent.clone_from(&net.user_agent);
        let (vc, events) = VoiceClient::new(VoiceClientConfig {
            user_id: uid,
            guild_id: Some(self.opts.guild_id),
            channel_id,
            gateway: Arc::clone(&net.link),
            http: net.http.clone(),
            rtc,
        });
        {
            let mut st = self.st.lock();
            if let Some(pump) = st
                .vc_pump
                .replace(spawn_client_events(Arc::downgrade(self), events))
            {
                pump.abort();
            }
            st.vc = Some(vc.clone());
        }
        let session_id = vc
            .connect(JoinOptions {
                timeout: self.opts.connect_timeout,
                self_mute: true,
                self_deaf: self.opts.self_deaf,
                on_voice_state: None,
            })
            .await?;
        net.set_rtc_connected(true);
        self.emit(PoolEvent::OutJoined {
            account: uid,
            generation,
            attempt,
            voice_session_id: Some(session_id),
        });
        self.emit_participants();

        let settings = output_settings(
            adaptive,
            params.fps,
            self.opts.network_stats,
            &net.user_agent,
        );
        if adaptive {
            // op 14 can land before create_stream returns: the hook must already be in place
            let key = StreamKey::Guild {
                guild_id: self.opts.guild_id,
                channel_id,
                owner_id: uid,
            };
            self.st.lock().codec_hook = Some((key.to_string(), generation, attempt));
        }
        let out = vc
            .create_stream(settings, self.opts.connect_timeout)
            .await?;
        // the sender never replays a keyframe, fixed codec or adaptive: viewer PLIs go to the source
        let me = Arc::downgrade(self);
        out.on_keyframe_request(move || {
            if let Some(inner) = me.upgrade() {
                inner.emit(PoolEvent::OutPli {
                    account: Some(inner.opts.user_id),
                });
            }
        });
        let codec = out.negotiated_video_codec()?.map(|c| c.as_str().to_owned());
        self.st.lock().out = Some(out.clone());
        self.emit(PoolEvent::OutReady {
            account: uid,
            generation,
            attempt,
            negotiated_video_codec: codec.clone(),
        });
        let source = match source {
            OutputSource::Ready(source) => source,
            OutputSource::ForCodec(make) => {
                // the caller builds the source for the negotiated codec
                let codec: VoiceCodec = codec
                    .as_deref()
                    .ok_or_else(|| AdapterError::failed("no video codec negotiated"))?
                    .parse()
                    .map_err(|e: relay_media::codec::UnknownCodec| {
                        AdapterError::failed(e.to_string())
                    })?;
                make(codec)
            }
        };
        out.play(source, params)?;
        let health = tokio::spawn(watch_health(
            Arc::downgrade(self),
            generation,
            attempt,
            out,
            vc,
        ));
        if let Some(old) = self.st.lock().health.replace(health) {
            old.abort();
        }
        Ok(codec)
    }

    async fn update_codecs(&self, names: &[&str]) -> Result<bool, AdapterError> {
        let out = self.st.lock().out.clone();
        let Some(out) = out else { return Ok(false) };
        let codecs = parse_names(names).map_err(|e| AdapterError::failed(e.to_string()))?;
        out.session_update(&video_codec_list(&codecs, true))?;
        Ok(true)
    }

    fn network_stats(&self) -> Value {
        let out = self.st.lock().out.clone();
        out.and_then(|out| out.receiver().ok())
            .map_or_else(|| json!({}), |r| json!(r.stats()))
    }

    /// Stops the output first, then disconnects the stream and leaves voice.
    async fn end(&self) -> Result<(), AdapterError> {
        let (out, vc, health, pump) = {
            let mut st = self.st.lock();
            st.viewers = None;
            st.codec_hook = None;
            (
                st.out.take(),
                st.vc.take(),
                st.health.take(),
                st.vc_pump.take(),
            )
        };
        if let Some(health) = health {
            health.abort();
        }
        if let Some(out) = out {
            out.disconnect().await;
        }
        if let Some(vc) = vc {
            self.st.lock().leaving = Some(vc.clone());
            vc.disconnect(LEAVE_TIMEOUT).await;
            self.st.lock().leaving = None;
        }
        if let Some(net) = self.net() {
            net.set_rtc_connected(false);
        }
        if let Some(pump) = pump {
            pump.abort();
        }
        Ok(())
    }

    async fn close(&self) {
        if let Err(e) = self.end().await {
            self.log(
                "close_step_error",
                json!({"account": self.opts.user_id, "error": e.to_string()}),
            );
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

    fn on_client_event(&self, event: ClientEvent) {
        match event {
            ClientEvent::SessionUpdate { stream_key, info } => {
                let hook = self.st.lock().codec_hook.clone();
                if let (Some(key), Some((hook_key, generation, attempt)), Some(codec)) =
                    (stream_key, hook, info.video_codec)
                    && key == hook_key
                {
                    self.emit(PoolEvent::OutCodecChanged {
                        account: self.opts.user_id,
                        generation,
                        attempt,
                        codec: codec.to_ascii_uppercase(),
                    });
                }
            }
            other => self.log(
                "voice_event",
                json!({"account": self.opts.user_id, "kind": format!("{other:?}")}),
            ),
        }
    }
}

/// `watch_health`'s verdict over successive polls. A stream or voice connection that is down but
/// on its way back (server move, forced close waiting for a server) is tolerated for up to
/// [`RECONNECT_TIMEOUT`]; one that gave up (`ClientEvent::Disconnected`) is not.
#[derive(Default)]
struct HealthWatch {
    recovering_since: Option<Instant>,
}

impl HealthWatch {
    /// `up`: both connections are live; `recovering`: each is live or reconnecting.
    fn healthy(&mut self, up: bool, recovering: bool, now: Instant) -> bool {
        if up {
            self.recovering_since = None;
            return true;
        }
        if !recovering {
            return false;
        }
        let since = *self.recovering_since.get_or_insert(now);
        now.saturating_duration_since(since) < RECONNECT_TIMEOUT
    }
}

/// A dropped stream or voice connection without a voice-state change is reported once as
/// unhealthy (see [`HealthWatch`] for reconnects).
async fn watch_health(
    adapter: Weak<Inner>,
    generation: u64,
    attempt: u64,
    out: StreamClient,
    vc: VoiceClient,
) {
    let mut health = HealthWatch::default();
    loop {
        let Some(inner) = adapter.upgrade() else {
            return;
        };
        let (out_up, vc_up) = (out.is_connected(), vc.is_connected());
        let recovering = (out_up || out.is_reconnecting()) && (vc_up || vc.is_reconnecting());
        if !health.healthy(out_up && vc_up, recovering, Instant::now()) {
            inner.emit(PoolEvent::OutHealth {
                account: inner.opts.user_id,
                generation,
                attempt,
                ok: false,
            });
            return;
        }
        let poll = inner.opts.health_poll;
        drop(inner);
        tokio::time::sleep(poll).await;
    }
}

async fn run_gateway(adapter: Weak<Inner>, mut events: UnboundedReceiver<GatewayEvent>) {
    while let Some(event) = events.recv().await {
        let Some(inner) = adapter.upgrade() else {
            return;
        };
        match event {
            GatewayEvent::Dispatch(dispatch) => inner.handle_dispatch(&dispatch),
            GatewayEvent::Disconnected { .. } => {}
            GatewayEvent::Closed(error) => {
                let before = inner.phase.send_replace(Phase::Closed(error.to_string()));
                // Before READY `start` reports the failure; afterwards nobody else would notice.
                if before == Phase::Ready {
                    inner.on_gateway_closed(&error);
                }
                return;
            }
        }
    }
    if let Some(inner) = adapter.upgrade() {
        inner.phase.send_if_modified(|phase| {
            if *phase == Phase::Connecting {
                *phase = Phase::Closed("gateway ended before READY".into());
                true
            } else {
                false
            }
        });
    }
}

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

impl OutputAdapter for OutputAccountAdapter {
    fn user_id(&self) -> u64 {
        self.inner.opts.user_id
    }

    fn display_name(&self) -> Option<String> {
        self.inner
            .cache
            .read()
            .display_name(self.inner.opts.guild_id, self.inner.opts.user_id)
    }

    fn start(&self, token: Token) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.start(token).await })
    }

    fn publish(
        &self,
        generation: u64,
        attempt: u64,
        source: OutputSource,
        params: relay_voice::publish::VideoParams,
        channel_id: u64,
        adaptive: bool,
    ) -> ApiFuture<'_, Option<String>> {
        Box::pin(async move {
            self.inner
                .publish(generation, attempt, source, params, channel_id, adaptive)
                .await
        })
    }

    fn update_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool> {
        Box::pin(async move { self.inner.update_codecs(names).await })
    }

    fn swap_source(&self, make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>> {
        Box::pin(async move {
            let out = self.inner.st.lock().out.clone();
            swap_source(out.as_ref(), make_source, &self.inner.log)
        })
    }

    fn end(&self) -> ApiFuture<'_, ()> {
        Box::pin(async move { self.inner.end().await })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.inner.close().await })
    }

    fn network_stats(&self) -> Value {
        self.inner.network_stats()
    }
}

#[cfg(test)]
mod tests;
