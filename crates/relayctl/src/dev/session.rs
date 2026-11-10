//! One logged-in account of a live probe: login, gateway, join the voice channel, watch a Go Live and
//! keep the books. Shared by `dev watch` (one session) and `dev relay` (a relay and a viewer session).
//!
//! JSON log lines go to the shared writer through `JsonLogger` (every secret of the run redacted); a
//! session with a `role` prefixes each line with it.

use std::fmt::Display;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use relay_core::clock::monotonic;
use relay_core::obs::JsonLogger;
use relay_discord::codecs::adaptive_decode_codecs;
use relay_discord::link::{GatewayLinkImpl, link_event, snowflake, target_is_live};
use relay_discord::{
    Dispatch, Gateway, GatewayConfig, GatewayEvent, GatewayHandle, RestClient, SharedCache,
    StreamKey, Token, Urls,
};
use relay_voice::dave::DaveSnapshot;
use relay_voice::receive::{MediaType, Packet, RtpObservation};
use relay_voice::voice_ws::StreamSpec;
use relay_voice::{
    ClientEvent, JoinOptions, StreamClient, VoiceClient, VoiceClientConfig,
    connection::{RtcConnection, RtcSettings},
};
use serde_json::{Map, Value, json};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use super::stats::{Milestone, Progress, WatchAccount};
use crate::tokens::redact;

pub const READY_TIMEOUT: Duration = Duration::from_secs(60);
pub const GUILD_TIMEOUT: Duration = Duration::from_secs(10);
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(15);
pub const WATCH_TIMEOUT: Duration = Duration::from_secs(15);
pub const TEARDOWN_STEP: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_millis(250);
pub const STATS_INTERVAL_S: f64 = 5.0;
pub const REQUEST_QUALITY: u32 = 100;

/// Called on the UDP dispatcher thread with every packet of the watched owner, after the accounting.
pub type PacketTap = Arc<dyn Fn(&Packet) + Send + Sync>;

type EventHook = Box<dyn Fn(&ClientEvent) + Send + Sync>;

#[derive(Default)]
pub struct Handles {
    pub gateway: Option<GatewayHandle>,
    pub voice: Option<VoiceClient>,
    pub stream: Option<StreamClient>,
    pub pump: Option<JoinHandle<()>>,
}

/// What the last sample of the watch connection saw; kept for the summary after the connection is gone.
#[derive(Default)]
pub struct Observed {
    pub dave_protocol_version: Option<u16>,
    pub transport_failures: u64,
    pub dave_failures: u64,
}

pub struct Probe {
    /// Log line prefix (`relay`, `viewer`); `None` for the single-session `dev watch`.
    role: Option<&'static str>,
    pub log: JsonLogger,
    pub token: String,
    /// Every secret of the run: all of them are redacted from anything this session reports.
    secrets: Vec<String>,
    pub guild_id: u64,
    /// The owner of the Go Live this session watches.
    target_id: AtomicU64,
    pub channel_id: u64,
    pub started: f64,
    pub account: Mutex<WatchAccount>,
    pub progress: Mutex<Progress>,
    pub observed: Mutex<Observed>,
    pub notify: Notify,
    pub gateway_closed: AtomicBool,
    pub handles: Mutex<Handles>,
    event_hook: OnceLock<EventHook>,
}

/// A session that is logged in, has its gateway READY and sits in the voice channel.
pub struct Connected {
    pub rest: RestClient,
    pub user_id: u64,
    pub voice: VoiceClient,
    pub voice_events: UnboundedReceiver<ClientEvent>,
    pub cache: SharedCache,
}

impl Probe {
    pub fn new(
        writer: Arc<StdMutex<dyn Write + Send>>,
        role: Option<&'static str>,
        token: String,
        other_secrets: &[String],
        guild_id: u64,
        target_id: u64,
        channel_id: u64,
    ) -> Self {
        let mut secrets = vec![token.clone()];
        secrets.extend(other_secrets.iter().cloned());
        Self {
            role,
            log: JsonLogger::new(writer, secrets.iter()),
            token,
            secrets,
            guild_id,
            target_id: AtomicU64::new(target_id),
            channel_id,
            started: monotonic(),
            account: Mutex::new(WatchAccount::new()),
            progress: Mutex::new(Progress::default()),
            observed: Mutex::new(Observed::default()),
            notify: Notify::new(),
            gateway_closed: AtomicBool::new(false),
            handles: Mutex::new(Handles::default()),
            event_hook: OnceLock::new(),
        }
    }

    pub fn target_id(&self) -> u64 {
        self.target_id.load(Ordering::Relaxed)
    }

    pub fn set_target_id(&self, id: u64) {
        self.target_id.store(id, Ordering::Relaxed);
    }

    /// Sees every voice/stream client event before the session reacts to it.
    pub fn set_event_hook(&self, hook: impl Fn(&ClientEvent) + Send + Sync + 'static) {
        let _ = self.event_hook.set(Box::new(hook));
    }

    pub fn redact_secrets(&self, text: &str) -> String {
        self.secrets
            .iter()
            .fold(text.to_owned(), |text, secret| redact(&text, secret))
    }

    pub fn log(&self, event: &str, fields: Value) {
        let mut map = Map::new();
        if let Some(role) = self.role {
            map.insert("role".into(), json!(role));
        }
        if let Value::Object(fields) = fields {
            map.extend(fields);
        }
        self.log.event(event, map);
    }

    /// Records a failure for the summary (secrets redacted) and logs it.
    pub fn error(&self, message: impl Display) {
        let message = self.redact_secrets(&message.to_string());
        self.log("error", json!({"message": message}));
        self.progress.lock().errors.push(message);
    }

    fn who(&self, user_id: u64) -> &'static str {
        if user_id == self.target_id() {
            "target"
        } else {
            "other"
        }
    }

    fn log_milestone(&self, milestone: Milestone) {
        let t = monotonic() - self.started;
        match milestone {
            Milestone::VideoPacket { ssrc, payload_type } => self.log(
                "first_video_packet",
                json!({"ssrc": ssrc, "payload_type": payload_type, "t_s": t}),
            ),
            Milestone::AccessUnit {
                ssrc,
                codec,
                keyframe,
                bytes,
            } => self.log(
                "first_access_unit",
                json!({"ssrc": ssrc, "codec": codec, "keyframe": keyframe, "bytes": bytes, "t_s": t}),
            ),
            Milestone::Frame(frame) => {
                let mut fields = frame.to_json();
                fields["t_s"] = json!(t);
                self.log("first_frame", fields);
            }
        }
    }

    pub async fn wait_until(&self, timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if cond() {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let step = (deadline - now).min(Duration::from_millis(500));
            let _ = tokio::time::timeout(step, self.notify.notified()).await;
        }
    }

    fn on_dispatch(&self, dispatch: &Dispatch, voice: &VoiceClient, self_id: u64) {
        let data = &dispatch.data;
        match dispatch.name.as_str() {
            "READY" => {
                self.progress.lock().ready = true;
                self.log(
                    "ready",
                    json!({
                        "user_id": data.get("user").and_then(|u| u.get("id")).and_then(snowflake),
                        "guilds": data.get("guilds").and_then(Value::as_array).map(Vec::len),
                    }),
                );
            }
            "READY_SUPPLEMENTAL" => self.log("ready_supplemental", json!({})),
            "VOICE_STATE_UPDATE" => {
                let user = data.get("user_id").and_then(snowflake);
                if let Some(user) = user.filter(|u| *u == self.target_id() || *u == self_id) {
                    self.log(
                        "voice_state",
                        json!({
                            "user_id": user,
                            "who": if user == self_id { "self" } else { "target" },
                            "channel_id": data.get("channel_id").and_then(snowflake),
                            "self_stream": data.get("self_stream").and_then(Value::as_bool),
                        }),
                    );
                }
            }
            "VOICE_SERVER_UPDATE" => self.log(
                "voice_server_update",
                json!({"endpoint": data.get("endpoint").and_then(Value::as_str)}),
            ),
            "STREAM_CREATE" | "STREAM_UPDATE" | "STREAM_DELETE" | "STREAM_SERVER_UPDATE" => {
                let key = data.get("stream_key").and_then(Value::as_str);
                let who = key
                    .and_then(|k| k.parse::<StreamKey>().ok())
                    .map(|k| self.who(k.owner_id()));
                self.log(
                    "stream_event",
                    json!({
                        "event": dispatch.name,
                        "key": key,
                        "who": who,
                        "reason": data.get("reason").and_then(Value::as_str),
                        "paused": data.get("paused").and_then(Value::as_bool),
                    }),
                );
            }
            _ => {}
        }
        if let Some(event) = link_event(&dispatch.name, data, self_id) {
            voice.handle_event(event);
        }
        self.notify.notify_waiters();
    }

    /// A voice/stream client event; `Some(reason)` ends the watch.
    pub fn on_client_event(&self, event: ClientEvent) -> Option<String> {
        if let Some(hook) = self.event_hook.get() {
            hook(&event);
        }
        match event {
            ClientEvent::SessionUpdate { stream_key, info } => {
                self.log(
                    "session_update",
                    json!({
                        "stream_key": stream_key,
                        "audio_codec": info.audio_codec,
                        "video_codec": info.video_codec,
                        "keyframe_interval": info.keyframe_interval,
                    }),
                );
                None
            }
            ClientEvent::DaveError { stream_key, error } => {
                self.log("dave_error", json!({"stream_key": stream_key}));
                self.error(format!("DAVE error: {error}"));
                None
            }
            ClientEvent::Reconnected { stream_key } => {
                self.log("reconnected", json!({"stream_key": stream_key}));
                None
            }
            ClientEvent::StreamTokenRefreshed { stream_key } => {
                self.log("stream_token_refreshed", json!({"stream_key": stream_key}));
                None
            }
            ClientEvent::StreamPaused { stream_key, paused } => {
                self.log(
                    "stream_paused",
                    json!({"stream_key": stream_key, "paused": paused}),
                );
                None
            }
            ClientEvent::StreamEnded {
                stream_key,
                reason,
                unavailable,
            } => {
                self.log(
                    "stream_ended",
                    json!({"stream_key": stream_key, "reason": reason, "unavailable": unavailable}),
                );
                Some(format!("stream ended: {reason}"))
            }
            ClientEvent::Disconnected {
                stream_key,
                code,
                reason,
            } => {
                self.log(
                    "disconnected",
                    json!({"stream_key": stream_key, "code": code, "reason": reason}),
                );
                Some(format!(
                    "{} disconnected (code {code:?}): {reason}",
                    if stream_key.is_some() {
                        "watch"
                    } else {
                        "voice"
                    }
                ))
            }
        }
    }

    /// Reads counters off the watch connection and remembers them for the summary.
    pub fn sample(&self, stream: &StreamClient) -> (Value, Value) {
        let Ok(conn) = stream.connection() else {
            return (json!({}), json!({}));
        };
        let receiver_stats = conn.receiver().stats();
        let transport = receiver_stats
            .get("rtp_decrypt_failures")
            .copied()
            .unwrap_or(0);
        let dave = conn.dave().decrypt_failures();
        {
            let mut seen = self.observed.lock();
            seen.transport_failures = transport;
            seen.dave_failures = dave;
            seen.dave_protocol_version = Some(conn.dave().protocol_version());
        }
        let udp = conn.udp_stats();
        (
            json!(receiver_stats),
            json!({
                "rtp_packets_received": udp.rtp_packets_received,
                "rtcp_packets_received": udp.rtcp_packets_received,
                "invalid_packets_received": udp.invalid_packets_received,
            }),
        )
    }

    fn log_stats(&self, stream: &StreamClient, voice: &VoiceClient, now: f64, start: f64) {
        let (receiver, udp) = self.sample(stream);
        let (transport, dave) = {
            let seen = self.observed.lock();
            (seen.transport_failures, seen.dave_failures)
        };
        let mut fields = {
            let mut account = self.account.lock();
            let fps = account.window_fps(now, start);
            account.stats_fields(fps, transport, dave)
        };
        fields["receiver"] = receiver;
        fields["udp"] = udp;
        fields["voice_connected"] = json!(voice.is_connected());
        fields["watch_connected"] = json!(stream.is_connected());
        self.log("stats", fields);
    }

    /// Login, gateway READY, guild present, voice channel joined (self-muted, like `session.py`).
    pub async fn connect(self: &Arc<Self>) -> Result<Connected, String> {
        let rest = RestClient::new(Urls::default(), Token::new(self.token.clone()))
            .await
            .map_err(|e| format!("http client: {e}"))?;
        let user = rest.login().await.map_err(|e| format!("login: {e}"))?;
        self.progress.lock().login_user_id = Some(user.id);
        self.log("login_ok", json!({"user_id": user.id}));

        let (gateway, events) = Gateway::spawn(GatewayConfig::from_rest(&rest));
        let mut rtc = RtcSettings::new(
            adaptive_decode_codecs(),
            vec![StreamSpec::video(REQUEST_QUALITY)],
            true,
        );
        rtc.user_agent = rest.headers_context().user_agent().to_owned();
        let (voice, voice_events) = VoiceClient::new(VoiceClientConfig {
            user_id: user.id,
            guild_id: Some(self.guild_id),
            channel_id: self.channel_id,
            gateway: Arc::new(GatewayLinkImpl {
                gateway: gateway.clone(),
            }),
            http: rest.http_client().clone(),
            rtc,
        });
        {
            let pump = tokio::spawn(pump(Arc::clone(self), events, voice.clone(), user.id));
            let mut handles = self.handles.lock();
            handles.gateway = Some(gateway.clone());
            handles.voice = Some(voice.clone());
            handles.pump = Some(pump);
        }

        self.wait_until(READY_TIMEOUT, || {
            self.progress.lock().ready || self.gateway_closed.load(Ordering::Relaxed)
        })
        .await;
        if !self.progress.lock().ready {
            return Err("gateway did not reach READY".into());
        }
        let cache: SharedCache = gateway.cache().clone();
        if !self
            .wait_until(GUILD_TIMEOUT, || cache.read().has_guild(self.guild_id))
            .await
        {
            return Err(format!(
                "guild {} is not in this account's READY (not a member?)",
                self.guild_id
            ));
        }

        voice
            .connect(JoinOptions {
                timeout: JOIN_TIMEOUT,
                self_mute: true,
                self_deaf: false,
                on_voice_state: None,
            })
            .await
            .map_err(|e| format!("join voice channel {}: {e}", self.channel_id))?;
        gateway.set_rtc_connected(true);
        self.progress.lock().voice_joined = true;
        self.log("voice_joined", json!({"channel_id": self.channel_id}));
        Ok(Connected {
            rest,
            user_id: user.id,
            voice,
            voice_events,
            cache,
        })
    }

    /// Waits until the target owner is live in the channel, watches its Go Live and listens.
    pub async fn watch_target(
        self: &Arc<Self>,
        conn: &Connected,
        wait: Duration,
        tap: Option<PacketTap>,
    ) -> Result<StreamClient, String> {
        let key = StreamKey::Guild {
            guild_id: self.guild_id,
            channel_id: self.channel_id,
            owner_id: self.target_id(),
        };
        self.log("waiting_for_target_stream", json!({"key": key.to_string()}));
        let live = self
            .wait_until(wait, || {
                let cache = conn.cache.read();
                target_is_live(
                    cache.voice_state(self.guild_id, self.target_id()),
                    cache.stream(&key).is_some(),
                    self.channel_id,
                ) || self.gateway_closed.load(Ordering::Relaxed)
            })
            .await;
        if !live || self.gateway_closed.load(Ordering::Relaxed) {
            return Err(format!(
                "target {} did not go live in channel {} within {} s",
                self.target_id(),
                self.channel_id,
                wait.as_secs()
            ));
        }
        self.progress.lock().stream_key_found = true;
        self.log("stream_found", json!({"key": key.to_string()}));

        let mut settings = RtcSettings::watch(adaptive_decode_codecs(), true);
        settings.user_agent = conn.rest.headers_context().user_agent().to_owned();
        settings.receive.debug_stats = true;
        settings.receive.rtp_tap = Some(rtp_tap(Arc::clone(self)));
        let stream = conn
            .voice
            .watch_stream(&key.to_string(), settings, WATCH_TIMEOUT)
            .await
            .map_err(|e| format!("watch {key}: {e}"))?;
        self.handles.lock().stream = Some(stream.clone());
        stream
            .listen(packet_callback(Arc::clone(self), tap))
            .map_err(|e| format!("listen: {e}"))?;
        self.progress.lock().watch_connected = true;
        self.log("watch_connected", json!({"key": key.to_string()}));
        Ok(stream)
    }

    /// Stop listening, leave the voice channel and close the gateway.
    pub async fn teardown(&self) {
        self.log("teardown_start", json!({}));
        let (gateway, voice, stream, pump) = {
            let mut handles = self.handles.lock();
            (
                handles.gateway.take(),
                handles.voice.take(),
                handles.stream.take(),
                handles.pump.take(),
            )
        };
        if let Some(stream) = &stream {
            self.sample(stream);
            stream.stop_listening();
            let done = tokio::time::timeout(TEARDOWN_STEP, stream.disconnect()).await;
            self.log(
                "teardown_step",
                json!({"step": "watch_disconnect", "ok": done.is_ok()}),
            );
        }
        if let Some(voice) = &voice {
            let done =
                tokio::time::timeout(TEARDOWN_STEP * 2, voice.disconnect(TEARDOWN_STEP)).await;
            self.log(
                "teardown_step",
                json!({"step": "vc_disconnect", "ok": done.is_ok()}),
            );
        }
        if let Some(gateway) = &gateway {
            gateway.set_rtc_connected(false);
            gateway.close();
        }
        if let Some(pump) = pump {
            let abort = pump.abort_handle();
            let closed = tokio::time::timeout(TEARDOWN_STEP, pump).await.is_ok();
            if !closed {
                abort.abort();
            }
            self.log(
                "teardown_step",
                json!({"step": "gateway_close", "ok": closed}),
            );
        }
        let (transport, dave) = {
            let seen = self.observed.lock();
            (seen.transport_failures, seen.dave_failures)
        };
        let fields = self.account.lock().stats_fields(0.0, transport, dave);
        self.log("final_stats", fields);
    }
}

/// Gateway events: log the interesting ones, feed the voice clients, wake the waiting flow.
async fn pump(
    probe: Arc<Probe>,
    mut events: UnboundedReceiver<GatewayEvent>,
    voice: VoiceClient,
    self_id: u64,
) {
    while let Some(event) = events.recv().await {
        match event {
            GatewayEvent::Dispatch(dispatch) => probe.on_dispatch(&dispatch, &voice, self_id),
            GatewayEvent::Disconnected {
                code,
                reason,
                resume,
            } => probe.log(
                "gateway_disconnected",
                json!({"code": code, "reason": reason, "resume": resume}),
            ),
            GatewayEvent::Closed(error) => probe.error(format!("gateway closed: {error}")),
        }
    }
    probe.gateway_closed.store(true, Ordering::Relaxed);
    probe.notify.notify_waiters();
}

fn rtp_tap(probe: Arc<Probe>) -> relay_voice::receive::RtpTap {
    Arc::new(move |obs: &RtpObservation<'_>| {
        let milestone = probe
            .account
            .lock()
            .on_rtp(obs.ssrc, obs.payload_type, monotonic());
        if let Some(milestone) = milestone {
            probe.log_milestone(milestone);
        }
    })
}

/// Runs on the UDP dispatcher thread: account only, no awaiting.
fn packet_callback(
    probe: Arc<Probe>,
    tap: Option<PacketTap>,
) -> impl FnMut(Packet) + Send + 'static {
    move |packet: Packet| {
        if packet.user_id.is_some_and(|user| user != probe.target_id()) {
            return;
        }
        match packet.media_type {
            MediaType::Video => {
                let milestones = probe.account.lock().on_access_unit(
                    packet.ssrc,
                    packet.codec.name(),
                    &packet.payload,
                    monotonic(),
                );
                for milestone in milestones {
                    probe.log_milestone(milestone);
                }
            }
            MediaType::Audio => probe.account.lock().on_audio_frame(packet.ssrc),
        }
        if let Some(tap) = &tap {
            tap(&packet);
        }
    }
}

pub fn dave_view(conn: &RtcConnection, target_id: u64) -> (DaveSnapshot, bool) {
    (conn.dave().snapshot(), conn.dave().can_decrypt(target_id))
}

pub fn log_dave_change(
    probe: &Probe,
    which: &str,
    last: &mut Option<(DaveSnapshot, bool)>,
    now: (DaveSnapshot, bool),
) {
    if last.as_ref() == Some(&now) {
        return;
    }
    let (snapshot, can_decrypt) = &now;
    probe.log(
        "dave_state",
        json!({
            "connection": which,
            "protocol_version": snapshot.protocol_version,
            "has_session": snapshot.has_session,
            "status": snapshot.status,
            "ready": snapshot.ready,
            "epoch": snapshot.epoch,
            "pending_transitions": snapshot.pending_transitions,
            "downgraded": snapshot.downgraded,
            "can_decrypt_target": can_decrypt,
        }),
    );
    *last = Some(now);
}

/// Watches `stream` for `seconds` (`None`: until something ends it or the caller drops the future):
/// DAVE changes, the first video request and PLIs, periodic stats, connection health.
pub async fn watch_loop(
    probe: &Arc<Probe>,
    voice: &VoiceClient,
    stream: &StreamClient,
    voice_events: &mut UnboundedReceiver<ClientEvent>,
    seconds: Option<u64>,
) {
    let start = monotonic();
    let end = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_stats = start + STATS_INTERVAL_S;
    let mut last_voice_dave = None;
    let mut last_watch_dave = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            event = voice_events.recv() => {
                let Some(event) = event else {
                    probe.error("voice client event stream ended");
                    break;
                };
                if let Some(reason) = probe.on_client_event(event) {
                    probe.error(reason);
                    break;
                }
                continue;
            }
            () = async {
                match end {
                    Some(end) => tokio::time::sleep_until(end).await,
                    None => std::future::pending().await,
                }
            } => {
                probe.log("duration_elapsed", json!({"seconds": seconds}));
                break;
            }
        }
        let now = monotonic();

        if let Some(conn) = voice.connection() {
            log_dave_change(
                probe,
                "voice",
                &mut last_voice_dave,
                dave_view(&conn, probe.target_id()),
            );
        }
        if let Ok(conn) = stream.connection() {
            log_dave_change(
                probe,
                "watch",
                &mut last_watch_dave,
                dave_view(&conn, probe.target_id()),
            );
        }

        let (first_request, pli) = {
            let mut account = probe.account.lock();
            let first = account.take_video_ssrc_to_request();
            let pli = if first.is_some() {
                None
            } else {
                account.pli_due(now)
            };
            if first.is_some() || pli.is_some() {
                account.note_pli(now);
            }
            (first, pli)
        };
        if let Some(ssrc) = first_request {
            let requested = stream.request_video(ssrc, REQUEST_QUALITY);
            let pli = stream.send_pli(ssrc);
            probe.log(
                "video_requested",
                json!({
                    "ssrc": ssrc,
                    "request_ok": requested.is_ok(),
                    "pli_ok": pli.is_ok(),
                }),
            );
        }
        if let Some(ssrc) = pli {
            let sent = stream.send_pli(ssrc);
            probe.log("pli_request", json!({"ssrc": ssrc, "ok": sent.is_ok()}));
        }

        if now >= next_stats {
            next_stats += STATS_INTERVAL_S;
            probe.log_stats(stream, voice, now, start);
        }

        if probe.gateway_closed.load(Ordering::Relaxed) {
            probe.error("gateway closed during the watch");
            break;
        }
        if !voice.is_connected() {
            probe.error("voice connection lost");
            break;
        }
        if !stream.is_connected() {
            probe.error("watch connection lost");
            break;
        }
    }
    probe.sample(stream);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(token: &str, role: Option<&'static str>) -> (Probe, Arc<StdMutex<Vec<u8>>>) {
        let buffer = Arc::new(StdMutex::new(Vec::new()));
        let writer: Arc<StdMutex<dyn Write + Send>> = buffer.clone();
        (
            Probe::new(writer, role, token.to_owned(), &[], 1, 2, 3),
            buffer,
        )
    }

    fn logged(buffer: &StdMutex<Vec<u8>>) -> String {
        String::from_utf8(buffer.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn errors_are_redacted_in_the_summary_and_the_log() {
        let (probe, buffer) = probe("tok-123.secret", None);
        probe.error("login failed for tok-123.secret (tok-123.secret)");
        let errors = probe.progress.lock().errors.clone();
        assert_eq!(errors, ["login failed for *** (***)"]);
        let log = logged(&buffer);
        assert!(!log.contains("tok-123"), "{log}");
        assert!(log.contains(r#""event":"error""#), "{log}");
    }

    #[test]
    fn log_lines_never_carry_the_token_even_in_free_text() {
        let (probe, buffer) = probe("tok-123.secret", None);
        probe.log(
            "note",
            json!({"detail": "echo tok-123.secret", "token": "tok-123.secret"}),
        );
        let log = logged(&buffer);
        assert!(!log.contains("tok-123"), "{log}");
    }

    #[test]
    fn every_secret_of_the_run_is_redacted() {
        let buffer = Arc::new(StdMutex::new(Vec::new()));
        let writer: Arc<StdMutex<dyn Write + Send>> = buffer.clone();
        let probe = Probe::new(
            writer,
            Some("viewer"),
            "own-token".into(),
            &["other-token".to_owned()],
            1,
            2,
            3,
        );
        probe.error("saw own-token and other-token");
        assert_eq!(probe.progress.lock().errors, ["saw *** and ***"]);
        let log = logged(&buffer);
        assert!(
            !log.contains("own-token") && !log.contains("other-token"),
            "{log}"
        );
        assert_eq!(probe.redact_secrets("x other-token"), "x ***");
    }

    #[test]
    fn log_lines_carry_the_role_prefix_only_when_there_is_one() {
        let (with, buffer) = probe("t", Some("relay"));
        with.log("note", json!({"n": 1}));
        let log = logged(&buffer);
        assert!(
            log.contains(r#""event":"note","role":"relay","n":1"#),
            "{log}"
        );

        let (without, buffer) = probe("t", None);
        without.log("note", json!({"n": 1}));
        assert!(!logged(&buffer).contains("role"));
    }

    #[test]
    fn the_watched_owner_can_be_set_after_construction() {
        let (probe, _buffer) = probe("t", None);
        assert_eq!(probe.target_id(), 2);
        probe.set_target_id(99);
        assert_eq!(probe.target_id(), 99);
        assert_eq!(probe.who(99), "target");
        assert_eq!(probe.who(2), "other");
    }

    #[test]
    fn dave_state_is_logged_only_when_it_changes() {
        let (probe, buffer) = probe("t", None);
        let snapshot = DaveSnapshot {
            protocol_version: 1,
            has_session: true,
            status: Some("PENDING".into()),
            ready: false,
            epoch: None,
            pending_transitions: 0,
            downgraded: false,
        };
        let mut last = None;
        log_dave_change(&probe, "watch", &mut last, (snapshot.clone(), false));
        log_dave_change(&probe, "watch", &mut last, (snapshot.clone(), false));
        let ready = DaveSnapshot {
            ready: true,
            epoch: Some(1),
            status: Some("ACTIVE".into()),
            ..snapshot
        };
        log_dave_change(&probe, "watch", &mut last, (ready, true));
        let log = logged(&buffer);
        assert_eq!(log.matches(r#""event":"dave_state""#).count(), 2, "{log}");
        assert!(log.contains(r#""epoch":1"#), "{log}");
        assert!(log.contains(r#""can_decrypt_target":true"#), "{log}");
    }

    #[test]
    fn client_events_end_the_watch_only_when_the_connection_is_gone() {
        let (probe, _buffer) = probe("t", None);
        let paused = ClientEvent::StreamPaused {
            stream_key: "k".into(),
            paused: true,
        };
        assert_eq!(probe.on_client_event(paused), None);
        let ended = ClientEvent::StreamEnded {
            stream_key: "k".into(),
            reason: "user_requested".into(),
            unavailable: false,
        };
        assert_eq!(
            probe.on_client_event(ended).as_deref(),
            Some("stream ended: user_requested")
        );
        let dave = ClientEvent::DaveError {
            stream_key: None,
            error: "boom".into(),
        };
        assert_eq!(probe.on_client_event(dave), None);
        assert_eq!(probe.progress.lock().errors, ["DAVE error: boom"]);
    }

    #[test]
    fn the_event_hook_sees_events_before_the_session_reacts() {
        let (probe, _buffer) = probe("t", None);
        let seen = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&seen);
        probe.set_event_hook(move |event| {
            if matches!(event, ClientEvent::StreamPaused { .. }) {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        let paused = ClientEvent::StreamPaused {
            stream_key: "k".into(),
            paused: false,
        };
        probe.on_client_event(paused);
        assert_eq!(seen.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn wait_until_returns_when_the_condition_holds_or_time_runs_out() {
        let (probe, _buffer) = probe("t", None);
        assert!(probe.wait_until(Duration::from_millis(50), || true).await);
        let mut calls = 0;
        let started = tokio::time::Instant::now();
        let ok = probe
            .wait_until(Duration::from_millis(80), || {
                calls += 1;
                false
            })
            .await;
        assert!(!ok);
        assert!(calls >= 1);
        assert!(started.elapsed() >= Duration::from_millis(80));
    }
}
