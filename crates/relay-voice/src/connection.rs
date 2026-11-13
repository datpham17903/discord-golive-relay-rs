//! One RTC media connection: voice websocket, UDP transport, transport crypto, DAVE and the
//! receive pipeline. The voice client and every stream client are built on one of these.
//!
//! Port of `discord/ext/native_voice/client.py` (`_ConnectionState`, `_connect_rtc_transport`,
//! `request_video`, `send_video_pli`, `listen`) and the handshake of `discord/gateway.py`
//! (`DiscordVoiceWebSocket.initial_connection` / `received_message`) on top of [`VoiceWs`].
//!
//! The handshake is READY, UDP connect + IP discovery, SELECT_PROTOCOL, SESSION_DESCRIPTION. After it
//! the connection announces itself the way the Python client does: an empty receiver report, UDP
//! ping, and an empty VIDEO (op 12) so the server starts sending video. A websocket that drops the
//! resumable way (4015, no close code) is resumed underneath, keeping UDP, keys and DAVE
//! ([`ConnEvent::Resumed`]); when it ends for good the owner gets [`ConnEvent::Closed`] and decides
//! about reconnecting.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

use crate::dave::Dave;
use crate::error::VoiceError;
use crate::receive::{Packet, ReceiveConfig, Receiver, SsrcMap, StreamSsrcs, VideoState};
use crate::transport::TransportEncryptionMode;
use crate::transport::net::{ListenerKinds, UdpStats, UdpTransport};
use crate::transport::packets::SharedTransportCrypto;
use crate::video::PacketSink;
use crate::voice_ws::{
    CodecSpec, SessionInfo, StreamSpec, VOICE_GATEWAY_VERSION, VoiceEvent, VoiceToken, VoiceWs,
    VoiceWsConfig, choose_mode, gateway_url, op,
};

/// The packet callback of a listening connection. It runs on the UDP dispatcher thread.
pub type MediaCallback = Arc<Mutex<Box<dyn FnMut(Packet) + Send>>>;

/// Wraps `callback` for [`RtcConnection::listen`].
pub fn media_callback(callback: impl FnMut(Packet) + Send + 'static) -> MediaCallback {
    Arc::new(Mutex::new(Box::new(callback)))
}

/// Where and as whom to connect (the voice or stream server named by the gateway).
#[derive(Clone, Debug)]
pub struct RtcTarget {
    /// `host[:port]`, without a scheme.
    pub endpoint: String,
    /// IDENTIFY `server_id`: the guild id (or channel id for calls); the RTC server id for a stream.
    pub server_id: u64,
    pub channel_id: u64,
    pub user_id: u64,
    pub session_id: String,
    pub token: VoiceToken,
    /// MLS group id: the voice channel id; for a stream `rtc_server_id - 1`.
    pub dave_group_id: u64,
}

/// Everything about a connection that is not the target.
#[derive(Clone)]
pub struct RtcSettings {
    /// Advertised codecs, RTX, receive tuning; `own_ssrc` is filled in at READY.
    pub receive: ReceiveConfig,
    /// IDENTIFY stream descriptors (`video_simulcast_streams`).
    pub streams: Vec<StreamSpec>,
    /// `wss` in production; tests use `ws` against a local fake.
    pub url_scheme: String,
    pub origin: String,
    /// Handshake `User-Agent`; the service passes the REST client's.
    pub user_agent: String,
    pub udp_buffer_size: usize,
    /// `udp_ping_initial_delay`, `udp_ping_interval`, `udp_ping_timeout`.
    pub ping: (Duration, Duration, Duration),
    /// Video send pacing, used when the connection publishes.
    pub send: SendSettings,
    /// Python's `reconnect`: when the websocket ended for good with anything but 1000 / 4021 (final)
    /// or 4014 / 4022 (forced; reconnected when the gateway names a server, regardless), a failed
    /// resume included (4015 / no close code are resumed regardless), handshake again with a fresh
    /// IDENTIFY instead of giving up. The relay passes `reconnect=False` everywhere, hence the
    /// default `false`.
    pub reconnect: bool,
}

/// `video_packet_burst_size` / `video_packet_burst_interval` of the native voice client.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SendSettings {
    /// RTP packets of a frame sent back to back before the next pause; 0 sends a frame at once.
    pub burst_size: usize,
    /// Seconds between bursts.
    pub burst_interval: f64,
}

impl Default for SendSettings {
    fn default() -> Self {
        Self {
            burst_size: 12,
            burst_interval: 0.001,
        }
    }
}

impl RtcSettings {
    /// `codecs` lose their RTX payload types when `rtx_enabled` is false, like `get_voice_codecs`.
    pub fn new(mut codecs: Vec<CodecSpec>, streams: Vec<StreamSpec>, rtx_enabled: bool) -> Self {
        if !rtx_enabled {
            for codec in &mut codecs {
                codec.rtx_payload_type = None;
            }
        }
        let mut receive = ReceiveConfig::new(codecs, 0);
        receive.rtx_enabled = rtx_enabled;
        Self {
            receive,
            streams,
            url_scheme: "wss".into(),
            origin: "https://discord.com".into(),
            user_agent: String::new(),
            udp_buffer_size: 4 * 1024 * 1024,
            ping: (
                Duration::from_millis(100),
                Duration::from_secs(5),
                Duration::from_secs(5),
            ),
            send: SendSettings::default(),
            reconnect: false,
        }
    }
}

/// What a running connection reports to its owner.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnEvent {
    /// op 14: the negotiated audio/video codec changed.
    SessionUpdate(SessionInfo),
    /// Websocket traffic the connection does not interpret itself (MEDIA_SINK_WANTS, CLIENTS_CONNECT,
    /// CLIENT_DISCONNECT, unknown ops).
    Message {
        op: u8,
        data: Value,
    },
    DaveError(String),
    /// The websocket dropped and was resumed (op 9 RESUMED); the connection carries on.
    Resumed,
    /// The websocket ended; the connection is dead. `code` is the close code when there was one.
    Closed {
        code: Option<u16>,
        reason: String,
    },
}

/// `timeout` of a reconnect: how long a forced close waits for the gateway to name a server, the
/// budget of a reconnect handshake, and how long a sender waits for a new connection.
pub const RECONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Handshakes tried by a `reconnect` (`_inner_connect` tries 5 times).
pub(crate) const RECONNECT_ATTEMPTS: u32 = 5;

/// What `_poll_voice_ws` does once the websocket has ended for good with `code` (the resumable
/// closes were already resumed, or given up on, by the connection).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AfterClose {
    /// 1000 and 4021 (rate limited), and anything else without `reconnect`.
    GiveUp,
    /// 4014 / 4022: disconnected or moved by Discord; reconnect when the gateway names a server.
    AwaitServer,
    /// `reconnect`: handshake again.
    Reconnect,
}

pub(crate) fn after_close(code: Option<u16>, reconnect: bool) -> AfterClose {
    match code {
        Some(4014 | 4022) => AfterClose::AwaitServer,
        Some(1000 | 4021) => AfterClose::GiveUp,
        _ if reconnect => AfterClose::Reconnect,
        _ => AfterClose::GiveUp,
    }
}

/// `_inner_connect`: up to `attempts` handshakes within `timeout`, pausing `1 + 2i` seconds after the
/// i-th failure. The last error is returned.
pub(crate) async fn connect_with_retries(
    http: &wreq::Client,
    target: &RtcTarget,
    settings: &RtcSettings,
    dave: &Arc<Dave>,
    timeout: Duration,
    attempts: u32,
) -> Result<(RtcConnection, mpsc::UnboundedReceiver<ConnEvent>), VoiceError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt = 0u32;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let error =
            match RtcConnection::connect(http, target, settings, Arc::clone(dave), remaining).await
            {
                Ok(connected) => return Ok(connected),
                Err(e) => e,
            };
        let pause = Duration::from_secs(1 + 2 * u64::from(attempt));
        attempt += 1;
        if attempt >= attempts || tokio::time::Instant::now() + pause >= deadline {
            return Err(error);
        }
        tokio::time::sleep(pause).await;
    }
}

#[derive(Default)]
struct Wants {
    by_ssrc: BTreeMap<u32, u32>,
}

/// `_local_media_sink_wants_any`.
const WANTS_ANY: u32 = 100;

struct Shared {
    info: Mutex<SessionInfo>,
    wants: Mutex<Wants>,
    closed: AtomicBool,
}

struct Listening {
    rtp: u64,
    nack_timer: Option<JoinHandle<()>>,
}

pub struct RtcConnection {
    ws: VoiceWs,
    udp: Arc<UdpTransport>,
    receiver: Arc<Receiver>,
    /// The outgoing cipher: one instance (one nonce counter) for RTCP and every RTP sent.
    crypto_tx: SharedTransportCrypto,
    /// The `streams` of READY: the SSRCs the server assigned to our video streams.
    ready_streams: Vec<Value>,
    rtcp_listener: Mutex<Option<u64>>,
    dave: Arc<Dave>,
    ssrcs: Arc<SsrcMap>,
    shared: Arc<Shared>,
    listening: Mutex<Option<Listening>>,
    pump: JoinHandle<()>,
    runtime: tokio::runtime::Handle,
    ssrc: u32,
    mode: TransportEncryptionMode,
}

impl RtcConnection {
    /// Runs the whole handshake within `timeout` and returns the live connection plus its event
    /// stream. Must be called inside a tokio runtime.
    pub async fn connect(
        http: &wreq::Client,
        target: &RtcTarget,
        settings: &RtcSettings,
        dave: Arc<Dave>,
        timeout: Duration,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ConnEvent>), VoiceError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut cfg = VoiceWsConfig::new(
            &target.endpoint,
            target.server_id,
            target.channel_id,
            target.user_id,
            target.session_id.clone(),
            target.token.clone(),
        );
        cfg.url = gateway_url(&target.endpoint, VOICE_GATEWAY_VERSION).replacen(
            "wss",
            &settings.url_scheme,
            1,
        );
        cfg.origin = settings.origin.clone();
        cfg.user_agent = settings.user_agent.clone();
        cfg.video = true;
        cfg.streams = settings.streams.clone();
        cfg.codecs = settings.receive.codecs.clone();
        cfg.connect_timeout = timeout;
        cfg.resume_on_drop = true;
        let modes = cfg.supported_modes.clone();

        let (ws, mut events) = VoiceWs::connect(http, cfg, Arc::clone(&dave)).await?;
        let ssrcs = Arc::new(SsrcMap::new());
        let udp = Arc::new(UdpTransport::new(Some(settings.udp_buffer_size), Some(0))?);
        let mut early: Vec<ConnEvent> = Vec::new();
        let mut own_ssrc = 0u32;
        let mut ready_streams = Vec::new();
        let (session, mode) = loop {
            let event = match timeout_at(deadline, events.recv()).await {
                Err(_) => {
                    ws.close(1000);
                    return Err(VoiceError::Timeout("waiting for the voice handshake"));
                }
                Ok(None) => {
                    return Err(VoiceError::Closed {
                        code: None,
                        reason: "event stream ended".into(),
                    });
                }
                Ok(Some(event)) => event,
            };
            match event {
                VoiceEvent::Ready(ready) => {
                    ssrcs.clear();
                    own_ssrc = ready.ssrc;
                    ready_streams = ready.streams.clone();
                    let mode = choose_mode(&modes, &ready.modes)
                        .ok_or_else(|| VoiceError::NoSupportedMode(ready.modes.clone()))?;
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let udp2 = Arc::clone(&udp);
                    let (ip, port) = (ready.ip.clone(), ready.port);
                    let ssrc = ready.ssrc;
                    let (external_ip, external_port) = tokio::task::spawn_blocking(move || {
                        udp2.connect(&ip, port)?;
                        udp2.discover_ip(ssrc, remaining)
                    })
                    .await
                    .map_err(|e| VoiceError::Invalid(format!("ip discovery task: {e}")))??;
                    ws.select_protocol(&external_ip, external_port, mode)?;
                }
                VoiceEvent::SessionDescription(sd) => {
                    let mode = TransportEncryptionMode::from_str(&sd.mode)?;
                    break (sd, mode);
                }
                VoiceEvent::Message { op, data } => {
                    apply_message(&ssrcs, op, &data);
                    early.push(ConnEvent::Message { op, data });
                }
                VoiceEvent::SessionUpdate(info) => early.push(ConnEvent::SessionUpdate(info)),
                VoiceEvent::DaveError(error) => early.push(ConnEvent::DaveError(error)),
                VoiceEvent::Closed { code, reason } => {
                    return Err(VoiceError::Closed { code, reason });
                }
                VoiceEvent::Resumed | VoiceEvent::BackendVersion { .. } => {}
            }
        };

        let crypto_rx = SharedTransportCrypto::new(mode, &session.secret_key)?;
        let crypto_tx = SharedTransportCrypto::new(mode, &session.secret_key)?;
        let crypto_tx_for_send = crypto_tx.clone();
        let mut receive_cfg = settings.receive.clone();
        receive_cfg.own_ssrc = own_ssrc;
        let sink: Arc<dyn PacketSink + Send + Sync> = Arc::clone(&udp) as _;
        let receiver = Arc::new(Receiver::new(
            receive_cfg,
            crypto_rx,
            crypto_tx,
            sink,
            Arc::clone(&dave),
            Arc::clone(&ssrcs),
        ));

        // `_connect_rtc_transport`, after the handshake: our (empty) receiver report, UDP ping, and
        // the empty VIDEO message that makes the server forward video to us.
        let _ = receiver.send_receiver_report(&[]);
        let (initial, interval, ping_timeout) = settings.ping;
        udp.start_ping(initial, interval, ping_timeout);
        ws.video_state(0, 0, &[])?;

        let shared = Arc::new(Shared {
            info: Mutex::new(session.info.clone()),
            wants: Mutex::new(Wants::default()),
            closed: AtomicBool::new(false),
        });
        let (tx, rx) = mpsc::unbounded_channel();
        for event in early {
            let _ = tx.send(event);
        }
        let pump = tokio::spawn(pump(events, tx, Arc::clone(&ssrcs), Arc::clone(&shared)));
        Ok((
            Self {
                ws,
                udp,
                receiver,
                crypto_tx: crypto_tx_for_send,
                ready_streams,
                rtcp_listener: Mutex::new(None),
                dave,
                ssrcs,
                shared,
                listening: Mutex::new(None),
                pump,
                runtime: tokio::runtime::Handle::current(),
                ssrc: own_ssrc,
                mode,
            },
            rx,
        ))
    }

    /// Our own SSRC from READY.
    pub fn ssrc(&self) -> u32 {
        self.ssrc
    }

    pub fn mode(&self) -> TransportEncryptionMode {
        self.mode
    }

    /// Negotiated media session, as updated by op 14.
    pub fn session_info(&self) -> SessionInfo {
        self.shared.info.lock().clone()
    }

    pub fn dave(&self) -> &Arc<Dave> {
        &self.dave
    }

    pub fn receiver(&self) -> &Arc<Receiver> {
        &self.receiver
    }

    pub fn ssrcs(&self) -> &Arc<SsrcMap> {
        &self.ssrcs
    }

    pub fn ws(&self) -> &VoiceWs {
        &self.ws
    }

    /// The UDP transport (the packet sink of the send pipeline).
    pub fn udp(&self) -> &Arc<UdpTransport> {
        &self.udp
    }

    /// The outgoing transport cipher; every datagram this connection sends must go through it so the
    /// nonce counter is never reused.
    pub fn crypto_tx(&self) -> &SharedTransportCrypto {
        &self.crypto_tx
    }

    /// The `streams` array of READY (our video streams with the SSRCs the server assigned).
    pub fn ready_streams(&self) -> &[Value] {
        &self.ready_streams
    }

    /// Handles incoming RTCP (`Receiver::handle_rtcp`) from now on, if not already; used by listening
    /// and by publishing (NACK / PLI of viewers). Stays until the connection closes.
    pub fn ensure_rtcp_listener(&self) {
        let mut listener = self.rtcp_listener.lock();
        if listener.is_some() {
            return;
        }
        let receiver = Arc::clone(&self.receiver);
        *listener = Some(self.udp.add_listener(
            move |datagram| receiver.handle_rtcp(datagram),
            ListenerKinds {
                rtp: false,
                rtcp: true,
            },
        ));
    }

    pub fn udp_stats(&self) -> UdpStats {
        self.udp.stats()
    }

    /// Round trip of the UDP keepalive pings in milliseconds (`udp_ping_rtt`).
    pub fn udp_ping_rtt_ms(&self) -> Option<f64> {
        self.udp.ping_rtt_ms()
    }

    pub fn udp_ping_timeouts(&self) -> usize {
        self.udp.ping_timeouts()
    }

    /// The websocket has ended or [`close`](Self::close) was called.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Relaxed)
    }

    /// Starts delivering media to `callback`, on the UDP dispatcher thread (`listen`). RTCP is
    /// handled too (sender reports feed [`Packet::rtcp_time`]). Fails if already listening.
    pub fn listen(&self, callback: MediaCallback) -> Result<(), VoiceError> {
        let mut listening = self.listening.lock();
        if listening.is_some() {
            return Err(VoiceError::AlreadyListening);
        }
        let receiver = Arc::clone(&self.receiver);
        let rtp = self.udp.add_batch_listener(
            move |batch| {
                let packets = receiver.process_batch(batch, Instant::now());
                if packets.is_empty() {
                    return;
                }
                let mut callback = callback.lock();
                for packet in packets {
                    callback(packet);
                }
            },
            ListenerKinds {
                rtp: true,
                rtcp: false,
            },
        );
        self.ensure_rtcp_listener();
        let nack_timer = self.receiver.rtx_enabled().then(|| {
            let receiver = Arc::clone(&self.receiver);
            self.runtime.spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tick.tick().await;
                    receiver.retry_pending_nacks(Instant::now());
                }
            })
        });
        *listening = Some(Listening { rtp, nack_timer });
        Ok(())
    }

    pub fn is_listening(&self) -> bool {
        self.listening.lock().is_some()
    }

    /// `stop_listening`: no more callbacks; receive state is dropped.
    pub fn stop_listening(&self) {
        if let Some(listening) = self.listening.lock().take() {
            self.udp.remove_listener(listening.rtp);
            if let Some(timer) = listening.nack_timer {
                timer.abort();
            }
            self.receiver.reset();
        }
    }

    /// `request_video`: ask the server to forward `ssrc` at `quality` (op 15 MEDIA_SINK_WANTS; the
    /// fallback quality for unlisted streams stays 100).
    pub fn request_video(&self, ssrc: u32, quality: u32) -> Result<(), VoiceError> {
        let wants: Vec<(u32, u32)> = {
            let mut wants = self.shared.wants.lock();
            wants.by_ssrc.insert(ssrc, quality);
            wants.by_ssrc.iter().map(|(s, q)| (*s, *q)).collect()
        };
        self.ws.media_sink_wants(&wants, Some(WANTS_ANY), None)?;
        Ok(())
    }

    /// `send_video_pli`.
    pub fn send_pli(&self, media_ssrc: u32) -> Result<(), VoiceError> {
        Ok(self.receiver.send_pli(media_ssrc)?)
    }

    /// Encrypts and sends an RTCP packet built by the caller (TWCC feedback).
    pub fn send_rtcp(&self, header: &[u8], body: &[u8]) -> Result<(), VoiceError> {
        Ok(self.receiver.send_rtcp(header, body)?)
    }

    /// op 14 SESSION_UPDATE: renegotiate the codec list.
    pub fn session_update(&self, codecs: &[CodecSpec]) -> Result<(), VoiceError> {
        Ok(self.ws.session_update(codecs)?)
    }

    /// Closes the websocket with `code`, stops listening and shuts the UDP socket. Idempotent.
    pub fn close(&self, code: u16) {
        self.shared.closed.store(true, Ordering::Relaxed);
        self.stop_listening();
        self.receiver.set_feedback_handler(None);
        if let Some(id) = self.rtcp_listener.lock().take() {
            self.udp.remove_listener(id);
        }
        self.ws.close(code);
        self.udp.stop_ping();
        self.udp.close();
    }
}

impl Drop for RtcConnection {
    fn drop(&mut self) {
        self.close(1000);
        self.pump.abort();
    }
}

/// Reads the websocket events after the handshake: keeps the SSRC map current and forwards what the
/// owner cares about.
async fn pump(
    mut events: mpsc::UnboundedReceiver<VoiceEvent>,
    tx: mpsc::UnboundedSender<ConnEvent>,
    ssrcs: Arc<SsrcMap>,
    shared: Arc<Shared>,
) {
    while let Some(event) = events.recv().await {
        let forward = match event {
            VoiceEvent::Message { op, data } => {
                apply_message(&ssrcs, op, &data);
                ConnEvent::Message { op, data }
            }
            VoiceEvent::SessionUpdate(info) => {
                *shared.info.lock() = info.clone();
                ConnEvent::SessionUpdate(info)
            }
            VoiceEvent::DaveError(error) => ConnEvent::DaveError(error),
            VoiceEvent::Resumed => ConnEvent::Resumed,
            VoiceEvent::Closed { code, reason } => {
                shared.closed.store(true, Ordering::Relaxed);
                let _ = tx.send(ConnEvent::Closed { code, reason });
                return;
            }
            // READY/SESSION_DESCRIPTION only happen during the handshake.
            VoiceEvent::Ready(_)
            | VoiceEvent::SessionDescription(_)
            | VoiceEvent::BackendVersion { .. } => continue,
        };
        if tx.send(forward).is_err() {
            return;
        }
    }
    shared.closed.store(true, Ordering::Relaxed);
}

fn json_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_ssrc(d: &Value, key: &str) -> Option<u32> {
    d.get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
}

/// `received_message` for SPEAKING and VIDEO: the SSRC to user mapping.
fn apply_message(ssrcs: &SsrcMap, opcode: u8, d: &Value) {
    match opcode {
        op::SPEAKING => {
            if let (Some(user), Some(ssrc)) =
                (d.get("user_id").and_then(json_u64), json_ssrc(d, "ssrc"))
            {
                ssrcs.set_speaking(ssrc, user);
            }
        }
        op::VIDEO => {
            if let Some(user) = d.get("user_id").and_then(json_u64) {
                let previous = ssrcs.video_state(user);
                ssrcs.update_video_state(user, video_state(d, previous.as_ref()));
            }
        }
        op::CLIENT_DISCONNECT => {
            if let Some(user) = d.get("user_id").and_then(json_u64) {
                ssrcs.remove_user(user);
            }
        }
        _ => {}
    }
}

/// `_update_video_state_streams`: `video_ssrc`, `rtx_ssrc` and each stream's SSRCs carry over from
/// the user's previous message when the new one omits them.
fn video_state(d: &Value, previous: Option<&VideoState>) -> VideoState {
    let streams = d
        .get("streams")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .map(|payload| {
                    let rid = payload
                        .get("rid")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let before = previous
                        .and_then(|p| p.streams.iter().find(|s| s.rid == rid))
                        .cloned()
                        .unwrap_or_default();
                    StreamSsrcs {
                        ssrc: json_ssrc(payload, "ssrc").or(before.ssrc),
                        rtx_ssrc: json_ssrc(payload, "rtx_ssrc").or(before.rtx_ssrc),
                        rid,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    VideoState {
        audio_ssrc: json_ssrc(d, "audio_ssrc"),
        video_ssrc: json_ssrc(d, "video_ssrc").or(previous.and_then(|p| p.video_ssrc)),
        rtx_ssrc: json_ssrc(d, "rtx_ssrc").or(previous.and_then(|p| p.rtx_ssrc)),
        streams,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn video_message_carries_over_missing_ssrcs() {
        let first = video_state(
            &json!({"user_id": "5", "audio_ssrc": 1, "video_ssrc": 2, "rtx_ssrc": 3,
                    "streams": [{"rid": "100", "ssrc": 2, "rtx_ssrc": 3}]}),
            None,
        );
        assert_eq!(first.video_ssrc, Some(2));
        let second = video_state(
            &json!({"user_id": "5", "streams": [{"rid": "100", "quality": 50}]}),
            Some(&first),
        );
        assert_eq!((second.video_ssrc, second.rtx_ssrc), (Some(2), Some(3)));
        assert_eq!(second.audio_ssrc, None);
        assert_eq!(second.streams[0].ssrc, Some(2));
    }

    #[test]
    fn speaking_and_video_messages_fill_the_map() {
        let map = SsrcMap::new();
        apply_message(
            &map,
            op::SPEAKING,
            &json!({"user_id": "9", "ssrc": 11, "speaking": 1}),
        );
        assert_eq!(map.user_for(11), Some(9));
        apply_message(
            &map,
            op::VIDEO,
            &json!({"user_id": "9", "audio_ssrc": 11, "video_ssrc": 12, "rtx_ssrc": 13, "streams": []}),
        );
        assert_eq!(map.user_for(12), Some(9));
        assert_eq!(map.user_for(13), Some(9));
        // malformed messages are ignored
        apply_message(&map, op::VIDEO, &json!({"video_ssrc": 12}));
        apply_message(&map, op::SPEAKING, &json!({"user_id": "x", "ssrc": 1}));
    }
}
