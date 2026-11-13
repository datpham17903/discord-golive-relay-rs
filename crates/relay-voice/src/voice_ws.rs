//! Voice gateway websocket, port of `discord/gateway.py::DiscordVoiceWebSocket` (IDENTIFY/RESUME,
//! HELLO heartbeat with `seq_ack`, READY, SELECT_PROTOCOL, SESSION_DESCRIPTION, SESSION_UPDATE,
//! SPEAKING, VIDEO, MEDIA_SINK_WANTS and the DAVE ops 21-31) as used by `discord-native-voice`.
//!
//! One tokio task owns the socket. Frames are delivered as [`VoiceEvent`]s; the caller (the voice
//! client) answers READY with UDP IP discovery and [`VoiceWs::select_protocol`]. With
//! [`VoiceWsConfig::resume_on_drop`] the task resumes by itself when the socket drops the way
//! `_poll_voice_ws` resumes (close code 4015, or no close code at all): a new socket on the same
//! URL sends RESUME (op 7) with the same session, token and `seq_ack`, commands sent meanwhile go out
//! on it, and the server's RESUMED shows up as [`VoiceEvent::Resumed`]. Anything else ends in
//! [`VoiceEvent::Closed`] and the caller decides. The DAVE ops are handled here against a shared
//! [`Dave`], which survives resumes.
//!
//! Differences from the Python library, all deliberate:
//! - READY does not run IP discovery inline (that belongs to the UDP transport owner); it is reported
//!   as an event and the READY-time `VOICE_BACKEND_VERSION` request is sent here.
//! - No `Sec-WebSocket-Extensions: permessage-deflate` header: `wreq` cannot inflate frames.
//! - A DAVE failure (bad external sender, proposals...) is reported as [`VoiceEvent::DaveError`]
//!   instead of tearing the socket down.
//! - A resume does not send op 4 again nor wait for a new SESSION_DESCRIPTION (the voice server
//!   answers RESUME with RESUMED only); it keeps the UDP socket and the transport keys. It is tried up
//!   to [`RESUME_ATTEMPTS`] times within `connect_timeout`, pausing `1 + 2i` seconds between failed
//!   attempts like `_inner_connect`.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use relay_media::codec::{CODEC_ORDER, OPUS_PAYLOAD_TYPE, VoiceCodec};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;
use wreq::ws::message::{CloseFrame, Message};

use crate::dave::{Dave, DaveOut};
use crate::transport::TransportEncryptionMode;

/// Voice gateway version of the pinned `discord.py-self` (`wss://{endpoint}/?v=9`).
pub const VOICE_GATEWAY_VERSION: u8 = 9;
/// A send stuck longer than this stalls heartbeats and receives: the connection is dead.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MESSAGE_SIZE: usize = 15 * 1024 * 1024;
/// Resume attempts in a row without a RESUMED (`_inner_connect` tries 5 times).
pub const RESUME_ATTEMPTS: u32 = 5;
/// `_poll_voice_ws`: the server crashed and the session can be resumed.
pub const CLOSE_RESUMABLE: u16 = 4015;

pub mod op {
    pub const IDENTIFY: u8 = 0;
    pub const SELECT_PROTOCOL: u8 = 1;
    pub const READY: u8 = 2;
    pub const HEARTBEAT: u8 = 3;
    pub const SESSION_DESCRIPTION: u8 = 4;
    pub const SPEAKING: u8 = 5;
    pub const HEARTBEAT_ACK: u8 = 6;
    pub const RESUME: u8 = 7;
    pub const HELLO: u8 = 8;
    pub const RESUMED: u8 = 9;
    pub const CLIENTS_CONNECT: u8 = 11;
    pub const VIDEO: u8 = 12;
    pub const CLIENT_DISCONNECT: u8 = 13;
    pub const SESSION_UPDATE: u8 = 14;
    pub const MEDIA_SINK_WANTS: u8 = 15;
    pub const VOICE_BACKEND_VERSION: u8 = 16;
    pub const DAVE_PREPARE_TRANSITION: u8 = 21;
    pub const DAVE_EXECUTE_TRANSITION: u8 = 22;
    pub const DAVE_TRANSITION_READY: u8 = 23;
    pub const DAVE_PREPARE_EPOCH: u8 = 24;
    pub const MLS_EXTERNAL_SENDER: u8 = 25;
    pub const MLS_KEY_PACKAGE: u8 = 26;
    pub const MLS_PROPOSALS: u8 = 27;
    pub const MLS_COMMIT_WELCOME: u8 = 28;
    pub const MLS_ANNOUNCE_COMMIT_TRANSITION: u8 = 29;
    pub const MLS_WELCOME: u8 = 30;
    pub const MLS_INVALID_COMMIT_WELCOME: u8 = 31;
}

/// `SpeakingFlags` bits.
pub mod speaking {
    pub const NONE: u32 = 0;
    pub const VOICE: u32 = 1 << 0;
    pub const SOUNDSHARE: u32 = 1 << 1;
    pub const PRIORITY: u32 = 1 << 2;
}

#[derive(Debug, thiserror::Error)]
pub enum VoiceWsError {
    #[error("voice websocket connect failed: {0}")]
    Connect(String),
    #[error("voice websocket is closed")]
    Closed,
}

/// Voice server token from VOICE_SERVER_UPDATE. Never printed: `Debug` shows `***`.
#[derive(Clone)]
pub struct VoiceToken(String);

impl VoiceToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }
}

impl fmt::Debug for VoiceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VoiceToken(***)")
    }
}

/// `VoiceCodec.to_dict`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecSpec {
    pub name: &'static str,
    pub kind: &'static str,
    pub priority: u32,
    pub payload_type: u8,
    pub rtx_payload_type: Option<u8>,
    pub encode: bool,
    pub decode: bool,
}

impl CodecSpec {
    /// `VoiceCodec.opus(payload_type=120)`.
    pub fn opus() -> Self {
        Self {
            name: "opus",
            kind: "audio",
            priority: 1000,
            payload_type: OPUS_PAYLOAD_TYPE,
            rtx_payload_type: None,
            encode: true,
            decode: true,
        }
    }

    /// `VoiceCodec.video(name, priority, payload_type, rtx_payload_type, encode, decode)`; payload
    /// types come from the codec table.
    pub fn video(codec: VoiceCodec, priority: u32, encode: bool, decode: bool) -> Self {
        Self {
            name: codec.as_str(),
            kind: "video",
            priority,
            payload_type: codec.payload_type(),
            rtx_payload_type: Some(codec.rtx_payload_type()),
            encode,
            decode,
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("name".into(), json!(self.name));
        m.insert("type".into(), json!(self.kind));
        m.insert("priority".into(), json!(self.priority));
        m.insert("payload_type".into(), json!(self.payload_type));
        m.insert("encode".into(), json!(self.encode));
        m.insert("decode".into(), json!(self.decode));
        if let Some(rtx) = self.rtx_payload_type {
            m.insert("rtx_payload_type".into(), json!(rtx));
        }
        Value::Object(m)
    }
}

/// `native_voice.VoiceClient.voice_codec_specs`: Opus plus every video codec in `CODEC_ORDER`
/// (priorities 1000..5000), neither encoding nor decoding.
pub fn default_codecs() -> Vec<CodecSpec> {
    let mut out = vec![CodecSpec::opus()];
    for (i, name) in CODEC_ORDER.iter().enumerate() {
        let codec: VoiceCodec = name.parse().expect("CODEC_ORDER holds valid codecs");
        out.push(CodecSpec::video(codec, 1000 * (i as u32 + 1), false, false));
    }
    out
}

/// `adapter.codecs.video_codec_list`: Opus plus `names` in the given order (priorities 1000, 2000...),
/// decode only unless `encode`.
pub fn video_codec_list(names: &[VoiceCodec], encode: bool) -> Vec<CodecSpec> {
    let mut out = vec![CodecSpec::opus()];
    for (i, &codec) in names.iter().enumerate() {
        out.push(CodecSpec::video(
            codec,
            1000 * (i as u32 + 1),
            encode,
            !encode,
        ));
    }
    out
}

/// `VoiceStreamResolution`: `kind` is `fixed` or `source`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamResolution {
    pub kind: String,
    pub width: u32,
    pub height: u32,
}

impl StreamResolution {
    /// `VoiceStreamResolution.fixed(width=, height=)`.
    pub fn fixed(width: u32, height: u32) -> Self {
        Self {
            kind: "fixed".into(),
            width,
            height,
        }
    }
}

/// `VoiceStream.to_dict` (stream descriptors in IDENTIFY and op 12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamSpec {
    pub kind: &'static str,
    pub rid: String,
    pub quality: u32,
    pub active: bool,
    pub max_bitrate: Option<u64>,
    pub max_framerate: Option<u32>,
    pub max_resolution: Option<StreamResolution>,
    pub ssrc: Option<u32>,
    pub rtx_ssrc: Option<u32>,
}

impl StreamSpec {
    /// `VoiceStream.video(quality=...)`; the rid is the quality.
    pub fn video(quality: u32) -> Self {
        Self::new("video", quality.to_string(), quality, None)
    }

    /// `VoiceStream.screen(rid=..., quality=..., max_framerate=...)`.
    pub fn screen(rid: &str, quality: u32, max_framerate: Option<u32>) -> Self {
        Self::new("screen", rid.to_owned(), quality, max_framerate)
    }

    fn new(kind: &'static str, rid: String, quality: u32, max_framerate: Option<u32>) -> Self {
        Self {
            kind,
            rid,
            quality,
            active: true,
            max_bitrate: None,
            max_framerate,
            max_resolution: None,
            ssrc: None,
            rtx_ssrc: None,
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("type".into(), json!(self.kind));
        m.insert("rid".into(), json!(self.rid));
        m.insert("quality".into(), json!(self.quality));
        m.insert("active".into(), json!(self.active));
        if let Some(v) = self.max_bitrate {
            m.insert("max_bitrate".into(), json!(v));
        }
        if let Some(v) = self.max_framerate {
            m.insert("max_framerate".into(), json!(v));
        }
        if let Some(r) = &self.max_resolution {
            m.insert(
                "max_resolution".into(),
                json!({"type": r.kind, "width": r.width, "height": r.height}),
            );
        }
        if let Some(v) = self.ssrc {
            m.insert("ssrc".into(), json!(v));
        }
        if let Some(v) = self.rtx_ssrc {
            m.insert("rtx_ssrc".into(), json!(v));
        }
        Value::Object(m)
    }
}

/// `gateway.py` `wss://{endpoint}/?v=9`.
pub fn gateway_url(endpoint: &str, version: u8) -> String {
    format!("wss://{endpoint}/?v={version}")
}

/// First of `supported` (ordered by preference) that READY lists in `modes`.
pub fn choose_mode(
    supported: &[TransportEncryptionMode],
    ready_modes: &[String],
) -> Option<TransportEncryptionMode> {
    supported
        .iter()
        .copied()
        .find(|m| ready_modes.iter().any(|r| r == m.as_str()))
}

#[derive(Clone)]
pub struct VoiceWsConfig {
    /// Full websocket URL; see [`gateway_url`].
    pub url: String,
    /// `Origin` header of the handshake (`https://discord.com`).
    pub origin: String,
    pub user_agent: String,
    pub server_id: u64,
    pub channel_id: u64,
    pub user_id: u64,
    pub session_id: String,
    pub token: VoiceToken,
    /// `voice_client.supports_video()`.
    pub video: bool,
    /// `voice_client.video_streams`, sent in IDENTIFY when `video`.
    pub streams: Vec<StreamSpec>,
    /// `voice_client.codecs`, sent in SELECT_PROTOCOL.
    pub codecs: Vec<CodecSpec>,
    /// Ordered by preference.
    pub supported_modes: Vec<TransportEncryptionMode>,
    /// `voice_client.get_experiments(...)`, sent in SELECT_PROTOCOL when not empty.
    pub selected_experiments: Vec<String>,
    /// `state.rtc_connection_id`.
    pub rtc_connection_id: String,
    pub max_dave_protocol_version: u16,
    /// `_max_heartbeat_timeout`: silence for this long closes the socket.
    pub heartbeat_timeout: Duration,
    pub connect_timeout: Duration,
    /// Send RESUME (op 7) instead of IDENTIFY.
    pub resume: bool,
    /// `seq_ack` carried over from the previous socket (`-1` on a fresh connection).
    pub seq_ack: i64,
    /// Resume on a new socket when this one drops with 4015 or without a close code (see the module
    /// documentation) instead of ending the event stream.
    pub resume_on_drop: bool,
}

impl VoiceWsConfig {
    pub fn new(
        endpoint: &str,
        server_id: u64,
        channel_id: u64,
        user_id: u64,
        session_id: impl Into<String>,
        token: VoiceToken,
    ) -> Self {
        Self {
            url: gateway_url(endpoint, VOICE_GATEWAY_VERSION),
            origin: "https://discord.com".into(),
            user_agent: String::new(),
            server_id,
            channel_id,
            user_id,
            session_id: session_id.into(),
            token,
            video: true,
            streams: vec![StreamSpec::video(100)],
            codecs: default_codecs(),
            supported_modes: vec![
                TransportEncryptionMode::AeadAes256GcmRtpSize,
                TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize,
            ],
            selected_experiments: vec!["fixed_keyframe_interval".into()],
            rtc_connection_id: uuid::Uuid::new_v4().to_string(),
            max_dave_protocol_version: Dave::max_protocol_version(),
            heartbeat_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(30),
            resume: false,
            seq_ack: -1,
            resume_on_drop: false,
        }
    }
}

impl fmt::Debug for VoiceWsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VoiceWsConfig")
            .field("url", &self.url)
            .field("server_id", &self.server_id)
            .field("channel_id", &self.channel_id)
            .field("user_id", &self.user_id)
            .field("token", &self.token)
            .field("resume", &self.resume)
            .field("seq_ack", &self.seq_ack)
            .field("resume_on_drop", &self.resume_on_drop)
            .finish_non_exhaustive()
    }
}

/// READY (op 2).
#[derive(Clone, Debug, PartialEq)]
pub struct Ready {
    pub ssrc: u32,
    pub ip: String,
    pub port: u16,
    pub modes: Vec<String>,
    pub streams: Vec<Value>,
    pub experiments: Vec<String>,
}

/// Negotiated media session: SESSION_DESCRIPTION, then merged with each SESSION_UPDATE (op 14),
/// where absent keys keep their previous value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionInfo {
    pub audio_codec: Option<String>,
    pub video_codec: Option<String>,
    pub media_session_id: Option<String>,
    pub keyframe_interval: Option<u64>,
}

/// SESSION_DESCRIPTION (op 4). `secret_key` is never shown by `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionDescription {
    pub mode: String,
    pub secret_key: Vec<u8>,
    pub info: SessionInfo,
    pub dave_protocol_version: u64,
}

impl fmt::Debug for SessionDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionDescription")
            .field("mode", &self.mode)
            .field("secret_key", &"***")
            .field("info", &self.info)
            .field("dave_protocol_version", &self.dave_protocol_version)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VoiceEvent {
    Ready(Ready),
    SessionDescription(SessionDescription),
    SessionUpdate(SessionInfo),
    Resumed,
    BackendVersion {
        voice: Option<String>,
        rtc_worker: Option<String>,
    },
    /// Any other text message (SPEAKING, VIDEO, CLIENTS_CONNECT, CLIENT_DISCONNECT, MEDIA_SINK_WANTS,
    /// unknown ops): the Python `hook`.
    Message {
        op: u8,
        data: Value,
    },
    DaveError(String),
    /// The socket ended; the task is gone. `code` is the close code when there was one.
    Closed {
        code: Option<u16>,
        reason: String,
    },
}

enum Outgoing {
    Json(Value),
    Binary(Vec<u8>),
    Close(u16),
}

struct Shared {
    ssrc: AtomicU32,
    seq_ack: AtomicI64,
}

/// Handle to a connected voice websocket.
#[derive(Clone)]
pub struct VoiceWs {
    cmd_tx: mpsc::UnboundedSender<Outgoing>,
    shared: Arc<Shared>,
    config: Arc<VoiceWsConfig>,
}

impl VoiceWs {
    /// Opens the socket and sends IDENTIFY (or RESUME) straight away, before HELLO, like Python.
    pub async fn connect(
        http: &wreq::Client,
        config: VoiceWsConfig,
        dave: Arc<Dave>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<VoiceEvent>), VoiceWsError> {
        let ws = open(http, &config, config.resume, config.seq_ack).await?;
        let config = Arc::new(config);
        let shared = Arc::new(Shared {
            ssrc: AtomicU32::new(0),
            seq_ack: AtomicI64::new(config.seq_ack),
        });
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let now = Instant::now();
        let task = Task {
            ws: Some(ws),
            http: http.clone(),
            config: Arc::clone(&config),
            dave,
            shared: Arc::clone(&shared),
            cmd_rx,
            ev_tx,
            info: SessionInfo::default(),
            interval: None,
            next_beat: None,
            last_ack: now,
            last_recv: now,
            pending: VecDeque::new(),
            resume_attempts: 0,
        };
        tokio::spawn(task.run());
        Ok((
            Self {
                cmd_tx,
                shared,
                config,
            },
            ev_rx,
        ))
    }

    /// `seq_ack`: the last `seq` received (`-1` before any); pass it to the next socket on a resume.
    pub fn seq_ack(&self) -> i64 {
        self.shared.seq_ack.load(Ordering::Relaxed)
    }

    /// Own SSRC from READY (0 before it).
    pub fn ssrc(&self) -> u32 {
        self.shared.ssrc.load(Ordering::Relaxed)
    }

    fn send(&self, msg: Outgoing) -> Result<(), VoiceWsError> {
        self.cmd_tx.send(msg).map_err(|_| VoiceWsError::Closed)
    }

    pub fn send_json(&self, op: u8, d: Value) -> Result<(), VoiceWsError> {
        self.send(Outgoing::Json(json!({"op": op, "d": d})))
    }

    /// `send_binary`: one byte of opcode, then the payload.
    pub fn send_binary(&self, op: u8, data: &[u8]) -> Result<(), VoiceWsError> {
        let mut frame = Vec::with_capacity(data.len() + 1);
        frame.push(op);
        frame.extend_from_slice(data);
        self.send(Outgoing::Binary(frame))
    }

    /// Sends the messages of a DAVE event (ops 23, 26, 28, 31).
    pub fn send_dave(&self, outs: &[DaveOut]) -> Result<(), VoiceWsError> {
        for out in outs {
            match out {
                DaveOut::TransitionReady(id) => {
                    self.send_json(op::DAVE_TRANSITION_READY, json!({"transition_id": id}))?
                }
                DaveOut::InvalidCommitWelcome(id) => {
                    self.send_json(op::MLS_INVALID_COMMIT_WELCOME, json!({"transition_id": id}))?
                }
                DaveOut::KeyPackage(data) => self.send_binary(op::MLS_KEY_PACKAGE, data)?,
                DaveOut::CommitWelcome(data) => self.send_binary(op::MLS_COMMIT_WELCOME, data)?,
            }
        }
        Ok(())
    }

    /// op 1 SELECT_PROTOCOL.
    pub fn select_protocol(
        &self,
        ip: &str,
        port: u16,
        mode: TransportEncryptionMode,
    ) -> Result<(), VoiceWsError> {
        self.send(Outgoing::Json(select_protocol_payload(
            &self.config,
            ip,
            port,
            mode,
        )))
    }

    /// op 12 VIDEO (`video_state`).
    pub fn video_state(
        &self,
        video_ssrc: u32,
        rtx_ssrc: u32,
        streams: &[StreamSpec],
    ) -> Result<(), VoiceWsError> {
        self.send_json(
            op::VIDEO,
            json!({
                "audio_ssrc": self.ssrc(),
                "video_ssrc": video_ssrc,
                "rtx_ssrc": rtx_ssrc,
                "streams": streams.iter().map(StreamSpec::to_json).collect::<Vec<_>>(),
            }),
        )
    }

    /// op 15 MEDIA_SINK_WANTS.
    pub fn media_sink_wants(
        &self,
        wants: &[(u32, u32)],
        any: Option<u32>,
        pixel_counts: Option<&[(u32, u64)]>,
    ) -> Result<(), VoiceWsError> {
        let mut data = Map::new();
        for (ssrc, quality) in wants {
            data.insert(ssrc.to_string(), json!(quality));
        }
        if let Some(any) = any {
            data.insert("any".into(), json!(any));
        }
        if let Some(counts) = pixel_counts {
            let mut m = Map::new();
            for (ssrc, count) in counts {
                m.insert(ssrc.to_string(), json!(count));
            }
            // camelCase on the wire
            data.insert("pixelCounts".into(), Value::Object(m));
        }
        self.send_json(op::MEDIA_SINK_WANTS, Value::Object(data))
    }

    /// op 14 SESSION_UPDATE.
    pub fn session_update(&self, codecs: &[CodecSpec]) -> Result<(), VoiceWsError> {
        self.send_json(
            op::SESSION_UPDATE,
            json!({"codecs": codecs.iter().map(CodecSpec::to_json).collect::<Vec<_>>()}),
        )
    }

    /// op 5 SPEAKING (`flags` from [`speaking`]).
    pub fn speak(&self, flags: u32, delay: u32) -> Result<(), VoiceWsError> {
        self.send_json(
            op::SPEAKING,
            json!({"speaking": flags, "delay": delay, "ssrc": self.ssrc()}),
        )
    }

    /// Closes the socket with `code`; the event stream ends with [`VoiceEvent::Closed`].
    pub fn close(&self, code: u16) {
        let _ = self.send(Outgoing::Close(code));
    }
}

// ---------------------------------------------------------------------------------------------
// payloads

fn identify_payload(c: &VoiceWsConfig) -> Value {
    let streams: Vec<Value> = if c.video {
        c.streams.iter().map(StreamSpec::to_json).collect()
    } else {
        Vec::new()
    };
    json!({
        "op": op::IDENTIFY,
        "d": {
            "server_id": c.server_id.to_string(),
            "channel_id": c.channel_id.to_string(),
            "user_id": c.user_id.to_string(),
            "session_id": c.session_id,
            "token": c.token.0,
            "video": c.video,
            "streams": streams,
            "max_dave_protocol_version": c.max_dave_protocol_version,
        },
    })
}

fn resume_payload(c: &VoiceWsConfig, seq_ack: i64) -> Value {
    json!({
        "op": op::RESUME,
        "d": {
            "token": c.token.0,
            "server_id": c.server_id.to_string(),
            "channel_id": c.channel_id.to_string(),
            "session_id": c.session_id,
            "seq_ack": seq_ack,
        },
    })
}

fn select_protocol_payload(
    c: &VoiceWsConfig,
    ip: &str,
    port: u16,
    mode: TransportEncryptionMode,
) -> Value {
    let mut d = Map::new();
    d.insert("protocol".into(), json!("udp"));
    d.insert(
        "data".into(),
        json!({"address": ip, "port": port, "mode": mode.as_str()}),
    );
    d.insert(
        "codecs".into(),
        Value::Array(c.codecs.iter().map(CodecSpec::to_json).collect()),
    );
    if !c.rtc_connection_id.is_empty() {
        d.insert("rtc_connection_id".into(), json!(c.rtc_connection_id));
    }
    if !c.selected_experiments.is_empty() {
        d.insert("experiments".into(), json!(c.selected_experiments));
    }
    json!({"op": op::SELECT_PROTOCOL, "d": Value::Object(d)})
}

fn json_frame(op: u8, d: Value) -> Message {
    Message::text(json!({"op": op, "d": d}).to_string())
}

fn binary_frame(op: u8, data: &[u8]) -> Message {
    let mut frame = Vec::with_capacity(data.len() + 1);
    frame.push(op);
    frame.extend_from_slice(data);
    Message::binary(frame)
}

fn dave_frame(out: &DaveOut) -> Message {
    match out {
        DaveOut::TransitionReady(id) => {
            json_frame(op::DAVE_TRANSITION_READY, json!({"transition_id": id}))
        }
        DaveOut::InvalidCommitWelcome(id) => {
            json_frame(op::MLS_INVALID_COMMIT_WELCOME, json!({"transition_id": id}))
        }
        DaveOut::KeyPackage(data) => binary_frame(op::MLS_KEY_PACKAGE, data),
        DaveOut::CommitWelcome(data) => binary_frame(op::MLS_COMMIT_WELCOME, data),
    }
}

// ---------------------------------------------------------------------------------------------
// connection task

type Ws = wreq::ws::WebSocket;

/// `from_connection_state`: opens a socket to `config.url` and sends IDENTIFY, or RESUME with
/// `seq_ack`, straight away.
async fn open(
    http: &wreq::Client,
    config: &VoiceWsConfig,
    resume: bool,
    seq_ack: i64,
) -> Result<Ws, VoiceWsError> {
    let request = http
        .websocket(config.url.clone())
        .header("Accept", "*/*")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Origin", &config.origin)
        .header("User-Agent", &config.user_agent)
        .max_message_size(MAX_MESSAGE_SIZE)
        .send();
    let mut ws = tokio::time::timeout(config.connect_timeout, async {
        request.await?.into_websocket().await
    })
    .await
    .map_err(|_| VoiceWsError::Connect("timed out".into()))?
    .map_err(|e| VoiceWsError::Connect(e.to_string()))?;

    let first = if resume {
        resume_payload(config, seq_ack)
    } else {
        identify_payload(config)
    };
    ws.send(Message::text(first.to_string()))
        .await
        .map_err(|e| VoiceWsError::Connect(e.to_string()))?;
    Ok(ws)
}

struct Exit {
    code: Option<u16>,
    reason: String,
    /// The server or the network ended the socket in a way `_poll_voice_ws` resumes from.
    resumable: bool,
}

impl Exit {
    /// Ended by us or by a protocol error: never resumed.
    fn new(code: Option<u16>, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            resumable: false,
        }
    }

    /// The socket broke without a close frame (`CurlError`, no code).
    fn dropped(reason: impl Into<String>) -> Self {
        Self {
            resumable: true,
            ..Self::new(None, reason)
        }
    }

    /// The server sent a close frame.
    fn closed(code: Option<u16>, reason: impl Into<String>) -> Self {
        Self {
            resumable: code.is_none_or(|c| c == CLOSE_RESUMABLE),
            ..Self::new(code, reason)
        }
    }
}

struct Task {
    /// `None` between the sockets of a resume.
    ws: Option<Ws>,
    /// For the sockets of a resume.
    http: wreq::Client,
    config: Arc<VoiceWsConfig>,
    dave: Arc<Dave>,
    shared: Arc<Shared>,
    cmd_rx: mpsc::UnboundedReceiver<Outgoing>,
    ev_tx: mpsc::UnboundedSender<VoiceEvent>,
    info: SessionInfo,
    interval: Option<Duration>,
    next_beat: Option<Instant>,
    /// `VoiceKeepAliveHandler._last_recv` (only refreshed by HEARTBEAT_ACK).
    last_ack: Instant,
    /// Any frame (`poll_event` wait_for timeout).
    last_recv: Instant,
    /// Commands that arrived while there was no socket (a resume in progress), sent on the next.
    pending: VecDeque<Outgoing>,
    /// Resumes tried since the last RESUMED.
    resume_attempts: u32,
}

async fn recv_opt(ws: &mut Option<Ws>) -> Option<Result<Message, wreq::Error>> {
    match ws {
        Some(ws) => ws.recv().await,
        None => std::future::pending().await,
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

impl Task {
    async fn run(mut self) {
        let exit = loop {
            let exit = self.serve().await;
            if !(exit.resumable && self.config.resume_on_drop) {
                break exit;
            }
            if let Err(exit) = self.resume(exit).await {
                break exit;
            }
        };
        let _ = self.ev_tx.send(VoiceEvent::Closed {
            code: exit.code,
            reason: exit.reason,
        });
    }

    /// `_poll_voice_ws` on 4015 / no code: a new socket that sends RESUME. Gives up (with the exit
    /// that ended the last socket) after [`RESUME_ATTEMPTS`] attempts without a RESUMED or once
    /// `connect_timeout` has passed; a close by the owner meanwhile ends it too.
    async fn resume(&mut self, last: Exit) -> Result<(), Exit> {
        // let the server see the old connection go at once
        self.ws = None;
        let deadline = Instant::now() + self.config.connect_timeout;
        let mut pause = Duration::ZERO;
        let mut failure: Option<String> = None;
        let give_up = |failure: Option<String>| match failure {
            Some(e) => Exit::new(last.code, format!("{}; resume failed: {e}", last.reason)),
            None => Exit::new(last.code, last.reason.clone()),
        };
        loop {
            let wake = Instant::now() + pause;
            if self.resume_attempts >= RESUME_ATTEMPTS || wake >= deadline {
                return Err(give_up(failure));
            }
            self.wait_without_socket(wake).await?;
            let attempt = self.resume_attempts;
            self.resume_attempts += 1;
            let seq_ack = self.shared.seq_ack.load(Ordering::Relaxed);
            let config = Arc::clone(&self.config);
            let http = self.http.clone();
            let connect = async move { open(&http, &config, true, seq_ack).await };
            match self.wait_without_socket_for(deadline, connect).await? {
                Some(Ok(ws)) => {
                    self.ws = Some(ws);
                    let now = Instant::now();
                    self.interval = None;
                    self.next_beat = None;
                    self.last_ack = now;
                    self.last_recv = now;
                    return Ok(());
                }
                Some(Err(e)) => failure = Some(e.to_string()),
                None => return Err(give_up(Some("timed out".into()))),
            }
            // `_inner_connect`: 1, 3, 5... seconds between attempts
            pause = Duration::from_secs(1 + 2 * u64::from(attempt));
        }
    }

    /// Sleeps until `until` while there is no socket; see [`Self::wait_without_socket_for`].
    async fn wait_without_socket(&mut self, until: Instant) -> Result<(), Exit> {
        self.wait_without_socket_for(until, std::future::pending::<()>())
            .await
            .map(|_| ())
    }

    /// Runs `work` until `until` (`None` when it is not done by then) while there is no socket:
    /// commands are kept for the next socket, and a close or a dropped handle ends the wait.
    async fn wait_without_socket_for<T>(
        &mut self,
        until: Instant,
        work: impl std::future::Future<Output = T>,
    ) -> Result<Option<T>, Exit> {
        tokio::pin!(work);
        loop {
            tokio::select! {
                out = &mut work => return Ok(Some(out)),
                () = tokio::time::sleep_until(until) => return Ok(None),
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(Outgoing::Close(code)) => return Err(Exit::new(Some(code), "closed by client")),
                    Some(other) => self.pending.push_back(other),
                    None => return Err(Exit::new(Some(1000), "handle dropped")),
                },
                () = self.ev_tx.closed() => {
                    return Err(Exit::new(Some(1000), "event receiver dropped"));
                }
            }
        }
    }

    async fn serve(&mut self) -> Exit {
        while let Some(cmd) = self.pending.pop_front() {
            if let Some(exit) = self.on_command(Some(cmd)).await {
                return exit;
            }
        }
        loop {
            let beat_at = self.next_beat;
            let silent_at = self.last_recv + self.config.heartbeat_timeout;
            let exit = tokio::select! {
                msg = recv_opt(&mut self.ws) => self.on_ws_message(msg).await,
                () = sleep_until_opt(beat_at) => self.beat().await,
                () = tokio::time::sleep_until(silent_at) => {
                    Some(Exit::new(None, "no message received"))
                }
                cmd = self.cmd_rx.recv() => self.on_command(cmd).await,
                () = self.ev_tx.closed() => {
                    self.close_ws(1000).await;
                    Some(Exit::new(Some(1000), "event receiver dropped"))
                }
            };
            if let Some(exit) = exit {
                return exit;
            }
        }
    }

    async fn close_ws(&mut self, code: u16) {
        let frame = CloseFrame {
            code: code.into(),
            reason: "".into(),
        };
        if let Some(ws) = self.ws.as_mut() {
            let _ = ws.send(Message::Close(Some(frame))).await;
        }
    }

    async fn send(&mut self, msg: Message) -> Option<Exit> {
        let Some(ws) = self.ws.as_mut() else {
            return Some(Exit::dropped("no socket"));
        };
        match tokio::time::timeout(SEND_TIMEOUT, ws.send(msg)).await {
            Ok(result) => result.err().map(|e| Exit::dropped(e.to_string())),
            Err(_) => Some(Exit::dropped("websocket send timed out")),
        }
    }

    async fn on_command(&mut self, cmd: Option<Outgoing>) -> Option<Exit> {
        match cmd {
            Some(Outgoing::Json(v)) => self.send(Message::text(v.to_string())).await,
            Some(Outgoing::Binary(b)) => self.send(Message::binary(b)).await,
            Some(Outgoing::Close(code)) => {
                self.close_ws(code).await;
                Some(Exit::new(Some(code), "closed by client"))
            }
            None => {
                self.close_ws(1000).await;
                Some(Exit::new(Some(1000), "handle dropped"))
            }
        }
    }

    /// `VoiceKeepAliveHandler.run`: no ack within the timeout closes with 4000.
    async fn beat(&mut self) -> Option<Exit> {
        let interval = self.interval?;
        if self.last_ack + self.config.heartbeat_timeout < Instant::now() {
            self.close_ws(4000).await;
            return Some(Exit::new(Some(4000), "heartbeat not acknowledged"));
        }
        self.next_beat = Some(Instant::now() + interval);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let seq_ack = self.shared.seq_ack.load(Ordering::Relaxed);
        self.send(json_frame(
            op::HEARTBEAT,
            json!({"t": t, "seq_ack": seq_ack}),
        ))
        .await
    }

    async fn on_ws_message(&mut self, msg: Option<Result<Message, wreq::Error>>) -> Option<Exit> {
        self.last_recv = Instant::now();
        match msg {
            None => Some(Exit::dropped("socket closed")),
            Some(Err(e)) => Some(Exit::dropped(e.to_string())),
            Some(Ok(Message::Close(frame))) => {
                let (code, reason) = frame.map_or((None, String::new()), |f| {
                    (Some(u16::from(f.code)), f.reason.as_str().to_owned())
                });
                Some(Exit::closed(code, reason))
            }
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<Value>(t.as_str()) {
                Ok(v) => self.on_text(v).await,
                Err(_) => Some(Exit::new(None, "invalid voice JSON")),
            },
            Some(Ok(Message::Binary(b))) => self.on_binary(&b).await,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => None,
        }
    }

    fn emit(&self, event: VoiceEvent) {
        let _ = self.ev_tx.send(event);
    }

    async fn send_dave(&mut self, outs: Vec<DaveOut>) -> Option<Exit> {
        for out in &outs {
            if let Some(exit) = self.send(dave_frame(out)).await {
                return Some(exit);
            }
        }
        None
    }

    /// `received_binary_message`: `seq` (u16 BE), opcode, payload.
    async fn on_binary(&mut self, msg: &[u8]) -> Option<Exit> {
        let [s0, s1, opcode, payload @ ..] = msg else {
            return None;
        };
        let seq = i64::from(u16::from_be_bytes([*s0, *s1]));
        self.shared.seq_ack.store(seq, Ordering::Relaxed);
        match self.dave.on_binary(*opcode, payload) {
            Ok(outs) => self.send_dave(outs).await,
            Err(e) => {
                self.emit(VoiceEvent::DaveError(e.to_string()));
                None
            }
        }
    }

    /// `received_message`.
    async fn on_text(&mut self, msg: Value) -> Option<Exit> {
        let Some(opcode) = msg
            .get("op")
            .and_then(Value::as_u64)
            .and_then(|o| u8::try_from(o).ok())
        else {
            return Some(Exit::new(None, "voice message without op"));
        };
        let d = msg.get("d").cloned().unwrap_or(Value::Null);
        if let Some(seq) = msg.get("seq").and_then(Value::as_i64) {
            self.shared.seq_ack.store(seq, Ordering::Relaxed);
        }

        match opcode {
            op::READY => return self.on_ready(&d).await,
            op::HEARTBEAT_ACK => self.last_ack = Instant::now(),
            op::RESUMED => {
                self.resume_attempts = 0;
                self.emit(VoiceEvent::Resumed);
            }
            op::SESSION_DESCRIPTION => return self.on_session_description(&d).await,
            op::SESSION_UPDATE => {
                let update = session_info(&d, &self.info);
                self.info = update.clone();
                self.emit(VoiceEvent::SessionUpdate(update));
            }
            op::HELLO => {
                let ms = d.get("heartbeat_interval").and_then(Value::as_f64);
                let Some(ms) = ms.filter(|ms| *ms > 0.0) else {
                    return Some(Exit::new(None, "HELLO without heartbeat_interval"));
                };
                let interval = Duration::from_secs_f64(ms / 1000.0);
                self.interval = Some(interval);
                self.last_ack = Instant::now();
                self.next_beat = Some(Instant::now() + interval);
            }
            op::VOICE_BACKEND_VERSION => self.emit(VoiceEvent::BackendVersion {
                voice: str_field(&d, "voice"),
                rtc_worker: str_field(&d, "rtc_worker"),
            }),
            op::DAVE_PREPARE_TRANSITION | op::DAVE_EXECUTE_TRANSITION | op::DAVE_PREPARE_EPOCH => {
                return self.on_dave_text(opcode, &d).await;
            }
            _ => self.emit(VoiceEvent::Message {
                op: opcode,
                data: d,
            }),
        }
        None
    }

    async fn on_ready(&mut self, d: &Value) -> Option<Exit> {
        let (Some(ssrc), Some(ip), Some(port)) = (
            d.get("ssrc")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok()),
            str_field(d, "ip"),
            d.get("port")
                .and_then(Value::as_u64)
                .and_then(|v| u16::try_from(v).ok()),
        ) else {
            return Some(Exit::new(None, "malformed READY"));
        };
        self.shared.ssrc.store(ssrc, Ordering::Relaxed);
        let strings = |key: &str| -> Vec<String> {
            d.get(key)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let ready = Ready {
            ssrc,
            ip,
            port,
            modes: strings("modes"),
            streams: d
                .get("streams")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            experiments: strings("experiments"),
        };
        // request_voice_backend_version
        if let Some(exit) = self
            .send(json_frame(op::VOICE_BACKEND_VERSION, json!({})))
            .await
        {
            return Some(exit);
        }
        self.emit(VoiceEvent::Ready(ready));
        None
    }

    async fn on_session_description(&mut self, d: &Value) -> Option<Exit> {
        let (Some(mode), Some(key)) = (
            str_field(d, "mode"),
            d.get("secret_key").and_then(Value::as_array),
        ) else {
            return Some(Exit::new(None, "malformed SESSION_DESCRIPTION"));
        };
        let secret_key: Option<Vec<u8>> = key
            .iter()
            .map(|v| v.as_u64().and_then(|b| u8::try_from(b).ok()))
            .collect();
        let Some(secret_key) = secret_key else {
            return Some(Exit::new(None, "malformed SESSION_DESCRIPTION secret_key"));
        };
        let dave_version = d
            .get("dave_protocol_version")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        self.info = SessionInfo {
            audio_codec: str_field(d, "audio_codec"),
            video_codec: str_field(d, "video_codec"),
            media_session_id: str_field(d, "media_session_id"),
            keyframe_interval: d.get("keyframe_interval").and_then(Value::as_u64),
        };
        self.emit(VoiceEvent::SessionDescription(SessionDescription {
            mode,
            secret_key,
            info: self.info.clone(),
            dave_protocol_version: dave_version,
        }));
        // load_secret_key: a "not speaking" SPEAKING tells Discord our SSRC, which it needs before
        // it sends any voice data.
        let ssrc = self.shared.ssrc.load(Ordering::Relaxed);
        let speak = json_frame(
            op::SPEAKING,
            json!({"speaking": speaking::NONE, "delay": 0, "ssrc": ssrc}),
        );
        if let Some(exit) = self.send(speak).await {
            return Some(exit);
        }
        match self.dave.on_session_description(dave_version) {
            Ok(outs) => self.send_dave(outs).await,
            Err(e) => {
                self.emit(VoiceEvent::DaveError(e.to_string()));
                None
            }
        }
    }

    async fn on_dave_text(&mut self, opcode: u8, d: &Value) -> Option<Exit> {
        let uint = |key: &str| d.get(key).and_then(Value::as_u64);
        let result = match opcode {
            op::DAVE_PREPARE_TRANSITION => match (
                uint("transition_id").and_then(|v| u16::try_from(v).ok()),
                uint("protocol_version").and_then(|v| u16::try_from(v).ok()),
            ) {
                (Some(id), Some(ver)) => self.dave.on_prepare_transition(id, ver),
                _ => Err(crate::dave::DaveError::Malformed("DAVE_PREPARE_TRANSITION")),
            },
            op::DAVE_EXECUTE_TRANSITION => {
                match uint("transition_id").and_then(|v| u16::try_from(v).ok()) {
                    Some(id) => self.dave.on_execute_transition(id).map(|()| Vec::new()),
                    None => Err(crate::dave::DaveError::Malformed("DAVE_EXECUTE_TRANSITION")),
                }
            }
            _ => match (
                uint("epoch"),
                uint("protocol_version").and_then(|v| u16::try_from(v).ok()),
            ) {
                (Some(epoch), Some(ver)) => self.dave.on_prepare_epoch(epoch, ver),
                _ => Err(crate::dave::DaveError::Malformed("DAVE_PREPARE_EPOCH")),
            },
        };
        match result {
            Ok(outs) => self.send_dave(outs).await,
            Err(e) => {
                self.emit(VoiceEvent::DaveError(e.to_string()));
                None
            }
        }
    }
}

fn str_field(d: &Value, key: &str) -> Option<String> {
    d.get(key).and_then(Value::as_str).map(str::to_owned)
}

/// SESSION_UPDATE: keys absent from `d` keep their value in `previous`.
fn session_info(d: &Value, previous: &SessionInfo) -> SessionInfo {
    SessionInfo {
        audio_codec: str_field(d, "audio_codec").or_else(|| previous.audio_codec.clone()),
        video_codec: str_field(d, "video_codec").or_else(|| previous.video_codec.clone()),
        media_session_id: str_field(d, "media_session_id")
            .or_else(|| previous.media_session_id.clone()),
        keyframe_interval: d
            .get("keyframe_interval")
            .and_then(Value::as_u64)
            .or(previous.keyframe_interval),
    }
}
