//! Gateway v9 client, port of `discord/gateway.py::DiscordWebSocket` plus the reconnect loop of
//! `Client.connect` (user accounts: no intents, capabilities bitfield, zstd-stream transport).
//!
//! One tokio task owns the socket. Dispatches are applied to the shared [`Cache`] and then delivered
//! on an unbounded channel (a slow consumer must not stall heartbeats). Commands go through a
//! [`GatewayHandle`]; the 110/60 s send ratelimit is awaited in the caller, like the Python
//! `_rate_limiter.block()`, so a throttled send never blocks reading.
//!
//! Differences from the Python library, all deliberate:
//! - No `Sec-WebSocket-Extensions: permessage-deflate` header: wreq cannot inflate frames, and the
//!   zstd-stream transport already compresses.
//! - A RESUME is only attempted when a session id exists; otherwise the reconnect IDENTIFYs.
//! - Commands queued while the socket is down are dropped (Python's closed socket ignores them),
//!   never replayed onto the next session.

use std::fmt;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;
use wreq::ws::message::{CloseFrame, Message};

use crate::cache::{Cache, CacheChange, SharedCache, StreamKey, StreamType};
use crate::error::{Error, Result};
use crate::http::RestClient;
use crate::props::{API_VERSION, HeadersContext};
use crate::token::Token;

/// `Capabilities.default().value` (verified live: IDENTIFY with it yields READY).
pub const CAPABILITIES: u64 = 22525;
const QOS_HEARTBEAT_VERSION: u32 = 27;
const TIME_SPENT_INTERVAL: Duration = Duration::from_secs(30 * 60);
const MAX_MESSAGE_SIZE: usize = 15 * 1024 * 1024;
/// A connection that stayed up this long reconnects at once and resets the backoff.
const HEALTHY_AFTER: Duration = Duration::from_secs(30);
const COMPRESSION_TYPE: &str = "zstd-stream";

mod op {
    pub const DISPATCH: u64 = 0;
    pub const HEARTBEAT: u64 = 1;
    pub const IDENTIFY: u64 = 2;
    pub const VOICE_STATE: u64 = 4;
    pub const RESUME: u64 = 6;
    pub const RECONNECT: u64 = 7;
    pub const INVALIDATE_SESSION: u64 = 9;
    pub const HELLO: u64 = 10;
    pub const HEARTBEAT_ACK: u64 = 11;
    pub const STREAM_CREATE: u64 = 18;
    pub const STREAM_DELETE: u64 = 19;
    pub const STREAM_WATCH: u64 = 20;
    pub const STREAM_SET_PAUSED: u64 = 22;
    pub const QOS_HEARTBEAT: u64 = 40;
    pub const UPDATE_TIME_SPENT_SESSION_ID: u64 = 41;
}

#[derive(Clone)]
pub struct GatewayConfig {
    pub token: Token,
    pub http: wreq::Client,
    pub headers: Arc<HeadersContext>,
    /// Initial gateway URL; RESUMEs use the `resume_gateway_url` from READY.
    pub url: String,
    /// `Origin` header of the handshake.
    pub origin: String,
    pub api_version: u32,
    /// Transport compression (`compress=zstd-stream`); IDENTIFY then sends `compress: false`.
    pub compress: bool,
    /// No message for this long means the connection is a zombie (`heartbeat_timeout`, 60 s).
    pub heartbeat_timeout: Duration,
    /// `asyncio.wait_for(from_client, 60)`.
    pub connect_timeout: Duration,
}

impl fmt::Debug for GatewayConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("token", &self.token)
            .field("url", &self.url)
            .field("origin", &self.origin)
            .field("api_version", &self.api_version)
            .field("compress", &self.compress)
            .field("heartbeat_timeout", &self.heartbeat_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl GatewayConfig {
    pub fn from_rest(rest: &RestClient) -> Self {
        Self {
            token: rest.token().clone(),
            http: rest.http_client().clone(),
            headers: Arc::clone(rest.headers_context()),
            url: rest.urls().gateway.clone(),
            origin: rest.urls().origin.clone(),
            api_version: API_VERSION,
            compress: true,
            heartbeat_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dispatch {
    /// Event name (`t`), e.g. `READY`, `VOICE_STATE_UPDATE`.
    pub name: String,
    pub seq: Option<u64>,
    pub data: Value,
    /// Cache transition for voice/stream events, `None` for everything else.
    pub change: Option<CacheChange>,
}

#[derive(Debug)]
pub enum GatewayEvent {
    Dispatch(Box<Dispatch>),
    /// The socket went away and the task is reconnecting (`resume`: with RESUME, else IDENTIFY).
    Disconnected {
        code: Option<u16>,
        reason: String,
        resume: bool,
    },
    /// Terminal: closed with a code that must not be retried. The channel closes after this.
    Closed(Error),
}

/// Op 4 payload fields. `self_stream` is only put on the wire when set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VoiceStateUpdate {
    pub guild_id: Option<u64>,
    pub channel_id: Option<u64>,
    pub self_mute: bool,
    pub self_deaf: bool,
    pub self_video: bool,
    pub self_stream: Option<bool>,
}

enum Outgoing {
    Json(String),
    Close,
}

struct Shared {
    limiter: tokio::sync::Mutex<RateLimiter>,
    rtc_connected: AtomicBool,
    /// Heartbeat round trip in microseconds; `u64::MAX` until the first ACK.
    latency_us: AtomicU64,
    /// Session id of the current gateway session (`DiscordWebSocket.session_id`).
    session_id: parking_lot::Mutex<Option<String>>,
    cache: SharedCache,
}

/// Cheap to clone; every clone sends through the same task.
#[derive(Clone)]
pub struct GatewayHandle {
    tx: mpsc::UnboundedSender<Outgoing>,
    shared: Arc<Shared>,
}

impl fmt::Debug for GatewayHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayHandle").finish_non_exhaustive()
    }
}

impl GatewayHandle {
    pub fn cache(&self) -> &SharedCache {
        &self.shared.cache
    }

    /// Latency between the last heartbeat and its ACK.
    pub fn latency(&self) -> Option<Duration> {
        match self.shared.latency_us.load(Ordering::Relaxed) {
            u64::MAX => None,
            us => Some(Duration::from_micros(us)),
        }
    }

    /// Session id of the current gateway session; `None` before READY and after an invalidated
    /// session.
    pub fn session_id(&self) -> Option<String> {
        self.shared.session_id.lock().clone()
    }

    /// Adds the `rtc_connected` reason to heartbeats (`ConnectionState._has_voice_client`).
    pub fn set_rtc_connected(&self, connected: bool) {
        self.shared
            .rtc_connected
            .store(connected, Ordering::Relaxed);
    }

    /// Op 4. Preferred RTC regions from READY are attached when joining a channel.
    pub async fn voice_state(&self, update: VoiceStateUpdate) -> Result<()> {
        let regions = self.shared.cache.read().preferred_rtc_regions().to_vec();
        self.send_json(voice_state_payload(&update, &regions)).await
    }

    /// Op 18 STREAM_CREATE.
    pub async fn stream_create(
        &self,
        stream_type: StreamType,
        guild_id: Option<u64>,
        channel_id: u64,
    ) -> Result<()> {
        let regions = self.shared.cache.read().preferred_rtc_regions().to_vec();
        self.send_json(stream_create_payload(
            stream_type,
            guild_id,
            channel_id,
            &regions,
        ))
        .await
    }

    /// Op 20 STREAM_WATCH.
    pub async fn stream_watch(&self, key: &StreamKey) -> Result<()> {
        self.send_json(json!({"op": op::STREAM_WATCH, "d": {"stream_key": key.to_string()}}))
            .await
    }

    /// Op 19 STREAM_DELETE.
    pub async fn stream_delete(&self, key: &StreamKey) -> Result<()> {
        self.send_json(json!({"op": op::STREAM_DELETE, "d": {"stream_key": key.to_string()}}))
            .await
    }

    /// Op 22 STREAM_SET_PAUSED.
    pub async fn stream_set_paused(&self, key: &StreamKey, paused: bool) -> Result<()> {
        self.send_json(json!({"op": op::STREAM_SET_PAUSED, "d": {"stream_key": key.to_string(), "paused": paused}}))
            .await
    }

    /// Closes the socket with 1000 and ends the task.
    pub fn close(&self) {
        let _ = self.tx.send(Outgoing::Close);
    }

    async fn send_json(&self, payload: Value) -> Result<()> {
        let delay = self.shared.limiter.lock().await.get_delay(Instant::now());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.tx
            .send(Outgoing::Json(payload.to_string()))
            .map_err(|_| Error::GatewayShutDown)
    }
}

pub struct Gateway;

impl Gateway {
    /// Starts the connection task (must run inside a tokio runtime) and returns immediately; the
    /// connection is made, resumed and re-made in the background. Dropping every handle or the
    /// event receiver stops it.
    pub fn spawn(config: GatewayConfig) -> (GatewayHandle, mpsc::UnboundedReceiver<GatewayEvent>) {
        Self::spawn_with_cache(config, Cache::shared())
    }

    /// Like [`Gateway::spawn`], applying dispatches to `cache` (which the caller keeps reading).
    pub fn spawn_with_cache(
        config: GatewayConfig,
        cache: SharedCache,
    ) -> (GatewayHandle, mpsc::UnboundedReceiver<GatewayEvent>) {
        let (tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            limiter: tokio::sync::Mutex::new(RateLimiter::new(110, Duration::from_secs(60))),
            rtc_connected: AtomicBool::new(false),
            latency_us: AtomicU64::new(u64::MAX),
            session_id: parking_lot::Mutex::new(None),
            cache,
        });
        let task = Task {
            config,
            shared: Arc::clone(&shared),
            cmd_rx,
            ev_tx,
            session_id: None,
            seq: None,
            resume_url: None,
        };
        tokio::spawn(task.run());
        (GatewayHandle { tx, shared }, ev_rx)
    }
}

// ---------------------------------------------------------------------------------------------
// payloads

fn voice_state_payload(u: &VoiceStateUpdate, regions: &[String]) -> Value {
    let mut d = Map::new();
    d.insert("guild_id".into(), json!(u.guild_id));
    d.insert("channel_id".into(), json!(u.channel_id));
    d.insert("self_mute".into(), json!(u.self_mute));
    d.insert("self_deaf".into(), json!(u.self_deaf));
    d.insert("self_video".into(), json!(u.self_video));
    if let Some(s) = u.self_stream {
        d.insert("self_stream".into(), json!(s));
    }
    if u.channel_id.is_some_and(|c| c != 0) && !regions.is_empty() {
        d.insert("preferred_regions".into(), json!(regions));
        d.insert("preferred_region".into(), json!(regions[0]));
    }
    json!({"op": op::VOICE_STATE, "d": d})
}

fn stream_create_payload(
    stream_type: StreamType,
    guild_id: Option<u64>,
    channel_id: u64,
    regions: &[String],
) -> Value {
    let mut d = Map::new();
    d.insert("type".into(), json!(stream_type.as_str()));
    d.insert("guild_id".into(), json!(guild_id.map(|g| g.to_string())));
    d.insert("channel_id".into(), json!(channel_id.to_string()));
    if let Some(region) = regions.first() {
        d.insert("preferred_region".into(), json!(region));
    }
    json!({"op": op::STREAM_CREATE, "d": d})
}

fn identify_payload(token: &Token, properties: Map<String, Value>, compress: bool) -> Value {
    json!({
        "op": op::IDENTIFY,
        "d": {
            "token": token.expose(),
            "capabilities": CAPABILITIES,
            "properties": properties,
            "presence": {"status": "unknown", "activities": [], "afk": false, "since": 0},
            // "We require at least one form of compression": inflate is only asked for when the
            // transport is not compressed.
            "compress": !compress,
            "client_state": {"guild_versions": {}},
        },
    })
}

fn resume_payload(token: &Token, session_id: &str, seq: Option<u64>) -> Value {
    json!({"op": op::RESUME, "d": {"seq": seq, "session_id": session_id, "token": token.expose()}})
}

fn heartbeat_payload(seq: Option<u64>, rtc_connected: bool) -> Value {
    let mut reasons = vec!["foregrounded"];
    if rtc_connected {
        reasons.push("rtc_connected");
    }
    json!({
        "op": op::QOS_HEARTBEAT,
        "d": {"qos": {"ver": QOS_HEARTBEAT_VERSION, "active": true, "reasons": reasons}, "seq": seq},
    })
}

fn time_spent_payload(ctx: &HeadersContext) -> Value {
    let props = ctx.gateway_properties();
    json!({
        "op": op::UPDATE_TIME_SPENT_SESSION_ID,
        "d": {
            "initialization_timestamp": ctx.initialization_timestamp_ms,
            "session_id": props.get("client_heartbeat_session_id"),
            "client_launch_id": props.get("client_launch_id"),
        },
    })
}

/// yarl `URL.with_query`: replaces the query and gives a bare authority the `/` path.
fn with_query(base: &str, query: &[(&str, String)]) -> String {
    let base = base.split('?').next().unwrap_or(base);
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let mut url = base.to_owned();
    if !base[after_scheme..].contains('/') {
        url.push('/');
    }
    let query: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{url}?{}", query.join("&"))
}

/// `DiscordWebSocket._can_handle_close`.
fn can_handle_close(code: Option<u16>, own_close_code: Option<u16>) -> bool {
    let code = code.or(own_close_code);
    // A remote 1000 that we did not ask for is an improper close.
    let improper = own_close_code.is_none() && code == Some(1000);
    improper || !matches!(code, Some(1000 | 4004 | 4010 | 4011 | 4012 | 4013 | 4014))
}

// ---------------------------------------------------------------------------------------------
// ratelimit + backoff

/// `GatewayRatelimiter`: `max` sends per `per` window.
struct RateLimiter {
    max: u32,
    remaining: u32,
    window: Option<Instant>,
    per: Duration,
}

impl RateLimiter {
    fn new(max: u32, per: Duration) -> Self {
        Self {
            max,
            remaining: max,
            window: None,
            per,
        }
    }

    fn get_delay(&mut self, now: Instant) -> Duration {
        if self.window.is_none_or(|w| now > w + self.per) {
            self.remaining = self.max;
        }
        if self.remaining == self.max {
            self.window = Some(now);
        }
        if self.remaining == 0 {
            let w = self.window.unwrap_or(now);
            return self.per.saturating_sub(now.saturating_duration_since(w));
        }
        self.remaining -= 1;
        Duration::ZERO
    }
}

/// `backoff.ExponentialBackoff(base=1)`.
struct Backoff {
    exp: u32,
    last: Instant,
}

impl Backoff {
    const BASE_SECS: f64 = 1.0;
    const MAX_EXP: u32 = 10;

    fn new() -> Self {
        Self {
            exp: 0,
            last: Instant::now(),
        }
    }

    fn reset(&mut self) {
        self.exp = 0;
    }

    fn delay(&mut self) -> Duration {
        let now = Instant::now();
        if now.duration_since(self.last).as_secs_f64() > Self::BASE_SECS * 2f64.powi(11) {
            self.exp = 0;
        }
        self.last = now;
        self.exp = (self.exp + 1).min(Self::MAX_EXP);
        Duration::from_secs_f64(fastrand::f64() * Self::BASE_SECS * 2f64.powi(self.exp as i32))
    }
}

// ---------------------------------------------------------------------------------------------
// transport compression

/// One streaming zstd decoder per connection; every WS message is a complete gateway message but
/// the compression context spans messages.
struct ZstdStream(zstd::stream::write::Decoder<'static, Vec<u8>>);

impl ZstdStream {
    fn new() -> std::io::Result<Self> {
        Ok(Self(zstd::stream::write::Decoder::new(Vec::new())?))
    }

    fn decompress(&mut self, data: &[u8]) -> std::io::Result<String> {
        self.0.write_all(data)?;
        self.0.flush()?;
        String::from_utf8(std::mem::take(self.0.get_mut()))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

// ---------------------------------------------------------------------------------------------
// connection task

type Ws = wreq::ws::WebSocket;

/// Why a connection ended.
enum Exit {
    Reconnect {
        resume: bool,
        code: Option<u16>,
        reason: String,
    },
    Fatal {
        code: u16,
        reason: String,
    },
    Shutdown,
}

struct Conn {
    ws: Ws,
    zstd: Option<ZstdStream>,
    /// Set once HELLO arrives.
    interval: Option<Duration>,
    next_beat: Option<Instant>,
    last_recv: Instant,
    last_send: Instant,
    last_time_spent: Instant,
}

struct Task {
    config: GatewayConfig,
    shared: Arc<Shared>,
    cmd_rx: mpsc::UnboundedReceiver<Outgoing>,
    ev_tx: mpsc::UnboundedSender<GatewayEvent>,
    session_id: Option<String>,
    seq: Option<u64>,
    /// `resume_gateway_url` from READY.
    resume_url: Option<String>,
}

impl Task {
    async fn run(mut self) {
        let mut backoff = Backoff::new();
        let mut resume = false;
        loop {
            // A connection that lasted reconnects at once (e.g. a routine op 7); one that dropped
            // soon after connecting waits a growing delay, so an IDENTIFY/close loop slows down.
            let mut lasted = false;
            let exit = match self.connect(resume).await {
                Ok(mut conn) => {
                    let up_since = Instant::now();
                    let exit = self.serve(&mut conn).await;
                    if up_since.elapsed() >= HEALTHY_AFTER {
                        backoff.reset();
                        lasted = true;
                    }
                    exit
                }
                Err(e) => {
                    // OSError/HTTPException/CurlError: back off, then always try to RESUME.
                    self.emit(GatewayEvent::Disconnected {
                        code: None,
                        reason: e.to_string(),
                        resume: self.session_id.is_some(),
                    });
                    let delay = backoff.delay();
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = self.ev_tx.closed() => return,
                    }
                    resume = true;
                    if self.drain_commands() {
                        return;
                    }
                    continue;
                }
            };
            match exit {
                Exit::Reconnect {
                    resume: r,
                    code,
                    reason,
                } => {
                    resume = r && self.session_id.is_some();
                    self.emit(GatewayEvent::Disconnected {
                        code,
                        reason,
                        resume,
                    });
                    if !lasted {
                        let delay = backoff.delay();
                        tokio::select! {
                            () = tokio::time::sleep(delay) => {}
                            () = self.ev_tx.closed() => return,
                        }
                    }
                    if self.drain_commands() {
                        return;
                    }
                }
                Exit::Fatal { code, reason } => {
                    self.emit(GatewayEvent::Closed(Error::GatewayClosed {
                        code: i32::from(code),
                        reason,
                    }));
                    return;
                }
                Exit::Shutdown => return,
            }
        }
    }

    fn emit(&self, event: GatewayEvent) {
        let _ = self.ev_tx.send(event);
    }

    /// Drops commands queued for the dead socket; true when a close request was among them.
    fn drain_commands(&mut self) -> bool {
        let mut close = false;
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            close |= matches!(cmd, Outgoing::Close);
        }
        close
    }

    /// Opens the socket and sends IDENTIFY or RESUME right away (before HELLO, as the Python client).
    async fn connect(&mut self, resume: bool) -> Result<Conn> {
        let resume = resume && self.session_id.is_some();
        let base = if resume {
            self.resume_url
                .clone()
                .unwrap_or_else(|| self.config.url.clone())
        } else {
            self.config.url.clone()
        };
        let mut query = vec![
            ("v", self.config.api_version.to_string()),
            ("encoding", "json".to_owned()),
        ];
        if self.config.compress {
            query.push(("compress", COMPRESSION_TYPE.to_owned()));
        }
        let url = with_query(&base, &query);

        let ctx = &self.config.headers;
        let request = self
            .config
            .http
            .websocket(url)
            .header("Accept", "*/*")
            .header("Accept-Language", "en-US,en;q=0.9")
            // No Origin header: the live gateway answers the wreq upgrade with 403 when it is present
            // (verified 2026-11-09), although the Python client sends one through curl_cffi.
            .header("User-Agent", ctx.user_agent())
            .max_message_size(MAX_MESSAGE_SIZE)
            .send();
        let ws = tokio::time::timeout(self.config.connect_timeout, async {
            request.await?.into_websocket().await
        })
        .await
        .map_err(|_| Error::Http("gateway connect timed out".into()))??;

        let now = Instant::now();
        let mut conn = Conn {
            ws,
            zstd: if self.config.compress {
                Some(ZstdStream::new().map_err(|e| Error::Http(e.to_string()))?)
            } else {
                None
            },
            interval: None,
            next_beat: None,
            last_recv: now,
            last_send: now,
            last_time_spent: now,
        };
        let payload = if resume {
            resume_payload(
                &self.config.token,
                self.session_id.as_deref().unwrap_or_default(),
                self.seq,
            )
        } else {
            self.seq = None;
            let mut props = self.config.headers.gateway_properties();
            if let Some(id) = self.shared.cache.read().installation_id() {
                props.insert("installation_id".into(), json!(id));
            }
            identify_payload(&self.config.token, props, self.config.compress)
        };
        send_text(&mut conn.ws, payload.to_string())
            .await
            .map_err(|e| Error::Http(e.to_string()))?;
        Ok(conn)
    }

    async fn serve(&mut self, conn: &mut Conn) -> Exit {
        loop {
            let beat_at = conn.next_beat;
            let zombie_at = conn.last_recv + self.config.heartbeat_timeout;
            let exit = tokio::select! {
                msg = conn.ws.recv() => self.on_ws_message(conn, msg).await,
                () = sleep_until_opt(beat_at) => self.beat(conn).await,
                () = tokio::time::sleep_until(zombie_at) => {
                    // Nothing received for heartbeat_timeout: close with 4000 and resume.
                    close_ws(&mut conn.ws, 4000).await;
                    Some(Exit::Reconnect { resume: true, code: Some(4000), reason: "no message received".into() })
                }
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(Outgoing::Json(text)) => send_text(&mut conn.ws, text).await.err().map(|e| Exit::Reconnect {
                        resume: true,
                        code: None,
                        reason: e.to_string(),
                    }),
                    Some(Outgoing::Close) | None => {
                        close_ws(&mut conn.ws, 1000).await;
                        Some(Exit::Shutdown)
                    }
                },
                () = self.ev_tx.closed() => {
                    close_ws(&mut conn.ws, 1000).await;
                    Some(Exit::Shutdown)
                }
            };
            if let Some(exit) = exit {
                return exit;
            }
        }
    }

    async fn on_ws_message(
        &mut self,
        conn: &mut Conn,
        msg: Option<std::result::Result<Message, wreq::Error>>,
    ) -> Option<Exit> {
        let reconnect = |code: Option<u16>, reason: String| Exit::Reconnect {
            resume: true,
            code,
            reason,
        };
        let text = match msg {
            None => return Some(self.on_close(None, String::new())),
            Some(Err(e)) => return Some(reconnect(None, e.to_string())),
            Some(Ok(Message::Close(frame))) => {
                let (code, reason) = frame.map_or((None, String::new()), |f| {
                    (Some(u16::from(f.code)), f.reason.as_str().to_owned())
                });
                return Some(self.on_close(code, reason));
            }
            Some(Ok(Message::Text(t))) => t.as_str().to_owned(),
            Some(Ok(Message::Binary(b))) => match conn.zstd.as_mut().map(|z| z.decompress(&b)) {
                Some(Ok(t)) => t,
                Some(Err(e)) => return Some(reconnect(None, format!("zstd: {e}"))),
                None => match String::from_utf8(b.to_vec()) {
                    Ok(t) => t,
                    Err(e) => return Some(reconnect(None, e.to_string())),
                },
            },
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => return None,
        };
        let Ok(msg) = serde_json::from_str::<Value>(&text) else {
            return Some(reconnect(None, "invalid gateway JSON".into()));
        };
        self.handle_payload(conn, msg).await
    }

    fn on_close(&self, code: Option<u16>, reason: String) -> Exit {
        if can_handle_close(code, None) {
            Exit::Reconnect {
                resume: true,
                code,
                reason,
            }
        } else {
            Exit::Fatal {
                code: code.unwrap_or(0),
                reason,
            }
        }
    }

    /// `DiscordWebSocket.received_message`.
    async fn handle_payload(&mut self, conn: &mut Conn, msg: Value) -> Option<Exit> {
        let event = msg.get("t").and_then(Value::as_str);
        let seq = msg.get("s").and_then(Value::as_u64);
        if let Some(s) = seq {
            self.seq = Some(s);
        }
        conn.last_recv = Instant::now();
        let data = msg.get("d").cloned().unwrap_or(Value::Null);

        match msg.get("op").and_then(Value::as_u64) {
            Some(op::DISPATCH) => {
                return self
                    .on_dispatch(conn, event.unwrap_or_default(), seq, data)
                    .await;
            }
            Some(op::RECONNECT) => {
                close_ws(&mut conn.ws, 4000).await;
                return Some(Exit::Reconnect {
                    resume: true,
                    code: Some(4000),
                    reason: "RECONNECT".into(),
                });
            }
            Some(op::HEARTBEAT_ACK) => {
                let latency = conn.last_send.elapsed();
                self.shared
                    .latency_us
                    .store(latency.as_micros() as u64, Ordering::Relaxed);
            }
            Some(op::HEARTBEAT) => return self.send_heartbeat(conn).await,
            Some(op::HELLO) => {
                let ms = data
                    .get("heartbeat_interval")
                    .and_then(Value::as_f64)
                    .unwrap_or(41_250.0);
                let interval = Duration::from_secs_f64(ms / 1000.0);
                conn.interval = Some(interval);
                // Send a heartbeat immediately, then every interval.
                if let Some(exit) = self.send_heartbeat(conn).await {
                    return Some(exit);
                }
                conn.next_beat = Some(Instant::now() + interval);
            }
            Some(op::INVALIDATE_SESSION) => {
                if data.as_bool() == Some(true) {
                    close_ws(&mut conn.ws, 4000).await;
                    return Some(Exit::Reconnect {
                        resume: true,
                        code: Some(4000),
                        reason: "INVALID_SESSION".into(),
                    });
                }
                self.seq = None;
                self.session_id = None;
                *self.shared.session_id.lock() = None;
                self.resume_url = None;
                close_ws(&mut conn.ws, 1000).await;
                return Some(Exit::Reconnect {
                    resume: false,
                    code: Some(1000),
                    reason: "session invalidated".into(),
                });
            }
            _ => {}
        }
        None
    }

    async fn on_dispatch(
        &mut self,
        conn: &mut Conn,
        name: &str,
        seq: Option<u64>,
        data: Value,
    ) -> Option<Exit> {
        if name == "READY" {
            self.session_id = data
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            self.shared.session_id.lock().clone_from(&self.session_id);
            if let Some(url) = data.get("resume_gateway_url").and_then(Value::as_str) {
                self.resume_url = Some(url.to_owned());
            }
            // At READY: UPDATE_TIME_SPENT_SESSION_ID, a heartbeat, then the initial op 4.
            let time_spent = time_spent_payload(&self.config.headers).to_string();
            if let Err(e) = send_text(&mut conn.ws, time_spent).await {
                return Some(Exit::Reconnect {
                    resume: true,
                    code: None,
                    reason: e.to_string(),
                });
            }
            conn.last_time_spent = Instant::now();
            if let Some(exit) = self.send_heartbeat(conn).await {
                return Some(exit);
            }
            let initial = voice_state_payload(&VoiceStateUpdate::default(), &[]).to_string();
            if let Err(e) = send_text(&mut conn.ws, initial).await {
                return Some(Exit::Reconnect {
                    resume: true,
                    code: None,
                    reason: e.to_string(),
                });
            }
        }
        let change = self.shared.cache.write().apply(name, &data);
        self.emit(GatewayEvent::Dispatch(Box::new(Dispatch {
            name: name.to_owned(),
            seq,
            data,
            change,
        })));
        None
    }

    /// Heartbeat tick: time-spent update when due, then the QOS heartbeat.
    async fn beat(&mut self, conn: &mut Conn) -> Option<Exit> {
        if conn.last_time_spent + TIME_SPENT_INTERVAL < Instant::now() {
            let payload = time_spent_payload(&self.config.headers).to_string();
            if let Err(e) = send_text(&mut conn.ws, payload).await {
                return Some(Exit::Reconnect {
                    resume: true,
                    code: None,
                    reason: e.to_string(),
                });
            }
            conn.last_time_spent = Instant::now();
        }
        if let Some(exit) = self.send_heartbeat(conn).await {
            return Some(exit);
        }
        conn.next_beat = conn.interval.map(|i| Instant::now() + i);
        None
    }

    async fn send_heartbeat(&mut self, conn: &mut Conn) -> Option<Exit> {
        let payload =
            heartbeat_payload(self.seq, self.shared.rtc_connected.load(Ordering::Relaxed))
                .to_string();
        // Stamp before the send so a slow send does not shrink the measured latency.
        conn.last_send = Instant::now();
        send_text(&mut conn.ws, payload)
            .await
            .err()
            .map(|e| Exit::Reconnect {
                resume: true,
                code: None,
                reason: e.to_string(),
            })
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

async fn send_text(ws: &mut Ws, text: String) -> std::result::Result<(), wreq::Error> {
    ws.send(Message::text(text)).await
}

async fn close_ws(ws: &mut Ws, code: u16) {
    let frame = CloseFrame {
        code: code.into(),
        reason: "".into(),
    };
    let _ = ws.send(Message::Close(Some(frame))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_query_matches_yarl() {
        let q = [
            ("v", "9".to_owned()),
            ("encoding", "json".to_owned()),
            ("compress", "zstd-stream".to_owned()),
        ];
        assert_eq!(
            with_query("wss://gateway.discord.gg/", &q),
            "wss://gateway.discord.gg/?v=9&encoding=json&compress=zstd-stream"
        );
        assert_eq!(
            with_query("wss://gw-us.discord.gg", &q[..2]),
            "wss://gw-us.discord.gg/?v=9&encoding=json"
        );
        assert_eq!(
            with_query("ws://127.0.0.1:99/x?old=1", &q[..1]),
            "ws://127.0.0.1:99/x?v=9"
        );
    }

    #[test]
    fn close_code_classification() {
        // Resumable server closes.
        for code in [
            None,
            Some(1006),
            Some(4000),
            Some(4001),
            Some(4007),
            Some(4009),
        ] {
            assert!(can_handle_close(code, None), "{code:?}");
        }
        // Remote 1000 that we did not ask for is improper, so reconnect.
        assert!(can_handle_close(Some(1000), None));
        // Fatal.
        for code in [4004, 4010, 4011, 4012, 4013, 4014] {
            assert!(!can_handle_close(Some(code), None), "{code}");
        }
        // Our own 1000.
        assert!(!can_handle_close(Some(1000), Some(1000)));
    }

    #[test]
    fn voice_state_payload_shape() {
        let regions = vec!["rotterdam".to_owned(), "frankfurt".to_owned()];
        let join = VoiceStateUpdate {
            guild_id: Some(1),
            channel_id: Some(2),
            self_video: true,
            self_stream: Some(true),
            ..Default::default()
        };
        assert_eq!(
            voice_state_payload(&join, &regions).to_string(),
            r#"{"op":4,"d":{"guild_id":1,"channel_id":2,"self_mute":false,"self_deaf":false,"self_video":true,"self_stream":true,"preferred_regions":["rotterdam","frankfurt"],"preferred_region":"rotterdam"}}"#
        );
        let leave = VoiceStateUpdate {
            guild_id: Some(1),
            ..Default::default()
        };
        assert_eq!(
            voice_state_payload(&leave, &regions).to_string(),
            r#"{"op":4,"d":{"guild_id":1,"channel_id":null,"self_mute":false,"self_deaf":false,"self_video":false}}"#
        );
    }

    #[test]
    fn stream_payload_shapes() {
        let regions = vec!["us-central".to_owned()];
        assert_eq!(
            stream_create_payload(StreamType::Guild, Some(5), 6, &regions).to_string(),
            r#"{"op":18,"d":{"type":"guild","guild_id":"5","channel_id":"6","preferred_region":"us-central"}}"#
        );
        assert_eq!(
            stream_create_payload(StreamType::Call, None, 6, &[]).to_string(),
            r#"{"op":18,"d":{"type":"call","guild_id":null,"channel_id":"6"}}"#
        );
    }

    #[test]
    fn heartbeat_payload_shape() {
        assert_eq!(
            heartbeat_payload(Some(7), false).to_string(),
            r#"{"op":40,"d":{"qos":{"ver":27,"active":true,"reasons":["foregrounded"]},"seq":7}}"#
        );
        assert_eq!(
            heartbeat_payload(None, true).to_string(),
            r#"{"op":40,"d":{"qos":{"ver":27,"active":true,"reasons":["foregrounded","rtc_connected"]},"seq":null}}"#
        );
    }

    #[test]
    fn rate_limiter_blocks_after_max() {
        let mut rl = RateLimiter::new(3, Duration::from_secs(60));
        let t = Instant::now();
        for _ in 0..3 {
            assert_eq!(rl.get_delay(t), Duration::ZERO);
        }
        let d = rl.get_delay(t + Duration::from_secs(10));
        assert_eq!(d, Duration::from_secs(50));
        assert_eq!(rl.get_delay(t + Duration::from_secs(61)), Duration::ZERO);
    }

    #[test]
    fn zstd_stream_keeps_context_across_messages() {
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        let mut dec = ZstdStream::new().unwrap();
        for msg in [
            r#"{"op":10,"d":{"heartbeat_interval":41250}}"#,
            r#"{"op":11,"d":null}"#,
        ] {
            enc.write_all(msg.as_bytes()).unwrap();
            enc.flush().unwrap();
            let chunk = std::mem::take(enc.get_mut());
            assert_eq!(dec.decompress(&chunk).unwrap(), msg);
        }
    }
}
