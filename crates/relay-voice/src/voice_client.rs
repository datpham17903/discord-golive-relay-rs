//! Voice client: joins a voice channel and keeps the session, port of the connect half of
//! `discord/voice_state.py::VoiceConnectionState` (op 4 / VOICE_STATE_UPDATE / VOICE_SERVER_UPDATE
//! dance) and `native_voice.VoiceClient` (`_connect_rtc_transport`, `disconnect`, `watch_stream`).
//!
//! The main gateway is reached through [`GatewayLink`] and fed back through
//! [`VoiceClient::handle_event`]; see the `link` module for the contract.
//!
//! The parent client holds its own media connection (voice websocket, UDP, DAVE) like the Python
//! `VoiceClient`: Discord only gives a session to a client that really joined the channel, and the
//! stream clients borrow its `session_id`.
//!
//! Recovery follows `_poll_voice_ws`, for this connection and for each stream's:
//! - a websocket that drops with 4015 or without a close code is resumed underneath (RESUME, same
//!   session); [`ClientEvent::Reconnected`] reports it;
//! - a new VOICE_SERVER_UPDATE / STREAM_SERVER_UPDATE naming another server while connected closes
//!   the socket with 4014 and handshakes against the new server, keeping the DAVE state;
//! - a close with 4014 / 4022 waits up to [`RECONNECT_TIMEOUT`] for the gateway to name a server
//!   (being disconnected from voice meanwhile ends it);
//! - anything else ends in [`ClientEvent::Disconnected`] unless [`RtcSettings::reconnect`] is set
//!   (the Python relay passes `reconnect=False`), in which case a fresh handshake is tried.
//!
//! Deviation: with `reconnect`, the voice connection handshakes again with the voice server it
//! knows; Python also left and re-joined the channel (op 4) first.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::connection::{
    AfterClose, ConnEvent, RECONNECT_ATTEMPTS, RECONNECT_TIMEOUT, RtcConnection, RtcSettings,
    RtcTarget, after_close, connect_with_retries,
};
use crate::dave::Dave;
use crate::error::VoiceError;
use crate::link::{
    GatewayLink, LinkEvent, Secret, StreamCreateRequest, StreamDelete, VoiceStateRequest,
};
use crate::stream_client::{Open, StreamClient, StreamInner};
use crate::voice_ws::{SessionInfo, VoiceToken};

/// Reported by the clients, on the channel returned by [`VoiceClient::new`].
#[derive(Clone, Debug, PartialEq)]
pub enum ClientEvent {
    /// op 14: the negotiated codec changed (`stream_key` is `None` for the voice connection itself).
    SessionUpdate {
        stream_key: Option<String>,
        info: SessionInfo,
    },
    DaveError {
        stream_key: Option<String>,
        error: String,
    },
    /// The media connection is gone for good (websocket closed and not recovered, or we were
    /// disconnected from voice).
    Disconnected {
        stream_key: Option<String>,
        code: Option<u16>,
        reason: String,
    },
    /// The connection recovered: its websocket was resumed, or it was re-established (against a new
    /// server). A stream we publish keeps playing.
    Reconnected { stream_key: Option<String> },
    /// STREAM_DELETE for a stream being watched; the stream client is already torn down.
    StreamEnded {
        stream_key: String,
        reason: String,
        unavailable: bool,
    },
    /// STREAM_UPDATE changed the paused flag.
    StreamPaused { stream_key: String, paused: bool },
    /// STREAM_SERVER_UPDATE of our own stream named the same endpoint with a new token: the
    /// connection is kept.
    StreamTokenRefreshed { stream_key: String },
}

pub struct VoiceClientConfig {
    pub user_id: u64,
    /// `None` for a private call.
    pub guild_id: Option<u64>,
    pub channel_id: u64,
    pub gateway: Arc<dyn GatewayLink>,
    /// Shared with REST and the main gateway (one browser fingerprint).
    pub http: wreq::Client,
    /// Codecs, RTX, receive tuning and handshake details of the voice connection itself.
    pub rtc: RtcSettings,
}

/// Called once with the voice session id; see [`JoinOptions::on_voice_state`].
pub type VoiceStateCallback = Box<dyn FnOnce(&str) + Send>;

pub struct JoinOptions {
    pub timeout: Duration,
    pub self_mute: bool,
    pub self_deaf: bool,
    /// Called once, with the session id, as soon as the gateway echoes our voice state for the
    /// channel and before the media connection is set up. From then on watching may be prepared.
    pub on_voice_state: Option<VoiceStateCallback>,
}

impl Default for JoinOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            self_mute: false,
            self_deaf: false,
            on_voice_state: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Joining,
    Connected,
    Leaving,
}

#[derive(Clone)]
pub(crate) struct ServerInfo {
    pub token: Secret,
    /// `host[:port]`, scheme stripped.
    pub endpoint: String,
}

#[derive(Clone, Copy)]
pub(crate) struct StreamInfo {
    pub rtc_server_id: u64,
    pub rtc_channel_id: u64,
    pub paused: bool,
    pub unavailable: bool,
}

pub(crate) struct VcState {
    phase: Phase,
    channel_id: u64,
    session_id: Option<String>,
    server: Option<(ServerInfo, u64)>,
    /// The server the live (or reconnecting) connection was made to; `None` while a forced close
    /// waits for the gateway to name a server.
    connected_server: Option<(String, String)>,
    on_voice_state: Option<VoiceStateCallback>,
    conn: Option<Arc<RtcConnection>>,
    /// DAVE state of the connection, kept across reconnects.
    dave: Option<Arc<Dave>>,
    /// We asked to leave: the next VOICE_STATE_UPDATE without a channel is the answer.
    expecting_leave: bool,
    left: bool,
    pub(crate) streams: HashMap<String, StreamInfo>,
    pub(crate) stream_servers: HashMap<String, ServerInfo>,
    /// STREAM_DELETE reasons seen for streams that are not (yet) clients.
    pub(crate) stream_deleted: HashMap<String, String>,
    pub(crate) stream_clients: HashMap<String, StreamClient>,
}

pub(crate) struct VcInner {
    pub(crate) cfg: VoiceClientConfig,
    pub(crate) state: Mutex<VcState>,
    notify: Notify,
    pub(crate) events: mpsc::UnboundedSender<ClientEvent>,
    generation: AtomicU64,
}

/// Cheap to clone; every clone is the same client.
#[derive(Clone)]
pub struct VoiceClient {
    inner: Arc<VcInner>,
}

impl VoiceClient {
    pub fn new(cfg: VoiceClientConfig) -> (Self, mpsc::UnboundedReceiver<ClientEvent>) {
        let (events, rx) = mpsc::unbounded_channel();
        let channel_id = cfg.channel_id;
        let inner = Arc::new(VcInner {
            cfg,
            state: Mutex::new(VcState {
                phase: Phase::Idle,
                channel_id,
                session_id: None,
                server: None,
                connected_server: None,
                on_voice_state: None,
                conn: None,
                dave: None,
                expecting_leave: false,
                left: false,
                streams: HashMap::new(),
                stream_servers: HashMap::new(),
                stream_deleted: HashMap::new(),
                stream_clients: HashMap::new(),
            }),
            notify: Notify::new(),
            events,
            generation: AtomicU64::new(0),
        });
        (Self { inner }, rx)
    }

    pub fn user_id(&self) -> u64 {
        self.inner.cfg.user_id
    }

    /// The voice session id, once the gateway has echoed our voice state.
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id()
    }

    pub fn channel_id(&self) -> u64 {
        self.inner.state.lock().channel_id
    }

    /// The media connection is up.
    pub fn is_connected(&self) -> bool {
        let st = self.inner.state.lock();
        st.phase == Phase::Connected && st.conn.as_ref().is_some_and(|c| !c.is_closed())
    }

    /// Negotiated media session of the voice connection (`audio_codec`, `video_codec`...).
    pub fn session_info(&self) -> Option<SessionInfo> {
        self.inner
            .state
            .lock()
            .conn
            .as_ref()
            .map(|c| c.session_info())
    }

    /// op 14 SESSION_UPDATE on the voice connection: renegotiate its codec list.
    pub fn session_update(&self, codecs: &[crate::voice_ws::CodecSpec]) -> Result<(), VoiceError> {
        self.conn()?.session_update(codecs)
    }

    /// `send_video_pli` on the voice connection.
    pub fn pli(&self, ssrc: u32) -> Result<(), VoiceError> {
        self.conn()?.send_pli(ssrc)
    }

    /// op 15 MEDIA_SINK_WANTS on the voice connection.
    pub fn request_video(&self, ssrc: u32, quality: u32) -> Result<(), VoiceError> {
        self.conn()?.request_video(ssrc, quality)
    }

    /// The voice connection's media connection (for `send_pli`, stats, ...).
    pub fn connection(&self) -> Option<Arc<RtcConnection>> {
        self.inner.state.lock().conn.clone()
    }

    fn conn(&self) -> Result<Arc<RtcConnection>, VoiceError> {
        self.connection().ok_or(VoiceError::NotConnected)
    }

    /// The Go Live streams currently watched, by stream key.
    pub fn stream_clients(&self) -> Vec<StreamClient> {
        self.inner
            .state
            .lock()
            .stream_clients
            .values()
            .cloned()
            .collect()
    }

    /// Joins the voice channel: op 4, wait for VOICE_STATE_UPDATE and VOICE_SERVER_UPDATE, then the
    /// whole media handshake, all within `opts.timeout`. Returns the voice session id. On failure the
    /// voice state is left again.
    pub async fn connect(&self, opts: JoinOptions) -> Result<String, VoiceError> {
        let inner = &self.inner;
        let deadline = Instant::now() + opts.timeout;
        let channel_id = {
            let mut st = inner.state.lock();
            if st.phase != Phase::Idle {
                return Err(VoiceError::Invalid(
                    "voice client is already connecting or connected".into(),
                ));
            }
            st.phase = Phase::Joining;
            st.session_id = None;
            st.server = None;
            st.left = false;
            st.on_voice_state = opts.on_voice_state;
            st.channel_id
        };
        let result = async {
            inner
                .cfg
                .gateway
                .voice_state(VoiceStateRequest {
                    guild_id: inner.cfg.guild_id,
                    channel_id: Some(channel_id),
                    self_mute: opts.self_mute,
                    self_deaf: opts.self_deaf,
                    self_video: false,
                })
                .await?;
            let (session_id, server, server_id, channel_id) = inner
                .wait_for(deadline, "waiting for the voice server", |st| {
                    let session_id = st.session_id.clone()?;
                    let (server, server_id) = st.server.clone()?;
                    Some((session_id, server, server_id, st.channel_id))
                })
                .await?;
            let target = RtcTarget {
                endpoint: server.endpoint.clone(),
                server_id,
                channel_id,
                user_id: inner.cfg.user_id,
                session_id: session_id.clone(),
                token: VoiceToken::new(server.token.expose()),
                dave_group_id: channel_id,
            };
            let dave = Arc::new(Dave::new(inner.cfg.user_id, channel_id));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (conn, events) =
                RtcConnection::connect(&inner.cfg.http, &target, &inner.cfg.rtc, dave, remaining)
                    .await?;
            Ok::<_, VoiceError>((session_id, server, Arc::new(conn), events))
        }
        .await;
        match result {
            Ok((session_id, server, conn, events)) => {
                let generation = inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
                {
                    let mut st = inner.state.lock();
                    st.phase = Phase::Connected;
                    st.connected_server = Some((server.endpoint, server.token.expose().to_owned()));
                    st.dave = Some(Arc::clone(conn.dave()));
                    st.conn = Some(conn);
                }
                tokio::spawn(Arc::clone(inner).run_voice_events(generation, events));
                Ok(session_id)
            }
            Err(e) => {
                {
                    let mut st = inner.state.lock();
                    st.phase = Phase::Idle;
                    st.on_voice_state = None;
                    st.conn = None;
                }
                // Python disconnects (leaves the voice state) when connecting fails.
                let _ = inner.leave_voice_state().await;
                Err(e)
            }
        }
    }

    /// Feeds one gateway dispatch to the client (see the `link` module for which ones).
    pub fn handle_event(&self, event: LinkEvent) {
        Arc::clone(&self.inner).handle_event(event);
    }

    /// Leaves voice: every watched stream is disconnected first (`_disconnect_stream_clients`), then
    /// op 4 without a channel, the media connection is closed, and the answering VOICE_STATE_UPDATE
    /// is awaited for up to `timeout`.
    pub async fn disconnect(&self, timeout: Duration) {
        let inner = &self.inner;
        let streams = self.stream_clients();
        for stream in streams {
            stream.disconnect().await;
        }
        inner.generation.fetch_add(1, Ordering::SeqCst);
        let conn = {
            let mut st = inner.state.lock();
            if st.phase == Phase::Idle && st.conn.is_none() && st.session_id.is_none() {
                return;
            }
            st.phase = Phase::Leaving;
            st.expecting_leave = true;
            st.left = false;
            st.on_voice_state = None;
            st.conn.take()
        };
        let _ = inner.cfg.gateway.voice_state(inner.leave_request()).await;
        if let Some(conn) = conn {
            conn.close(1000);
        }
        let deadline = Instant::now() + timeout;
        let _ = inner
            .wait_for(deadline, "waiting for the voice disconnect", |st| {
                st.left.then_some(())
            })
            .await;
        let mut st = inner.state.lock();
        st.phase = Phase::Idle;
        st.session_id = None;
        st.server = None;
        st.connected_server = None;
        st.expecting_leave = false;
    }

    /// Watches the Go Live stream `stream_key` (op 20, STREAM_CREATE, STREAM_SERVER_UPDATE, then the
    /// media handshake against the stream server). `settings` is built with
    /// [`RtcSettings::watch`]. Only valid while connected to the stream's voice channel.
    pub async fn watch_stream(
        &self,
        stream_key: &str,
        settings: RtcSettings,
        timeout: Duration,
    ) -> Result<StreamClient, VoiceError> {
        StreamInner::open(&self.inner, stream_key, settings, timeout, Open::Watch).await
    }

    /// Creates our own Go Live in the voice channel we are connected to (`create_stream`): op 18,
    /// wait for STREAM_CREATE and STREAM_SERVER_UPDATE, the media handshake against the stream
    /// server, then op 22 with `paused: false`. `settings` is built with [`RtcSettings::publish`];
    /// send media with [`StreamClient::play`].
    pub async fn create_stream(
        &self,
        settings: RtcSettings,
        timeout: Duration,
    ) -> Result<StreamClient, VoiceError> {
        let cfg = &self.inner.cfg;
        let channel_id = self.channel_id();
        let key = match cfg.guild_id {
            Some(guild) => format!("guild:{guild}:{channel_id}:{}", cfg.user_id),
            None => format!("call:{channel_id}:{}", cfg.user_id),
        };
        let request = StreamCreateRequest {
            guild_id: cfg.guild_id,
            channel_id,
        };
        StreamInner::open(&self.inner, &key, settings, timeout, Open::Create(request)).await
    }
}

/// How a watch request starts: the stream is already watched, or it needs the voice session id.
pub(crate) enum WatchStart {
    Existing(StreamClient),
    Fresh { session_id: String },
}

impl VcInner {
    /// Checks that a watch of `key` (in voice channel `channel`) is allowed right now and clears
    /// what an earlier watch of the same key left behind.
    pub(crate) fn begin_watch(
        &self,
        key: &str,
        channel: Option<u64>,
    ) -> Result<WatchStart, VoiceError> {
        let mut st = self.state.lock();
        // Like Python's watch_stream, only the voice-state registration (session id) is needed,
        // not a finished voice connection: the service watches concurrently with the join.
        let registered = match st.phase {
            Phase::Connected => true,
            Phase::Joining => st.session_id.is_some(),
            Phase::Idle | Phase::Leaving => false,
        };
        if !registered {
            return Err(VoiceError::NotConnected);
        }
        if channel.is_some_and(|c| c != st.channel_id) {
            return Err(VoiceError::Invalid(
                "must be connected to the stream voice channel before watching".into(),
            ));
        }
        if let Some(existing) = st.stream_clients.get(key) {
            return Ok(WatchStart::Existing(existing.clone()));
        }
        let session_id = st.session_id.clone().ok_or(VoiceError::NotConnected)?;
        st.streams.remove(key);
        st.stream_deleted.remove(key);
        Ok(WatchStart::Fresh { session_id })
    }

    /// Registers `client` for `key`; when another one got there first, that one is returned.
    pub(crate) fn register_stream(&self, key: &str, client: StreamClient) -> StreamClient {
        self.state
            .lock()
            .stream_clients
            .entry(key.to_owned())
            .or_insert(client)
            .clone()
    }

    pub(crate) fn remove_stream(&self, key: &str) {
        self.state.lock().stream_clients.remove(key);
    }

    pub(crate) fn stream_server(&self, key: &str) -> Option<ServerInfo> {
        self.state.lock().stream_servers.get(key).cloned()
    }

    pub(crate) fn session_id(&self) -> Option<String> {
        self.state.lock().session_id.clone()
    }

    fn leave_request(&self) -> VoiceStateRequest {
        VoiceStateRequest {
            guild_id: self.cfg.guild_id,
            channel_id: None,
            ..VoiceStateRequest::default()
        }
    }

    async fn leave_voice_state(&self) -> Result<(), crate::link::LinkError> {
        self.cfg.gateway.voice_state(self.leave_request()).await
    }

    /// Waits until `f` produces a value from the state, re-checking on every gateway event.
    pub(crate) async fn wait_for<T>(
        &self,
        deadline: Instant,
        what: &'static str,
        mut f: impl FnMut(&mut VcState) -> Option<T>,
    ) -> Result<T, VoiceError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(value) = f(&mut self.state.lock()) {
                return Ok(value);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(VoiceError::Timeout(what));
            }
        }
    }

    pub(crate) fn emit(&self, event: ClientEvent) {
        let _ = self.events.send(event);
    }

    fn handle_event(self: Arc<Self>, event: LinkEvent) {
        match event {
            LinkEvent::VoiceStateUpdate {
                channel_id,
                session_id,
            } => self.on_voice_state_update(channel_id, session_id),
            LinkEvent::VoiceServerUpdate {
                token,
                endpoint,
                guild_id,
                channel_id,
            } => {
                let (Some(token), Some(endpoint)) = (token, endpoint) else {
                    // endpoint not allocated yet; a later update carries it
                    return;
                };
                let endpoint = endpoint
                    .strip_prefix("wss://")
                    .unwrap_or(&endpoint)
                    .to_owned();
                let mut reconnect = false;
                {
                    let mut st = self.state.lock();
                    let server_id = guild_id
                        .or(channel_id)
                        .or(self.cfg.guild_id)
                        .unwrap_or(st.channel_id);
                    let info = ServerInfo { token, endpoint };
                    if st.phase == Phase::Connected
                        && st
                            .connected_server
                            .as_ref()
                            .is_none_or(|(endpoint, token)| {
                                *endpoint != info.endpoint || token != info.token.expose()
                            })
                    {
                        reconnect = true;
                    }
                    st.server = Some((info, server_id));
                }
                self.notify.notify_waiters();
                if reconnect {
                    self.spawn_reconnect(1);
                }
            }
            LinkEvent::StreamCreate {
                stream_key,
                rtc_server_id,
                rtc_channel_id,
            } => {
                let client = {
                    let mut st = self.state.lock();
                    st.stream_deleted.remove(&stream_key);
                    st.streams.insert(
                        stream_key.clone(),
                        StreamInfo {
                            rtc_server_id,
                            rtc_channel_id,
                            paused: false,
                            unavailable: false,
                        },
                    );
                    st.stream_clients.get(&stream_key).cloned()
                };
                if let Some(client) = client {
                    client.on_stream_create(rtc_server_id, rtc_channel_id);
                }
                self.notify.notify_waiters();
            }
            LinkEvent::StreamServerUpdate {
                stream_key,
                token,
                endpoint,
            } => {
                let (Some(token), Some(endpoint)) = (token, endpoint) else {
                    return;
                };
                let endpoint = endpoint
                    .strip_prefix("wss://")
                    .unwrap_or(&endpoint)
                    .to_owned();
                let client = {
                    let mut st = self.state.lock();
                    st.stream_servers
                        .insert(stream_key.clone(), ServerInfo { token, endpoint });
                    st.stream_clients.get(&stream_key).cloned()
                };
                if let Some(client) = client {
                    client.on_stream_server_update();
                }
                self.notify.notify_waiters();
            }
            LinkEvent::StreamUpdate { stream_key, paused } => {
                let changed = {
                    let mut st = self.state.lock();
                    match (st.streams.get_mut(&stream_key), paused) {
                        (Some(info), Some(paused)) if info.paused != paused => {
                            info.paused = paused;
                            Some(paused)
                        }
                        _ => None,
                    }
                };
                if let Some(paused) = changed {
                    self.emit(ClientEvent::StreamPaused { stream_key, paused });
                }
            }
            LinkEvent::StreamDelete(StreamDelete {
                stream_key,
                reason,
                unavailable,
            }) => {
                let client = {
                    let mut st = self.state.lock();
                    if unavailable {
                        if let Some(info) = st.streams.get_mut(&stream_key) {
                            info.unavailable = true;
                        }
                    } else {
                        st.streams.remove(&stream_key);
                        st.stream_servers.remove(&stream_key);
                        st.stream_deleted.insert(stream_key.clone(), reason.clone());
                    }
                    st.stream_clients.get(&stream_key).cloned()
                };
                if let Some(client) = client {
                    // `_disconnect_from_gateway`: reconnect=False, so the stream is just dropped,
                    // without a STREAM_DELETE of our own.
                    tokio::spawn(async move { client.end(reason, unavailable).await });
                }
                self.notify.notify_waiters();
            }
        }
    }

    fn on_voice_state_update(self: Arc<Self>, channel_id: Option<u64>, session_id: Option<String>) {
        let mut callback = None;
        let mut external = false;
        {
            let mut st = self.state.lock();
            match channel_id {
                None => {
                    st.left = true;
                    if st.expecting_leave {
                        st.expecting_leave = false;
                    } else if matches!(st.phase, Phase::Joining | Phase::Connected) {
                        external = st.phase == Phase::Connected;
                    }
                }
                Some(channel) => {
                    if let Some(session_id) = session_id {
                        st.session_id = Some(session_id.clone());
                        if channel == self.cfg.channel_id {
                            callback = st.on_voice_state.take().map(|cb| (cb, session_id));
                        }
                    }
                    if matches!(st.phase, Phase::Joining | Phase::Connected) {
                        st.channel_id = channel;
                    }
                }
            }
        }
        self.notify.notify_waiters();
        if let Some((callback, session_id)) = callback {
            callback(&session_id);
        }
        if external {
            self.drop_connection(None, "disconnected from voice".into());
        }
    }

    /// The media connection is gone for good: forget it and tell the owner.
    fn drop_connection(&self, code: Option<u16>, reason: String) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let conn = {
            let mut st = self.state.lock();
            st.phase = Phase::Idle;
            st.connected_server = None;
            st.dave = None;
            st.conn.take()
        };
        if let Some(conn) = conn {
            conn.close(1000);
        }
        self.emit(ClientEvent::Disconnected {
            stream_key: None,
            code,
            reason,
        });
    }

    /// Consumes the voice connection's events until it is replaced or closed.
    async fn run_voice_events(
        self: Arc<Self>,
        generation: u64,
        mut events: mpsc::UnboundedReceiver<ConnEvent>,
    ) {
        while let Some(event) = events.recv().await {
            if self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            match event {
                ConnEvent::SessionUpdate(info) => self.emit(ClientEvent::SessionUpdate {
                    stream_key: None,
                    info,
                }),
                ConnEvent::DaveError(error) => self.emit(ClientEvent::DaveError {
                    stream_key: None,
                    error,
                }),
                ConnEvent::Message { .. } => {}
                ConnEvent::Resumed => self.emit(ClientEvent::Reconnected { stream_key: None }),
                ConnEvent::Closed { code, reason } => {
                    match after_close(code, self.cfg.rtc.reconnect) {
                        AfterClose::GiveUp => self.drop_connection(code, reason),
                        AfterClose::AwaitServer => {
                            self.await_voice_server(generation, code, reason)
                        }
                        AfterClose::Reconnect => self.spawn_reconnect(RECONNECT_ATTEMPTS),
                    }
                    return;
                }
            }
        }
    }

    /// `_potential_reconnect` after a 4014 / 4022 close: the next VOICE_SERVER_UPDATE reconnects (see
    /// `handle_event`); without one within [`RECONNECT_TIMEOUT`] the connection is dropped. Being
    /// disconnected from voice meanwhile drops it at once.
    fn await_voice_server(self: &Arc<Self>, generation: u64, code: Option<u16>, reason: String) {
        let conn = {
            let mut st = self.state.lock();
            if self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            st.connected_server = None;
            st.conn.take()
        };
        if let Some(conn) = conn {
            conn.close(1000);
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(RECONNECT_TIMEOUT).await;
            if this.generation.load(Ordering::SeqCst) == generation {
                this.drop_connection(code, reason);
            }
        });
    }

    /// Not async: spawning [`Self::reconnect_voice`] from `run_voice_events` directly would make
    /// their future types depend on each other.
    fn spawn_reconnect(self: &Arc<Self>, attempts: u32) {
        tokio::spawn(Arc::clone(self).reconnect_voice(attempts));
    }

    /// Handshakes again (fresh IDENTIFY, up to `attempts` times) against the voice server the
    /// gateway named last, keeping the DAVE state and the session; a live socket is closed with 4014
    /// first. Runs on VOICE_SERVER_UPDATE while connected (the server moved, or a forced close was
    /// waiting for it) and for `reconnect`. A newer reconnect or a disconnect supersedes it.
    async fn reconnect_voice(self: Arc<Self>, attempts: u32) {
        let (generation, old, dave, target) = {
            let mut st = self.state.lock();
            let (Phase::Connected, Some((server, server_id)), Some(session_id), Some(dave)) = (
                st.phase,
                st.server.clone(),
                st.session_id.clone(),
                st.dave.clone(),
            ) else {
                return;
            };
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            st.connected_server = Some((server.endpoint.clone(), server.token.expose().to_owned()));
            let target = RtcTarget {
                endpoint: server.endpoint,
                server_id,
                channel_id: st.channel_id,
                user_id: self.cfg.user_id,
                session_id,
                token: VoiceToken::new(server.token.expose()),
                dave_group_id: st.channel_id,
            };
            (generation, st.conn.take(), dave, target)
        };
        if let Some(old) = old {
            old.close(4014);
        }
        let connected = connect_with_retries(
            &self.cfg.http,
            &target,
            &self.cfg.rtc,
            &dave,
            RECONNECT_TIMEOUT,
            attempts,
        )
        .await;
        let current = |st: &VcState| {
            self.generation.load(Ordering::SeqCst) == generation && st.phase == Phase::Connected
        };
        match connected {
            Ok((conn, events)) => {
                let conn = Arc::new(conn);
                {
                    let mut st = self.state.lock();
                    if !current(&st) {
                        drop(st);
                        conn.close(1000);
                        return;
                    }
                    st.conn = Some(conn);
                }
                tokio::spawn(Arc::clone(&self).run_voice_events(generation, events));
                self.emit(ClientEvent::Reconnected { stream_key: None });
            }
            Err(e) => {
                let current = current(&self.state.lock());
                if current {
                    self.drop_connection(None, e.to_string());
                }
            }
        }
    }
}
