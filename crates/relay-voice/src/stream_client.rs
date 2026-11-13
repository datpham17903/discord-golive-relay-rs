//! Stream client: watches or publishes a Go Live stream. Port of `native_voice/stream_client.py`
//! (`StreamConnectionState`, `StreamClient`) and `VoiceClient.watch_stream` / `create_stream` /
//! `ConnectionState._connect_stream`.
//!
//! A watch is: STREAM_WATCH (op 20), wait for STREAM_CREATE (which carries `rtc_server_id` /
//! `rtc_channel_id`) and STREAM_SERVER_UPDATE (token + endpoint), then an ordinary RTC handshake
//! against the stream server using the parent voice session's `session_id`. The DAVE group of a
//! stream is `rtc_server_id - 1`. Creating a stream is the same with STREAM_CREATE (op 18) as the
//! request and our own stream key; the owner then sends media through [`StreamClient::play`] (see
//! the `publish` module).
//!
//! A stream recovers like the voice connection (see the `voice_client` module). When our own stream
//! moves to another server (`StreamConnectionState.stream_server_update`) the new connection keeps
//! the DAVE session, no second op 18 goes out, and the sender carries on with the same source on the
//! new connection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::connection::{
    AfterClose, ConnEvent, MediaCallback, RECONNECT_ATTEMPTS, RECONNECT_TIMEOUT, RtcConnection,
    RtcSettings, RtcTarget, after_close, connect_with_retries, media_callback,
};
use crate::dave::Dave;
use crate::error::VoiceError;
use crate::link::{GatewayLink, StreamCreateRequest};
use crate::publish::{PliHandler, SendStats, Sender, VideoParams};
use crate::receive::{Packet, Receiver};
use crate::voice_client::{ClientEvent, VcInner, WatchStart};
use crate::voice_ws::{CodecSpec, SessionInfo, StreamSpec, VoiceToken};
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;

impl RtcSettings {
    /// Settings of a watch connection: `codecs` advertised for decoding, one `screen` stream
    /// descriptor (`StreamClient.video_simulcast_streams`).
    pub fn watch(codecs: Vec<CodecSpec>, rtx_enabled: bool) -> Self {
        Self::new(
            codecs,
            vec![StreamSpec::screen("100", 100, None)],
            rtx_enabled,
        )
    }
}

impl RtcSettings {
    /// Settings of the connection of a stream we publish (`output_stream_client`): the codecs we may
    /// send (`encode`), one `screen` stream descriptor with `max_framerate = fps`.
    pub fn publish(codecs: Vec<CodecSpec>, rtx_enabled: bool, fps: u32) -> Self {
        Self::new(
            codecs,
            vec![StreamSpec::screen("100", 100, Some(fps))],
            rtx_enabled,
        )
    }
}

/// How a stream connection is requested.
pub(crate) enum Open {
    Watch,
    Create(StreamCreateRequest),
}

/// `(channel id, owner id)` of a guild or call stream key; `None` channel for `test:` keys.
fn parse_stream_key(key: &str) -> Result<(Option<u64>, u64), VoiceError> {
    let bad = || VoiceError::Invalid(format!("invalid stream key: {key:?}"));
    let parts: Vec<&str> = key.split(':').collect();
    let num = |s: &str| s.parse::<u64>().map_err(|_| bad());
    match (parts.first().copied(), parts.len()) {
        (Some("guild"), 4) => {
            num(parts[1])?;
            Ok((Some(num(parts[2])?), num(parts[3])?))
        }
        (Some("call"), 3) => Ok((Some(num(parts[1])?), num(parts[2])?)),
        (Some("test"), 2) => Ok((None, num(parts[1])?)),
        _ => Err(bad()),
    }
}

struct StreamState {
    conn: Option<Arc<RtcConnection>>,
    callback: Option<MediaCallback>,
    rtc_server_id: u64,
    rtc_channel_id: u64,
    /// `(endpoint, token)` the live connection uses, or the one being reconnected to; `None` while
    /// a forced close waits for a server.
    connected_server: Option<(String, String)>,
    /// The connection was dropped to be replaced (server moved, forced close, `reconnect`).
    reconnecting: bool,
    /// DAVE state of the connection, kept across reconnects.
    dave: Option<Arc<Dave>>,
    ended: bool,
    /// Set by [`StreamClient::play`]; only a stream we own has one.
    sender: Option<Arc<Sender>>,
    pli: Option<PliHandler>,
}

pub(crate) struct StreamInner {
    key: String,
    vc: Weak<VcInner>,
    gateway: Arc<dyn GatewayLink>,
    settings: RtcSettings,
    state: Mutex<StreamState>,
    generation: AtomicU64,
    /// We created this stream (so we can publish on it).
    own: bool,
}

/// A connected Go Live stream (watched or our own). Cheap to clone.
#[derive(Clone)]
pub struct StreamClient {
    inner: Arc<StreamInner>,
}

impl StreamInner {
    /// `VoiceClient::watch_stream` / `create_stream`.
    pub(crate) async fn open(
        vc: &Arc<VcInner>,
        key: &str,
        settings: RtcSettings,
        timeout: Duration,
        how: Open,
    ) -> Result<StreamClient, VoiceError> {
        let (channel, owner) = parse_stream_key(key)?;
        match &how {
            Open::Watch if owner == vc.cfg.user_id => {
                return Err(VoiceError::Invalid("cannot watch a stream you own".into()));
            }
            Open::Create(_) if owner != vc.cfg.user_id => {
                return Err(VoiceError::Invalid(
                    "cannot create a stream for another user".into(),
                ));
            }
            _ => {}
        }
        let deadline = Instant::now() + timeout;
        let session_id = match vc.begin_watch(key, channel)? {
            WatchStart::Existing(client) => return Ok(client),
            WatchStart::Fresh { session_id } => session_id,
        };
        match &how {
            Open::Watch => vc.cfg.gateway.stream_watch(key).await?,
            Open::Create(request) => vc.cfg.gateway.stream_create(*request).await?,
        }
        let info = vc
            .wait_for(deadline, "waiting for STREAM_CREATE", |st| {
                if let Some(reason) = st.stream_deleted.get(key) {
                    return Some(Err(VoiceError::StreamRejected(reason.clone())));
                }
                st.streams.get(key).copied().map(Ok)
            })
            .await??;

        let inner = Arc::new(StreamInner {
            key: key.to_owned(),
            vc: Arc::downgrade(vc),
            gateway: Arc::clone(&vc.cfg.gateway),
            settings,
            state: Mutex::new(StreamState {
                conn: None,
                callback: None,
                rtc_server_id: info.rtc_server_id,
                rtc_channel_id: info.rtc_channel_id,
                connected_server: None,
                reconnecting: false,
                dave: None,
                ended: false,
                sender: None,
                pli: None,
            }),
            generation: AtomicU64::new(0),
            own: matches!(how, Open::Create(_)),
        });
        let client = vc.register_stream(
            key,
            StreamClient {
                inner: Arc::clone(&inner),
            },
        );
        if !Arc::ptr_eq(&client.inner, &inner) {
            return Ok(client);
        }
        match inner.establish(vc, session_id, deadline).await {
            Ok(()) => {
                if inner.own {
                    // The Discord client resumes a stream it just created; the Python library never
                    // sent op 22, so a failure here is not worth aborting the stream.
                    let _ = inner.gateway.stream_set_paused(key, false).await;
                }
                Ok(client)
            }
            Err(e) => {
                inner.teardown(true).await;
                Err(e)
            }
        }
    }

    /// Waits for the stream server, then connects (`StreamConnectionState._voice_connect`).
    async fn establish(
        self: &Arc<Self>,
        vc: &Arc<VcInner>,
        session_id: String,
        deadline: Instant,
    ) -> Result<(), VoiceError> {
        let key = self.key.clone();
        let server = vc
            .wait_for(deadline, "waiting for STREAM_SERVER_UPDATE", |st| {
                if let Some(reason) = st.stream_deleted.get(&key) {
                    return Some(Err(VoiceError::StreamRejected(reason.clone())));
                }
                st.stream_servers.get(&key).cloned().map(Ok)
            })
            .await??;
        let (rtc_server_id, rtc_channel_id) = {
            let st = self.state.lock();
            (st.rtc_server_id, st.rtc_channel_id)
        };
        let target = RtcTarget {
            endpoint: server.endpoint.clone(),
            server_id: rtc_server_id,
            channel_id: rtc_channel_id,
            user_id: vc.cfg.user_id,
            session_id,
            token: VoiceToken::new(server.token.expose()),
            dave_group_id: rtc_server_id.saturating_sub(1),
        };
        let dave = Arc::new(Dave::new(vc.cfg.user_id, target.dave_group_id));
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (conn, events) =
            RtcConnection::connect(&vc.cfg.http, &target, &self.settings, dave, remaining).await?;
        self.install(
            conn,
            events,
            (server.endpoint, server.token.expose().to_owned()),
        );
        Ok(())
    }

    /// Makes `conn` the live connection: events pumped, the listener (if any) attached.
    fn install(
        self: &Arc<Self>,
        conn: RtcConnection,
        events: mpsc::UnboundedReceiver<ConnEvent>,
        server: (String, String),
    ) {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let conn = Arc::new(conn);
        let callback = {
            let mut st = self.state.lock();
            st.connected_server = Some(server);
            st.reconnecting = false;
            st.dave = Some(Arc::clone(conn.dave()));
            st.conn = Some(Arc::clone(&conn));
            st.callback.clone()
        };
        if let Some(callback) = callback {
            let _ = conn.listen(callback);
        }
        tokio::spawn(Arc::clone(self).run_events(generation, events));
    }

    async fn run_events(
        self: Arc<Self>,
        generation: u64,
        mut events: mpsc::UnboundedReceiver<ConnEvent>,
    ) {
        let Some(vc) = self.vc.upgrade() else { return };
        let stream_key = Some(self.key.clone());
        while let Some(event) = events.recv().await {
            if self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            match event {
                ConnEvent::SessionUpdate(info) => vc.emit(ClientEvent::SessionUpdate {
                    stream_key: stream_key.clone(),
                    info,
                }),
                ConnEvent::DaveError(error) => vc.emit(ClientEvent::DaveError {
                    stream_key: stream_key.clone(),
                    error,
                }),
                ConnEvent::Message { .. } => {}
                ConnEvent::Resumed => vc.emit(ClientEvent::Reconnected {
                    stream_key: stream_key.clone(),
                }),
                ConnEvent::Closed { code, reason } => {
                    match after_close(code, self.settings.reconnect) {
                        AfterClose::GiveUp => {
                            // Python's poll loop ends in `disconnect()`, which also tells the gateway.
                            self.teardown(true).await;
                            vc.emit(ClientEvent::Disconnected {
                                stream_key,
                                code,
                                reason,
                            });
                        }
                        AfterClose::AwaitServer => self.await_server(generation, code, reason),
                        AfterClose::Reconnect => {
                            tokio::spawn(Arc::clone(&self).migrate(RECONNECT_ATTEMPTS));
                        }
                    }
                    return;
                }
            }
        }
    }

    /// `_potential_reconnect` after a 4014 / 4022 close: the next STREAM_SERVER_UPDATE reconnects;
    /// without one within [`RECONNECT_TIMEOUT`] the stream is torn down (STREAM_DELETE meanwhile
    /// ends it at once).
    fn await_server(self: &Arc<Self>, generation: u64, code: Option<u16>, reason: String) {
        let conn = {
            let mut st = self.state.lock();
            if st.ended || self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            st.reconnecting = true;
            st.connected_server = None;
            st.conn.take()
        };
        if let Some(conn) = conn {
            conn.close(1000);
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(RECONNECT_TIMEOUT).await;
            if this.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            this.teardown(true).await;
            if let Some(vc) = this.vc.upgrade() {
                vc.emit(ClientEvent::Disconnected {
                    stream_key: Some(this.key.clone()),
                    code,
                    reason,
                });
            }
        });
    }

    /// Drops the connection and unregisters the stream; `delete` also sends STREAM_DELETE (op 19).
    pub(crate) async fn teardown(self: &Arc<Self>, delete: bool) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let (conn, sender) = {
            let mut st = self.state.lock();
            if st.ended {
                return;
            }
            st.ended = true;
            st.connected_server = None;
            (st.conn.take(), st.sender.take())
        };
        if let Some(vc) = self.vc.upgrade() {
            vc.remove_stream(&self.key);
        }
        if let Some(sender) = sender {
            // Opus silence and the empty VIDEO state go out before the connection closes; the
            // thread may be mid-burst, so wait for it off the runtime.
            let _ = tokio::task::spawn_blocking(move || sender.stop()).await;
        }
        if delete {
            let _ = self.gateway.stream_delete(&self.key).await;
        }
        if let Some(conn) = conn {
            conn.close(1000);
        }
    }

    /// STREAM_SERVER_UPDATE for a connected stream: move to the new server if it differs from the
    /// one in use (the Python client reconnects on every one; a repeated identical update only
    /// churns the connection). A stream waiting after a forced close reconnects on any.
    fn on_server_update(self: &Arc<Self>) {
        let Some(vc) = self.vc.upgrade() else { return };
        let Some(server) = vc.stream_server(&self.key) else {
            return;
        };
        {
            let mut st = self.state.lock();
            if st.ended || (st.conn.is_none() && !st.reconnecting) {
                return;
            }
            match st.connected_server.as_ref() {
                Some((endpoint, token)) if *endpoint == server.endpoint => {
                    if token == server.token.expose() {
                        return;
                    }
                    if self.own && !st.reconnecting {
                        // Same server, fresh token: the live connection stays valid, so keep it
                        // and only remember the token (the owner never reconnects with it).
                        st.connected_server =
                            Some((server.endpoint, server.token.expose().to_owned()));
                        drop(st);
                        vc.emit(ClientEvent::StreamTokenRefreshed {
                            stream_key: self.key.clone(),
                        });
                        return;
                    }
                }
                _ => {}
            }
        }
        tokio::spawn(Arc::clone(self).migrate(1));
    }

    /// Closes the old socket with 4014 and handshakes (up to `attempts` times) against the stream
    /// server the gateway named last, keeping the DAVE state, the listener, the session and, for our
    /// own stream, the sender with its source. A newer migration or a teardown supersedes it.
    async fn migrate(self: Arc<Self>, attempts: u32) {
        let Some(vc) = self.vc.upgrade() else { return };
        let (Some(server), Some(session_id)) = (vc.stream_server(&self.key), vc.session_id())
        else {
            return;
        };
        let (generation, old, dave, rtc_server_id, rtc_channel_id) = {
            let mut st = self.state.lock();
            let (false, Some(dave)) = (st.ended, st.dave.clone()) else {
                return;
            };
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            st.reconnecting = true;
            st.connected_server = Some((server.endpoint.clone(), server.token.expose().to_owned()));
            (
                generation,
                st.conn.take(),
                dave,
                st.rtc_server_id,
                st.rtc_channel_id,
            )
        };
        if let Some(old) = old {
            old.close(4014);
        }
        let target = RtcTarget {
            endpoint: server.endpoint.clone(),
            server_id: rtc_server_id,
            channel_id: rtc_channel_id,
            user_id: vc.cfg.user_id,
            session_id,
            token: VoiceToken::new(server.token.expose()),
            dave_group_id: rtc_server_id.saturating_sub(1),
        };
        let connected = connect_with_retries(
            &vc.cfg.http,
            &target,
            &self.settings,
            &dave,
            RECONNECT_TIMEOUT,
            attempts,
        )
        .await;
        let superseded =
            || self.state.lock().ended || self.generation.load(Ordering::SeqCst) != generation;
        let failure = match connected {
            _ if superseded() => {
                // a newer migration took over; a connection made here is dropped (and closed)
                return;
            }
            Ok((conn, events)) => {
                self.install(
                    conn,
                    events,
                    (server.endpoint, server.token.expose().to_owned()),
                );
                match self.rebind_sender() {
                    Ok(()) => {
                        vc.emit(ClientEvent::Reconnected {
                            stream_key: Some(self.key.clone()),
                        });
                        return;
                    }
                    Err(e) => e,
                }
            }
            Err(e) => e,
        };
        self.teardown(true).await;
        vc.emit(ClientEvent::Disconnected {
            stream_key: Some(self.key.clone()),
            code: None,
            reason: failure.to_string(),
        });
    }

    /// Moves the sender of our own stream (if any) to the live connection. When the server
    /// negotiated another video codec the owner is told with a `SessionUpdate`, and swaps the
    /// source for it (`swap_output_source`) the way it does after op 14.
    fn rebind_sender(&self) -> Result<(), VoiceError> {
        let (sender, conn) = {
            let st = self.state.lock();
            (st.sender.clone(), st.conn.clone())
        };
        let (Some(sender), Some(conn)) = (sender, conn) else {
            return Ok(());
        };
        let info = conn.session_info();
        if sender.rebind(conn)?
            && let Some(vc) = self.vc.upgrade()
        {
            vc.emit(ClientEvent::SessionUpdate {
                stream_key: Some(self.key.clone()),
                info,
            });
        }
        Ok(())
    }
}

impl StreamClient {
    /// The stream key (`guild:<guild>:<channel>:<owner>`).
    pub fn key(&self) -> &str {
        &self.inner.key
    }

    fn conn(&self) -> Result<Arc<RtcConnection>, VoiceError> {
        self.inner
            .state
            .lock()
            .conn
            .clone()
            .ok_or(VoiceError::NotConnected)
    }

    pub fn is_connected(&self) -> bool {
        self.inner
            .state
            .lock()
            .conn
            .as_ref()
            .is_some_and(|c| !c.is_closed())
    }

    /// Not connected, but on the way back: the stream moved to another server, a forced close waits
    /// for one, `reconnect` handshakes again, or the connection has just closed and the client is
    /// deciding what to do. Ends with a new connection ([`ClientEvent::Reconnected`]) or with the
    /// stream torn down ([`ClientEvent::Disconnected`]), within about
    /// [`RECONNECT_TIMEOUT`]. A websocket resume keeps [`StreamClient::is_connected`] true.
    pub fn is_reconnecting(&self) -> bool {
        let st = self.inner.state.lock();
        !st.ended && (st.reconnecting || st.conn.as_ref().is_some_and(|c| c.is_closed()))
    }

    /// Our own SSRC on the stream connection (the sender SSRC of RTCP feedback).
    pub fn own_ssrc(&self) -> Result<u32, VoiceError> {
        Ok(self.conn()?.ssrc())
    }

    /// Negotiated media session (`video_codec`...), as updated by op 14.
    pub fn session_info(&self) -> Result<SessionInfo, VoiceError> {
        Ok(self.conn()?.session_info())
    }

    /// Starts delivering [`Packet`]s to `callback` on the UDP dispatcher thread (`listen`). The
    /// callback is kept across a server migration. Call it right after the watch connects: media
    /// that arrives before is dropped.
    pub fn listen(&self, callback: impl FnMut(Packet) + Send + 'static) -> Result<(), VoiceError> {
        let callback = media_callback(callback);
        let conn = self.conn()?;
        let mut st = self.inner.state.lock();
        if st.callback.is_some() {
            return Err(VoiceError::AlreadyListening);
        }
        conn.listen(Arc::clone(&callback))?;
        st.callback = Some(callback);
        Ok(())
    }

    pub fn is_listening(&self) -> bool {
        self.inner.state.lock().callback.is_some()
    }

    /// `stop_listening`.
    pub fn stop_listening(&self) {
        let (conn, had) = {
            let mut st = self.inner.state.lock();
            (st.conn.clone(), st.callback.take().is_some())
        };
        if let (Some(conn), true) = (conn, had) {
            conn.stop_listening();
        }
    }

    /// op 12 `request_video` equivalent for a viewer: MEDIA_SINK_WANTS (op 15) for `ssrc` at
    /// `quality`.
    pub fn request_video(&self, ssrc: u32, quality: u32) -> Result<(), VoiceError> {
        self.conn()?.request_video(ssrc, quality)
    }

    /// `send_video_pli`: ask the sender of `ssrc` for a keyframe.
    pub fn send_pli(&self, ssrc: u32) -> Result<(), VoiceError> {
        self.conn()?.send_pli(ssrc)
    }

    /// Sends an RTCP packet built by the caller (`header` in the clear, `body` encrypted), such as
    /// TWCC feedback.
    pub fn send_rtcp(&self, header: &[u8], body: &[u8]) -> Result<(), VoiceError> {
        self.conn()?.send_rtcp(header, body)
    }

    /// op 14 SESSION_UPDATE.
    pub fn session_update(&self, codecs: &[CodecSpec]) -> Result<(), VoiceError> {
        self.conn()?.session_update(codecs)
    }

    /// The live receive pipeline, for [`Receiver::forget_other_payload_types`] and `stats`.
    pub fn receiver(&self) -> Result<Arc<Receiver>, VoiceError> {
        Ok(Arc::clone(self.conn()?.receiver()))
    }

    /// The live media connection.
    pub fn connection(&self) -> Result<Arc<RtcConnection>, VoiceError> {
        self.conn()
    }

    /// Reconnects the RTC connection to the stream server in place: same stream (no STREAM_WATCH),
    /// DAVE state, listener and session kept. Returns once the new connection is installed or the
    /// stream was torn down (a [`ClientEvent::Disconnected`]).
    pub async fn reconnect(&self) {
        Arc::clone(&self.inner).migrate(1).await;
    }

    /// Repairs the DAVE session of the live connection and sends the resulting ops (31, 26).
    /// Returns the number of messages sent.
    pub fn recover_dave(&self) -> Result<usize, VoiceError> {
        let conn = self.conn()?;
        let outs = conn
            .dave()
            .recover()
            .map_err(|e| VoiceError::Invalid(e.to_string()))?;
        conn.ws().send_dave(&outs)?;
        Ok(outs.len())
    }

    /// Stops watching (or ends our own stream): the sender stops, STREAM_DELETE (op 19) goes out,
    /// the connection is closed and the client unregistered.
    pub async fn disconnect(&self) {
        self.inner.teardown(true).await;
    }

    /// [`disconnect`](Self::disconnect), then waits up to `wait` for the STREAM_DELETE echo of
    /// this key, so a late echo cannot be read as the rejection of a watch started right after.
    /// No echo within `wait` is not an error.
    pub async fn disconnect_awaiting_echo(&self, wait: Duration) {
        let vc = self.inner.vc.upgrade();
        if let Some(vc) = &vc {
            vc.state.lock().stream_deleted.remove(&self.inner.key);
        }
        self.inner.teardown(true).await;
        if let Some(vc) = vc {
            let key = self.inner.key.clone();
            let _ = vc
                .wait_for(
                    Instant::now() + wait,
                    "waiting for the stream delete echo",
                    |st| st.stream_deleted.contains_key(&key).then_some(()),
                )
                .await;
        }
    }

    /// We created this stream and may send media on it.
    pub fn is_owner(&self) -> bool {
        self.inner.own
    }

    /// The video codec the voice server negotiated (`negotiated_video_codec`), as updated by op 14.
    pub fn negotiated_video_codec(&self) -> Result<Option<VoiceCodec>, VoiceError> {
        Ok(self
            .conn()?
            .session_info()
            .video_codec
            .and_then(|name| name.to_ascii_uppercase().parse().ok()))
    }

    /// Called on the UDP dispatcher thread when a viewer asks for a keyframe of the stream we send
    /// (PLI or FIR). The owner then asks its source for a keyframe; the sender never replays one.
    pub fn on_keyframe_request(&self, handler: impl Fn() + Send + Sync + 'static) {
        let handler: PliHandler = Arc::new(handler);
        let mut st = self.inner.state.lock();
        if let Some(sender) = &st.sender {
            sender.set_pli_handler(Some(Arc::clone(&handler)));
        }
        st.pli = Some(handler);
    }

    /// Starts sending `source` on the codec negotiated now (`StreamClient.play`): the thread pulls
    /// video and Opus from it, paced, until [`StreamClient::stop_playing`] or disconnect.
    pub fn play(
        &self,
        source: impl FrameSource + 'static,
        params: VideoParams,
    ) -> Result<(), VoiceError> {
        let sender = self.sender()?;
        sender.start(Box::new(source), params)
    }

    /// `swap_output_source`: replaces the running source in place, without reconnecting, with the one
    /// `make_source` builds for the codec negotiated at this moment, which is returned (`None` when
    /// no codec is negotiated). A sender that has stopped is restarted with the same video
    /// parameters. No frame of the old source goes out on the new codec.
    pub fn swap_output_source(
        &self,
        make_source: impl FnOnce(VoiceCodec) -> Box<dyn FrameSource>,
    ) -> Result<Option<VoiceCodec>, VoiceError> {
        let Some(codec) = self.negotiated_video_codec()? else {
            return Ok(None);
        };
        let sender = self.sender()?;
        let source = make_source(codec);
        if sender.is_running() {
            sender.swap(source)?;
        } else {
            let params = sender
                .video_params()
                .ok_or_else(|| VoiceError::Invalid("no media was played yet".into()))?;
            sender.start(source, params)?;
        }
        Ok(Some(codec))
    }

    /// Stops the send thread (Opus silence, then an empty VIDEO state) and waits for it; the stream
    /// itself stays. Does nothing when nothing plays.
    pub async fn stop_playing(&self) {
        let sender = self.inner.state.lock().sender.clone();
        if let Some(sender) = sender {
            let _ = tokio::task::spawn_blocking(move || sender.stop()).await;
        }
    }

    pub fn is_playing(&self) -> bool {
        self.inner
            .state
            .lock()
            .sender
            .as_ref()
            .is_some_and(|s| s.is_running())
    }

    /// Counters of what the send pipeline did since the first [`StreamClient::play`].
    pub fn send_stats(&self) -> SendStats {
        self.inner
            .state
            .lock()
            .sender
            .as_ref()
            .map_or_else(SendStats::default, |s| s.stats())
    }

    /// The sender of this stream, created on first use.
    fn sender(&self) -> Result<Arc<Sender>, VoiceError> {
        if !self.inner.own {
            return Err(VoiceError::Invalid(
                "cannot send media to a stream you do not own".into(),
            ));
        }
        let conn = self.conn()?;
        let mut st = self.inner.state.lock();
        if let Some(sender) = &st.sender {
            return Ok(Arc::clone(sender));
        }
        let settings = &self.inner.settings;
        let sender = Sender::new(
            conn,
            settings.streams.clone(),
            settings.receive.codecs.clone(),
            settings.receive.rtx_enabled,
            settings.send,
            st.pli.clone(),
        );
        st.sender = Some(Arc::clone(&sender));
        Ok(sender)
    }

    pub(crate) fn on_stream_create(&self, rtc_server_id: u64, rtc_channel_id: u64) {
        let mut st = self.inner.state.lock();
        st.rtc_server_id = rtc_server_id;
        st.rtc_channel_id = rtc_channel_id;
    }

    pub(crate) fn on_stream_server_update(&self) {
        self.inner.on_server_update();
    }

    /// STREAM_DELETE from the gateway: tear down without sending anything back, then report.
    pub(crate) async fn end(&self, reason: String, unavailable: bool) {
        self.inner.teardown(false).await;
        if let Some(vc) = self.inner.vc.upgrade() {
            vc.emit(ClientEvent::StreamEnded {
                stream_key: self.inner.key.clone(),
                reason,
                unavailable,
            });
        }
    }
}
