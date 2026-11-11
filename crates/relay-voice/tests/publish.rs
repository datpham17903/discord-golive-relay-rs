//! Publish a Go Live end to end on localhost: a fake gateway link, a fake voice server and a fake
//! stream server (tokio-tungstenite websocket + a UDP socket that answers IP discovery and records
//! every datagram with its arrival time). A "viewer" decrypts what the client sends: the transport
//! cipher always, and, in the DAVE test, a second `davey` session that joined the MLS group. Never
//! touches Discord.

// The handshake callback's error type is fixed by tokio-tungstenite.
#![allow(clippy::result_large_err)]

mod common;

use std::collections::VecDeque;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::mls::{FakeExternalSender, split_commit_welcome};
use common::{annex_b_nalus, codec_fixture};
use futures_util::{SinkExt, StreamExt};
use relay_media::FrameSource;
use relay_media::OutFrame;
use relay_media::codec::VoiceCodec;
use relay_media::opus::OPUS_SILENCE;
use relay_voice::dave::{Dave, DaveMedia, DaveOut};
use relay_voice::publish::VideoParams;
use relay_voice::rtcp;
use relay_voice::transport::TransportEncryptionMode;
use relay_voice::transport::packets::{DecryptedRtpPacket, SharedTransportCrypto};
use relay_voice::video::depacketizer_for;
use relay_voice::voice_ws::{StreamSpec, default_codecs, op, video_codec_list};
use relay_voice::{
    ClientEvent, JoinOptions, StreamClient, VoiceClient, VoiceClientConfig,
    connection::RtcSettings,
    link::{
        GatewayLink, LinkError, LinkEvent, LinkFuture, Secret, StreamCreateRequest,
        VoiceStateRequest,
    },
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const WAIT: Duration = Duration::from_secs(10);
const GUILD: u64 = 111;
const CHANNEL: u64 = 222;
const ME: u64 = 333;
const VIEWER: u64 = 444;
const RTC_SERVER: u64 = 9001;
const RTC_CHANNEL: u64 = 9002;
const OWN_AUDIO_SSRC: u32 = 4242;
const VIDEO_SSRC: u32 = 4243;
const RTX_SSRC: u32 = 4244;
const VIEWER_SSRC: u32 = 9999;
const FPS: u32 = 30;
const FRAME_MS: f64 = 1000.0 / 30.0;
const OPUS_FRAME: [u8; 13] = [0x78, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

fn own_key() -> String {
    format!("guild:{GUILD}:{CHANNEL}:{ME}")
}

fn secret_key() -> Vec<u8> {
    (0u8..32).collect()
}

fn crypto() -> SharedTransportCrypto {
    SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &secret_key())
        .unwrap()
}

// ---------------------------------------------------------------------------------------------
// fake RTC server: websocket + UDP

/// The MLS side of a fake stream server: the gateway's external sender and a second member
/// (the viewer) that the client's group adds.
struct DaveFake {
    sender: FakeExternalSender,
    viewer: Arc<Dave>,
    key_package: Vec<u8>,
}

impl DaveFake {
    fn new() -> Self {
        let group = RTC_SERVER - 1;
        let viewer = Arc::new(Dave::new(VIEWER, group));
        let outs = viewer.on_session_description(1).unwrap();
        let [DaveOut::KeyPackage(key_package)] = outs.as_slice() else {
            panic!("expected a key package, got {outs:?}");
        };
        let key_package = key_package.clone();
        let sender = FakeExternalSender::new(group);
        viewer
            .on_binary(op::MLS_EXTERNAL_SENDER, &sender.marshalled())
            .unwrap();
        Self {
            sender,
            viewer,
            key_package,
        }
    }
}

struct RtcCfg {
    own_ssrc: u32,
    /// The `streams` of READY.
    streams: Vec<Value>,
    video_codec: &'static str,
    dave: Option<DaveFake>,
}

impl RtcCfg {
    fn voice() -> Self {
        Self {
            own_ssrc: 7001,
            streams: Vec::new(),
            video_codec: "H264",
            dave: None,
        }
    }

    fn stream(video_codec: &'static str, dave: Option<DaveFake>) -> Self {
        Self {
            own_ssrc: OWN_AUDIO_SSRC,
            streams: vec![json!({
                "type": "screen", "rid": "100", "quality": 100, "active": false,
                "ssrc": VIDEO_SSRC, "rtx_ssrc": RTX_SSRC,
            })],
            video_codec,
            dave,
        }
    }
}

/// What a test can ask the websocket half of a fake server to do.
enum Ctl {
    Send(Value),
}

struct FakeRtc {
    endpoint: String,
    ctl: mpsc::UnboundedSender<Ctl>,
    /// Every JSON message the client sent (heartbeats excluded).
    received: Arc<Mutex<Vec<Value>>>,
    udp: Arc<UdpSide>,
}

struct UdpSide {
    /// Datagrams the client sent that are not IP discovery, with their arrival time.
    received: Mutex<Vec<(Instant, Vec<u8>)>>,
    peer: Mutex<Option<SocketAddr>>,
    outgoing: Mutex<std::sync::mpsc::Sender<Vec<u8>>>,
    stop: AtomicBool,
}

impl UdpSide {
    fn send(&self, packets: &[Vec<u8>]) {
        for packet in packets {
            self.outgoing.lock().unwrap().send(packet.clone()).unwrap();
        }
    }

    fn take(&self) -> Vec<(Instant, Vec<u8>)> {
        std::mem::take(&mut *self.received.lock().unwrap())
    }
}

impl Drop for UdpSide {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// `send_to` on the non-blocking fake socket, waiting out a momentarily full send buffer.
fn send_to(socket: &UdpSocket, packet: &[u8], to: SocketAddr) {
    loop {
        match socket.send_to(packet, to) {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            other => {
                other.unwrap();
                return;
            }
        }
    }
}

fn start_udp() -> (SocketAddr, Arc<UdpSide>) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    // Polling a non-blocking socket: on Windows a receive that times out can swallow a datagram
    // that arrives at that moment (SO_RCVTIMEO leaves the socket state indeterminate).
    socket.set_nonblocking(true).unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let side = Arc::new(UdpSide {
        received: Mutex::new(Vec::new()),
        peer: Mutex::new(None),
        outgoing: Mutex::new(tx),
        stop: AtomicBool::new(false),
    });
    let weak = Arc::downgrade(&side);
    std::thread::spawn(move || {
        let mut buf = [0u8; 2048];
        loop {
            let Some(side) = weak.upgrade() else { return };
            if side.stop.load(Ordering::Relaxed) {
                return;
            }
            // Drain what is queued, so a burst of media is stamped with (almost) its arrival time.
            let mut idle = true;
            while let Ok((n, from)) = socket.recv_from(&mut buf) {
                idle = false;
                let now = Instant::now();
                let data = &buf[..n];
                if n == 74 && data[..2] == [0, 1] {
                    // IP discovery: answer with the address we saw
                    let mut reply = [0u8; 74];
                    reply[..2].copy_from_slice(&2u16.to_be_bytes());
                    reply[2..4].copy_from_slice(&70u16.to_be_bytes());
                    reply[4..8].copy_from_slice(&data[4..8]);
                    reply[8..17].copy_from_slice(b"127.0.0.1");
                    reply[72..74].copy_from_slice(&from.port().to_be_bytes());
                    send_to(&socket, &reply, from);
                    *side.peer.lock().unwrap() = Some(from);
                } else {
                    side.received.lock().unwrap().push((now, data.to_vec()));
                }
            }
            if let Some(peer) = *side.peer.lock().unwrap() {
                while let Ok(packet) = rx.try_recv() {
                    send_to(&socket, &packet, peer);
                }
            }
            if idle {
                std::thread::sleep(Duration::from_micros(500));
            }
        }
    });
    (addr, side)
}

/// Frame of a server-to-client binary message: `seq`, opcode, payload.
fn binary_to_client(seq: u16, opcode: u8, payload: &[u8]) -> Message {
    let mut frame = seq.to_be_bytes().to_vec();
    frame.push(opcode);
    frame.extend_from_slice(payload);
    Message::binary(frame)
}

async fn start_rtc(cfg: RtcCfg) -> FakeRtc {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = listener.local_addr().unwrap();
    let (udp_addr, udp) = start_udp();
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();
    let received = Arc::new(Mutex::new(Vec::new()));
    let received2 = Arc::clone(&received);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(stream, |_: &Request, resp: Response| Ok(resp))
                .await
                .unwrap();
        ws.send(Message::text(
            json!({"op": 8, "d": {"heartbeat_interval": 5000}}).to_string(),
        ))
        .await
        .unwrap();
        let mut seq = 0u16;
        loop {
            tokio::select! {
                msg = ws.next() => {
                    let Some(Ok(msg)) = msg else { return };
                    match msg {
                        Message::Binary(data) if data.first() == Some(&op::MLS_COMMIT_WELCOME) => {
                            let Some(dave) = &cfg.dave else { continue };
                            // the client committed the viewer's add: announce the commit (transition
                            // 0 applies at once) and hand the welcome to the viewer
                            let (commit, welcome) = split_commit_welcome(&data[1..]);
                            let mut announce = 0u16.to_be_bytes().to_vec();
                            announce.extend_from_slice(&commit);
                            seq += 1;
                            ws.send(binary_to_client(seq, op::MLS_ANNOUNCE_COMMIT_TRANSITION, &announce))
                                .await
                                .unwrap();
                            let mut welcome_payload = 0u16.to_be_bytes().to_vec();
                            welcome_payload.extend_from_slice(&welcome);
                            dave.viewer.on_binary(op::MLS_WELCOME, &welcome_payload).unwrap();
                        }
                        Message::Text(text) => {
                            let v: Value = serde_json::from_str(text.as_str()).unwrap();
                            match v["op"].as_u64().unwrap() {
                                3 => {
                                    ws.send(Message::text(json!({"op": 6, "d": {"t": v["d"]["t"]}}).to_string())).await.unwrap();
                                    continue;
                                }
                                0 => {
                                    ws.send(Message::text(json!({"op": 2, "d": {
                                        "ssrc": cfg.own_ssrc, "ip": "127.0.0.1", "port": udp_addr.port(),
                                        "modes": ["aead_xchacha20_poly1305_rtpsize", "aead_aes256_gcm_rtpsize"],
                                        "streams": cfg.streams, "experiments": ["fixed_keyframe_interval"],
                                    }}).to_string())).await.unwrap();
                                }
                                1 => {
                                    ws.send(Message::text(json!({"op": 4, "d": {
                                        "mode": "aead_aes256_gcm_rtpsize", "secret_key": secret_key(),
                                        "audio_codec": "opus", "video_codec": cfg.video_codec,
                                        "media_session_id": "media-1", "keyframe_interval": 2000,
                                        "dave_protocol_version": u8::from(cfg.dave.is_some()),
                                    }}).to_string())).await.unwrap();
                                    if let Some(dave) = &cfg.dave {
                                        // external sender, then the proposal that adds the viewer
                                        seq += 1;
                                        ws.send(binary_to_client(seq, op::MLS_EXTERNAL_SENDER, &dave.sender.marshalled()))
                                            .await
                                            .unwrap();
                                        let mut proposals = vec![0u8]; // append
                                        proposals.extend_from_slice(&dave.sender.propose_add(0, &dave.key_package));
                                        seq += 1;
                                        ws.send(binary_to_client(seq, op::MLS_PROPOSALS, &proposals))
                                            .await
                                            .unwrap();
                                    }
                                }
                                _ => {}
                            }
                            received2.lock().unwrap().push(v);
                        }
                        _ => {}
                    }
                }
                ctl = ctl_rx.recv() => match ctl {
                    Some(Ctl::Send(v)) => ws.send(Message::text(v.to_string())).await.unwrap(),
                    None => return,
                },
            }
        }
    });
    FakeRtc {
        endpoint: ws_addr.to_string(),
        ctl: ctl_tx,
        received,
        udp,
    }
}

impl FakeRtc {
    fn messages(&self, op: u64) -> Vec<Value> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["op"] == op)
            .cloned()
            .collect()
    }

    async fn wait_message(&self, op: u64) -> Value {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(v) = self.messages(op).into_iter().next() {
                return v;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no op {op} from the client"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Everything the client sent over UDP so far, decrypted: RTP packets (with arrival time) and RTCP.
#[derive(Default)]
struct Wire {
    rtp: Vec<(Instant, DecryptedRtpPacket)>,
    rtcp: Vec<(u8, u8, u32, Vec<u8>)>,
}

/// Accumulates the datagrams of `udp` into one [`Wire`] (calls drain the socket's queue).
struct Tap {
    udp: Arc<UdpSide>,
    reader: SharedTransportCrypto,
    wire: Wire,
}

impl Tap {
    fn new(rtc: &FakeRtc) -> Self {
        Self {
            udp: Arc::clone(&rtc.udp),
            reader: crypto(),
            wire: Wire::default(),
        }
    }

    fn pump(&mut self) {
        for (at, datagram) in self.udp.take() {
            if datagram.len() <= 12 {
                continue; // the UDP keepalive
            }
            if datagram.len() > 12 && (200..=206).contains(&datagram[1]) {
                for p in self.reader.decrypt_rtcp_packets(&datagram, 8).unwrap() {
                    self.wire
                        .rtcp
                        .push((p.packet_type, p.count, p.sender_ssrc, p.payload));
                }
            } else {
                let (failed, mut packets) = self.reader.decrypt_rtp_packets(&[datagram]).unwrap();
                assert_eq!(failed, 0, "an RTP datagram failed to decrypt");
                self.wire.rtp.extend(packets.drain(..).map(|p| (at, p)));
            }
        }
    }

    /// Waits until `done(&wire)` holds.
    async fn wait(&mut self, what: &str, done: impl Fn(&Wire) -> bool) -> &Wire {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            self.pump();
            if done(&self.wire) {
                return &self.wire;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn video_packets(wire: &Wire, ssrc: u32) -> Vec<&DecryptedRtpPacket> {
    wire.rtp
        .iter()
        .map(|(_, p)| p)
        .filter(|p| p.packet.ssrc == ssrc)
        .collect()
}

fn audio_packets(wire: &Wire) -> Vec<&(Instant, DecryptedRtpPacket)> {
    wire.rtp
        .iter()
        .filter(|(_, p)| p.packet.ssrc == OWN_AUDIO_SSRC)
        .collect()
}

/// Splits the packets of one SSRC into frames at each marker bit.
fn frames_of<'a>(packets: &[&'a DecryptedRtpPacket]) -> Vec<Vec<&'a DecryptedRtpPacket>> {
    let mut frames = vec![Vec::new()];
    for packet in packets {
        frames.last_mut().unwrap().push(*packet);
        if packet.packet.marker {
            frames.push(Vec::new());
        }
    }
    if frames.last().is_some_and(Vec::is_empty) {
        frames.pop();
    }
    frames
}

fn depacketize(codec: VoiceCodec, frame: &[&DecryptedRtpPacket]) -> Vec<u8> {
    let mut depacketizer = depacketizer_for(codec);
    let mut out = None;
    for packet in frame {
        if let Some(au) = depacketizer
            .push_packet(
                &packet.media_payload,
                packet.packet.marker,
                packet.packet.sequence,
                packet.packet.timestamp,
            )
            .unwrap()
        {
            out = Some(au);
        }
    }
    out.expect("the frame ends in an access unit")
}

// ---------------------------------------------------------------------------------------------
// fake gateway link

#[derive(Debug, PartialEq)]
enum Call {
    VoiceState(VoiceStateRequest),
    StreamCreate(StreamCreateRequest),
    StreamDelete(String),
    SetPaused(String, bool),
}

struct FakeGateway {
    calls: mpsc::UnboundedSender<Call>,
}

impl FakeGateway {
    fn record(&self, call: Call) -> LinkFuture<'_> {
        Box::pin(async move { self.calls.send(call).map_err(|e| LinkError(e.to_string())) })
    }
}

impl GatewayLink for FakeGateway {
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_> {
        self.record(Call::VoiceState(request))
    }

    fn stream_create(&self, request: StreamCreateRequest) -> LinkFuture<'_> {
        self.record(Call::StreamCreate(request))
    }

    fn stream_watch(&self, _stream_key: &str) -> LinkFuture<'_> {
        Box::pin(async { Err(LinkError("a publisher never watches".into())) })
    }

    fn stream_delete(&self, stream_key: &str) -> LinkFuture<'_> {
        self.record(Call::StreamDelete(stream_key.to_owned()))
    }

    fn stream_set_paused(&self, stream_key: &str, paused: bool) -> LinkFuture<'_> {
        self.record(Call::SetPaused(stream_key.to_owned(), paused))
    }
}

/// Plays the main gateway: answers what the client sends with the dispatches the real one would.
fn spawn_gateway(
    client: VoiceClient,
    mut calls: mpsc::UnboundedReceiver<Call>,
    voice_endpoint: String,
    stream_endpoint: String,
    seen: Arc<Mutex<Vec<String>>>,
) {
    tokio::spawn(async move {
        while let Some(call) = calls.recv().await {
            let label = match &call {
                Call::VoiceState(r) => format!("voice_state:{:?}", r.channel_id),
                Call::StreamCreate(r) => format!("stream_create:{:?}:{}", r.guild_id, r.channel_id),
                Call::StreamDelete(k) => format!("stream_delete:{k}"),
                Call::SetPaused(k, p) => format!("stream_set_paused:{k}:{p}"),
            };
            seen.lock().unwrap().push(label);
            match call {
                Call::VoiceState(r) if r.channel_id.is_some() => {
                    client.handle_event(LinkEvent::VoiceStateUpdate {
                        channel_id: r.channel_id,
                        session_id: Some("sess-1".into()),
                    });
                    client.handle_event(LinkEvent::VoiceServerUpdate {
                        token: Some(Secret::new("FAKE-voice-token")),
                        endpoint: Some(voice_endpoint.clone()),
                        guild_id: Some(GUILD),
                        channel_id: None,
                    });
                }
                Call::VoiceState(_) => client.handle_event(LinkEvent::VoiceStateUpdate {
                    channel_id: None,
                    session_id: None,
                }),
                Call::StreamCreate(_) => {
                    client.handle_event(LinkEvent::StreamServerUpdate {
                        stream_key: own_key(),
                        token: Some(Secret::new("FAKE-stream-token")),
                        endpoint: Some(format!("wss://{stream_endpoint}")),
                    });
                    client.handle_event(LinkEvent::StreamCreate {
                        stream_key: own_key(),
                        rtc_server_id: RTC_SERVER,
                        rtc_channel_id: RTC_CHANNEL,
                    });
                }
                Call::StreamDelete(_) | Call::SetPaused(..) => {}
            }
        }
    });
}

// ---------------------------------------------------------------------------------------------
// the rig and a frame source

struct Rig {
    client: VoiceClient,
    events: mpsc::UnboundedReceiver<ClientEvent>,
    seen: Arc<Mutex<Vec<String>>>,
    stream_rtc: FakeRtc,
    voice_rtc: FakeRtc,
}

impl Rig {
    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    async fn wait_seen(&self, label: &str) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while !self.seen().iter().any(|s| s == label) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "gateway never saw {label}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn publish_settings() -> RtcSettings {
    let codecs = video_codec_list(
        &["AV1", "H265", "H264", "VP8", "VP9"].map(|c| c.parse::<VoiceCodec>().unwrap()),
        true,
    );
    let mut s = RtcSettings::publish(codecs, true, FPS);
    s.url_scheme = "ws".into();
    s
}

async fn rig(stream_cfg: RtcCfg) -> Rig {
    let voice_rtc = start_rtc(RtcCfg::voice()).await;
    let stream_rtc = start_rtc(stream_cfg).await;
    let (calls_tx, calls_rx) = mpsc::unbounded_channel();
    let mut rtc = RtcSettings::new(default_codecs(), vec![StreamSpec::video(100)], true);
    rtc.url_scheme = "ws".into();
    let (client, events) = VoiceClient::new(VoiceClientConfig {
        user_id: ME,
        guild_id: Some(GUILD),
        channel_id: CHANNEL,
        gateway: Arc::new(FakeGateway { calls: calls_tx }),
        http: wreq::Client::builder().build().unwrap(),
        rtc,
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    spawn_gateway(
        client.clone(),
        calls_rx,
        voice_rtc.endpoint.clone(),
        stream_rtc.endpoint.clone(),
        Arc::clone(&seen),
    );
    client
        .connect(JoinOptions {
            timeout: WAIT,
            self_mute: true,
            ..JoinOptions::default()
        })
        .await
        .expect("join");
    Rig {
        client,
        events,
        seen,
        stream_rtc,
        voice_rtc,
    }
}

/// Plays `first` once, then `then` forever; every Opus tick is `OPUS_FRAME`.
struct TestSource {
    first: VecDeque<Vec<u8>>,
    then: Option<Vec<u8>>,
    audio_ticks: Arc<AtomicUsize>,
}

impl TestSource {
    fn new(first: Vec<u8>, then: Vec<u8>) -> Self {
        Self {
            first: VecDeque::from([first]),
            then: Some(then),
            audio_ticks: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl FrameSource for TestSource {
    fn next_video(&mut self) -> Option<OutFrame> {
        let data = self.first.pop_front().or_else(|| self.then.clone())?;
        Some(OutFrame {
            data,
            frame_time_ms: FRAME_MS,
        })
    }

    fn next_audio(&mut self) -> Vec<u8> {
        self.audio_ticks.fetch_add(1, Ordering::Relaxed);
        OPUS_FRAME.to_vec()
    }
}

fn h264_source() -> TestSource {
    TestSource::new(
        codec_fixture("h264", "1920x1080", "key"),
        codec_fixture("h264", "1920x1080", "delta"),
    )
}

/// The same NAL units behind 4-byte start codes, the way the depacketizer hands access units to the
/// worker (davey 0.2 refuses H.264 frames with 3-byte start codes).
fn four_byte(au: &[u8]) -> Vec<u8> {
    annex_b_nalus(au)
        .into_iter()
        .flat_map(|nal| [&[0u8, 0, 0, 1][..], nal].concat())
        .collect()
}

fn params() -> VideoParams {
    VideoParams {
        width: 1920,
        height: 1080,
        fps: FPS,
        bitrate: 0,
    }
}

async fn create(rig: &Rig) -> StreamClient {
    rig.client
        .create_stream(publish_settings(), WAIT)
        .await
        .expect("create_stream")
}

// ---------------------------------------------------------------------------------------------
// tests

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_stream_announces_the_stream_and_plays() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;

    // op 18 for our channel, the handshake against the stream server, then op 22 paused=false
    assert!(
        rig.seen()
            .contains(&format!("stream_create:Some({GUILD}):{CHANNEL}"))
    );
    rig.wait_seen(&format!("stream_set_paused:{}:false", own_key()))
        .await;
    assert_eq!(stream.key(), own_key());
    assert!(stream.is_owner());
    let identify = rig.stream_rtc.wait_message(0).await;
    assert_eq!(identify["d"]["server_id"], RTC_SERVER.to_string());
    assert_eq!(identify["d"]["session_id"], "sess-1");
    assert_eq!(identify["d"]["streams"][0]["type"], "screen");
    assert_eq!(identify["d"]["streams"][0]["max_framerate"], FPS);
    assert_eq!(
        stream.negotiated_video_codec().unwrap(),
        Some(VoiceCodec::H264)
    );

    let mut tap = Tap::new(&rig.stream_rtc);
    stream.play(h264_source(), params()).unwrap();
    assert!(stream.is_playing());
    assert!(
        stream.play(h264_source(), params()).is_err(),
        "a second play while one runs is refused"
    );

    // op 12: our streams with the SSRCs the server assigned, bitrate and resolution of the source
    let deadline = tokio::time::Instant::now() + WAIT;
    let video = loop {
        // the join sent an empty VIDEO state first; ours carries the SSRCs
        let found = rig
            .stream_rtc
            .messages(12)
            .into_iter()
            .find(|m| m["d"]["video_ssrc"] != 0);
        if let Some(found) = found {
            break found;
        }
        assert!(tokio::time::Instant::now() < deadline, "no VIDEO state");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(video["d"]["audio_ssrc"], OWN_AUDIO_SSRC);
    assert_eq!(video["d"]["video_ssrc"], VIDEO_SSRC);
    assert_eq!(video["d"]["rtx_ssrc"], RTX_SSRC);
    let announced = &video["d"]["streams"][0];
    assert_eq!(announced["ssrc"], VIDEO_SSRC);
    assert_eq!(announced["rtx_ssrc"], RTX_SSRC);
    assert_eq!(announced["active"], true);
    assert_eq!(announced["max_bitrate"], 8_000_000);
    assert_eq!(announced["max_framerate"], FPS);
    assert_eq!(
        announced["max_resolution"],
        json!({"type": "fixed", "width": 1920, "height": 1080})
    );
    // SPEAKING with the soundshare flag
    let deadline = tokio::time::Instant::now() + WAIT;
    while !rig
        .stream_rtc
        .messages(5)
        .iter()
        .any(|m| m["d"]["speaking"] == 2 && m["d"]["ssrc"] == OWN_AUDIO_SSRC)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no SPEAKING(soundshare)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // a sender report goes out with the first packets
    tap.wait("an RTCP sender report", |w| {
        w.rtcp
            .iter()
            .any(|(t, _, ssrc, _)| *t == rtcp::RTCP_SENDER_REPORT && *ssrc == VIDEO_SSRC)
    })
    .await;

    // a stream we only watch cannot send, and a second stream of ours is not created
    assert_eq!(rig.stream_rtc.messages(0).len(), 1);

    stream.disconnect().await;
    rig.wait_seen(&format!("stream_delete:{}", own_key())).await;
    assert!(!stream.is_playing());
    assert!(!stream.is_connected());
    let _ = (&rig.voice_rtc, &rig.events);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_h264_idr_au_becomes_valid_rtp_passthrough() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    let mut tap = Tap::new(&rig.stream_rtc);
    stream.play(h264_source(), params()).unwrap();

    let wire = tap
        .wait("three video frames", |w| {
            frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 4
        })
        .await;
    let packets = video_packets(wire, VIDEO_SSRC);
    let frames = frames_of(&packets);
    // the last group may be incomplete (no marker yet); judge the complete ones
    let complete: Vec<_> = frames
        .iter()
        .filter(|f| f.last().unwrap().packet.marker)
        .collect();
    assert!(complete.len() >= 3);
    let key = codec_fixture("h264", "1920x1080", "key");
    let delta = codec_fixture("h264", "1920x1080", "delta");
    let mut previous: Option<(u16, u32)> = None;
    for (n, frame) in complete.iter().enumerate() {
        assert!(
            frame.len() > 1 || n > 0,
            "the IDR frame needs several packets"
        );
        let ts = frame[0].packet.timestamp;
        for (i, p) in frame.iter().enumerate() {
            assert_eq!(p.packet.payload_type, VoiceCodec::H264.payload_type());
            assert_eq!(p.packet.timestamp, ts, "one timestamp per frame");
            assert_eq!(
                p.packet.marker,
                i + 1 == frame.len(),
                "marker on the last packet"
            );
            if let Some((seq, _)) = previous {
                assert_eq!(
                    p.packet.sequence,
                    seq.wrapping_add(1),
                    "consecutive sequence"
                );
            }
            previous = Some((p.packet.sequence, ts));
        }
        if n > 0 {
            let prev_ts = complete[n - 1][0].packet.timestamp;
            let step = ts.wrapping_sub(prev_ts);
            assert!(
                (2999..=3001).contains(&step),
                "30 fps on a 90 kHz clock steps 3000, got {step}"
            );
        }
        let au = depacketize(VoiceCodec::H264, frame);
        let expected = if n == 0 { &key } else { &delta };
        assert_eq!(annex_b_nalus(&au), annex_b_nalus(expected));
    }
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dave_encrypted_frames_decrypt_for_the_viewer() {
    let dave = DaveFake::new();
    let viewer = Arc::clone(&dave.viewer);
    let rig = rig(RtcCfg::stream("H264", Some(dave))).await;
    let stream = create(&rig).await;
    let connection = stream.connection().unwrap();
    // the commit that adds the viewer makes the group usable
    let deadline = tokio::time::Instant::now() + WAIT;
    while !(connection.dave().can_encrypt() && viewer.can_decrypt(ME)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "DAVE never became active"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(connection.dave().protocol_version(), 1);

    let mut tap = Tap::new(&rig.stream_rtc);
    let source = TestSource::new(
        four_byte(&codec_fixture("h264", "1920x1080", "key")),
        four_byte(&codec_fixture("h264", "1920x1080", "delta")),
    );
    stream.play(source, params()).unwrap();
    let wire = tap
        .wait("encrypted frames and Opus", |w| {
            frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 3 && audio_packets(w).len() >= 3
        })
        .await;

    // video: reassemble, then the viewer decrypts back to the original access unit
    let packets = video_packets(wire, VIDEO_SSRC);
    let frames = frames_of(&packets);
    let key = codec_fixture("h264", "1920x1080", "key");
    let delta = codec_fixture("h264", "1920x1080", "delta");
    for (n, frame) in frames
        .iter()
        .filter(|f| f.last().unwrap().packet.marker)
        .take(3)
        .enumerate()
    {
        let sealed = depacketize(VoiceCodec::H264, frame);
        let expected = if n == 0 { &key } else { &delta };
        assert_ne!(annex_b_nalus(&sealed), annex_b_nalus(expected));
        let opened = viewer.decrypt(ME, DaveMedia::Video, &sealed).unwrap();
        assert_eq!(annex_b_nalus(&opened), annex_b_nalus(expected), "frame {n}");
    }
    // audio: the Opus payload after the RTP extension is a DAVE frame
    for (_, packet) in audio_packets(wire).into_iter().take(3) {
        assert_eq!(packet.packet.payload_type, 120);
        assert_ne!(packet.media_payload, OPUS_FRAME);
        let opened = viewer
            .decrypt(ME, DaveMedia::Audio, &packet.media_payload)
            .unwrap();
        assert_eq!(opened, OPUS_FRAME);
    }
    assert_eq!(viewer.decrypt_failures(), 0);
    assert_eq!(stream.send_stats().dave_encrypt_errors, 0);
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_codec_change_via_session_update_is_reflected_in_the_payloader() {
    let mut rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    let mut tap = Tap::new(&rig.stream_rtc);
    stream.play(h264_source(), params()).unwrap();
    tap.wait("H264 frames", |w| {
        frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 2
    })
    .await;
    assert_eq!(rig.stream_rtc.messages(0).len(), 1);
    let announcements = rig.stream_rtc.messages(12).len();

    // the server renegotiates: op 14 with the new video codec
    rig.stream_rtc
        .ctl
        .send(Ctl::Send(json!({"op": 14, "d": {
            "audio_codec": "opus", "video_codec": "H265", "media_session_id": "media-1",
        }})))
        .unwrap();
    let event = loop {
        match timeout(WAIT, rig.events.recv()).await.unwrap().unwrap() {
            ClientEvent::SessionUpdate { stream_key, info } if stream_key.is_some() => break info,
            _ => {}
        }
    };
    assert_eq!(event.video_codec.as_deref(), Some("H265"));
    assert_eq!(
        stream.negotiated_video_codec().unwrap(),
        Some(VoiceCodec::H265)
    );

    // swap in place: the new source is built for the codec negotiated now
    let asked = Arc::new(Mutex::new(None));
    let asked2 = Arc::clone(&asked);
    let bound = stream
        .swap_output_source(move |codec| {
            *asked2.lock().unwrap() = Some(codec);
            Box::new(TestSource::new(
                codec_fixture("h265", "1920x1080", "key"),
                codec_fixture("h265", "1920x1080", "delta"),
            ))
        })
        .unwrap();
    assert_eq!(bound, Some(VoiceCodec::H265));
    assert_eq!(*asked.lock().unwrap(), Some(VoiceCodec::H265));
    assert!(stream.is_playing());

    let pt265 = VoiceCodec::H265.payload_type();
    let wire = tap
        .wait("H265 frames", |w| {
            let h265: Vec<_> = video_packets(w, VIDEO_SSRC)
                .into_iter()
                .filter(|p| p.packet.payload_type == pt265)
                .collect();
            frames_of(&h265).len() >= 3
        })
        .await;
    let packets = video_packets(wire, VIDEO_SSRC);
    let first265 = packets
        .iter()
        .position(|p| p.packet.payload_type == pt265)
        .unwrap();
    assert!(
        packets[first265..]
            .iter()
            .all(|p| p.packet.payload_type == pt265),
        "no H264 packet after the first H265 one"
    );
    assert!(
        packets[..first265]
            .iter()
            .all(|p| p.packet.payload_type == VoiceCodec::H264.payload_type())
    );
    // the first H265 frame is the IDR of the new source, and it reassembles
    let h265: Vec<_> = packets[first265..].to_vec();
    let frames = frames_of(&h265);
    let au = depacketize(VoiceCodec::H265, &frames[0]);
    assert_eq!(
        annex_b_nalus(&au),
        annex_b_nalus(&codec_fixture("h265", "1920x1080", "key"))
    );
    // the video state was announced again, on the same connection (no reconnect, no second IDENTIFY)
    assert!(rig.stream_rtc.messages(12).len() > announcements);
    assert_eq!(rig.stream_rtc.messages(0).len(), 1);
    // the swap restarts the packetizer (`start_video(reset=True)`), so the new codec has its own
    // consecutive run of sequence numbers
    let seqs: Vec<u16> = h265.iter().map(|p| p.packet.sequence).collect();
    assert!(seqs.windows(2).all(|w| w[1] == w[0].wrapping_add(1)));
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_viewer_pli_and_fir_reach_the_callback_and_other_ssrcs_do_not() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = Arc::clone(&count);
    stream.on_keyframe_request(move || {
        count2.fetch_add(1, Ordering::SeqCst);
    });
    let mut tap = Tap::new(&rig.stream_rtc);
    stream.play(h264_source(), params()).unwrap();
    tap.wait("some video", |w| !video_packets(w, VIDEO_SSRC).is_empty())
        .await;

    let send = |header_body: (Vec<u8>, Vec<u8>)| {
        let packet = crypto()
            .encrypt_rtcp(&header_body.0, &header_body.1)
            .unwrap();
        rig.stream_rtc.udp.send(&[packet]);
    };
    // someone else's SSRC: ignored
    send(rtcp::pli(VIEWER_SSRC, 12345));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);

    send(rtcp::pli(VIEWER_SSRC, VIDEO_SSRC));
    let deadline = tokio::time::Instant::now() + WAIT;
    while count.load(Ordering::SeqCst) < 1 {
        assert!(tokio::time::Instant::now() < deadline, "PLI never arrived");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(stream.send_stats().pli_received, 1);

    // FIR: payload-specific feedback format 4, media SSRC 0, the target in the FCI entry
    let (mut header, mut body) = rtcp::pli(VIEWER_SSRC, 0);
    header[0] = 0x84;
    header[3] = 4; // length in words minus one: header + sender + media + FCI(2) = 5 words
    body.extend_from_slice(&VIDEO_SSRC.to_be_bytes());
    body.extend_from_slice(&[1, 0, 0, 0]);
    send((header, body));
    while count.load(Ordering::SeqCst) < 2 {
        assert!(tokio::time::Instant::now() < deadline, "FIR never arrived");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_viewer_nack_is_answered_with_an_rtx_packet() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    let mut tap = Tap::new(&rig.stream_rtc);
    stream.play(h264_source(), params()).unwrap();
    let wire = tap
        .wait("the IDR frame", |w| {
            frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 2
        })
        .await;
    let wanted = video_packets(wire, VIDEO_SSRC)[1];
    let (wanted_seq, wanted_ts, wanted_payload) = (
        wanted.packet.sequence,
        wanted.packet.timestamp,
        wanted.media_payload.clone(),
    );
    let (header, body) = rtcp::nack(VIEWER_SSRC, VIDEO_SSRC, &[wanted_seq]).unwrap();
    rig.stream_rtc
        .udp
        .send(&[crypto().encrypt_rtcp(&header, &body).unwrap()]);

    let wire = tap
        .wait("the RTX packet", |w| !video_packets(w, RTX_SSRC).is_empty())
        .await;
    let rtx = video_packets(wire, RTX_SSRC)[0];
    assert_eq!(rtx.packet.payload_type, VoiceCodec::H264.rtx_payload_type());
    assert_eq!(rtx.packet.timestamp, wanted_ts);
    assert_eq!(rtx.media_payload[..2], wanted_seq.to_be_bytes());
    assert_eq!(rtx.media_payload[2..], wanted_payload[..]);
    assert!(stream.send_stats().rtx_packets_resent >= 1);
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_opus_goes_out_every_20_ms_and_stops_with_silence() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    let mut tap = Tap::new(&rig.stream_rtc);
    let source = h264_source();
    let ticks = Arc::clone(&source.audio_ticks);
    stream.play(source, params()).unwrap();

    let wire = tap
        .wait("26 Opus packets", |w| audio_packets(w).len() >= 26)
        .await;
    let audio = audio_packets(wire);
    let first = audio[0];
    let last = audio.last().unwrap();
    let span = last.0.duration_since(first.0).as_secs_f64() * 1000.0;
    let average = span / (audio.len() - 1) as f64;
    // Shared CI runners add scheduler jitter (macOS measured 22.08 ms once); a real pacing bug
    // shows up as ~10 ms or ~40 ms.
    assert!(
        (18.0..=24.0).contains(&average),
        "Opus every 20 ms on average, got {average:.2} ms over {} packets",
        audio.len()
    );
    for pair in audio.windows(2) {
        let (a, b) = (&pair[0].1.packet, &pair[1].1.packet);
        assert_eq!(b.sequence, a.sequence.wrapping_add(1));
        assert_eq!(b.timestamp, a.timestamp.wrapping_add(960));
        assert_eq!(a.payload_type, 120);
    }
    assert_eq!(audio[0].1.media_payload, OPUS_FRAME);
    assert!(ticks.load(Ordering::Relaxed) >= 26);
    let sent_before_stop = audio.len();

    // stopping sends trailing Opus silence, then withdraws the video
    stream.stop_playing().await;
    assert!(!stream.is_playing());
    let wire = tap
        .wait("the trailing silence", |w| {
            audio_packets(w)
                .iter()
                .filter(|(_, p)| p.media_payload == OPUS_SILENCE)
                .count()
                >= 10
        })
        .await;
    let silent = audio_packets(wire)
        .iter()
        .filter(|(_, p)| p.media_payload == OPUS_SILENCE)
        .count();
    assert_eq!(silent, 10, "MediaPlayer.SILENCE_PACKET_COUNT");
    assert!(audio_packets(wire).len() >= sent_before_stop + 10);
    let last_video_state = rig.stream_rtc.messages(12).pop().unwrap();
    assert_eq!(last_video_state["d"]["video_ssrc"], 0);
    assert_eq!(last_video_state["d"]["streams"], json!([]));
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watching_your_own_stream_is_refused() {
    let rig = rig(RtcCfg::stream("H264", None)).await;
    let own = rig
        .client
        .watch_stream(&own_key(), publish_settings(), WAIT)
        .await;
    assert!(own.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_own_stream_server_update_with_a_new_token_keeps_the_connection() {
    let mut rig = rig(RtcCfg::stream("H264", None)).await;
    let stream = create(&rig).await;
    stream.play(h264_source(), params()).unwrap();
    let mut tap = Tap::new(&rig.stream_rtc);
    tap.wait("H264 frames", |w| {
        frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 2
    })
    .await;

    // same endpoint, fresh token
    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: own_key(),
        token: Some(Secret::new("FAKE-stream-token-2")),
        endpoint: Some(format!("wss://{}", rig.stream_rtc.endpoint)),
    });
    loop {
        match timeout(WAIT, rig.events.recv()).await.unwrap().unwrap() {
            ClientEvent::StreamTokenRefreshed { stream_key } => {
                assert_eq!(stream_key, own_key());
                break;
            }
            ClientEvent::Disconnected { reason, .. } => panic!("torn down: {reason}"),
            _ => {}
        }
    }
    assert!(stream.is_connected() && stream.is_playing());
    assert!(
        !rig.seen().iter().any(|s| s.starts_with("stream_delete:")),
        "no STREAM_DELETE: {:?}",
        rig.seen()
    );
    // the stream keeps sending on the same connection
    let before = frames_of(&video_packets(tap.wait("now", |_| true).await, VIDEO_SSRC)).len();
    tap.wait("more frames", |w| {
        frames_of(&video_packets(w, VIDEO_SSRC)).len() > before + 2
    })
    .await;
    // an identical update afterwards is a no-op, not a second refresh
    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: own_key(),
        token: Some(Secret::new("FAKE-stream-token-2")),
        endpoint: Some(format!("wss://{}", rig.stream_rtc.endpoint)),
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    while let Ok(event) = rig.events.try_recv() {
        assert!(
            !matches!(
                event,
                ClientEvent::StreamTokenRefreshed { .. } | ClientEvent::Disconnected { .. }
            ),
            "{event:?}"
        );
    }
    stream.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_own_stream_moves_to_another_server_and_keeps_playing() {
    const MOVED_AUDIO_SSRC: u32 = OWN_AUDIO_SSRC + 100;
    const MOVED_VIDEO_SSRC: u32 = VIDEO_SSRC + 100;
    let mut rig = rig(RtcCfg::stream("H264", None)).await;
    let mut moved_cfg = RtcCfg::stream("H264", None);
    moved_cfg.own_ssrc = MOVED_AUDIO_SSRC;
    moved_cfg.streams = vec![json!({
        "type": "screen", "rid": "100", "quality": 100, "active": false,
        "ssrc": MOVED_VIDEO_SSRC, "rtx_ssrc": MOVED_VIDEO_SSRC + 1,
    })];
    let moved = start_rtc(moved_cfg).await;
    let stream = create(&rig).await;
    let dave = Arc::clone(stream.connection().unwrap().dave());
    stream.play(h264_source(), params()).unwrap();
    let mut tap = Tap::new(&rig.stream_rtc);
    tap.wait("H264 frames", |w| {
        frames_of(&video_packets(w, VIDEO_SSRC)).len() >= 2
    })
    .await;
    let frames_before = stream.send_stats().video_frames;

    // StreamConnectionState.stream_server_update: close with 4014, handshake with the new server
    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: own_key(),
        token: Some(Secret::new("FAKE-stream-token-moved")),
        endpoint: Some(format!("wss://{}", moved.endpoint)),
    });
    loop {
        match timeout(WAIT, rig.events.recv()).await.unwrap().unwrap() {
            ClientEvent::Reconnected { stream_key } => {
                assert_eq!(stream_key, Some(own_key()));
                break;
            }
            ClientEvent::Disconnected { reason, .. } => panic!("torn down: {reason}"),
            _ => {}
        }
    }
    let identify = moved.wait_message(0).await;
    assert_eq!(identify["d"]["token"], "FAKE-stream-token-moved");
    assert_eq!(identify["d"]["session_id"], "sess-1");
    assert_eq!(identify["d"]["server_id"], RTC_SERVER.to_string());
    assert!(stream.is_connected() && stream.is_playing());
    assert_eq!(stream.own_ssrc().unwrap(), MOVED_AUDIO_SSRC);
    assert!(
        Arc::ptr_eq(stream.connection().unwrap().dave(), &dave),
        "the DAVE session is kept"
    );

    // the same source goes on, on the new connection's SSRCs
    let mut moved_tap = Tap::new(&moved);
    moved_tap
        .wait("frames and audio on the new server", |w| {
            frames_of(&video_packets(w, MOVED_VIDEO_SSRC)).len() >= 2
                && w.rtp.iter().any(|(_, p)| p.packet.ssrc == MOVED_AUDIO_SSRC)
        })
        .await;
    assert!(stream.send_stats().video_frames > frames_before);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let announced = moved.messages(12).iter().any(|m| {
            m["d"]["video_ssrc"] == MOVED_VIDEO_SSRC && m["d"]["audio_ssrc"] == MOVED_AUDIO_SSRC
        });
        let speaking = moved
            .messages(5)
            .iter()
            .any(|m| m["d"]["speaking"] == 2 && m["d"]["ssrc"] == MOVED_AUDIO_SSRC);
        if announced && speaking {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "op 12 with the new SSRCs and SPEAKING on the new connection"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // no second op 18, and nothing deleted
    let seen = rig.seen();
    assert_eq!(
        seen.iter()
            .filter(|s| s.starts_with("stream_create:"))
            .count(),
        1,
        "{seen:?}"
    );
    assert!(
        !seen.iter().any(|s| s.starts_with("stream_delete:")),
        "{seen:?}"
    );
    stream.disconnect().await;
    rig.wait_seen(&format!("stream_delete:{}", own_key())).await;
}
