//! Join + watch end to end on localhost: a fake gateway link, fake voice and stream servers
//! (tokio-tungstenite websocket + a UDP socket answering IP discovery and sending encrypted RTP built
//! from the T4 fixtures). DAVE runs in passthrough (protocol version 0). Never touches Discord.

// The handshake callback's error type is fixed by tokio-tungstenite.
#![allow(clippy::result_large_err)]

mod common;

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{read_wire_packets, rtp_manifest};
use futures_util::{SinkExt, StreamExt};
use relay_media::codec::VoiceCodec;
use relay_voice::receive::{MediaType, Packet, PacketCodec};
use relay_voice::rtcp::{RTCP_PAYLOAD_FEEDBACK, RTCP_RECEIVER_REPORT, RTCP_RTP_FEEDBACK};
use relay_voice::transport::TransportEncryptionMode;
use relay_voice::transport::packets::SharedTransportCrypto;
use relay_voice::voice_ws::{CodecSpec, default_codecs, video_codec_list};
use relay_voice::{
    ClientEvent, JoinOptions, VoiceClient, VoiceClientConfig, VoiceError,
    connection::RtcSettings,
    link::{
        GatewayLink, LinkError, LinkEvent, LinkFuture, Secret, StreamCreateRequest, StreamDelete,
        VoiceStateRequest,
    },
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const WAIT: Duration = Duration::from_secs(10);
const FAKE_VOICE_TOKEN: &str = "FAKE-voice-token-do-not-leak";
const FAKE_STREAM_TOKEN: &str = "FAKE-stream-token-do-not-leak";
const GUILD: u64 = 111;
const CHANNEL: u64 = 222;
const ME: u64 = 333;
const TARGET: u64 = 555;
const RTC_SERVER: u64 = 9001;
const RTC_CHANNEL: u64 = 9002;
const MEDIA_SSRC: u32 = 287_454_020;
const RTX_SSRC: u32 = 1_432_778_632;
const AUDIO_SSRC: u32 = 777;
const OWN_STREAM_SSRC: u32 = 4242;

fn stream_key() -> String {
    format!("guild:{GUILD}:{CHANNEL}:{TARGET}")
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

/// What a test can ask the websocket half of a fake server to do.
enum Ctl {
    Send(Value),
    Close(u16),
    /// Drop the TCP connection without a close frame.
    Drop,
}

struct FakeRtc {
    endpoint: String,
    ctl: mpsc::UnboundedSender<Ctl>,
    /// Every JSON message the client sent (heartbeats excluded).
    received: Arc<Mutex<Vec<Value>>>,
    /// Handshake request: path and headers.
    uri: Arc<Mutex<String>>,
    udp: Arc<UdpSide>,
}

struct UdpSide {
    /// Datagrams the client sent that are not IP discovery.
    received: Mutex<Vec<Vec<u8>>>,
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

    fn take(&self) -> Vec<Vec<u8>> {
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
            let got = socket.recv_from(&mut buf);
            if got.is_err() {
                std::thread::sleep(Duration::from_millis(1));
            }
            if let Ok((n, from)) = got {
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
                    side.received.lock().unwrap().push(data.to_vec());
                }
            }
            if let Some(peer) = *side.peer.lock().unwrap() {
                while let Ok(packet) = rx.try_recv() {
                    send_to(&socket, &packet, peer);
                }
            }
        }
    });
    (addr, side)
}

/// `video`: the op 12 message to send right after SESSION_DESCRIPTION. The websocket half takes one
/// connection after the other (a resume opens a new one) and answers RESUME with RESUMED.
async fn start_rtc(own_ssrc: u32, video: Option<Value>) -> FakeRtc {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = listener.local_addr().unwrap();
    let (udp_addr, udp) = start_udp();
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();
    let received = Arc::new(Mutex::new(Vec::new()));
    let uri = Arc::new(Mutex::new(String::new()));
    let (received2, uri2) = (Arc::clone(&received), Arc::clone(&uri));
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let uri3 = Arc::clone(&uri2);
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                move |req: &Request, resp: Response| {
                    *uri3.lock().unwrap() = req.uri().to_string();
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            ws.send(Message::text(
                json!({"op": 8, "d": {"heartbeat_interval": 5000}}).to_string(),
            ))
            .await
            .unwrap();
            loop {
                tokio::select! {
                    msg = ws.next() => {
                        let Some(Ok(msg)) = msg else { break };
                        let Message::Text(text) = msg else { continue };
                        let v: Value = serde_json::from_str(text.as_str()).unwrap();
                        match v["op"].as_u64().unwrap() {
                            3 => {
                                ws.send(Message::text(json!({"op": 6, "d": {"t": v["d"]["t"]}}).to_string())).await.unwrap();
                                continue;
                            }
                            7 => {
                                ws.send(Message::text(json!({"op": 9, "d": null}).to_string())).await.unwrap();
                            }
                            0 => {
                                ws.send(Message::text(json!({"op": 2, "d": {
                                    "ssrc": own_ssrc, "ip": "127.0.0.1", "port": udp_addr.port(),
                                    "modes": ["aead_xchacha20_poly1305_rtpsize", "aead_aes256_gcm_rtpsize"],
                                    "streams": [], "experiments": ["fixed_keyframe_interval"],
                                }}).to_string())).await.unwrap();
                            }
                            1 => {
                                ws.send(Message::text(json!({"op": 4, "d": {
                                    "mode": "aead_aes256_gcm_rtpsize", "secret_key": secret_key(),
                                    "audio_codec": "opus", "video_codec": "H264",
                                    "media_session_id": "media-1", "keyframe_interval": 2000,
                                    "dave_protocol_version": 0,
                                }}).to_string())).await.unwrap();
                                if let Some(video) = &video {
                                    ws.send(Message::text(video.to_string())).await.unwrap();
                                }
                            }
                            _ => {}
                        }
                        received2.lock().unwrap().push(v);
                    }
                    ctl = ctl_rx.recv() => match ctl {
                        Some(Ctl::Send(v)) => ws.send(Message::text(v.to_string())).await.unwrap(),
                        Some(Ctl::Close(code)) => {
                            let frame = CloseFrame { code: CloseCode::from(code), reason: "".into() };
                            let _ = ws.send(Message::Close(Some(frame))).await;
                        }
                        Some(Ctl::Drop) => break,
                        None => return,
                    },
                }
            }
        }
    });
    FakeRtc {
        endpoint: ws_addr.to_string(),
        ctl: ctl_tx,
        received,
        uri,
        udp,
    }
}

fn video_message() -> Value {
    json!({"op": 12, "d": {
        "user_id": TARGET.to_string(), "audio_ssrc": AUDIO_SSRC,
        "video_ssrc": MEDIA_SSRC, "rtx_ssrc": RTX_SSRC,
        "streams": [{"type": "screen", "rid": "100", "quality": 100, "active": true, "ssrc": MEDIA_SSRC, "rtx_ssrc": RTX_SSRC}],
    }})
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

    /// RTCP the client sent over UDP, decrypted: `(type, count, sender ssrc, payload)`.
    fn rtcp(&self) -> Vec<(u8, u8, u32, Vec<u8>)> {
        let reader = crypto();
        self.udp
            .take()
            .iter()
            .filter(|d| d.len() > 12 && (200..=206).contains(&d[1]))
            .flat_map(|d| reader.decrypt_rtcp_packets(d, 8).unwrap())
            .map(|p| (p.packet_type, p.count, p.sender_ssrc, p.payload))
            .collect()
    }

    async fn wait_rtcp(&self, wanted: u8) -> Vec<(u8, u8, u32, Vec<u8>)> {
        let deadline = tokio::time::Instant::now() + WAIT;
        let mut all = Vec::new();
        loop {
            all.extend(self.rtcp());
            if all.iter().any(|(t, ..)| *t == wanted) {
                return all;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no RTCP type {wanted}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// fake gateway link

#[derive(Debug, PartialEq)]
enum Call {
    VoiceState(VoiceStateRequest),
    StreamCreate(StreamCreateRequest),
    StreamWatch(String),
    StreamDelete(String),
}

struct FakeGateway {
    calls: mpsc::UnboundedSender<Call>,
}

impl GatewayLink for FakeGateway {
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_> {
        Box::pin(async move {
            self.calls
                .send(Call::VoiceState(request))
                .map_err(|e| LinkError(e.to_string()))
        })
    }

    fn stream_create(&self, request: StreamCreateRequest) -> LinkFuture<'_> {
        Box::pin(async move {
            self.calls
                .send(Call::StreamCreate(request))
                .map_err(|e| LinkError(e.to_string()))
        })
    }

    fn stream_watch(&self, stream_key: &str) -> LinkFuture<'_> {
        let key = stream_key.to_owned();
        Box::pin(async move {
            self.calls
                .send(Call::StreamWatch(key))
                .map_err(|e| LinkError(e.to_string()))
        })
    }

    fn stream_delete(&self, stream_key: &str) -> LinkFuture<'_> {
        let key = stream_key.to_owned();
        Box::pin(async move {
            self.calls
                .send(Call::StreamDelete(key))
                .map_err(|e| LinkError(e.to_string()))
        })
    }

    fn stream_set_paused(&self, _stream_key: &str, _paused: bool) -> LinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
enum StreamAnswer {
    /// STREAM_SERVER_UPDATE, then STREAM_CREATE (the order the real gateway tends to use).
    Create {
        endpoint: String,
    },
    /// STREAM_DELETE instead.
    Delete(&'static str),
    Silent,
}

struct Script {
    /// `None`: the gateway never answers the join.
    voice_endpoint: Option<String>,
    stream: StreamAnswer,
}

/// Plays the main gateway: answers what the client sends with the dispatches the real one would.
fn spawn_gateway(
    client: VoiceClient,
    mut calls: mpsc::UnboundedReceiver<Call>,
    script: Arc<Mutex<Script>>,
    seen: Arc<Mutex<Vec<String>>>,
) {
    tokio::spawn(async move {
        while let Some(call) = calls.recv().await {
            let label = match &call {
                Call::VoiceState(r) => format!("voice_state:{:?}", r.channel_id),
                Call::StreamCreate(r) => format!("stream_create:{:?}", r.channel_id),
                Call::StreamWatch(k) => format!("stream_watch:{k}"),
                Call::StreamDelete(k) => format!("stream_delete:{k}"),
            };
            seen.lock().unwrap().push(label);
            match call {
                Call::VoiceState(r) if r.channel_id.is_some() => {
                    let endpoint = script.lock().unwrap().voice_endpoint.clone();
                    if let Some(endpoint) = endpoint {
                        client.handle_event(LinkEvent::VoiceStateUpdate {
                            channel_id: r.channel_id,
                            session_id: Some("sess-1".into()),
                        });
                        client.handle_event(LinkEvent::VoiceServerUpdate {
                            token: Some(Secret::new(FAKE_VOICE_TOKEN)),
                            endpoint: Some(endpoint),
                            guild_id: Some(GUILD),
                            channel_id: None,
                        });
                    }
                }
                Call::VoiceState(_) => client.handle_event(LinkEvent::VoiceStateUpdate {
                    channel_id: None,
                    session_id: None,
                }),
                Call::StreamWatch(key) => {
                    let answer = script.lock().unwrap().stream.clone();
                    match answer {
                        StreamAnswer::Create { endpoint } => {
                            client.handle_event(LinkEvent::StreamServerUpdate {
                                stream_key: key.clone(),
                                token: Some(Secret::new(FAKE_STREAM_TOKEN)),
                                endpoint: Some(format!("wss://{endpoint}")),
                            });
                            client.handle_event(LinkEvent::StreamCreate {
                                stream_key: key,
                                rtc_server_id: RTC_SERVER,
                                rtc_channel_id: RTC_CHANNEL,
                            });
                        }
                        StreamAnswer::Delete(reason) => {
                            client.handle_event(LinkEvent::StreamDelete(StreamDelete {
                                stream_key: key,
                                reason: reason.into(),
                                unavailable: false,
                            }));
                        }
                        StreamAnswer::Silent => {}
                    }
                }
                Call::StreamCreate(_) | Call::StreamDelete(_) => {}
            }
        }
    });
}

struct Rig {
    client: VoiceClient,
    events: mpsc::UnboundedReceiver<ClientEvent>,
    script: Arc<Mutex<Script>>,
    seen: Arc<Mutex<Vec<String>>>,
    voice: FakeRtc,
}

fn watch_settings() -> RtcSettings {
    let codecs = video_codec_list(
        &["AV1", "H265", "H264", "VP8", "VP9"].map(|c| c.parse::<VoiceCodec>().unwrap()),
        false,
    );
    let mut s = RtcSettings::watch(codecs, true);
    s.url_scheme = "ws".into();
    s.receive.debug_stats = true;
    s
}

async fn rig(stream: StreamAnswer) -> Rig {
    let voice = start_rtc(7001, None).await;
    let (calls_tx, calls_rx) = mpsc::unbounded_channel();
    let mut rtc = RtcSettings::new(
        default_codecs(),
        vec![relay_voice::voice_ws::StreamSpec::video(100)],
        true,
    );
    rtc.url_scheme = "ws".into();
    let (client, events) = VoiceClient::new(VoiceClientConfig {
        user_id: ME,
        guild_id: Some(GUILD),
        channel_id: CHANNEL,
        gateway: Arc::new(FakeGateway { calls: calls_tx }),
        http: wreq::Client::builder().build().unwrap(),
        rtc,
    });
    let script = Arc::new(Mutex::new(Script {
        voice_endpoint: Some(voice.endpoint.clone()),
        stream,
    }));
    let seen = Arc::new(Mutex::new(Vec::new()));
    spawn_gateway(
        client.clone(),
        calls_rx,
        Arc::clone(&script),
        Arc::clone(&seen),
    );
    Rig {
        client,
        events,
        script,
        seen,
        voice,
    }
}

impl Rig {
    async fn join(&self) -> String {
        self.client
            .connect(JoinOptions {
                timeout: WAIT,
                self_mute: true,
                ..JoinOptions::default()
            })
            .await
            .expect("join")
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    /// The gateway stand-in records calls on its own task, so a call made just now may need a moment.
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

    async fn next_event(&mut self) -> ClientEvent {
        timeout(WAIT, self.events.recv())
            .await
            .expect("client event")
            .expect("event channel open")
    }
}

/// Wire packets of a codec capture (primary frames only) and the expected AU hashes.
fn capture(codec: &str) -> (Vec<Vec<u8>>, Vec<Value>) {
    let manifest = rtp_manifest();
    let entry = &manifest["codecs"][codec];
    let frames = entry["frames"].as_array().unwrap().clone();
    let primary: usize = frames
        .iter()
        .map(|f| f["packet_count"].as_u64().unwrap() as usize)
        .sum();
    let mut wire = read_wire_packets(entry["wire_file"].as_str().unwrap());
    wire.truncate(primary);
    (wire, frames)
}

async fn next_packet(rx: &mut mpsc::UnboundedReceiver<Packet>) -> Packet {
    timeout(WAIT, rx.recv())
        .await
        .expect("media packet")
        .expect("callback alive")
}

async fn wait_user(stream: &relay_voice::StreamClient, ssrc: u32) {
    let ssrcs = Arc::clone(stream.connection().unwrap().ssrcs());
    let deadline = tokio::time::Instant::now() + WAIT;
    while ssrcs.user_for(ssrc).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "VIDEO message never processed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// tests

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_watch_receive_one_au_per_frame() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;

    // --- join: op 4, echo, voice server, media handshake
    let called = Arc::new(Mutex::new(None::<String>));
    let called2 = Arc::clone(&called);
    let session = rig
        .client
        .connect(JoinOptions {
            timeout: WAIT,
            self_mute: true,
            on_voice_state: Some(Box::new(move |sid| {
                *called2.lock().unwrap() = Some(sid.to_owned())
            })),
            ..JoinOptions::default()
        })
        .await
        .expect("join");
    assert_eq!(session, "sess-1");
    assert_eq!(called.lock().unwrap().as_deref(), Some("sess-1"));
    assert!(rig.client.is_connected());
    assert_eq!(rig.client.session_id().as_deref(), Some("sess-1"));
    assert_eq!(rig.seen(), [format!("voice_state:Some({CHANNEL})")]);
    let identify = rig.voice.wait_message(0).await;
    assert_eq!(identify["d"]["server_id"], GUILD.to_string());
    assert_eq!(identify["d"]["channel_id"], CHANNEL.to_string());
    assert_eq!(identify["d"]["user_id"], ME.to_string());
    assert_eq!(identify["d"]["session_id"], "sess-1");
    assert_eq!(identify["d"]["token"], FAKE_VOICE_TOKEN);
    assert_eq!(*rig.voice.uri.lock().unwrap(), "/?v=9");
    // the join announces itself: empty VIDEO state so the server forwards video to us
    let video = rig.voice.wait_message(12).await;
    assert_eq!(video["d"]["video_ssrc"], 0);
    assert_eq!(video["d"]["streams"], json!([]));
    // codecs and protocol of SELECT_PROTOCOL
    let select = rig.voice.wait_message(1).await;
    assert_eq!(select["d"]["data"]["mode"], "aead_aes256_gcm_rtpsize");
    assert_eq!(select["d"]["data"]["address"], "127.0.0.1");
    assert!(
        select["d"]["codecs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "opus")
    );
    // an empty receiver report goes out right after the handshake
    let sent = rig.voice.wait_rtcp(RTCP_RECEIVER_REPORT).await;
    assert!(
        sent.iter()
            .any(|(t, count, ssrc, body)| *t == RTCP_RECEIVER_REPORT
                && *count == 0
                && *ssrc == 7001
                && body.is_empty())
    );

    // --- watch: op 20, STREAM_SERVER_UPDATE + STREAM_CREATE, stream handshake
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .expect("watch");
    assert_eq!(stream.key(), stream_key());
    assert!(
        rig.seen()
            .contains(&format!("stream_watch:{}", stream_key()))
    );
    let identify = stream_rtc.wait_message(0).await;
    assert_eq!(identify["d"]["server_id"], RTC_SERVER.to_string());
    assert_eq!(identify["d"]["channel_id"], RTC_CHANNEL.to_string());
    assert_eq!(
        identify["d"]["session_id"], "sess-1",
        "the stream uses the voice session"
    );
    assert_eq!(identify["d"]["token"], FAKE_STREAM_TOKEN);
    assert_eq!(identify["d"]["streams"][0]["type"], "screen");
    assert_eq!(stream.own_ssrc().unwrap(), OWN_STREAM_SSRC);
    assert_eq!(
        stream.session_info().unwrap().video_codec.as_deref(),
        Some("H264")
    );
    assert!(stream.is_connected());

    // --- listen, request the video, receive
    let (tx, mut rx) = mpsc::unbounded_channel();
    stream
        .listen(move |p| {
            let _ = tx.send(p);
        })
        .unwrap();
    assert!(matches!(
        stream.listen(|_| {}),
        Err(VoiceError::AlreadyListening)
    ));
    wait_user(&stream, MEDIA_SSRC).await;
    stream.request_video(MEDIA_SSRC, 100).unwrap();
    let wants = stream_rtc.wait_message(15).await;
    assert_eq!(wants["d"][MEDIA_SSRC.to_string()], 100);
    assert_eq!(wants["d"]["any"], 100);

    let (wire, frames) = capture("h264");
    stream_rtc.udp.send(&wire);
    for frame in &frames {
        let packet = next_packet(&mut rx).await;
        assert_eq!(packet.media_type, MediaType::Video);
        assert_eq!(packet.codec, PacketCodec::Video(VoiceCodec::H264));
        assert_eq!(packet.user_id, Some(TARGET));
        assert_eq!(packet.ssrc, MEDIA_SSRC);
        assert_eq!(packet.payload_type, 105);
        assert_eq!(
            common::sha256_hex(&packet.payload),
            frame["depacketized_sha256"].as_str().unwrap()
        );
        assert_eq!(
            packet.rtp_packets.len() as u64,
            frame["packet_count"].as_u64().unwrap()
        );
        let last = frame["packets"].as_array().unwrap().last().unwrap();
        assert_eq!(packet.sequence as u64, last["sequence"].as_u64().unwrap());
        let twcc = packet
            .rtp_extensions
            .iter()
            .find(|e| e.id == 5)
            .expect("TWCC extension");
        assert_eq!(
            twcc.data,
            (last["transport_sequence"].as_u64().unwrap() as u16).to_be_bytes()
        );
    }
    assert!(
        timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_err(),
        "exactly one packet per frame"
    );

    // --- feedback: the startup PLI, an explicit PLI, caller-built RTCP (TWCC)
    let sent = stream_rtc.wait_rtcp(RTCP_PAYLOAD_FEEDBACK).await;
    let pli = sent
        .iter()
        .find(|(t, ..)| *t == RTCP_PAYLOAD_FEEDBACK)
        .unwrap();
    assert_eq!((pli.1, pli.2), (1, OWN_STREAM_SSRC));
    assert_eq!(
        u32::from_be_bytes(pli.3[..4].try_into().unwrap()),
        MEDIA_SSRC
    );
    stream.send_pli(MEDIA_SSRC).unwrap();
    assert!(!stream_rtc.wait_rtcp(RTCP_PAYLOAD_FEEDBACK).await.is_empty());
    let header = [0x8F, 205, 0, 3, 0, 0, 0x10, 0x92];
    stream.send_rtcp(&header, &[0xAA; 8]).unwrap();
    let sent = stream_rtc.wait_rtcp(RTCP_RTP_FEEDBACK).await;
    let twcc = sent.iter().find(|(t, ..)| *t == RTCP_RTP_FEEDBACK).unwrap();
    assert_eq!(
        (twcc.1, twcc.2, twcc.3.as_slice()),
        (15, 0x1092, &[0xAA; 8][..])
    );
    assert_eq!(
        stream.receiver().unwrap().stats()["video_frames_completed"],
        2
    );

    // --- stop: STREAM_DELETE is sent, the voice stays
    stream.disconnect().await;
    rig.wait_seen(&format!("stream_delete:{}", stream_key()))
        .await;
    assert!(!stream.is_connected());
    assert!(rig.client.stream_clients().is_empty());
    assert!(rig.client.is_connected());

    // --- leave
    rig.client.disconnect(WAIT).await;
    assert_eq!(rig.seen().last().unwrap(), "voice_state:None");
    assert!(!rig.client.is_connected());
    assert_eq!(rig.client.session_id(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_every_codec_comes_through_the_watch() {
    for codec in ["h265", "av1", "vp8", "vp9"] {
        let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
        let rig = rig(StreamAnswer::Create {
            endpoint: stream_rtc.endpoint.clone(),
        })
        .await;
        rig.join().await;
        let stream = rig
            .client
            .watch_stream(&stream_key(), watch_settings(), WAIT)
            .await
            .unwrap_or_else(|e| panic!("{codec}: watch failed: {e}"));
        let (tx, mut rx) = mpsc::unbounded_channel();
        stream.listen(move |p| drop(tx.send(p))).unwrap();
        wait_user(&stream, MEDIA_SSRC).await;
        let (wire, frames) = capture(codec);
        stream_rtc.udp.send(&wire);
        for frame in &frames {
            let packet = next_packet(&mut rx).await;
            assert_eq!(
                common::sha256_hex(&packet.payload),
                frame["depacketized_sha256"].as_str().unwrap(),
                "{codec}"
            );
            assert_eq!(packet.codec.name(), codec.to_uppercase());
        }
        stream.disconnect().await;
        rig.client.disconnect(WAIT).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_delete_before_create_rejects_the_watch() {
    let rig = rig(StreamAnswer::Delete("no such stream")).await;
    rig.join().await;
    let err = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .err()
        .expect("rejected");
    assert!(
        matches!(&err, VoiceError::StreamRejected(r) if r == "no such stream"),
        "{err}"
    );
    assert!(rig.client.stream_clients().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stale_stream_delete_echo_does_not_reject_the_rewatch() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .expect("first watch");
    // Discord echoes our op 19 back a little later
    let (client, seen) = (rig.client.clone(), Arc::clone(&rig.seen));
    tokio::spawn(async move {
        while !seen
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.starts_with("stream_delete:"))
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.handle_event(LinkEvent::StreamDelete(StreamDelete {
            stream_key: stream_key(),
            reason: "user_requested".into(),
            unavailable: false,
        }));
    });
    let started = std::time::Instant::now();
    stream.disconnect_awaiting_echo(WAIT).await;
    assert!(started.elapsed() >= Duration::from_millis(100));
    let again = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .expect("the stale echo must not reject the new watch");
    again.disconnect().await;
    rig.client.disconnect(WAIT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_silent_gateway_times_out_the_watch() {
    let rig = rig(StreamAnswer::Silent).await;
    rig.join().await;
    let err = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), Duration::from_millis(300))
        .await
        .err()
        .expect("timeout");
    assert!(matches!(err, VoiceError::Timeout(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_preconditions() {
    let rig = rig(StreamAnswer::Silent).await;
    // not joined yet
    let err = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, VoiceError::NotConnected), "{err}");
    rig.join().await;
    // our own stream
    let own = format!("guild:{GUILD}:{CHANNEL}:{ME}");
    let err = rig
        .client
        .watch_stream(&own, watch_settings(), WAIT)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, VoiceError::Invalid(_)), "{err}");
    // another voice channel
    let other = format!("guild:{GUILD}:{}:{TARGET}", CHANNEL + 1);
    let err = rig
        .client
        .watch_stream(&other, watch_settings(), WAIT)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, VoiceError::Invalid(_)), "{err}");
    // not a stream key
    let err = rig
        .client
        .watch_stream("nonsense", watch_settings(), WAIT)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, VoiceError::Invalid(_)), "{err}");
    // none of this reached the gateway
    assert_eq!(rig.seen(), [format!("voice_state:Some({CHANNEL})")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_times_out_and_leaves_the_voice_state() {
    let rig = rig(StreamAnswer::Silent).await;
    rig.script.lock().unwrap().voice_endpoint = None;
    let err = rig
        .client
        .connect(JoinOptions {
            timeout: Duration::from_millis(300),
            ..JoinOptions::default()
        })
        .await
        .expect_err("no voice server");
    assert!(matches!(err, VoiceError::Timeout(_)), "{err}");
    rig.wait_seen("voice_state:None").await;
    assert_eq!(
        rig.seen(),
        [
            format!("voice_state:Some({CHANNEL})"),
            "voice_state:None".to_owned()
        ]
    );
    // the client can try again
    rig.script.lock().unwrap().voice_endpoint = Some(rig.voice.endpoint.clone());
    rig.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gateway_stream_delete_ends_the_watch_without_a_reply() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    rig.client
        .handle_event(LinkEvent::StreamDelete(StreamDelete {
            stream_key: stream_key(),
            reason: "ended".into(),
            unavailable: false,
        }));
    assert_eq!(
        rig.next_event().await,
        ClientEvent::StreamEnded {
            stream_key: stream_key(),
            reason: "ended".into(),
            unavailable: false
        }
    );
    assert!(!stream.is_connected());
    assert!(rig.client.stream_clients().is_empty());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!rig.seen().iter().any(|s| s.starts_with("stream_delete")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_websocket_close_is_reported_and_the_gateway_told() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    stream_rtc.ctl.send(Ctl::Close(4006)).unwrap();
    match rig.next_event().await {
        ClientEvent::Disconnected {
            stream_key: key,
            code,
            ..
        } => {
            assert_eq!(key.as_deref(), Some(stream_key().as_str()));
            assert_eq!(code, Some(4006));
        }
        other => panic!("{other:?}"),
    }
    assert!(!stream.is_connected());
    assert!(!stream.is_reconnecting(), "gave up");
    rig.wait_seen(&format!("stream_delete:{}", stream_key()))
        .await;
    assert!(matches!(
        stream.request_video(1, 100),
        Err(VoiceError::NotConnected)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_codec_update_is_reported() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    stream_rtc
        .ctl
        .send(Ctl::Send(json!({"op": 14, "d": {"video_codec": "VP9"}})))
        .unwrap();
    match rig.next_event().await {
        ClientEvent::SessionUpdate {
            stream_key: key,
            info,
        } => {
            assert_eq!(key.as_deref(), Some(stream_key().as_str()));
            assert_eq!(info.video_codec.as_deref(), Some("VP9"));
            assert_eq!(
                info.audio_codec.as_deref(),
                Some("opus"),
                "absent keys keep their value"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        stream.session_info().unwrap().video_codec.as_deref(),
        Some("VP9")
    );
    // and the other direction: we renegotiate
    let narrowed: Vec<CodecSpec> = video_codec_list(&[VoiceCodec::H264], false);
    stream.session_update(&narrowed).unwrap();
    let update = stream_rtc.wait_message(14).await;
    assert_eq!(update["d"]["codecs"][1]["name"], "H264");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_update_moves_the_watch_and_keeps_the_listener() {
    let first = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let second = start_rtc(OWN_STREAM_SSRC + 1, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: first.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    stream.listen(move |p| drop(tx.send(p))).unwrap();
    wait_user(&stream, MEDIA_SSRC).await;
    let (wire, frames) = capture("h264");

    // a repeated identical update changes nothing
    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: stream_key(),
        token: Some(Secret::new(FAKE_STREAM_TOKEN)),
        endpoint: Some(first.endpoint.clone()),
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(stream.is_connected());
    assert_eq!(stream.own_ssrc().unwrap(), OWN_STREAM_SSRC);

    // a different server: close the old socket with 4014 and handshake against the new one
    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: stream_key(),
        token: Some(Secret::new("FAKE-new-token")),
        endpoint: Some(second.endpoint.clone()),
    });
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected {
            stream_key: Some(stream_key())
        }
    );
    assert_eq!(stream.own_ssrc().unwrap(), OWN_STREAM_SSRC + 1);
    let identify = second.wait_message(0).await;
    assert_eq!(identify["d"]["token"], "FAKE-new-token");
    wait_user(&stream, MEDIA_SSRC).await;
    second.udp.send(&wire);
    for frame in &frames {
        let packet = next_packet(&mut rx).await;
        assert_eq!(
            common::sha256_hex(&packet.payload),
            frame["depacketized_sha256"].as_str().unwrap()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropped_stream_socket_is_resumed_and_media_keeps_flowing() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    stream.listen(move |p| drop(tx.send(p))).unwrap();
    wait_user(&stream, MEDIA_SSRC).await;

    // a sequenced message, then the voice server "crashes": 4015 is resumed
    stream_rtc
        .ctl
        .send(Ctl::Send(json!({"op": 15, "d": {"any": 100}, "seq": 7})))
        .unwrap();
    stream_rtc.ctl.send(Ctl::Close(4015)).unwrap();
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected {
            stream_key: Some(stream_key())
        }
    );
    let resume = stream_rtc.wait_message(7).await;
    assert_eq!(
        resume["d"],
        json!({
            "token": FAKE_STREAM_TOKEN,
            "server_id": RTC_SERVER.to_string(),
            "channel_id": RTC_CHANNEL.to_string(),
            "session_id": "sess-1",
            "seq_ack": 7,
        })
    );
    assert_eq!(stream_rtc.messages(0).len(), 1, "no second IDENTIFY");
    assert!(stream.is_connected());
    assert_eq!(stream.own_ssrc().unwrap(), OWN_STREAM_SSRC);

    // the UDP session and the listener are the same as before
    let (wire, frames) = capture("h264");
    stream_rtc.udp.send(&wire);
    for frame in &frames {
        let packet = next_packet(&mut rx).await;
        assert_eq!(
            common::sha256_hex(&packet.payload),
            frame["depacketized_sha256"].as_str().unwrap()
        );
    }
    // commands go out on the new socket
    stream.request_video(MEDIA_SSRC, 100).unwrap();
    stream_rtc.wait_message(15).await;
    assert!(!rig.seen().iter().any(|s| s.starts_with("stream_delete")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropped_voice_socket_is_resumed() {
    let mut rig = rig(StreamAnswer::Silent).await;
    rig.join().await;
    rig.voice.ctl.send(Ctl::Drop).unwrap();
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected { stream_key: None }
    );
    let resume = rig.voice.wait_message(7).await;
    assert_eq!(resume["d"]["token"], FAKE_VOICE_TOKEN);
    assert_eq!(resume["d"]["server_id"], GUILD.to_string());
    assert_eq!(resume["d"]["session_id"], "sess-1");
    assert!(rig.client.is_connected());
    assert_eq!(rig.voice.messages(0).len(), 1, "no second IDENTIFY");
    assert_eq!(rig.seen(), [format!("voice_state:Some({CHANNEL})")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_forced_close_waits_for_the_stream_server_update() {
    let first = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let second = start_rtc(OWN_STREAM_SSRC + 1, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: first.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let stream = rig
        .client
        .watch_stream(&stream_key(), watch_settings(), WAIT)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    stream.listen(move |p| drop(tx.send(p))).unwrap();

    // 4014 before the gateway's STREAM_SERVER_UPDATE: wait for it instead of giving up
    assert!(!stream.is_reconnecting());
    first.ctl.send(Ctl::Close(4014)).unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    while stream.is_connected() {
        assert!(tokio::time::Instant::now() < deadline, "still connected");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rig.events.try_recv().is_err(),
        "nothing reported while waiting"
    );
    assert!(stream.is_reconnecting());
    assert!(!rig.seen().iter().any(|s| s.starts_with("stream_delete")));

    rig.client.handle_event(LinkEvent::StreamServerUpdate {
        stream_key: stream_key(),
        token: Some(Secret::new("FAKE-new-token")),
        endpoint: Some(second.endpoint.clone()),
    });
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected {
            stream_key: Some(stream_key())
        }
    );
    assert_eq!(stream.own_ssrc().unwrap(), OWN_STREAM_SSRC + 1);
    assert!(stream.is_connected() && !stream.is_reconnecting());
    let identify = second.wait_message(0).await;
    assert_eq!(identify["d"]["token"], "FAKE-new-token");
    wait_user(&stream, MEDIA_SSRC).await;
    let (wire, frames) = capture("h264");
    second.udp.send(&wire);
    for frame in &frames {
        let packet = next_packet(&mut rx).await;
        assert_eq!(
            common::sha256_hex(&packet.payload),
            frame["depacketized_sha256"].as_str().unwrap()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_voice_server_moves_mid_session() {
    let second = start_rtc(7002, None).await;
    let third = start_rtc(7003, None).await;
    let mut rig = rig(StreamAnswer::Silent).await;
    rig.join().await;
    let voice_server_update = |endpoint: &str, token: &str| LinkEvent::VoiceServerUpdate {
        token: Some(Secret::new(token)),
        endpoint: Some(format!("wss://{endpoint}")),
        guild_id: Some(GUILD),
        channel_id: None,
    };

    // VOICE_SERVER_UPDATE while connected: close with 4014, IDENTIFY on the new server
    rig.client
        .handle_event(voice_server_update(&second.endpoint, "FAKE-voice-token-2"));
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected { stream_key: None }
    );
    let identify = second.wait_message(0).await;
    assert_eq!(identify["d"]["token"], "FAKE-voice-token-2");
    assert_eq!(identify["d"]["session_id"], "sess-1");
    assert!(rig.client.is_connected());
    assert_eq!(rig.client.connection().unwrap().ssrc(), 7002);

    // 4014 first, the gateway's VOICE_SERVER_UPDATE after: wait for it
    second.ctl.send(Ctl::Close(4014)).unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    while rig.client.is_connected() {
        assert!(tokio::time::Instant::now() < deadline, "still connected");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rig.events.try_recv().is_err(),
        "nothing reported while waiting"
    );
    assert!(rig.client.is_reconnecting());
    rig.client
        .handle_event(voice_server_update(&third.endpoint, "FAKE-voice-token-3"));
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected { stream_key: None }
    );
    assert_eq!(
        third.wait_message(0).await["d"]["token"],
        "FAKE-voice-token-3"
    );
    assert!(rig.client.is_connected() && !rig.client.is_reconnecting());
    // never left nor re-joined the channel
    assert_eq!(rig.seen(), [format!("voice_state:Some({CHANNEL})")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reconnect_option_handshakes_again_after_a_close() {
    let stream_rtc = start_rtc(OWN_STREAM_SSRC, Some(video_message())).await;
    let mut rig = rig(StreamAnswer::Create {
        endpoint: stream_rtc.endpoint.clone(),
    })
    .await;
    rig.join().await;
    let mut settings = watch_settings();
    settings.reconnect = true;
    let stream = rig
        .client
        .watch_stream(&stream_key(), settings, WAIT)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    stream.listen(move |p| drop(tx.send(p))).unwrap();

    // 4006 is not resumable: without `reconnect` it ends the watch (see above); with it, a fresh
    // IDENTIFY on the same server
    stream_rtc.ctl.send(Ctl::Close(4006)).unwrap();
    assert_eq!(
        rig.next_event().await,
        ClientEvent::Reconnected {
            stream_key: Some(stream_key())
        }
    );
    assert_eq!(stream_rtc.messages(0).len(), 2);
    assert!(stream.is_connected());
    wait_user(&stream, MEDIA_SSRC).await;
    let (wire, frames) = capture("h264");
    stream_rtc.udp.send(&wire);
    for frame in &frames {
        let packet = next_packet(&mut rx).await;
        assert_eq!(
            common::sha256_hex(&packet.payload),
            frame["depacketized_sha256"].as_str().unwrap()
        );
    }
    assert!(!rig.seen().iter().any(|s| s.starts_with("stream_delete")));
}

#[test]
fn test_tokens_never_show_up_in_debug_output() {
    let events = [
        LinkEvent::VoiceServerUpdate {
            token: Some(Secret::new(FAKE_VOICE_TOKEN)),
            endpoint: Some("x".into()),
            guild_id: None,
            channel_id: None,
        },
        LinkEvent::StreamServerUpdate {
            stream_key: "k".into(),
            token: Some(Secret::new(FAKE_STREAM_TOKEN)),
            endpoint: None,
        },
    ];
    for event in events {
        let shown = format!("{event:?}");
        assert!(!shown.contains("FAKE-"), "{shown}");
        assert!(shown.contains("***"));
    }
}
