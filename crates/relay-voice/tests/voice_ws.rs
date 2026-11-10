//! Voice websocket tests against a fake voice server (tokio-tungstenite on 127.0.0.1); never touches
//! Discord.

// The handshake callback's error type is fixed by tokio-tungstenite.
#![allow(clippy::result_large_err)]

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::mls::{FakeExternalSender, split_commit_welcome};
use futures_util::{SinkExt, StreamExt};
use relay_voice::dave::{Dave, DaveMedia};
use relay_voice::transport::TransportEncryptionMode;
use relay_voice::voice_ws::{
    CodecSpec, SessionInfo, StreamSpec, VoiceEvent, VoiceToken, VoiceWs, VoiceWsConfig,
    choose_mode, gateway_url, op, speaking,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const FAKE_TOKEN: &str = "FAKE-voice-token-do-not-leak";
const WAIT: Duration = Duration::from_secs(10);
const SERVER_ID: u64 = 111;
const CHANNEL_ID: u64 = 222;
const USER_ID: u64 = 333;

// ---------------------------------------------------------------------------------------------
// fake server

enum Frame {
    Json(Value),
    Binary(Vec<u8>),
}

struct Server {
    ws: WebSocketStream<TcpStream>,
    /// Path and query of the upgrade request.
    uri: String,
    headers: Vec<(String, String)>,
    /// `seq_ack` of every heartbeat the server answered.
    beats: Vec<i64>,
}

impl Server {
    async fn accept(listener: &TcpListener) -> Self {
        let (stream, _) = timeout(WAIT, listener.accept())
            .await
            .expect("client connects")
            .unwrap();
        let seen = Arc::new(Mutex::new((String::new(), Vec::new())));
        let seen2 = Arc::clone(&seen);
        let ws =
            tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, resp: Response| {
                let headers = req
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_owned(),
                            v.to_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect();
                *seen2.lock().unwrap() = (req.uri().to_string(), headers);
                Ok(resp)
            })
            .await
            .unwrap();
        let (uri, headers) = std::mem::take(&mut *seen.lock().unwrap());
        Self {
            ws,
            uri,
            headers,
            beats: Vec::new(),
        }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    async fn send(&mut self, v: Value) {
        self.ws.send(Message::text(v.to_string())).await.unwrap();
    }

    /// Voice binary frame: `seq` (u16 BE), opcode, payload.
    async fn send_binary(&mut self, seq: u16, opcode: u8, payload: &[u8]) {
        let mut frame = seq.to_be_bytes().to_vec();
        frame.push(opcode);
        frame.extend_from_slice(payload);
        self.ws.send(Message::binary(frame)).await.unwrap();
    }

    async fn hello(&mut self, interval_ms: u64) {
        self.send(json!({"op": 8, "d": {"heartbeat_interval": interval_ms}}))
            .await;
    }

    async fn ready(&mut self) {
        self.send(json!({"op": 2, "d": {
            "ssrc": 4242,
            "ip": "127.0.0.1",
            "port": 50001,
            "modes": ["xsalsa20_poly1305", "aead_xchacha20_poly1305_rtpsize", "aead_aes256_gcm_rtpsize"],
            "streams": [{"type": "video", "rid": "100", "quality": 100, "ssrc": 4243, "rtx_ssrc": 4244}],
            "experiments": ["fixed_keyframe_interval"],
        }}))
        .await;
    }

    /// Next frame, answering heartbeats on the way.
    async fn recv_opt(&mut self) -> Option<Frame> {
        loop {
            match self.ws.next().await? {
                Ok(Message::Text(t)) => {
                    let v: Value = serde_json::from_str(t.as_str()).unwrap();
                    if v["op"] == 3 {
                        self.beats.push(v["d"]["seq_ack"].as_i64().unwrap());
                        self.send(json!({"op": 6, "d": {"t": v["d"]["t"]}})).await;
                        continue;
                    }
                    return Some(Frame::Json(v));
                }
                Ok(Message::Binary(b)) => return Some(Frame::Binary(b.to_vec())),
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }

    async fn recv(&mut self) -> Frame {
        timeout(WAIT, self.recv_opt())
            .await
            .expect("message from client")
            .expect("socket open")
    }

    async fn recv_json(&mut self) -> Value {
        match self.recv().await {
            Frame::Json(v) => v,
            Frame::Binary(b) => panic!("expected JSON, got binary {b:?}"),
        }
    }

    async fn recv_binary(&mut self) -> Vec<u8> {
        match self.recv().await {
            Frame::Binary(b) => b,
            Frame::Json(v) => panic!("expected binary, got {v}"),
        }
    }

    async fn close(&mut self, code: u16) {
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: "".into(),
        };
        let _ = self.ws.send(Message::Close(Some(frame))).await;
    }
}

// ---------------------------------------------------------------------------------------------
// harness

fn config(addr: SocketAddr) -> VoiceWsConfig {
    let mut c = VoiceWsConfig::new(
        &addr.to_string(),
        SERVER_ID,
        CHANNEL_ID,
        USER_ID,
        "session-1",
        VoiceToken::new(FAKE_TOKEN),
    );
    c.url = format!("ws://{addr}/?v=9");
    c.user_agent = "relay-test-agent".into();
    c.streams = vec![StreamSpec::screen("100", 100, Some(30))];
    c.rtc_connection_id = "rtc-1".into();
    c
}

async fn events_until<F: Fn(&VoiceEvent) -> bool>(
    rx: &mut UnboundedReceiver<VoiceEvent>,
    pred: F,
) -> VoiceEvent {
    let deadline = Instant::now() + WAIT;
    loop {
        let ev = timeout(deadline - Instant::now(), rx.recv())
            .await
            .expect("event in time")
            .expect("event stream open");
        if pred(&ev) {
            return ev;
        }
    }
}

struct Harness {
    server: Server,
    ws: VoiceWs,
    events: UnboundedReceiver<VoiceEvent>,
    dave: Arc<Dave>,
}

async fn start(tweak: impl FnOnce(&mut VoiceWsConfig)) -> Harness {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = config(addr);
    tweak(&mut cfg);
    let dave = Arc::new(Dave::new(USER_ID, CHANNEL_ID));
    let http = wreq::Client::builder().build().unwrap();
    let client = tokio::spawn({
        let dave = Arc::clone(&dave);
        async move { VoiceWs::connect(&http, cfg, dave).await }
    });
    let server = Server::accept(&listener).await;
    let (ws, events) = client.await.unwrap().expect("connect");
    Harness {
        server,
        ws,
        events,
        dave,
    }
}

fn secret_key() -> Vec<u8> {
    (0u8..32).collect()
}

fn session_description(dave_version: u64) -> Value {
    json!({"op": 4, "d": {
        "mode": "aead_aes256_gcm_rtpsize",
        "secret_key": secret_key(),
        "audio_codec": "opus",
        "video_codec": "H264",
        "media_session_id": "media-1",
        "keyframe_interval": 2000,
        "dave_protocol_version": dave_version,
    }})
}

// ---------------------------------------------------------------------------------------------
// tests

#[test]
fn gateway_url_uses_the_pinned_voice_version() {
    assert_eq!(
        gateway_url("c-fra01.discord.media:443", 9),
        "wss://c-fra01.discord.media:443/?v=9"
    );
    assert_eq!(
        VoiceWsConfig::new("x.discord.media", 1, 2, 3, "s", VoiceToken::new("t")).url,
        "wss://x.discord.media/?v=9"
    );
}

#[test]
fn choose_mode_follows_client_preference() {
    let ready = vec![
        "xsalsa20_poly1305".to_owned(),
        "aead_xchacha20_poly1305_rtpsize".to_owned(),
        "aead_aes256_gcm_rtpsize".to_owned(),
    ];
    let prefs = [
        TransportEncryptionMode::AeadAes256GcmRtpSize,
        TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize,
    ];
    assert_eq!(choose_mode(&prefs, &ready), Some(prefs[0]));
    assert_eq!(choose_mode(&prefs, &ready[..2]), Some(prefs[1]));
    assert_eq!(choose_mode(&prefs, &ready[..1]), None);
}

#[tokio::test]
async fn handshake_identify_ready_select_protocol_session_description() {
    let mut h = start(|_| {}).await;
    h.server.hello(40).await;

    // IDENTIFY is sent before HELLO is even read.
    let identify = h.server.recv_json().await;
    assert_eq!(h.server.uri, "/?v=9");
    assert_eq!(h.server.header("user-agent"), Some("relay-test-agent"));
    assert_eq!(h.server.header("origin"), Some("https://discord.com"));
    assert_eq!(
        identify,
        json!({"op": 0, "d": {
            "server_id": "111",
            "channel_id": "222",
            "user_id": "333",
            "session_id": "session-1",
            "token": FAKE_TOKEN,
            "video": true,
            "streams": [{"type": "screen", "rid": "100", "quality": 100, "active": true, "max_framerate": 30}],
            "max_dave_protocol_version": 1,
        }})
    );

    h.server.ready().await;
    let VoiceEvent::Ready(ready) =
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::Ready(_))).await
    else {
        unreachable!()
    };
    assert_eq!(
        (ready.ssrc, ready.ip.as_str(), ready.port),
        (4242, "127.0.0.1", 50001)
    );
    assert_eq!(ready.streams.len(), 1);
    assert_eq!(ready.experiments, ["fixed_keyframe_interval"]);
    assert_eq!(h.ws.ssrc(), 4242);
    // READY triggers the voice backend version request.
    assert_eq!(h.server.recv_json().await, json!({"op": 16, "d": {}}));

    // The caller did UDP IP discovery and now selects the protocol with the preferred mode.
    let cfg = config("127.0.0.1:1".parse().unwrap());
    let mode = choose_mode(&cfg.supported_modes, &ready.modes).unwrap();
    assert_eq!(mode, TransportEncryptionMode::AeadAes256GcmRtpSize);
    h.ws.select_protocol("203.0.113.9", 51234, mode).unwrap();
    let select = h.server.recv_json().await;
    assert_eq!(select["op"], 1);
    assert_eq!(select["d"]["protocol"], "udp");
    assert_eq!(
        select["d"]["data"],
        json!({"address": "203.0.113.9", "port": 51234, "mode": "aead_aes256_gcm_rtpsize"})
    );
    assert_eq!(select["d"]["rtc_connection_id"], "rtc-1");
    assert_eq!(
        select["d"]["experiments"],
        json!(["fixed_keyframe_interval"])
    );
    let codecs = select["d"]["codecs"].as_array().unwrap();
    assert_eq!(codecs.len(), 6);
    assert_eq!(
        codecs[0],
        json!({"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120, "encode": true, "decode": true})
    );
    assert_eq!(
        codecs[3],
        json!({"name": "H264", "type": "video", "priority": 3000, "payload_type": 105,
               "encode": false, "decode": false, "rtx_payload_type": 106})
    );

    // SESSION_DESCRIPTION without DAVE: secret key, codecs, a "not speaking" SPEAKING, no key package.
    h.server.send(session_description(0)).await;
    let VoiceEvent::SessionDescription(sd) = events_until(&mut h.events, |e| {
        matches!(e, VoiceEvent::SessionDescription(_))
    })
    .await
    else {
        unreachable!()
    };
    assert_eq!(sd.mode, "aead_aes256_gcm_rtpsize");
    assert_eq!(sd.secret_key, secret_key());
    assert_eq!(sd.info.video_codec.as_deref(), Some("H264"));
    assert_eq!(sd.info.media_session_id.as_deref(), Some("media-1"));
    assert_eq!(sd.info.keyframe_interval, Some(2000));
    assert_eq!(sd.dave_protocol_version, 0);
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 5, "d": {"speaking": 0, "delay": 0, "ssrc": 4242}})
    );
    assert!(!h.dave.has_session());
}

#[tokio::test]
async fn heartbeat_carries_seq_ack_from_text_and_binary_frames() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(30).await;

    // Nothing received yet: seq_ack is -1.
    let deadline = Instant::now() + WAIT;
    while h.server.beats.is_empty() {
        assert!(Instant::now() < deadline);
        let _ = timeout(Duration::from_millis(100), h.server.recv_opt()).await;
    }
    assert_eq!(h.server.beats[0], -1);

    // A text message with `seq`, then a binary frame whose header carries one.
    h.server
        .send(json!({"op": 5, "seq": 5, "d": {"user_id": "9", "ssrc": 7, "speaking": 1}}))
        .await;
    let msg = events_until(&mut h.events, |e| matches!(e, VoiceEvent::Message { .. })).await;
    assert_eq!(
        msg,
        VoiceEvent::Message {
            op: 5,
            data: json!({"user_id": "9", "ssrc": 7, "speaking": 1})
        }
    );
    assert_eq!(h.ws.seq_ack(), 5);
    let deadline = Instant::now() + WAIT;
    while h.server.beats.last() != Some(&5) {
        assert!(Instant::now() < deadline, "beats {:?}", h.server.beats);
        let _ = timeout(Duration::from_millis(100), h.server.recv_opt()).await;
    }
    h.server.send_binary(0x0102, 99, b"ignored").await;
    while h.server.beats.last() != Some(&258) {
        assert!(Instant::now() < deadline, "beats {:?}", h.server.beats);
        let _ = timeout(Duration::from_millis(100), h.server.recv_opt()).await;
    }
    assert_eq!(h.ws.seq_ack(), 258);
}

#[tokio::test]
async fn session_update_merges_into_the_negotiated_session() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server.send(session_description(0)).await;
    events_until(&mut h.events, |e| {
        matches!(e, VoiceEvent::SessionDescription(_))
    })
    .await;

    h.server
        .send(json!({"op": 14, "d": {"video_codec": "VP8"}}))
        .await;
    assert_eq!(
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::SessionUpdate(_))).await,
        VoiceEvent::SessionUpdate(SessionInfo {
            audio_codec: Some("opus".into()),
            video_codec: Some("VP8".into()),
            media_session_id: Some("media-1".into()),
            keyframe_interval: Some(2000),
        })
    );

    h.server
        .send(json!({"op": 14, "d": {"keyframe_interval": 5000, "media_session_id": "media-2"}}))
        .await;
    assert_eq!(
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::SessionUpdate(_))).await,
        VoiceEvent::SessionUpdate(SessionInfo {
            audio_codec: Some("opus".into()),
            video_codec: Some("VP8".into()),
            media_session_id: Some("media-2".into()),
            keyframe_interval: Some(5000),
        })
    );
}

#[tokio::test]
async fn outgoing_ops_match_the_python_payloads() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server.ready().await;
    events_until(&mut h.events, |e| matches!(e, VoiceEvent::Ready(_))).await;
    assert_eq!(h.server.recv_json().await["op"], 16);

    let stream = StreamSpec::screen("100", 100, Some(30));
    h.ws.video_state(4243, 4244, std::slice::from_ref(&stream))
        .unwrap();
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 12, "d": {
            "audio_ssrc": 4242, "video_ssrc": 4243, "rtx_ssrc": 4244,
            "streams": [{"type": "screen", "rid": "100", "quality": 100, "active": true, "max_framerate": 30}],
        }})
    );
    h.ws.video_state(0, 0, &[]).unwrap();
    assert_eq!(
        h.server.recv_json().await["d"],
        json!({"audio_ssrc": 4242, "video_ssrc": 0, "rtx_ssrc": 0, "streams": []})
    );

    h.ws.session_update(&[CodecSpec::opus()]).unwrap();
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 14, "d": {"codecs": [
            {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120, "encode": true, "decode": true}
        ]}})
    );

    h.ws.speak(speaking::VOICE, 0).unwrap();
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 5, "d": {"speaking": 1, "delay": 0, "ssrc": 4242}})
    );

    h.ws.media_sink_wants(&[(4243, 100)], Some(100), Some(&[(4243, 921_600)]))
        .unwrap();
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 15, "d": {"4243": 100, "any": 100, "pixelCounts": {"4243": 921_600}}})
    );

    h.ws.send_binary(26, b"kp").unwrap();
    assert_eq!(h.server.recv_binary().await, vec![26, b'k', b'p']);
}

#[tokio::test]
async fn other_text_ops_reach_the_hook_and_backend_version_is_typed() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server
        .send(json!({"op": 16, "d": {"voice": "v1.2", "rtc_worker": "w3"}}))
        .await;
    assert_eq!(
        events_until(&mut h.events, |e| matches!(
            e,
            VoiceEvent::BackendVersion { .. }
        ))
        .await,
        VoiceEvent::BackendVersion {
            voice: Some("v1.2".into()),
            rtc_worker: Some("w3".into())
        }
    );
    for (op, data) in [
        (11, json!({"user_ids": ["1", "2"]})),
        (12, json!({"user_id": "9", "video_ssrc": 5, "streams": []})),
        (13, json!({"user_id": "9"})),
        (15, json!({"any": 100})),
    ] {
        h.server.send(json!({"op": op, "d": data.clone()})).await;
        assert_eq!(
            events_until(&mut h.events, |e| matches!(e, VoiceEvent::Message { .. })).await,
            VoiceEvent::Message { op, data }
        );
    }
}

#[tokio::test]
async fn resume_sends_op7_with_the_previous_seq_ack() {
    let mut h = start(|c| {
        c.resume = true;
        c.seq_ack = 42;
    })
    .await;
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 7, "d": {
            "token": FAKE_TOKEN,
            "server_id": "111",
            "channel_id": "222",
            "session_id": "session-1",
            "seq_ack": 42,
        }})
    );
    assert_eq!(h.ws.seq_ack(), 42);
    h.server.send(json!({"op": 9, "d": null})).await;
    assert_eq!(
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::Resumed)).await,
        VoiceEvent::Resumed
    );
}

#[tokio::test]
async fn server_close_code_is_reported() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.close(4014).await;
    let VoiceEvent::Closed { code, .. } =
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::Closed { .. })).await
    else {
        unreachable!()
    };
    assert_eq!(code, Some(4014));
    assert!(h.ws.send_json(op::SPEAKING, json!({})).is_err());
}

#[tokio::test]
async fn client_close_sends_the_code() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.ws.close(1000);
    let VoiceEvent::Closed { code, .. } =
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::Closed { .. })).await
    else {
        unreachable!()
    };
    assert_eq!(code, Some(1000));
    assert!(timeout(WAIT, h.server.recv_opt()).await.unwrap().is_none());
}

#[tokio::test]
async fn unacknowledged_heartbeats_close_with_4000() {
    let mut h = start(|c| c.heartbeat_timeout = Duration::from_millis(200)).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(40).await;
    // Keep the socket busy (so only the missing acks count) but never answer a heartbeat.
    let feeder = async {
        loop {
            h.server
                .ws
                .send(Message::text(json!({"op": 15, "d": {}}).to_string()))
                .await
                .ok();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let closed =
        async { events_until(&mut h.events, |e| matches!(e, VoiceEvent::Closed { .. })).await };
    tokio::select! {
        ev = closed => {
            let VoiceEvent::Closed { code, .. } = ev else { unreachable!() };
            assert_eq!(code, Some(4000));
        }
        () = feeder => unreachable!(),
    }
}

#[tokio::test]
async fn silent_socket_is_closed_after_the_heartbeat_timeout() {
    let mut h = start(|c| c.heartbeat_timeout = Duration::from_millis(150)).await;
    let _identify = h.server.recv_json().await;
    let VoiceEvent::Closed { code, reason } =
        events_until(&mut h.events, |e| matches!(e, VoiceEvent::Closed { .. })).await
    else {
        unreachable!()
    };
    assert_eq!(code, None);
    assert!(reason.contains("no message"), "{reason}");
}

#[tokio::test]
async fn dave_ops_round_trip_over_the_socket() {
    let sender = FakeExternalSender::new(CHANNEL_ID);
    let peer_id = 777u64;
    let peer = Dave::new(peer_id, CHANNEL_ID);

    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server.ready().await;
    assert_eq!(h.server.recv_json().await["op"], 16);

    // SESSION_DESCRIPTION with DAVE v1: speak(none), then our key package as binary op 26.
    h.server.send(session_description(1)).await;
    assert_eq!(h.server.recv_json().await["op"], 5);
    let kp = h.server.recv_binary().await;
    assert_eq!(kp[0], op::MLS_KEY_PACKAGE);
    assert!(kp.len() > 1);
    assert!(h.dave.has_session());
    assert_eq!(h.dave.protocol_version(), 1);

    // The gateway's peer joins with its own key package.
    let peer_kp = match peer.on_session_description(1).unwrap().as_slice() {
        [relay_voice::dave::DaveOut::KeyPackage(kp)] => kp.clone(),
        other => panic!("{other:?}"),
    };
    peer.on_binary(op::MLS_EXTERNAL_SENDER, &sender.marshalled())
        .unwrap();

    // op 25 external sender, then op 27 proposals (append: add the peer) as seq-framed binary.
    h.server
        .send_binary(1, op::MLS_EXTERNAL_SENDER, &sender.marshalled())
        .await;
    let mut proposals = vec![0u8];
    proposals.extend_from_slice(&sender.propose_add(0, &peer_kp));
    h.server.send_binary(2, op::MLS_PROPOSALS, &proposals).await;

    // We answer with op 28: commit then welcome, no seq in client frames.
    let cw = h.server.recv_binary().await;
    assert_eq!(cw[0], op::MLS_COMMIT_WELCOME);
    let (commit, welcome) = split_commit_welcome(&cw[1..]);

    // op 29 announces the commit for transition 5; we report ready.
    let mut announce = 5u16.to_be_bytes().to_vec();
    announce.extend_from_slice(&commit);
    h.server
        .send_binary(3, op::MLS_ANNOUNCE_COMMIT_TRANSITION, &announce)
        .await;
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 23, "d": {"transition_id": 5}})
    );
    assert_eq!(h.ws.seq_ack(), 3);

    // The peer gets the welcome; both execute the transition (op 22 for us).
    let mut welcome_msg = 5u16.to_be_bytes().to_vec();
    welcome_msg.extend_from_slice(&welcome);
    peer.on_binary(op::MLS_WELCOME, &welcome_msg).unwrap();
    peer.on_execute_transition(5).unwrap();
    assert!(!h.dave.can_encrypt());
    h.server
        .send(json!({"op": 22, "d": {"transition_id": 5}}))
        .await;
    let deadline = Instant::now() + WAIT;
    while !h.dave.can_encrypt() {
        assert!(Instant::now() < deadline, "transition never executed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let opus = [0x78u8, 1, 2, 3, 4, 5, 6, 7];
    let sealed = h.dave.encrypt_opus(&opus).unwrap();
    assert_ne!(sealed.as_ref(), &opus[..]);
    assert_eq!(
        peer.decrypt(USER_ID, DaveMedia::Audio, &sealed).unwrap(),
        opus.to_vec()
    );
    let back = peer.encrypt_opus(&opus).unwrap();
    assert_eq!(
        h.dave.decrypt(peer_id, DaveMedia::Audio, &back).unwrap(),
        opus.to_vec()
    );
}

#[tokio::test]
async fn invalid_welcome_answers_op31_and_a_new_key_package() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server.send(session_description(1)).await;
    assert_eq!(h.server.recv_json().await["op"], 5);
    assert_eq!(h.server.recv_binary().await[0], op::MLS_KEY_PACKAGE);

    // No external sender was ever set, so the welcome cannot be processed.
    let mut garbage = 9u16.to_be_bytes().to_vec();
    garbage.extend_from_slice(b"not a welcome");
    h.server.send_binary(1, op::MLS_WELCOME, &garbage).await;
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 31, "d": {"transition_id": 9}})
    );
    assert_eq!(h.server.recv_binary().await[0], op::MLS_KEY_PACKAGE);
}

#[tokio::test]
async fn dave_text_ops_prepare_epoch_and_downgrade() {
    let mut h = start(|_| {}).await;
    let _identify = h.server.recv_json().await;
    h.server.hello(60_000).await;
    h.server.send(session_description(1)).await;
    assert_eq!(h.server.recv_json().await["op"], 5);
    assert_eq!(h.server.recv_binary().await[0], op::MLS_KEY_PACKAGE);

    // op 24 epoch 1: a new MLS group, so a new key package.
    h.server
        .send(json!({"op": 24, "d": {"epoch": 1, "protocol_version": 1}}))
        .await;
    assert_eq!(h.server.recv_binary().await[0], op::MLS_KEY_PACKAGE);

    // op 21 to version 0 with a transition id: we answer op 23.
    h.server
        .send(json!({"op": 21, "d": {"transition_id": 4, "protocol_version": 0}}))
        .await;
    assert_eq!(
        h.server.recv_json().await,
        json!({"op": 23, "d": {"transition_id": 4}})
    );
    h.server
        .send(json!({"op": 22, "d": {"transition_id": 4}}))
        .await;
    let deadline = Instant::now() + WAIT;
    while h.dave.protocol_version() != 0 {
        assert!(Instant::now() < deadline, "downgrade never executed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn connect_failure_does_not_leak_the_token() {
    // Nothing listens on this port.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    let http = wreq::Client::builder().build().unwrap();
    let err = VoiceWs::connect(&http, config(addr), Arc::new(Dave::new(1, 2)))
        .await
        .err()
        .expect("connect fails");
    assert!(!err.to_string().contains(FAKE_TOKEN));
}

#[test]
fn debug_output_never_shows_the_token_or_secret_key() {
    let cfg = config("127.0.0.1:1".parse().unwrap());
    assert!(!format!("{cfg:?}").contains(FAKE_TOKEN));
    assert!(format!("{:?}", cfg.token).contains("***"));
    let sd = relay_voice::voice_ws::SessionDescription {
        mode: "m".into(),
        secret_key: vec![7, 8, 9],
        info: SessionInfo::default(),
        dave_protocol_version: 0,
    };
    let shown = format!("{sd:?}");
    assert!(shown.contains("***") && !shown.contains("[7, 8, 9]"));
}
