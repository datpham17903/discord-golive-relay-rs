//! Gateway tests against a fake server (tokio-tungstenite on 127.0.0.1); never touches Discord.

// The handshake callback's error type is fixed by tokio-tungstenite.
#![allow(clippy::result_large_err)]

use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay_discord::{
    Error, Gateway, GatewayConfig, GatewayEvent, HeadersContext, StreamKey, StreamType, Token,
    VoiceStateUpdate,
};
use serde_json::{Map, Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const FAKE_TOKEN: &str = "FAKE.token-for-tests.do-not-leak";
const WAIT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------------------------
// fake server

struct ServerConn {
    ws: WebSocketStream<TcpStream>,
    enc: Option<zstd::stream::write::Encoder<'static, Vec<u8>>>,
    /// Path and query of the upgrade request.
    uri: String,
    headers: Vec<(String, String)>,
}

impl ServerConn {
    async fn accept(listener: &TcpListener, compress: bool) -> Self {
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
        let enc = compress.then(|| zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap());
        Self {
            ws,
            enc,
            uri,
            headers,
        }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// One WS message per payload; with compression the encoder lives across messages.
    async fn send(&mut self, payload: Value) {
        let text = payload.to_string();
        let msg = match &mut self.enc {
            Some(enc) => {
                enc.write_all(text.as_bytes()).unwrap();
                enc.flush().unwrap();
                Message::binary(std::mem::take(enc.get_mut()))
            }
            None => Message::text(text),
        };
        self.ws.send(msg).await.unwrap();
    }

    async fn hello(&mut self, interval_ms: u64) {
        self.send(json!({"op": 10, "d": {"heartbeat_interval": interval_ms}}))
            .await;
    }

    async fn recv(&mut self) -> Value {
        timeout(WAIT, self.recv_opt())
            .await
            .expect("message from client")
            .expect("socket open")
    }

    async fn recv_opt(&mut self) -> Option<Value> {
        loop {
            match self.ws.next().await? {
                Ok(Message::Text(t)) => return Some(serde_json::from_str(t.as_str()).unwrap()),
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
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

async fn listener() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, addr)
}

// ---------------------------------------------------------------------------------------------
// client helpers

fn context() -> Arc<HeadersContext> {
    let mut props = Map::new();
    for (k, v) in [
        ("os", json!("Windows")),
        ("browser", json!("Chrome")),
        (
            "browser_user_agent",
            json!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/158.0.0.0 Safari/537.36"),
        ),
        ("browser_version", json!("158.0.0.0")),
        ("client_build_number", json!(667580)),
        ("client_launch_id", json!("launch-1")),
        ("client_heartbeat_session_id", json!("hb-1")),
    ] {
        props.insert(k.into(), v);
    }
    let mut extra = Map::new();
    extra.insert("is_fast_connect".into(), json!(false));
    extra.insert("gateway_connect_reasons".into(), json!("AppSkeleton"));
    Arc::new(HeadersContext::new(props, None, Some(extra), 158))
}

fn config(addr: SocketAddr, compress: bool) -> GatewayConfig {
    let ctx = context();
    GatewayConfig {
        token: Token::new(FAKE_TOKEN),
        http: wreq::Client::builder()
            .emulation(ctx.emulation())
            .build()
            .unwrap(),
        headers: ctx,
        url: format!("ws://{addr}/"),
        origin: "https://discord.com".into(),
        api_version: 9,
        compress,
        heartbeat_timeout: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(10),
    }
}

async fn next_event(rx: &mut UnboundedReceiver<GatewayEvent>) -> GatewayEvent {
    timeout(WAIT, rx.recv())
        .await
        .expect("gateway event")
        .expect("event channel open")
}

/// Skips events until the named dispatch arrives.
async fn next_dispatch(
    rx: &mut UnboundedReceiver<GatewayEvent>,
    name: &str,
) -> Box<relay_discord::Dispatch> {
    loop {
        if let GatewayEvent::Dispatch(d) = next_event(rx).await
            && d.name == name
        {
            return d;
        }
    }
}

fn ready_payload(addr: SocketAddr, session: &str) -> Value {
    json!({"op": 0, "t": "READY", "s": 1, "d": {
        "user": {"id": "99"},
        "session_id": session,
        "resume_gateway_url": format!("ws://{addr}/resume"),
        "geo_ordered_rtc_regions": ["rotterdam"],
        "guilds": [{"id": "10", "voice_states": [{"user_id": "20", "channel_id": "30", "self_stream": true}]}],
    }})
}

fn assert_no_token_in_events(events: &[String]) {
    for e in events {
        assert!(!e.contains(FAKE_TOKEN), "{e}");
    }
}

// ---------------------------------------------------------------------------------------------
// tests

#[tokio::test]
async fn hello_identify_ready_over_zstd_stream_then_commands_and_cache() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c = ServerConn::accept(&l, true).await;
        assert_eq!(c.uri, "/?v=9&encoding=json&compress=zstd-stream");
        assert_eq!(c.header("origin"), None);
        assert!(c.header("user-agent").unwrap().contains("Chrome/158.0.0.0"));
        c.hello(60_000).await;

        // IDENTIFY goes out before HELLO is processed, then the first heartbeat.
        let identify = c.recv().await;
        assert_eq!(identify["op"], 2);
        let d = &identify["d"];
        assert_eq!(d["token"], FAKE_TOKEN);
        assert_eq!(d["capabilities"], 22525);
        assert_eq!(
            d["compress"], false,
            "transport compression replaces payload compression"
        );
        assert_eq!(
            d["presence"],
            json!({"status": "unknown", "activities": [], "afk": false, "since": 0})
        );
        assert_eq!(d["client_state"], json!({"guild_versions": {}}));
        assert_eq!(d["properties"]["client_build_number"], 667580);
        assert_eq!(
            d["properties"]["gateway_connect_reasons"], "AppSkeleton",
            "extra gateway properties merged"
        );
        assert_eq!(d["properties"]["is_fast_connect"], false);
        let hb = c.recv().await;
        assert_eq!(hb["op"], 40);
        assert_eq!(
            hb["d"]["qos"],
            json!({"ver": 27, "active": true, "reasons": ["foregrounded"]})
        );
        assert_eq!(hb["d"]["seq"], Value::Null);

        c.send(ready_payload(addr, "sess-1")).await;
        // READY triggers: time-spent update, a heartbeat carrying the sequence, initial op 4.
        let spent = c.recv().await;
        assert_eq!(spent["op"], 41);
        assert_eq!(spent["d"]["session_id"], "hb-1");
        assert_eq!(spent["d"]["client_launch_id"], "launch-1");
        let hb = c.recv().await;
        assert_eq!(
            (hb["op"].clone(), hb["d"]["seq"].clone()),
            (json!(40), json!(1))
        );
        let vs = c.recv().await;
        assert_eq!(
            vs,
            json!({"op": 4, "d": {"guild_id": null, "channel_id": null, "self_mute": false, "self_deaf": false, "self_video": false}})
        );

        c.send(json!({"op": 0, "t": "READY_SUPPLEMENTAL", "s": 2, "d": {"guilds": [{"voice_states": [{"user_id": "21", "channel_id": "30"}]}]}})).await;

        // Commands sent by the test, in order.
        let mut got = Vec::new();
        for _ in 0..5 {
            got.push(c.recv().await);
        }
        c.send(json!({"op": 0, "t": "VOICE_STATE_UPDATE", "s": 3, "d": {"guild_id": "10", "user_id": "20", "channel_id": null, "self_stream": true}})).await;
        c.send(json!({"op": 0, "t": "STREAM_CREATE", "s": 4, "d": {"stream_key": "guild:10:30:20", "rtc_server_id": "7", "rtc_channel_id": "8", "region": "rotterdam", "viewer_ids": [], "paused": false}})).await;
        c.send(json!({"op": 0, "t": "STREAM_UPDATE", "s": 5, "d": {"stream_key": "guild:10:30:20", "paused": true}})).await;
        got
    });

    let (handle, mut rx) = Gateway::spawn(config(addr, true));
    let ready = next_dispatch(&mut rx, "READY").await;
    assert_eq!(ready.seq, Some(1));
    assert_eq!(handle.cache().read().self_id(), Some(99));
    assert!(
        handle
            .cache()
            .read()
            .voice_state(10, 20)
            .unwrap()
            .self_stream
    );

    next_dispatch(&mut rx, "READY_SUPPLEMENTAL").await;
    assert!(handle.cache().read().voice_state(10, 21).is_some());

    let key = StreamKey::Guild {
        guild_id: 10,
        channel_id: 30,
        owner_id: 20,
    };
    handle
        .voice_state(VoiceStateUpdate {
            guild_id: Some(10),
            channel_id: Some(30),
            self_video: true,
            self_stream: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    handle
        .stream_create(StreamType::Guild, Some(10), 30)
        .await
        .unwrap();
    handle.stream_watch(&key).await.unwrap();
    handle.stream_set_paused(&key, true).await.unwrap();
    handle.stream_delete(&key).await.unwrap();

    let got = server.await.unwrap();
    assert_eq!(
        got[0],
        json!({"op": 4, "d": {"guild_id": 10, "channel_id": 30, "self_mute": false, "self_deaf": false, "self_video": true, "self_stream": true, "preferred_regions": ["rotterdam"], "preferred_region": "rotterdam"}})
    );
    assert_eq!(
        got[1],
        json!({"op": 18, "d": {"type": "guild", "guild_id": "10", "channel_id": "30", "preferred_region": "rotterdam"}})
    );
    assert_eq!(
        got[2],
        json!({"op": 20, "d": {"stream_key": "guild:10:30:20"}})
    );
    assert_eq!(
        got[3],
        json!({"op": 22, "d": {"stream_key": "guild:10:30:20", "paused": true}})
    );
    assert_eq!(
        got[4],
        json!({"op": 19, "d": {"stream_key": "guild:10:30:20"}})
    );

    // The cache saw the later dispatches before the events were delivered.
    let vs = next_dispatch(&mut rx, "VOICE_STATE_UPDATE").await;
    assert!(vs.change.is_some());
    assert!(handle.cache().read().voice_state(10, 20).is_none());
    next_dispatch(&mut rx, "STREAM_CREATE").await;
    next_dispatch(&mut rx, "STREAM_UPDATE").await;
    let cache = handle.cache().read();
    let stream = cache.stream(&key).unwrap();
    assert!(stream.paused);
    assert_eq!(stream.region.as_deref(), Some("rotterdam"));
    handle.close();
}

#[tokio::test]
async fn heartbeats_follow_the_interval_and_track_ack_and_sequence() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c = ServerConn::accept(&l, false).await;
        assert_eq!(
            c.uri, "/?v=9&encoding=json",
            "no compress param when transport compression is off"
        );
        c.hello(100).await;
        let identify = c.recv().await;
        assert_eq!(identify["op"], 2);
        assert_eq!(
            identify["d"]["compress"], true,
            "payload compression requested instead"
        );

        c.send(json!({"op": 0, "t": "GUILD_CREATE", "s": 5, "d": {"id": "10"}}))
            .await;
        let mut beats = Vec::new();
        let start = Instant::now();
        while beats.len() < 5 {
            let msg = c.recv().await;
            assert_eq!(msg["op"], 40, "{msg}");
            beats.push((start.elapsed(), msg["d"]["seq"].clone()));
            c.send(json!({"op": 11})).await;
        }
        beats
    });

    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    let beats = server.await.unwrap();
    // First heartbeat right after HELLO, then one per interval.
    for pair in beats[1..].windows(2) {
        let gap = pair[1].0 - pair[0].0;
        assert!(
            gap >= Duration::from_millis(80) && gap <= Duration::from_millis(250),
            "interval gap {gap:?} in {beats:?}"
        );
    }
    // The sequence of the GUILD_CREATE (5) is echoed by later beats.
    assert_eq!(beats.last().unwrap().1, json!(5));
    assert!(handle.latency().is_some(), "ACKs update the latency");
    assert!(matches!(
        next_event(&mut rx).await,
        GatewayEvent::Dispatch(_)
    ));
    handle.close();
}

#[tokio::test]
async fn resumes_after_a_resumable_close_with_session_sequence_and_resume_url() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c1 = ServerConn::accept(&l, false).await;
        assert_eq!(c1.uri, "/?v=9&encoding=json");
        c1.hello(60_000).await;
        assert_eq!(c1.recv().await["op"], 2);
        c1.send(ready_payload(addr, "sess-r")).await;
        c1.send(json!({"op": 0, "t": "GUILD_CREATE", "s": 2, "d": {"id": "11"}}))
            .await;
        c1.close(4000).await;

        let mut c2 = ServerConn::accept(&l, false).await;
        assert_eq!(
            c2.uri, "/resume?v=9&encoding=json",
            "RESUME goes to resume_gateway_url"
        );
        c2.hello(60_000).await;
        let resume = c2.recv().await;
        c2.send(json!({"op": 0, "t": "RESUMED", "s": 3, "d": {}}))
            .await;
        resume
    });

    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    next_dispatch(&mut rx, "READY").await;
    next_dispatch(&mut rx, "GUILD_CREATE").await;
    match next_event(&mut rx).await {
        GatewayEvent::Disconnected { code, resume, .. } => {
            assert_eq!((code, resume), (Some(4000), true))
        }
        other => panic!("{other:?}"),
    }
    next_dispatch(&mut rx, "RESUMED").await;

    let resume = server.await.unwrap();
    assert_eq!(
        resume,
        json!({"op": 6, "d": {"seq": 2, "session_id": "sess-r", "token": FAKE_TOKEN}})
    );
    // The cache survives a RESUME.
    assert!(handle.cache().read().has_guild(11));
    handle.close();
}

#[tokio::test]
async fn op7_reconnect_resumes() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c1 = ServerConn::accept(&l, false).await;
        c1.hello(60_000).await;
        assert_eq!(c1.recv().await["op"], 2);
        c1.send(ready_payload(addr, "sess-7")).await;
        c1.send(json!({"op": 7, "d": null})).await;
        // The client closes with 4000.
        while c1.recv_opt().await.is_some() {}

        let mut c2 = ServerConn::accept(&l, false).await;
        c2.hello(60_000).await;
        c2.recv().await
    });
    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    next_dispatch(&mut rx, "READY").await;
    assert!(matches!(
        next_event(&mut rx).await,
        GatewayEvent::Disconnected { resume: true, .. }
    ));
    let first = server.await.unwrap();
    assert_eq!(first["op"], 6);
    assert_eq!(first["d"]["session_id"], "sess-7");
    handle.close();
}

#[tokio::test]
async fn invalid_session_false_starts_a_fresh_identify_on_the_default_url() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c1 = ServerConn::accept(&l, false).await;
        c1.hello(60_000).await;
        assert_eq!(c1.recv().await["op"], 2);
        c1.send(ready_payload(addr, "sess-dead")).await;
        c1.send(json!({"op": 9, "d": false})).await;
        while c1.recv_opt().await.is_some() {}

        let mut c2 = ServerConn::accept(&l, false).await;
        c2.hello(60_000).await;
        (c2.uri.clone(), c2.recv().await)
    });
    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    next_dispatch(&mut rx, "READY").await;
    assert!(matches!(
        next_event(&mut rx).await,
        GatewayEvent::Disconnected { resume: false, .. }
    ));
    let (uri, first) = server.await.unwrap();
    assert_eq!(uri, "/?v=9&encoding=json");
    assert_eq!(
        first["op"], 2,
        "session was invalidated: IDENTIFY, not RESUME"
    );
    handle.close();
}

#[tokio::test]
async fn silent_server_is_a_zombie_and_the_client_resumes() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c1 = ServerConn::accept(&l, false).await;
        c1.hello(60_000).await;
        assert_eq!(c1.recv().await["op"], 2);
        c1.send(ready_payload(addr, "sess-z")).await;
        // Go silent: the client must give up by itself.
        let started = Instant::now();
        while c1.recv_opt().await.is_some() {}
        let silent_for = started.elapsed();

        let mut c2 = ServerConn::accept(&l, false).await;
        c2.hello(60_000).await;
        (silent_for, c2.recv().await)
    });
    let mut cfg = config(addr, false);
    cfg.heartbeat_timeout = Duration::from_millis(400);
    let (handle, mut rx) = Gateway::spawn(cfg);
    next_dispatch(&mut rx, "READY").await;
    match next_event(&mut rx).await {
        GatewayEvent::Disconnected { code, resume, .. } => {
            assert_eq!((code, resume), (Some(4000), true))
        }
        other => panic!("{other:?}"),
    }
    let (silent_for, first) = server.await.unwrap();
    assert!(silent_for >= Duration::from_millis(300), "{silent_for:?}");
    assert_eq!(first["op"], 6);
    handle.close();
}

#[tokio::test]
async fn fatal_close_code_ends_the_stream_with_an_error() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c = ServerConn::accept(&l, false).await;
        c.hello(60_000).await;
        assert_eq!(c.recv().await["op"], 2);
        c.close(4004).await;
        // No further connection attempt must come.
        timeout(Duration::from_millis(500), l.accept())
            .await
            .is_err()
    });
    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    match next_event(&mut rx).await {
        GatewayEvent::Closed(Error::GatewayClosed { code, .. }) => assert_eq!(code, 4004),
        other => panic!("{other:?}"),
    }
    assert!(
        rx.recv().await.is_none(),
        "channel ends after the terminal event"
    );
    assert!(
        server.await.unwrap(),
        "authentication failure is not retried"
    );
    assert!(matches!(
        handle.voice_state(VoiceStateUpdate::default()).await,
        Err(Error::GatewayShutDown)
    ));
}

#[tokio::test]
async fn events_and_debug_output_never_contain_the_token() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c = ServerConn::accept(&l, false).await;
        c.hello(60_000).await;
        c.recv().await;
        c.send(ready_payload(addr, "sess-t")).await;
        c.close(4004).await;
        // Keep the socket until the client has seen the close frame.
        while c.recv_opt().await.is_some() {}
    });
    let cfg = config(addr, false);
    let mut shown = vec![format!("{cfg:?}")];
    let (handle, mut rx) = Gateway::spawn(cfg);
    shown.push(format!("{handle:?}"));
    loop {
        let ev = next_event(&mut rx).await;
        shown.push(format!("{ev:?}"));
        if matches!(ev, GatewayEvent::Closed(_)) {
            break;
        }
    }
    server.await.unwrap();
    assert_no_token_in_events(&shown);
}

#[tokio::test]
async fn dropping_every_handle_stops_the_task() {
    let (l, addr) = listener().await;
    let server = tokio::spawn(async move {
        let mut c = ServerConn::accept(&l, false).await;
        c.hello(60_000).await;
        c.recv().await;
        // The client closes 1000 once the handle is gone.
        let mut closed = false;
        while let Some(msg) = c.ws.next().await {
            if matches!(msg, Ok(Message::Close(_))) {
                closed = true;
                break;
            }
        }
        closed
    });
    let (handle, mut rx) = Gateway::spawn(config(addr, false));
    // Let the connection establish before dropping.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(handle);
    assert!(server.await.unwrap());
    assert!(timeout(WAIT, rx.recv()).await.unwrap().is_none());
}
