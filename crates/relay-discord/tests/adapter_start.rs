//! The adapters end to end up to the voice layer: REST login against wiremock, the gateway against a
//! fake server (tokio-tungstenite on 127.0.0.1), events out of the pump. Never touches Discord.

// The handshake callback's error type is fixed by tokio-tungstenite.
#![allow(clippy::result_large_err)]

use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay_core::config::ControlConfig;
use relay_core::supervisor::model::{Event, GatewayState};
use relay_core::supervisor::outputs_model::PoolEvent;
use relay_discord::{
    Adapter, AdapterError, AdapterEvent, AdapterOptions, DiscordAdapter, OutputAccountAdapter,
    OutputAdapter, OutputOptions, Token, Urls,
};
use relay_media::codec::VoiceCodec;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FAKE_TOKEN: &str = "FAKE.token-for-tests.do-not-leak";
const WAIT: Duration = Duration::from_secs(10);
const GUILD: u64 = 1;
const CHAN: u64 = 2;
const TARGET: u64 = 3;
const RELAY: u64 = 4;
const OUT: u64 = 7;

// ---------------------------------------------------------------------------------------------
// fakes

async fn rest_server(user_id: &str, status: u16) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/properties/web"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "properties": {
                "os": "Windows", "browser": "Chrome",
                "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36",
                "browser_version": "158.0.0.0", "client_build_number": 667580,
                "client_launch_id": "launch-1", "client_heartbeat_session_id": "hb-1",
            },
            "extra_gateway_properties": {"is_fast_connect": false, "gateway_connect_reasons": "AppSkeleton"},
            "encoded": "ZW5jb2RlZA==",
        })))
        .mount(&server)
        .await;
    let body = if status == 200 {
        json!({"id": user_id, "username": "tester"})
    } else {
        json!({"message": "401: Unauthorized", "code": 0})
    };
    Mock::given(method("GET"))
        .and(path("/api/v9/users/@me"))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(&server)
        .await;
    server
}

fn urls(server: &MockServer, gateway: &str) -> Urls {
    let base = server.uri();
    Urls {
        origin: base.clone(),
        api_base: format!("{base}/api/v9"),
        gateway: gateway.to_owned(),
        cordapi: base.clone(),
        login_page: format!("{base}/login"),
        chrome_versions: format!("{base}/chrome/versions"),
    }
}

struct Gateway {
    ws: WebSocketStream<TcpStream>,
    enc: zstd::stream::write::Encoder<'static, Vec<u8>>,
}

impl Gateway {
    async fn bind() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    async fn accept(listener: &TcpListener) -> Self {
        let (stream, _) = timeout(WAIT, listener.accept())
            .await
            .expect("the gateway connects")
            .unwrap();
        Self {
            ws: tokio_tungstenite::accept_async(stream).await.unwrap(),
            enc: zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap(),
        }
    }

    /// One compressed frame (the encoder keeps its context across messages, like the real stream).
    async fn send(&mut self, payload: Value) {
        self.enc.write_all(payload.to_string().as_bytes()).unwrap();
        self.enc.flush().unwrap();
        let frame = std::mem::take(self.enc.get_mut());
        self.ws.send(Message::binary(frame)).await.unwrap();
    }

    async fn dispatch(&mut self, seq: u64, name: &str, data: Value) {
        self.send(json!({"op": 0, "s": seq, "t": name, "d": data}))
            .await;
    }

    /// HELLO, then waits for the client's IDENTIFY.
    async fn handshake(&mut self) {
        self.send(json!({"op": 10, "d": {"heartbeat_interval": 41250}}))
            .await;
        loop {
            let msg = timeout(WAIT, self.ws.next())
                .await
                .expect("a message from the client")
                .expect("socket open")
                .unwrap();
            if let Message::Text(text) = msg {
                let v: Value = serde_json::from_str(text.as_str()).unwrap();
                if v["op"] == 2 {
                    let shown = text.to_string();
                    assert!(shown.contains(FAKE_TOKEN), "IDENTIFY carries the token");
                    return;
                }
            }
        }
    }

    async fn ready(&mut self, user_id: u64) {
        self.dispatch(
            1,
            "READY",
            json!({
                "user": {"id": user_id.to_string()}, "session_id": "sess-1",
                "resume_gateway_url": "ws://127.0.0.1:1", "geo_ordered_rtc_regions": ["us-central"],
                "guilds": [{"id": GUILD.to_string(), "channels": [{"id": CHAN.to_string(), "type": 2}]}],
            }),
        )
        .await;
        self.dispatch(
            2,
            "READY_SUPPLEMENTAL",
            json!({"guilds": [{"voice_states": [
                {"user_id": TARGET.to_string(), "channel_id": CHAN.to_string(), "self_stream": true}
            ]}]}),
        )
        .await;
    }

    async fn close(&mut self, code: u16) {
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: "".into(),
        };
        let _ = self.ws.send(Message::Close(Some(frame))).await;
    }
}

fn ingest_options(urls: Urls) -> AdapterOptions {
    AdapterOptions {
        guild_id: GUILD,
        target_id: TARGET,
        channel_id: CHAN,
        relay_id: RELAY,
        output_account_ids: vec![OUT],
        control: ControlConfig {
            chat_commands: true,
            ..ControlConfig::default()
        },
        paused_verified: false,
        adaptive: true,
        parent_codecs: vec![VoiceCodec::H264],
        watch_codecs: vec![VoiceCodec::H264],
        network_stats: false,
        self_deaf: true,
        urls,
        connect_timeout: Duration::from_secs(1),
    }
}

fn output_options(urls: Urls) -> OutputOptions {
    OutputOptions {
        user_id: OUT,
        guild_id: GUILD,
        channel_id: CHAN,
        parent_codecs: vec![VoiceCodec::H264],
        adaptive: true,
        network_stats: false,
        excluded_viewer_ids: Default::default(),
        bot_user_ids: [RELAY, OUT].into(),
        self_deaf: true,
        urls,
        connect_timeout: Duration::from_secs(1),
        health_poll: Duration::from_millis(50),
    }
}

type Seen<T> = Arc<Mutex<Vec<T>>>;

fn ingest(urls: Urls) -> (DiscordAdapter, Seen<AdapterEvent>, Seen<(String, Value)>) {
    let events: Seen<AdapterEvent> = Arc::default();
    let logs: Seen<(String, Value)> = Arc::default();
    let (e, l) = (Arc::clone(&events), Arc::clone(&logs));
    let adapter = DiscordAdapter::new(
        ingest_options(urls),
        Arc::new(move |ev| e.lock().unwrap().push(ev)),
        Arc::new(|_| {}),
        Arc::new(move |name, fields| l.lock().unwrap().push((name.to_owned(), fields))),
    );
    (adapter, events, logs)
}

async fn until<T: Clone>(seen: &Seen<T>, what: &str, done: impl Fn(&[T]) -> bool) -> Vec<T> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let snapshot = seen.lock().unwrap().clone();
        if done(&snapshot) {
            return snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// the ingest adapter

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_token_is_auth_rejected_not_an_error() {
    let rest = rest_server("0", 401).await;
    let (adapter, events, _) = ingest(urls(&rest, "ws://127.0.0.1:1"));
    adapter.start(Token::new(FAKE_TOKEN)).await.unwrap();
    assert!(matches!(
        events.lock().unwrap().as_slice(),
        [AdapterEvent::Model(Event::AuthRejected)]
    ));
    adapter.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_events_flow_through_the_pump_in_order() {
    let rest = rest_server(&RELAY.to_string(), 200).await;
    let (listener, url) = Gateway::bind().await;
    let (adapter, events, logs) = ingest(urls(&rest, &url));
    adapter.start(Token::new(FAKE_TOKEN)).await.unwrap();

    let mut gw = Gateway::accept(&listener).await;
    gw.handshake().await;
    gw.ready(RELAY).await;
    let seen = until(&events, "the READY snapshot", |e| !e.is_empty()).await;
    let AdapterEvent::Model(Event::GatewaySnapshot {
        target,
        stream,
        participants,
        new_session,
    }) = &seen[0]
    else {
        panic!("{:?}", seen[0])
    };
    assert!(new_session);
    assert!(target.is_some_and(|t| t.self_stream && t.channel_id == Some(CHAN)));
    assert_eq!(
        stream.as_ref().map(|s| s.key.as_str()),
        Some("guild:1:2:3"),
        "synthesized from the target's voice state"
    );
    assert_eq!(participants.as_ref().unwrap(), &[TARGET].into());
    assert_eq!(adapter.display_name(TARGET), None);

    // a chat command through the whole pipeline
    gw.dispatch(
        3,
        "MESSAGE_CREATE",
        json!({"id": "900", "channel_id": "60", "guild_id": GUILD.to_string(),
               "author": {"id": "50", "username": "op"}, "content": "!relay status",
               "member": {"roles": ["700"], "nick": "Op"}}),
    )
    .await;
    let seen = until(&events, "the chat command", |e| e.len() >= 2).await;
    let AdapterEvent::Chat(chat) = &seen[1] else {
        panic!("{:?}", seen[1])
    };
    assert_eq!(
        (chat.author_id, chat.message_id, chat.text.as_str()),
        (50, 900, "!relay status")
    );
    assert_eq!(chat.author_role_ids, [700].into());
    assert_eq!(
        adapter.display_name(50).as_deref(),
        Some("Op"),
        "the author is cached"
    );

    // a dropped socket is RESUMING; the gateway log never carries the token
    gw.close(4000).await;
    let seen = until(&events, "RESUMING", |e| e.len() >= 3).await;
    assert!(matches!(
        &seen[2],
        AdapterEvent::Model(Event::GatewayChanged {
            state: GatewayState::Resuming
        })
    ));
    assert!(!format!("{:?}{:?}", seen, logs.lock().unwrap()).contains(FAKE_TOKEN));
    adapter.close().await;
}

// ---------------------------------------------------------------------------------------------
// the output account

fn output(urls: Urls) -> (OutputAccountAdapter, Seen<PoolEvent>) {
    let events: Seen<PoolEvent> = Arc::default();
    let e = Arc::clone(&events);
    let adapter = OutputAccountAdapter::new(
        output_options(urls),
        Arc::new(move |ev| e.lock().unwrap().push(ev)),
        Arc::new(|_, _| {}),
    );
    (adapter, events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_login_failure_emits_out_auth_rejected() {
    let rest = rest_server("0", 401).await;
    let (adapter, events) = output(urls(&rest, "ws://127.0.0.1:1"));
    adapter.start(Token::new(FAKE_TOKEN)).await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![PoolEvent::OutAuthRejected { account: OUT }]
    );
    adapter.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_start_waits_for_ready_and_rejects_another_account() {
    // the right account: start returns once READY is in
    let rest = rest_server(&OUT.to_string(), 200).await;
    let (listener, url) = Gateway::bind().await;
    let (adapter, events) = output(urls(&rest, &url));
    let started = tokio::spawn(async move {
        let result = adapter.start(Token::new(FAKE_TOKEN)).await;
        (adapter, result)
    });
    let mut gw = Gateway::accept(&listener).await;
    gw.handshake().await;
    assert!(!started.is_finished(), "no READY yet");
    gw.ready(OUT).await;
    let (adapter, result) = timeout(WAIT, started).await.unwrap().unwrap();
    result.unwrap();
    assert!(events.lock().unwrap().is_empty());
    adapter.close().await;

    // a token of another account: an error, and nothing is reported
    let rest = rest_server(&OUT.to_string(), 200).await;
    let (listener, url) = Gateway::bind().await;
    let (adapter, events) = output(urls(&rest, &url));
    let started = tokio::spawn(async move { adapter.start(Token::new(FAKE_TOKEN)).await });
    let mut gw = Gateway::accept(&listener).await;
    gw.handshake().await;
    gw.ready(99).await;
    let result = timeout(WAIT, started).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(AdapterError::AccountMismatch)),
        "{result:?}"
    );
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_start_fails_when_the_gateway_ends_before_ready() {
    let rest = rest_server(&OUT.to_string(), 200).await;
    let (listener, url) = Gateway::bind().await;
    let (adapter, _events) = output(urls(&rest, &url));
    let started = tokio::spawn(async move { adapter.start(Token::new(FAKE_TOKEN)).await });
    let mut gw = Gateway::accept(&listener).await;
    gw.handshake().await;
    gw.close(4004).await; // authentication failed: the gateway does not retry
    let result = timeout(WAIT, started).await.unwrap().unwrap();
    let err = result.expect_err("no READY");
    assert!(!err.to_string().contains(FAKE_TOKEN));
}
