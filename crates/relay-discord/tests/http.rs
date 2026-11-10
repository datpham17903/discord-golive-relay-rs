use std::time::Duration;

use base64::Engine as _;
use relay_discord::{Error, GatewayConfig, HeadersContext, RestClient, Token, Urls};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FAKE_TOKEN: &str = "FAKE.token-for-tests.do-not-leak";

fn urls_for(server: &MockServer) -> Urls {
    let base = server.uri();
    Urls {
        origin: base.clone(),
        api_base: format!("{base}/api/v9"),
        gateway: base.replace("http://", "ws://"),
        cordapi: base.clone(),
        login_page: format!("{base}/login"),
        chrome_versions: format!("{base}/chrome/versions"),
    }
}

fn api_properties() -> Value {
    json!({
        "properties": {
            "os": "Windows",
            "browser": "Chrome",
            "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36",
            "browser_version": "158.0.0.0",
            "client_build_number": 667580,
            "client_launch_id": "launch-1",
            "client_heartbeat_session_id": "hb-1",
        },
        "extra_gateway_properties": {"is_fast_connect": false, "gateway_connect_reasons": "AppSkeleton"},
        "encoded": "ZW5jb2RlZC1wcm9wZXJ0aWVz",
    })
}

async fn mount_cordapi(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v2/properties/web"))
        .respond_with(ResponseTemplate::new(200).set_body_json(api_properties()))
        .mount(server)
        .await;
}

#[tokio::test]
async fn login_gets_users_me_with_browser_headers_and_authorization() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v9/users/@me"))
        .and(query_param("with_analytics_token", "true"))
        .and(header("authorization", FAKE_TOKEN))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"id": "123456789012345678", "username": "tester"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let rest = RestClient::new(urls_for(&server), Token::new(FAKE_TOKEN))
        .await
        .unwrap();
    assert_eq!(rest.headers_context().build_number(), Some(667580));
    let user = rest.login().await.unwrap();
    assert_eq!(user.id, 123456789012345678);
    assert_eq!(user.username.as_deref(), Some("tester"));

    let requests = server.received_requests().await.unwrap();
    let me = requests
        .iter()
        .find(|r| r.url.path() == "/api/v9/users/@me")
        .unwrap();
    let h = |name: &str| me.headers.get(name).map(|v| v.to_str().unwrap().to_owned());
    assert_eq!(
        h("x-super-properties").as_deref(),
        Some("ZW5jb2RlZC1wcm9wZXJ0aWVz"),
        "cordapi 'encoded' is used verbatim"
    );
    assert!(h("user-agent").unwrap().contains("Chrome/158.0.0.0"));
    assert_eq!(h("x-discord-locale").as_deref(), Some("en-US"));
    assert_eq!(h("origin").unwrap(), server.uri());
    assert_eq!(
        h("referer").unwrap(),
        format!("{}/channels/@me", server.uri())
    );
    assert_eq!(h("sec-fetch-site").as_deref(), Some("same-origin"));
    assert!(h("sec-ch-ua").unwrap().contains("v=\"158\""));
    assert_eq!(h("sec-ch-ua-platform").as_deref(), Some("\"Windows\""));
    assert_eq!(h("sec-ch-ua-mobile").as_deref(), Some("?0"));
}

#[tokio::test]
async fn login_401_is_login_failed_and_token_is_not_in_the_error() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v9/users/@me"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(json!({"message": "401: Unauthorized", "code": 0})),
        )
        .mount(&server)
        .await;

    let rest = RestClient::new(urls_for(&server), Token::new(FAKE_TOKEN))
        .await
        .unwrap();
    let err = rest.login().await.unwrap_err();
    assert!(matches!(err, Error::LoginFailed), "{err:?}");
    let shown = format!("{err} {err:?}");
    assert!(!shown.contains(FAKE_TOKEN));
}

#[tokio::test]
async fn other_statuses_carry_the_api_message() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v9/users/@me"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_json(json!({"message": "Forbidden", "code": 40002})),
        )
        .mount(&server)
        .await;

    let rest = RestClient::new(urls_for(&server), Token::new(FAKE_TOKEN))
        .await
        .unwrap();
    match rest.login().await.unwrap_err() {
        Error::Status { status, message } => {
            assert_eq!((status, message.as_str()), (403, "Forbidden"))
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn token_never_appears_in_debug_output() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    let token = Token::new(FAKE_TOKEN);
    let rest = RestClient::new(urls_for(&server), token.clone())
        .await
        .unwrap();
    let cfg = GatewayConfig::from_rest(&rest);

    for text in [
        format!("{token:?}"),
        format!("{token}"),
        format!("{rest:?}"),
        format!("{cfg:?}"),
        format!("{:#?}", rest.headers_context()),
    ] {
        assert!(!text.contains(FAKE_TOKEN), "{text}");
    }
    assert_eq!(format!("{token:?}"), "Token(***)");
    assert_eq!(token.to_string(), "***");
    assert_eq!(token.expose(), FAKE_TOKEN);
}

#[tokio::test]
async fn context_falls_back_to_scraped_build_number_and_chrome_version() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/properties/web"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<script>window.GLOBAL_ENV = {"BUILD_NUMBER": "424242", "X": 1}</script>"#,
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/chrome/versions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"versions": [{"version": "150.0.7000.1"}]})),
        )
        .mount(&server)
        .await;

    let http = wreq::Client::new();
    let ctx = HeadersContext::fetch_default(&http, &urls_for(&server)).await;
    assert_eq!(ctx.build_number(), Some(424242));
    assert_eq!(ctx.browser_major_version, 150);
    assert!(ctx.user_agent().contains("Chrome/150.0.0.0 Safari/537.36"));
    assert_eq!(ctx.super_properties["browser_version"], "150.0.0.0");
    assert_eq!(ctx.super_properties["client_event_source"], Value::Null);
    assert_eq!(
        ctx.extra_gateway_properties["gateway_connect_reasons"],
        "AppSkeleton"
    );
    // Without 'encoded' the header is base64 of the compact JSON.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&ctx.encoded_super_properties)
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&decoded).unwrap(),
        Value::Object(ctx.super_properties.clone())
    );
}

#[tokio::test]
async fn context_uses_hardcoded_constants_when_everything_is_down() {
    let server = MockServer::start().await; // every path 404s
    let ctx = HeadersContext::fetch_default(&wreq::Client::new(), &urls_for(&server)).await;
    assert_eq!(ctx.build_number(), Some(9999));
    assert!(ctx.browser_major_version >= 136);
    assert!(
        ctx.user_agent()
            .contains(&format!("Chrome/{}.0.0.0", ctx.browser_major_version))
    );
}

#[tokio::test]
async fn slow_info_api_times_out_and_falls_back() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/properties/web"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(api_properties())
                .set_delay(Duration::from_secs(2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#""BUILD_NUMBER":"31337""#))
        .mount(&server)
        .await;

    let ctx = HeadersContext::fetch_default_with_timeout(
        &wreq::Client::new(),
        &urls_for(&server),
        Duration::from_millis(200),
    )
    .await;
    assert_eq!(ctx.build_number(), Some(31337));
}

#[tokio::test]
async fn info_api_202_means_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/properties/web"))
        .respond_with(ResponseTemplate::new(202).set_body_json(api_properties()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#""BUILD_NUMBER":"555""#))
        .mount(&server)
        .await;
    let ctx = HeadersContext::fetch_default(&wreq::Client::new(), &urls_for(&server)).await;
    assert_eq!(ctx.build_number(), Some(555));
}

#[tokio::test]
async fn reply_never_mentions_anyone() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v9/channels/60/messages"))
        .and(header("authorization", FAKE_TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "1"})))
        .expect(1)
        .mount(&server)
        .await;

    let rest = RestClient::new(urls_for(&server), Token::new(FAKE_TOKEN))
        .await
        .unwrap();
    rest.reply(Some(1), 60, 900, "ok @everyone <@5>")
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let sent = requests
        .iter()
        .find(|r| r.url.path() == "/api/v9/channels/60/messages")
        .unwrap();
    let body: Value = serde_json::from_slice(&sent.body).unwrap();
    assert_eq!(body["content"], "ok @everyone <@5>");
    assert_eq!(
        body["allowed_mentions"],
        json!({"parse": [], "replied_user": false}),
        "no author mention, no parsed mentions"
    );
    assert_eq!(
        body["message_reference"],
        json!({"message_id": "900", "channel_id": "60", "guild_id": "1", "fail_if_not_exists": true})
    );
    assert_eq!(body["tts"], false);
    assert!(body["nonce"].as_str().is_some_and(|n| !n.is_empty()));
    assert_eq!(
        sent.headers.get("x-context-properties").unwrap(),
        "eyJsb2NhdGlvbiI6IkNoYXQgSW5wdXQifQ=="
    );
}

#[tokio::test]
async fn reply_failure_carries_the_api_message() {
    let server = MockServer::start().await;
    mount_cordapi(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v9/channels/60/messages"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_json(json!({"message": "Missing Permissions", "code": 50013})),
        )
        .mount(&server)
        .await;
    let rest = RestClient::new(urls_for(&server), Token::new(FAKE_TOKEN))
        .await
        .unwrap();
    match rest.reply(None, 60, 900, "x").await.unwrap_err() {
        Error::Status { status, message } => {
            assert_eq!((status, message.as_str()), (403, "Missing Permissions"))
        }
        other => panic!("{other:?}"),
    }
}
