//! `relayctl run --metrics-listen ADDR`: `GET /healthz` and `GET /metrics` (Prometheus text format
//! 0.0.4) over plain HTTP/1.1, one request per connection.
//!
//! Both read the last `status.json` the service wrote (every second), never a fresh status: building
//! one takes the per-interval send windows that `status.json` reports. Nothing beyond what
//! `status.json` holds is exposed, and of its IDs only the output account user IDs (as labels).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// `/healthz` turns 503 once `status.json` is older than this.
pub const HEALTH_MAX_AGE_S: f64 = 15.0;
const MAX_REQUEST_HEAD: usize = 8 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PROMETHEUS_TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";
const PLAIN_TEXT: &str = "text/plain; charset=utf-8";

/// What the two endpoints answer.
pub struct Endpoints {
    pub healthy: Box<dyn Fn() -> bool + Send + Sync>,
    pub metrics: Box<dyn Fn() -> String + Send + Sync>,
}

/// Accept connections until the task is aborted.
pub async fn serve(listener: TcpListener, endpoints: Arc<Endpoints>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let endpoints = Arc::clone(&endpoints);
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(REQUEST_TIMEOUT, answer(stream, &endpoints)).await;
                });
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn answer(mut stream: TcpStream, endpoints: &Endpoints) -> std::io::Result<()> {
    // The whole head is read before answering: closing with unread input would reset the connection.
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && !head.windows(2).any(|w| w == b"\n\n") {
        if head.len() > MAX_REQUEST_HEAD {
            return reply(
                &mut stream,
                431,
                "Request Header Fields Too Large",
                PLAIN_TEXT,
                "",
            )
            .await;
        }
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let line = head.split(|&b| b == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return reply(&mut stream, 400, "Bad Request", PLAIN_TEXT, "bad request\n").await;
    };
    let path = target.split('?').next().unwrap_or_default();
    match (method, path) {
        ("GET", "/healthz") if (endpoints.healthy)() => {
            reply(&mut stream, 200, "OK", PLAIN_TEXT, "ok").await
        }
        ("GET", "/healthz") => {
            reply(
                &mut stream,
                503,
                "Service Unavailable",
                PLAIN_TEXT,
                "unavailable",
            )
            .await
        }
        ("GET", "/metrics") => {
            let body = (endpoints.metrics)();
            reply(&mut stream, 200, "OK", PROMETHEUS_TEXT, &body).await
        }
        ("GET", _) => reply(&mut stream, 404, "Not Found", PLAIN_TEXT, "not found\n").await,
        _ => {
            reply(
                &mut stream,
                405,
                "Method Not Allowed",
                PLAIN_TEXT,
                "method not allowed\n",
            )
            .await
        }
    }
}

async fn reply(
    stream: &mut TcpStream,
    code: u16,
    reason: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

// ------------------------------------------------------------------ text format

struct Text(String);

impl Text {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: &str) {
        self.0.push_str(name);
        if !labels.is_empty() {
            let inner: Vec<String> = labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
                .collect();
            let _ = write!(self.0, "{{{}}}", inner.join(","));
        }
        let _ = writeln!(self.0, " {value}");
    }

    /// One unlabelled sample, skipped when `value` is not a number or a bool.
    fn single(&mut self, name: &str, kind: &str, help: &str, value: Option<&Value>) {
        if let Some(v) = value.and_then(number) {
            self.family(name, kind, help);
            self.sample(name, &[], &v);
        }
    }
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// A sample value: numbers as written in status.json, booleans as 1/0, anything else skipped.
fn number(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        _ => None,
    }
}

fn flag(set: bool) -> Value {
    Value::from(u8::from(set))
}

/// Numeric fields of `obj`, in order.
fn numeric_fields(obj: Option<&Value>) -> Vec<(String, String)> {
    obj.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| number(v).map(|n| (k.clone(), n)))
                .collect()
        })
        .unwrap_or_default()
}

/// Per-account fields that are counts of the last status interval (the send window).
const ACCOUNT_SEND_FIELDS: [&str; 7] = [
    "sent",
    "stalls",
    "max_send_gap_ms",
    "audio_played",
    "audio_held_reads",
    "audio_late_dropped",
    "audio_evicted",
];

/// Prometheus text for the last `status.json` (`None` before the first write), `age_s` seconds old.
pub fn render(status: Option<&Value>, age_s: Option<f64>, up: bool) -> String {
    let mut t = Text(String::new());
    t.single(
        "relay_up",
        "gauge",
        "1 while the service runs and status.json is fresh.",
        Some(&flag(up)),
    );
    t.single(
        "relay_status_age_seconds",
        "gauge",
        "Seconds since status.json was last written.",
        age_s.map(Value::from).as_ref(),
    );
    let Some(d) = status else {
        return t.0;
    };
    if let Some(state) = d.get("state").and_then(Value::as_str) {
        t.family(
            "relay_state",
            "gauge",
            "Session state (the series with value 1).",
        );
        t.sample("relay_state", &[("state", state)], "1");
    }
    t.single(
        "relay_desired_enabled",
        "gauge",
        "1 when relaying is enabled.",
        d.get("desired_enabled"),
    );
    t.single(
        "relay_fatal",
        "gauge",
        "1 while the ingest account's FATAL latch is set.",
        Some(&flag(d.get("fatal").is_some_and(|f| !f.is_null()))),
    );
    t.single(
        "relay_generation",
        "gauge",
        "Session generation.",
        d.get("generation"),
    );
    t.single(
        "relay_epoch",
        "gauge",
        "Pipeline epoch of the session.",
        d.get("epoch"),
    );
    t.single(
        "relay_adapter_running",
        "gauge",
        "1 while the ingest adapter runs.",
        d.get("adapter_running"),
    );
    t.single(
        "relay_consecutive_rejections",
        "gauge",
        "Consecutive server rejections of the ingest account.",
        d.get("consecutive_rejections"),
    );
    t.single(
        "relay_viewers",
        "gauge",
        "Main-channel members other than the target and the relay accounts.",
        d.pointer("/details/viewers"),
    );

    let counters = numeric_fields(d.pointer("/metrics/counters"));
    if !counters.is_empty() {
        t.family(
            "relay_counter_total",
            "counter",
            "Service counters of status.json metrics.counters, by name.",
        );
        for (name, v) in &counters {
            t.sample("relay_counter_total", &[("name", name)], v);
        }
    }
    let gauges = numeric_fields(d.pointer("/metrics/gauges"));
    if !gauges.is_empty() {
        t.family(
            "relay_gauge",
            "gauge",
            "Service gauges of status.json metrics.gauges (worker.* included), by name.",
        );
        for (name, v) in &gauges {
            t.sample("relay_gauge", &[("name", name)], v);
        }
    }
    let ingest = numeric_fields(d.get("ingest_send"));
    if !ingest.is_empty() {
        t.family(
            "relay_ingest_send",
            "gauge",
            "Ingest account sends during the last status interval, by stat.",
        );
        for (stat, v) in &ingest {
            t.sample("relay_ingest_send", &[("stat", stat)], v);
        }
    }

    let accounts: Vec<(String, &Value)> = d
        .get("accounts")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| Some((number(r.get("account")?)?, r)))
                .collect()
        })
        .unwrap_or_default();
    if accounts.is_empty() {
        return t.0;
    }
    t.family(
        "relay_account_state",
        "gauge",
        "Output account state (the series with value 1).",
    );
    for (uid, r) in &accounts {
        if let Some(state) = r.get("state").and_then(Value::as_str) {
            t.sample(
                "relay_account_state",
                &[("account", uid), ("state", state)],
                "1",
            );
        }
    }
    t.family(
        "relay_account_fatal",
        "gauge",
        "1 while the output account's FATAL latch is set.",
    );
    for (uid, r) in &accounts {
        let fatal = r.get("fatal").is_some_and(|f| !f.is_null());
        t.sample(
            "relay_account_fatal",
            &[("account", uid)],
            if fatal { "1" } else { "0" },
        );
    }
    let per_account = [
        (
            "relay_account_viewers",
            "Viewers of the output account's Go Live.",
            "viewers",
        ),
        (
            "relay_account_consecutive_rejections",
            "Consecutive server rejections of the output account.",
            "consecutive_rejections",
        ),
    ];
    for (name, help, field) in per_account {
        let rows: Vec<(&str, String)> = accounts
            .iter()
            .filter_map(|(uid, r)| Some((uid.as_str(), number(r.get(field)?)?)))
            .collect();
        if rows.is_empty() {
            continue;
        }
        t.family(name, "gauge", help);
        for (uid, v) in rows {
            t.sample(name, &[("account", uid)], &v);
        }
    }
    let sends: Vec<(&str, &str, String)> = accounts
        .iter()
        .flat_map(|(uid, r)| {
            ACCOUNT_SEND_FIELDS
                .iter()
                .filter_map(move |f| Some((uid.as_str(), *f, number(r.get(*f)?)?)))
        })
        .collect();
    if !sends.is_empty() {
        t.family(
            "relay_account_send",
            "gauge",
            "Output account sends during the last status interval, by stat.",
        );
        for (uid, stat, v) in sends {
            t.sample(
                "relay_account_send",
                &[("account", uid), ("stat", stat)],
                &v,
            );
        }
    }
    t.0
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn status() -> Value {
        json!({
            "state": "relaying",
            "desired_enabled": true,
            "fatal": null,
            "generation": 7,
            "epoch": 2,
            "adapter_running": true,
            "consecutive_rejections": 0,
            "metrics": {
                "counters": {"action.open_gate": 3, "worker.need_keyframe": 1},
                "gauges": {"worker.frames_out": 1200, "worker.jitter_buffer_ms": 412.5,
                           "label\"odd": 1},
                "timings": {}
            },
            "ingest_send": {"sent": 60, "stalls": 1, "max_send_gap_ms": 70.25, "codec": "H264"},
            "routing": {"target_user_id": "999", "channel_id": "555"},
            "accounts": [
                {"account": 424242, "state": "live", "fatal": null, "viewers": 2,
                 "consecutive_rejections": 0, "channel_id": 555, "sent": 59, "stalls": 0},
                {"account": 434343, "state": "idle", "fatal": "RELAY_REJECTED_BY_SERVER",
                 "viewers": null, "consecutive_rejections": 3, "channel_id": 556}
            ],
            "details": {"viewers": 4}
        })
    }

    #[test]
    fn renders_status_json_as_prometheus_text() {
        let text = render(Some(&status()), Some(0.5), true);
        for line in [
            "relay_up 1",
            "relay_status_age_seconds 0.5",
            "relay_state{state=\"relaying\"} 1",
            "relay_desired_enabled 1",
            "relay_fatal 0",
            "relay_generation 7",
            "relay_epoch 2",
            "relay_viewers 4",
            "relay_counter_total{name=\"action.open_gate\"} 3",
            "relay_gauge{name=\"worker.frames_out\"} 1200",
            "relay_gauge{name=\"worker.jitter_buffer_ms\"} 412.5",
            "relay_gauge{name=\"label\\\"odd\"} 1",
            "relay_ingest_send{stat=\"max_send_gap_ms\"} 70.25",
            "relay_account_state{account=\"424242\",state=\"live\"} 1",
            "relay_account_state{account=\"434343\",state=\"idle\"} 1",
            "relay_account_fatal{account=\"434343\"} 1",
            "relay_account_viewers{account=\"424242\"} 2",
            "relay_account_consecutive_rejections{account=\"434343\"} 3",
            "relay_account_send{account=\"424242\",stat=\"sent\"} 59",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line}:\n{text}");
        }
        // strings are not samples; channel and target IDs never appear
        for absent in ["codec", "555", "556", "999"] {
            assert!(!text.contains(absent), "{absent}:\n{text}");
        }
        assert!(!text.contains("relay_account_viewers{account=\"434343\"}"));
    }

    #[test]
    fn every_family_is_declared_once_and_contiguous() {
        let text = render(Some(&status()), Some(0.5), true);
        let mut seen = Vec::new();
        let mut current = String::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(!seen.contains(&name), "{name} declared twice");
                seen.push(name.clone());
                current = name;
            } else if !line.starts_with('#') {
                let name = line.split(['{', ' ']).next().unwrap();
                assert_eq!(name, current, "{line} outside its family");
            }
        }
    }

    #[test]
    fn before_the_first_status_only_up_is_reported() {
        assert_eq!(
            render(None, None, false),
            "# HELP relay_up 1 while the service runs and status.json is fresh.\n\
             # TYPE relay_up gauge\nrelay_up 0\n"
        );
    }

    async fn get(addr: std::net::SocketAddr, request: &str) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(request.as_bytes()).await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn http_endpoints() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let h = Arc::clone(&healthy);
        let task = tokio::spawn(serve(
            listener,
            Arc::new(Endpoints {
                healthy: Box::new(move || h.load(std::sync::atomic::Ordering::SeqCst)),
                metrics: Box::new(|| "relay_up 1\n".to_string()),
            }),
        ));
        let ok = get(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(ok.ends_with("\r\n\r\nok"), "{ok}");
        healthy.store(false, std::sync::atomic::Ordering::SeqCst);
        let down = get(addr, "GET /healthz HTTP/1.0\r\n\r\n").await;
        assert!(down.starts_with("HTTP/1.1 503 "), "{down}");
        let m = get(addr, "GET /metrics?x=1 HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(m.starts_with("HTTP/1.1 200 OK\r\n"), "{m}");
        assert!(
            m.contains(&format!("Content-Type: {PROMETHEUS_TEXT}\r\n")),
            "{m}"
        );
        assert!(
            m.contains("Content-Length: 11\r\n") && m.ends_with("relay_up 1\n"),
            "{m}"
        );
        assert!(
            get(addr, "GET / HTTP/1.1\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 404 ")
        );
        assert!(
            get(addr, "POST /metrics HTTP/1.1\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 405 ")
        );
        assert!(
            get(addr, "garbage\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 400 ")
        );
        task.abort();
    }
}
