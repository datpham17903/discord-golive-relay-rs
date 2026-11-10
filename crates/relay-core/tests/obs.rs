//! Port of `tests/test_obs.py` (control-server round-trip tests belong to the control socket task).

use std::sync::{Arc, Mutex};

use relay_core::obs::{JsonLogger, Metrics, write_status};
use serde_json::{Value, json};

fn buffer() -> Arc<Mutex<Vec<u8>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn contents(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8(buf.lock().unwrap().clone()).unwrap()
}

#[test]
fn test_logger_redacts_secrets_everywhere() {
    let buf = buffer();
    let log = JsonLogger::new(buf.clone(), ["s3cr3t-token"]);
    log.event(
        "login",
        [
            ("detail", json!("bad s3cr3t-token here")),
            ("nested", json!({"a": ["x s3cr3t-token"]})),
            ("token", json!("whatever")),
        ],
    );
    let line = contents(&buf);
    assert!(!line.contains("s3cr3t-token") && !line.contains("whatever"));
    let rec: Value = serde_json::from_str(&line).unwrap();
    assert!(rec["event"] == "login" && rec.get("ts").is_some());
    assert_eq!(rec["token"], "***");
}

#[test]
fn test_logger_ignores_empty_secret() {
    // Python also passes `None`; Rust's `AsRef<str>` items cannot be `None`, so only "" is exercised.
    let buf = buffer();
    JsonLogger::new(buf.clone(), [""]).event("x", [("a", json!("abc"))]);
    let rec: Value = serde_json::from_str(&contents(&buf)).unwrap();
    assert_eq!(rec["a"], "abc");
}

#[test]
fn test_metrics_snapshot() {
    let mut m = Metrics::new();
    m.inc("retries", 1);
    m.inc("retries", 2);
    m.gauge_int("queue", 5);
    m.timing("encode_ms", 10.0);
    m.timing("encode_ms", 30.0);
    let snap = m.snapshot();
    assert_eq!(snap["counters"]["retries"], 3);
    assert_eq!(snap["gauges"]["queue"], 5);
    let t = &snap["timings"]["encode_ms"];
    assert!(t["count"] == 2 && t["max"] == 30.0 && t["avg"] == 20.0);
}

#[test]
fn test_write_status_atomic() {
    let dir = tempfile::tempdir().unwrap();
    write_status(dir.path(), &json!({"state": "idle"})).unwrap();
    let text = std::fs::read_to_string(dir.path().join("status.json")).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["state"],
        "idle"
    );
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn logger_line_is_compact_ascii_and_ordered() {
    let buf = buffer();
    let log = JsonLogger::new(buf.clone(), ["sek"]);
    log.event(
        "e",
        [
            ("zeta", json!("sek caf\u{e9} \u{1F600}")),
            ("alpha", json!([1, 2.5, null, true])),
            ("Auth_Token", json!({"x": 1})),
            ("b", json!({"My-Password": "p", "ok": "sek"})),
        ],
    );
    let line = contents(&buf);
    assert!(line.ends_with('\n') && line.is_ascii());
    let body = line.trim_end();
    let (head, rest) = body.split_once(r#","event":"e","#).unwrap();
    assert!(head.starts_with(r#"{"ts":"#));
    assert_eq!(
        rest,
        r#""zeta":"*** caf\u00e9 \ud83d\ude00","alpha":[1,2.5,null,true],"Auth_Token":"***","b":{"My-Password":"***","ok":"***"}}"#
    );
}

#[test]
fn logger_field_named_event_replaces_value_in_place() {
    // Python: {"ts": .., "event": name, **fields} keeps position and takes the later value.
    let buf = buffer();
    JsonLogger::new(buf.clone(), [] as [&str; 0]).event("e", [("event", json!("other"))]);
    let line = contents(&buf);
    assert!(line.contains(r#","event":"other"}"#), "{line}");
}

#[test]
fn write_status_matches_python_indent_2_ascii() {
    let dir = tempfile::tempdir().unwrap();
    write_status(
        dir.path(),
        &json!({"state": "idle", "n": [], "o": {}, "l": [1, {"a": 1.0}], "name": "Tr\u{1ea7}n"}),
    )
    .unwrap();
    let text = std::fs::read_to_string(dir.path().join("status.json")).unwrap();
    assert_eq!(
        text,
        "{\n  \"state\": \"idle\",\n  \"n\": [],\n  \"o\": {},\n  \"l\": [\n    1,\n    {\n      \"a\": 1.0\n    }\n  ],\n  \"name\": \"Tr\\u1ea7n\"\n}"
    );
}

#[test]
fn write_status_creates_directory_and_replaces_existing() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("a").join("b");
    write_status(&sub, &json!({"v": 1})).unwrap();
    write_status(&sub, &json!({"v": 2})).unwrap();
    let text = std::fs::read_to_string(sub.join("status.json")).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["v"], 2);
}

#[test]
fn metrics_snapshot_preserves_insertion_order_and_int_gauges() {
    let mut m = Metrics::new();
    m.inc("b", 1);
    m.inc("a", 1);
    m.gauge("g", 2.5);
    m.gauge_int("h", 0);
    m.timing("t", 4.0);
    m.timing("t", 1.0);
    let snap = m.snapshot();
    let keys: Vec<_> = snap["counters"].as_object().unwrap().keys().collect();
    assert_eq!(keys, ["b", "a"]);
    assert_eq!(snap["gauges"]["h"].to_string(), "0");
    let t = &snap["timings"]["t"];
    assert_eq!(
        (
            t["count"].clone(),
            t["avg"].clone(),
            t["max"].clone(),
            t["last"].clone()
        ),
        (json!(2), json!(2.5), json!(4.0), json!(1.0))
    );
}
