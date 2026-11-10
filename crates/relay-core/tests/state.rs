//! Port of `tests/control/test_state.py`. `account_view_and_set_account_roundtrip` checks the
//! `FatalReason` half of Python's `account_view` (`AccountPersisted` itself belongs to Task C6).

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Barrier, Mutex};

use indexmap::IndexMap;
use relay_core::control::lock::{InstanceLock, InstanceLocks, LockError};
use relay_core::control::state::{
    AccountRecord, PersistedRouting, PersistedState, StateError, StateStore, set_account,
};
use relay_core::supervisor::model::FatalReason;
use relay_core::supervisor::routing::Routing;
use serde_json::{Value, json};

fn read_json(dir: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(dir.join("state.json")).unwrap()).unwrap()
}

fn write_json(dir: &Path, v: &Value) {
    std::fs::write(dir.join("state.json"), serde_json::to_vec(v).unwrap()).unwrap();
}

fn is_corrupt<T: std::fmt::Debug>(r: Result<T, StateError>) -> bool {
    matches!(r, Err(StateError::Corrupt(_)))
}

fn is_locked<T: std::fmt::Debug>(r: Result<T, LockError>) -> bool {
    matches!(r, Err(LockError::Locked(_)))
}

fn as_value(m: &Option<serde_json::Map<String, Value>>) -> Option<Value> {
    m.as_ref().map(|m| Value::Object(m.clone()))
}

fn v3_doc() -> Value {
    json!({
        "schema_version": 3, "revision": 1, "desired_enabled": false, "fatal_latch": null,
        "quarantine": null, "consecutive_rejections": 0, "accounts": {}, "routing": null,
    })
}

#[test]
fn missing_file_gives_disabled_default() {
    let tmp = tempfile::tempdir().unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert!(st.revision == 0 && !st.desired_enabled);
    assert!(st.fatal_latch.is_none() && st.quarantine.is_none() && st.consecutive_rejections == 0);
}

#[test]
fn update_increments_revision_and_persists() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    let st = store
        .update(|s| PersistedState {
            desired_enabled: true,
            ..s
        })
        .unwrap();
    assert_eq!(st.revision, 1);
    let st = store
        .update(|s| PersistedState {
            consecutive_rejections: 2,
            fatal_latch: json!({"reason": "X", "detail": "", "at": 1.0})
                .as_object()
                .cloned(),
            ..s
        })
        .unwrap();
    assert_eq!(st.revision, 2);
    let again = StateStore::new(tmp.path()).load().unwrap();
    assert!(again == st && again.desired_enabled);
    assert_eq!(read_json(tmp.path())["schema_version"], 3);
    let tmps: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(tmps.is_empty(), "{tmps:?}");
}

#[test]
fn update_ignores_revision_set_by_fn() {
    let tmp = tempfile::tempdir().unwrap();
    let st = StateStore::new(tmp.path())
        .update(|s| PersistedState { revision: 99, ..s })
        .unwrap();
    assert_eq!(st.revision, 1);
}

#[test]
fn concurrent_update_revision_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    let base = store.update(|s| s).unwrap().revision;
    let barrier = Barrier::new(2);
    let results: Mutex<Vec<Result<PersistedState, StateError>>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for enabled in [true, false] {
            let (store, barrier, results) = (&store, &barrier, &results);
            scope.spawn(move || {
                barrier.wait();
                let r = store.update_expecting(base, |s| PersistedState {
                    desired_enabled: enabled,
                    ..s
                });
                results.lock().unwrap().push(r);
            });
        }
    });
    let results = results.into_inner().unwrap();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StateError::Conflict(_))))
            .count(),
        1
    );
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(store.load().unwrap().revision, base + 1);
}

#[test]
fn stale_expected_revision_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    store.update(|s| s).unwrap();
    assert!(matches!(
        store.update_expecting(0, |s| s),
        Err(StateError::Conflict(_))
    ));
}

#[test]
fn corrupt_state_raises() {
    for content in [&b"{not json"[..], b"[]", br#"{"schema_version": 2}"#, b""] {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("state.json"), content).unwrap();
        let store = StateStore::new(tmp.path());
        assert!(is_corrupt(store.load()), "{content:?}");
        assert!(is_corrupt(store.update(|s| s)), "{content:?}");
        assert_eq!(
            std::fs::read(tmp.path().join("state.json")).unwrap(),
            content
        );
    }
}

/// Child half of `instance_lock_across_processes`: a no-op unless the parent re-runs this test
/// binary with `RELAY_TEST_LOCK_HOLDER` set.
#[test]
fn instance_lock_holder_child() {
    let Ok(spec) = std::env::var("RELAY_TEST_LOCK_HOLDER") else {
        return;
    };
    let (uid, dir) = spec.split_once('|').unwrap();
    let _lock = InstanceLock::acquire(uid.parse().unwrap(), Some(Path::new(dir))).unwrap();
    println!("locked");
    std::io::stdout().flush().unwrap();
    std::io::stdin()
        .lock()
        .read_line(&mut String::new())
        .unwrap();
}

#[test]
fn instance_lock_across_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "instance_lock_holder_child", "--nocapture"])
        .env(
            "RELAY_TEST_LOCK_HOLDER",
            format!("42|{}", tmp.path().display()),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    let mut line = String::new();
    loop {
        line.clear();
        assert!(out.read_line(&mut line).unwrap() > 0, "child exited early");
        if line.trim() == "locked" {
            break;
        }
    }
    let during = std::panic::catch_unwind(|| {
        assert!(is_locked(InstanceLock::acquire(42, Some(tmp.path()))));
        drop(InstanceLock::acquire(43, Some(tmp.path())).unwrap());
    });
    stdin.write_all(b"\n").unwrap();
    stdin.flush().unwrap();
    assert!(child.wait().unwrap().success());
    during.unwrap();
    drop(InstanceLock::acquire(42, Some(tmp.path())).unwrap());
}

#[test]
fn malformed_quarantine_is_corrupt() {
    let cases = [
        json!({}),
        json!({"owner_id": 1}),
        json!({"owner_id": "x", "stream_key": "k", "incarnation": 1, "since": 1.0}),
    ];
    for quarantine in cases {
        let tmp = tempfile::tempdir().unwrap();
        StateStore::new(tmp.path()).update(|s| s).unwrap();
        let mut data = read_json(tmp.path());
        data["quarantine"] = quarantine.clone();
        write_json(tmp.path(), &data);
        assert!(
            is_corrupt(StateStore::new(tmp.path()).load()),
            "{quarantine}"
        );
    }
}

#[test]
fn v1_file_reads_as_v2_keeps_ingest_fields_and_writes_v2() {
    let tmp = tempfile::tempdir().unwrap();
    let q = json!({"owner_id": 1, "stream_key": "k", "incarnation": 2, "since": 1.0});
    let latch = json!({"reason": "AUTH_REJECTED", "detail": "", "at": 1.0});
    let v1 = json!({
        "schema_version": 1, "revision": 5, "desired_enabled": true,
        "fatal_latch": latch, "quarantine": q, "consecutive_rejections": 2,
    });
    write_json(tmp.path(), &v1);
    let store = StateStore::new(tmp.path());
    let st = store.load().unwrap();
    assert!(st.accounts.is_empty() && st.schema_version == 3);
    assert_eq!(as_value(&st.fatal_latch), Some(latch.clone()));
    assert_eq!(as_value(&st.quarantine), Some(q.clone()));
    assert_eq!(st.consecutive_rejections, 2);
    let st = store.update(|s| s).unwrap();
    let on_disk = read_json(tmp.path());
    assert!(on_disk["schema_version"] == 3 && on_disk["accounts"] == json!({}));
    assert!(on_disk["quarantine"] == q && on_disk["fatal_latch"] == latch);
    assert_eq!(st.revision, 6);
}

/// Python asserts `account_view(...) == AccountPersisted(FatalReason...)`; `AccountPersisted` is a
/// Task C6 type, so the raw record and the `FatalReason` of its latch are checked instead.
#[test]
fn account_view_and_set_account_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    let empty = AccountRecord {
        fatal_latch: None,
        quarantine_incarnation: None,
        consecutive_rejections: 0,
    };
    assert_eq!(store.load().unwrap().account_record(7), empty);
    store
        .update(|s| {
            set_account(s, 7, |r| {
                r.quarantine_incarnation = Some(3);
                r.consecutive_rejections = 1;
            })
        })
        .unwrap();
    let latch = json!({"reason": "AUTH_REJECTED", "detail": "", "at": 1.0});
    store
        .update(|s| set_account(s, 7, |r| r.fatal_latch = latch.as_object().cloned()))
        .unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert_eq!(
        st.account_record(7),
        AccountRecord {
            fatal_latch: latch.as_object().cloned(),
            quarantine_incarnation: Some(3),
            consecutive_rejections: 1,
        }
    );
    assert_eq!(st.account_record(8), empty);
    assert_eq!(
        FatalReason::from_latch(st.account_record(7).fatal_latch.as_ref()),
        Some(FatalReason::AuthRejected)
    );
    assert_eq!(
        FatalReason::from_latch(st.account_record(8).fatal_latch.as_ref()),
        None
    );
}

#[test]
fn bad_accounts_are_corrupt() {
    let bad_rec =
        json!({"fatal_latch": null, "quarantine_incarnation": "a", "consecutive_rejections": 0});
    let cases = [
        json!([]),
        json!({"x": {}}),
        json!({"7": {"fatal_latch": null}}),
        json!({"7": bad_rec}),
    ];
    for accounts in cases {
        let tmp = tempfile::tempdir().unwrap();
        StateStore::new(tmp.path()).update(|s| s).unwrap();
        let mut d = read_json(tmp.path());
        d["accounts"] = accounts.clone();
        write_json(tmp.path(), &d);
        assert!(is_corrupt(StateStore::new(tmp.path()).load()), "{accounts}");
    }
}

#[test]
fn instance_locks_all_or_none() {
    let tmp = tempfile::tempdir().unwrap();
    let d = Some(tmp.path());
    {
        let _two = InstanceLock::acquire(2, d).unwrap();
        assert!(is_locked(InstanceLocks::acquire([1, 2, 3], d)));
        drop(InstanceLock::acquire(1, d).unwrap()); // 1 was released after the failed acquisition
        drop(InstanceLock::acquire(3, d).unwrap());
    }
    {
        let _all = InstanceLocks::acquire([1, 2, 3, 1], d).unwrap();
        assert!(is_locked(InstanceLock::acquire(3, d)));
    }
    drop(InstanceLock::acquire(3, d).unwrap());
}

#[test]
fn v2_state_reads_with_routing_none() {
    let tmp = tempfile::tempdir().unwrap();
    let v2 = json!({
        "schema_version": 2, "revision": 3, "desired_enabled": true, "fatal_latch": null,
        "quarantine": null, "consecutive_rejections": 0, "accounts": {},
    });
    write_json(tmp.path(), &v2);
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert!(st.routing.is_none() && st.schema_version == 3 && st.revision == 3);
}

#[test]
fn routing_roundtrip_v3() {
    let tmp = tempfile::tempdir().unwrap();
    let default = Routing {
        target_user_id: 1,
        channel_id: 2,
        mirrors: IndexMap::new(),
    };
    let rt = Routing {
        target_user_id: 10,
        channel_id: 20,
        mirrors: [(30, 40), (31, 41)].into(),
    };
    let store = StateStore::new(tmp.path());
    store
        .update(|s| PersistedState {
            routing: Some(rt.to_persisted()),
            ..s
        })
        .unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    let persisted = PersistedRouting {
        target_user_id: 10,
        channel_id: 20,
        mirrors: [("30".to_string(), 40), ("31".to_string(), 41)].into(),
    };
    assert_eq!(st.routing.as_ref(), Some(&persisted));
    assert_eq!(Routing::from_persisted(st.routing.as_ref(), &default), rt);
    assert_eq!(Routing::from_persisted(None, &default), default);
    assert_eq!(
        read_json(tmp.path())["routing"],
        json!({"target_user_id": 10, "channel_id": 20, "mirrors": {"30": 40, "31": 41}})
    );
}

#[test]
fn routing_rejects_bad_shape() {
    let cases = [
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"abc": 3}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 3, "6": 3}}),
        json!({"target_user_id": "1", "channel_id": 2, "mirrors": {}}),
        json!({"target_user_id": 1, "channel_id": 2}),
        json!({"target_user_id": 0, "channel_id": 2, "mirrors": {}}),
        json!({"target_user_id": 1, "channel_id": -2, "mirrors": {}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"0": 3}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 0}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"5": -3}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"01": 3}}),
        json!({"target_user_id": 1, "channel_id": 2, "mirrors": {"5": 3, "05": 4}}),
    ];
    for routing in cases {
        let tmp = tempfile::tempdir().unwrap();
        let mut doc = v3_doc();
        doc["routing"] = routing.clone();
        write_json(tmp.path(), &doc);
        assert!(is_corrupt(StateStore::new(tmp.path()).load()), "{routing}");
    }
}

#[test]
fn autoscale_override_is_optional_v3_key() {
    let tmp = tempfile::tempdir().unwrap();
    let mut doc = v3_doc();
    doc["prejoin_voice"] = Value::Null;
    write_json(tmp.path(), &doc);
    let store = StateStore::new(tmp.path());
    assert_eq!(store.load().unwrap().relay_autoscale, None); // nothing persisted: config decides
    store
        .update(|s| PersistedState {
            relay_autoscale: Some(false),
            ..s
        })
        .unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert!(
        st.relay_autoscale == Some(false) && st.schema_version == 3 && st.prejoin_voice.is_none()
    );
    doc["relay_autoscale"] = json!(1);
    write_json(tmp.path(), &doc);
    assert!(is_corrupt(store.load()));
}

#[test]
fn prejoin_override_is_optional_v3_key() {
    let tmp = tempfile::tempdir().unwrap();
    let mut doc = v3_doc();
    write_json(tmp.path(), &doc);
    let store = StateStore::new(tmp.path());
    assert_eq!(store.load().unwrap().prejoin_voice, None); // nothing persisted: config decides
    store
        .update(|s| PersistedState {
            prejoin_voice: Some(true),
            ..s
        })
        .unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert!(st.prejoin_voice == Some(true) && st.schema_version == 3);
    doc["prejoin_voice"] = json!("yes");
    write_json(tmp.path(), &doc);
    assert!(is_corrupt(store.load()));
}

#[test]
fn chat_operator_ids_are_optional_v3_key() {
    let tmp = tempfile::tempdir().unwrap();
    let mut doc = v3_doc();
    write_json(tmp.path(), &doc);
    let store = StateStore::new(tmp.path());
    // old file without the key: nobody added by chat
    assert!(store.load().unwrap().chat_operator_ids.is_empty());
    store
        .update(|s| PersistedState {
            chat_operator_ids: vec![123456789012345678, 5],
            ..s
        })
        .unwrap();
    assert_eq!(
        StateStore::new(tmp.path())
            .load()
            .unwrap()
            .chat_operator_ids,
        vec![123456789012345678, 5]
    );
    let bad = [
        json!("5"),
        json!(["5"]),
        json!([0]),
        json!([-1]),
        json!([true]),
        json!([5, 5]),
        json!({"5": 1}),
    ];
    for b in bad {
        doc["chat_operator_ids"] = b.clone();
        write_json(tmp.path(), &doc);
        assert!(is_corrupt(store.load()), "{b}");
    }
}

/// Fixture text produced by CPython `json.dumps(asdict(state), indent=2)` (revision 2).
const PYTHON_STATE_JSON: &str = r#"{
  "schema_version": 3,
  "revision": 2,
  "desired_enabled": true,
  "fatal_latch": {
    "reason": "X",
    "detail": "\u00e9",
    "at": 1.5
  },
  "quarantine": {
    "owner_id": 1,
    "stream_key": "k",
    "incarnation": 2,
    "since": 1731123456.25
  },
  "consecutive_rejections": 2,
  "accounts": {
    "7": {
      "fatal_latch": null,
      "quarantine_incarnation": 3,
      "consecutive_rejections": 1
    }
  },
  "routing": {
    "target_user_id": 10,
    "channel_id": 20,
    "mirrors": {
      "30": 40
    }
  },
  "prejoin_voice": true,
  "relay_autoscale": null,
  "chat_operator_ids": [
    5,
    6
  ]
}
"#;

#[test]
fn on_disk_format_matches_python() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("state.json"), PYTHON_STATE_JSON.trim()).unwrap();
    let store = StateStore::new(tmp.path());
    store.update(|s| s).unwrap();
    let written = std::fs::read_to_string(tmp.path().join("state.json")).unwrap();
    assert_eq!(
        written,
        PYTHON_STATE_JSON
            .trim()
            .replace("\"revision\": 2", "\"revision\": 3")
    );
}
