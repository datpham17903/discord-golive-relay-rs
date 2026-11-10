//! Durable `state.json` store (spec §15): atomic, revisioned, lock-serialized. Port of
//! `golive_relay/control/state.py`.
//!
//! Python keeps `fatal_latch` and `quarantine` as plain dicts and only validates their shape; the same
//! is done here with `JsonObject`, so unknown latch contents and int/float distinctions survive a
//! load/store round trip byte for byte. Conversion to `FatalReason`/`AccountPersisted`
//! (`account_view`) belongs to Task C4, which owns those types; `PersistedState::account_record` is the
//! raw half.

use std::collections::HashSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use indexmap::IndexMap;
use serde_json::{Map, Value};

use super::lock::{lock_blocking, unlock};
use crate::obs::dumps_indent;

pub const SCHEMA_VERSION: i64 = 3;
// v1 has no `accounts` (read as {}); v1/v2 have no `routing` (read as None); `prejoin_voice` and
// `relay_autoscale` are optional v3 keys (missing reads as None); `chat_operator_ids` is an optional
// v3 key (missing reads as []).
const READ_VERSIONS: [i64; 3] = [1, 2, 3];
const ACCOUNT_KEYS: [&str; 3] = [
    "fatal_latch",
    "quarantine_incarnation",
    "consecutive_rejections",
];
const ROUTING_KEYS: [&str; 3] = ["target_user_id", "channel_id", "mirrors"];
const QUARANTINE_KEYS: [&str; 4] = ["owner_id", "stream_key", "incarnation", "since"];

/// Windows refuses a rename over a file another handle has open.
const RETRY_REPLACE: bool = cfg!(windows);
const REPLACE_ATTEMPTS: u32 = 50;

pub type JsonObject = Map<String, Value>;

/// state.json exists but is unreadable or has an unknown shape; never auto-reset.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StateCorrupt(pub String);

/// The stored revision differs from the expected one.
#[derive(Debug, thiserror::Error)]
#[error("expected revision {expected}, stored {stored}")]
pub struct RevisionConflict {
    pub expected: i64,
    pub stored: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error(transparent)]
    Corrupt(#[from] StateCorrupt),
    #[error(transparent)]
    Conflict(#[from] RevisionConflict),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// `accounts[user_id]`.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountRecord {
    pub fatal_latch: Option<JsonObject>, // {"reason", "detail", "at"}
    pub quarantine_incarnation: Option<i64>,
    pub consecutive_rejections: i64,
}

impl AccountRecord {
    fn empty() -> Self {
        Self {
            fatal_latch: None,
            quarantine_incarnation: None,
            consecutive_rejections: 0,
        }
    }

    fn to_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("fatal_latch".into(), obj_or_null(&self.fatal_latch));
        m.insert(
            "quarantine_incarnation".into(),
            self.quarantine_incarnation.map_or(Value::Null, Value::from),
        );
        m.insert(
            "consecutive_rejections".into(),
            Value::from(self.consecutive_rejections),
        );
        Value::Object(m)
    }
}

/// `routing`: mirror keys are canonical decimal channel ids, kept as strings like the JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedRouting {
    pub target_user_id: u64,
    pub channel_id: u64,
    pub mirrors: IndexMap<String, u64>,
}

impl PersistedRouting {
    fn to_value(&self) -> Value {
        let mirrors: Map<String, Value> = self
            .mirrors
            .iter()
            .map(|(ch, acct)| (ch.clone(), Value::from(*acct)))
            .collect();
        let mut m = Map::new();
        m.insert("target_user_id".into(), Value::from(self.target_user_id));
        m.insert("channel_id".into(), Value::from(self.channel_id));
        m.insert("mirrors".into(), Value::Object(mirrors));
        Value::Object(m)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PersistedState {
    pub schema_version: i64,
    pub revision: i64,
    pub desired_enabled: bool,
    pub fatal_latch: Option<JsonObject>, // {"reason", "detail", "at"}
    pub quarantine: Option<JsonObject>,  // {"owner_id", "stream_key", "incarnation", "since"}
    pub consecutive_rejections: i64,
    /// `str(user_id)` -> record.
    pub accounts: IndexMap<String, AccountRecord>,
    /// `None` = use config.
    pub routing: Option<PersistedRouting>,
    /// Chat override of `runtime.prejoin_voice`; `None` = use config.
    pub prejoin_voice: Option<bool>,
    /// Chat override of `runtime.relay_autoscale`; `None` = use config.
    pub relay_autoscale: Option<bool>,
    /// User ids added by `op add`; config owners are not included.
    pub chat_operator_ids: Vec<u64>,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            revision: 0,
            desired_enabled: false,
            fatal_latch: None,
            quarantine: None,
            consecutive_rejections: 0,
            accounts: IndexMap::new(),
            routing: None,
            prejoin_voice: None,
            relay_autoscale: None,
            chat_operator_ids: Vec::new(),
        }
    }
}

impl PersistedState {
    /// The stored record for `user_id`, or an empty one (Python `account_view` minus the enum cast).
    pub fn account_record(&self, user_id: u64) -> AccountRecord {
        self.accounts
            .get(&user_id.to_string())
            .cloned()
            .unwrap_or_else(AccountRecord::empty)
    }

    /// Key order of Python's `asdict`.
    pub fn to_value(&self) -> Value {
        let accounts: Map<String, Value> = self
            .accounts
            .iter()
            .map(|(k, rec)| (k.clone(), rec.to_value()))
            .collect();
        let mut m = Map::new();
        m.insert("schema_version".into(), Value::from(self.schema_version));
        m.insert("revision".into(), Value::from(self.revision));
        m.insert("desired_enabled".into(), Value::from(self.desired_enabled));
        m.insert("fatal_latch".into(), obj_or_null(&self.fatal_latch));
        m.insert("quarantine".into(), obj_or_null(&self.quarantine));
        m.insert(
            "consecutive_rejections".into(),
            Value::from(self.consecutive_rejections),
        );
        m.insert("accounts".into(), Value::Object(accounts));
        m.insert(
            "routing".into(),
            self.routing
                .as_ref()
                .map_or(Value::Null, PersistedRouting::to_value),
        );
        m.insert(
            "prejoin_voice".into(),
            self.prejoin_voice.map_or(Value::Null, Value::from),
        );
        m.insert(
            "relay_autoscale".into(),
            self.relay_autoscale.map_or(Value::Null, Value::from),
        );
        m.insert(
            "chat_operator_ids".into(),
            Value::Array(
                self.chat_operator_ids
                    .iter()
                    .map(|u| Value::from(*u))
                    .collect(),
            ),
        );
        Value::Object(m)
    }
}

fn obj_or_null(o: &Option<JsonObject>) -> Value {
    o.as_ref().map_or(Value::Null, |m| Value::Object(m.clone()))
}

/// Copy of `st` with `accounts[user_id]` (created empty if missing) modified by `change`.
pub fn set_account(
    mut st: PersistedState,
    user_id: u64,
    change: impl FnOnce(&mut AccountRecord),
) -> PersistedState {
    let rec = st
        .accounts
        .entry(user_id.to_string())
        .or_insert_with(AccountRecord::empty);
    change(rec);
    st
}

fn is_int(v: &Value) -> bool {
    v.is_i64() || v.is_u64()
}

fn positive_int(v: &Value) -> Option<u64> {
    v.as_u64().filter(|n| *n > 0)
}

fn keys_are(m: &JsonObject, expected: &[&str]) -> bool {
    m.len() == expected.len() && expected.iter().all(|k| m.contains_key(*k))
}

fn key<'a>(m: &'a JsonObject, name: &str) -> Result<&'a Value, String> {
    m.get(name).ok_or_else(|| format!("missing key {name:?}"))
}

fn int_field(m: &JsonObject, name: &str) -> Result<i64, String> {
    key(m, name)?
        .as_i64()
        .ok_or_else(|| "bad field type".to_string())
}

fn opt_object(v: &Value) -> Result<Option<JsonObject>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Object(m) => Ok(Some(m.clone())),
        _ => Err("bad field type".into()),
    }
}

fn opt_bool(m: &JsonObject, name: &str) -> Result<Option<bool>, String> {
    match m.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err("bad field type".into()),
    }
}

fn check_routing(rt: &Value) -> Result<PersistedRouting, String> {
    let m = rt
        .as_object()
        .filter(|m| keys_are(m, &ROUTING_KEYS))
        .ok_or("bad routing")?;
    let (Some(target_user_id), Some(channel_id)) = (
        positive_int(&m["target_user_id"]),
        positive_int(&m["channel_id"]),
    ) else {
        return Err("bad routing ids".into());
    };
    let raw = m["mirrors"].as_object().ok_or("bad routing mirrors")?;
    let mut accounts = HashSet::new();
    let mut mirrors = IndexMap::new();
    for (ch, acct) in raw {
        // canonical decimal keys only, so two keys can never collapse to the same channel int
        let canonical = ch
            .parse::<u64>()
            .is_ok_and(|n| n > 0 && ch.bytes().all(|b| b.is_ascii_digit()) && n.to_string() == *ch);
        let Some(acct) = positive_int(acct).filter(|_| canonical) else {
            return Err("bad routing mirror entry".into());
        };
        if !accounts.insert(acct) {
            return Err("routing account mirrored in more than one channel".into());
        }
        mirrors.insert(ch.clone(), acct);
    }
    Ok(PersistedRouting {
        target_user_id,
        channel_id,
        mirrors,
    })
}

fn check_quarantine(q: &JsonObject) -> Result<(), String> {
    if keys_are(q, &QUARANTINE_KEYS)
        && is_int(&q["owner_id"])
        && q["stream_key"].is_string()
        && is_int(&q["incarnation"])
        && q["since"].is_number()
    {
        Ok(())
    } else {
        Err("bad quarantine record".into())
    }
}

fn check_account(key: &str, rec: &Value) -> Result<AccountRecord, String> {
    let bad = || format!("bad account record {key:?}");
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let m = rec
        .as_object()
        .filter(|m| keys_are(m, &ACCOUNT_KEYS))
        .ok_or_else(bad)?;
    let fatal_latch = opt_object(&m["fatal_latch"]).map_err(|_| bad())?;
    let quarantine_incarnation = match &m["quarantine_incarnation"] {
        Value::Null => None,
        v => Some(v.as_i64().ok_or_else(bad)?),
    };
    let consecutive_rejections = m["consecutive_rejections"].as_i64().ok_or_else(bad)?;
    Ok(AccountRecord {
        fatal_latch,
        quarantine_incarnation,
        consecutive_rejections,
    })
}

fn parse_inner(raw: &[u8]) -> Result<PersistedState, String> {
    let text = std::str::from_utf8(raw).map_err(|e| e.to_string())?;
    let data: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let data = data.as_object().ok_or("unsupported schema_version")?;
    let version = data
        .get("schema_version")
        .and_then(Value::as_i64)
        .filter(|v| READ_VERSIONS.contains(v))
        .ok_or("unsupported schema_version")?;

    let revision = int_field(data, "revision")?;
    let desired_enabled = key(data, "desired_enabled")?
        .as_bool()
        .ok_or("bad field type")?;
    let fatal_latch = opt_object(key(data, "fatal_latch")?)?;
    let quarantine = opt_object(key(data, "quarantine")?)?;
    let consecutive_rejections = int_field(data, "consecutive_rejections")?;
    if let Some(q) = &quarantine {
        check_quarantine(q)?;
    }

    let mut accounts = IndexMap::new();
    if version >= 2 {
        let raw_accounts = key(data, "accounts")?.as_object().ok_or("bad accounts")?;
        for (k, rec) in raw_accounts {
            accounts.insert(k.clone(), check_account(k, rec)?);
        }
    }
    let mut routing = None;
    let mut prejoin_voice = None;
    let mut relay_autoscale = None;
    let mut chat_operator_ids = Vec::new();
    if version == 3 {
        let rt = key(data, "routing")?;
        if !rt.is_null() {
            routing = Some(check_routing(rt)?);
        }
        prejoin_voice = opt_bool(data, "prejoin_voice")?;
        relay_autoscale = opt_bool(data, "relay_autoscale")?;
        if let Some(ops) = data.get("chat_operator_ids") {
            let bad = || "bad chat_operator_ids".to_string();
            for v in ops.as_array().ok_or_else(bad)? {
                chat_operator_ids.push(positive_int(v).ok_or_else(bad)?);
            }
            let unique: HashSet<_> = chat_operator_ids.iter().collect();
            if unique.len() != chat_operator_ids.len() {
                return Err(bad());
            }
        }
    }
    Ok(PersistedState {
        schema_version: SCHEMA_VERSION,
        revision,
        desired_enabled,
        fatal_latch,
        quarantine,
        consecutive_rejections,
        accounts,
        routing,
        prejoin_voice,
        relay_autoscale,
        chat_operator_ids,
    })
}

fn parse(raw: &[u8]) -> Result<PersistedState, StateCorrupt> {
    parse_inner(raw).map_err(|e| StateCorrupt(format!("state.json is corrupt: {e}")))
}

/// Runs `replace`, retrying a permission error up to 50 times 10 ms apart where the platform needs it.
fn replace_with_retry(retry: bool, mut replace: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    for attempt in 1..=REPLACE_ATTEMPTS {
        match replace() {
            Err(e)
                if retry
                    && e.kind() == io::ErrorKind::PermissionDenied
                    && attempt < REPLACE_ATTEMPTS =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => return other,
        }
    }
    unreachable!("the last attempt always returns")
}

#[derive(Debug)]
pub struct StateStore {
    dir: PathBuf,
    path: PathBuf,
    lock_path: PathBuf,
}

impl StateStore {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        let dir = directory.as_ref().to_path_buf();
        Self {
            path: dir.join("state.json"),
            lock_path: dir.join("state.json.lock"),
            dir,
        }
    }

    /// Current state; a missing file yields the disabled default at revision 0.
    pub fn load(&self) -> Result<PersistedState, StateError> {
        match std::fs::read(&self.path) {
            Ok(raw) => Ok(parse(&raw)?),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(PersistedState::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Applies `f` under an exclusive lock on a fresh read; the revision increments by one.
    pub fn update(
        &self,
        f: impl FnOnce(PersistedState) -> PersistedState,
    ) -> Result<PersistedState, StateError> {
        self.update_inner(None, f)
    }

    /// Like `update`, but fails with `RevisionConflict` unless the stored revision is `expected`.
    pub fn update_expecting(
        &self,
        expected_revision: i64,
        f: impl FnOnce(PersistedState) -> PersistedState,
    ) -> Result<PersistedState, StateError> {
        self.update_inner(Some(expected_revision), f)
    }

    fn update_inner(
        &self,
        expected_revision: Option<i64>,
        f: impl FnOnce(PersistedState) -> PersistedState,
    ) -> Result<PersistedState, StateError> {
        std::fs::create_dir_all(&self.dir)?;
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.lock_path)?;
        lock_blocking(&lock_file)?;
        let result: Result<PersistedState, StateError> = (|| {
            let current = self.load()?;
            if let Some(expected) = expected_revision
                && current.revision != expected
            {
                return Err(RevisionConflict {
                    expected,
                    stored: current.revision,
                }
                .into());
            }
            let mut new = f(current.clone());
            new.schema_version = SCHEMA_VERSION;
            new.revision = current.revision + 1;
            self.write(&new)?;
            Ok(new)
        })();
        let _ = unlock(&lock_file);
        result
    }

    fn write(&self, st: &PersistedState) -> io::Result<()> {
        // Dropping the temp path on any error deletes the temp file.
        let mut tmp = tempfile::Builder::new()
            .prefix("state.json.")
            .suffix(".tmp")
            .tempfile_in(&self.dir)?;
        tmp.write_all(dumps_indent(&st.to_value(), 2).as_bytes())?;
        tmp.flush()?;
        tmp.as_file().sync_all()?;
        let tmp_path = tmp.into_temp_path();
        replace_with_retry(RETRY_REPLACE, || std::fs::rename(&tmp_path, &self.path))?;
        let _ = tmp_path.keep();
        #[cfg(unix)]
        std::fs::File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_retries_on_windows_permission_error() {
        let mut calls = 0;
        let r = replace_with_retry(true, || {
            calls += 1;
            if calls < 3 {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            } else {
                Ok(())
            }
        });
        assert!(r.is_ok());
        assert_eq!(calls, 3);
    }

    #[test]
    fn replace_permission_error_is_final_without_retry_or_after_50_attempts() {
        let mut calls = 0;
        let r = replace_with_retry(false, || {
            calls += 1;
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        });
        assert!(r.is_err());
        assert_eq!(calls, 1);

        calls = 0;
        let r = replace_with_retry(true, || {
            calls += 1;
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        });
        assert!(r.is_err());
        assert_eq!(calls, 50);
    }
}
