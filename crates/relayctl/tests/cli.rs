//! Port of `tests/control/test_cli.py`. Monkeypatched process state (environment, `fetch_user_id`,
//! `InstanceLocks`) is replaced by the [`Ctx`] hooks; nothing here talks to Discord.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use relay_core::config::{ConfigInvalid, load_config};
use relay_core::control::state::{PersistedState, StateStore, set_account};
use relay_discord::Urls;
use relayctl::cli::{self, Ctx, EXIT_CONFIG, EXIT_FAIL};
use relayctl::tokens::{TokenError, load_token_with, read_token_with};
use serde_json::{Map, Value, json};

type LookupFuture = Pin<Box<dyn Future<Output = Result<u64, String>> + Send>>;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn example_text() -> String {
    std::fs::read_to_string(root().join("config.example.yaml")).unwrap()
}

struct Output {
    code: i32,
    out: String,
    err: String,
}

/// One `relayctl` invocation with fake process state.
struct Harness {
    env: HashMap<String, String>,
    lookup: Arc<dyn Fn(String) -> LookupFuture + Send + Sync>,
    run_service: Box<cli::RunService<'static>>,
}

impl Harness {
    fn new() -> Self {
        Self {
            env: HashMap::new(),
            lookup: Arc::new(|_| Box::pin(async { panic!("no lookup expected") })),
            run_service: Box::new(|cfg, token, metrics| {
                Box::pin(relayctl::service::run_service(cfg, token, metrics))
            }),
        }
    }

    fn with_ids(mut self, ids: &'static [(&'static str, u64)]) -> Self {
        self.lookup = Arc::new(move |token| {
            let found = ids.iter().find(|(t, _)| *t == token).map(|(_, id)| *id);
            Box::pin(async move { found.ok_or_else(|| "unknown token".to_string()) })
        });
        self
    }

    fn token_dir(mut self, tmp: &Path, names: &[&str]) -> Self {
        let d = tmp.join("tokens");
        std::fs::create_dir_all(&d).unwrap();
        for n in names {
            std::fs::write(d.join(n), format!("tok-{n}\n")).unwrap();
        }
        self.env
            .insert("RELAY_TOKEN_DIR".into(), d.to_string_lossy().into_owned());
        self
    }

    fn main(&self, args: &[&str]) -> Output {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let env = |var: &str| self.env.get(var).cloned();
        let lookup = |token: String| (self.lookup)(token);
        let code = cli::main(
            args.iter().copied(),
            &mut Ctx {
                out: &mut out,
                err: &mut err,
                env: &env,
                lookup: &lookup,
                run_service: &*self.run_service,
            },
        );
        Output {
            code,
            out: String::from_utf8(out).unwrap(),
            err: String::from_utf8(err).unwrap(),
        }
    }
}

fn state_args<'a>(cmd: &'a str, dir: &'a str) -> [&'a str; 3] {
    [cmd, "--state-dir", dir]
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn latch(reason: &str) -> Map<String, Value> {
    json!({"reason": reason, "detail": "d", "at": 1.0})
        .as_object()
        .unwrap()
        .clone()
}

fn set_latch(
    store: &StateStore,
    reason: &str,
    rejections: i64,
    quarantine: Option<Map<String, Value>>,
) {
    let l = latch(reason);
    store
        .update(|mut st| {
            st.fatal_latch = Some(l);
            st.consecutive_rejections = rejections;
            st.quarantine = quarantine;
            st
        })
        .unwrap();
}

fn set_acct_latch(
    store: &StateStore,
    uid: u64,
    reason: &str,
    rejections: i64,
    quarantine: Option<i64>,
) {
    let l = latch(reason);
    store
        .update(|st: PersistedState| {
            set_account(st, uid, |rec| {
                rec.fatal_latch = Some(l);
                rec.consecutive_rejections = rejections;
                rec.quarantine_incarnation = quarantine;
            })
        })
        .unwrap();
}

// ---------------------------------------------------------------- state commands

#[test]
fn enable_disable_offline_never_touch_yaml() {
    let tmp = tempfile::tempdir().unwrap();
    let yaml_copy = tmp.path().join("config.yaml");
    std::fs::copy(root().join("config.example.yaml"), &yaml_copy).unwrap();
    let before = std::fs::read(&yaml_copy).unwrap();
    let h = Harness::new();
    let store = StateStore::new(tmp.path());
    assert_eq!(h.main(&state_args("enable", s(tmp.path()))).code, 0);
    assert!(store.load().unwrap().desired_enabled);
    let disabled = h.main(&state_args("disable", s(tmp.path())));
    assert_eq!(disabled.code, 0);
    assert!(!store.load().unwrap().desired_enabled);
    assert_eq!(
        disabled.out.trim(),
        r#"{"ok": true, "revision": 2, "desired_enabled": false}"#
    );
    assert_eq!(std::fs::read(&yaml_copy).unwrap(), before);
}

#[test]
fn reset_fatal_rejected_by_server_zeroes_counter_keeps_quarantine() {
    let tmp = tempfile::tempdir().unwrap();
    let q = json!({"owner_id": 1, "stream_key": "k", "incarnation": 2, "since": 1.0})
        .as_object()
        .unwrap()
        .clone();
    let store = StateStore::new(tmp.path());
    set_latch(&store, "RELAY_REJECTED_BY_SERVER", 3, Some(q.clone()));
    assert_eq!(
        Harness::new()
            .main(&state_args("reset-fatal", s(tmp.path())))
            .code,
        0
    );
    let st = store.load().unwrap();
    assert!(st.fatal_latch.is_none());
    assert_eq!(st.consecutive_rejections, 0);
    assert_eq!(st.quarantine, Some(q));
    assert!(!st.desired_enabled);
}

#[test]
fn reset_fatal_other_reason_keeps_counter() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    set_latch(&store, "AUTH_REJECTED", 2, None);
    assert_eq!(
        Harness::new()
            .main(&state_args("reset-fatal", s(tmp.path())))
            .code,
        0
    );
    let st = store.load().unwrap();
    assert!(st.fatal_latch.is_none());
    assert_eq!(st.consecutive_rejections, 2);
}

#[test]
fn write_failure_reported_as_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let not_a_dir = tmp.path().join("file");
    std::fs::write(&not_a_dir, "x").unwrap();
    let r = Harness::new().main(&["disable", "--state-dir", s(&not_a_dir)]);
    assert_eq!(r.code, 1);
    assert!(r.err.to_lowercase().contains("failed"), "{}", r.err);
}

#[test]
fn corrupt_state_is_failure() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("state.json"), "{not json").unwrap();
    assert_eq!(
        Harness::new()
            .main(&state_args("enable", s(tmp.path())))
            .code,
        1
    );
}

#[test]
fn status_reads_status_json_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("status.json"), r#"{"state": "idle"}"#).unwrap();
    let r = Harness::new().main(&state_args("status", s(tmp.path())));
    assert_eq!(r.code, 0);
    assert!(r.out.contains(r#""state": "idle""#), "{}", r.out);
}

#[test]
fn status_without_service_shows_persisted_state() {
    let tmp = tempfile::tempdir().unwrap();
    let r = Harness::new().main(&state_args("status", s(tmp.path())));
    assert_eq!(r.code, 0);
    let out: Value = serde_json::from_str(&r.out).unwrap();
    assert_eq!(out["desired_enabled"], json!(false));
}

/// Windows: the state commands ask the service over its named pipe and fall back to the files once
/// the pipe is gone.
#[cfg(windows)]
#[test]
fn state_commands_use_the_named_pipe_then_fall_back_to_state_json() {
    use relay_core::control::server::ControlServer;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut srv = ControlServer::create(
        dir.join(cli::SOCKET_NAME),
        Arc::new(
            |req| json!({"ok": true, "via": "pipe", "cmd": req["cmd"], "peer": req["_peer_uid"]}),
        ),
    )
    .unwrap();
    rt.block_on(srv.start()).unwrap();
    let h = Harness::new();
    let r = h.main(&state_args("enable", s(dir)));
    assert_eq!(r.code, 0, "{}", r.err);
    let resp: Value = serde_json::from_str(&r.out).unwrap();
    assert_eq!(
        resp,
        json!({"ok": true, "via": "pipe", "cmd": "enable", "peer": null})
    );
    let r = h.main(&state_args("status", s(dir)));
    let resp: Value = serde_json::from_str(&r.out).unwrap();
    assert_eq!(resp["via"], "pipe");
    // the service answered: nothing was written offline
    assert!(!dir.join("state.json").exists());

    srv.close();
    let r = h.main(&state_args("enable", s(dir)));
    assert_eq!(r.code, 0, "{}", r.err);
    let resp: Value = serde_json::from_str(&r.out).unwrap();
    assert!(resp.get("via").is_none() && resp["desired_enabled"] == json!(true));
    assert!(StateStore::new(dir).load().unwrap().desired_enabled);
    drop(rt);
}

#[test]
fn reset_fatal_account_only_that_account() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    set_latch(&store, "AUTH_REJECTED", 1, None);
    set_acct_latch(&store, 7, "RELAY_REJECTED_BY_SERVER", 3, Some(2));
    set_acct_latch(&store, 8, "AUTH_REJECTED", 2, None);
    let r = Harness::new().main(&[
        "reset-fatal",
        "--state-dir",
        s(tmp.path()),
        "--account",
        "7",
    ]);
    assert_eq!(r.code, 0);
    let st = store.load().unwrap();
    let (a7, a8) = (st.account_record(7), st.account_record(8));
    assert!(a7.fatal_latch.is_none());
    assert_eq!(a7.consecutive_rejections, 0);
    assert_eq!(a7.quarantine_incarnation, Some(2));
    assert!(a8.fatal_latch.is_some());
    assert_eq!(a8.consecutive_rejections, 2);
    assert!(st.fatal_latch.is_some());
    assert_eq!(st.consecutive_rejections, 1);
}

#[test]
fn reset_fatal_account_other_reason_keeps_counter() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    set_acct_latch(&store, 7, "AUTH_REJECTED", 2, Some(2));
    let r = Harness::new().main(&[
        "reset-fatal",
        "--state-dir",
        s(tmp.path()),
        "--account",
        "7",
    ]);
    assert_eq!(r.code, 0);
    assert_eq!(
        store
            .load()
            .unwrap()
            .account_record(7)
            .consecutive_rejections,
        2
    );
}

#[test]
fn reset_fatal_unknown_account_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let r = Harness::new().main(&[
        "reset-fatal",
        "--state-dir",
        s(tmp.path()),
        "--account",
        "9",
    ]);
    assert_eq!(r.code, 0);
    assert!(r.out.contains("no fatal latch"), "{}", r.out);
}

#[test]
fn reset_fatal_bad_account_id_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    for bad in ["abc", "0", "-1"] {
        let r = Harness::new().main(&[
            "reset-fatal",
            "--state-dir",
            s(tmp.path()),
            "--account",
            bad,
        ]);
        assert_ne!(r.code, 0, "{bad}");
    }
}

#[test]
fn status_shows_per_account_state() {
    let tmp = tempfile::tempdir().unwrap();
    set_acct_latch(&StateStore::new(tmp.path()), 7, "AUTH_REJECTED", 1, Some(4));
    let r = Harness::new().main(&state_args("status", s(tmp.path())));
    assert_eq!(r.code, 0);
    let out: Value = serde_json::from_str(&r.out).unwrap();
    assert_eq!(
        out["accounts"]["7"],
        json!({"fatal": "AUTH_REJECTED", "quarantine_incarnation": 4, "consecutive_rejections": 1})
    );
}

// ---------------------------------------------------------------- config commands

#[test]
fn validate_bad_config_exits_1_with_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let bad = tmp.path().join("bad.yaml");
    std::fs::write(&bad, "bogus_key: 1\n").unwrap();
    let r = Harness::new().main(&["validate", "--config", s(&bad)]);
    assert_eq!(r.code, 1);
    assert!(r.err.contains("bogus_key"), "{}", r.err);
}

#[test]
fn validate_example_config_ok() {
    let r = Harness::new().main(&[
        "validate",
        "--config",
        s(&root().join("config.example.yaml")),
    ]);
    assert_eq!(r.code, 0, "{}", r.err);
    assert!(r.out.contains("config valid"));
}

#[test]
fn run_refuses_unusable_config_exit_78() {
    let tmp = tempfile::tempdir().unwrap();
    let bad = tmp.path().join("bad.yaml");
    std::fs::write(&bad, "bogus_key: 1\n").unwrap();
    assert_eq!(
        Harness::new().main(&["run", "--config", s(&bad)]).code,
        EXIT_CONFIG
    );
}

/// Single-quoted YAML scalar for a path (backslashes stay literal).
fn yaml_path(p: &Path) -> String {
    format!("'{}'", p.display())
}

/// The example config with a measured `T_dead` and its state under `state_dir`.
fn base_config(state_dir: &Path) -> String {
    example_text()
        .replace(
            "source_dead_timeout_seconds: null",
            "source_dead_timeout_seconds: 30",
        )
        .replace(
            "state_directory: \"/var/lib/discord-relay\"",
            &format!("state_directory: {}", yaml_path(state_dir)),
        )
}

/// Capability manifest next to the config that makes it operable.
fn write_manifests(cfg_path: &Path) {
    let dir = cfg_path.parent().unwrap().join("manifests");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("verified-capabilities.json"),
        json!({"live": {"source_dead_timeout_seconds": 30}}).to_string(),
    )
    .unwrap();
}

#[test]
fn run_corrupt_state_exits_78() {
    let tmp = tempfile::tempdir().unwrap();
    let text = base_config(tmp.path()).replace(
        "# expected_relay_user_id: \"100000000000000001\"",
        "expected_relay_user_id: \"100000000000000001\"",
    );
    let cfg = tmp.path().join("c.yaml");
    std::fs::write(&cfg, text).unwrap();
    write_manifests(&cfg);
    std::fs::write(tmp.path().join("state.json"), "{not json").unwrap();
    let h = Harness::new().token_dir(tmp.path(), &["relay-token"]);
    assert_eq!(h.main(&["run", "--config", s(&cfg)]).code, EXIT_CONFIG);
}

fn short_form_config(tmp: &Path, relay: &str, mirror: &str) -> PathBuf {
    let text = base_config(&tmp.join("state"))
        .replacen("relay_accounts: []", &format!("relay_accounts: {relay}"), 1)
        .replacen(
            "mirror_accounts: []",
            &format!("mirror_accounts: {mirror}"),
            1,
        );
    let p = tmp.join("c.yaml");
    std::fs::write(&p, text).unwrap();
    p
}

fn default_short_form(tmp: &Path) -> PathBuf {
    short_form_config(tmp, "[{credential: r2}]", "[{credential: m1}]")
}

const IDS: &[(&str, u64)] = &[("tok-relay-token", 11), ("tok-r2", 12), ("tok-m1", 13)];
const ALL_TOKENS: &[&str] = &["relay-token", "r2", "m1"];

#[test]
fn validate_short_form_notes_ids_resolved_at_run() {
    let tmp = tempfile::tempdir().unwrap();
    let r = Harness::new().main(&["validate", "--config", s(&default_short_form(tmp.path()))]);
    assert_eq!(r.code, 0, "{}", r.err);
    assert!(r.out.contains("config valid"));
    for needle in ["resolved at run time", "relay-token", "r2", "m1"] {
        assert!(r.out.contains(needle), "{needle}: {}", r.out);
    }
}

fn tokens_map() -> HashMap<String, String> {
    ALL_TOKENS
        .iter()
        .map(|n| (n.to_string(), format!("tok-{n}")))
        .collect()
}

#[tokio::test]
async fn resolve_account_ids_concurrent_and_logs_names_only() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = load_config(&short_form_config(
        tmp.path(),
        "[{credential: r2}, {user_id: \"7\", credential: r3}]",
        "[{credential: m1}]",
    ))
    .unwrap();
    let started = Arc::new(Mutex::new(Vec::new()));
    let all_in = Arc::new(tokio::sync::Barrier::new(3));
    let (st, barrier) = (Arc::clone(&started), Arc::clone(&all_in));
    let lookup = move |token: String| -> LookupFuture {
        st.lock().unwrap().push(token.clone());
        let barrier = Arc::clone(&barrier);
        Box::pin(async move {
            // Sequential lookups would time out here.
            tokio::time::timeout(Duration::from_secs(1), barrier.wait())
                .await
                .map_err(|_| "not concurrent".to_string())?;
            Ok(IDS.iter().find(|(t, _)| *t == token).unwrap().1)
        })
    };
    let tokens = tokens_map();
    let mut err = Vec::new();
    let out = cli::resolve_account_ids(&cfg, &tokens, &lookup, &mut err)
        .await
        .unwrap();
    let d = &out.discord;
    assert_eq!(d.expected_relay_user_id, 11);
    let accts: Vec<_> = d
        .output_accounts
        .iter()
        .map(|a| (a.credential.as_str(), a.user_id))
        .collect();
    assert_eq!(accts, [("r2", 12), ("r3", 7), ("m1", 13)]);
    let mut seen = started.lock().unwrap().clone();
    seen.sort();
    let mut want: Vec<_> = tokens.values().cloned().collect();
    want.sort();
    assert_eq!(seen, want, "explicit ids are not looked up");
    let err = String::from_utf8(err).unwrap();
    for needle in ["relay-token -> 11", "r2 -> 12", "m1 -> 13"] {
        assert!(err.contains(needle), "{needle}: {err}");
    }
    assert!(!err.contains("tok-"), "{err}");
}

#[tokio::test]
async fn resolve_account_ids_failed_lookup_names_credential() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = load_config(&default_short_form(tmp.path())).unwrap();
    let lookup = |token: String| -> LookupFuture {
        Box::pin(async move {
            if token == "tok-r2" {
                return Err(format!("401 Unauthorized for {token}"));
            }
            Ok(IDS.iter().find(|(t, _)| *t == token).unwrap().1)
        })
    };
    let ConfigInvalid { errors } =
        cli::resolve_account_ids(&cfg, &tokens_map(), &lookup, &mut Vec::new())
            .await
            .unwrap_err();
    assert!(errors.iter().any(|e| e.contains("r2")), "{errors:?}");
    assert!(!errors.iter().any(|e| e.contains("tok-r2")), "{errors:?}");
}

/// A `run_service` that records the account ids it would lock (`[ingest, outputs...]`) and stops
/// there with exit 1, like the Python test's `InstanceLocks` raising `InstanceLocked`.
fn recording_service(h: &mut Harness) -> Arc<Mutex<Vec<Vec<u64>>>> {
    let locked = Arc::new(Mutex::new(Vec::new()));
    let rec = Arc::clone(&locked);
    h.run_service = Box::new(move |cfg, _token, _metrics| {
        let ids = std::iter::once(cfg.discord.expected_relay_user_id)
            .chain(cfg.output_accounts().iter().map(|a| a.user_id))
            .collect();
        rec.lock().unwrap().push(ids);
        Box::pin(async { 1 })
    });
    locked
}

#[test]
fn run_resolves_ids_before_locks() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_short_form(tmp.path());
    // Approved only for the resolved ingest id: the operable check must see 11.
    write_manifests(&cfg);
    let mut h = Harness::new()
        .token_dir(tmp.path(), ALL_TOKENS)
        .with_ids(IDS);
    let locked = recording_service(&mut h);
    let r = h.main(&["run", "--config", s(&cfg)]);
    assert_eq!(r.code, 1, "{}", r.err);
    assert_eq!(*locked.lock().unwrap(), [vec![11, 12, 13]]);
    assert!(!r.err.contains("tok-"), "{}", r.err);
}

#[test]
fn run_failed_lookup_is_config_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut h = Harness::new().token_dir(tmp.path(), ALL_TOKENS);
    h.lookup = Arc::new(|token| {
        Box::pin(async move {
            if token == "tok-m1" {
                return Err("bad token".to_string());
            }
            Ok(IDS.iter().find(|(t, _)| *t == token).unwrap().1)
        })
    });
    let r = h.main(&["run", "--config", s(&default_short_form(tmp.path()))]);
    assert_eq!(r.code, EXIT_CONFIG);
    assert!(r.err.contains("m1") && !r.err.contains("tok-"), "{}", r.err);
}

#[test]
fn run_missing_token_for_unresolved_account_is_config_error() {
    let tmp = tempfile::tempdir().unwrap();
    let h = Harness::new()
        .token_dir(tmp.path(), &["relay-token", "r2"])
        .with_ids(IDS);
    let r = h.main(&["run", "--config", s(&default_short_form(tmp.path()))]);
    assert_eq!(r.code, EXIT_CONFIG);
    assert!(r.err.contains("m1"), "{}", r.err);
}

#[test]
fn run_explicit_ids_need_no_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = short_form_config(
        tmp.path(),
        "[{user_id: \"21\", credential: r2}]",
        "[{user_id: \"22\", credential: m1}]",
    );
    let text = std::fs::read_to_string(&cfg).unwrap().replacen(
        "discord:\n",
        "discord:\n  expected_relay_user_id: '20'\n",
        1,
    );
    std::fs::write(&cfg, text).unwrap();
    write_manifests(&cfg);
    let mut h = Harness::new().token_dir(tmp.path(), ALL_TOKENS);
    let locked = recording_service(&mut h);
    let r = h.main(&["run", "--config", s(&cfg)]);
    assert_eq!(r.code, 1, "{}", r.err);
    assert_eq!(*locked.lock().unwrap(), [vec![20, 21, 22]]);
}

/// A `run_service` that records the metrics address it was given and stops there with exit 1.
fn metrics_recorder(h: &mut Harness) -> Arc<Mutex<Vec<Option<SocketAddr>>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let rec = Arc::clone(&seen);
    h.run_service = Box::new(move |_cfg, _token, metrics| {
        rec.lock().unwrap().push(metrics);
        Box::pin(async { 1 })
    });
    seen
}

fn runnable(tmp: &Path) -> (PathBuf, Harness) {
    let cfg = default_short_form(tmp);
    write_manifests(&cfg);
    let h = Harness::new().token_dir(tmp, ALL_TOKENS).with_ids(IDS);
    (cfg, h)
}

#[test]
fn run_metrics_listen_comes_from_the_flag_or_the_environment() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut h) = runnable(tmp.path());
    let seen = metrics_recorder(&mut h);
    let flag = [
        "run",
        "--config",
        s(&cfg),
        "--metrics-listen",
        "127.0.0.1:9108",
    ];
    assert_eq!(h.main(&["run", "--config", s(&cfg)]).code, 1);
    assert_eq!(h.main(&flag).code, 1);
    h.env
        .insert(cli::METRICS_LISTEN_ENV.into(), " [::1]:9200 ".into());
    assert_eq!(h.main(&["run", "--config", s(&cfg)]).code, 1);
    assert_eq!(h.main(&flag).code, 1); // the flag wins
    let addr = |a: &str| Some(a.parse::<SocketAddr>().unwrap());
    assert_eq!(
        *seen.lock().unwrap(),
        [
            None,
            addr("127.0.0.1:9108"),
            addr("[::1]:9200"),
            addr("127.0.0.1:9108")
        ]
    );
}

#[test]
fn run_refuses_a_bad_metrics_address() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut h) = runnable(tmp.path());
    let seen = metrics_recorder(&mut h);
    let r = h.main(&["run", "--config", s(&cfg), "--metrics-listen", "localhost"]);
    assert_eq!(r.code, 2, "{}", r.err);
    h.env.insert(cli::METRICS_LISTEN_ENV.into(), "nope".into());
    let r = h.main(&["run", "--config", s(&cfg)]);
    assert_eq!(r.code, EXIT_CONFIG);
    assert!(r.err.contains(cli::METRICS_LISTEN_ENV), "{}", r.err);
    assert!(seen.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------- tokens

#[test]
fn load_token_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("relay-token-2"), " abc \n").unwrap();
    std::fs::write(tmp.path().join("empty"), "\n").unwrap();
    let mut env: HashMap<String, String> = HashMap::new();
    let get = |env: &HashMap<String, String>| {
        let env = env.clone();
        move |var: &str| env.get(var).cloned()
    };
    assert!(matches!(
        load_token_with("relay-token-2", get(&env)),
        Err(TokenError::NoDirectory(_))
    ));
    env.insert(
        "CREDENTIALS_DIRECTORY".into(),
        tmp.path().to_string_lossy().into_owned(),
    );
    assert_eq!(load_token_with("relay-token-2", get(&env)).unwrap(), "abc");
    assert!(matches!(
        load_token_with("empty", get(&env)),
        Err(TokenError::Empty(_))
    ));
    match load_token_with("missing", get(&env)) {
        Err(TokenError::Unreadable { source, .. }) => {
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound)
        }
        other => panic!("{other:?}"),
    }
    let other = tmp.path().join("dev");
    std::fs::create_dir(&other).unwrap();
    std::fs::write(other.join("relay-token-2"), "dev-tok").unwrap();
    env.insert(
        "RELAY_TOKEN_DIR".into(),
        other.to_string_lossy().into_owned(),
    );
    assert_eq!(
        load_token_with("relay-token-2", get(&env)).unwrap(),
        "dev-tok"
    );
}

#[test]
fn read_token_file_env_still_works_for_ingest() {
    let tmp = tempfile::tempdir().unwrap();
    let f = tmp.path().join("t");
    std::fs::write(&f, "legacy\n").unwrap();
    let path = f.to_string_lossy().into_owned();
    let env = move |var: &str| (var == "RELAY_TOKEN_FILE").then(|| path.clone());
    assert_eq!(
        read_token_with("relay-token", env).as_deref(),
        Some("legacy")
    );
}

#[tokio::test]
async fn fetch_user_id_uses_http_login_only() {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v9/users/@me"))
        .and(header("authorization", "tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "4242", "username": "x", "discriminator": "0", "avatar": null, "global_name": null,
        })))
        .expect(1)
        .mount(&server)
        .await;
    let base = server.uri();
    let urls = Urls {
        origin: base.clone(),
        api_base: format!("{base}/api/v9"),
        gateway: format!("{}/gateway", base.replace("http://", "ws://")),
        cordapi: base.clone(),
        login_page: format!("{base}/login"),
        chrome_versions: format!("{base}/chrome/versions"),
    };
    assert_eq!(
        cli::fetch_user_id_at(urls, "tok".into()).await.unwrap(),
        4242
    );
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests
            .iter()
            .any(|r| r.url.path().starts_with("/gateway")),
        "no gateway connection expected"
    );
}

// ---------------------------------------------------------------- doctor

fn doctor_setup(tmp: &Path) -> (Harness, PathBuf) {
    let h = Harness::new().token_dir(tmp, ALL_TOKENS);
    (h, default_short_form(tmp))
}

#[test]
fn doctor_reports_resolved_account_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let (h, cfg) = doctor_setup(tmp.path());
    let r = h.with_ids(IDS).main(&["doctor", "--config", s(&cfg)]);
    assert!(r.out.contains("[ok] account ids resolved"), "{}", r.out);
    assert!(!r.out.contains("tok-"), "{}", r.out);
}

#[test]
fn doctor_reports_failed_account_id_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut h, cfg) = doctor_setup(tmp.path());
    h.lookup = Arc::new(|_| Box::pin(async { Err("bad token".to_string()) }));
    let r = h.main(&["doctor", "--config", s(&cfg)]);
    assert_eq!(r.code, EXIT_FAIL);
    assert!(r.out.contains("[FAIL] account ids: "), "{}", r.out);
    assert!(!r.out.contains("tok-"), "{}", r.out);
}

/// Replaces the encoder/decoder probes of the Python doctor: this build checks the voice stack,
/// the state directory and the operable config, and flags transcode modes it cannot run.
#[test]
fn doctor_checks_passthrough_build() {
    let tmp = tempfile::tempdir().unwrap();
    let text = base_config(&tmp.path().join("state"))
        .replace(
            "# expected_relay_user_id: \"100000000000000001\"",
            "expected_relay_user_id: \"100000000000000001\"",
        )
        .replace("video_mode: passthrough", "video_mode: transcode");
    let cfg = tmp.path().join("c.yaml");
    std::fs::write(&cfg, text).unwrap();
    write_manifests(&cfg);
    let r = Harness::new().main(&["doctor", "--config", s(&cfg)]);
    for needle in [
        "[ok] voice crate: relay-voice",
        "[ok] davey session init",
        "[no] codecs.video_mode transcode",
        "[ok] state directory writable",
        "[FAIL] capability: codecs.video_mode: transcode not supported in this build",
    ] {
        assert!(r.out.contains(needle), "{needle}: {}", r.out);
    }
    assert!(!r.out.contains("account ids"), "{}", r.out);
    assert_eq!(r.code, EXIT_FAIL);
    assert!(tmp.path().join("state").is_dir());
}

#[test]
fn validate_prints_ignored_setting_notes() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path().join("c.yaml");
    std::fs::write(
        &cfg,
        base_config(tmp.path()).replace("audio_bitrate_kbps: 128", "audio_bitrate_kbps: 96"),
    )
    .unwrap();
    let r = Harness::new().main(&["validate", "--config", s(&cfg)]);
    assert_eq!(r.code, 0, "{}", r.err);
    assert!(
        r.out
            .contains("note: quality.audio_bitrate_kbps 96 is ignored"),
        "{}",
        r.out
    );
}
