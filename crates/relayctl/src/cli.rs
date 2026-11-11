//! `relayctl`: validate | doctor | run | status | disable | enable | reset-fatal, port of
//! `control/cli.py` (spec §16).
//!
//! Everything a test needs to replace (output streams, environment, the user-id lookup, the service
//! itself) comes in through [`Ctx`]; [`run_process`] wires the real ones.

use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use clap::{Args, Parser, Subcommand};
use relay_core::config::{
    AudioMode, ConfigInvalid, RelayConfig, VideoMode, apply_account_ids, check_operable,
    ignored_setting_notes, load_config_with_warnings, unresolved_credentials,
};
use relay_core::control::state::{PersistedState, StateError, StateStore, set_account};
use relay_discord::{RestClient, Token, Urls};
use relay_voice::dave::{Dave, DaveOut};
use serde_json::{Map, Value, json};

use crate::tokens::{load_token_with, read_token_with};

pub use crate::service::EXIT_CONFIG;

pub const EXIT_OK: i32 = 0;
pub const EXIT_FAIL: i32 = 1;
pub const DEFAULT_STATE_DIR: &str = "/var/lib/discord-relay";
pub const SOCKET_NAME: &str = "control.sock";
/// `relayctl run` reads the metrics address from here when `--metrics-listen` is not given.
pub const METRICS_LISTEN_ENV: &str = "RELAY_METRICS_LISTEN";

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check the config file and print every problem.
    Validate(ConfigArgs),
    /// Check that this machine and config can run the relay.
    Doctor(ConfigArgs),
    /// Run the relay service.
    Run(RunArgs),
    /// Print the service status (or the persisted state when it is not running).
    Status(StateArgs),
    /// Stop relaying.
    Disable(StateArgs),
    /// Start relaying.
    Enable(StateArgs),
    /// Clear a fatal latch.
    ResetFatal(ResetFatalArgs),
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    #[arg(long)]
    pub config: PathBuf,
    /// Serve GET /healthz and GET /metrics (Prometheus) over plain HTTP on this IP:port, e.g.
    /// 127.0.0.1:9108. Off by default; also read from RELAY_METRICS_LISTEN.
    #[arg(long, value_name = "IP:PORT")]
    pub metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct StateArgs {
    #[arg(long, default_value = DEFAULT_STATE_DIR)]
    pub state_dir: PathBuf,
}

#[derive(Debug, Args)]
pub struct ResetFatalArgs {
    #[command(flatten)]
    pub state: StateArgs,
    /// Output account user ID (default: the ingest account).
    #[arg(long, value_parser = account_id)]
    pub account: Option<u64>,
}

fn account_id(v: &str) -> Result<u64, String> {
    v.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| v.parse::<u64>().ok())
        .flatten()
        .filter(|id| *id > 0)
        .ok_or_else(|| "must be a decimal user ID".to_string())
}

#[derive(Debug, Parser)]
#[command(name = "relayctl")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

pub type Env<'a> = dyn Fn(&str) -> Option<String> + 'a;
pub type Lookup<'a> =
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send>> + 'a;
pub type RunService<'a> =
    dyn Fn(RelayConfig, Token, Option<SocketAddr>) -> Pin<Box<dyn Future<Output = i32>>> + 'a;

/// The process around a command: output streams, environment, the token -> user-id lookup and the
/// service entry.
pub struct Ctx<'a> {
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
    pub env: &'a Env<'a>,
    pub lookup: &'a Lookup<'a>,
    pub run_service: &'a RunService<'a>,
}

impl Ctx<'_> {
    fn say(&mut self, line: &str) {
        let _ = writeln!(self.out, "{line}");
    }

    fn warn(&mut self, line: &str) {
        let _ = writeln!(self.err, "{line}");
    }
}

/// `main(argv)`: `argv` without the program name; parse errors print usage and return clap's code.
pub fn main<I, T>(argv: I, ctx: &mut Ctx<'_>) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args = std::iter::once(OsString::from("relayctl")).chain(argv.into_iter().map(Into::into));
    match Cli::try_parse_from(args) {
        Ok(cli) => run(cli.command, ctx),
        Err(e) => {
            let text = e.render().to_string();
            if e.use_stderr() {
                ctx.warn(text.trim_end());
            } else {
                ctx.say(text.trim_end());
            }
            e.exit_code()
        }
    }
}

pub fn run(command: Command, ctx: &mut Ctx<'_>) -> i32 {
    match command {
        Command::Validate(a) => validate(&a.config, ctx),
        Command::Doctor(a) => doctor(&a.config, ctx),
        Command::Run(a) => run_relay(&a, ctx),
        Command::Status(a) => status(&a.state_dir, ctx),
        Command::Disable(a) => state_command("disable", &a.state_dir, None, ctx),
        Command::Enable(a) => state_command("enable", &a.state_dir, None, ctx),
        Command::ResetFatal(a) => state_command("reset-fatal", &a.state.state_dir, a.account, ctx),
    }
}

/// [`run`] with the real stdout/stderr, environment, Discord login and service.
pub fn run_process(command: Command) -> i32 {
    let env = |var: &str| std::env::var(var).ok();
    let lookup = |token: String| -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send>> {
        Box::pin(fetch_user_id(token))
    };
    let run_service = |cfg: RelayConfig,
                       token: Token,
                       metrics_listen: Option<SocketAddr>|
     -> Pin<Box<dyn Future<Output = i32>>> {
        Box::pin(crate::service::run_service(cfg, token, metrics_listen))
    };
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    run(
        command,
        &mut Ctx {
            out: &mut out,
            err: &mut err,
            env: &env,
            lookup: &lookup,
            run_service: &run_service,
        },
    )
}

fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

// ---------------------------------------------------------------- state commands

/// Whether a running service may be listening: its socket file on Unix. A Windows named pipe has no
/// file, so there [`send_request`] finds out (no pipe: `None`).
fn may_listen(socket: &Path) -> bool {
    cfg!(windows) || socket.exists()
}

/// The running service's reply to one request on `control.sock`, `None` when it cannot be reached.
///
/// One request line, one response line, 5 s timeouts. On Windows the request goes over the service's
/// named pipe ([`pipe_name`](relay_core::control::server::pipe_name) of the same path).
#[cfg(unix)]
fn send_request(socket: &Path, request: &Value) -> Option<Value> {
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let timeout = Some(Duration::from_secs(5));
    let mut stream = UnixStream::connect(socket).ok()?;
    stream.set_read_timeout(timeout).ok()?;
    stream.set_write_timeout(timeout).ok()?;
    writeln!(stream, "{request}").ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    serde_json::from_str(&line).ok()
}

#[cfg(windows)]
fn send_request(socket: &Path, request: &Value) -> Option<Value> {
    let name = relay_core::control::server::pipe_name(socket);
    let line = format!("{request}\n");
    // Own thread and runtime: the caller may already be inside one.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        rt.block_on(pipe_request(&name, &line))
    })
    .join()
    .ok()
    .flatten()
}

#[cfg(windows)]
async fn pipe_request(name: &str, line: &str) -> Option<Value> {
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::windows::named_pipe::ClientOptions;

    const ERROR_PIPE_BUSY: i32 = 231;
    let exchange = async {
        let mut pipe = loop {
            match ClientOptions::new().open(name) {
                Ok(pipe) => break pipe,
                // every instance is taken: the service is creating the next one
                Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(_) => return None,
            }
        };
        pipe.write_all(line.as_bytes()).await.ok()?;
        let mut resp = String::new();
        BufReader::new(pipe).read_line(&mut resp).await.ok()?;
        serde_json::from_str(&resp).ok()
    };
    tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .ok()
        .flatten()
}

#[cfg(not(any(unix, windows)))]
fn send_request(_socket: &Path, _request: &Value) -> Option<Value> {
    None
}

/// Python `type(exc).__name__` for the offline errors.
fn error_kind(e: &StateError) -> &'static str {
    match e {
        StateError::Corrupt(_) => "StateCorrupt",
        StateError::Conflict(_) => "RevisionConflict",
        StateError::Io(_) => "OSError",
    }
}

fn latch_is_rejection(latch: &Map<String, Value>) -> bool {
    latch.get("reason").and_then(Value::as_str) == Some("RELAY_REJECTED_BY_SERVER")
}

fn clear_latch(mut s: PersistedState) -> PersistedState {
    if let Some(latch) = s.fatal_latch.take()
        && latch_is_rejection(&latch)
    {
        s.consecutive_rejections = 0;
    }
    s
}

fn clear_account_latch(s: PersistedState, user_id: u64) -> PersistedState {
    let zero = match s.accounts.get(&user_id.to_string()) {
        Some(rec) => match &rec.fatal_latch {
            Some(latch) => latch_is_rejection(latch),
            None => return s,
        },
        None => return s,
    };
    set_account(s, user_id, |rec| {
        rec.fatal_latch = None;
        if zero {
            rec.consecutive_rejections = 0;
        }
    })
}

fn account_latch_set(st: &PersistedState, user_id: Option<u64>) -> bool {
    match user_id {
        None => st.fatal_latch.is_some(),
        Some(uid) => st.account_record(uid).fatal_latch.is_some(),
    }
}

fn state_command(cmd: &str, state_dir: &Path, account: Option<u64>, ctx: &mut Ctx<'_>) -> i32 {
    let sock = state_dir.join(SOCKET_NAME);
    if may_listen(&sock) {
        let request = match account {
            None => json!({"cmd": cmd}),
            Some(uid) => json!({"cmd": cmd, "account": uid.to_string()}),
        };
        // Unreachable (stale socket): fall through to the direct path.
        if let Some(resp) = send_request(&sock, &request) {
            if resp.get("ok").and_then(Value::as_bool) == Some(true) {
                ctx.say(&serde_json::to_string_pretty(&resp).unwrap_or_default());
                return EXIT_OK;
            }
            let why = resp
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("service refused");
            ctx.warn(&format!("{cmd} failed: {why}"));
            return EXIT_FAIL;
        }
    }
    let store = StateStore::new(state_dir);
    let result = if cmd == "reset-fatal" {
        match store.load() {
            Ok(st) if !account_latch_set(&st, account) => {
                ctx.say("no fatal latch set");
                return EXIT_OK;
            }
            Ok(_) => store.update(|s| match account {
                None => clear_latch(s),
                Some(uid) => clear_account_latch(s, uid),
            }),
            Err(e) => Err(e),
        }
    } else {
        let enabled = cmd == "enable";
        store.update(|mut s| {
            s.desired_enabled = enabled;
            s
        })
    };
    match result {
        Ok(st) => {
            ctx.say(&format!(
                "{{\"ok\": true, \"revision\": {}, \"desired_enabled\": {}}}",
                st.revision, st.desired_enabled
            ));
            EXIT_OK
        }
        Err(e) => {
            ctx.warn(&format!("{cmd} failed: {}: {e}", error_kind(&e)));
            EXIT_FAIL
        }
    }
}

fn accounts_view(st: &PersistedState) -> Value {
    let view: Map<String, Value> = st
        .accounts
        .iter()
        .map(|(uid, rec)| {
            let fatal = rec
                .fatal_latch
                .as_ref()
                .and_then(|l| l.get("reason"))
                .cloned()
                .unwrap_or(Value::Null);
            (
                uid.clone(),
                json!({
                    "fatal": fatal,
                    "quarantine_incarnation": rec.quarantine_incarnation,
                    "consecutive_rejections": rec.consecutive_rejections,
                }),
            )
        })
        .collect();
    Value::Object(view)
}

fn status(state_dir: &Path, ctx: &mut Ctx<'_>) -> i32 {
    let sock = state_dir.join(SOCKET_NAME);
    if may_listen(&sock)
        && let Some(resp) = send_request(&sock, &json!({"cmd": "status"}))
    {
        ctx.say(&serde_json::to_string_pretty(&resp).unwrap_or_default());
        return EXIT_OK;
    }
    let data = match std::fs::read_to_string(state_dir.join("status.json")) {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(mut data) => {
                if let Value::Object(m) = &mut data {
                    m.insert(
                        "source".into(),
                        json!("status.json (service may not be running)"),
                    );
                }
                data
            }
            Err(e) => {
                ctx.warn(&format!("status failed: {e}"));
                return EXIT_FAIL;
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match StateStore::new(state_dir).load() {
                Ok(st) => {
                    let mut m = Map::new();
                    m.insert("source".into(), json!("state.json (service not running)"));
                    if let Value::Object(fields) = st.to_value() {
                        m.extend(fields);
                    }
                    m.insert("accounts".into(), accounts_view(&st));
                    Value::Object(m)
                }
                Err(e) => {
                    ctx.warn(&format!("status failed: {e}"));
                    return EXIT_FAIL;
                }
            }
        }
        Err(e) => {
            ctx.warn(&format!("status failed: {e}"));
            return EXIT_FAIL;
        }
    };
    ctx.say(&serde_json::to_string_pretty(&data).unwrap_or_default());
    EXIT_OK
}

// ---------------------------------------------------------------- config commands

fn load(path: &Path, ctx: &mut Ctx<'_>) -> Option<RelayConfig> {
    match load_config_with_warnings(path) {
        Ok((cfg, warnings)) => {
            for w in warnings {
                ctx.warn(&format!("warning: {w}"));
            }
            Some(cfg)
        }
        Err(e) => {
            for line in e.errors {
                ctx.warn(&line);
            }
            None
        }
    }
}

/// Manifests are looked up next to the config file unless `name` is absolute.
fn manifest(cfg_path: &Path, name: &str) -> Result<Value, String> {
    let mut p = PathBuf::from(name);
    if !p.is_absolute() {
        p = cfg_path.parent().unwrap_or(Path::new("")).join(p);
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", p.display()))
}

fn operable_errors(cfg: &RelayConfig, cfg_path: &Path) -> Vec<String> {
    let manifests =
        manifest(cfg_path, &cfg.liveness.verified_capability_manifest).and_then(|cap| {
            Ok((
                cap,
                manifest(cfg_path, &cfg.quality.approved_profile_manifest)?,
            ))
        });
    match manifests {
        Ok((cap, prof)) => check_operable(cfg, &cap, &prof),
        Err(e) => vec![format!("manifest: {e}")],
    }
}

fn validate(cfg_path: &Path, ctx: &mut Ctx<'_>) -> i32 {
    let Some(cfg) = load(cfg_path, ctx) else {
        return EXIT_FAIL;
    };
    ctx.say("config valid");
    let unresolved = unresolved_credentials(&cfg);
    if !unresolved.is_empty() {
        ctx.say(&format!(
            "note: user ids of {} are resolved at run time from their tokens; id-dependent checks skipped",
            unresolved.join(", ")
        ));
    }
    for note in ignored_setting_notes(&cfg) {
        ctx.say(&note);
    }
    for p in operable_errors(&cfg, cfg_path) {
        ctx.say(&format!("not operable: {p}"));
    }
    EXIT_OK
}

/// DAVE needs a working MLS session: create one and its key package, as a SESSION_DESCRIPTION would.
fn davey_probe() -> Result<(), String> {
    let dave = Dave::new(1, 1);
    let out = dave
        .on_session_description(u64::from(Dave::max_protocol_version()))
        .map_err(|e| e.to_string())?;
    if out.iter().any(|o| matches!(o, DaveOut::KeyPackage(_))) {
        Ok(())
    } else {
        Err("no key package".to_string())
    }
}

/// Probe write: permission bits alone do not tell whether we may create files there.
fn dir_writable(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let probe = dir.join(format!(".relayctl-doctor-{}", std::process::id()));
    std::fs::write(&probe, b"")?;
    std::fs::remove_file(&probe)
}

fn doctor(cfg_path: &Path, ctx: &mut Ctx<'_>) -> i32 {
    let Some(mut cfg) = load(cfg_path, ctx) else {
        return EXIT_FAIL;
    };
    let mut failed = false;
    let mut report = |ctx: &mut Ctx<'_>, name: &str, ok: bool, detail: &str| {
        failed |= !ok;
        let tag = if ok { "ok" } else { "FAIL" };
        let sep = if detail.is_empty() { "" } else { ": " };
        ctx.say(&format!("[{tag}] {name}{sep}{detail}"));
    };

    report(
        ctx,
        "voice crate",
        true,
        &format!(
            "relay-voice (DAVE protocol v{})",
            Dave::max_protocol_version()
        ),
    );
    match davey_probe() {
        Ok(()) => report(ctx, "davey session init", true, ""),
        Err(e) => report(ctx, "davey session init", false, &e),
    }
    // Passthrough-only build: a transcode mode cannot run (also reported under capability below).
    if cfg.codecs.video_mode == VideoMode::Transcode {
        ctx.say("[no] codecs.video_mode transcode: not supported in this build (passthrough only)");
    }
    if cfg.codecs.audio_mode == AudioMode::Transcode {
        ctx.say("[no] codecs.audio_mode transcode: not supported in this build (passthrough only)");
    }
    if !unresolved_credentials(&cfg).is_empty() {
        match resolve_ids(cfg.clone(), ctx) {
            Ok(resolved) => {
                cfg = resolved;
                report(ctx, "account ids resolved", true, "");
            }
            Err(e) => {
                for line in e.errors {
                    report(ctx, "account ids", false, &line);
                }
            }
        }
    }
    let sd = cfg.storage.state_directory.clone();
    match dir_writable(&sd) {
        Ok(()) => report(
            ctx,
            "state directory writable",
            true,
            &sd.display().to_string(),
        ),
        Err(e) => report(ctx, "state directory writable", false, &e.to_string()),
    }
    for p in operable_errors(&cfg, cfg_path) {
        report(ctx, "capability", false, &p);
    }
    if failed { EXIT_FAIL } else { EXIT_OK }
}

// ---------------------------------------------------------------- account ids

/// Who the token belongs to: the HTTP login (`GET /users/@me`, same client fingerprint as the
/// gateway login), without opening a gateway connection.
pub async fn fetch_user_id(token: String) -> Result<u64, String> {
    fetch_user_id_at(Urls::default(), token).await
}

/// [`fetch_user_id`] against `urls`.
pub async fn fetch_user_id_at(urls: Urls, token: String) -> Result<u64, String> {
    let rest = RestClient::new(urls, Token::new(token))
        .await
        .map_err(|e| e.to_string())?;
    rest.login().await.map(|u| u.id).map_err(|e| e.to_string())
}

/// Look up every missing user ID concurrently (one try each) and return the fully resolved config.
/// `tokens` maps each unresolved credential to its token; only credential names reach `err`.
pub async fn resolve_account_ids(
    cfg: &RelayConfig,
    tokens: &HashMap<String, String>,
    lookup: &Lookup<'_>,
    err: &mut dyn Write,
) -> Result<RelayConfig, ConfigInvalid> {
    let names = unresolved_credentials(cfg);
    let tasks: Vec<_> = names
        .iter()
        .map(|n| tokio::spawn(lookup(tokens[n].clone())))
        .collect();
    let mut errors = Vec::new();
    let mut ids = HashMap::new();
    for (name, task) in names.iter().zip(tasks) {
        match task.await.unwrap_or_else(|e| Err(e.to_string())) {
            Ok(id) => {
                ids.insert(name.clone(), id);
            }
            Err(detail) => {
                let detail = detail.replace(&tokens[name], "<token>");
                errors.push(format!(
                    "credential {name}: cannot resolve user id ({detail})"
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err(ConfigInvalid { errors });
    }
    for name in &names {
        let _ = writeln!(err, "account {name} -> {}", ids[name]);
    }
    apply_account_ids(cfg, &ids)
}

/// `cfg` with every user ID filled in; the error names each credential that failed.
fn resolve_ids(cfg: RelayConfig, ctx: &mut Ctx<'_>) -> Result<RelayConfig, ConfigInvalid> {
    let names = unresolved_credentials(&cfg);
    if names.is_empty() {
        return Ok(cfg);
    }
    let env = ctx.env;
    let mut tokens = HashMap::new();
    let mut errors = Vec::new();
    for name in names {
        if name == cfg.discord.ingest_credential {
            match read_token_with(&name, env) {
                Some(t) => {
                    tokens.insert(name, t);
                }
                None => errors.push(format!(
                    "credential {name}: no token (set RELAY_TOKEN_DIR or systemd LoadCredential)"
                )),
            }
        } else {
            match load_token_with(&name, env) {
                Ok(t) => {
                    tokens.insert(name, t);
                }
                Err(e) => errors.push(format!("credential {name}: {e}")),
            }
        }
    }
    if !errors.is_empty() {
        return Err(ConfigInvalid { errors });
    }
    let rt = runtime().map_err(|e| ConfigInvalid {
        errors: vec![format!("cannot start runtime: {e}")],
    })?;
    rt.block_on(resolve_account_ids(&cfg, &tokens, ctx.lookup, ctx.err))
}

fn run_relay(args: &RunArgs, ctx: &mut Ctx<'_>) -> i32 {
    let metrics_listen = match args.metrics_listen {
        Some(addr) => Some(addr),
        None => match (ctx.env)(METRICS_LISTEN_ENV).filter(|v| !v.trim().is_empty()) {
            None => None,
            Some(v) => match v.trim().parse::<SocketAddr>() {
                Ok(addr) => Some(addr),
                Err(_) => {
                    ctx.warn(&format!(
                        "{METRICS_LISTEN_ENV}: not an IP:port address: {}",
                        v.trim()
                    ));
                    return EXIT_CONFIG;
                }
            },
        },
    };
    let cfg_path = args.config.as_path();
    let Some(cfg) = load(cfg_path, ctx) else {
        return EXIT_CONFIG;
    };
    let cfg = match resolve_ids(cfg, ctx) {
        Ok(cfg) => cfg,
        Err(e) => {
            for line in e.errors {
                ctx.warn(&line);
            }
            return EXIT_CONFIG;
        }
    };
    let problems = operable_errors(&cfg, cfg_path);
    if !problems.is_empty() {
        for p in problems {
            ctx.warn(&format!("not operable: {p}"));
        }
        return EXIT_CONFIG;
    }
    let Some(token) = read_token_with(&cfg.discord.ingest_credential, ctx.env) else {
        ctx.warn(
            "no credential: set RELAY_TOKEN_FILE or run under systemd LoadCredential=relay-token",
        );
        return EXIT_CONFIG;
    };
    let rt = match runtime() {
        Ok(rt) => rt,
        Err(e) => {
            ctx.warn(&format!("service unavailable: {e}"));
            return EXIT_FAIL;
        }
    };
    rt.block_on((ctx.run_service)(cfg, Token::new(token), metrics_listen))
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use std::sync::Arc;

    use relay_core::control::server::ControlServer;

    use super::*;

    #[tokio::test]
    async fn send_request_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        let mut srv = ControlServer::create(
            &sock,
            Arc::new(|req| json!({"ok": true, "echo": req["cmd"]})),
        )
        .unwrap();
        srv.start().await.unwrap();
        let path = sock.clone();
        let resp =
            tokio::task::spawn_blocking(move || send_request(&path, &json!({"cmd": "status"})))
                .await
                .unwrap();
        assert_eq!(resp, Some(json!({"ok": true, "echo": "status"})));
        srv.close();
        let resp =
            tokio::task::spawn_blocking(move || send_request(&sock, &json!({"cmd": "status"})))
                .await
                .unwrap();
        assert_eq!(resp, None);
    }
}
