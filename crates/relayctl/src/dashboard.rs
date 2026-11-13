//! `relayctl dashboard`: public read-only status page. Reads the relay's `status.json` (polled) and
//! `relay.log` (tailed, JSON lines from `relayctl run ... | tee -a relay.log`) and serves `/`
//! (the embedded page) and `/api/status`. Exposes no ids, tokens or controls.

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Args;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;

use crate::metrics::{read_request, reply, reply_with};

const POLL: Duration = Duration::from_secs(5);
/// status.json older than this: the relay process is not running.
const STALE_S: f64 = 30.0;
/// 1 h of 5 s samples.
const SERIES_LEN: usize = 720;
const INCIDENTS_KEPT: usize = 200;
const TRANSITIONS_KEPT_S: f64 = 7.0 * 86400.0;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PAGE: &str = include_str!("../../../deploy/dashboard/index.html");

/// Session states whose time counts against availability (idle/disabled: the target is not live).
fn is_down(state: &str) -> bool {
    matches!(
        state,
        "hold" | "connecting" | "backoff" | "quarantine" | "fatal"
    )
}
const UP: &str = "relaying";

#[derive(Args, Debug)]
pub struct DashboardArgs {
    /// The relay's status.json.
    #[arg(long, default_value = "/var/lib/discord-relay/status.json")]
    pub status: PathBuf,
    /// The relay's log (JSON event lines).
    #[arg(long, default_value = "/opt/discord-relay-rs/relay.log")]
    pub log: PathBuf,
    /// Directory for history.json.
    #[arg(long, default_value = "/var/lib/relay-dashboard")]
    pub data: PathBuf,
    /// Address to serve on.
    #[arg(long, default_value = "127.0.0.1:8787")]
    pub listen: String,
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

#[cfg(unix)]
fn inode(m: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(m.ino())
}

#[cfg(not(unix))]
fn inode(_: &std::fs::Metadata) -> Option<u64> {
    None
}

/// Counters of the previous poll, for rate deltas.
struct Prev {
    t: f64,
    pkts: Option<f64>,
    rtx: Option<f64>,
    frames: Option<f64>,
}

/// (packets/s, frames/s, rtx loss %) between two polls; `None` when a counter is missing or went
/// backwards (the relay restarted).
fn rates(p: &Prev, c: &Prev) -> Option<(f64, f64, f64)> {
    let (Some(cp), Some(cr), Some(cf), Some(pp), Some(pr), Some(pf)) =
        (c.pkts, c.rtx, c.frames, p.pkts, p.rtx, p.frames)
    else {
        return None;
    };
    let dt = c.t - p.t;
    let dp = cp - pp;
    if dt > 0.0 && dp >= 0.0 && cf >= pf {
        let loss = if dp > 0.0 {
            100.0 * (cr - pr) / dp
        } else {
            0.0
        };
        Some((dp / dt, (cf - pf) / dt, loss))
    } else {
        None
    }
}

/// Id of a JSON number or string, as the string key used by `names` and `routing.mirrors`.
fn id_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// target / channel / outputs / mirror_list / free_mirrors from a status.json. Names come from the
/// optional top-level `names` and are null when unknown; user ids are never emitted.
fn topology(s: &Value) -> Map<String, Value> {
    let name = |kind: &str, id: Option<String>| -> Value {
        id.and_then(|id| s["names"][kind][id.as_str()].as_str().map(Value::from))
            .unwrap_or(Value::Null)
    };
    let routing = &s["routing"];
    let mirrors = routing["mirrors"].as_object();
    let assigned: Vec<String> = mirrors
        .map(|m| m.values().filter_map(id_str).collect())
        .unwrap_or_default();
    let rows = s["details"]["accounts"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let mut outputs = Vec::new();
    let mut free = 0;
    for r in rows {
        let (id, role, state) = (id_str(&r[0]), r[1].as_str(), r[3].as_str());
        let channel_id = id_str(&r[2]);
        if role == Some("mirror")
            && state == Some("off")
            && id.as_ref().is_some_and(|id| !assigned.contains(id))
        {
            free += 1;
        }
        outputs.push(json!({
            "name": name("users", id),
            "role": role,
            "state": state,
            "channel": name("channels", channel_id.clone()),
            "channel_id": channel_id,
            "viewers": r[5],
            "uptime_s": r[4],
        }));
    }
    let mirror_list: Vec<Value> = mirrors
        .map(|m| {
            m.iter()
                .map(|(ch, acc)| {
                    json!({
                        "channel": name("channels", Some(ch.clone())),
                        "channel_id": ch,
                        "account": name("users", id_str(acc)),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut out = Map::new();
    out.insert(
        "target".into(),
        json!({"name": name("users", id_str(&routing["target_user_id"]))}),
    );
    out.insert(
        "channel".into(),
        json!({"name": name("channels", id_str(&routing["channel_id"]))}),
    );
    out.insert("outputs".into(), Value::Array(outputs));
    out.insert("mirror_list".into(), Value::Array(mirror_list));
    out.insert("free_mirrors".into(), json!(free));
    out
}

struct State {
    series: VecDeque<Value>,
    prev: Option<Prev>,
    snapshot: Map<String, Value>,
    log_pos: u64,
    log_ino: Option<u64>,
    /// (ts, state) in order; a process start is a "restart" entry.
    transitions: Vec<(f64, String)>,
    incidents: Vec<Value>,
    open_incident: Option<Map<String, Value>>,
    state: Option<String>,
    /// Newest event of any kind: when a killed run was last alive.
    last_ts: Option<f64>,
}

pub struct Collector {
    status_path: PathBuf,
    log_path: PathBuf,
    history_path: PathBuf,
    st: Mutex<State>,
}

impl Collector {
    pub fn new(status_path: &Path, log_path: &Path, data_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let history_path = data_dir.join("history.json");
        let h: Value = std::fs::read(&history_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        let transitions = h["transitions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| Some((t[0].as_f64()?, t[1].as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let st = State {
            series: VecDeque::new(),
            prev: None,
            snapshot: Map::new(),
            log_pos: h["log_pos"].as_u64().unwrap_or(0),
            log_ino: h["log_ino"].as_u64(),
            transitions,
            incidents: h["incidents"].as_array().cloned().unwrap_or_default(),
            open_incident: h["open_incident"].as_object().cloned(),
            state: h["state"].as_str().map(str::to_string),
            last_ts: h["last_ts"].as_f64(),
        };
        Ok(Self {
            status_path: status_path.to_path_buf(),
            log_path: log_path.to_path_buf(),
            history_path,
            st: Mutex::new(st),
        })
    }

    fn save_history(&self, st: &State) -> std::io::Result<()> {
        let doc = json!({
            "log_pos": st.log_pos,
            "log_ino": st.log_ino,
            "transitions": st.transitions.iter().map(|(t, s)| json!([t, s])).collect::<Vec<_>>(),
            "incidents": st.incidents,
            "open_incident": st.open_incident,
            "state": st.state,
            "last_ts": st.last_ts,
        });
        let mut tmp = self.history_path.clone().into_os_string();
        tmp.push(".tmp");
        std::fs::write(&tmp, serde_json::to_vec(&doc)?)?;
        std::fs::rename(&tmp, &self.history_path)
    }

    // --------------------------------------------------------------- log tail

    pub fn tail_log(&self, now: f64) -> std::io::Result<()> {
        let Ok(meta) = std::fs::metadata(&self.log_path) else {
            return Ok(());
        };
        let mut st = self.st.lock();
        let ino = inode(&meta);
        if ino != st.log_ino || meta.len() < st.log_pos {
            st.log_ino = ino;
            st.log_pos = 0; // rotated or truncated
        }
        if meta.len() == st.log_pos {
            return Ok(());
        }
        let mut f = std::fs::File::open(&self.log_path)?;
        f.seek(SeekFrom::Start(st.log_pos))?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        // keep a partial last line for the next read
        let end = data.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        for raw in data[..end].split(|&b| b == b'\n') {
            let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
            st.line(&String::from_utf8_lossy(raw));
        }
        st.log_pos += end as u64;
        let cutoff = now - TRANSITIONS_KEPT_S;
        st.transitions.retain(|t| t.0 >= cutoff);
        if st.incidents.len() > INCIDENTS_KEPT {
            let drop = st.incidents.len() - INCIDENTS_KEPT;
            st.incidents.drain(..drop);
        }
        self.save_history(&st)
    }

    // ------------------------------------------------------------- status poll

    pub fn poll(&self) {
        let read = || -> Option<(Value, f64)> {
            let mtime = std::fs::metadata(&self.status_path).ok()?.modified().ok()?;
            let mtime = mtime.duration_since(UNIX_EPOCH).ok()?.as_secs_f64();
            let s = serde_json::from_slice(&std::fs::read(&self.status_path).ok()?).ok()?;
            Some((s, mtime))
        };
        let now = unix_now();
        self.poll_status(now, read().map(|(s, mtime)| (s, now - mtime)));
    }

    fn poll_status(&self, now: f64, status: Option<(Value, f64)>) {
        let age = status.as_ref().map(|s| s.1);
        let online = status.as_ref().is_some_and(|s| s.1 < STALE_S);
        let mut snap = Map::new();
        snap.insert("now".into(), json!(now));
        snap.insert("online".into(), json!(online));
        snap.insert("status_age_s".into(), json!(age));
        let mut sample = json!({"t": now, "state": null, "pps": null, "fps": null, "loss": null});
        let mut st = self.st.lock();
        match status {
            Some((s, _)) if online => {
                let w = &s["network"]["watch"];
                let cur = Prev {
                    t: now,
                    pkts: w["video_packets_received"].as_f64(),
                    rtx: w["rtx_packets_received"].as_f64(),
                    frames: s["metrics"]["gauges"]["worker.frames_out"].as_f64(),
                };
                let r = st.prev.as_ref().and_then(|p| rates(p, &cur));
                st.prev = Some(cur);
                let accounts: &[Value] = s["accounts"].as_array().map_or(&[], Vec::as_slice);
                let live = |a: &&Value| a["state"] == "live";
                let mirrors = match &s["routing"]["mirrors"] {
                    Value::Object(m) => m.len(),
                    Value::Array(m) => m.len(),
                    _ => 0,
                };
                let (pps, fps, loss) =
                    r.map_or((None, None, None), |(a, b, c)| (Some(a), Some(b), Some(c)));
                for (k, v) in [
                    ("state", s["state"].clone()),
                    ("hold_reason", s["hold_reason"].clone()),
                    ("enabled", s["desired_enabled"].clone()),
                    ("codec", s["codec"].clone()),
                    ("stream_full_s", s["stream_full_s"].clone()),
                    ("session_uptime_s", s["details"]["session_uptime_s"].clone()),
                    ("channel_members", s["details"]["viewers"].clone()),
                    (
                        "relay_viewers",
                        json!(
                            accounts
                                .iter()
                                .filter(live)
                                .map(|a| a["viewers"].as_i64().unwrap_or(0))
                                .sum::<i64>()
                        ),
                    ),
                    ("outputs_live", json!(accounts.iter().filter(live).count())),
                    ("mirrors", json!(mirrors)),
                    ("pps", json!(pps)),
                    ("fps", json!(fps)),
                    ("loss_pct", json!(loss)),
                ] {
                    snap.insert(k.into(), v);
                }
                snap.extend(topology(&s));
                sample =
                    json!({"t": now, "state": s["state"], "pps": pps, "fps": fps, "loss": loss});
            }
            _ => st.prev = None,
        }
        if st.series.len() == SERIES_LEN {
            st.series.pop_front();
        }
        st.series.push_back(sample);
        st.snapshot = snap;
    }

    // ----------------------------------------------------------------- report

    pub fn report(&self, now: f64) -> Value {
        let st = self.st.lock();
        let mut snap = st.snapshot.clone();
        let last = st.transitions.iter().rev().find(|t| t.1 != "restart");
        let since = last
            .filter(|t| snap.get("state").and_then(Value::as_str) == Some(t.1.as_str()))
            .map(|t| t.0);
        snap.insert("state_since".into(), json!(since));
        let from = st.incidents.len().saturating_sub(30);
        let mut incidents: Vec<Value> = st.incidents[from..].to_vec();
        if let Some(open) = &st.open_incident {
            let mut open = open.clone();
            open.insert("ongoing".into(), json!(true));
            incidents.push(Value::Object(open));
        }
        incidents.reverse();
        snap.insert("incidents".into(), Value::Array(incidents));
        snap.insert(
            "availability".into(),
            json!({
                "24h": st.availability(86400.0, now),
                "7d": st.availability(7.0 * 86400.0, now),
            }),
        );
        snap.insert("series".into(), json!(st.series));
        Value::Object(snap)
    }

    fn run(&self) {
        loop {
            std::thread::sleep(POLL);
            self.tick();
        }
    }

    fn tick(&self) {
        // keep serving on error; the page shows the stale age
        if let Err(e) = self.tail_log(unix_now()) {
            eprintln!("collector error: {e:?}");
        }
        self.poll();
    }
}

impl State {
    fn line(&mut self, line: &str) {
        if line.starts_with("account ingest ->") {
            // first line of `relayctl run`; the previous run ended at an unknown time
            self.restart(self.last_ts);
            return;
        }
        let Some(i) = line.find("{\"ts\"") else {
            return;
        };
        let Ok(ev) = serde_json::from_str::<Value>(&line[i..]) else {
            return;
        };
        let Some(ts) = ev["ts"].as_f64() else {
            return;
        };
        self.last_ts = Some(ts);
        match ev["event"].as_str() {
            Some("state") => {
                if let Some(new) = ev["new"].as_str() {
                    self.on_state(ts, new, &ev["hold_reason"]);
                }
            }
            Some("gateway_disconnected") => {
                if let Some(open) = &mut self.open_incident {
                    let notes = open
                        .entry("notes")
                        .or_insert_with(|| Value::Array(Vec::new()));
                    if let Some(a) = notes.as_array_mut() {
                        a.push(json!("gateway"));
                    }
                }
            }
            _ => {}
        }
    }

    fn restart(&mut self, ts: Option<f64>) {
        let ts = ts.filter(|t| *t != 0.0);
        if let (Some(t), Some(_)) = (ts, &self.open_incident) {
            self.close_incident(t, false, true);
        }
        self.state = None;
        if let Some(t) = ts
            && self
                .transitions
                .last()
                .is_none_or(|l| *l != (t, "restart".to_string()))
        {
            self.transitions.push((t, "restart".to_string()));
            self.incidents
                .push(json!({"start": t, "end": t, "kind": "restart"}));
        }
    }

    fn on_state(&mut self, ts: f64, new: &str, hold_reason: &Value) {
        let old = self.state.replace(new.to_string());
        self.transitions.push((ts, new.to_string()));
        let down = is_down(new);
        // a hold or reconnect after relaying, or any failed connect (a normal start passes
        // through `connecting` only)
        if down
            && (old.as_deref() == Some(UP) || new != "connecting")
            && self.open_incident.is_none()
        {
            let mut inc = Map::new();
            inc.insert("start".into(), json!(ts));
            inc.insert("kind".into(), json!(new));
            inc.insert("reason".into(), hold_reason.clone());
            self.open_incident = Some(inc);
        } else if down && let Some(open) = &mut self.open_incident {
            let has_reason = open
                .get("reason")
                .and_then(Value::as_str)
                .is_some_and(|r| !r.is_empty());
            if !has_reason && hold_reason.as_str().is_some_and(|r| !r.is_empty()) {
                open.insert("reason".into(), hold_reason.clone());
            }
        } else if !down && self.open_incident.is_some() {
            self.close_incident(ts, new == UP, false);
        }
    }

    fn close_incident(&mut self, ts: f64, recovered: bool, by_restart: bool) {
        let Some(mut inc) = self.open_incident.take() else {
            return;
        };
        inc.insert("end".into(), json!(ts));
        inc.insert("recovered".into(), json!(recovered));
        if by_restart {
            inc.insert("by_restart".into(), json!(true));
        }
        if let Some(notes) = inc.get_mut("notes").and_then(Value::as_array_mut) {
            let mut names: Vec<String> = notes
                .iter()
                .filter_map(|n| n.as_str().map(str::to_string))
                .collect();
            names.sort();
            names.dedup();
            *notes = names.into_iter().map(Value::from).collect();
        }
        self.incidents.push(Value::Object(inc));
    }

    fn availability(&self, window_s: f64, now: f64) -> Value {
        let start = now - window_s;
        let (mut up, mut down) = (0.0_f64, 0.0_f64);
        let tr = &self.transitions;
        for (i, (ts, state)) in tr.iter().enumerate() {
            let end = tr.get(i + 1).map_or(now, |n| n.0);
            let (a, b) = (ts.max(start), end.min(now));
            if b <= a {
                continue;
            }
            if state == UP {
                up += b - a;
            } else if is_down(state) {
                down += b - a;
            }
        }
        let total = up + down;
        let pct = (total > 0.0).then(|| 100.0 * up / total);
        json!({"pct": pct, "up_s": up, "down_s": down})
    }
}

// ------------------------------------------------------------------ HTTP

async fn answer(mut stream: tokio::net::TcpStream, collector: &Collector) -> std::io::Result<()> {
    let Some((method, path)) = read_request(&mut stream).await? else {
        return Ok(());
    };
    const HEADERS: &str = "X-Content-Type-Options: nosniff\r\n";
    let page_headers = format!("Cache-Control: no-cache\r\n{HEADERS}");
    let api_headers = format!("Cache-Control: no-store\r\n{HEADERS}");
    match (method.as_str(), path.as_str()) {
        ("GET", "/" | "/index.html") => {
            reply_with(
                &mut stream,
                200,
                "OK",
                "text/html; charset=utf-8",
                &page_headers,
                PAGE.as_bytes(),
            )
            .await
        }
        ("GET", "/api/status") => {
            let body = serde_json::to_vec(&collector.report(unix_now()))?;
            reply_with(
                &mut stream,
                200,
                "OK",
                "application/json",
                &api_headers,
                &body,
            )
            .await
        }
        ("GET", _) => {
            reply(
                &mut stream,
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                "not found\n",
            )
            .await
        }
        _ => {
            reply(
                &mut stream,
                405,
                "Method Not Allowed",
                "text/plain; charset=utf-8",
                "method not allowed\n",
            )
            .await
        }
    }
}

async fn serve(listener: TcpListener, collector: Arc<Collector>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let collector = Arc::clone(&collector);
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(REQUEST_TIMEOUT, answer(stream, &collector)).await;
                });
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Run until the process is stopped; the return value is the exit code on startup failure.
pub fn run(args: &DashboardArgs) -> i32 {
    let collector = match Collector::new(&args.status, &args.log, &args.data) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("dashboard: {e}");
            return 1;
        }
    };
    collector.tick();
    let background = Arc::clone(&collector);
    std::thread::spawn(move || background.run());
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("dashboard: {e}");
            return 1;
        }
    };
    rt.block_on(async {
        let listener = match TcpListener::bind(&args.listen).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("dashboard: cannot listen on {}: {e}", args.listen);
                return 1;
            }
        };
        println!("dashboard on http://{}", args.listen);
        serve(listener, collector).await;
        0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector(dir: &Path) -> Collector {
        Collector::new(&dir.join("status.json"), &dir.join("relay.log"), dir).unwrap()
    }

    fn ev(c: &Collector, ts: f64, new: &str, reason: Option<&str>) {
        let line = json!({"ts": ts, "event": "state", "new": new, "hold_reason": reason});
        c.st.lock().line(&format!("2025-01-01 INFO {line}"));
    }

    #[test]
    fn transitions_open_and_close_incidents() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        // a normal start passes through connecting only: no incident
        ev(&c, 100.0, "connecting", None);
        ev(&c, 110.0, "relaying", None);
        assert!(c.st.lock().incidents.is_empty());
        ev(&c, 200.0, "hold", None);
        ev(&c, 210.0, "hold", Some("target_left"));
        c.st.lock()
            .line(r#"{"ts":220,"event":"gateway_disconnected"}"#);
        c.st.lock()
            .line(r#"{"ts":221,"event":"gateway_disconnected"}"#);
        ev(&c, 300.0, "relaying", None);
        let st = c.st.lock();
        assert!(st.open_incident.is_none());
        assert_eq!(
            st.incidents,
            vec![
                json!({"start": 200.0, "kind": "hold", "reason": "target_left", "end": 300.0,
                        "recovered": true, "notes": ["gateway"]})
            ]
        );
    }

    #[test]
    fn restart_closes_an_open_hold() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        ev(&c, 100.0, "relaying", None);
        ev(&c, 200.0, "hold", Some("x"));
        c.st.lock().line("account ingest -> 1");
        let st = c.st.lock();
        assert!(st.open_incident.is_none() && st.state.is_none());
        assert_eq!(
            st.incidents,
            vec![
                json!({"start": 200.0, "kind": "hold", "reason": "x", "end": 200.0,
                       "recovered": false, "by_restart": true}),
                json!({"start": 200.0, "end": 200.0, "kind": "restart"}),
            ]
        );
        assert_eq!(st.transitions.last(), Some(&(200.0, "restart".to_string())));
    }

    #[test]
    fn availability_counts_up_and_down_in_window() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        c.st.lock().transitions = vec![
            (0.0, "relaying".into()),
            (600.0, "hold".into()),
            (700.0, "idle".into()),
            (800.0, "relaying".into()),
        ];
        let st = c.st.lock();
        // now = 1000: up 600 + 200, down 100, idle ignored
        let a = st.availability(86400.0, 1000.0);
        assert_eq!(a["up_s"], 800.0);
        assert_eq!(a["down_s"], 100.0);
        assert!((a["pct"].as_f64().unwrap() - 100.0 * 800.0 / 900.0).abs() < 1e-9);
        // window 300 s: only [700, 1000]: up 200, down 0
        let a = st.availability(300.0, 1000.0);
        assert_eq!(
            (a["up_s"].as_f64(), a["down_s"].as_f64()),
            (Some(200.0), Some(0.0))
        );
        assert!(st.availability(50.0, 750.0)["pct"].is_null());
    }

    fn status(pkts: u64, rtx: u64, frames: u64) -> Value {
        json!({"state": "relaying", "network": {"watch": {"video_packets_received": pkts,
               "rtx_packets_received": rtx}}, "metrics": {"gauges": {"worker.frames_out": frames}},
               "accounts": [{"state": "live", "viewers": 3}, {"state": "idle", "viewers": 9}]})
    }

    #[test]
    fn stream_full_is_passed_through() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        let mut s = status(1, 0, 1);
        s["stream_full_s"] = json!(42.5);
        c.poll_status(100.0, Some((s, 1.0)));
        assert_eq!(c.st.lock().snapshot["stream_full_s"], 42.5);
        c.poll_status(105.0, Some((status(1, 0, 1), 1.0)));
        assert!(c.st.lock().snapshot["stream_full_s"].is_null());
        c.poll_status(110.0, None);
        assert!(!c.st.lock().snapshot.contains_key("stream_full_s"));
    }

    #[test]
    fn rate_deltas_and_counter_reset() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        c.poll_status(100.0, Some((status(1000, 10, 100), 1.0)));
        assert!(c.st.lock().snapshot["pps"].is_null());
        c.poll_status(105.0, Some((status(1500, 20, 250), 1.0)));
        {
            let st = c.st.lock();
            assert_eq!(st.snapshot["pps"], 100.0);
            assert_eq!(st.snapshot["fps"], 30.0);
            assert_eq!(st.snapshot["loss_pct"], 2.0);
            assert_eq!(st.snapshot["relay_viewers"], 3);
            assert_eq!(st.snapshot["outputs_live"], 1);
        }
        // counters went backwards: the relay restarted, no rate
        c.poll_status(110.0, Some((status(10, 0, 5), 1.0)));
        assert!(c.st.lock().snapshot["pps"].is_null());
        // stale status: offline, previous counters dropped
        c.poll_status(115.0, Some((status(20, 0, 10), 60.0)));
        let st = c.st.lock();
        assert_eq!(st.snapshot["online"], false);
        assert!(st.prev.is_none());
        assert_eq!(st.series.len(), 4);
    }

    #[test]
    fn partial_last_line_waits_for_its_newline() {
        let dir = tempfile::tempdir().unwrap();
        let c = collector(dir.path());
        let log = dir.path().join("relay.log");
        let first = r#"{"ts":100,"event":"state","new":"relaying"}"#;
        let second = r#"{"ts":200,"event":"state","new":"hold","hold_reason":"r"}"#;
        std::fs::write(&log, format!("{first}\n{}", &second[..20])).unwrap();
        c.tail_log(300.0).unwrap();
        assert_eq!(c.st.lock().transitions.len(), 1);
        assert_eq!(c.st.lock().log_pos, first.len() as u64 + 1);
        std::fs::write(&log, format!("{first}\n{second}\n")).unwrap();
        c.tail_log(300.0).unwrap();
        let st = c.st.lock();
        assert_eq!(st.transitions.len(), 2);
        assert_eq!(st.open_incident.as_ref().unwrap()["reason"], "r");
        drop(st);
        // history survives a reload
        let again = collector(dir.path());
        assert_eq!(again.st.lock().transitions.len(), 2);
        assert!(again.st.lock().open_incident.is_some());
    }

    fn topo_status(with_names: bool) -> Value {
        let mut s = json!({
            "state": "relaying",
            "routing": {"target_user_id": "11", "channel_id": "500",
                        "mirrors": {"501": 22}},
            "details": {"accounts": [
                [21, "relay", 500, "live", 90.5, 4],
                [22, "mirror", 501, "live", 30.0, 1],
                [23, "mirror", null, "off", null, null],
                [24, "mirror", null, "off", null, null],
                [25, "mirror", null, "starting", null, null],
            ]},
        });
        if with_names {
            s["names"] = json!({"users": {"11": "Tai", "21": "Relay1", "22": "Mir<b>"},
                                "channels": {"500": "Main", "501": "Side"}});
        }
        s
    }

    #[test]
    fn topology_with_names() {
        let t = topology(&topo_status(true));
        assert_eq!(t["target"], json!({"name": "Tai"}));
        assert_eq!(t["channel"], json!({"name": "Main"}));
        assert_eq!(t["free_mirrors"], 2);
        assert_eq!(
            t["mirror_list"],
            json!([{"channel": "Side", "channel_id": "501", "account": "Mir<b>"}])
        );
        let o = t["outputs"].as_array().unwrap();
        assert_eq!(o.len(), 5);
        assert_eq!(
            o[0],
            json!({"name": "Relay1", "role": "relay", "state": "live", "channel": "Main",
                   "channel_id": "500", "viewers": 4, "uptime_s": 90.5})
        );
        assert_eq!(o[2]["name"], Value::Null);
        assert_eq!(o[2]["channel_id"], Value::Null);
        // no account id leaks
        let text = Value::Object(t).to_string();
        assert!(!text.contains("\"account\":22") && !text.contains("\"11\""));
    }

    #[test]
    fn topology_without_names_falls_back_to_null() {
        let t = topology(&topo_status(false));
        assert_eq!(t["target"], json!({"name": null}));
        assert_eq!(t["channel"], json!({"name": null}));
        assert_eq!(t["outputs"][1]["channel"], Value::Null);
        assert_eq!(t["outputs"][1]["channel_id"], "501");
        assert_eq!(t["mirror_list"][0]["account"], Value::Null);
        assert_eq!(t["free_mirrors"], 2);
    }
}
