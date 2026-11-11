//! `relayctl calibrate-tdead`: derive `source_dead_timeout_seconds` from watch logs (port of
//! `scripts/calibrate_tdead.py`, spec §10/18).
//!
//! Logs are JSON lines (non-JSON lines are ignored). Each `"stats"` event carries
//! `streams["<ssrc>/<audio|video>"].max_gap_s`, cumulative per run, so the last value per stream is
//! that run's maximum. Recommended T_dead = `max(3.0, ceil_to_0.5(2 * observed_max_gap + 1.0))`.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Args;
use serde_json::{Map, Value};

pub const MIN_T_DEAD: f64 = 3.0;

#[derive(Debug, Args)]
pub struct CalibrateArgs {
    /// JSON-lines logs of watched runs.
    #[arg(required = true)]
    pub logs: Vec<PathBuf>,
    /// Minimum minutes of watched source needed.
    #[arg(long, default_value_t = 10.0)]
    pub min_minutes: f64,
    /// Accept less than --min-minutes of evidence.
    #[arg(long)]
    pub force: bool,
    /// Update --config.
    #[arg(long, requires = "config")]
    pub write: bool,
    /// Config YAML to update with --write.
    #[arg(long)]
    pub config: Option<PathBuf>,
}

pub fn recommend(observed_max_gap_s: f64) -> f64 {
    MIN_T_DEAD.max(((2.0 * observed_max_gap_s + 1.0) * 2.0).ceil() / 2.0)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Run {
    pub path: String,
    pub seconds: f64,
    pub max_gaps: BTreeMap<String, f64>,
}

/// `None` when the log has no stats event with packets.
pub fn analyse_log(path: &Path) -> std::io::Result<Option<Run>> {
    let bytes = std::fs::read(path)?;
    let text = String::from_utf8_lossy(&bytes);
    let (mut first_t, mut last_t) = (None, None);
    let mut gaps: BTreeMap<String, f64> = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with('{') {
            continue;
        }
        let Ok(ev) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if ev.get("event").and_then(Value::as_str) != Some("stats") {
            continue;
        }
        // `t` in the Python M0 logs, `ts` in the Rust JSON logger
        let Some(t) = ev.get("t").or_else(|| ev.get("ts")).and_then(Value::as_f64) else {
            continue;
        };
        let empty = Map::new();
        let streams = ev
            .get("streams")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let packets = |s: &Value| s.get("packets").and_then(Value::as_f64).unwrap_or(0.0);
        if !streams.values().any(|s| packets(s) > 0.0) {
            continue;
        }
        first_t.get_or_insert(t);
        last_t = Some(t);
        for (name, s) in streams {
            let gap = s.get("max_gap_s").and_then(Value::as_f64).unwrap_or(0.0);
            let slot = gaps.entry(name.clone()).or_insert(0.0);
            *slot = slot.max(gap);
        }
    }
    Ok(first_t.zip(last_t).map(|(first, last)| Run {
        path: path.display().to_string(),
        seconds: last - first,
        max_gaps: gaps,
    }))
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub runs: usize,
    pub minutes: f64,
    pub max_gap_by_kind_s: BTreeMap<String, f64>,
    pub observed_max_gap_s: f64,
    pub recommended_t_dead_s: f64,
}

fn round4(v: f64) -> f64 {
    (v * 1e4).round() / 1e4
}

pub fn aggregate(runs: &[Run]) -> Aggregate {
    let mut by_kind: BTreeMap<String, f64> = BTreeMap::new();
    for r in runs {
        for (name, g) in &r.max_gaps {
            let kind = name.rsplit('/').next().unwrap_or(name).to_owned();
            let slot = by_kind.entry(kind).or_insert(0.0);
            *slot = slot.max(*g);
        }
    }
    let observed = by_kind.values().copied().fold(0.0, f64::max);
    Aggregate {
        runs: runs.len(),
        minutes: runs.iter().map(|r| r.seconds).sum::<f64>() / 60.0,
        max_gap_by_kind_s: by_kind.into_iter().map(|(k, v)| (k, round4(v))).collect(),
        observed_max_gap_s: round4(observed),
        recommended_t_dead_s: recommend(observed),
    }
}

/// Python `{value:g}` for the values T_dead takes.
fn fmt_g(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{}", value as i64)
    } else {
        value.to_string()
    }
}

/// `(prefix, value, suffix)` of an indented `source_dead_timeout_seconds:` line.
fn split_timeout_line(line: &str) -> Option<(&str, &str, &str)> {
    const KEY: &str = "source_dead_timeout_seconds:";
    let body = line.trim_start();
    let indent = line.len() - body.len();
    if indent == 0 {
        return None;
    }
    let after_key = body.strip_prefix(KEY)?;
    let value_at = after_key.len() - after_key.trim_start().len();
    let rest = &after_key[value_at..];
    let value_len = rest
        .find(|c: char| c.is_whitespace() || c == '#')
        .unwrap_or(rest.len());
    if value_len == 0 {
        return None;
    }
    let prefix_len = indent + KEY.len() + value_at;
    Some((&line[..prefix_len], &rest[..value_len], &rest[value_len..]))
}

/// Edits only the `liveness.source_dead_timeout_seconds` line, keeping everything else byte-equal.
pub fn write_config(path: &Path, value: f64) -> Result<(), String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut in_liveness = false;
    let mut out = String::with_capacity(raw.len());
    let mut done = false;
    for line in raw.split_inclusive('\n') {
        let stripped = line.trim_end_matches(['\r', '\n']);
        let top_level = stripped
            .chars()
            .next()
            .is_some_and(|c| !c.is_whitespace() && c != '#');
        if top_level {
            in_liveness = stripped.starts_with("liveness:");
        } else if !done
            && in_liveness
            && let Some((prefix, _, suffix)) = split_timeout_line(stripped)
        {
            out.push_str(&format!(
                "{prefix}{}{suffix}{}",
                fmt_g(value),
                &line[stripped.len()..]
            ));
            done = true;
            continue;
        }
        out.push_str(line);
    }
    if !done {
        return Err(format!(
            "error: liveness.source_dead_timeout_seconds not found in {}",
            path.display()
        ));
    }
    std::fs::write(path, out).map_err(|e| format!("{}: {e}", path.display()))
}

/// Exit code: 0, 1 (cannot read/write), 2 (not enough evidence).
pub fn run(args: &CalibrateArgs, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let mut runs = Vec::new();
    for p in &args.logs {
        match analyse_log(p) {
            Ok(Some(r)) => {
                let gaps: Vec<String> = r
                    .max_gaps
                    .iter()
                    .map(|(k, v)| format!("{k}={v:.3}s"))
                    .collect();
                let _ = writeln!(
                    out,
                    "run {}: {:.1} min, max gaps {}",
                    p.display(),
                    r.seconds / 60.0,
                    gaps.join(", ")
                );
                runs.push(r);
            }
            Ok(None) => {
                let _ = writeln!(out, "skip {}: no stats events with packets", p.display());
            }
            Err(e) => {
                let _ = writeln!(err, "error: {}: {e}", p.display());
                return 1;
            }
        }
    }
    let result = aggregate(&runs);
    let _ = writeln!(
        out,
        "runs={} minutes_watched={:.1}",
        result.runs, result.minutes
    );
    let _ = writeln!(out, "max gap by kind (s): {:?}", result.max_gap_by_kind_s);
    let _ = writeln!(
        out,
        "note: per-stream gaps only; observed gap = max over streams (conservative)"
    );
    let _ = writeln!(out, "observed_max_gap_s={:?}", result.observed_max_gap_s);
    let _ = writeln!(
        out,
        "recommended source_dead_timeout_seconds={:?}",
        result.recommended_t_dead_s
    );

    if runs.is_empty() || result.minutes < args.min_minutes {
        let msg = format!(
            "only {:.1} min of watched source, need >= {}",
            result.minutes,
            fmt_g(args.min_minutes)
        );
        if !args.force {
            let _ = writeln!(err, "refused: {msg} (use --force to override)");
            return 2;
        }
        let _ = writeln!(err, "warning: {msg} (forced)");
        if runs.is_empty() {
            return 2;
        }
    }
    if args.write {
        let Some(config) = &args.config else {
            return 1;
        };
        if let Err(e) = write_config(config, result.recommended_t_dead_s) {
            let _ = writeln!(err, "{e}");
            return 1;
        }
        let _ = writeln!(out, "wrote {}", config.display());
    }
    0
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use serde_json::json;

    use super::*;

    const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        args: CalibrateArgs,
    }

    fn log(path: &Path, minutes: u32, audio_gap: f64, video_gap: f64) {
        let n = minutes * 12;
        let mut lines = vec![
            "garbage line".to_string(),
            json!({"t": 1000.0, "event": "start"}).to_string(),
        ];
        for i in 0..=n {
            let frac = f64::from(i) / f64::from(n);
            lines.push(
                json!({"t": 1000.0 + f64::from(i) * 5.0, "event": "stats", "streams": {
                    "1/audio": {"packets": 10 + i, "max_gap_s": audio_gap * frac},
                    "2/video": {"packets": 10 + i, "max_gap_s": video_gap * frac},
                }})
                .to_string(),
            );
        }
        std::fs::write(path, lines.join("\n")).unwrap();
    }

    fn args(logs: &[&Path], extra: &[&str]) -> CalibrateArgs {
        let argv = ["x"]
            .into_iter()
            .map(str::to_owned)
            .chain(logs.iter().map(|p| p.display().to_string()))
            .chain(extra.iter().map(|s| (*s).to_owned()));
        Wrap::parse_from(argv).args
    }

    fn call(a: &CalibrateArgs) -> (i32, String) {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = run(a, &mut out, &mut err);
        (code, String::from_utf8(out).unwrap())
    }

    #[test]
    fn recommend_math() {
        assert_eq!(recommend(0.0), 3.0);
        assert_eq!(recommend(1.0), 3.0);
        assert_eq!(recommend(1.2), 3.5);
        assert_eq!(recommend(2.0), 5.0);
        assert_eq!(recommend(2.3), 6.0);
    }

    #[test]
    fn analyse_uses_final_cumulative_gap_and_duration() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        log(&p, 10, 0.05, 1.2);
        let r = analyse_log(&p).unwrap().unwrap();
        assert!((r.seconds - 600.0).abs() < 1e-9);
        assert!((r.max_gaps["2/video"] - 1.2).abs() < 1e-9);
    }

    #[test]
    fn reads_rust_logs_timestamped_with_ts() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        log(&p, 10, 0.05, 1.2);
        let text = std::fs::read_to_string(&p)
            .unwrap()
            .replace("\"t\":", "\"ts\":");
        std::fs::write(&p, text).unwrap();
        let r = analyse_log(&p).unwrap().unwrap();
        assert!((r.seconds - 600.0).abs() < 1e-9);
        assert!((r.max_gaps["2/video"] - 1.2).abs() < 1e-9);
    }

    #[test]
    fn refuses_short_run_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        log(&p, 5, 0.1, 0.2);
        assert_eq!(call(&args(&[&p], &[])).0, 2);
        assert_eq!(call(&args(&[&p], &["--force"])).0, 0);
    }

    #[test]
    fn two_runs_add_up() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.jsonl"), dir.path().join("b.txt"));
        log(&a, 6, 0.1, 0.2);
        log(&b, 6, 0.1, 0.9);
        let (code, out) = call(&args(&[&a, &b], &[]));
        assert_eq!(code, 0);
        assert!(
            out.contains("recommended source_dead_timeout_seconds=3.0"),
            "{out}"
        );
    }

    #[test]
    fn write_requires_config() {
        assert!(Wrap::try_parse_from(["x", "a.jsonl", "--write"]).is_err());
    }

    #[test]
    fn write_edits_only_target_lines() {
        let dir = tempfile::tempdir().unwrap();
        let logp = dir.path().join("a.jsonl");
        log(&logp, 10, 0.1, 1.2);
        let cfg_src = std::fs::read_to_string(Path::new(ROOT).join("config.example.yaml")).unwrap();
        let cfg = dir.path().join("c.yaml");
        std::fs::write(&cfg, &cfg_src).unwrap();
        let c = cfg.display().to_string();
        let (code, _) = call(&args(&[&logp], &["--write", "--config", &c]));
        assert_eq!(code, 0);

        let new_cfg = std::fs::read_to_string(&cfg).unwrap();
        let (new_lines, old_lines): (Vec<_>, Vec<_>) =
            (new_cfg.lines().collect(), cfg_src.lines().collect());
        let diff: Vec<_> = old_lines
            .iter()
            .zip(&new_lines)
            .filter(|(o, n)| o != n)
            .collect();
        assert_eq!(new_lines.len(), old_lines.len());
        assert_eq!(diff.len(), 1);
        assert_eq!(
            diff[0].1.trim(),
            "source_dead_timeout_seconds: 3.5   # must be the value measured in M0 before enabling"
        );
    }
}
