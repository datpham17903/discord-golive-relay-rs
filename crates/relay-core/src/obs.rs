//! JSON logging with secret redaction, metrics, and the atomic status file (spec §17), port of
//! `golive_relay/obs.py`.
//!
//! Output is byte-compatible with Python's `json.dumps`: ASCII only (`ensure_ascii`), floats printed
//! like `repr(float)`, keys in insertion order. Log lines use compact separators, `status.json` uses
//! `indent=2`. Non-finite floats cannot be held by `serde_json::Value` and are emitted as `null`
//! (Python would write `NaN`/`Infinity`).

use std::fmt::Write as _;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;
use serde_json::{Map, Number, Value};

pub const REDACTED: &str = "***";
const SECRET_KEYS: [&str; 4] = ["token", "password", "secret", "credential"];

/// One JSON object per line; configured secrets and credential-named fields never appear.
///
/// Cheap to clone (shared writer). Write errors are ignored on purpose: a closed or full stderr must
/// not take the relay down, whereas Python would raise out of the logging call.
#[derive(Clone)]
pub struct JsonLogger {
    writer: Arc<Mutex<dyn Write + Send>>,
    secrets: Arc<[String]>,
}

impl JsonLogger {
    /// Empty secrets are dropped (they would redact every position of every string).
    pub fn new(
        writer: Arc<Mutex<dyn Write + Send>>,
        secrets: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        let secrets = secrets
            .into_iter()
            .map(|s| s.as_ref().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        Self { writer, secrets }
    }

    fn clean(&self, value: Value) -> Value {
        match value {
            Value::String(mut s) => {
                for secret in self.secrets.iter() {
                    s = s.replace(secret.as_str(), REDACTED);
                }
                Value::String(s)
            }
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| {
                        let lower = k.to_lowercase();
                        if SECRET_KEYS.iter().any(|w| lower.contains(w)) {
                            (k, Value::String(REDACTED.to_owned()))
                        } else {
                            (k, self.clean(v))
                        }
                    })
                    .collect(),
            ),
            Value::Array(items) => Value::Array(items.into_iter().map(|v| self.clean(v)).collect()),
            other => other,
        }
    }

    /// Writes `{"ts":..,"event":name,<fields in order>}` followed by a newline. A field named `ts` or
    /// `event` replaces that value in place, like the Python dict spread.
    pub fn event<K: Into<String>>(&self, name: &str, fields: impl IntoIterator<Item = (K, Value)>) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        self.event_at(ts, name, fields);
    }

    fn event_at<K: Into<String>>(
        &self,
        ts: f64,
        name: &str,
        fields: impl IntoIterator<Item = (K, Value)>,
    ) {
        let fields: Map<String, Value> = fields.into_iter().map(|(k, v)| (k.into(), v)).collect();
        let mut rec = Map::new();
        rec.insert("ts".to_owned(), Value::from(ts));
        rec.insert("event".to_owned(), Value::String(name.to_owned()));
        if let Value::Object(cleaned) = self.clean(Value::Object(fields)) {
            rec.extend(cleaned);
        }
        let mut line = dumps(&Value::Object(rec));
        line.push('\n');
        let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = w.write_all(line.as_bytes()).and_then(|()| w.flush());
    }
}

struct Timing {
    count: u64,
    total: f64,
    max: f64,
    last: f64,
}

/// Counters, gauges and timings (count/avg/max/last); everything is O(1) memory per name.
#[derive(Default)]
pub struct Metrics {
    counters: IndexMap<String, i64>,
    gauges: IndexMap<String, Value>,
    timings: IndexMap<String, Timing>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&mut self, name: &str, n: i64) {
        *self.counters.entry(name.to_owned()).or_insert(0) += n;
    }

    pub fn gauge(&mut self, name: &str, value: f64) {
        self.gauges.insert(name.to_owned(), Value::from(value));
    }

    /// Integer gauge; Python keeps `int` and `float` apart and so does `status.json` (`0` vs `0.0`).
    pub fn gauge_int(&mut self, name: &str, value: i64) {
        self.gauges.insert(name.to_owned(), Value::from(value));
    }

    pub fn timing(&mut self, name: &str, value: f64) {
        let t = self.timings.entry(name.to_owned()).or_insert(Timing {
            count: 0,
            total: 0.0,
            max: value,
            last: value,
        });
        t.count += 1;
        t.total += value;
        t.max = t.max.max(value);
        t.last = value;
    }

    pub fn snapshot(&self) -> Value {
        let counters: Map<String, Value> = self
            .counters
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(*v)))
            .collect();
        let gauges: Map<String, Value> = self
            .gauges
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let timings: Map<String, Value> = self
            .timings
            .iter()
            .map(|(k, t)| {
                let mut o = Map::new();
                o.insert("count".to_owned(), Value::from(t.count));
                o.insert("avg".to_owned(), Value::from(t.total / t.count as f64));
                o.insert("max".to_owned(), Value::from(t.max));
                o.insert("last".to_owned(), Value::from(t.last));
                (k.clone(), Value::Object(o))
            })
            .collect();
        let mut snap = Map::new();
        snap.insert("counters".to_owned(), Value::Object(counters));
        snap.insert("gauges".to_owned(), Value::Object(gauges));
        snap.insert("timings".to_owned(), Value::Object(timings));
        Value::Object(snap)
    }
}

/// Atomically replaces `status.json` in `directory` (created if missing).
pub fn write_status(directory: &Path, status: &Value) -> io::Result<()> {
    std::fs::create_dir_all(directory)?;
    // On any error the temp file is dropped, which deletes it.
    let mut tmp = tempfile::Builder::new()
        .prefix("status.json.")
        .suffix(".tmp")
        .tempfile_in(directory)?;
    tmp.write_all(dumps_indent(status, 2).as_bytes())?;
    tmp.persist(directory.join("status.json"))
        .map_err(|e| e.error)?;
    Ok(())
}

/// `json.dumps(v, separators=(",", ":"))`
fn dumps(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v, None, 0);
    out
}

/// `json.dumps(v, indent=indent)`
pub(crate) fn dumps_indent(v: &Value, indent: usize) -> String {
    let mut out = String::new();
    write_value(&mut out, v, Some(indent), 0);
    out
}

fn newline(out: &mut String, indent: Option<usize>, level: usize) {
    if let Some(n) = indent {
        out.push('\n');
        out.extend(std::iter::repeat_n(' ', n * level));
    }
}

fn write_value(out: &mut String, v: &Value, indent: Option<usize>, level: usize) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => write_number(out, n),
        Value::String(s) => write_string(out, s),
        Value::Array(items) if items.is_empty() => out.push_str("[]"),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                newline(out, indent, level + 1);
                write_value(out, item, indent, level + 1);
            }
            newline(out, indent, level);
            out.push(']');
        }
        Value::Object(map) if map.is_empty() => out.push_str("{}"),
        Value::Object(map) => {
            out.push('{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                newline(out, indent, level + 1);
                write_string(out, k);
                out.push(':');
                if indent.is_some() {
                    out.push(' ');
                }
                write_value(out, item, indent, level + 1);
            }
            newline(out, indent, level);
            out.push('}');
        }
    }
}

fn write_number(out: &mut String, n: &Number) {
    if let Some(i) = n.as_i64() {
        let _ = write!(out, "{i}");
    } else if let Some(u) = n.as_u64() {
        let _ = write!(out, "{u}");
    } else {
        out.push_str(&float_repr(n.as_f64().unwrap_or(0.0)));
    }
}

/// Python `repr(float)` for finite values: shortest round-trip digits, exponent form when the decimal
/// point position is `<= -4` or `> 16`.
fn float_repr(f: f64) -> String {
    // `{:e}` gives the shortest digit count, but on an exact tie it can pick the upper neighbour where
    // CPython rounds half-even; re-render that many digits exactly (half-even) to match.
    let shortest = format!("{:e}", f.abs());
    let ndigits = shortest
        .split_once('e')
        .map_or(1, |(m, _)| m.len() - usize::from(m.contains('.')));
    let sci = format!("{:.*e}", ndigits - 1, f.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let decpt = exp.parse::<i32>().unwrap_or(0) + 1;
    let n = digits.len() as i32;
    let mut s = String::new();
    if f.is_sign_negative() {
        s.push('-');
    }
    if decpt <= -4 || decpt > 16 {
        s.push_str(&digits[..1]);
        if n > 1 {
            s.push('.');
            s.push_str(&digits[1..]);
        }
        let e = decpt - 1;
        let _ = write!(s, "e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs());
    } else if decpt <= 0 {
        s.push_str("0.");
        s.extend(std::iter::repeat_n('0', (-decpt) as usize));
        s.push_str(digits);
    } else if decpt >= n {
        s.push_str(digits);
        s.extend(std::iter::repeat_n('0', (decpt - n) as usize));
        s.push_str(".0");
    } else {
        s.push_str(&digits[..decpt as usize]);
        s.push('.');
        s.push_str(&digits[decpt as usize..]);
    }
    s
}

/// Python `json.dumps` string with `ensure_ascii=True`: everything outside space..`~` is escaped
/// (including DEL), non-BMP characters as surrogate pairs, lowercase hex.
fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{u:04x}");
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    #[allow(clippy::excessive_precision)]
    fn float_repr_matches_python() {
        // Expected strings produced by CPython `repr(float)`.
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (5.0, "5.0"),
            (-2.5, "-2.5"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (123456789.125, "123456789.125"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.2345e17, "1.2345e+17"),
            (1e22, "1e+22"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (5e-324, "5e-324"),
            (1731123456.123456, "1731123456.123456"),
            (0.30000000000000004, "0.30000000000000004"),
            (9007199254740993.0, "9007199254740992.0"),
            (12345678901234567.0, "1.2345678901234568e+16"),
            (910764418279333.25, "910764418279333.2"),
        ];
        for (f, want) in cases {
            assert_eq!(float_repr(*f), *want, "{f:e}");
        }
    }

    #[test]
    fn string_escapes_match_python_ensure_ascii() {
        let mut out = String::new();
        write_string(
            &mut out,
            "a\"b\\c\n\r\t\u{8}\u{c}\u{1}\u{7f}\u{e9}\u{1F600}/",
        );
        assert_eq!(out, r#""a\"b\\c\n\r\t\b\f\u0001\u007f\u00e9\ud83d\ude00/""#);
    }

    #[test]
    fn compact_and_indent_layouts() {
        let v = json!({"a": [1, {"b": null}], "c": {}, "d": []});
        assert_eq!(dumps(&v), r#"{"a":[1,{"b":null}],"c":{},"d":[]}"#);
        assert_eq!(
            dumps_indent(&v, 2),
            "{\n  \"a\": [\n    1,\n    {\n      \"b\": null\n    }\n  ],\n  \"c\": {},\n  \"d\": []\n}"
        );
    }

    #[test]
    fn event_at_uses_given_timestamp() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        JsonLogger::new(buf.clone(), [] as [&str; 0]).event_at(
            1731123456.5,
            "x",
            [("n", json!(1))],
        );
        let got = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(got, "{\"ts\":1731123456.5,\"event\":\"x\",\"n\":1}\n");
    }
}
