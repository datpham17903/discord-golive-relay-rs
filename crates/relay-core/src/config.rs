//! Static YAML config loader and validator (spec §15), port of `golive_relay/config.py`.
//!
//! Every problem is collected and returned together as [`ConfigInvalid`]. The config is read-only; nothing
//! here writes YAML. The validator walks the `saphyr` tree directly (no serde) so the error strings stay
//! identical to the Python ones.
//!
//! Differences from the Python version, all because this build only passes through:
//! - `codecs.video_mode` defaults to `passthrough` (Python: `transcode`) and `transcode` for either mode is
//!   reported by [`check_operable`].
//! - `codecs.video_encoder`, `codecs.hw_decode` and `quality.audio_bitrate_kbps` are validated but unused;
//!   [`ignored_setting_notes`] lists them for `relayctl validate`.
//! - With `output_codec: H264`, `parent_codecs`/`watch_codecs` are treated as `h264_only`
//!   ([`CodecsConfig::effective_parent_codecs`]); the value read from the file is kept as is.
//! - A key that is present but `null` is a type error (Python silently read it as 0/""/false).
//! - IDs are `u64`; a digit string that overflows `u64` is rejected as "must be a string of decimal digits".
//!
//! Deprecation warnings are returned by [`load_config_with_warnings`] ([`load_config`] prints them to stderr),
//! so they never reach the JSON log.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use saphyr::{LoadableYamlNode, Yaml};

pub const MAX_INPUT_PIXELS: u64 = 3840 * 2160; // decode guard; not configurable in v1
pub const VIDEO_ENCODERS: &[&str] = &[
    "libx264",
    "h264_nvenc",
    "h264_amf",
    "h264_qsv",
    "h264_mf",
    "h264_videotoolbox",
    "h264_vaapi",
];
pub const HW_DECODE_DEVICES: &[&str] = &["none", "d3d11va", "cuda", "vaapi", "videotoolbox"];
pub const MAX_OUTPUT_ACCOUNTS: usize = 9;
/// Chat command names a deployment may expose to everybody (`op` is deliberately absent: never public).
/// Owned by the chat-command task later; kept here because the config validator needs it.
pub const COMMAND_NAMES: &[&str] = &[
    "status",
    "mirror",
    "unmirror",
    "on",
    "off",
    "target",
    "channel",
    "prejoin",
    "autoscale",
];
pub const DEFAULT_PUBLIC_COMMANDS: &[&str] = &["status", "mirror", "unmirror"];
const LOG_LEVELS: &[&str] = &["CRITICAL", "DEBUG", "ERROR", "INFO", "WARNING"]; // sorted, like the Python message
const AUDIO_BITRATE_DEFAULT_KBPS: u64 = 128;
const DEFAULT_PROFILE_MANIFEST: &str = "manifests/verified-profiles.json";

macro_rules! str_enum {
    ($(#[$m:meta])* $name:ident { $($var:ident => $s:literal),+ $(,)? }) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name { $($var),+ }

        impl $name {
            /// Accepted spellings, in the order the Python validator lists them.
            pub const NAMES: &'static [&'static str] = &[$($s),+];

            pub fn as_str(self) -> &'static str {
                match self { $(Self::$var => $s),+ }
            }

            pub fn parse(s: &str) -> Option<Self> {
                match s { $($s => Some(Self::$var),)+ _ => None }
            }
        }
    };
}

str_enum!(
    /// `quality.mode`.
    QualityMode { Auto => "auto", Fixed => "fixed" }
);
str_enum!(
    /// `codecs.video_mode`.
    VideoMode { Transcode => "transcode", Passthrough => "passthrough" }
);
str_enum!(
    /// `codecs.audio_mode`.
    AudioMode { Transcode => "transcode", Passthrough => "passthrough" }
);
str_enum!(
    /// `codecs.parent_codecs` / `codecs.watch_codecs`.
    CodecPolicy { AutoDecode => "auto_decode", H264Only => "h264_only" }
);
str_enum!(
    /// `codecs.output_codec`.
    OutputCodec { Adaptive => "adaptive", H264 => "H264" }
);
str_enum!(
    /// `OutputProfile.codec_mode`: `h264` passes through only H.264, `adaptive` any codec of the codec table.
    CodecMode { H264 => "h264", Adaptive => "adaptive" }
);
str_enum!(
    /// Role of an extra output account.
    AccountRole { Relay => "relay", Mirror => "mirror" }
);

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{}", .errors.join("; "))]
pub struct ConfigInvalid {
    /// Every problem found.
    pub errors: Vec<String>,
}

/// An extra Go Live account: relay (autoscaled, main channel) or mirror (fixed to another channel).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputAccount {
    /// 0 until resolved from the token at startup (see [`apply_account_ids`]).
    pub user_id: u64,
    /// Credential name, e.g. `relay-token-2`.
    pub credential: String,
    pub role: AccountRole,
    /// Mirror only: the one channel this account is fixed to.
    pub channel_id: Option<u64>,
}

/// What the worker needs to know about the output (port of `worker.messages.OutputProfile`;
/// `video_encoder`, `hw_decode`, `video_mode`, `audio_mode` are dropped: the worker only passes through).
#[derive(Debug, Clone, PartialEq)]
pub struct OutputProfile {
    pub mode: QualityMode,
    pub max_width: u64,
    pub max_height: u64,
    pub fps: u64,
    pub fixed_width: u64,
    pub fixed_height: u64,
    pub video_bitrate_kbps: u64,
    pub keyframe_interval_s: f64,
    pub max_input_pixels: u64,
    pub audio_enabled: bool,
    pub codec_mode: CodecMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscordConfig {
    /// 0 until resolved from the ingest token at startup (see [`apply_account_ids`]).
    pub expected_relay_user_id: u64,
    pub guild_id: u64,
    pub target_user_id: u64,
    pub allowed_voice_channel_id: u64,
    /// Every extra account: relay accounts, then mirror accounts.
    pub output_accounts: Vec<OutputAccount>,
    pub ingest_credential: String,
    pub ingest_publishes: bool,
}

impl DiscordConfig {
    pub fn relay_accounts(&self) -> Vec<OutputAccount> {
        self.accounts_with_role(AccountRole::Relay)
    }

    pub fn mirror_accounts(&self) -> Vec<OutputAccount> {
        self.accounts_with_role(AccountRole::Mirror)
    }

    fn accounts_with_role(&self, role: AccountRole) -> Vec<OutputAccount> {
        self.output_accounts
            .iter()
            .filter(|a| a.role == role)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationConfig {
    pub agreement_ref: String,
    pub excluded_viewer_ids: BTreeSet<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodecsConfig {
    pub parent_codecs: CodecPolicy,
    pub watch_codecs: CodecPolicy,
    pub output_codec: OutputCodec,
    pub output_rids: u64,
    pub video_encoder: String,
    pub hw_decode: String,
    pub video_mode: VideoMode,
    pub audio_mode: AudioMode,
}

impl CodecsConfig {
    /// `parent_codecs` as the relay applies it: a fixed H.264 output cannot decode anything else.
    pub fn effective_parent_codecs(&self) -> CodecPolicy {
        self.effective(self.parent_codecs)
    }

    /// `watch_codecs` as the relay applies it, see [`Self::effective_parent_codecs`].
    pub fn effective_watch_codecs(&self) -> CodecPolicy {
        self.effective(self.watch_codecs)
    }

    fn effective(&self, configured: CodecPolicy) -> CodecPolicy {
        if self.output_codec == OutputCodec::H264 {
            CodecPolicy::H264Only
        } else {
            configured
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QualityConfig {
    pub mode: QualityMode,
    pub max_width: u64,
    pub max_height: u64,
    pub max_fps: u64,
    pub fixed_width: u64,
    pub fixed_height: u64,
    pub fixed_fps: u64,
    /// 0 is `auto`.
    pub video_bitrate_kbps: u64,
    pub audio_bitrate_kbps: u64,
    pub approved_profile_manifest: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AudioConfig {
    pub enabled: bool,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    pub start_debounce_ms: u64,
    pub idle_voice_grace_seconds: f64,
    pub hold_recovery_timeout_seconds: f64,
    pub connect_timeout_seconds: f64,
    pub first_media_timeout_seconds: f64,
    pub retry_delays_seconds: Vec<f64>,
    pub retry_cooldown_seconds: f64,
    pub keyframe_interval_seconds: f64,
    pub feedback_idr_min_interval_ms: u64,
    pub repeated_rejection_limit: u64,
    pub recording_enabled: bool,
    pub prejoin_voice: bool,
    pub relay_autoscale: bool,
    pub relay_idle_stop_seconds: f64,
    pub stream_viewer_cap: u64,
    pub self_deaf_ingest: bool,
    pub self_deaf_outputs: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LivenessConfig {
    pub source_dead_timeout_seconds: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StorageConfig {
    pub state_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoggingConfig {
    pub level: String,
    pub include_media: bool,
    pub network_stats: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ControlConfig {
    pub chat_commands: bool,
    pub command_prefix: String,
    pub operator_user_ids: BTreeSet<u64>,
    pub operator_role_ids: BTreeSet<u64>,
    pub command_channel_id: Option<u64>,
    /// Anyone in the guild may use these.
    pub public_commands: BTreeSet<String>,
    /// Per author; operators are exempt.
    pub command_cooldown_seconds: u64,
    /// Global, between applied state changes; everyone.
    pub change_cooldown_seconds: u64,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            chat_commands: true,
            command_prefix: "!relay".to_string(),
            operator_user_ids: BTreeSet::new(),
            operator_role_ids: BTreeSet::new(),
            command_channel_id: None,
            public_commands: default_public_commands(),
            command_cooldown_seconds: 5,
            change_cooldown_seconds: 3,
        }
    }
}

impl ControlConfig {
    /// Listed in config (user or role): every command, including managing chat-added operators.
    pub fn is_owner(&self, user_id: u64, role_ids: &BTreeSet<u64>) -> bool {
        self.operator_user_ids.contains(&user_id) || !self.operator_role_ids.is_disjoint(role_ids)
    }

    /// An owner or a chat-added operator (`added`, from state.json): every command but `op add/remove`.
    pub fn is_operator(
        &self,
        user_id: u64,
        role_ids: &BTreeSet<u64>,
        added: &BTreeSet<u64>,
    ) -> bool {
        added.contains(&user_id) || self.is_owner(user_id, role_ids)
    }
}

fn default_public_commands() -> BTreeSet<String> {
    DEFAULT_PUBLIC_COMMANDS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayConfig {
    pub schema_version: u64,
    pub discord: DiscordConfig,
    pub authorization: AuthorizationConfig,
    pub codecs: CodecsConfig,
    pub quality: QualityConfig,
    pub audio: AudioConfig,
    pub runtime: RuntimeConfig,
    pub liveness: LivenessConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub control: ControlConfig,
}

impl RelayConfig {
    pub fn output_accounts(&self) -> Vec<OutputAccount> {
        self.discord.output_accounts.clone()
    }

    pub fn output_profile(&self) -> OutputProfile {
        let q = &self.quality;
        OutputProfile {
            mode: q.mode,
            max_width: q.max_width,
            max_height: q.max_height,
            fps: if q.mode == QualityMode::Auto {
                q.max_fps
            } else {
                q.fixed_fps
            },
            fixed_width: q.fixed_width,
            fixed_height: q.fixed_height,
            video_bitrate_kbps: q.video_bitrate_kbps,
            keyframe_interval_s: self.runtime.keyframe_interval_seconds,
            max_input_pixels: MAX_INPUT_PIXELS,
            audio_enabled: self.audio.enabled,
            codec_mode: if self.codecs.output_codec == OutputCodec::Adaptive {
                CodecMode::Adaptive
            } else {
                CodecMode::H264
            },
        }
    }

    /// (width, height, fps) of the profile the relay will output.
    pub fn profile_key(&self) -> (u64, u64, u64) {
        let q = &self.quality;
        if q.mode == QualityMode::Auto {
            (q.max_width, q.max_height, q.max_fps)
        } else {
            (q.fixed_width, q.fixed_height, q.fixed_fps)
        }
    }
}

// ---- reader ---------------------------------------------------------------------------------------------

/// Error list shared by every reader so errors keep the order Python produces them in.
#[derive(Clone, Default)]
struct Errors(Rc<RefCell<Vec<String>>>);

impl Errors {
    fn push(&self, msg: String) {
        self.0.borrow_mut().push(msg);
    }

    fn is_empty(&self) -> bool {
        self.0.borrow().is_empty()
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

/// Typed access to one section that records errors instead of failing.
struct Reader<'a, 'y> {
    name: &'static str,
    errors: Errors,
    seen: HashSet<String>,
    entries: Vec<(String, &'a Yaml<'y>)>,
}

impl<'a, 'y> Reader<'a, 'y> {
    fn new(name: &'static str, data: Option<&'a Yaml<'y>>, errors: &Errors) -> Self {
        let mut entries = Vec::new();
        match data.and_then(Yaml::as_mapping) {
            Some(map) => entries.extend(map.iter().map(|(k, v)| (key_label(k), v))),
            None => errors.push(format!("{name}: must be a mapping")),
        }
        Self {
            name,
            errors: errors.clone(),
            seen: HashSet::new(),
            entries,
        }
    }

    fn lookup(&self, key: &str) -> Option<&'a Yaml<'y>> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
    }

    fn has(&self, key: &str) -> bool {
        self.lookup(key).is_some()
    }

    fn mark(&mut self, key: &str) {
        self.seen.insert(key.to_string());
    }

    /// Reads a required key; a missing key is recorded and gives `None` (a present `null` is `Some`).
    fn get(&mut self, key: &str) -> Option<&'a Yaml<'y>> {
        self.mark(key);
        let v = self.lookup(key);
        if v.is_none() {
            self.errors.push(format!("{}.{key}: missing", self.name));
        }
        v
    }

    /// Reads an optional key without recording anything when it is absent.
    fn optional(&mut self, key: &str) -> Option<&'a Yaml<'y>> {
        self.mark(key);
        self.lookup(key)
    }

    fn err(&self, key: &str, msg: &str) {
        self.errors.push(format!("{}.{key}: {msg}", self.name));
    }

    fn finish(&self) {
        for (key, _) in &self.entries {
            if !self.seen.contains(key) {
                self.errors
                    .push(format!("{}.{key}: unknown key", self.name));
            }
        }
    }

    fn id(&mut self, key: &str) -> u64 {
        let Some(v) = self.get(key) else { return 0 };
        match v.as_str().and_then(parse_id) {
            Some(id) => id,
            None => {
                self.err(key, "must be a string of decimal digits");
                0
            }
        }
    }

    fn ids(&mut self, key: &str) -> BTreeSet<u64> {
        let Some(v) = self.get(key) else {
            return BTreeSet::new();
        };
        let Some(items) = v.as_sequence() else {
            self.err(key, "must be a list of ID strings");
            return BTreeSet::new();
        };
        let mut out = BTreeSet::new();
        for item in items {
            match item.as_str().and_then(parse_id) {
                Some(id) => {
                    out.insert(id);
                }
                None => self.err(key, &format!("bad id {}", py_repr(item))),
            }
        }
        out
    }

    fn str_(&mut self, key: &str, allowed: Option<&[&str]>) -> String {
        let Some(v) = self.get(key) else {
            return String::new();
        };
        let Some(s) = v.as_str().filter(|s| !s.is_empty()) else {
            self.err(key, "must be a non-empty string");
            return String::new();
        };
        if let Some(allowed) = allowed
            && !allowed.contains(&s)
        {
            self.err(key, &format!("must be one of {}", py_str_list(allowed)));
        }
        s.to_string()
    }

    /// Optional string key, `default` when absent; `auto` is always accepted.
    fn opt_str(&mut self, key: &str, allowed: &[&str], default: &str) -> String {
        self.mark(key);
        let Some(v) = self.lookup(key) else {
            return default.to_string();
        };
        match v
            .as_str()
            .filter(|s| !s.is_empty() && (allowed.contains(s) || *s == "auto"))
        {
            Some(s) => s.to_string(),
            None => {
                self.err(
                    key,
                    &format!("must be auto or one of {}", allowed.join(", ")),
                );
                "auto".to_string()
            }
        }
    }

    /// True (and the key counted as read) when the key is absent, `null` or the string `auto`.
    fn is_auto(&mut self, key: &str) -> bool {
        let auto = match self.lookup(key) {
            None => true,
            Some(v) => v.is_null() || v.as_str() == Some("auto"),
        };
        if auto {
            self.mark(key);
        }
        auto
    }

    /// Returns the value as read (Python returns it even when it is out of range).
    fn int_(&mut self, key: &str, minimum: i64, even: bool) -> i64 {
        let Some(v) = self.get(key) else { return 0 };
        let Some(n) = v.as_integer() else {
            self.err(key, "must be an integer");
            return 0;
        };
        if n < minimum {
            self.err(key, &format!("must be >= {minimum}"));
        } else if even && n % 2 != 0 {
            self.err(key, "must be even");
        }
        n
    }

    fn uint(&mut self, key: &str, minimum: i64, even: bool) -> u64 {
        self.int_(key, minimum, even).max(0) as u64
    }

    fn num(&mut self, key: &str, nullable: bool) -> Option<f64> {
        let v = self.get(key)?;
        if v.is_null() {
            if !nullable {
                self.err(key, "must be a number");
            }
            return None;
        }
        let n = positive_number(v);
        if n.is_none() {
            self.err(key, "must be a positive number");
        }
        n
    }

    fn only_false(&mut self, key: &str) -> bool {
        let v = self.get(key);
        if v.and_then(Yaml::as_bool) != Some(false) {
            self.err(key, "must be false (not implemented)");
        }
        false
    }

    /// Optional boolean key, `default` when absent.
    fn opt_bool(&mut self, key: &str, default: bool) -> bool {
        self.mark(key);
        let Some(v) = self.lookup(key) else {
            return default;
        };
        match v.as_bool() {
            Some(b) => b,
            None => {
                self.err(key, "must be a boolean");
                default
            }
        }
    }

    fn bool_(&mut self, key: &str) -> bool {
        let Some(v) = self.get(key) else { return false };
        match v.as_bool() {
            Some(b) => b,
            None => {
                self.err(key, "must be a boolean");
                false
            }
        }
    }
}

fn parse_id(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok().filter(|n| *n > 0)
}

fn valid_credential(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn positive_number(v: &Yaml) -> Option<f64> {
    let n = match (v.as_integer(), v.as_floating_point()) {
        (Some(i), _) => i as f64,
        (None, Some(f)) => f,
        (None, None) => return None,
    };
    (n.is_finite() && n > 0.0).then_some(n)
}

fn key_label(k: &Yaml) -> String {
    k.as_str().map_or_else(|| py_repr(k), str::to_string)
}

/// Python `repr` of a list of strings, as in `f"must be one of {list(allowed)}"`.
fn py_str_list(items: &[&str]) -> String {
    let inner: Vec<String> = items.iter().map(|s| py_str_repr(s)).collect();
    format!("[{}]", inner.join(", "))
}

fn py_str_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Python `repr` of a YAML value, for messages like `bad id 5`.
fn py_repr(v: &Yaml) -> String {
    if let Some(s) = v.as_str() {
        py_str_repr(s)
    } else if v.is_null() {
        "None".to_string()
    } else if let Some(b) = v.as_bool() {
        if b { "True" } else { "False" }.to_string()
    } else if let Some(i) = v.as_integer() {
        i.to_string()
    } else if let Some(f) = v.as_floating_point() {
        if f.is_nan() {
            "nan".to_string()
        } else if f.is_infinite() {
            if f > 0.0 { "inf" } else { "-inf" }.to_string()
        } else {
            format!("{f:?}")
        }
    } else if let Some(items) = v.as_sequence() {
        let inner: Vec<String> = items.iter().map(py_repr).collect();
        format!("[{}]", inner.join(", "))
    } else if let Some(map) = v.as_mapping() {
        let mut out = String::from("{");
        for (i, (k, val)) in map.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{}: {}", py_repr(k), py_repr(val));
        }
        out.push('}');
        out
    } else {
        "<unsupported>".to_string()
    }
}

// ---- sections -------------------------------------------------------------------------------------------

/// Everything `_output_accounts` needs from the part of `discord` already read.
struct DiscordIds {
    target_user_id: u64,
    expected_relay_user_id: u64,
    allowed_voice_channel_id: u64,
}

/// Extra accounts: `relay_accounts` then `mirror_accounts`; `output_accounts` is a deprecated alias of relays.
fn output_accounts(
    r: &mut Reader,
    ids: &DiscordIds,
    ingest_credential: &str,
    warnings: &mut Vec<String>,
) -> Vec<OutputAccount> {
    let lists: &[(&str, AccountRole)] = if r.has("output_accounts") {
        if r.has("relay_accounts") || r.has("mirror_accounts") {
            r.err(
                "output_accounts",
                "deprecated; cannot be combined with relay_accounts or mirror_accounts",
            );
            for key in ["output_accounts", "relay_accounts", "mirror_accounts"] {
                r.mark(key);
            }
            return Vec::new();
        }
        warnings.push(
            "discord.output_accounts is deprecated; read as discord.relay_accounts".to_string(),
        );
        &[("output_accounts", AccountRole::Relay)]
    } else {
        &[
            ("relay_accounts", AccountRole::Relay),
            ("mirror_accounts", AccountRole::Mirror),
        ]
    };
    let mut out: Vec<OutputAccount> = Vec::new();
    let mut seen_ids: HashSet<u64> = [ids.target_user_id, ids.expected_relay_user_id]
        .into_iter()
        .collect();
    seen_ids.remove(&0);
    let mut seen_creds: HashSet<String> = HashSet::from([ingest_credential.to_string()]);
    let mut fixed_channels: HashSet<u64> = HashSet::new();
    let mut total = 0;
    for &(key, role) in lists {
        let empty = Vec::new();
        let items = match r.optional(key) {
            None => &empty,
            Some(raw) => match raw.as_sequence() {
                Some(items) => items,
                None => {
                    r.err(key, "must be a list of {user_id, credential}");
                    continue;
                }
            },
        };
        total += items.len();
        let allowed: &[&str] = if role == AccountRole::Mirror {
            &["user_id", "credential", "channel_id"]
        } else {
            &["user_id", "credential"]
        };
        for (i, item) in items.iter().enumerate() {
            let at = format!("{key}[{i}]");
            let map = item.as_mapping().filter(|m| {
                m.iter().any(|(k, _)| k.as_str() == Some("credential"))
                    && m.iter()
                        .all(|(k, _)| k.as_str().is_some_and(|k| allowed.contains(&k)))
            });
            let Some(map) = map else {
                r.err(
                    &at,
                    &format!(
                        "must be a mapping with credential and optional user_id{}",
                        if role == AccountRole::Mirror {
                            " and channel_id"
                        } else {
                            ""
                        }
                    ),
                );
                continue;
            };
            let field = |name: &str| {
                map.iter()
                    .find(|(k, _)| k.as_str() == Some(name))
                    .map(|(_, v)| v)
            };
            let mut uid = 0; // 0: resolved from the token at startup
            if let Some(v) = field("user_id") {
                match v.as_str().and_then(parse_id) {
                    Some(id) => uid = id,
                    None => {
                        r.err(&at, "user_id must be a string of decimal digits");
                        continue;
                    }
                }
            }
            let Some(cred) = field("credential")
                .and_then(Yaml::as_str)
                .filter(|c| valid_credential(c))
            else {
                r.err(&at, "credential must match [A-Za-z0-9_.-]+");
                continue;
            };
            let mut channel = None;
            if let Some(v) = field("channel_id").filter(|v| !v.is_null()) {
                let Some(ch) = v.as_str().and_then(parse_id) else {
                    r.err(&at, "channel_id must be a string of decimal digits");
                    continue;
                };
                if ch == ids.allowed_voice_channel_id {
                    r.err(&at, "channel_id must differ from allowed_voice_channel_id");
                    continue;
                }
                if fixed_channels.contains(&ch) {
                    r.err(&at, "channel_id is already fixed to another mirror account");
                    continue;
                }
                channel = Some(ch);
            }
            if uid != 0 && seen_ids.contains(&uid) {
                r.err(&at, "user_id must differ from the target, the ingest relay and other output accounts");
                continue;
            }
            if seen_creds.contains(cred) {
                r.err(&at, "credential must be unique (also vs ingest_credential)");
                continue;
            }
            seen_ids.insert(uid);
            seen_creds.insert(cred.to_string());
            if let Some(ch) = channel {
                fixed_channels.insert(ch);
            }
            out.push(OutputAccount {
                user_id: uid,
                credential: cred.to_string(),
                role,
                channel_id: channel,
            });
        }
    }
    if total > MAX_OUTPUT_ACCOUNTS {
        r.err(
            lists[0].0,
            &format!("at most {MAX_OUTPUT_ACCOUNTS} accounts in total"),
        );
    }
    out
}

/// The optional `control` section; absent means the defaults (chat commands on, only public ones for all).
fn control(section: Option<&Yaml>, errors: &Errors) -> ControlConfig {
    let Some(section) = section else {
        return ControlConfig::default();
    };
    let mut r = Reader::new("control", Some(section), errors);
    let defaults = ControlConfig::default();
    let enabled = r.opt_bool("chat_commands", true);
    let prefix = match r.optional("command_prefix") {
        None => defaults.command_prefix.clone(),
        Some(v) => match v
            .as_str()
            .filter(|p| !p.is_empty() && !p.chars().any(char::is_whitespace))
        {
            Some(p) => p.to_string(),
            None => {
                r.err(
                    "command_prefix",
                    "must be a non-empty string without whitespace",
                );
                defaults.command_prefix.clone()
            }
        },
    };
    let operators = if r.has("operator_user_ids") {
        r.ids("operator_user_ids")
    } else {
        BTreeSet::new()
    };
    r.mark("operator_user_ids");
    let roles = if r.has("operator_role_ids") {
        r.ids("operator_role_ids")
    } else {
        BTreeSet::new()
    };
    r.mark("operator_role_ids");
    let public = match r.optional("public_commands") {
        None => defaults.public_commands.clone(),
        Some(v) => {
            let names: Option<BTreeSet<String>> = v.as_sequence().and_then(|items| {
                items
                    .iter()
                    .map(|n| {
                        n.as_str()
                            .filter(|n| COMMAND_NAMES.contains(n))
                            .map(str::to_string)
                    })
                    .collect()
            });
            names.unwrap_or_else(|| {
                r.err(
                    "public_commands",
                    &format!(
                        "must be a list of command names from {}",
                        COMMAND_NAMES.join(", ")
                    ),
                );
                defaults.public_commands.clone()
            })
        }
    };
    let mut cooldowns = [0u64; 2];
    for (slot, (key, default)) in cooldowns.iter_mut().zip([
        ("command_cooldown_seconds", 5),
        ("change_cooldown_seconds", 3),
    ]) {
        *slot = match r.optional(key) {
            None => default,
            Some(v) => match v.as_integer().and_then(|n| u64::try_from(n).ok()) {
                Some(n) => n,
                None => {
                    r.err(key, "must be an integer >= 0");
                    default
                }
            },
        };
    }
    let mut channel = None;
    if let Some(v) = r.optional("command_channel_id").filter(|v| !v.is_null()) {
        match v.as_str().and_then(parse_id) {
            Some(id) => channel = Some(id),
            None => r.err("command_channel_id", "must be a string of decimal digits"),
        }
    }
    r.finish();
    ControlConfig {
        chat_commands: enabled,
        command_prefix: prefix,
        operator_user_ids: operators,
        operator_role_ids: roles,
        command_channel_id: channel,
        public_commands: public,
        command_cooldown_seconds: cooldowns[0],
        change_cooldown_seconds: cooldowns[1],
    }
}

fn enum_of<T: Copy>(name: &str, parse: fn(&str) -> Option<T>, fallback: T) -> T {
    // The reader already recorded an error for a bad value; the fallback only keeps the build going.
    parse(name).unwrap_or(fallback)
}

fn build(raw: Option<&Yaml>, errors: &Errors, warnings: &mut Vec<String>) -> Option<RelayConfig> {
    let Some(raw) = raw.filter(|y| y.is_mapping()) else {
        errors.push("config: top level must be a mapping".to_string());
        return None;
    };
    let mut top = Reader::new("config", Some(raw), errors);
    let sv = top.int_("schema_version", 1, false);
    if sv != 0 && sv != 1 {
        errors.push("config.schema_version: unsupported (expected 1)".to_string());
    }

    let section = top.get("discord");
    let mut r = Reader::new("discord", section, errors);
    // 0: resolved at startup
    let expected_relay_user_id = if r.has("expected_relay_user_id") {
        r.id("expected_relay_user_id")
    } else {
        0
    };
    r.mark("expected_relay_user_id");
    let guild_id = r.id("guild_id");
    let target_user_id = r.id("target_user_id");
    let allowed_voice_channel_id = r.id("allowed_voice_channel_id");
    let ingest_credential = match r.optional("ingest_credential") {
        None => "relay-token".to_string(),
        Some(v) => match v.as_str().filter(|c| valid_credential(c)) {
            Some(c) => c.to_string(),
            None => {
                r.err("ingest_credential", "must match [A-Za-z0-9_.-]+");
                "relay-token".to_string()
            }
        },
    };
    let ingest_publishes = match r.optional("ingest_publishes") {
        None => true,
        Some(v) => v.as_bool().unwrap_or_else(|| {
            r.err("ingest_publishes", "must be a boolean");
            true
        }),
    };
    let ids = DiscordIds {
        target_user_id,
        expected_relay_user_id,
        allowed_voice_channel_id,
    };
    let accounts = output_accounts(&mut r, &ids, &ingest_credential, warnings);
    r.finish();
    let discord = DiscordConfig {
        expected_relay_user_id,
        guild_id,
        target_user_id,
        allowed_voice_channel_id,
        output_accounts: accounts,
        ingest_credential,
        ingest_publishes,
    };
    if discord.target_user_id != 0 && discord.target_user_id == discord.expected_relay_user_id {
        errors.push("discord.target_user_id: must differ from expected_relay_user_id".to_string());
    }

    let section = top.get("authorization");
    let mut r = Reader::new("authorization", section, errors);
    let authorization = AuthorizationConfig {
        agreement_ref: r.str_("agreement_ref", None),
        excluded_viewer_ids: r.ids("excluded_viewer_ids"),
    };
    r.finish();

    let section = top.get("codecs");
    let mut r = Reader::new("codecs", section, errors);
    let parent = r.str_("parent_codecs", Some(CodecPolicy::NAMES));
    let watch = r.str_("watch_codecs", Some(CodecPolicy::NAMES));
    let output_codec = if r.has("output_codec") {
        enum_of(
            &r.str_("output_codec", Some(OutputCodec::NAMES)),
            OutputCodec::parse,
            OutputCodec::Adaptive,
        )
    } else {
        OutputCodec::Adaptive
    };
    let output_rids = r.uint("output_rids", 1, false);
    let video_encoder = r.opt_str("video_encoder", VIDEO_ENCODERS, "auto");
    let hw_decode = r.opt_str("hw_decode", HW_DECODE_DEVICES, "none");
    let video_mode = if r.has("video_mode") {
        enum_of(
            &r.str_("video_mode", Some(VideoMode::NAMES)),
            VideoMode::parse,
            VideoMode::Passthrough,
        )
    } else {
        VideoMode::Passthrough
    };
    let audio_mode = if r.has("audio_mode") {
        enum_of(
            &r.str_("audio_mode", Some(AudioMode::NAMES)),
            AudioMode::parse,
            AudioMode::Passthrough,
        )
    } else {
        AudioMode::Passthrough
    };
    let codecs = CodecsConfig {
        parent_codecs: enum_of(&parent, CodecPolicy::parse, CodecPolicy::H264Only),
        watch_codecs: enum_of(&watch, CodecPolicy::parse, CodecPolicy::H264Only),
        output_codec,
        output_rids,
        video_encoder,
        hw_decode,
        video_mode,
        audio_mode,
    };
    if codecs.output_rids > 1 {
        errors.push("codecs.output_rids: only 1 is supported".to_string());
    }
    r.finish();

    let section = top.get("quality");
    let mut r = Reader::new("quality", section, errors);
    let mode = r.str_("mode", Some(QualityMode::NAMES));
    let quality = QualityConfig {
        mode: enum_of(&mode, QualityMode::parse, QualityMode::Auto),
        max_width: r.uint("max_width", 1, true),
        max_height: r.uint("max_height", 1, true),
        max_fps: r.uint("max_fps", 1, false),
        fixed_width: r.uint("fixed_width", 1, true),
        fixed_height: r.uint("fixed_height", 1, true),
        fixed_fps: r.uint("fixed_fps", 1, false),
        // `auto` (or absent) is 0: the encoder picks a bitrate from the output size; otherwise an integer kbps
        video_bitrate_kbps: if r.is_auto("video_bitrate_kbps") {
            0
        } else {
            r.uint("video_bitrate_kbps", 1, false)
        },
        audio_bitrate_kbps: r.uint("audio_bitrate_kbps", 1, false),
        approved_profile_manifest: if r.has("approved_profile_manifest") {
            r.str_("approved_profile_manifest", None)
        } else {
            DEFAULT_PROFILE_MANIFEST.to_string()
        },
    };
    r.finish();

    let section = top.get("audio");
    let mut r = Reader::new("audio", section, errors);
    let audio = AudioConfig {
        enabled: r.bool_("enabled"),
        source: r.str_("source", Some(&["target_stream_only"])),
    };
    r.finish();

    let section = top.get("runtime");
    let mut r = Reader::new("runtime", section, errors);
    let mut delays: Vec<f64> = Vec::new();
    if let Some(v) = r.get("retry_delays_seconds") {
        let parsed: Option<Vec<f64>> = v
            .as_sequence()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.iter().map(positive_number).collect());
        match parsed {
            Some(d) => delays = d,
            None => r.err(
                "retry_delays_seconds",
                "must be a non-empty list of positive numbers",
            ),
        }
    }
    let start_debounce_ms = r.uint("start_debounce_ms", 0, false);
    let idle_voice_grace_seconds = r.num("idle_voice_grace_seconds", false).unwrap_or(0.0);
    let hold_recovery_timeout_seconds =
        r.num("hold_recovery_timeout_seconds", false).unwrap_or(0.0);
    let connect_timeout_seconds = r.num("connect_timeout_seconds", false).unwrap_or(0.0);
    let first_media_timeout_seconds = r.num("first_media_timeout_seconds", false).unwrap_or(0.0);
    let retry_cooldown_seconds = r.num("retry_cooldown_seconds", false).unwrap_or(0.0);
    let keyframe_interval_seconds = r.num("keyframe_interval_seconds", false).unwrap_or(0.0);
    let feedback_idr_min_interval_ms = r.uint("feedback_idr_min_interval_ms", 0, false);
    let repeated_rejection_limit = r.uint("repeated_rejection_limit", 1, false);
    let recording_enabled = r.only_false("recording_enabled");
    let prejoin_voice = r.opt_bool("prejoin_voice", true);
    let relay_autoscale = r.opt_bool("relay_autoscale", true);
    let relay_idle_stop_seconds = if r.has("relay_idle_stop_seconds") {
        r.num("relay_idle_stop_seconds", false).unwrap_or(30.0)
    } else {
        30.0
    };
    let stream_viewer_cap = if r.has("stream_viewer_cap") {
        r.uint("stream_viewer_cap", 1, false)
    } else {
        50
    };
    let self_deaf_ingest = r.opt_bool("self_deaf_ingest", true);
    let self_deaf_outputs = r.opt_bool("self_deaf_outputs", true);
    let runtime = RuntimeConfig {
        start_debounce_ms,
        idle_voice_grace_seconds,
        hold_recovery_timeout_seconds,
        connect_timeout_seconds,
        first_media_timeout_seconds,
        retry_delays_seconds: delays,
        retry_cooldown_seconds,
        keyframe_interval_seconds,
        feedback_idr_min_interval_ms,
        repeated_rejection_limit,
        recording_enabled,
        prejoin_voice,
        relay_autoscale,
        relay_idle_stop_seconds,
        stream_viewer_cap,
        self_deaf_ingest,
        self_deaf_outputs,
    };
    if r.has("relay_scale_down_seconds") {
        r.mark("relay_scale_down_seconds");
        warnings.push(
            "runtime.relay_scale_down_seconds is deprecated and ignored; extra relays stop after \
             runtime.relay_idle_stop_seconds without viewers"
                .to_string(),
        );
    }
    if runtime.keyframe_interval_seconds > 2.0 {
        // num() already rejects values <= 0
        r.err("keyframe_interval_seconds", "must be > 0 and <= 2.0");
    }
    r.finish();

    let section = top.get("liveness");
    let mut r = Reader::new("liveness", section, errors);
    let liveness = LivenessConfig {
        source_dead_timeout_seconds: r.num("source_dead_timeout_seconds", true),
    };
    // Accepted for configs that still set it; no capability manifest is read.
    if r.has("verified_capability_manifest") {
        r.str_("verified_capability_manifest", None);
    }
    r.finish();

    let section = top.get("storage");
    let mut r = Reader::new("storage", section, errors);
    let state_directory = r.str_("state_directory", None);
    let storage = StorageConfig {
        state_directory: PathBuf::from(if state_directory.is_empty() {
            "."
        } else {
            &state_directory
        }),
    };
    r.finish();

    let section = top.get("logging");
    let mut r = Reader::new("logging", section, errors);
    let logging = LoggingConfig {
        level: r.str_("level", Some(LOG_LEVELS)),
        include_media: r.only_false("include_media"),
        network_stats: r.opt_bool("network_stats", false),
    };
    r.finish();

    let control_section = top.optional("control").filter(|v| !v.is_null());
    let control = control(control_section, errors);
    top.finish();

    if !errors.is_empty() {
        return None;
    }
    Some(RelayConfig {
        schema_version: sv.max(0) as u64,
        discord,
        authorization,
        codecs,
        quality,
        audio,
        runtime,
        liveness,
        storage,
        logging,
        control,
    })
}

/// PyYAML accepts a flow collection whose closing `]`/`}` sits on its own line at a column not deeper than
/// the line that opened it (the production Python configs do this); saphyr rejects it. Re-indents exactly
/// such lines to one column deeper than the opening line, keeping the line count. A line is touched only if
/// it is a lone `]` or `}` (plus an optional `#` comment), closes a flow collection opened on an earlier
/// line that was itself indented, and is not deeper than that line. Quoted scalars (also multi-line) and
/// block scalar bodies (`|`/`>`) are skipped. Not modelled: tags/anchors, `?` complex keys, and plain
/// scalars containing brackets or quotes after a space.
fn reindent_flow_closers(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    // Indent of the line that opened each flow collection still open.
    let mut open: Vec<usize> = Vec::new();
    let mut quote: Option<char> = None;
    // Indent of the line owning the block scalar whose body we are inside.
    let mut block: Option<usize> = None;
    for line in text.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let content = body.trim_start_matches(' ');
        let indent = body.len() - content.len();
        if let Some(owner) = block {
            if content.is_empty() || indent > owner {
                out.push_str(line);
                continue;
            }
            block = None;
        }
        if quote.is_none()
            && let Some(&opener) = open.last()
            && opener > 0
            && indent <= opener
            && let Some(rest) = content.strip_prefix([']', '}'])
            && (rest.trim().is_empty() || rest.trim_start().starts_with('#'))
        {
            out.push_str(&" ".repeat(opener + 1));
            out.push_str(content);
            out.push_str(&line[body.len()..]);
            open.pop();
            continue;
        }
        let mut prev_ws = true;
        let mut last_sig = ' ';
        let mut it = body.chars().peekable();
        while let Some(c) = it.next() {
            if let Some(q) = quote {
                if q == '"' && c == '\\' {
                    it.next();
                } else if c == q {
                    if q == '\'' && it.peek() == Some(&'\'') {
                        it.next();
                    } else {
                        quote = None;
                    }
                }
                continue;
            }
            let token_start = prev_ws || matches!(last_sig, '[' | '{' | ',' | ':' | '-');
            match c {
                '#' if prev_ws => break,
                '"' | '\'' if token_start => quote = Some(c),
                '[' | '{' if token_start => open.push(indent),
                ']' | '}' => {
                    open.pop();
                }
                _ => {}
            }
            prev_ws = c == ' ' || c == '\t';
            if !prev_ws {
                last_sig = c;
            }
        }
        if quote.is_none() && open.is_empty() && is_block_header(body) {
            block = Some(indent);
        }
        out.push_str(line);
    }
    out
}

/// A line ending in a block-scalar header: `key: |-`, `- >2+`, `|`.
fn is_block_header(body: &str) -> bool {
    let code = body.split(" #").next().unwrap_or("").trim_end();
    let Some(i) = code.rfind(['|', '>']) else {
        return false;
    };
    let (before, ind) = code.split_at(i);
    ind[1..].chars().all(|c| matches!(c, '+' | '-' | '1'..='9'))
        && ind.len() <= 3
        && (before.is_empty() || before.ends_with([' ', ':', '-']))
}

/// Load and validate `path`, returning the deprecation warnings next to the config.
pub fn load_config_with_warnings(path: &Path) -> Result<(RelayConfig, Vec<String>), ConfigInvalid> {
    let unreadable = |why: &dyn std::fmt::Display| ConfigInvalid {
        errors: vec![format!("config: cannot read {}: {why}", path.display())],
    };
    let text = std::fs::read_to_string(path).map_err(|e| unreadable(&e))?;
    let text = reindent_flow_closers(&text);
    let docs = Yaml::load_from_str(&text).map_err(|e| {
        let hint = if e.info().contains("indentation") {
            format!(
                "; hint: line {}: indent it deeper than the line that opens the enclosing `[`/`{{` collection",
                e.marker().line()
            )
        } else {
            String::new()
        };
        unreadable(&format!("{e}{hint}"))
    })?;
    if docs.len() > 1 {
        return Err(unreadable(&"expected a single document"));
    }
    let errors = Errors::default();
    let mut warnings = Vec::new();
    match build(docs.first(), &errors, &mut warnings) {
        Some(cfg) => Ok((cfg, warnings)),
        None => Err(ConfigInvalid {
            errors: errors.take(),
        }),
    }
}

/// Load and validate `path`; deprecation warnings go to stderr.
pub fn load_config(path: &Path) -> Result<RelayConfig, ConfigInvalid> {
    let (cfg, warnings) = load_config_with_warnings(path)?;
    for w in warnings {
        eprintln!("warning: {w}");
    }
    Ok(cfg)
}

/// Credential names whose user ID is not in the config (ingest first); resolved from the token at startup.
pub fn unresolved_credentials(cfg: &RelayConfig) -> Vec<String> {
    let d = &cfg.discord;
    let mut out = Vec::new();
    if d.expected_relay_user_id == 0 {
        out.push(d.ingest_credential.clone());
    }
    out.extend(
        d.output_accounts
            .iter()
            .filter(|a| a.user_id == 0)
            .map(|a| a.credential.clone()),
    );
    out
}

/// Fill the missing user IDs from `ids` (credential name -> ID) and re-check every ID rule.
///
/// Fails, naming the credentials, when a resolved ID clashes.
pub fn apply_account_ids(
    cfg: &RelayConfig,
    ids: &HashMap<String, u64>,
) -> Result<RelayConfig, ConfigInvalid> {
    let id_of = |cred: &str| ids.get(cred).copied().unwrap_or(0);
    let mut d = cfg.discord.clone();
    if d.expected_relay_user_id == 0 {
        d.expected_relay_user_id = id_of(&d.ingest_credential);
    }
    for a in &mut d.output_accounts {
        if a.user_id == 0 {
            a.user_id = id_of(&a.credential);
        }
    }
    let mut errors = Vec::new();
    let mut owner: HashMap<u64, &str> = HashMap::new();
    let pairs = std::iter::once((d.ingest_credential.as_str(), d.expected_relay_user_id)).chain(
        d.output_accounts
            .iter()
            .map(|a| (a.credential.as_str(), a.user_id)),
    );
    for (cred, uid) in pairs {
        if uid == 0 {
            errors.push(format!("credential {cred}: user id not resolved"));
        } else if uid == d.target_user_id {
            errors.push(format!(
                "credential {cred}: account {uid} is discord.target_user_id"
            ));
        } else if let Some(first) = owner.get(&uid) {
            errors.push(format!(
                "credential {cred}: account {uid} is the same as credential {first}"
            ));
        } else {
            owner.insert(uid, cred);
        }
    }
    if !errors.is_empty() {
        return Err(ConfigInvalid { errors });
    }
    Ok(RelayConfig {
        discord: d,
        ..cfg.clone()
    })
}

/// Return reasons the service must not run yet; an empty list means it may.
pub fn check_operable(cfg: &RelayConfig) -> Vec<String> {
    let mut errors = Vec::new();
    if cfg.liveness.source_dead_timeout_seconds.is_none() {
        errors.push(
            "liveness.source_dead_timeout_seconds: must be the value measured in M0 (is null)"
                .to_string(),
        );
    }
    // This build cannot encode, so a transcode mode can never run.
    if cfg.codecs.video_mode == VideoMode::Transcode {
        errors.push("codecs.video_mode: transcode not supported in this build".to_string());
    }
    if cfg.codecs.audio_mode == AudioMode::Transcode {
        errors.push("codecs.audio_mode: transcode not supported in this build".to_string());
    }
    errors
}

/// `note: ...` lines for settings that are validated but have no effect in this passthrough-only build
/// (printed by `relayctl validate`). A setting still at its documented default produces no note.
pub fn ignored_setting_notes(cfg: &RelayConfig) -> Vec<String> {
    let mut notes = Vec::new();
    if cfg.codecs.video_encoder != "auto" {
        notes.push(format!(
            "note: codecs.video_encoder {} is ignored (no video encoder in this build)",
            cfg.codecs.video_encoder
        ));
    }
    if cfg.codecs.hw_decode != "none" {
        notes.push(format!(
            "note: codecs.hw_decode {} is ignored (no video decoder in this build)",
            cfg.codecs.hw_decode
        ));
    }
    if cfg.quality.audio_bitrate_kbps != AUDIO_BITRATE_DEFAULT_KBPS {
        notes.push(format!(
            "note: quality.audio_bitrate_kbps {} is ignored (Opus is forwarded as received, never re-encoded)",
            cfg.quality.audio_bitrate_kbps
        ));
    }
    if cfg.quality.approved_profile_manifest != DEFAULT_PROFILE_MANIFEST {
        notes.push(format!(
            "note: quality.approved_profile_manifest {} is ignored (no profile approval gate in this build)",
            cfg.quality.approved_profile_manifest
        ));
    }
    let c = &cfg.codecs;
    if c.output_codec == OutputCodec::H264
        && (c.parent_codecs == CodecPolicy::AutoDecode || c.watch_codecs == CodecPolicy::AutoDecode)
    {
        notes.push(
            "note: codecs.output_codec H264: parent_codecs and watch_codecs are treated as h264_only \
             (no video decoder in this build)"
                .to_string(),
        );
    }
    notes
}

#[cfg(test)]
mod reindent_tests {
    use super::reindent_flow_closers as fix;

    #[test]
    fn lone_closer_is_reindented_one_past_the_opener() {
        assert_eq!(
            fix("a:\n  b: [\n    {c: d}\n]   # x\n  e: 1\n"),
            "a:\n  b: [\n    {c: d}\n   ]   # x\n  e: 1\n"
        );
        assert_eq!(fix("a:\n  b: [\n    1\n  }\n"), "a:\n  b: [\n    1\n   }\n");
    }

    #[test]
    fn crlf_and_nesting() {
        assert_eq!(
            fix("a:

  b: [

    [1,

2

]

]

"),
            "a:

  b: [

    [1,

2

     ]

   ]

"
        );
    }

    #[test]
    fn everything_else_is_untouched() {
        for src in [
            "b: [\n]\n",                         // opener at column 0
            "a:\n  b: [1, 2]\nc: 1\n",           // closed on its own line
            "a:\n  b: [\n    1]\n",              // closer not alone
            "a:\n  b: |\n    x: [\n]\n  c: 1\n", // block scalar body
            "a:\n  b: \"[\n]\"\n",               // quoted
            "a:\n  b: '[' \n]\n",
            "a:\n  b: x # [\n]\n", // comment
        ] {
            assert_eq!(fix(src), src, "{src:?}");
        }
    }
}
