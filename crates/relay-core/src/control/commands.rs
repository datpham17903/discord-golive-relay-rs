//! Chat-command parser and routing planner. Pure: no I/O, no Discord objects. Port of
//! `control/commands.py`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

use crate::supervisor::routing::Routing;

pub use crate::config::{COMMAND_NAMES, DEFAULT_PUBLIC_COMMANDS};

// `op` is deliberately not in COMMAND_NAMES: it can never be made public

const HELP_LINES: &[(&str, &str)] = &[
    (
        "status",
        "• `status` - xem trạng thái hiện tại (`status -v`: thêm bitrate, fps, số lần đứng hình)",
    ),
    ("on", "• `on` / `off` - bật hoặc tắt relay"),
    ("target", "• `target <user_id>` - đổi người được relay"),
    ("channel", "• `channel <channel_id>` - đổi kênh voice chính"),
    ("mirror", "• `mirror` - mirror kênh voice bạn đang ngồi"),
    (
        "mirror",
        "• `mirror <channel_id> [account_id]` - phát lại sang một kênh voice khác (có thể chỉ định tài khoản mirror)",
    ),
    (
        "unmirror",
        "• `unmirror` - gỡ mirror của kênh voice bạn đang ngồi",
    ),
    ("unmirror", "• `unmirror <channel_id|all>` - gỡ mirror"),
    (
        "prejoin",
        "• `prejoin <on|off>` - bật hoặc tắt việc vào sẵn kênh voice trước khi target stream",
    ),
    (
        "autoscale",
        "• `autoscale <on|off>` - bật hoặc tắt tự động tăng/giảm số relay theo người xem (chỉ operator)",
    ),
    ("op", "• `op list` - xem danh sách operator"),
    (
        "op",
        "• `op add <user_id>` / `op remove <user_id>` - thêm hoặc gỡ operator (chỉ chủ sở hữu)",
    ),
];

pub fn help_text(public: &BTreeSet<String>) -> String {
    let mut lines = vec![
        "ℹ️ **Lệnh relay** (lệnh *công khai* ai cũng dùng được; lệnh khác cần quyền operator)"
            .to_string(),
    ];
    for (name, line) in HELP_LINES {
        let open = public.contains(*name) && (*name != "on" || public.contains("off"));
        lines.push(format!(
            "{line}{}",
            if open { " *(công khai)*" } else { "" }
        ));
    }
    lines.join("\n")
}

fn default_public() -> BTreeSet<String> {
    DEFAULT_PUBLIC_COMMANDS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// `help_text` for `DEFAULT_PUBLIC_COMMANDS`.
pub static HELP: LazyLock<String> = LazyLock::new(|| help_text(&default_public()));

fn usage(sub: &str) -> &'static str {
    match sub {
        "target" => "`target <user_id>`",
        "channel" => "`channel <channel_id>`",
        "mirror" => "`mirror` (đang ở kênh voice) hoặc `mirror <channel_id> [account_id]`",
        "unmirror" => "`unmirror` (đang ở kênh voice) hoặc `unmirror <channel_id|all>`",
        "on" => "`on`",
        "off" => "`off`",
        "status" => "`status` hoặc `status -v`",
        "prejoin" => "`prejoin <on|off>`",
        "autoscale" => "`autoscale <on|off>`",
        "op" => "`op list`, `op add <user_id>` hoặc `op remove <user_id>`",
        _ => unreachable!("no usage text for {sub}"),
    }
}

/// A guild message, already filtered by guild/channel/relay account by the adapter; `text` is
/// never logged (no `Debug` on purpose).
#[derive(Clone, PartialEq, Eq)]
pub struct ChatCommand {
    pub author_id: u64,
    pub channel_id: u64,
    pub message_id: u64,
    pub text: String,
    pub author_role_ids: BTreeSet<u64>,
    /// Author's current voice channel in the configured guild.
    pub author_voice_channel_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Status {
        verbose: bool,
    },
    SetEnabled(bool),
    SetPrejoin(bool),
    SetAutoscale(bool),
    SetTarget(u64),
    SetChannel(u64),
    AddMirror {
        /// `None` = the author's current voice channel.
        channel_id: Option<u64>,
        /// `None` = relay picks the account.
        account: Option<u64>,
    },
    RemoveMirror {
        /// `None` = all, unless `here`.
        channel_id: Option<u64>,
        /// Bare `unmirror`: the author's current voice channel.
        here: bool,
    },
    OpList,
    OpAdd(u64),
    OpRemove(u64),
}

/// Ready-to-send reply for a malformed command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

type IdFn = Box<dyn Fn(u64) -> Option<String> + Send + Sync>;

pub struct RoutingContext {
    /// Ingest, relay and mirror accounts.
    pub relay_user_ids: BTreeSet<u64>,
    pub mirror_order: Vec<u64>,
    pub unavailable: BTreeSet<u64>,
    pub voice_channel_ids: BTreeSet<u64>,
    pub user_name: IdFn,
    /// Mirror account -> its fixed channel.
    pub mirror_fixed: HashMap<u64, u64>,
    pub is_bot: Box<dyn Fn(u64) -> bool + Send + Sync>,
    pub role_name: IdFn,
}

impl RoutingContext {
    pub fn new(
        relay_user_ids: BTreeSet<u64>,
        mirror_order: Vec<u64>,
        unavailable: BTreeSet<u64>,
        voice_channel_ids: BTreeSet<u64>,
    ) -> Self {
        Self {
            relay_user_ids,
            mirror_order,
            unavailable,
            voice_channel_ids,
            user_name: Box::new(|_| None),
            mirror_fixed: HashMap::new(),
            is_bot: Box::new(|_| false),
            role_name: Box::new(|_| None),
        }
    }
}

pub struct OperatorContext {
    /// Owners listed in config.
    pub config_user_ids: BTreeSet<u64>,
    /// Owner roles listed in config.
    pub role_ids: BTreeSet<u64>,
    /// Chat-added operators (state.json).
    pub added: BTreeSet<u64>,
    pub target_user_id: u64,
}

/// One row of `StatusDetails::accounts`.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountStatus {
    pub uid: u64,
    pub role: String,
    pub channel_id: Option<u64>,
    pub state: String,
    pub uptime_s: Option<f64>,
    /// `None` when unknown.
    pub viewers: Option<u32>,
    /// Seconds until an idle extra relay stops.
    pub stop_in: Option<f64>,
}

/// One row of `StatusDetails::tech`; `uid` `None` = the ingest output.
#[derive(Debug, Clone, PartialEq)]
pub struct TechRow {
    pub uid: Option<u64>,
    /// (kbps, fps); `None` when not playing.
    pub rate: Option<(f64, f64)>,
    /// Freezes in the last 5 min.
    pub freezes_recent: u32,
    /// Freezes this session.
    pub freezes_session: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatusDetails {
    pub session_state: String,
    pub session_uptime_s: Option<f64>,
    pub viewers: u32,
    pub relay_wanted: u32,
    pub relay_running: u32,
    pub relay_available: u32,
    pub relay_short_by: u32,
    /// Extra relays still running because their Go Live has viewers.
    pub relay_kept: u32,
    pub prejoin: bool,
    pub autoscale: bool,
    pub debounce_ms: u32,
    pub audio_mode: String,
    pub output_size: String,
    pub last_watch_ms: Option<f64>,
    pub audio_ok: bool,
    pub accounts: Vec<AccountStatus>,
    pub public_commands: Vec<String>,
    pub command_cooldown_s: u64,
    pub change_cooldown_s: u64,
    /// Viewers one Go Live carries.
    pub viewer_cap: u32,
    pub ingest_publishes: bool,
    pub fatal_reason: Option<String>,
    /// Exactly the extra relays counted in `relay_kept`.
    pub relay_kept_ids: Vec<u64>,
    /// Codec every output carries now (one for the whole chain); `None` = unknown.
    pub codec: Option<String>,
    /// (from, to) while the chain is changing codec.
    pub codec_switch: Option<(String, String)>,
    /// `status -v` only; `None` = not asked. One row per output.
    pub tech: Option<Vec<TechRow>>,
    /// Source frames lost this session.
    pub gap_frames: Option<u32>,
    /// Seconds the target's Go Live has been full; `None` = not full.
    pub stream_full_s: Option<f64>,
}

impl Default for StatusDetails {
    /// Python dataclass defaults (`viewer_cap` = 50); the fields without one are zero/empty.
    fn default() -> Self {
        Self {
            session_state: String::new(),
            session_uptime_s: None,
            viewers: 0,
            relay_wanted: 0,
            relay_running: 0,
            relay_available: 0,
            relay_short_by: 0,
            relay_kept: 0,
            prejoin: false,
            autoscale: false,
            debounce_ms: 0,
            audio_mode: String::new(),
            output_size: String::new(),
            last_watch_ms: None,
            audio_ok: false,
            accounts: Vec::new(),
            public_commands: Vec::new(),
            command_cooldown_s: 0,
            change_cooldown_s: 0,
            viewer_cap: 50,
            ingest_publishes: false,
            fatal_reason: None,
            relay_kept_ids: Vec::new(),
            codec: None,
            codec_switch: None,
            tech: None,
            gap_frames: None,
            stream_full_s: None,
        }
    }
}

/// Every output is passthrough in this port; the footer still shows the mode.
const VIDEO_MODE: &str = "passthrough";

fn codec_name(codec: &str) -> &str {
    match codec {
        "AV1" => "AV1",
        "H265" => "H.265",
        "H264" => "H.264",
        "VP8" => "VP8",
        "VP9" => "VP9",
        other => other,
    }
}

/// Python `str.isspace` (Unicode White_Space plus the ASCII separators U+001C..U+001F).
fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.split()` with no argument.
fn py_split(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_isspace).filter(|t| !t.is_empty())
}

/// Unicode general category Cf.
fn is_format_char(c: char) -> bool {
    matches!(c as u32,
        0x00AD | 0x0600..=0x0605 | 0x061C | 0x06DD | 0x070F | 0x0890..=0x0891 | 0x08E2 | 0x180E
        | 0x200B..=0x200F | 0x202A..=0x202E | 0x2060..=0x2064 | 0x2066..=0x206F | 0xFEFF
        | 0xFFF9..=0xFFFB | 0x110BD | 0x110CD | 0x13430..=0x1343F | 0x1BCA0..=0x1BCA3
        | 0x1D173..=0x1D17A | 0xE0001 | 0xE0020..=0xE007F)
}

const NAME_UNSAFE: &str = "<>@`[]()*_~|\\:";

fn clean(raw: Option<String>) -> String {
    // display names are user-controlled: no mention, link, markdown or bidi/format trick may survive into a reply
    let raw: String = raw
        .unwrap_or_default()
        .chars()
        .filter(|c| !NAME_UNSAFE.contains(*c))
        .filter(|c| !(is_format_char(*c) || c.is_control()) || py_isspace(*c))
        .collect();
    py_split(&raw).collect::<Vec<_>>().join(" ")
}

fn user(ctx: &RoutingContext, uid: u64) -> String {
    let name = clean((ctx.user_name)(uid));
    if name.is_empty() {
        uid.to_string()
    } else {
        format!("{name} ({uid})")
    }
}

fn ch(cid: u64) -> String {
    format!("<#{cid}>")
}

/// First `n` characters (Python `s[:n]`).
fn prefix_chars(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn usage_error(sub: &str, why: &str) -> ParseError {
    ParseError(format!("❌ {why}. Cú pháp: {}", usage(sub)))
}

fn parse_id(sub: &str, raw: &str) -> Result<u64, ParseError> {
    let id = if raw.bytes().all(|b| b.is_ascii_digit()) && raw.len() <= 20 {
        raw.parse::<u64>().ok().filter(|n| *n > 0)
    } else {
        None
    };
    id.ok_or_else(|| {
        usage_error(
            sub,
            "**ID không hợp lệ** (chỉ nhận số thập phân, không dùng mention)",
        )
    })
}

/// The config name of a parsed command (as in `control.public_commands`).
pub fn command_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::SetEnabled(true) => "on",
        Command::SetEnabled(false) => "off",
        Command::Status { .. } => "status",
        Command::SetPrejoin(_) => "prejoin",
        Command::SetAutoscale(_) => "autoscale",
        Command::SetTarget(_) => "target",
        Command::SetChannel(_) => "channel",
        Command::AddMirror { .. } => "mirror",
        Command::RemoveMirror { .. } => "unmirror",
        Command::OpList | Command::OpAdd(_) | Command::OpRemove(_) => "op",
    }
}

/// `None`: not a relay command (other prefix).
pub fn parse(
    text: &str,
    prefix: &str,
    public: &BTreeSet<String>,
) -> Option<Result<Command, ParseError>> {
    let tokens: Vec<&str> = py_split(text).collect();
    if tokens.is_empty() || tokens[0].to_lowercase() != prefix.to_lowercase() {
        return None;
    }
    Some(parse_tokens(&tokens, public))
}

fn parse_tokens(tokens: &[&str], public: &BTreeSet<String>) -> Result<Command, ParseError> {
    if tokens.len() == 1 {
        return Err(ParseError(help_text(public)));
    }
    let sub = tokens[1].to_lowercase();
    let sub = sub.as_str();
    let args = &tokens[2..];
    if sub == "status"
        && args.len() == 1
        && matches!(args[0].to_lowercase().as_str(), "-v" | "verbose")
    {
        return Ok(Command::Status { verbose: true });
    }
    match sub {
        "status" | "on" | "off" => {
            if !args.is_empty() {
                return Err(usage_error(sub, "**Thừa tham số**"));
            }
            Ok(match sub {
                "status" => Command::Status { verbose: false },
                _ => Command::SetEnabled(sub == "on"),
            })
        }
        "prejoin" | "autoscale" => {
            let arg = args.first().map(|a| a.to_lowercase());
            match (args.len(), arg.as_deref()) {
                (1, Some(a @ ("on" | "off"))) => {
                    let on = a == "on";
                    Ok(if sub == "prejoin" {
                        Command::SetPrejoin(on)
                    } else {
                        Command::SetAutoscale(on)
                    })
                }
                _ => Err(usage_error(sub, "**Cần đúng một tham số** `on` hoặc `off`")),
            }
        }
        "op" => {
            let action = args.first().map(|a| a.to_lowercase());
            match action.as_deref() {
                Some("list") => {
                    if args.len() == 1 {
                        Ok(Command::OpList)
                    } else {
                        Err(usage_error(sub, "**Thừa tham số**"))
                    }
                }
                Some(a @ ("add" | "remove")) => {
                    if args.len() != 2 {
                        return Err(usage_error(sub, "**Cần đúng một user ID**"));
                    }
                    let n = parse_id(sub, args[1])?;
                    Ok(if a == "add" {
                        Command::OpAdd(n)
                    } else {
                        Command::OpRemove(n)
                    })
                }
                _ => Err(usage_error(sub, "**Thiếu hoặc sai hành động**")),
            }
        }
        "mirror" => {
            if args.is_empty() {
                return Ok(Command::AddMirror {
                    channel_id: None,
                    account: None,
                });
            }
            if args.len() > 2 {
                return Err(usage_error(sub, "**Cần kênh và tùy chọn một tài khoản**"));
            }
            let ids = args
                .iter()
                .map(|a| parse_id(sub, a))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::AddMirror {
                channel_id: Some(ids[0]),
                account: ids.get(1).copied(),
            })
        }
        "target" | "channel" | "unmirror" => {
            if sub == "unmirror" && args.is_empty() {
                return Ok(Command::RemoveMirror {
                    channel_id: None,
                    here: true,
                });
            }
            if args.len() != 1 {
                return Err(usage_error(sub, "**Cần đúng một tham số**"));
            }
            if sub == "unmirror" && args[0].to_lowercase() == "all" {
                return Ok(Command::RemoveMirror {
                    channel_id: None,
                    here: false,
                });
            }
            let n = parse_id(sub, args[0])?;
            Ok(match sub {
                "target" => Command::SetTarget(n),
                "channel" => Command::SetChannel(n),
                _ => Command::RemoveMirror {
                    channel_id: Some(n),
                    here: false,
                },
            })
        }
        _ => Err(ParseError(help_text(public))),
    }
}

fn reject(why: &str, usage: Option<&str>) -> String {
    match usage {
        Some(u) => format!("⚠️ {why} Dùng: {u}"),
        None => format!("⚠️ {why}"),
    }
}

/// Bare `mirror`/`unmirror` -> the concrete channel command, or a rejection reply.
pub fn resolve_here(
    cmd: &Command,
    routing: &Routing,
    voice_channel_id: Option<u64>,
) -> Result<Command, String> {
    let bare_add = matches!(
        cmd,
        Command::AddMirror {
            channel_id: None,
            ..
        }
    );
    let bare_remove = matches!(cmd, Command::RemoveMirror { here: true, .. });
    if !(bare_add || bare_remove) {
        return Ok(cmd.clone());
    }
    let Some(vc) = voice_channel_id else {
        return Err(reject(
            "**Bạn đang không ở kênh voice nào.**",
            Some(usage(if bare_add { "mirror" } else { "unmirror" })),
        ));
    };
    if let Command::AddMirror { account, .. } = cmd {
        return Ok(Command::AddMirror {
            channel_id: Some(vc),
            account: *account,
        });
    }
    if !routing.mirrors.contains_key(&vc) {
        return Err(reject(
            &format!("**Kênh** {} chưa có mirror.", ch(vc)),
            Some("`status` để xem danh sách mirror"),
        ));
    }
    Ok(Command::RemoveMirror {
        channel_id: Some(vc),
        here: false,
    })
}

/// New routing, or a rejection reply. Status/SetEnabled/SetPrejoin/SetAutoscale leave routing
/// unchanged.
pub fn plan_change(
    cmd: &Command,
    routing: &Routing,
    ctx: &RoutingContext,
) -> Result<Routing, String> {
    match cmd {
        Command::SetTarget(uid) => {
            if ctx.relay_user_ids.contains(uid) {
                return Err(reject(
                    &format!(
                        "**Không thể đặt target** {}: đây là tài khoản của relay (ingest/output).",
                        user(ctx, *uid)
                    ),
                    Some("`target <user_id>` với người dùng thật"),
                ));
            }
            Ok(Routing {
                target_user_id: *uid,
                ..routing.clone()
            })
        }
        Command::SetChannel(c) => {
            let c = *c;
            if routing.mirrors.contains_key(&c) {
                return Err(reject(
                    &format!(
                        "**Không thể đặt kênh chính** {}: kênh này đang là mirror.",
                        ch(c)
                    ),
                    Some(&format!("`unmirror {c}` trước")),
                ));
            }
            if !ctx.voice_channel_ids.contains(&c) {
                return Err(reject(
                    &format!("**Kênh** {} không phải kênh voice của server.", ch(c)),
                    Some("`channel <channel_id>` với kênh voice"),
                ));
            }
            Ok(Routing {
                channel_id: c,
                ..routing.clone()
            })
        }
        Command::AddMirror {
            channel_id: Some(c),
            account,
        } => plan_add_mirror(*c, *account, routing, ctx),
        Command::RemoveMirror { channel_id, .. } => match channel_id {
            None => {
                if routing.mirrors.is_empty() {
                    return Err(reject("**Chưa có mirror nào** để gỡ.", None));
                }
                Ok(Routing {
                    mirrors: Default::default(),
                    ..routing.clone()
                })
            }
            Some(c) => {
                if !routing.mirrors.contains_key(c) {
                    return Err(reject(
                        &format!("**Kênh** {} không phải mirror.", ch(*c)),
                        Some("`status` để xem danh sách mirror"),
                    ));
                }
                let mut r = routing.clone();
                r.mirrors.shift_remove(c);
                Ok(r)
            }
        },
        _ => Ok(routing.clone()),
    }
}

fn plan_add_mirror(
    c: u64,
    account: Option<u64>,
    routing: &Routing,
    ctx: &RoutingContext,
) -> Result<Routing, String> {
    if c == routing.channel_id {
        return Err(reject(
            &format!("**Không thể mirror** {}: đây là kênh chính.", ch(c)),
            Some("`mirror <channel_id>` với kênh khác"),
        ));
    }
    if let Some(&by) = routing.mirrors.get(&c) {
        return Err(reject(
            &format!("**Kênh** {} đã có mirror bởi {}.", ch(c), user(ctx, by)),
            Some(&format!("`unmirror {c}` để gỡ")),
        ));
    }
    if !ctx.voice_channel_ids.contains(&c) {
        return Err(reject(
            &format!("**Kênh** {} không phải kênh voice của server.", ch(c)),
            Some("`mirror <channel_id>` với kênh voice"),
        ));
    }
    let used: HashMap<u64, u64> = routing.mirrors.iter().map(|(c, a)| (*a, *c)).collect();
    let with_mirror = |acct: u64| {
        let mut r = routing.clone();
        r.mirrors.insert(c, acct);
        r
    };
    if let Some(acct) = account {
        if !ctx.mirror_order.contains(&acct) {
            let why = if ctx.relay_user_ids.contains(&acct) {
                "đây là tài khoản relay, không dùng làm mirror."
            } else {
                "không phải tài khoản mirror trong config."
            };
            return Err(reject(
                &format!("**Không thể dùng** {}: {why}", user(ctx, acct)),
                Some("`status` để xem tài khoản mirror"),
            ));
        }
        if let Some(&used_ch) = used.get(&acct) {
            return Err(reject(
                &format!(
                    "**Tài khoản** {} đang mirror {}.",
                    user(ctx, acct),
                    ch(used_ch)
                ),
                Some(&format!("`unmirror {used_ch}` trước")),
            ));
        }
        if ctx.unavailable.contains(&acct) {
            return Err(reject(
                &format!(
                    "**Tài khoản** {} đang lỗi, không dùng được.",
                    user(ctx, acct)
                ),
                None,
            ));
        }
        if let Some(&fixed) = ctx.mirror_fixed.get(&acct)
            && fixed != c
        {
            return Err(reject(
                &format!(
                    "**Tài khoản** {} được cố định cho kênh {}.",
                    user(ctx, acct),
                    ch(fixed)
                ),
                None,
            ));
        }
        return Ok(with_mirror(acct));
    }
    let usable: Vec<u64> = ctx
        .mirror_order
        .iter()
        .copied()
        .filter(|a| !used.contains_key(a) && !ctx.unavailable.contains(a))
        .collect();
    let own = usable
        .iter()
        .copied()
        .find(|a| ctx.mirror_fixed.get(a) == Some(&c));
    let free = usable
        .iter()
        .copied()
        .rfind(|a| !ctx.mirror_fixed.contains_key(a));
    match own.or(free) {
        Some(acct) => Ok(with_mirror(acct)),
        None => Err(reject(
            &format!(
                "**Hết tài khoản mirror** cho kênh này: đang mirror {} kênh.",
                used.len()
            ),
            Some("`unmirror <channel_id>` để giải phóng một tài khoản"),
        )),
    }
}

/// Reply for an applied command. For `AddMirror`/`RemoveMirror` with a channel, `new`/`old` must
/// hold the planned/previous mirror (as Python indexes them).
pub fn describe_change(
    cmd: &Command,
    old: &Routing,
    new: &Routing,
    ctx: &RoutingContext,
) -> String {
    match cmd {
        Command::SetEnabled(on) => if *on {
            "✅ **Đã bật relay**"
        } else {
            "✅ **Đã tắt relay**"
        }
        .to_string(),
        Command::SetPrejoin(true) => {
            "✅ **Đã bật pre-join**: relay vào sẵn kênh voice chính khi target có mặt.".to_string()
        }
        Command::SetPrejoin(false) => {
            "✅ **Đã tắt pre-join**: relay chỉ vào voice khi target bắt đầu stream.".to_string()
        }
        Command::SetAutoscale(true) => {
            "✅ **Đã bật autoscale**: số relay chạy theo số người xem; relay thừa chỉ dừng khi không còn ai xem."
                .to_string()
        }
        Command::SetAutoscale(false) => {
            "✅ **Đã tắt autoscale**: mọi tài khoản relay đều phát.".to_string()
        }
        Command::SetTarget(_) => format!(
            "✅ **Đã đổi target** thành {}. Relay sẽ vào lại khi người này stream trong {}.",
            user(ctx, new.target_user_id),
            ch(new.channel_id)
        ),
        Command::SetChannel(_) => {
            format!("✅ **Đã đổi kênh chính** thành {}.", ch(new.channel_id))
        }
        Command::AddMirror {
            channel_id: Some(c),
            ..
        } => format!(
            "✅ **Đã thêm mirror** {}: {} sẽ phát ở đó.",
            ch(*c),
            user(ctx, new.mirrors[c])
        ),
        Command::RemoveMirror {
            channel_id: None, ..
        } => format!("✅ **Đã gỡ toàn bộ mirror** ({} kênh).", old.mirrors.len()),
        Command::RemoveMirror {
            channel_id: Some(c),
            ..
        } => format!(
            "✅ **Đã gỡ mirror** {}; {} được giải phóng.",
            ch(*c),
            user(ctx, old.mirrors[c])
        ),
        _ => "✅ **Xong**".to_string(),
    }
}

/// New chat-added operator set, or a rejection reply. Owner-only: the caller checks the author.
/// `cmd` must be `OpAdd` or `OpRemove`.
pub fn plan_op(
    cmd: &Command,
    ops: &OperatorContext,
    ctx: &RoutingContext,
) -> Result<BTreeSet<u64>, String> {
    let (add, uid) = match cmd {
        Command::OpAdd(u) => (true, *u),
        Command::OpRemove(u) => (false, *u),
        _ => panic!("plan_op takes OpAdd or OpRemove"),
    };
    if add {
        if ctx.relay_user_ids.contains(&uid) || (ctx.is_bot)(uid) {
            return Err(reject(
                &format!(
                    "**Không thể thêm** {}: đây là tài khoản bot/relay, không bao giờ là operator.",
                    user(ctx, uid)
                ),
                None,
            ));
        }
        if ops.config_user_ids.contains(&uid) {
            return Err(reject(
                &format!(
                    "{} **đã là operator** (khai báo trong config).",
                    user(ctx, uid)
                ),
                None,
            ));
        }
        if ops.added.contains(&uid) {
            return Err(reject(
                &format!("{} **đã là operator**.", user(ctx, uid)),
                Some("`op list` để xem danh sách"),
            ));
        }
        let mut added = ops.added.clone();
        added.insert(uid);
        return Ok(added);
    }
    if ops.config_user_ids.contains(&uid) {
        return Err(reject(
            &format!(
                "**Không thể gỡ** {}: được khai báo trong config, sửa config để gỡ.",
                user(ctx, uid)
            ),
            None,
        ));
    }
    if !ops.added.contains(&uid) {
        return Err(reject(
            &format!("{} **không phải operator** thêm qua chat.", user(ctx, uid)),
            Some("`op list` để xem danh sách"),
        ));
    }
    let mut added = ops.added.clone();
    added.remove(&uid);
    Ok(added)
}

/// `cmd` must be `OpAdd` or `OpRemove`.
pub fn describe_op(cmd: &Command, ctx: &RoutingContext) -> String {
    match cmd {
        Command::OpAdd(uid) => format!("✅ **Đã thêm operator** {}.", user(ctx, *uid)),
        Command::OpRemove(uid) => format!("✅ **Đã gỡ operator** {}.", user(ctx, *uid)),
        _ => panic!("describe_op takes OpAdd or OpRemove"),
    }
}

/// Sanitized name and full id (copyable for `op remove`).
fn who(ctx: &RoutingContext, uid: u64) -> String {
    let sid = uid.to_string();
    if user(ctx, uid) == sid {
        format!("`{sid}`")
    } else {
        format!("**{}** `{sid}`", short(ctx, uid))
    }
}

const MAX_LEN: usize = 2000;

pub fn format_operators(ops: &OperatorContext, ctx: &RoutingContext) -> String {
    fn block(title: &str, items: Vec<String>) -> Vec<String> {
        let mut out = vec![title.to_string()];
        if items.is_empty() {
            out.push("> không có".to_string());
        } else {
            out.extend(items.into_iter().map(|i| format!("> {i}")));
        }
        out
    }

    let roles: Vec<String> = ops
        .role_ids
        .iter()
        .map(|rid| {
            let name = clean((ctx.role_name)(*rid));
            let name = prefix_chars(&name, 32).trim_end();
            if name.is_empty() {
                format!("`{rid}`")
            } else {
                format!("**{name}** `{rid}`")
            }
        })
        .collect();
    let mut lines = vec!["👥 **Operator**".to_string()];
    lines.extend(block(
        "**Chủ sở hữu** (config)",
        ops.config_user_ids.iter().map(|u| who(ctx, *u)).collect(),
    ));
    lines.extend(block("**Role chủ sở hữu** (config)", roles));
    lines.extend(block(
        "**Thêm qua chat**",
        ops.added.iter().map(|u| who(ctx, *u)).collect(),
    ));
    let out = lines.join("\n");
    if out.chars().count() >= MAX_LEN {
        let cut = prefix_chars(&out, MAX_LEN - 2);
        let cut = cut.rsplit_once('\n').map_or(cut, |(head, _)| head);
        return format!("{cut}\n…");
    }
    out
}

fn dur(s: Option<f64>) -> String {
    let t = s.unwrap_or(0.0) as i64;
    let (h, m) = (t.div_euclid(3600), t.div_euclid(60).rem_euclid(60));
    if h != 0 {
        if m != 0 {
            format!("{h} giờ {m} phút")
        } else {
            format!("{h} giờ")
        }
    } else if m != 0 {
        format!("{m} phút")
    } else {
        format!("{t} giây")
    }
}

fn session_label(state: &str) -> Option<&'static str> {
    Some(match state {
        "relaying" => "🟢 Đang phát",
        "connecting" | "joining" => "🟡 Đang kết nối",
        "hold" => "🟠 Tạm giữ",
        "backoff" => "🟠 Đang thử lại",
        "stopping" => "🟠 Đang dừng",
        "idle" => "🔵 Chờ Target phát",
        "disabled" => "⚫ Đã tắt",
        "fatal" | "quarantine" => "🔴 Lỗi",
        _ => return None,
    })
}

fn account_dot(state: &str) -> &'static str {
    match state {
        "live" => "🟢",
        "joining" | "ready" | "stopping" => "🟡",
        "backoff" => "🟠",
        "fatal" | "quarantine" => "🔴",
        "off" => "⚫",
        _ => "⚪",
    }
}

fn account_text(state: &str) -> &str {
    match state {
        "joining" | "ready" => "đang kết nối",
        "stopping" => "đang dừng",
        "backoff" => "đang thử lại",
        "fatal" | "quarantine" => "lỗi",
        other => other,
    }
}

const BAR_CELLS: i64 = 10;

/// Sanitized display name (capped), or the short id when unknown.
fn short(ctx: &RoutingContext, uid: u64) -> String {
    let raw = user(ctx, uid);
    let sid = uid.to_string();
    let name = if raw != sid {
        &raw[..raw.len() - format!(" ({uid})").len()]
    } else {
        ""
    };
    if !name.is_empty() {
        return prefix_chars(name, 32).trim_end().to_string();
    }
    if sid.len() <= 8 {
        sid
    } else {
        format!("{}…{}", &sid[..4], &sid[sid.len() - 4..])
    }
}

fn rate(kbps: f64) -> String {
    if kbps >= 1000.0 {
        format!("{:.1} Mbps", kbps / 1000.0).replace('.', ",")
    } else {
        format!("{kbps:.0} kbps")
    }
}

fn tech_line(name: &str, row: &TechRow) -> String {
    match row.rate {
        None => format!("> **{name}** · không phát"),
        Some((kbps, fps)) => format!(
            "> **{name}** · {} · {fps:.0} fps · đứng hình {} lần/5 phút ({} lần cả phiên)",
            rate(kbps),
            row.freezes_recent,
            row.freezes_session
        ),
    }
}

fn capacity_bar(used: i64, total: i64) -> String {
    let filled = if total <= 0 {
        0
    } else {
        BAR_CELLS.min(used.max(0) * BAR_CELLS / total)
    };
    "▰".repeat(filled as usize) + &"▱".repeat((BAR_CELLS - filled) as usize)
}

fn footer(d: &StatusDetails, with_public: bool) -> String {
    let mut parts = vec![
        format!("prejoin {}", if d.prejoin { "bật" } else { "tắt" }),
        format!("debounce {} ms", d.debounce_ms),
        format!("autoscale {}", if d.autoscale { "bật" } else { "tắt" }),
        format!("{} {VIDEO_MODE}", d.output_size.replacen('x', "×", 1)),
        format!("audio {}", d.audio_mode),
    ];
    if with_public {
        let public = if d.public_commands.is_empty() {
            "không có".to_string()
        } else {
            d.public_commands.join(", ")
        };
        parts.push(format!("lệnh công khai: {public}"));
        parts.push(format!(
            "cooldown {}s/{}s",
            d.command_cooldown_s, d.change_cooldown_s
        ));
    }
    format!("-# ⚙️ {}", parts.join(" · "))
}

fn viewers_text(viewers: Option<u32>) -> String {
    viewers.map_or("?".to_string(), |v| v.to_string())
}

pub fn format_status(
    routing: &Routing,
    ctx: &RoutingContext,
    d: &StatusDetails,
    routing_error: Option<&str>,
) -> String {
    let mut label = match session_label(&d.session_state) {
        Some(l) => l.to_string(),
        None => format!("⚪ {}", d.session_state),
    };
    if matches!(d.session_state.as_str(), "fatal" | "quarantine")
        && let Some(reason) = d.fatal_reason.as_deref().filter(|r| !r.is_empty())
    {
        label.push_str(&format!(" ({reason})"));
    }
    let mut head = vec![format!("# 📡 Relay · {label}")];
    let mut sub: Vec<String> = Vec::new();
    if let Some(up) = d.session_uptime_s {
        sub.push(format!("chạy {}", dur(Some(up))));
    }
    if d.session_state == "relaying" {
        if let Some(ms) = d.last_watch_ms {
            sub.push(format!("watch {} ms", ms as i64));
        }
        sub.push(
            if d.audio_ok {
                "🔊 audio OK"
            } else {
                "🔇 không có audio"
            }
            .to_string(),
        );
    }
    if !sub.is_empty() {
        head.push(format!("-# {}", sub.join(" · ")));
    }
    if let Some(s) = d.stream_full_s {
        head.push(format!("🈵 **Stream của target đã đầy slot** ({}) — bot đang chờ chỗ trống. **Cần một người đang xem stream của target thoát ra để nhường slot.**", dur(Some(s))));
    }
    if routing_error.is_some() {
        head.push("⚠️ **state.json có routing không hợp lệ** (đang bị bỏ qua); relay sẽ dừng ở lần khởi động sau.".to_string());
    }

    let tid = routing.target_user_id;
    let sid = tid.to_string();
    let who = if user(ctx, tid) == sid {
        format!("`{sid}`") // name unknown: full id
    } else if sid.len() > 8 {
        format!(
            "**{}** `{}…{}`",
            short(ctx, tid),
            &sid[..4],
            &sid[sid.len() - 4..]
        )
    } else {
        format!("**{}** `{sid}`", short(ctx, tid))
    };
    let mut source = vec![
        "### 🎯 Nguồn".to_string(),
        format!("> {who}"),
        format!(
            "> Kênh chính {} · 👥 **{}** người",
            ch(routing.channel_id),
            d.viewers
        ),
    ];
    if let Some(codec) = &d.codec {
        let mut line = format!("> Codec: {}", codec_name(codec));
        if let Some((_, to)) = &d.codec_switch {
            line.push_str(&format!(" → {} (đang chuyển)", codec_name(to)));
        }
        source.push(line);
    }

    let relays: Vec<&AccountStatus> = d.accounts.iter().filter(|a| a.role == "relay").collect();
    let mut relay_title = String::new();
    let mut relay_head: Vec<String> = Vec::new();
    let mut relay_lines: Vec<String> = Vec::new();
    if !relays.is_empty() {
        relay_title = format!(
            "### 📶 Relay · {}/{} chạy{}",
            d.relay_running,
            relays.len(),
            if d.autoscale {
                ""
            } else {
                " · autoscale tắt"
            }
        );
        let cap = i64::from(d.viewer_cap);
        let base = (cap - 1) + if d.ingest_publishes { cap } else { 0 };
        let total = base + cap * i64::from(d.relay_running);
        let mut line = format!(
            "> {} sức chứa {}/{total}",
            capacity_bar(i64::from(d.viewers), total),
            d.viewers
        );
        if (d.relay_running as usize) < relays.len() {
            // only running relays count; say what all of them would hold
            line.push_str(&format!(
                " (tối đa {} khi bật đủ {} relay)",
                base + cap * relays.len() as i64,
                relays.len()
            ));
        }
        relay_head.push(line);
        if d.relay_short_by > 0 {
            relay_head.push(format!("> ⚠️ thiếu {} relay", d.relay_short_by));
        }
        let kept: HashSet<u64> = d.relay_kept_ids.iter().copied().collect();
        for a in &relays {
            let mut line = format!("> {} **{}**", account_dot(&a.state), short(ctx, a.uid));
            if a.state == "live" {
                line.push_str(&format!(" · 👁 {}", viewers_text(a.viewers)));
            } else if a.state == "off" {
                line.push_str(" · rảnh");
            } else {
                line.push_str(&format!(" · {}", account_text(&a.state)));
            }
            if kept.contains(&a.uid) {
                line.push_str(" · *giữ vì còn người xem*");
            }
            if let Some(stop_in) = a.stop_in {
                line.push_str(&format!(
                    " · *tắt sau {}s nếu không ai xem*",
                    stop_in.ceil() as i64
                ));
            }
            relay_lines.push(line);
        }
    }

    let rows: HashMap<u64, &AccountStatus> = d.accounts.iter().map(|a| (a.uid, a)).collect();
    let mut mirror_lines: Vec<String> = Vec::new();
    let has_mirror = d.accounts.iter().any(|a| a.role == "mirror");
    for (c, acct) in &routing.mirrors {
        let (state, viewers) = match rows.get(acct) {
            Some(row) => (row.state.as_str(), row.viewers),
            None => ("off", None),
        };
        let mut line = format!(
            "> {} {} ← **{}**",
            account_dot(state),
            ch(*c),
            short(ctx, *acct)
        );
        if state == "live" {
            line.push_str(&format!(" · 👁 {}", viewers_text(viewers)));
        } else if state != "off" {
            line.push_str(&format!(" · {}", account_text(state)));
        }
        mirror_lines.push(line);
    }
    for a in &d.accounts {
        if a.role == "mirror"
            && a.channel_id.is_none()
            && !routing.mirrors.values().any(|v| *v == a.uid)
        {
            let mut line = format!("> ⚫ **{}** · rảnh", short(ctx, a.uid));
            if let Some(fixed) = ctx.mirror_fixed.get(&a.uid) {
                line.push_str(&format!(" · cố định {}", ch(*fixed)));
            }
            mirror_lines.push(line);
        }
    }

    let mut tech_head: Vec<String> = Vec::new();
    let mut tech_lines: Vec<String> = Vec::new();
    if let Some(tech) = &d.tech {
        tech_head.push("### 📊 Kỹ thuật".to_string());
        for row in tech {
            match row.uid {
                None => tech_head.push(tech_line("Ingest", row)),
                Some(uid) => tech_lines.push(tech_line(&short(ctx, uid), row)),
            }
        }
        if let Some(gap) = d.gap_frames {
            tech_lines.push(format!("> Nguồn: mất {gap} frame cả phiên"));
        }
    }

    let build = |nr: usize, nm: usize, with_public: bool, nt: usize| -> String {
        let mut blocks = vec![head.join("\n"), source.join("\n")];
        if !relays.is_empty() {
            let mut lines: Vec<String> = relay_lines.iter().take(nr).cloned().collect();
            if nr < relay_lines.len() {
                lines.push(format!("> +{} relay khác", relay_lines.len() - nr));
            }
            let mut block = vec![relay_title.clone()];
            block.extend(relay_head.iter().cloned());
            block.extend(lines);
            blocks.push(block.join("\n"));
        }
        if has_mirror || !mirror_lines.is_empty() {
            let mut lines: Vec<String> = mirror_lines.iter().take(nm).cloned().collect();
            if nm < mirror_lines.len() {
                lines.push(format!("> +{} mirror khác", mirror_lines.len() - nm));
            }
            let mut block = vec!["### 🪞 Mirror".to_string()];
            block.extend(lines);
            blocks.push(block.join("\n"));
        }
        if !tech_head.is_empty() {
            let mut lines: Vec<String> = tech_lines.iter().take(nt).cloned().collect();
            if nt < tech_lines.len() {
                lines.push(format!("> +{} dòng khác", tech_lines.len() - nt));
            }
            let mut block = tech_head.clone();
            block.extend(lines);
            blocks.push(block.join("\n"));
        }
        blocks.push(footer(d, with_public));
        blocks.join("\n\n")
    };

    let (mut nr, mut nm, mut nt) = (relay_lines.len(), mirror_lines.len(), tech_lines.len());
    let mut out = build(nr, nm, true, nt);
    if out.chars().count() >= MAX_LEN {
        out = build(nr, nm, false, nt);
    }
    while out.chars().count() >= MAX_LEN && (nr > 0 || nm > 0 || nt > 0) {
        if nt >= nr && nt >= nm {
            nt -= 1;
        } else if nr >= nm {
            nr -= 1;
        } else {
            nm -= 1;
        }
        out = build(nr, nm, false, nt);
    }
    prefix_chars(&out, MAX_LEN - 1).to_string()
}
