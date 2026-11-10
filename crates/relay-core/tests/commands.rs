//! Port of `tests/control/test_commands.py`. `video_mode` is gone from `StatusDetails` (always
//! "passthrough"), so `_details()` no longer sets it.

use std::collections::{BTreeSet, HashMap};

use relay_core::control::commands::{
    AccountStatus, Command, DEFAULT_PUBLIC_COMMANDS, HELP, OperatorContext, ParseError,
    RoutingContext, StatusDetails, TechRow, command_name, describe_change, describe_op,
    format_operators, format_status, help_text, parse, plan_change, plan_op, resolve_here,
};
use relay_core::supervisor::routing::Routing;

const P: &str = "!relay";
const INGEST: u64 = 10;
const O1: u64 = 21;
const O2: u64 = 22;
const O3: u64 = 23;
const R1: u64 = 31; // relay account, never a mirror

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn strs(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

fn default_public() -> BTreeSet<String> {
    strs(DEFAULT_PUBLIC_COMMANDS)
}

fn ctx() -> RoutingContext {
    RoutingContext::new(
        set(&[INGEST, R1, O1, O2, O3]),
        vec![O1, O2, O3],
        set(&[]),
        set(&[100, 200, 300, 400]),
    )
}

fn names(map: &[(u64, &str)]) -> Box<dyn Fn(u64) -> Option<String> + Send + Sync> {
    let m: HashMap<u64, String> = map.iter().map(|(k, v)| (*k, v.to_string())).collect();
    Box::new(move |u| m.get(&u).cloned())
}

fn ctx_names(map: &[(u64, &str)]) -> RoutingContext {
    let mut c = ctx();
    c.user_name = names(map);
    c
}

fn routing(mirrors: &[(u64, u64)]) -> Routing {
    Routing {
        target_user_id: 555,
        channel_id: 100,
        mirrors: mirrors.iter().copied().collect(),
    }
}

fn p(text: &str) -> Option<Result<Command, ParseError>> {
    parse(text, P, &default_public())
}

fn perr(text: &str) -> ParseError {
    match p(text) {
        Some(Err(e)) => e,
        other => panic!("{text:?} should be a ParseError, got {other:?}"),
    }
}

fn pok(text: &str) -> Command {
    match p(text) {
        Some(Ok(c)) => c,
        other => panic!("{text:?} should parse, got {other:?}"),
    }
}

fn status() -> Command {
    Command::Status { verbose: false }
}

fn add_mirror(channel: u64, account: Option<u64>) -> Command {
    Command::AddMirror {
        channel_id: Some(channel),
        account,
    }
}

fn bare_add() -> Command {
    Command::AddMirror {
        channel_id: None,
        account: None,
    }
}

fn remove_mirror(channel: Option<u64>) -> Command {
    Command::RemoveMirror {
        channel_id: channel,
        here: false,
    }
}

fn bare_remove() -> Command {
    Command::RemoveMirror {
        channel_id: None,
        here: true,
    }
}

fn acc(
    uid: u64,
    role: &str,
    channel_id: Option<u64>,
    state: &str,
    uptime_s: Option<f64>,
    viewers: Option<u32>,
    stop_in: Option<f64>,
) -> AccountStatus {
    AccountStatus {
        uid,
        role: role.to_string(),
        channel_id,
        state: state.to_string(),
        uptime_s,
        viewers,
        stop_in,
    }
}

fn details() -> StatusDetails {
    StatusDetails {
        session_state: "relaying".to_string(),
        session_uptime_s: Some(3725.0),
        viewers: 120,
        relay_wanted: 1,
        relay_running: 1,
        relay_available: 2,
        relay_short_by: 0,
        relay_kept: 0,
        prejoin: true,
        autoscale: true,
        debounce_ms: 100,
        audio_mode: "passthrough".to_string(),
        output_size: "1920x1080@60".to_string(),
        last_watch_ms: Some(412.0),
        audio_ok: true,
        accounts: vec![
            acc(R1, "relay", Some(100), "live", Some(65.0), Some(3), None),
            acc(32, "relay", Some(100), "off", None, None, None),
            acc(O1, "mirror", Some(200), "ready", Some(12.0), None, None),
            acc(O2, "mirror", None, "off", None, None, None),
        ],
        ..StatusDetails::default()
    }
}

fn fmt(r: &Routing, c: &RoutingContext, d: &StatusDetails) -> String {
    format_status(r, c, d, None)
}

fn plan_err(cmd: &Command, r: &Routing, c: &RoutingContext) -> String {
    plan_change(cmd, r, c).unwrap_err()
}

fn plan_ok(cmd: &Command, r: &Routing, c: &RoutingContext) -> Routing {
    plan_change(cmd, r, c).unwrap()
}

fn mirrors(r: &Routing) -> Vec<(u64, u64)> {
    r.mirrors.iter().map(|(c, a)| (*c, *a)).collect()
}

#[test]
fn test_parse_rejects_mention_syntax() {
    for t in [
        "!relay target <@123>",
        "!relay target <@!123>",
        "!relay mirror <#123>",
    ] {
        perr(t);
    }
}

#[test]
fn test_parse_rejects_oversized_id() {
    perr(&format!("!relay target {}", "1".repeat(5000)));
    perr(&format!("!relay channel {}", "1".repeat(21)));
}

#[test]
fn test_parse_ignores_other_prefix() {
    assert_eq!(p("hello"), None);
    assert_eq!(p("!relayx status"), None);
    assert_eq!(p("!other status"), None);
}

#[test]
fn test_parse_commands() {
    assert_eq!(pok("!RELAY status"), status());
    assert_eq!(pok("!relay on"), Command::SetEnabled(true));
    assert_eq!(pok("!relay target 5"), Command::SetTarget(5));
    assert_eq!(pok("!relay channel 5"), Command::SetChannel(5));
    assert_eq!(pok("!relay mirror 5"), add_mirror(5, None));
    assert_eq!(pok("!relay mirror 5 22"), add_mirror(5, Some(22)));
    perr("!relay mirror 5 22 23");
    perr("!relay mirror 5 <@22>");
    assert_eq!(pok("!relay unmirror ALL"), remove_mirror(None));
    assert_eq!(pok("!relay unmirror 5"), remove_mirror(Some(5)));
    for bad in [
        "!relay target",
        "!relay target 0",
        "!relay target -1",
        "!relay target 1 2",
        "!relay status x",
    ] {
        let e = perr(bad);
        assert!(e.0.contains("Cú pháp") && e.0.starts_with('❌'), "{bad}");
    }
    assert_eq!(perr("!relay wat"), ParseError(HELP.clone()));
}

#[test]
fn test_target_cannot_be_relay_account() {
    for uid in [INGEST, O2] {
        assert!(plan_err(&Command::SetTarget(uid), &routing(&[]), &ctx()).starts_with("⚠️"));
    }
    assert_eq!(
        plan_ok(&Command::SetTarget(999), &routing(&[]), &ctx()).target_user_id,
        999
    );
}

#[test]
fn test_mirror_picks_last_free_output_skipping_unavailable() {
    let mut c = ctx();
    c.unavailable = set(&[O3]);
    let r = plan_ok(&add_mirror(200, None), &routing(&[]), &c);
    assert_eq!(mirrors(&r), [(200, O2)]);
    let r2 = plan_ok(&add_mirror(300, None), &r, &c);
    assert_eq!(mirrors(&r2), [(200, O2), (300, O1)]);
}

#[test]
fn test_mirror_does_not_mutate_old_routing() {
    let old = routing(&[]);
    plan_ok(&add_mirror(200, None), &old, &ctx());
    assert!(old.mirrors.is_empty());
}

#[test]
fn test_mirror_rejects_main_channel_and_duplicate() {
    assert!(plan_err(&add_mirror(100, None), &routing(&[]), &ctx()).starts_with("⚠️"));
    assert!(plan_err(&add_mirror(200, None), &routing(&[(200, O1)]), &ctx()).starts_with("⚠️"));
    assert!(plan_err(&add_mirror(999, None), &routing(&[]), &ctx()).starts_with("⚠️"));
}

#[test]
fn test_channel_rejects_current_mirror_channel() {
    let msg = plan_err(&Command::SetChannel(200), &routing(&[(200, O1)]), &ctx());
    assert!(msg.starts_with("⚠️") && msg.contains("unmirror 200"));
    assert!(plan_err(&Command::SetChannel(999), &routing(&[]), &ctx()).starts_with("⚠️"));
    assert_eq!(
        plan_ok(&Command::SetChannel(300), &routing(&[]), &ctx()).channel_id,
        300
    );
}

#[test]
fn test_unmirror_all() {
    let r = plan_ok(
        &remove_mirror(None),
        &routing(&[(200, O1), (300, O2)]),
        &ctx(),
    );
    assert!(r.mirrors.is_empty());
    let r = plan_ok(
        &remove_mirror(Some(200)),
        &routing(&[(200, O1), (300, O2)]),
        &ctx(),
    );
    assert_eq!(mirrors(&r), [(300, O2)]);
    assert!(plan_err(&remove_mirror(Some(400)), &routing(&[(200, O1)]), &ctx()).starts_with("⚠️"));
}

#[test]
fn test_no_free_account_error() {
    let mut c = ctx();
    c.unavailable = set(&[O3]);
    let msg = plan_err(
        &add_mirror(400, None),
        &routing(&[(200, O1), (300, O2)]),
        &c,
    );
    assert!(msg.starts_with("⚠️") && msg.contains('2'));
}

#[test]
fn test_status_format() {
    let c = ctx_names(&[(555, "Alice"), (O1, "Out1")]);
    let s = fmt(&routing(&[(200, O1)]), &c, &details());
    assert!(s.starts_with("# 📡 Relay · 🟢 Đang phát") && s.matches('\n').count() >= 8);
    assert!(
        s.contains("**Alice**")
            && s.contains("<#100>")
            && s.contains("<#200>")
            && s.contains("**Out1**")
    );
    assert!(s.contains("🪞 Mirror"));
}

#[test]
fn test_no_reply_contains_user_mention() {
    let mut c = ctx();
    c.user_name = Box::new(|_| Some("Bob".to_string()));
    let old = routing(&[(200, O1)]);
    let mut outs = vec![HELP.clone(), fmt(&old, &c, &details())];
    for cmd in [
        Command::SetTarget(999),
        Command::SetChannel(300),
        add_mirror(400, None),
        remove_mirror(Some(200)),
        remove_mirror(None),
        Command::SetEnabled(true),
    ] {
        let new = plan_ok(&cmd, &old, &c);
        outs.push(describe_change(&cmd, &old, &new, &c));
    }
    for cmd in [
        Command::SetTarget(INGEST),
        add_mirror(100, None),
        Command::SetChannel(200),
        remove_mirror(Some(7)),
    ] {
        outs.push(plan_err(&cmd, &old, &c));
    }
    for t in ["!relay target <@1>", "!relay x", "!relay mirror 1 2 3"] {
        outs.push(perr(t).0);
    }
    let not_in_voice = resolve_here(&bare_add(), &old, None).unwrap_err();
    let no_mirror = resolve_here(&bare_remove(), &old, Some(300)).unwrap_err();
    assert!(not_in_voice.contains("không ở kênh voice") && no_mirror.contains("chưa có mirror"));
    outs.push(not_in_voice);
    outs.push(no_mirror);
    assert!(outs.iter().all(|o| !o.contains("<@")));
}

#[test]
fn test_display_names_are_sanitized() {
    let c = ctx_names(&[(555, "<@123> @everyone `x`"), (O1, "<@&9>"), (999, "@<>`")]);
    let old = routing(&[(200, O1)]);
    let d = StatusDetails {
        output_size: "1920x1080".to_string(),
        ..details()
    };
    let new = plan_ok(&Command::SetTarget(999), &old, &c);
    let outs = [
        fmt(&old, &c, &d),
        describe_change(&Command::SetTarget(999), &old, &new, &c),
    ];
    for o in &outs {
        assert!(!o.contains("<@") && !o.contains('@') && !o.contains("`x`"));
    }
    assert!(outs[0].contains("**123 everyone x**"));
    assert!(outs[1].contains("đổi target** thành 999.")); // nothing left of the name: ID only
}

#[test]
fn test_parse_prejoin() {
    assert_eq!(pok("!relay prejoin on"), Command::SetPrejoin(true));
    assert_eq!(pok("!relay PREJOIN Off"), Command::SetPrejoin(false));
    for bad in [
        "!relay prejoin",
        "!relay prejoin yes",
        "!relay prejoin on off",
    ] {
        let e = perr(bad);
        assert!(
            e.0.contains("Cú pháp") && e.0.contains("`prejoin <on|off>`"),
            "{bad}"
        );
    }
    assert!(HELP.contains("prejoin"));
}

#[test]
fn test_prejoin_plan_and_reply() {
    let c = ctx();
    let old = routing(&[(200, O1)]);
    for on in [true, false] {
        assert_eq!(plan_ok(&Command::SetPrejoin(on), &old, &c), old);
        let text = describe_change(&Command::SetPrejoin(on), &old, &old, &c);
        assert!(text.starts_with('✅') && text.contains("pre-join") && !text.contains("<@"));
    }
    assert!(describe_change(&Command::SetPrejoin(true), &old, &old, &c).contains("bật"));
    assert!(describe_change(&Command::SetPrejoin(false), &old, &old, &c).contains("tắt"));
}

#[test]
fn test_parse_autoscale() {
    assert_eq!(pok("!relay autoscale on"), Command::SetAutoscale(true));
    assert_eq!(pok("!relay AUTOSCALE Off"), Command::SetAutoscale(false));
    for bad in [
        "!relay autoscale",
        "!relay autoscale yes",
        "!relay autoscale on off",
    ] {
        let e = perr(bad);
        assert!(
            e.0.contains("Cú pháp") && e.0.contains("`autoscale <on|off>`"),
            "{bad}"
        );
    }
    let line = HELP
        .lines()
        .find(|x| x.starts_with("• `autoscale"))
        .unwrap();
    assert!(
        line.contains("`autoscale <on|off>`")
            && !line.contains("công khai")
            && line.contains("operator")
    );
    assert_eq!(command_name(&Command::SetAutoscale(true)), "autoscale");
    assert_eq!(command_name(&Command::SetAutoscale(false)), "autoscale");
}

#[test]
fn test_autoscale_plan_and_reply() {
    let c = ctx();
    let old = routing(&[(200, O1)]);
    for on in [true, false] {
        assert_eq!(plan_ok(&Command::SetAutoscale(on), &old, &c), old);
        let text = describe_change(&Command::SetAutoscale(on), &old, &old, &c);
        assert!(text.starts_with('✅') && text.contains("autoscale") && !text.contains("<@"));
    }
    assert!(describe_change(&Command::SetAutoscale(true), &old, &old, &c).contains("bật"));
    assert!(describe_change(&Command::SetAutoscale(false), &old, &old, &c).contains("tắt"));
}

#[test]
fn test_status_shows_autoscale() {
    let on = StatusDetails {
        autoscale: true,
        ..details()
    };
    let off = StatusDetails {
        autoscale: false,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &on).contains("autoscale bật"));
    assert!(fmt(&routing(&[]), &ctx(), &off).contains("autoscale tắt"));
}

#[test]
fn test_status_shows_prejoin() {
    let on = StatusDetails {
        prejoin: true,
        ..details()
    };
    let off = StatusDetails {
        prejoin: false,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &on).contains("prejoin bật"));
    assert!(fmt(&routing(&[]), &ctx(), &off).contains("prejoin tắt"));
}

#[test]
fn test_display_names_neutralise_markdown_and_format_chars() {
    let c = ctx_names(&[
        (555, "[x](https://evil.example)"),
        (O1, "\u{202e}abc"),
        (O2, "@everyone"),
        (O3, "<@1>"),
        (999, "\u{2066}\u{202e}\u{2069}\x00*_~|\\`[]()"),
    ]);
    let old = routing(&[(200, O1), (300, O2), (400, O3)]);
    let d = StatusDetails {
        output_size: "1920x1080".to_string(), // no "@60": the test bans every "@"
        ..details()
    };
    let s = fmt(&old, &c, &d);
    for bad in ["[", "]", "(https", "\u{202e}", "@", "<@"] {
        assert!(!s.contains(bad), "{bad}");
    }
    assert!(!s.contains("://") && !s.contains("<@"));
    assert!(s.contains("**abc**") && s.contains("**everyone**") && s.contains("**1**"));
    let new = plan_ok(&Command::SetTarget(999), &old, &c);
    let out = describe_change(&Command::SetTarget(999), &old, &new, &c);
    assert!(out.contains("đổi target** thành 999.")); // nothing left of the name: ID only
}

#[test]
fn test_parse_id_edges_and_prefix() {
    for bad in ["!relay target 0", "!relay channel -5", "!relay mirror 0"] {
        perr(bad);
    }
    assert_eq!(pok("!RELAY status"), status());
    assert_eq!(pok("!Relay STATUS"), status());
    assert_eq!(p("!relayx status"), None);
    let msg = plan_err(&Command::SetChannel(999), &routing(&[]), &ctx());
    assert!(msg.starts_with("⚠️") && msg.contains("voice"));
}

#[test]
fn test_status_shows_invalid_state_routing() {
    let s = format_status(
        &routing(&[]),
        &ctx(),
        &details(),
        Some("target is a relay account"),
    );
    assert!(s.contains("⚠️") && s.contains("state.json"));
    assert!(!fmt(&routing(&[]), &ctx(), &details()).contains("⚠️"));
}

#[test]
fn test_mirror_with_explicit_account() {
    let r = plan_ok(&add_mirror(200, Some(O1)), &routing(&[]), &ctx());
    assert_eq!(mirrors(&r), [(200, O1)]);
    let msg = plan_err(&add_mirror(300, Some(O1)), &r, &ctx());
    assert!(msg.starts_with("⚠️") && msg.contains("<#200>")); // already mirroring another channel
    let mut c = ctx();
    c.unavailable = set(&[O2]);
    let msg = plan_err(&add_mirror(300, Some(O2)), &r, &c);
    assert!(msg.starts_with("⚠️"));
}

#[test]
fn test_mirror_rejects_relay_account() {
    for uid in [R1, INGEST] {
        let msg = plan_err(&add_mirror(200, Some(uid)), &routing(&[]), &ctx());
        assert!(msg.starts_with("⚠️") && msg.contains("relay"));
    }
}

#[test]
fn test_mirror_rejects_account_fixed_to_other_channel() {
    let mut c = ctx();
    c.mirror_fixed = HashMap::from([(O3, 300)]);
    let msg = plan_err(&add_mirror(200, Some(O3)), &routing(&[]), &c);
    assert!(msg.starts_with("⚠️") && msg.contains("<#300>"));
    assert_eq!(
        mirrors(&plan_ok(&add_mirror(300, Some(O3)), &routing(&[]), &c)),
        [(300, O3)]
    );
    // without an account a fixed one is never borrowed by another channel
    assert_eq!(
        mirrors(&plan_ok(&add_mirror(200, None), &routing(&[]), &c)),
        [(200, O2)]
    );
}

#[test]
fn test_mirror_prefers_fixed_account_for_channel() {
    let mut c = ctx();
    c.mirror_fixed = HashMap::from([(O1, 200), (O3, 300)]);
    assert_eq!(
        mirrors(&plan_ok(&add_mirror(200, None), &routing(&[]), &c)),
        [(200, O1)]
    );
    // fixed account unusable: fall back to the last free unfixed one
    let mut c2 = ctx();
    c2.mirror_fixed = HashMap::from([(O1, 200)]);
    c2.unavailable = set(&[O1]);
    assert_eq!(
        mirrors(&plan_ok(&add_mirror(200, None), &routing(&[]), &c2)),
        [(200, O3)]
    );
    let mut c3 = ctx();
    c3.mirror_fixed = HashMap::from([(O2, 300), (O3, 300)]);
    let msg = plan_err(&add_mirror(400, None), &routing(&[(200, O1)]), &c3);
    assert!(msg.starts_with("⚠️"));
}

#[test]
fn test_mirror_rejects_unknown_account() {
    let msg = plan_err(&add_mirror(200, Some(999)), &routing(&[]), &ctx());
    assert!(msg.starts_with("⚠️") && msg.contains("999"));
}

#[test]
fn test_status_details_layout() {
    let c = ctx_names(&[(555, "<@1> Alice"), (R1, "Relay1"), (O1, "Mir1")]);
    let s = fmt(&routing(&[(200, O1)]), &c, &details());
    let lines: Vec<&str> = s.lines().collect();
    assert_eq!(lines[0], "# 📡 Relay · 🟢 Đang phát");
    assert_eq!(
        lines[1],
        "-# chạy 1 giờ 2 phút · watch 412 ms · 🔊 audio OK"
    );
    assert!(s.contains("> **1 Alice** `555`"));
    assert!(s.contains("> Kênh chính <#100> · 👥 **120** người"));
    assert!(s.contains("### 📶 Relay · 1/2 chạy"));
    // 49 + 50 * 1 running relay of 2
    assert!(s.contains("▰▰▰▰▰▰▰▰▰▰ sức chứa 120/99 (tối đa 149 khi bật đủ 2 relay)"));
    assert!(s.contains("> 🟢 **Relay1** · 👁 3"));
    assert!(s.contains("> ⚫ **32** · rảnh"));
    assert!(s.contains("> 🟡 <#200> ← **Mir1** · đang kết nối"));
    assert!(s.contains("> ⚫ **22** · rảnh"));
    assert_eq!(
        *lines.last().unwrap(),
        "-# ⚙️ prejoin bật · debounce 100 ms · autoscale bật · 1920×1080@60 passthrough · audio passthrough · lệnh công khai: không có · cooldown 0s/0s"
    );
    assert!(!s.contains("<@"));
}

#[test]
fn test_session_state_labels() {
    let cases = [
        ("relaying", "🟢 Đang phát"),
        ("connecting", "🟡 Đang kết nối"),
        ("joining", "🟡 Đang kết nối"),
        ("hold", "🟠 Tạm giữ"),
        ("backoff", "🟠 Đang thử lại"),
        ("idle", "🔵 Chờ Target phát"),
        ("disabled", "⚫ Đã tắt"),
        ("fatal", "🔴 Lỗi"),
        ("quarantine", "🔴 Lỗi"),
    ];
    for (state, label) in cases {
        let d = StatusDetails {
            session_state: state.to_string(),
            ..details()
        };
        assert_eq!(
            fmt(&routing(&[]), &ctx(), &d).lines().next().unwrap(),
            format!("# 📡 Relay · {label}")
        );
    }
    let d = StatusDetails {
        session_state: "fatal".to_string(),
        fatal_reason: Some("AUTH_REJECTED".to_string()),
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(s.lines().next().unwrap().contains("🔴 Lỗi (AUTH_REJECTED)"));
}

#[test]
fn test_subtext_only_when_relaying_and_unknown_watch_omitted() {
    let d = StatusDetails {
        last_watch_ms: None,
        audio_ok: false,
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert_eq!(
        s.lines().nth(1).unwrap(),
        "-# chạy 1 giờ 2 phút · 🔇 không có audio"
    );
    let d = StatusDetails {
        session_state: "idle".to_string(),
        session_uptime_s: None,
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(!s.split("### 🎯").next().unwrap().contains("-#"));
    let d = StatusDetails {
        session_uptime_s: Some(45.0),
        ..details()
    };
    assert!(
        fmt(&routing(&[]), &ctx(), &d)
            .lines()
            .nth(1)
            .unwrap()
            .starts_with("-# chạy 45 giây")
    );
    let d = StatusDetails {
        session_uptime_s: Some(720.0),
        ..details()
    };
    assert!(
        fmt(&routing(&[]), &ctx(), &d)
            .lines()
            .nth(1)
            .unwrap()
            .starts_with("-# chạy 12 phút")
    );
}

#[test]
fn test_source_block_ids() {
    let big: u64 = 5734000000009185;
    let r = Routing {
        target_user_id: big,
        ..routing(&[])
    };
    let s = fmt(&r, &ctx_names(&[(big, "NewbieLady")]), &details());
    assert!(s.contains("> **NewbieLady** `5734…9185`"));
    assert!(fmt(&r, &ctx(), &details()).contains("> `5734000000009185`"));
}

#[test]
fn test_capacity_bar_math_and_clamp() {
    let d = StatusDetails {
        viewers: 199,
        relay_running: 2,
        viewer_cap: 50,
        ingest_publishes: true,
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    // 49 + 50 + 2 * 50; every relay runs: no maximum
    assert!(s.contains("▰▰▰▰▰▰▰▰▰▰ sức chứa 199/199\n"));
    let d = StatusDetails {
        viewers: 100,
        relay_running: 1,
        viewer_cap: 50,
        ingest_publishes: true,
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(s.contains("▰▰▰▰▰▰▱▱▱▱ sức chứa 100/149 (tối đa 199 khi bật đủ 2 relay)"));
    let d = StatusDetails {
        viewers: 5000,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &d).contains("▰▰▰▰▰▰▰▰▰▰ sức chứa 5000/99"));
    let d = StatusDetails {
        viewers: 0,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &d).contains("▱▱▱▱▱▱▱▱▱▱ sức chứa 0/99"));
}

#[test]
fn test_status_shows_shortage_warning() {
    let d = StatusDetails {
        relay_wanted: 3,
        relay_running: 2,
        relay_available: 2,
        relay_short_by: 1,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &d).contains("> ⚠️ thiếu 1 relay"));
    assert!(!fmt(&routing(&[]), &ctx(), &details()).contains("thiếu"));
    let d = StatusDetails {
        autoscale: false,
        ..details()
    };
    assert!(fmt(&routing(&[]), &ctx(), &d).contains("Relay · 1/2 chạy · autoscale tắt"));
}

#[test]
fn test_status_relay_lines_show_viewers_kept_and_idle_stop() {
    let d = StatusDetails {
        relay_wanted: 0,
        relay_running: 4,
        relay_kept: 1,
        relay_kept_ids: vec![34],
        accounts: vec![
            acc(
                R1,
                "relay",
                Some(100),
                "live",
                Some(65.0),
                Some(0),
                Some(12.2),
            ),
            acc(32, "relay", Some(100), "live", Some(5.0), None, None),
            acc(33, "relay", Some(100), "live", Some(5.0), Some(7), None),
            acc(34, "relay", Some(100), "live", Some(5.0), Some(9), None),
        ],
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    let find = |needle: &str| s.lines().find(|x| x.contains(needle)).unwrap().to_string();
    let r1 = find("**31**");
    let r2 = find("**32**");
    let r3 = find("**33**");
    assert!(
        r1.contains("👁 0") && r1.contains("*tắt sau 13s nếu không ai xem*") && !r1.contains("giữ")
    );
    assert!(r2.contains("👁 ?") && !r2.contains("tắt sau") && !r2.contains("giữ"));
    let r4 = find("**34**");
    assert!(r3.contains("👁 7") && !r3.contains("giữ")); // only the pool's kept ids are tagged, not the least watched
    assert!(r4.contains("👁 9") && r4.contains("*giữ vì còn người xem*"));
}

#[test]
fn test_status_account_state_dots() {
    let d = StatusDetails {
        accounts: vec![
            acc(R1, "relay", Some(100), "joining", None, None, None),
            acc(32, "relay", Some(100), "backoff", None, None, None),
            acc(33, "relay", Some(100), "fatal", None, None, None),
            acc(34, "relay", Some(100), "quarantine", None, None, None),
        ],
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    for (dot, uid) in [("🟡", 31), ("🟠", 32), ("🔴", 33), ("🔴", 34)] {
        let prefix = format!("> {dot} **{uid}**");
        assert!(s.lines().any(|x| x.starts_with(&prefix)), "{uid}");
    }
}

#[test]
fn test_status_omits_empty_blocks() {
    let d = StatusDetails {
        accounts: vec![],
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(!s.contains("📶") && !s.contains("🪞") && s.contains("🎯"));
    let d = StatusDetails {
        accounts: vec![acc(
            R1,
            "relay",
            Some(100),
            "live",
            Some(1.0),
            Some(1),
            None,
        )],
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(s.contains("📶") && !s.contains("🪞"));
}

#[test]
fn test_status_mirror_block_lines_and_fixed() {
    let mut c = ctx_names(&[(O1, "Mir1")]);
    c.mirror_fixed = HashMap::from([(O2, 300)]);
    let d = StatusDetails {
        accounts: vec![
            acc(O1, "mirror", Some(200), "live", Some(3.0), Some(12), None),
            acc(O2, "mirror", None, "off", None, None, None),
        ],
        ..details()
    };
    let s = fmt(&routing(&[(200, O1)]), &c, &d);
    assert!(s.contains("> 🟢 <#200> ← **Mir1** · 👁 12"));
    assert!(s.contains("> ⚫ **22** · rảnh · cố định <#300>"));
}

#[test]
fn test_status_routing_error_under_header() {
    let s = format_status(&routing(&[]), &ctx(), &details(), Some("x"));
    let line = s.lines().nth(2).unwrap();
    assert!(line.starts_with("⚠️") && line.contains("state.json"));
}

fn is_more_line(line: &str, noun: &str) -> bool {
    line.strip_prefix("> +")
        .and_then(|r| r.strip_suffix(&format!(" {noun} khác")))
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

#[test]
fn test_status_stays_under_discord_limit() {
    let long = "N".repeat(80);
    let ids: Vec<u64> = (1000..1025).collect();
    let mut accounts: Vec<AccountStatus> = ids
        .iter()
        .map(|u| {
            acc(
                *u,
                "relay",
                Some(100),
                "live",
                Some(5.0),
                Some(3),
                Some(9.0),
            )
        })
        .collect();
    accounts.extend(
        ids.iter()
            .map(|u| acc(u + 100, "mirror", None, "off", None, None, None)),
    );
    let mut c = ctx();
    c.user_name = Box::new(move |_| Some(long.clone()));
    c.mirror_fixed = ids.iter().map(|u| (u + 100, 300)).collect();
    let d = StatusDetails {
        accounts,
        public_commands: ["status", "mirror", "unmirror", "on", "off"]
            .map(String::from)
            .to_vec(),
        relay_running: 25,
        command_cooldown_s: 5,
        change_cooldown_s: 3,
        ..details()
    };
    let s = fmt(&routing(&[]), &c, &d);
    assert!(s.chars().count() < 2000 && !s.contains("<@"));
    assert!(s.lines().any(|l| is_more_line(l, "relay")));
    assert!(s.lines().any(|l| is_more_line(l, "mirror")));
}

#[test]
fn test_parse_bare_mirror_forms() {
    assert_eq!(pok("!relay mirror"), bare_add());
    assert_eq!(pok("!relay unmirror"), bare_remove());
    assert_eq!(pok("!relay unmirror all"), remove_mirror(None));
    assert_ne!(pok("!relay unmirror all"), pok("!relay unmirror"));
    assert_eq!(pok("!relay mirror 5"), add_mirror(5, None));
    assert_eq!(pok("!relay unmirror 5"), remove_mirror(Some(5)));
}

#[test]
fn test_bare_mirror_requires_voice_channel() {
    for (cmd, word) in [(bare_add(), "mirror"), (bare_remove(), "unmirror")] {
        let r = resolve_here(&cmd, &routing(&[]), None).unwrap_err();
        assert!(
            r.starts_with("⚠️")
                && r.contains("không ở kênh voice")
                && r.contains(&format!("`{word}`"))
        );
    }
}

#[test]
fn test_bare_mirror_in_main_channel_rejected_by_planner() {
    let cmd = resolve_here(&bare_add(), &routing(&[]), Some(100)).unwrap();
    assert_eq!(cmd, add_mirror(100, None));
    assert!(plan_err(&cmd, &routing(&[]), &ctx()).contains("kênh chính"));
}

#[test]
fn test_bare_mirror_in_other_channel() {
    let cmd = resolve_here(&bare_add(), &routing(&[]), Some(200)).unwrap();
    assert_eq!(mirrors(&plan_ok(&cmd, &routing(&[]), &ctx())), [(200, O3)]);
    let mut fixed = ctx();
    fixed.mirror_fixed = HashMap::from([(O1, 200)]);
    assert_eq!(mirrors(&plan_ok(&cmd, &routing(&[]), &fixed)), [(200, O1)]);
    assert_eq!(
        resolve_here(&add_mirror_none_channel(22), &routing(&[]), Some(200)).unwrap(),
        add_mirror(200, Some(22))
    );
}

fn add_mirror_none_channel(account: u64) -> Command {
    Command::AddMirror {
        channel_id: None,
        account: Some(account),
    }
}

#[test]
fn test_bare_unmirror() {
    let r = routing(&[(200, O1)]);
    let cmd = resolve_here(&bare_remove(), &r, Some(200)).unwrap();
    assert_eq!(cmd, remove_mirror(Some(200)));
    assert!(plan_ok(&cmd, &r, &ctx()).mirrors.is_empty());
    let miss = resolve_here(&bare_remove(), &r, Some(300)).unwrap_err();
    assert!(miss.starts_with("⚠️") && miss.contains("<#300>") && miss.contains("chưa có mirror"));
    assert_eq!(
        resolve_here(&remove_mirror(None), &r, None).unwrap(),
        remove_mirror(None)
    );
}

#[test]
fn test_help_lists_bare_mirror_forms() {
    assert!(HELP.contains("• `mirror` - "));
    assert!(HELP.contains("• `unmirror` - "));
}

#[test]
fn test_help_marks_public_commands() {
    let h = help_text(&strs(&["status", "mirror"]));
    let lines: HashMap<&str, &str> = h
        .lines()
        .filter(|l| l.starts_with("• "))
        .map(|l| (l.split('`').nth(1).unwrap(), l))
        .collect();
    assert!(lines["status"].contains("công khai"));
    assert!(lines["mirror"].contains("công khai"));
    assert!(lines["mirror <channel_id> [account_id]"].contains("công khai"));
    for sub in [
        "on",
        "target <user_id>",
        "channel <channel_id>",
        "unmirror",
        "unmirror <channel_id|all>",
        "prejoin <on|off>",
        "autoscale <on|off>",
    ] {
        assert!(!lines[sub].contains("công khai"), "{sub}");
    }
    assert_eq!(*HELP, help_text(&strs(&["status", "mirror", "unmirror"])));
    assert_eq!(default_public(), strs(&["status", "mirror", "unmirror"]));
    let unmirror: Vec<String> = help_text(&strs(&["unmirror"]))
        .lines()
        .filter(|x| x.starts_with("• `unmirror"))
        .map(String::from)
        .collect();
    assert!(unmirror.len() == 2 && unmirror.iter().all(|x| x.contains("công khai")));
    assert_eq!(
        parse("!relay", P, &strs(&["on"])),
        Some(Err(ParseError(help_text(&strs(&["on"])))))
    );
    assert_eq!(perr("!relay wat"), ParseError(HELP.clone()));
}

#[test]
fn test_command_name() {
    let cases = [
        (status(), "status"),
        (Command::SetEnabled(true), "on"),
        (Command::SetEnabled(false), "off"),
        (Command::SetTarget(1), "target"),
        (Command::SetChannel(1), "channel"),
        (bare_add(), "mirror"),
        (bare_remove(), "unmirror"),
        (remove_mirror(None), "unmirror"),
        (Command::SetPrejoin(true), "prejoin"),
        (Command::SetAutoscale(false), "autoscale"),
    ];
    for (cmd, name) in cases {
        assert_eq!(command_name(&cmd), name);
    }
}

#[test]
fn test_status_shows_public_commands_and_cooldowns() {
    let d = StatusDetails {
        public_commands: vec!["status".to_string(), "mirror".to_string()],
        command_cooldown_s: 5,
        change_cooldown_s: 3,
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    let line = s.lines().last().unwrap();
    assert!(line.contains("lệnh công khai: status, mirror") && line.contains("cooldown 5s/3s"));
    assert!(fmt(&routing(&[]), &ctx(), &details()).contains("công khai: không có"));
}

#[test]
fn test_truncated_name_never_ends_in_space() {
    let mut c = ctx();
    let long = format!("{} Bcdef", "A".repeat(31));
    c.user_name = Box::new(move |u| (u == R1).then(|| long.clone()));
    let s = fmt(&routing(&[]), &c, &details());
    assert!(s.contains(&format!("**{}**", "A".repeat(31))));
}

// ------------------------------------------------------------------ op

#[test]
fn test_parse_op_commands() {
    assert_eq!(pok("!relay op list"), Command::OpList);
    assert_eq!(pok("!relay OP LIST"), Command::OpList);
    assert_eq!(
        pok("!relay op add 123456789012345678"),
        Command::OpAdd(123456789012345678)
    );
    assert_eq!(pok("!relay op remove 42"), Command::OpRemove(42));
    assert_eq!(command_name(&Command::OpList), "op");
    assert_eq!(command_name(&Command::OpAdd(1)), "op");
    assert_eq!(command_name(&Command::OpRemove(1)), "op");
    for bad in [
        "!relay op",
        "!relay op list x",
        "!relay op add",
        "!relay op add 1 2",
        "!relay op remove",
        "!relay op bogus 1",
        "!relay op add <@123>",
        "!relay op add <@!123>",
        "!relay op add 0",
        "!relay op add -5",
        "!relay op add 1e5",
        "!relay op remove abc",
        "!relay op add ١٢٣",
    ] {
        let r = perr(bad);
        assert!(r.0.starts_with('❌') && r.0.contains("op"), "{bad}");
        assert!(!r.0.contains("<@"));
    }
}

#[test]
fn test_help_lists_op_commands_owner_only() {
    let lines: Vec<&str> = HELP.lines().filter(|l| l.contains("`op ")).collect();
    assert!(lines.iter().any(|l| l.contains("op list")));
    assert!(
        lines
            .iter()
            .any(|l| l.contains("op add") && l.contains("op remove") && l.contains("chủ sở hữu"))
    );
    assert!(lines.iter().all(|l| !l.contains("công khai")));
}

fn ops() -> OperatorContext {
    OperatorContext {
        config_user_ids: set(&[1]),
        role_ids: set(&[500]),
        added: set(&[2]),
        target_user_id: 9,
    }
}

#[test]
fn test_plan_op_add_and_remove() {
    let c = ctx_names(&[(3, "Alice <@1>")]);
    assert_eq!(
        plan_op(&Command::OpAdd(3), &ops(), &c).unwrap(),
        set(&[2, 3])
    );
    // the target may be an operator
    assert_eq!(
        plan_op(&Command::OpAdd(9), &ops(), &c).unwrap(),
        set(&[2, 9])
    );
    assert_eq!(
        plan_op(&Command::OpRemove(2), &ops(), &c).unwrap(),
        set(&[])
    );
    let added = describe_op(&Command::OpAdd(3), &c);
    assert!(
        added.starts_with('✅')
            && added.contains("Alice")
            && added.contains('3')
            && !added.contains("<@")
    );
    let removed = describe_op(&Command::OpRemove(2), &c);
    assert!(removed.starts_with('✅') && removed.contains('2'));
}

#[test]
fn test_plan_op_rejections() {
    let mut c = ctx();
    c.is_bot = Box::new(|u| u == 77);
    for (cmd, why) in [
        (Command::OpAdd(1), "config"), // config-defined owner already an operator
        (Command::OpAdd(2), "đã là operator"),
        (Command::OpAdd(R1), "bot"), // relay account
        (Command::OpAdd(INGEST), "bot"),
        (Command::OpAdd(77), "bot"), // any bot account
        (
            Command::OpRemove(1),
            "được khai báo trong config, sửa config để gỡ",
        ),
        (Command::OpRemove(5), "không phải operator"),
    ] {
        let r = plan_op(&cmd, &ops(), &c).unwrap_err();
        assert!(
            r.starts_with("⚠️") && r.contains(why) && !r.contains("<@"),
            "{cmd:?} {r}"
        );
    }
}

#[test]
fn test_format_operators_lists_config_chat_and_roles_without_mentions() {
    let mut c = ctx_names(&[(1, "Owner`*"), (2, "Helper <@&5>")]);
    c.role_name = names(&[(500, "Mods @everyone")]);
    let big: u64 = 123456789012345678;
    let o = OperatorContext {
        added: set(&[2, big]),
        ..ops()
    };
    let text = format_operators(&o, &c);
    assert!(text.starts_with('👥') && !text.contains("<@") && !text.contains("@everyone"));
    assert!(
        text.contains("config")
            && text.contains("Owner")
            && text.contains("Helper")
            && text.contains("Mods")
            && text.contains("500")
    );
    assert!(text.contains("123456789012345678")); // unknown name: full id
    let o2 = OperatorContext {
        added: set(&[big]),
        ..ops()
    };
    let mut bob = ctx();
    bob.user_name = Box::new(|_| Some("Bob".to_string()));
    assert!(format_operators(&o2, &bob).contains("`123456789012345678`")); // full id with a name
    assert!(!text.contains("`*"));
    let o3 = OperatorContext {
        config_user_ids: set(&[]),
        role_ids: set(&[]),
        added: set(&[]),
        ..ops()
    };
    let empty = format_operators(&o3, &ctx());
    assert!(empty.starts_with('👥') && empty.contains("không có"));
    let o4 = OperatorContext {
        added: (10u64.pow(17)..10u64.pow(17) + 300).collect(),
        ..ops()
    };
    assert!(format_operators(&o4, &ctx()).chars().count() < 2000);
}

#[test]
fn test_status_single_codec_line() {
    let c = ctx();
    let d = StatusDetails {
        codec: Some("H265".to_string()),
        ..details()
    };
    let s = fmt(&routing(&[(200, O1)]), &c, &d);
    assert!(s.matches("Codec:").count() == 1 && s.contains("> Codec: H.265"));
    assert!(!fmt(&routing(&[(200, O1)]), &c, &details()).contains("Codec:"));
}

#[test]
fn test_status_switch_arrow() {
    let d = StatusDetails {
        codec: Some("H265".to_string()),
        codec_switch: Some(("H265".to_string(), "H264".to_string())),
        ..details()
    };
    let s = fmt(&routing(&[(200, O1)]), &ctx(), &d);
    assert!(s.contains("> Codec: H.265 → H.264 (đang chuyển)"));
}

#[test]
fn test_parse_status_verbose() {
    assert_eq!(pok("!relay status -v"), Command::Status { verbose: true });
    assert_eq!(
        pok("!relay STATUS Verbose"),
        Command::Status { verbose: true }
    );
    perr("!relay status -x");
    perr("!relay status -v extra");
    assert!(HELP.contains("status -v"));
}

fn tech_row(uid: Option<u64>, rate: Option<(f64, f64)>, recent: u32, session: u32) -> TechRow {
    TechRow {
        uid,
        rate,
        freezes_recent: recent,
        freezes_session: session,
    }
}

#[test]
fn test_status_verbose_section() {
    let tech = vec![
        tech_row(None, Some((1300.0, 60.0)), 0, 2),
        tech_row(Some(R1), Some((800.0, 30.0)), 1, 1),
        tech_row(Some(O1), None, 0, 0),
    ];
    let d = StatusDetails {
        tech: Some(tech),
        gap_frames: Some(3),
        ..details()
    };
    let s = fmt(&routing(&[(200, O1)]), &ctx(), &d);
    assert!(s.contains("### 📊 Kỹ thuật"));
    assert!(
        s.contains("> **Ingest** · 1,3 Mbps · 60 fps · đứng hình 0 lần/5 phút (2 lần cả phiên)")
    );
    assert!(s.contains("· 800 kbps · 30 fps · đứng hình 1 lần/5 phút (1 lần cả phiên)"));
    assert!(s.contains("không phát"));
    assert!(s.contains("> Nguồn: mất 3 frame cả phiên"));
    assert!(!fmt(&routing(&[]), &ctx(), &details()).contains("Kỹ thuật"));
}

#[test]
fn test_status_verbose_survives_trimming() {
    let mut tech = vec![tech_row(None, Some((1300.0, 60.0)), 0, 0)];
    tech.extend((0..60).map(|i| tech_row(Some(1000 + i), Some((5000.0, 60.0)), 0, 0)));
    let accounts: Vec<AccountStatus> = (0..60)
        .map(|i| {
            acc(
                1000 + i,
                "relay",
                Some(100),
                "live",
                Some(5.0),
                Some(1),
                None,
            )
        })
        .collect();
    let d = StatusDetails {
        tech: Some(tech),
        accounts,
        gap_frames: Some(1),
        ..details()
    };
    let s = fmt(&routing(&[]), &ctx(), &d);
    assert!(s.chars().count() < 2000);
    assert!(s.contains("### 📊 Kỹ thuật") && s.contains("> **Ingest** · 1,3 Mbps"));
}
