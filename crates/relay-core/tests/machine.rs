//! Port of `tests/supervisor/test_machine.py`: one scenario per acceptance code (spec §20).

use std::collections::HashSet;

use indexmap::IndexMap;
use relay_core::supervisor::machine::{PersistedStateView, Supervisor, SupervisorConfig};
use relay_core::supervisor::model::{
    Action, Event, FatalReason, GatewayState, HoldReason, KeyframeMethod, QuarantineRecord,
    RelayState, RelayVoiceObserved, SourceStream, StopReason, TargetVoice,
};
use relay_core::supervisor::routing::Routing;

const SRC: u64 = 1;
const CH: u64 = 10;
const OTHER: u64 = 11;
const IN: TargetVoice = TargetVoice {
    channel_id: Some(CH),
    self_stream: true,
};
const NOT_SHARING: TargetVoice = TargetVoice {
    channel_id: Some(CH),
    self_stream: false,
};

fn tv(channel: u64, self_stream: bool) -> TargetVoice {
    TargetVoice {
        channel_id: Some(channel),
        self_stream,
    }
}

fn stream_with(inc: i64, key: &str, f: impl FnOnce(&mut SourceStream)) -> SourceStream {
    let mut s = SourceStream {
        key: key.to_string(),
        owner_id: SRC,
        channel_id: CH,
        incarnation: inc,
        paused: None,
        unavailable: false,
    };
    f(&mut s);
    s
}

fn stream() -> SourceStream {
    stream_with(1, "k", |_| {})
}

fn stream_inc(inc: i64) -> SourceStream {
    stream_with(inc, "k", |_| {})
}

fn ids(v: &[u64]) -> HashSet<u64> {
    v.iter().copied().collect()
}

fn make() -> Supervisor {
    make_cfg(|_| {})
}

fn make_cfg(f: impl FnOnce(&mut SupervisorConfig)) -> Supervisor {
    make_full(true, None, None, 0, f)
}

fn make_state(
    enabled: bool,
    fatal: Option<FatalReason>,
    quarantine: Option<QuarantineRecord>,
    rej: u32,
) -> Supervisor {
    make_full(enabled, fatal, quarantine, rej, |_| {})
}

fn make_full(
    enabled: bool,
    fatal: Option<FatalReason>,
    quarantine: Option<QuarantineRecord>,
    rej: u32,
    f: impl FnOnce(&mut SupervisorConfig),
) -> Supervisor {
    let mut cfg = SupervisorConfig::new(SRC, 1, CH, 5.0);
    cfg.retry_jitter = 0.0;
    f(&mut cfg);
    Supervisor::new(
        cfg,
        PersistedStateView {
            desired_enabled: enabled,
            fatal_reason: fatal,
            quarantine,
            consecutive_rejections: rej,
        },
        IndexMap::new(),
    )
}

fn quarantine_rec(inc: i64) -> QuarantineRecord {
    QuarantineRecord {
        owner_id: SRC,
        stream_key: "k".to_string(),
        incarnation: inc,
        since: 0.0,
    }
}

fn snap_full(
    sup: &mut Supervisor,
    now: f64,
    target: Option<TargetVoice>,
    st: Option<SourceStream>,
    participants: Option<HashSet<u64>>,
    new_session: bool,
) -> Vec<Action> {
    sup.handle(
        Event::GatewaySnapshot {
            target,
            stream: st,
            participants,
            new_session,
        },
        now,
    )
}

fn snap(sup: &mut Supervisor, now: f64) -> Vec<Action> {
    snap_full(sup, now, Some(IN), Some(stream()), Some(ids(&[SRC])), false)
}

fn snap_st(sup: &mut Supervisor, now: f64, st: SourceStream, new_session: bool) -> Vec<Action> {
    snap_full(sup, now, Some(IN), Some(st), Some(ids(&[SRC])), new_session)
}

macro_rules! has {
    ($acts:expr, $($pat:tt)+) => {
        $acts.iter().any(|a| matches!(a, $($pat)+))
    };
}

/// (generation, epoch, stream) of the first `StartConnect`.
fn find_start(acts: &[Action]) -> (u64, u64, SourceStream) {
    acts.iter()
        .find_map(|a| match a {
            Action::StartConnect {
                generation,
                epoch,
                stream,
            } => Some((*generation, *epoch, stream.clone())),
            _ => None,
        })
        .expect("StartConnect")
}

/// (generation, leave_voice, reason) of the first `Teardown`.
fn find_teardown(acts: &[Action]) -> (u64, bool, StopReason) {
    acts.iter()
        .find_map(|a| match a {
            Action::Teardown {
                generation,
                leave_voice,
                reason,
            } => Some((*generation, *leave_voice, *reason)),
            _ => None,
        })
        .expect("Teardown")
}

fn find_set_quarantine(acts: &[Action]) -> Option<QuarantineRecord> {
    acts.iter()
        .find_map(|a| match a {
            Action::SetQuarantine { record } => Some(record.clone()),
            _ => None,
        })
        .expect("SetQuarantine")
}

fn find_set_rejections(acts: &[Action]) -> u32 {
    acts.iter()
        .find_map(|a| match a {
            Action::SetRejections { count } => Some(*count),
            _ => None,
        })
        .expect("SetRejections")
}

fn has_close_gate_first(acts: &[Action]) -> bool {
    matches!(acts.first(), Some(Action::CloseGate { .. }))
}

fn has_start(acts: &[Action]) -> bool {
    has!(acts, Action::StartConnect { .. })
}

fn has_leave(acts: &[Action]) -> bool {
    has!(acts, Action::LeaveVoice { .. })
}

fn has_join(acts: &[Action]) -> bool {
    has!(acts, Action::JoinVoice)
}

fn has_teardown(acts: &[Action]) -> bool {
    has!(acts, Action::Teardown { .. })
}

fn has_teardown_reason(acts: &[Action], reason: StopReason) -> bool {
    has!(acts, Action::Teardown { reason: r, .. } if *r == reason)
}

fn has_open_gate(acts: &[Action]) -> bool {
    has!(acts, Action::OpenGate { .. })
}

fn has_open_gate_for(acts: &[Action], g: u64, e: u64) -> bool {
    has!(acts, Action::OpenGate { generation, epoch } if *generation == g && *epoch == e)
}

fn has_set_quarantine_none(acts: &[Action]) -> bool {
    has!(acts, Action::SetQuarantine { record: None })
}

fn has_fatal_latch(acts: &[Action], reason: FatalReason) -> bool {
    has!(acts, Action::SetFatalLatch { reason: r, .. } if *r == reason)
}

fn has_slate(acts: &[Action], reason: HoldReason) -> bool {
    has!(acts, Action::ShowSlate { reason: r, .. } if *r == reason)
}

fn has_keyframe(acts: &[Action], method: KeyframeMethod) -> bool {
    has!(acts, Action::RequestInputKeyframe { method: m, .. } if *m == method)
}

fn has_rebuild_epoch(acts: &[Action], epoch: u64) -> bool {
    has!(acts, Action::RebuildPipeline { epoch: e, .. } if *e == epoch)
}

fn tick(sup: &mut Supervisor, now: f64) -> Vec<Action> {
    sup.handle(Event::Tick, now)
}

fn joined(g: u64, session: &str) -> Event {
    Event::Joined {
        generation: g,
        voice_session_id: Some(session.to_string()),
    }
}

fn watching(g: u64) -> Event {
    Event::Watching { generation: g }
}

fn progress(g: u64, at: f64) -> Event {
    Event::SourceProgress {
        generation: g,
        at,
        video: true,
    }
}

fn first_frame(g: u64, e: u64, w: u32, h: u32) -> Event {
    Event::FirstFrame {
        generation: g,
        epoch: e,
        width: w,
        height: h,
    }
}

fn fresh_frame(g: u64, e: u64) -> Event {
    Event::FreshFrame {
        generation: g,
        epoch: e,
    }
}

fn output_ready(g: u64, codec: &str) -> Event {
    Event::OutputReady {
        generation: g,
        negotiated_video_codec: Some(codec.to_string()),
    }
}

fn codec_changed(g: u64, codec: &str) -> Event {
    Event::CodecChanged {
        generation: g,
        negotiated_video_codec: Some(codec.to_string()),
    }
}

fn worker_failed(g: u64, e: u64) -> Event {
    Event::WorkerFailed {
        generation: g,
        epoch: e,
        detail: String::new(),
    }
}

fn connect_failed(g: u64, reason: StopReason, retryable: bool) -> Event {
    connect_failed_detail(g, reason, retryable, "", false)
}

fn connect_failed_detail(
    g: u64,
    reason: StopReason,
    retryable: bool,
    detail: &str,
    watch_retry: bool,
) -> Event {
    Event::ConnectFailed {
        generation: g,
        reason,
        retryable,
        detail: detail.to_string(),
        watch_retry,
    }
}

fn output_health(g: u64, ok: bool) -> Event {
    Event::OutputHealth { generation: g, ok }
}

fn inbound_health(g: u64, ok: bool) -> Event {
    Event::InboundHealth { generation: g, ok }
}

fn relay_voice(session: Option<&str>, channel: Option<u64>) -> Event {
    Event::RelayVoiceObserved(RelayVoiceObserved {
        session_id: session.map(str::to_string),
        channel_id: channel,
    })
}

fn gw(state: GatewayState) -> Event {
    Event::GatewayChanged { state }
}

fn target_changed(t: Option<TargetVoice>) -> Event {
    Event::TargetVoiceChanged { target: t }
}

fn stream_ended(key: &str, inc: i64) -> Event {
    Event::StreamEnded {
        key: key.to_string(),
        incarnation: inc,
    }
}

fn participants(p: Option<&[u64]>) -> Event {
    Event::ParticipantsChanged {
        participants: p.map(ids),
    }
}

fn routing(target: u64, channel: u64, mirrors: &[(u64, u64)]) -> Routing {
    Routing {
        target_user_id: target,
        channel_id: channel,
        mirrors: mirrors.iter().copied().collect(),
    }
}

fn routing_changed(r: Routing) -> Event {
    Event::RoutingChanged { routing: r }
}

fn enable(enabled: bool) -> Event {
    Event::EnableChanged { enabled }
}

fn prejoin_changed(on: bool) -> Event {
    Event::PrejoinChanged { on }
}

fn prejoin_failed(detail: &str) -> Event {
    Event::PrejoinFailed {
        detail: detail.to_string(),
    }
}

fn voice_lost(detail: &str, foreign_session: bool, from_prejoin: bool) -> Event {
    Event::VoiceLost {
        detail: detail.to_string(),
        foreign_session,
        from_prejoin,
    }
}

/// Snapshot at `t` then Tick after debounce; returns (generation, epoch).
fn start(sup: &mut Supervisor, t: f64) -> (u64, u64) {
    start_st(sup, t, None, false)
}

fn start_st(
    sup: &mut Supervisor,
    t: f64,
    st: Option<SourceStream>,
    new_session: bool,
) -> (u64, u64) {
    snap_st(sup, t, st.unwrap_or_else(stream), new_session);
    let acts = tick(sup, t + 0.5);
    let (g, e, _) = find_start(&acts);
    (g, e)
}

fn relaying(sup: &mut Supervisor, t: f64) -> (u64, u64) {
    relaying_st(sup, t, None)
}

fn relaying_st(sup: &mut Supervisor, t: f64, st: Option<SourceStream>) -> (u64, u64) {
    let (g, e) = start_st(sup, t, st, false);
    sup.handle(joined(g, "s1"), t + 0.5);
    sup.handle(watching(g), t + 0.5);
    sup.handle(progress(g, t + 0.75), t + 0.75);
    let acts = sup.handle(first_frame(g, e, 1280, 720), t + 0.75);
    assert!(has!(&acts, Action::CreateOutput { generation } if *generation == g));
    let acts = sup.handle(output_ready(g, "H264"), t + 1.0);
    assert!(has_open_gate_for(&acts, g, e));
    assert_eq!(sup.status(t + 1.0).state, RelayState::Relaying);
    (g, e)
}

fn approx(a: Option<f64>, b: f64) -> bool {
    a.is_some_and(|a| (a - b).abs() < 1e-9)
}

// ---------------------------------------------------------------- start

#[test]
fn test_a01_start_after_debounce() {
    let mut sup = make();
    assert_eq!(snap(&mut sup, 0.0), vec![]);
    assert_eq!(tick(&mut sup, 0.4), vec![]);
    let acts = tick(&mut sup, 0.5);
    let (_, epoch, st) = find_start(&acts);
    assert!(st == stream() && epoch >= 1);
    assert_eq!(sup.status(0.5).state, RelayState::Connecting);
}

#[test]
fn test_debounce_resets_when_source_vanishes() {
    let mut sup = make();
    snap(&mut sup, 0.0);
    sup.handle(target_changed(Some(tv(CH, false))), 0.3);
    sup.handle(stream_ended("k", 1), 0.3);
    assert_eq!(tick(&mut sup, 0.6), vec![]);
    snap(&mut sup, 1.0);
    assert_eq!(tick(&mut sup, 1.4), vec![]);
    assert!(has_start(&tick(&mut sup, 1.5)));
}

#[test]
fn test_a02_no_start_when_not_sharing() {
    let mut sup = make();
    snap_full(
        &mut sup,
        0.0,
        Some(tv(CH, false)),
        None,
        Some(ids(&[SRC])),
        true,
    );
    for t in [1.0, 10.0, 100.0] {
        assert_eq!(tick(&mut sup, t), vec![]);
    }
}

#[test]
fn test_a03_other_channel_no_start() {
    let mut sup = make();
    snap_full(
        &mut sup,
        0.0,
        Some(tv(OTHER, true)),
        Some(stream_with(1, "k", |s| s.channel_id = OTHER)),
        Some(ids(&[SRC])),
        false,
    );
    assert_eq!(tick(&mut sup, 5.0), vec![]);
}

#[test]
fn test_a04_other_user_stream_ignored() {
    let mut sup = make();
    snap_full(
        &mut sup,
        0.0,
        Some(tv(CH, false)),
        None,
        Some(ids(&[SRC, 99])),
        true,
    );
    sup.handle(
        Event::StreamStarted {
            stream: stream_with(1, "other", |s| s.owner_id = 99),
        },
        0.1,
    );
    assert_eq!(tick(&mut sup, 5.0), vec![]);
}

#[test]
fn test_no_start_without_fresh_gateway() {
    let mut sup = make();
    sup.handle(gw(GatewayState::Fresh), 0.0);
    sup.handle(target_changed(Some(IN)), 0.0);
    sup.handle(Event::StreamStarted { stream: stream() }, 0.0);
    assert!(has_start(&tick(&mut sup, 0.5)));
    let mut sup2 = make();
    sup2.handle(target_changed(Some(IN)), 0.0);
    sup2.handle(Event::StreamStarted { stream: stream() }, 0.0);
    assert_eq!(tick(&mut sup2, 5.0), vec![]);
}

#[test]
fn test_a08_disable_during_connecting_never_opens() {
    let mut sup = make();
    let (g, e) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    sup.handle(watching(g), 0.7);
    let acts = sup.handle(enable(false), 0.8);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: true, reason: StopReason::Disabled } if *generation == g
    ));
    assert_eq!(sup.status(0.8).state, RelayState::Disabled);
    let late = [
        sup.handle(progress(g, 0.9), 0.9),
        sup.handle(first_frame(g, e, 1, 1), 0.9),
        sup.handle(output_ready(g, "H264"), 1.0),
    ];
    assert!(late.iter().all(Vec::is_empty));
    assert!(!sup.gate_allows(g, e, 1.0));
    assert_eq!(tick(&mut sup, 10.0), vec![]);
}

#[test]
fn test_connect_timeouts() {
    let mut sup = make();
    start(&mut sup, 0.0);
    let acts = tick(&mut sup, 0.5 + 15.0);
    assert!(has_teardown_reason(&acts, StopReason::ConnectFailed));
    assert_eq!(sup.status(16.0).state, RelayState::Backoff);
    let mut sup = make();
    let (g, _) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.5);
    sup.handle(watching(g), 0.5);
    assert_eq!(tick(&mut sup, 10.0), vec![]);
    assert!(has_teardown_reason(
        &tick(&mut sup, 10.5),
        StopReason::ConnectFailed
    ));
}

#[test]
fn test_connecting_output_never_ready_times_out() {
    let mut sup = make();
    let (g, e) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.5);
    sup.handle(watching(g), 0.5);
    sup.handle(progress(g, 0.75), 0.75);
    assert!(has!(
        &sup.handle(first_frame(g, e, 1, 1), 0.75),
        Action::CreateOutput { .. }
    ));
    assert_eq!(tick(&mut sup, 15.0), vec![]);
    assert!(has_teardown_reason(
        &tick(&mut sup, 15.5),
        StopReason::ConnectFailed
    ));
}

#[test]
fn test_connecting_source_frozen_after_first_frame_times_out() {
    let mut sup = make();
    let (g, e) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.5);
    sup.handle(watching(g), 0.5);
    sup.handle(progress(g, 0.75), 0.75);
    sup.handle(first_frame(g, e, 1, 1), 0.75);
    // liveness already DEAD (t_dead=5), gate stays closed
    sup.handle(output_ready(g, "H264"), 6.0);
    assert_eq!(tick(&mut sup, 14.0), vec![]);
    assert!(has_teardown_reason(
        &tick(&mut sup, 15.5),
        StopReason::ConnectFailed
    ));
}

#[test]
fn test_codec_mismatch_never_opens() {
    let mut sup = make();
    let (g, e) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    sup.handle(watching(g), 0.7);
    sup.handle(progress(g, 0.8), 0.8);
    sup.handle(first_frame(g, e, 1, 1), 0.9);
    let acts = sup.handle(output_ready(g, "VP8"), 1.0);
    assert!(!has_open_gate(&acts));
    assert!(has_teardown_reason(&acts, StopReason::CodecMismatch));
    assert!(has_close_gate_first(&acts));
}

#[test]
fn test_codec_changed_while_relaying_stops() {
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(codec_changed(g, "VP9"), 2.0);
    assert!(has_teardown_reason(&acts, StopReason::CodecMismatch));
}

// ----------------------------------------------------------- stop paths

#[test]
fn test_a09_target_moves_while_relaying() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(target_changed(Some(tv(OTHER, true))), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: true, reason: StopReason::SourceLeftWhitelist } if *generation == g
    ));
    assert_eq!(sup.status(2.0).state, RelayState::Idle);
    assert!(!sup.gate_allows(g, e, 2.0));
}

#[test]
fn test_a09_target_moves_while_hold() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(gw(GatewayState::Resuming), 2.0);
    let acts = sup.handle(target_changed(Some(tv(OTHER, true))), 2.5);
    assert!(has_teardown_reason(&acts, StopReason::SourceLeftWhitelist));
}

#[test]
fn test_a10_key_reuse_new_incarnation() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(
        Event::StreamStarted {
            stream: stream_inc(2),
        },
        2.0,
    );
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown { generation, reason: StopReason::SourceStopped, .. } if *generation == g
    ));
    assert!(!sup.gate_allows(g, e, 2.0));
    let acts = tick(&mut sup, 2.5);
    let (sg, se, sst) = find_start(&acts);
    assert!(sst.incarnation == 2 && sg == g + 1 && se > e);
}

#[test]
fn test_a11_stale_generation_and_epoch_ignored() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    sup.handle(stream_ended("k", 1), 2.0);
    for ev in [
        first_frame(g, e, 1, 1),
        fresh_frame(g, e),
        output_ready(g, "H264"),
        joined(g, "x"),
        watching(g),
        progress(g, 2.1),
        worker_failed(g, e),
        connect_failed(g, StopReason::ConnectFailed, true),
    ] {
        assert_eq!(sup.handle(ev, 2.1), vec![]);
    }
    let (g2, e2) = start(&mut sup, 3.0);
    assert_eq!(sup.handle(worker_failed(g, e), 3.7), vec![]);
    assert_eq!(sup.handle(watching(g), 3.7), vec![]);
    assert!(g2 == g + 1 && e2 > e);
}

#[test]
fn test_a11_stale_epoch_frame_does_not_open() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    sup.handle(worker_failed(g, e), 2.0);
    assert_eq!(sup.status(2.0).epoch, e + 1);
    sup.handle(progress(g, 2.1), 2.1);
    assert!(!has_open_gate(&sup.handle(fresh_frame(g, e), 2.2)));
    assert!(has_open_gate_for(
        &sup.handle(fresh_frame(g, e + 1), 2.3),
        g,
        e + 1
    ));
}

#[test]
fn test_idle_grace_then_leave() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(stream_ended("k", 1), 2.0);
    assert!(has!(
        &acts,
        Action::Teardown {
            leave_voice: false,
            reason: StopReason::SourceStopped,
            ..
        }
    ));
    assert!(!has_leave(&tick(&mut sup, 31.9)));
    assert!(has_leave(&tick(&mut sup, 32.0)));
    assert_eq!(tick(&mut sup, 40.0), vec![]);
}

#[test]
fn test_idle_grace_source_leaves_voice_immediately() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(stream_ended("k", 1), 2.0);
    let acts = sup.handle(target_changed(None), 3.0);
    assert!(has_leave(&acts));
}

#[test]
fn test_idle_restart_within_grace_does_not_leave() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(stream_ended("k", 1), 2.0);
    sup.handle(
        Event::StreamStarted {
            stream: stream_inc(2),
        },
        3.0,
    );
    let acts = tick(&mut sup, 3.5);
    assert!(has_start(&acts));
    assert!(!has_leave(&acts));
}

// ------------------------------------------------------------ gateway

#[test]
fn test_a12_resume_holds_then_reopens_with_fresh_frame() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(gw(GatewayState::Resuming), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::ShowSlate { generation, reason: HoldReason::GatewayResume } if *generation == g
    ));
    let st = sup.status(2.0);
    assert!(st.state == RelayState::Hold && st.hold_deadline == Some(17.0));
    assert!(!sup.gate_allows(g, e, 2.0));
    sup.handle(gw(GatewayState::Fresh), 3.0);
    assert!(!sup.gate_allows(g, e, 3.0)); // frame since hold still required
    sup.handle(progress(g, 3.1), 3.1);
    let acts = sup.handle(fresh_frame(g, e), 3.2);
    assert!(has_open_gate_for(&acts, g, e));
    assert!(sup.gate_allows(g, e, 3.2));
}

#[test]
fn test_a13_replay_shows_source_stopped_no_reopen() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    sup.handle(gw(GatewayState::Resuming), 2.0);
    let acts = sup.handle(stream_ended("k", 1), 2.5);
    assert!(has_teardown_reason(&acts, StopReason::SourceStopped));
    sup.handle(gw(GatewayState::Fresh), 3.0);
    assert!(!has_open_gate(&sup.handle(fresh_frame(g, e), 3.1)));
    assert_eq!(sup.status(3.1).state, RelayState::Idle);
}

#[test]
fn test_hold_deadline_is_fixed_and_expires() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    sup.handle(gw(GatewayState::Resuming), 2.0);
    sup.handle(worker_failed(g, e), 10.0);
    assert_eq!(sup.status(10.0).hold_deadline, Some(17.0));
    let acts = tick(&mut sup, 17.0);
    assert!(has_teardown_reason(&acts, StopReason::HoldExpired));
    // gateway still not FRESH: source cannot be re-verified
    assert_eq!(sup.status(17.0).state, RelayState::Idle);
}

#[test]
fn test_invalid_gateway_stops_and_new_session_snapshot_stops() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(gw(GatewayState::Invalid), 2.0);
    assert!(has_teardown_reason(&acts, StopReason::SessionInvalid));
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = snap_st(&mut sup, 2.0, stream(), true);
    assert!(has_teardown_reason(&acts, StopReason::SessionInvalid));
}

// ------------------------------------------------------ disable / enable

#[test]
fn test_a20_disable_keeps_quarantine_and_never_publishes() {
    for where_ in ["hold", "backoff", "quarantine"] {
        let mut sup = make();
        let (g, _) = relaying(&mut sup, 0.0);
        match where_ {
            "hold" => {
                sup.handle(gw(GatewayState::Resuming), 2.0);
            }
            "backoff" => {
                sup.handle(connect_failed(g, StopReason::ConnectFailed, true), 2.0);
            }
            _ => {
                sup.handle(relay_voice(Some("s1"), None), 2.0);
                assert_eq!(sup.status(2.0).state, RelayState::Quarantine);
            }
        }
        let acts = sup.handle(enable(false), 3.0);
        assert!(!has!(&acts, Action::SetQuarantine { .. }));
        assert_eq!(sup.status(3.0).state, RelayState::Disabled);
        assert_eq!(sup.status(3.0).quarantine.is_some(), where_ == "quarantine");
        assert!(
            !tick(&mut sup, 500.0)
                .iter()
                .any(|a| matches!(a, Action::StartConnect { .. } | Action::OpenGate { .. }))
        );
    }
}

#[test]
fn test_a21_restart_after_disable_stays_disabled() {
    let mut sup = make_state(false, None, None, 0);
    assert_eq!(sup.status(0.0).state, RelayState::Disabled);
    snap(&mut sup, 0.0);
    assert_eq!(tick(&mut sup, 10.0), vec![]);
}

#[test]
fn test_a45_enable_order() {
    let cases = [
        (
            Some(FatalReason::AuthRejected),
            true,
            true,
            RelayState::Fatal,
        ),
        (
            Some(FatalReason::AuthRejected),
            false,
            false,
            RelayState::Fatal,
        ),
        (None, false, true, RelayState::Disabled),
        (None, true, true, RelayState::Quarantine),
        (None, true, false, RelayState::Idle),
    ];
    for (fatal, enabled, quarantined, expected) in cases {
        let q = quarantined.then(|| quarantine_rec(1));
        let mut sup = make_state(enabled, fatal, q, 0);
        assert_eq!(sup.status(0.0).state, expected);
        if expected == RelayState::Disabled {
            sup.handle(enable(true), 1.0);
            assert_eq!(sup.status(1.0).state, RelayState::Quarantine);
        }
    }
}

#[test]
fn test_a34_enable_with_latch_keeps_fatal() {
    let mut sup = make_state(false, Some(FatalReason::AuthRejected), None, 0);
    sup.handle(enable(true), 1.0);
    assert_eq!(sup.status(1.0).state, RelayState::Fatal);
    assert_eq!(sup.status(1.0).fatal.as_deref(), Some("AUTH_REJECTED"));
}

#[test]
fn test_enable_from_disabled_goes_idle_and_starts() {
    let mut sup = make_state(false, None, None, 0);
    snap(&mut sup, 0.0);
    sup.handle(enable(true), 1.0);
    assert_eq!(sup.status(1.0).state, RelayState::Idle);
    assert!(has_start(&tick(&mut sup, 1.5)));
}

// ------------------------------------------------------------- recovery

#[test]
fn test_a25_worker_crash_holds_with_new_epoch_and_recovers() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(worker_failed(g, e), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::ShowSlate { generation, reason: HoldReason::PipelineRebuild } if *generation == g
    ));
    assert!(has!(
        &acts,
        Action::RebuildPipeline { generation, epoch } if *generation == g && *epoch == e + 1
    ));
    assert!(has!(
        &acts,
        Action::RequestInputKeyframe { generation, method: KeyframeMethod::Pli } if *generation == g
    ));
    let st = sup.status(2.0);
    assert!(st.state == RelayState::Hold && st.epoch == e + 1 && st.generation == g);
    sup.handle(progress(g, 2.5), 2.5);
    let acts = sup.handle(fresh_frame(g, e + 1), 2.6);
    assert!(has_open_gate_for(&acts, g, e + 1));
    assert!(sup.gate_allows(g, e + 1, 2.6) && !sup.gate_allows(g, e, 2.6));
}

#[test]
fn test_keyframe_recovery_order_pli_rewatch_wait() {
    let mut sup = make_cfg(|c| {
        c.keyframe_step_seconds = 3.0;
        c.keyframe_methods = vec![KeyframeMethod::Pli, KeyframeMethod::Rewatch];
    });
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(worker_failed(g, e), 2.0);
    let methods: Vec<KeyframeMethod> = acts
        .iter()
        .filter_map(|a| match a {
            Action::RequestInputKeyframe { method, .. } => Some(*method),
            _ => None,
        })
        .collect();
    assert_eq!(methods, vec![KeyframeMethod::Pli]);
    assert_eq!(tick(&mut sup, 4.9), vec![]);
    let acts = tick(&mut sup, 5.0);
    assert!(has_rebuild_epoch(&acts, e + 2) && has_keyframe(&acts, KeyframeMethod::Rewatch));
    assert_eq!(
        sup.status(5.0).hold_reason.as_deref(),
        Some(HoldReason::Rewatch.as_str())
    );
    // waiting for a natural frame; deadline unchanged
    assert_eq!(tick(&mut sup, 9.0), vec![]);
    assert_eq!(sup.status(9.0).hold_deadline, Some(17.0));
    assert!(has_teardown_reason(
        &tick(&mut sup, 17.0),
        StopReason::HoldExpired
    ));
}

#[test]
fn test_default_recovery_repeats_pli_and_never_rewatches() {
    let mut sup = make_cfg(|c| c.keyframe_step_seconds = 3.0);
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(worker_failed(g, e), 2.0);
    assert!(has_keyframe(&acts, KeyframeMethod::Pli));
    for t in [5.0, 8.0, 11.0, 14.0] {
        sup.handle(progress(g, t - 0.1), t - 0.1);
        let acts = tick(&mut sup, t);
        assert!(has_keyframe(&acts, KeyframeMethod::Pli), "t={t}");
        assert!(!has_keyframe(&acts, KeyframeMethod::Rewatch), "t={t}");
    }
}

#[test]
fn test_hold_deadline_with_live_source_keeps_the_session() {
    let mut sup = make_cfg(|c| c.keyframe_step_seconds = 3.0);
    let (g, e) = relaying(&mut sup, 0.0);
    sup.handle(worker_failed(g, e), 2.0);
    for t in [5.0, 8.0, 11.0, 14.0] {
        sup.handle(progress(g, t - 0.1), t - 0.1);
        tick(&mut sup, t);
    }
    sup.handle(progress(g, 16.9), 16.9);
    let acts = tick(&mut sup, 17.0);
    assert!(!has_teardown_reason(&acts, StopReason::HoldExpired));
    assert!(has_keyframe(&acts, KeyframeMethod::Pli));
    let st = sup.status(17.0);
    assert_eq!(st.state, RelayState::Hold);
    assert_eq!(st.hold_deadline, Some(32.0));
    // still only PLI, never a rewatch
    sup.handle(progress(g, 19.9), 19.9);
    let acts = tick(&mut sup, 20.0);
    assert!(has_keyframe(&acts, KeyframeMethod::Pli));
    assert!(!has_keyframe(&acts, KeyframeMethod::Rewatch));
}

#[test]
fn test_keyframe_methods_subset() {
    let mut sup = make_cfg(|c| c.keyframe_methods = vec![KeyframeMethod::Rewatch]);
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(worker_failed(g, e), 2.0);
    assert!(has_keyframe(&acts, KeyframeMethod::Rewatch));
    assert!(!has_keyframe(&acts, KeyframeMethod::Pli));
}

#[test]
fn test_a37_pause_holds_without_deadline_and_reopens_on_new_frame() {
    let mut sup = make_cfg(|c| c.paused_capability_verified = true);
    let (g, e) = relaying_st(
        &mut sup,
        0.0,
        Some(stream_with(1, "k", |s| s.paused = Some(false))),
    );
    let acts = sup.handle(
        Event::StreamUpdated {
            stream: stream_with(1, "k", |s| s.paused = Some(true)),
        },
        2.0,
    );
    assert!(has_close_gate_first(&acts));
    assert!(has_slate(&acts, HoldReason::Paused));
    assert_eq!(sup.status(2.0).hold_deadline, None);
    assert_eq!(tick(&mut sup, 500.0), vec![]);
    assert_eq!(sup.status(500.0).state, RelayState::Hold);
    sup.handle(
        Event::StreamUpdated {
            stream: stream_with(1, "k", |s| s.paused = Some(false)),
        },
        600.0,
    );
    assert!(!sup.gate_allows(g, e, 600.0));
    sup.handle(progress(g, 600.1), 600.1);
    assert!(!has_open_gate(&tick(&mut sup, 600.2))); // no fresh frame yet
    assert!(has_open_gate(&sup.handle(fresh_frame(g, e), 600.3)));
}

#[test]
fn test_pause_ignored_without_capability() {
    let mut sup = make_cfg(|c| c.paused_capability_verified = false);
    let (g, e) = relaying(&mut sup, 0.0);
    assert_eq!(
        sup.handle(
            Event::StreamUpdated {
                stream: stream_with(1, "k", |s| s.paused = Some(true)),
            },
            2.0
        ),
        vec![]
    );
    assert!(sup.gate_allows(g, e, 2.0));
}

#[test]
fn test_a46_no_media_at_t_dead_then_teardown_at_deadline() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0); // last progress at 0.75
    assert_eq!(tick(&mut sup, 5.75), vec![]);
    assert!(sup.gate_allows(g, e, 5.75));
    let acts = tick(&mut sup, 5.875);
    assert!(has_close_gate_first(&acts));
    assert!(has_slate(&acts, HoldReason::NoMedia));
    assert!(!sup.gate_allows(g, e, 5.875));
    let st = sup.status(5.875);
    assert_eq!(st.hold_deadline, Some(20.875));
    let acts = tick(&mut sup, 21.0);
    assert!(has_teardown_reason(&acts, StopReason::HoldExpired));
    assert!(has_close_gate_first(&acts));
}

#[test]
fn test_gate_allows_closes_at_t_dead_before_tick() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    assert!(sup.gate_allows(g, e, 5.75));
    assert!(!sup.gate_allows(g, e, 5.875));
}

#[test]
fn test_inbound_or_output_unhealthy() {
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(output_health(g, false), 2.0);
    assert!(has_teardown_reason(&acts, StopReason::OutputDead));
    assert_eq!(sup.status(2.0).state, RelayState::Backoff);
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(inbound_health(g, false), 2.0);
    assert!(has_close_gate_first(&acts) && sup.status(2.0).state == RelayState::Hold);
}

// --------------------------------------------------------------- backoff

#[test]
fn test_backoff_retry_reverifies_source() {
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(connect_failed(g, StopReason::ConnectFailed, true), 2.0);
    assert!(has_teardown_reason(&acts, StopReason::ConnectFailed));
    let st = sup.status(2.0);
    assert!(st.state == RelayState::Backoff && st.backoff_until == Some(3.0));
    assert_eq!(tick(&mut sup, 2.9), vec![]);
    let (sg, _, _) = find_start(&tick(&mut sup, 3.0));
    assert_eq!(sg, g + 1);
}

#[test]
fn test_backoff_source_gone_goes_idle() {
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    sup.handle(connect_failed(g, StopReason::ConnectFailed, true), 2.0);
    sup.handle(stream_ended("k", 1), 2.5);
    assert_eq!(sup.status(2.5).state, RelayState::Idle);
    assert_eq!(tick(&mut sup, 100.0), vec![]);
}

#[test]
fn test_backoff_delays_grow() {
    let mut sup = make();
    snap(&mut sup, 0.0);
    let mut t = 0.5;
    let mut delays = vec![];
    for _ in 0..3 {
        let acts = tick(&mut sup, t);
        let (g, _, _) = find_start(&acts);
        sup.handle(connect_failed(g, StopReason::ConnectFailed, true), t);
        let until = sup.status(t).backoff_until.unwrap();
        delays.push(until - t);
        t = until;
    }
    assert_eq!(delays, vec![1.0, 2.0, 4.0]);
}

// ----------------------------------------------------------- ownership

#[test]
fn test_a14_displaced_same_session_quarantines_without_rejoin() {
    let mut sup = make();
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(relay_voice(Some("s1"), None), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown { generation, reason: StopReason::RelayDisplaced, .. } if *generation == g
    ));
    let rec = find_set_quarantine(&acts).unwrap();
    assert_eq!(
        (rec.owner_id, rec.stream_key.as_str(), rec.incarnation),
        (SRC, "k", 1)
    );
    assert!(has!(&acts, Action::SetRejections { count: 1 }));
    assert_eq!(sup.status(2.0).state, RelayState::Quarantine);
    for t in [5.0, 50.0, 500.0] {
        assert!(!has_start(&tick(&mut sup, t)));
    }
    assert_eq!(sup.handle(relay_voice(Some("s1"), None), 600.0), vec![]);
}

#[test]
fn test_a38_move_to_other_channel_same_session() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(relay_voice(Some("s1"), Some(OTHER)), 2.0);
    assert!(has!(&acts, Action::SetQuarantine { .. }));
    assert!(!has!(&acts, Action::SetFatalLatch { .. }));
}

#[test]
fn test_ownership_ok_observation_is_noop() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    assert_eq!(sup.handle(relay_voice(Some("s1"), Some(CH)), 2.0), vec![]);
    assert!(sup.gate_allows(g, e, 2.0));
}

#[test]
fn test_a35_other_session_is_fatal_ownership_uncertain() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(relay_voice(Some("other"), Some(CH)), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has_teardown_reason(&acts, StopReason::OwnershipLost));
    assert!(has_fatal_latch(
        &acts,
        FatalReason::AccountOwnershipUncertain
    ));
    assert_eq!(sup.status(2.0).state, RelayState::Fatal);
    assert_eq!(tick(&mut sup, 100.0), vec![]);
}

#[test]
fn test_missing_session_id_closes_gate_without_fatal() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(relay_voice(None, Some(CH)), 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(!has!(&acts, Action::SetFatalLatch { .. }));
    assert!(!sup.gate_allows(g, e, 2.0));
    assert_eq!(sup.status(2.0).state, RelayState::Hold);
}

#[test]
fn test_a36_supervisor_reconnect_is_not_ownership_loss() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(stream_ended("k", 1), 2.0);
    // our own cleanup, detector disarmed
    assert_eq!(sup.handle(relay_voice(Some("s1"), None), 2.1), vec![]);
    sup.handle(
        Event::StreamStarted {
            stream: stream_inc(2),
        },
        3.0,
    );
    let (g2, _, _) = find_start(&tick(&mut sup, 3.5));
    sup.handle(joined(g2, "s2"), 3.6);
    assert_eq!(sup.handle(relay_voice(Some("s2"), Some(CH)), 3.7), vec![]);
    assert_eq!(
        sup.status(3.7).voice_session_baseline.as_deref(),
        Some("s2")
    );
}

// ---------------------------------------------------------------- fatal

#[test]
fn test_a33_auth_rejected_latches_and_blocks() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(Event::AuthRejected, 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has_teardown_reason(&acts, StopReason::AuthRejected));
    assert!(has_fatal_latch(&acts, FatalReason::AuthRejected));
    assert_eq!(sup.status(2.0).state, RelayState::Fatal);
    snap(&mut sup, 3.0);
    assert_eq!(tick(&mut sup, 10.0), vec![]);
}

#[test]
fn test_auth_rejected_when_idle_has_no_teardown() {
    let mut sup = make();
    let acts = sup.handle(Event::AuthRejected, 0.0);
    assert!(has_close_gate_first(&acts));
    assert!(!has_teardown(&acts));
    assert!(has!(&acts, Action::SetFatalLatch { .. }));
}

#[test]
fn test_reset_fatal_clears_and_reevaluates() {
    let mut sup = make_state(true, Some(FatalReason::AuthRejected), None, 0);
    let acts = sup.handle(Event::ResetFatal, 1.0);
    assert!(has!(
        &acts,
        Action::ClearFatalLatch {
            reset_rejections: false
        }
    ));
    assert_eq!(sup.status(1.0).state, RelayState::Idle);
}

#[test]
fn test_a53_config_invalid_is_fatal_without_latch() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(
        Event::ConfigInvalid {
            detail: "bad".to_string(),
        },
        2.0,
    );
    assert!(has_close_gate_first(&acts));
    assert!(has_teardown_reason(&acts, StopReason::ConfigInvalid));
    assert!(!has!(&acts, Action::SetFatalLatch { .. }));
    let st = sup.status(2.0);
    assert!(st.state == RelayState::Fatal && st.fatal.as_deref() == Some("CONFIG_INVALID"));
}

#[test]
fn test_shutdown_closes_gate_first_and_exits() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(Event::Shutdown, 2.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown {
            reason: StopReason::Shutdown,
            leave_voice: true,
            ..
        }
    ));
    assert!(has!(&acts, Action::Exit { code: 0 }));
    assert_eq!(tick(&mut sup, 3.0), vec![]);
}

// ------------------------------------------------------------ quarantine

#[test]
fn test_restart_quarantine_active_stream_stays() {
    let q = quarantine_rec(7);
    let mut sup = make_state(true, None, Some(q), 0);
    assert_eq!(sup.status(0.0).state, RelayState::Quarantine);
    let acts = snap_st(&mut sup, 0.0, stream_inc(1), true);
    assert!(!has_set_quarantine_none(&acts));
    for t in [1.0, 50.0, 500.0] {
        assert!(!has_start(&tick(&mut sup, t)));
    }
    assert_eq!(sup.status(500.0).state, RelayState::Quarantine);
    assert_eq!(sup.status(500.0).quarantine.unwrap().stream_key, "k");
    let acts = sup.handle(stream_ended("k", 1), 501.0);
    assert!(has_set_quarantine_none(&acts));
    assert_eq!(sup.status(501.0).state, RelayState::Idle);
}

#[test]
fn test_restart_quarantine_cleared_when_source_gone() {
    let events = [
        (Some(tv(CH, false)), None, Some(ids(&[SRC]))),
        (None, None, Some(ids(&[]))),
        (
            Some(IN),
            Some(stream_with(1, "new", |_| {})),
            Some(ids(&[SRC])),
        ),
    ];
    for (target, st, parts) in events {
        let mut sup = make_state(true, None, Some(quarantine_rec(7)), 0);
        let acts = snap_full(&mut sup, 0.0, target, st, parts, true);
        assert!(has_set_quarantine_none(&acts));
        assert_eq!(sup.status(0.0).state, RelayState::Idle);
    }
}

#[test]
fn test_quarantine_cleared_by_new_incarnation_and_target_leaving() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(relay_voice(Some("s1"), None), 2.0);
    let acts = sup.handle(
        Event::StreamStarted {
            stream: stream_inc(2),
        },
        3.0,
    );
    assert!(has_set_quarantine_none(&acts));
    assert!(has_start(&tick(&mut sup, 3.5)));
    let mut sup = make();
    relaying(&mut sup, 0.0);
    sup.handle(relay_voice(Some("s1"), None), 2.0);
    let acts = sup.handle(target_changed(None), 3.0);
    assert!(has_set_quarantine_none(&acts));
}

#[test]
fn test_a44_three_rejections_then_latch_then_reset() {
    let mut sup = make();
    let mut t = 0.0;
    let mut counts = vec![];
    let mut acts = vec![];
    for inc in [1, 2, 3] {
        relaying_st(&mut sup, t, Some(stream_inc(inc)));
        acts = sup.handle(relay_voice(Some("s1"), None), t + 2.0);
        counts.push(find_set_rejections(&acts));
        if inc < 3 {
            assert!(!has!(&acts, Action::SetFatalLatch { .. }));
            assert_eq!(sup.status(t + 2.0).state, RelayState::Quarantine);
            assert!(has_set_quarantine_none(
                &sup.handle(stream_ended("k", inc), t + 3.0)
            ));
            assert!(!has!(
                &sup.handle(relay_voice(Some("s1"), None), t + 3.1),
                Action::SetRejections { .. }
            ));
            t += 10.0;
        }
    }
    assert_eq!(counts, vec![1, 2, 3]);
    assert!(has_fatal_latch(&acts, FatalReason::RelayRejectedByServer));
    let st = sup.status(t + 2.0);
    assert!(st.state == RelayState::Fatal && st.consecutive_rejections == 3);
    let acts = sup.handle(Event::ResetFatal, t + 5.0);
    assert!(has!(
        &acts,
        Action::ClearFatalLatch {
            reset_rejections: true
        }
    ));
    let st = sup.status(t + 5.0);
    assert!(
        st.state == RelayState::Quarantine
            && st.consecutive_rejections == 0
            && st.quarantine.is_some()
    );
}

#[test]
fn test_a44_normal_end_with_media_resets_counter() {
    let mut sup = make_state(true, None, None, 2);
    relaying(&mut sup, 0.0);
    let acts = sup.handle(stream_ended("k", 1), 2.0);
    assert!(has!(&acts, Action::SetRejections { count: 0 }));
    assert_eq!(sup.status(2.0).consecutive_rejections, 0);
}

#[test]
fn test_rejection_not_reset_when_no_media() {
    let mut sup = make_state(true, None, None, 2);
    start(&mut sup, 0.0);
    let acts = sup.handle(stream_ended("k", 1), 1.0);
    assert!(!has!(&acts, Action::SetRejections { .. }));
}

// ---------------------------------------------------- excluded viewers

#[test]
fn test_a41_excluded_viewer_blocks_start_and_stops_session() {
    let mut sup = make_cfg(|c| c.excluded_viewer_ids = ids(&[77]));
    snap_full(
        &mut sup,
        0.0,
        Some(IN),
        Some(stream()),
        Some(ids(&[SRC, 77])),
        false,
    );
    assert_eq!(tick(&mut sup, 5.0), vec![]);
    sup.handle(participants(Some(&[SRC])), 6.0);
    relaying(&mut sup, 6.0);
    let acts = sup.handle(participants(Some(&[SRC, 77])), 8.0);
    assert!(has_close_gate_first(&acts));
    assert!(has!(
        &acts,
        Action::Teardown {
            reason: StopReason::ExcludedViewer,
            leave_voice: true,
            ..
        }
    ));
    assert_eq!(tick(&mut sup, 20.0), vec![]);
    sup.handle(participants(Some(&[SRC])), 21.0);
    assert!(has_start(&tick(&mut sup, 21.5)));
}

#[test]
fn test_a51_missing_participant_view_blocks() {
    let mut sup = make_cfg(|c| c.excluded_viewer_ids = ids(&[77]));
    snap_full(&mut sup, 0.0, Some(IN), Some(stream()), None, true);
    assert_eq!(tick(&mut sup, 5.0), vec![]);
    let mut sup2 = make_cfg(|c| c.excluded_viewer_ids = ids(&[77]));
    snap_full(
        &mut sup2,
        0.0,
        Some(IN),
        Some(stream()),
        Some(ids(&[SRC])),
        true,
    );
    assert!(has_start(&tick(&mut sup2, 0.5)));
    let acts = sup2.handle(participants(None), 1.0);
    assert!(has_teardown_reason(&acts, StopReason::ExcludedViewer));
}

#[test]
fn test_empty_excluded_needs_no_participant_snapshot() {
    let mut sup = make();
    snap_full(&mut sup, 0.0, Some(IN), Some(stream()), None, true);
    assert!(has_start(&tick(&mut sup, 0.5)));
}

// ---------------------------------------------------------------- status

#[test]
fn test_status_fields() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let st = sup.status(2.0);
    assert!(st.state == RelayState::Relaying && st.generation == g && st.epoch == e);
    assert!(
        st.source_incarnation == Some(1)
            && st.gateway == "fresh"
            && st.voice_session_baseline.as_deref() == Some("s1")
    );
    assert!(st.last_source_progress_age == Some(1.25) && st.gate_failed.is_empty());
    assert!(st.desired_enabled && st.fatal.is_none());
}

// --------------------------------------------------------------- routing

#[test]
fn test_target_change_while_live_stops_and_restarts_with_new_target() {
    let mut sup = make();
    let (g, e) = relaying(&mut sup, 0.0);
    let acts = sup.handle(routing_changed(routing(2, CH, &[])), 2.0);
    assert!(has_close_gate_first(&acts));
    let (tg, leave, reason) = find_teardown(&acts);
    assert!(reason == StopReason::RoutingChanged && !leave && tg == g);
    let td_pos = acts
        .iter()
        .position(|a| matches!(a, Action::Teardown { .. }))
        .unwrap();
    let ar_pos = acts
        .iter()
        .position(|a| matches!(a, Action::ApplyRouting { .. }))
        .unwrap();
    assert!(td_pos < ar_pos);
    assert!(!sup.gate_allows(g, e, 2.0) && sup.generation() == g + 1);
    assert!(sup.routing().target_user_id == 2 && sup.status(2.0).state == RelayState::Idle);
    snap_full(
        &mut sup,
        3.0,
        Some(IN),
        Some(stream_with(1, "k2", |s| s.owner_id = 2)),
        Some(ids(&[2])),
        false,
    );
    let acts = tick(&mut sup, 3.5);
    let (sg, _, sst) = find_start(&acts);
    assert!(sg == g + 1 && sst.owner_id == 2);
}

#[test]
fn test_channel_change_leaves_voice() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(routing_changed(routing(SRC, OTHER, &[])), 2.0);
    assert!(find_teardown(&acts).1);
    assert_eq!(sup.routing().channel_id, OTHER);
}

#[test]
fn test_target_change_same_channel_keeps_voice() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(routing_changed(routing(2, CH, &[])), 2.0);
    assert!(!find_teardown(&acts).1);
}

#[test]
fn test_routing_change_while_disabled_only_updates_cfg() {
    let mut sup = make_state(false, None, None, 0);
    let acts = sup.handle(routing_changed(routing(2, OTHER, &[(5, 6)])), 1.0);
    assert_eq!(
        acts,
        vec![Action::ApplyRouting {
            routing: routing(2, OTHER, &[(5, 6)])
        }]
    );
    assert_eq!(sup.status(1.0).state, RelayState::Disabled);
    assert_eq!(sup.routing().mirrors, IndexMap::from([(5u64, 6u64)]));
}

#[test]
fn test_routing_change_is_not_a_rejection() {
    let q = quarantine_rec(1);
    let mut sup = make_state(true, None, Some(q.clone()), 2);
    sup.handle(routing_changed(routing(2, CH, &[])), 1.0);
    let st = sup.status(1.0);
    assert!(st.consecutive_rejections == 2 && st.backoff_until.is_none());
    assert_eq!(sup.quarantine(), Some(&q));
    let mut sup2 = make();
    let (g, _) = relaying(&mut sup2, 0.0);
    sup2.handle(connect_failed(g, StopReason::ConnectFailed, true), 2.0);
    let st = sup2.status(2.0);
    assert!(st.state == RelayState::Backoff && st.backoff_until == Some(3.0));
    let acts = sup2.handle(routing_changed(routing(2, CH, &[])), 2.5);
    let st = sup2.status(2.5);
    assert!(
        !has!(&acts, Action::SetRejections { .. })
            && st.state == RelayState::Idle
            && st.backoff_until.is_none()
    );
}

#[test]
fn test_mirrors_only_change_does_not_stop() {
    let mut sup = make();
    relaying(&mut sup, 0.0);
    let acts = sup.handle(routing_changed(routing(SRC, CH, &[(5, 6)])), 2.0);
    assert_eq!(
        acts,
        vec![Action::ApplyRouting {
            routing: routing(SRC, CH, &[(5, 6)])
        }]
    );
    assert_eq!(sup.status(2.0).state, RelayState::Relaying);
}

// ------------------------------------------------- slot race: prejoin, debounce, fast watch retry

fn idle_in_channel(sup: &mut Supervisor, now: f64) -> Vec<Action> {
    idle_in_channel_with(sup, now, ids(&[SRC]))
}

fn idle_in_channel_with(sup: &mut Supervisor, now: f64, parts: HashSet<u64>) -> Vec<Action> {
    snap_full(sup, now, Some(NOT_SHARING), None, Some(parts), true)
}

#[test]
fn test_prejoin_off_by_default() {
    let mut sup = make();
    let mut acts = idle_in_channel(&mut sup, 0.0);
    acts.extend(tick(&mut sup, 5.0));
    assert!(!has_join(&acts));
}

#[test]
fn test_prejoin_joins_once_when_target_in_channel() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    let acts = idle_in_channel(&mut sup, 0.0);
    assert_eq!(
        acts.iter()
            .filter(|a| matches!(a, Action::JoinVoice))
            .count(),
        1
    );
    assert!(!has_join(&tick(&mut sup, 1.0)));
    // no idle grace while prejoined
    assert!(!has_leave(&tick(&mut sup, 100.0)));
    assert_eq!(sup.status(100.0).state, RelayState::Idle);
}

#[test]
fn test_prejoin_keeps_join_gates() {
    let q = quarantine_rec(1);
    let sups = [
        make_full(false, None, None, 0, |c| c.prejoin_voice = true),
        make_full(true, Some(FatalReason::AuthRejected), None, 0, |c| {
            c.prejoin_voice = true
        }),
        make_cfg(|c| {
            c.prejoin_voice = true;
            c.excluded_viewer_ids = ids(&[77]);
        }),
    ];
    for mut sup in sups {
        let mut acts = idle_in_channel_with(&mut sup, 0.0, ids(&[SRC, 77]));
        acts.extend(tick(&mut sup, 1.0));
        assert!(!has_join(&acts));
    }
    let mut sup = make_cfg(|c| c.prejoin_voice = true); // quarantined incarnation still streaming
    let mut sup2 = make_full(true, None, Some(q), 0, |c| c.prejoin_voice = true);
    let mut acts = snap(&mut sup2, 0.0);
    acts.extend(tick(&mut sup2, 1.0));
    assert!(sup2.status(1.0).state == RelayState::Quarantine && !has_join(&acts));
    // gateway never synced
    sup.handle(target_changed(Some(tv(OTHER, false))), 0.0);
    assert!(!has_join(&tick(&mut sup, 1.0)));
    let mut sup3 = make_cfg(|c| c.prejoin_voice = true);
    let mut acts = snap_full(
        &mut sup3,
        0.0,
        Some(tv(OTHER, false)),
        None,
        Some(ids(&[SRC])),
        true,
    );
    acts.extend(tick(&mut sup3, 1.0));
    assert!(!has_join(&acts));
}

#[test]
fn test_prejoin_leaves_when_target_leaves_or_disabled() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    idle_in_channel(&mut sup, 0.0);
    assert!(has_leave(
        &sup.handle(target_changed(Some(tv(OTHER, false))), 1.0)
    ));
    // back in: joins again
    assert!(has_join(
        &sup.handle(target_changed(Some(NOT_SHARING)), 2.0)
    ));
    assert!(has_leave(&sup.handle(enable(false), 3.0)));
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    idle_in_channel(&mut sup, 0.0);
    assert!(has_leave(
        &sup.handle(routing_changed(routing(SRC, OTHER, &[])), 1.0)
    ));
}

#[test]
fn test_prejoin_gate_loss_leaves_after_idle_grace() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.excluded_viewer_ids = ids(&[77]);
    });
    idle_in_channel(&mut sup, 0.0);
    assert!(!has_leave(&sup.handle(participants(Some(&[SRC, 77])), 1.0)));
    assert!(!has_leave(&tick(&mut sup, 30.9)));
    assert!(has_leave(&tick(&mut sup, 31.0)));
}

#[test]
fn test_prejoin_stays_in_voice_after_stream_ends() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(target_changed(Some(NOT_SHARING)), 2.0);
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: false, .. } if *generation == g
    ));
    assert!(!has_leave(&tick(&mut sup, 100.0)));
}

#[test]
fn test_prejoined_stream_start_waits_at_most_debounce() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.debounce = 0.0;
    });
    idle_in_channel(&mut sup, 0.0);
    sup.handle(target_changed(Some(IN)), 1.0);
    let acts = sup.handle(Event::StreamStarted { stream: stream() }, 1.0);
    assert!(has_start(&acts) && !has_join(&acts));
}

#[test]
fn test_already_streaming_when_enabled_starts_within_debounce() {
    let mut sup = make_full(false, None, None, 0, |c| c.debounce = 0.05);
    snap(&mut sup, 0.0);
    assert!(!has_start(&sup.handle(enable(true), 1.0)));
    assert!(has_start(&tick(&mut sup, 1.05)));
}

fn watch_failed(sup: &mut Supervisor, g: u64, now: f64) -> Vec<Action> {
    sup.handle(
        connect_failed_detail(
            g,
            StopReason::ConnectFailed,
            true,
            "Stream connection rejected: stream_full",
            true,
        ),
        now,
    )
}

#[test]
fn test_watch_failure_retries_fast_without_escalation_or_rejection() {
    let mut sup = make_cfg(|c| c.retry_delays = vec![5.0, 10.0]);
    let (mut g, _) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    let mut t = 0.7;
    for i in 0..6 {
        let acts = watch_failed(&mut sup, g, t);
        // every third consecutive fast retry leaves voice, so a dead parent voice client is rebuilt
        assert!(has!(
            &acts,
            Action::Teardown { generation, leave_voice, .. } if *generation == g && *leave_voice == (i % 3 == 2)
        ));
        let st = sup.status(t);
        assert!(st.state == RelayState::Backoff && approx(st.backoff_until, t + 1.0));
        assert!(st.consecutive_rejections == 0 && !has!(&acts, Action::SetRejections { .. }));
        let (sg, _, _) = find_start(&tick(&mut sup, t + 1.0));
        g = sg;
        sup.handle(joined(g, "s1"), t + 1.0);
        t += 1.1;
    }
}

#[test]
fn test_watch_fast_retry_is_capped_then_normal_backoff() {
    let mut sup = make_cfg(|c| {
        c.retry_delays = vec![5.0, 10.0];
        c.watch_retry_cap = 3.0;
    });
    let (g, _) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.5);
    watch_failed(&mut sup, g, 1.0);
    let (g, _, _) = find_start(&tick(&mut sup, 2.0));
    watch_failed(&mut sup, g, 4.5); // 3.5 s after the first fast retry: cap reached
    let st = sup.status(4.5);
    assert!(st.state == RelayState::Backoff && approx(st.backoff_until, 9.5));
}

#[test]
fn test_join_failure_keeps_normal_backoff() {
    let mut sup = make_cfg(|c| c.retry_delays = vec![5.0, 10.0]);
    let (g, _) = start(&mut sup, 0.0);
    let acts = sup.handle(
        connect_failed_detail(g, StopReason::ConnectFailed, true, "join", false),
        1.0,
    );
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: true, .. } if *generation == g
    ));
    assert!(approx(sup.status(1.0).backoff_until, 6.0));
}

#[test]
fn test_watch_timeout_retries_fast() {
    let mut sup = make_cfg(|c| {
        c.retry_delays = vec![5.0, 10.0];
        c.connect_timeout = 15.0;
    });
    let (g, _) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    let acts = tick(&mut sup, 15.5);
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: false, .. } if *generation == g
    ));
    assert!(approx(sup.status(15.5).backoff_until, 16.5));
}

// ------------------------------------------------- prejoin toggle, prejoin retry, gate_ready

#[test]
fn test_prejoin_toggle_on_joins_and_off_leaves() {
    let mut sup = make();
    let mut acts = idle_in_channel(&mut sup, 0.0);
    acts.extend(tick(&mut sup, 1.0));
    assert!(!has_join(&acts));
    let acts = sup.handle(prejoin_changed(true), 2.0);
    assert_eq!(
        acts.iter()
            .filter(|a| matches!(a, Action::JoinVoice))
            .count(),
        1
    );
    assert!(sup.prejoin_voice());
    let acts = sup.handle(prejoin_changed(false), 3.0);
    assert!(has_leave(&acts) && !sup.prejoin_voice());
    assert!(!has_join(&tick(&mut sup, 4.0)));
}

#[test]
fn test_prejoin_on_when_target_elsewhere_does_not_join() {
    let mut sup = make();
    snap_full(
        &mut sup,
        0.0,
        Some(tv(OTHER, false)),
        None,
        Some(ids(&[SRC])),
        true,
    );
    assert!(!has_join(&sup.handle(prejoin_changed(true), 1.0)));
}

#[test]
fn test_prejoin_off_while_streaming_keeps_session() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    relaying(&mut sup, 0.0);
    let acts = sup.handle(prejoin_changed(false), 2.0);
    assert!(!has_leave(&acts) && !has_teardown(&acts));
    assert_eq!(sup.status(2.0).state, RelayState::Relaying);
}

#[test]
fn test_prejoin_failure_clears_voice_and_retries_with_backoff() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.retry_delays = vec![5.0, 10.0];
    });
    assert!(has_join(&idle_in_channel(&mut sup, 0.0)));
    let acts = sup.handle(prejoin_failed("boom"), 1.0);
    assert!(!has_join(&acts));
    assert!(!has_join(&tick(&mut sup, 5.9)));
    assert!(has_join(&tick(&mut sup, 6.0)));
    sup.handle(prejoin_failed("boom"), 6.5);
    assert!(!has_join(&tick(&mut sup, 16.4)));
    assert!(has_join(&tick(&mut sup, 16.5)));
}

#[test]
fn test_prejoin_failure_ignored_during_session() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    let (g, _) = start(&mut sup, 0.0);
    sup.handle(prejoin_failed("late"), 0.6);
    assert_eq!(sup.status(0.6).state, RelayState::Connecting);
    let acts = sup.handle(target_changed(Some(NOT_SHARING)), 1.0);
    // still believed in voice
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: false, .. } if *generation == g
    ));
}

#[test]
fn test_non_fast_watch_failure_uses_normal_backoff() {
    let mut sup = make_cfg(|c| c.retry_delays = vec![5.0, 10.0]);
    let (g, _) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    let acts = sup.handle(
        connect_failed_detail(g, StopReason::ConnectFailed, true, "unauthorized", false),
        1.0,
    );
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: true, .. } if *generation == g
    ));
    assert!(approx(sup.status(1.0).backoff_until, 6.0));
}

#[test]
fn test_gate_ready_at_is_debounce_start_or_backoff_expiry() {
    let mut sup = make_cfg(|c| {
        c.debounce = 0.5;
        c.retry_delays = vec![5.0];
    });
    snap(&mut sup, 2.0);
    assert!(has_start(&tick(&mut sup, 2.5)));
    assert_eq!(sup.gate_ready_at(), Some(2.0));
    let g = sup.generation();
    sup.handle(
        connect_failed_detail(g, StopReason::ConnectFailed, true, "join", false),
        3.0,
    );
    assert!(has_start(&tick(&mut sup, 8.0)));
    // backoff time is not start latency
    assert_eq!(sup.gate_ready_at(), Some(8.0));
}

#[test]
fn test_prejoin_off_during_debounce_then_stream_end_leaves_after_idle_grace() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.debounce = 5.0;
        c.idle_grace = 30.0;
    });
    assert!(has_join(&idle_in_channel(&mut sup, 0.0)));
    sup.handle(target_changed(Some(IN)), 1.0);
    // in the start debounce
    sup.handle(Event::StreamStarted { stream: stream() }, 1.0);
    assert!(!has_leave(&sup.handle(prejoin_changed(false), 2.0)));
    // ends before the start
    sup.handle(target_changed(Some(NOT_SHARING)), 3.0);
    assert!(!has_leave(&tick(&mut sup, 32.9)));
    assert!(has_leave(&tick(&mut sup, 33.0)));
}

#[test]
fn test_prejoin_retry_resets_after_confirmed_join() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.retry_delays = vec![5.0, 10.0];
    });
    assert!(has_join(&idle_in_channel(&mut sup, 0.0)));
    sup.handle(prejoin_failed("boom"), 1.0);
    assert!(has_join(&tick(&mut sup, 6.0)));
    // the pre-join landed in the main channel
    sup.handle(relay_voice(Some("vs"), Some(CH)), 6.5);
    sup.handle(prejoin_failed("boom"), 7.0);
    // first delay again, not the second
    assert!(has_join(&tick(&mut sup, 12.0)));
}

#[test]
fn test_voice_lost_idle_clears_in_voice_and_rejoins_after_retry_delay() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.retry_delays = vec![5.0, 10.0];
        c.retry_cooldown = 60.0;
    });
    assert!(has_join(&idle_in_channel(&mut sup, 0.0)));
    let acts = sup.handle(voice_lost("not connected", false, false), 1.0);
    assert!(!has_join(&acts) && !has_teardown(&acts));
    assert!(!has_join(&tick(&mut sup, 5.9)));
    assert!(has_join(&tick(&mut sup, 6.0)));
}

#[test]
fn test_voice_lost_in_session_tears_down_with_leave() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    let (g, _) = relaying(&mut sup, 0.0);
    let acts = sup.handle(voice_lost("not connected", false, false), 2.0);
    assert!(has!(&acts, Action::CloseGate { .. }));
    assert!(has!(
        &acts,
        Action::Teardown { generation, leave_voice: true, reason: StopReason::ConnectFailed } if *generation == g
    ));
    assert_eq!(sup.status(2.0).state, RelayState::Backoff);
}

#[test]
fn test_voice_lost_from_prejoin_ignored_during_session() {
    let mut sup = make_cfg(|c| c.prejoin_voice = true);
    start(&mut sup, 0.0);
    let acts = sup.handle(voice_lost("4006", true, true), 0.6);
    assert!(!has_teardown(&acts));
    assert_eq!(sup.status(0.6).state, RelayState::Connecting);
}

#[test]
fn test_voice_lost_foreign_session_uses_cooldown() {
    let mut sup = make_cfg(|c| {
        c.prejoin_voice = true;
        c.retry_delays = vec![5.0, 10.0];
        c.retry_cooldown = 60.0;
    });
    assert!(has_join(&idle_in_channel(&mut sup, 0.0)));
    sup.handle(voice_lost("4006", true, false), 1.0);
    assert!(!has_join(&tick(&mut sup, 60.9)));
    assert!(has_join(&tick(&mut sup, 61.0)));
}

#[test]
fn test_machine_adaptive_accepts_h265_ready() {
    let mut sup = make_cfg(|c| c.adaptive = true);
    let (g, e) = start(&mut sup, 0.0);
    sup.handle(joined(g, "s1"), 0.6);
    sup.handle(watching(g), 0.7);
    sup.handle(progress(g, 0.8), 0.8);
    sup.handle(first_frame(g, e, 1, 1), 0.9);
    let acts = sup.handle(output_ready(g, "H265"), 1.0);
    assert!(has_open_gate(&acts) && !has_teardown(&acts));
    // op 14 never tears down
    assert!(!has_teardown(&sup.handle(codec_changed(g, "AV1"), 2.0)));
    assert!(has_teardown_reason(
        &sup.handle(output_ready(g, "FOO"), 3.0),
        StopReason::CodecMismatch
    ));
}
