//! Port of `tests/supervisor/test_outputs.py`.

use std::collections::HashMap;

use indexmap::IndexMap;
use relay_core::config::{AccountRole, OutputAccount};
use relay_core::control::state::{StateStore, set_account};
use relay_core::supervisor::model::{FatalReason, HoldReason, StopReason};
use relay_core::supervisor::outputs::{OutputPool, PoolConfig, RelaySummary};
use relay_core::supervisor::outputs_model::{
    AccountPersisted, PoolAction, PoolEvent, account_view,
};
use relay_core::supervisor::retry::RetryPolicy;
use relay_core::supervisor::routing::Routing;
use serde_json::json;

const CH: u64 = 500;
const A: u64 = 11;
const B: u64 = 12;
const C: u64 = 13;
const SRC: u64 = 99;
const MCH: u64 = 600;
const MCH2: u64 = 601;

struct Opts {
    ids: Vec<u64>,
    persisted: HashMap<u64, AccountPersisted>,
    limit: i64,
    mirrors: Vec<(u64, u64)>,
    excluded: Vec<u64>,
    mirror_ids: Vec<u64>,
    idle: f64,
    adaptive: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            ids: vec![A, B],
            persisted: HashMap::new(),
            limit: 3,
            mirrors: vec![],
            excluded: vec![],
            mirror_ids: vec![],
            idle: 30.0,
            adaptive: false,
        }
    }
}

fn make_with(o: Opts) -> OutputPool {
    let accounts: Vec<OutputAccount> = o
        .ids
        .iter()
        .map(|&i| OutputAccount {
            user_id: i,
            credential: format!("c{i}"),
            role: if o.mirror_ids.contains(&i) {
                AccountRole::Mirror
            } else {
                AccountRole::Relay
            },
            channel_id: None,
        })
        .collect();
    let mirrors: IndexMap<u64, u64> = o.mirrors.iter().copied().collect();
    let mut cfg = PoolConfig::new(Routing {
        target_user_id: SRC,
        channel_id: CH,
        mirrors,
    });
    cfg.rejection_limit = o.limit;
    cfg.excluded_viewer_ids = o.excluded.iter().copied().collect();
    cfg.relay_idle_stop_s = o.idle;
    cfg.adaptive = o.adaptive;
    OutputPool::new(
        &accounts,
        &o.persisted,
        || RetryPolicy::new(&[1.0, 2.0], 30.0).jitter(0.0).seed(0),
        cfg,
    )
}

fn make(ids: &[u64]) -> OutputPool {
    make_with(Opts {
        ids: ids.to_vec(),
        ..Opts::default()
    })
}

fn routing(channel: u64, mirrors: &[(u64, u64)]) -> Routing {
    Routing {
        target_user_id: SRC,
        channel_id: channel,
        mirrors: mirrors.iter().copied().collect(),
    }
}

// ---- events

fn live(generation: u64, epoch: u64, incarnation: i64) -> PoolEvent {
    PoolEvent::SessionLive {
        generation,
        epoch,
        incarnation,
    }
}

fn hold(generation: u64, reason: HoldReason) -> PoolEvent {
    PoolEvent::SessionHold { generation, reason }
}

fn ended(generation: u64, reason: StopReason, incarnation_ended: bool) -> PoolEvent {
    PoolEvent::SessionEnded {
        generation,
        reason,
        incarnation_ended,
    }
}

fn joined(account: u64, generation: u64, attempt: u64, session: &str) -> PoolEvent {
    PoolEvent::OutJoined {
        account,
        generation,
        attempt,
        voice_session_id: Some(session.to_string()),
    }
}

fn ready(account: u64, generation: u64, attempt: u64, codec: &str) -> PoolEvent {
    PoolEvent::OutReady {
        account,
        generation,
        attempt,
        negotiated_video_codec: Some(codec.to_string()),
    }
}

fn failed(
    account: u64,
    generation: u64,
    attempt: u64,
    reason: StopReason,
    retryable: bool,
) -> PoolEvent {
    PoolEvent::OutFailed {
        account,
        generation,
        attempt,
        reason,
        retryable,
        detail: String::new(),
    }
}

fn health(account: u64, generation: u64, attempt: u64, ok: bool) -> PoolEvent {
    PoolEvent::OutHealth {
        account,
        generation,
        attempt,
        ok,
    }
}

fn voice(account: u64, session: &str, channel: Option<u64>) -> PoolEvent {
    PoolEvent::OutVoiceObserved {
        account,
        session_id: Some(session.to_string()),
        channel_id: channel,
    }
}

fn viewers(account: u64, v: &[u64]) -> PoolEvent {
    PoolEvent::OutViewers {
        account,
        viewers: v.iter().copied().collect(),
    }
}

fn participants(account: u64, v: &[u64], channel_id: u64) -> PoolEvent {
    PoolEvent::MirrorParticipants {
        account,
        participants: v.iter().copied().collect(),
        channel_id,
    }
}

const TICK: PoolEvent = PoolEvent::OutTick;

// ---- actions

fn start(account: u64, generation: u64, attempt: u64, channel_id: u64) -> PoolAction {
    PoolAction::Start {
        account,
        generation,
        attempt,
        channel_id,
    }
}

fn set_live(account: u64, generation: u64, epoch: u64) -> PoolAction {
    PoolAction::SetLive {
        account,
        generation,
        epoch,
    }
}

fn slate(account: u64, generation: u64) -> PoolAction {
    PoolAction::Slate {
        account,
        generation,
    }
}

fn close(account: u64, reason: &'static str) -> PoolAction {
    PoolAction::CloseGate { account, reason }
}

fn teardown(account: u64, generation: u64, reason: StopReason) -> PoolAction {
    PoolAction::Teardown {
        account,
        generation,
        reason,
    }
}

fn set_fatal(account: u64, reason: FatalReason) -> PoolAction {
    PoolAction::SetFatal {
        account,
        reason,
        detail: String::new(),
    }
}

fn set_quarantine(account: u64, incarnation: Option<i64>) -> PoolAction {
    PoolAction::SetQuarantine {
        account,
        incarnation,
    }
}

fn set_rejections(account: u64, count: i64) -> PoolAction {
    PoolAction::SetRejections { account, count }
}

fn kinds(actions: &[PoolAction]) -> Vec<&'static str> {
    actions
        .iter()
        .map(|a| match a {
            PoolAction::Start { .. } => "Start",
            PoolAction::SetLive { .. } => "SetLive",
            PoolAction::Slate { .. } => "Slate",
            PoolAction::CloseGate { .. } => "CloseGate",
            PoolAction::Teardown { .. } => "Teardown",
            PoolAction::SetFatal { .. } => "SetFatal",
            PoolAction::SetQuarantine { .. } => "SetQuarantine",
            PoolAction::SetRejections { .. } => "SetRejections",
        })
        .collect()
}

fn attempt_of(acts: &[PoolAction], uid: u64) -> Option<u64> {
    acts.iter().find_map(|a| match a {
        PoolAction::Start {
            account, attempt, ..
        } if *account == uid => Some(*attempt),
        _ => None,
    })
}

fn bring_live(p: &mut OutputPool, ids: &[u64], incarnation: i64, mut t: f64) -> f64 {
    let (g, e) = (1, 1);
    let mut acts = p.handle(live(g, e, incarnation), t);
    for &i in ids {
        let n = loop {
            if let Some(n) = attempt_of(&acts, i) {
                break n;
            }
            t += 2.0;
            acts = p.handle(TICK, t);
        };
        p.handle(joined(i, g, n, &format!("vs{i}")), t);
        let r = p.handle(ready(i, g, n, "H264"), t);
        assert_eq!(r, vec![set_live(i, g, e)]);
    }
    t
}

fn bring(p: &mut OutputPool, ids: &[u64]) -> f64 {
    bring_live(p, ids, 7, 0.0)
}

fn summary(wanted: usize, running: usize, available: usize, short_by: usize) -> RelaySummary {
    RelaySummary {
        wanted,
        running,
        available,
        short_by,
        kept: 0,
        kept_ids: vec![],
    }
}

fn state_of(p: &OutputPool, now: f64, idx: usize) -> &'static str {
    p.status(now)[idx].state.as_str()
}

#[test]
fn test_start_is_staggered_one_per_window() {
    let mut p = make(&[A, B, C]);
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 1.0), vec![]);
    assert_eq!(p.handle(TICK, 2.0), vec![start(B, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 3.9), vec![]);
    assert_eq!(p.handle(TICK, 4.0), vec![start(C, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 9.0), vec![]);
}

#[test]
fn test_ready_h264_goes_live_and_gate() {
    let mut p = make(&[A]);
    p.handle(live(1, 1, 7), 0.0);
    p.handle(joined(A, 1, 1, "vs"), 0.1);
    assert!(!p.account_live(A, 1, 1));
    assert_eq!(
        p.handle(ready(A, 1, 1, "H264"), 0.2),
        vec![set_live(A, 1, 1)]
    );
    assert!(p.account_live(A, 1, 1));
    assert!(!p.account_live(A, 1, 2));
    assert!(!p.account_live(A, 2, 1));
}

#[test]
fn test_ready_other_codec_backs_off_close_before_teardown() {
    let mut p = make(&[A]);
    p.handle(live(1, 1, 7), 0.0);
    p.handle(joined(A, 1, 1, "vs"), 0.1);
    let acts = p.handle(ready(A, 1, 1, "VP8"), 0.2);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown"]);
    assert_eq!(state_of(&p, 0.2, 0), "backoff");
}

#[test]
fn test_hold_slates_live_accounts_and_late_ready_slates() {
    let mut p = make(&[A, B]);
    bring(&mut p, &[A]);
    p.handle(TICK, 2.0); // B starts
    p.handle(joined(B, 1, 1, "vsb"), 2.1);
    assert_eq!(
        p.handle(hold(1, HoldReason::NoMedia), 3.0),
        vec![slate(A, 1)]
    );
    assert!(!p.account_live(A, 1, 1));
    assert_eq!(p.handle(ready(B, 1, 1, "H264"), 3.5), vec![slate(B, 1)]);
    assert_eq!(
        p.handle(live(1, 2, 7), 4.0),
        vec![set_live(A, 1, 2), set_live(B, 1, 2)]
    );
    assert!(p.account_live(A, 1, 2));
}

#[test]
fn test_session_end_closes_then_tears_down_all_and_clears_quarantine() {
    let mut p = make_with(Opts {
        persisted: HashMap::from([(
            B,
            AccountPersisted {
                fatal_reason: None,
                quarantine_incarnation: Some(7),
                consecutive_rejections: 1,
            },
        )]),
        ..Opts::default()
    });
    bring(&mut p, &[A]);
    assert_eq!(state_of(&p, 0.0, 1), "quarantine");
    let acts = p.handle(ended(1, StopReason::SourceStopped, true), 5.0);
    assert_eq!(
        acts[..2],
        [
            close(A, "source_stopped"),
            teardown(A, 1, StopReason::SourceStopped)
        ]
    );
    assert!(acts.contains(&set_quarantine(B, None)));
    assert!(!acts.contains(&set_rejections(A, 0))); // A had zero rejections: nothing to clear
    let states: Vec<_> = p.status(5.0).iter().map(|s| s.state.as_str()).collect();
    assert_eq!(states, ["off", "off"]);
}

fn persisted_rejections(n: i64) -> AccountPersisted {
    AccountPersisted {
        fatal_reason: None,
        quarantine_incarnation: None,
        consecutive_rejections: n,
    }
}

#[test]
fn test_session_end_zeroes_rejections_of_clean_streamers_only() {
    let mut p = make_with(Opts {
        persisted: HashMap::from([(A, persisted_rejections(2)), (B, persisted_rejections(2))]),
        ..Opts::default()
    });
    bring(&mut p, &[A]);
    let acts = p.handle(ended(1, StopReason::SourceStopped, true), 5.0);
    assert!(acts.contains(&set_rejections(A, 0)));
    assert!(!acts.contains(&set_rejections(B, 0)));
}

#[test]
fn test_session_end_without_incarnation_end_keeps_state() {
    let mut p = make_with(Opts {
        ids: vec![A],
        persisted: HashMap::from([(A, persisted_rejections(2))]),
        ..Opts::default()
    });
    bring(&mut p, &[A]);
    let acts = p.handle(ended(1, StopReason::HoldExpired, false), 5.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown"]);
}

#[test]
fn test_displacement_quarantines_and_counts_once_then_fatal_at_limit() {
    let mut p = make_with(Opts {
        ids: vec![A],
        limit: 3,
        ..Opts::default()
    });
    let mut t = bring_live(&mut p, &[A], 7, 0.0);
    let acts = p.handle(voice(A, &format!("vs{A}"), None), t + 1.0);
    assert_eq!(
        kinds(&acts),
        ["CloseGate", "Teardown", "SetQuarantine", "SetRejections"]
    );
    assert_eq!(acts[2], set_quarantine(A, Some(7)));
    assert_eq!(acts[3], set_rejections(A, 1));
    assert_eq!(state_of(&p, t, 0), "quarantine");
    assert!(!p.account_live(A, 1, 1));
    // same incarnation: stays quarantined, no restart
    assert_eq!(p.handle(TICK, t + 10.0), vec![]);
    // next incarnations: displaced again
    t = 20.0;
    let mut acts = vec![];
    for (inc, n) in [(8, 2), (9, 3)] {
        p.handle(ended(1, StopReason::SourceStopped, true), t);
        t = bring_live(&mut p, &[A], inc, t + 1.0);
        acts = p.handle(voice(A, &format!("vs{A}"), Some(CH + 1)), t + 1.0);
        assert!(acts.contains(&set_rejections(A, n)));
    }
    assert_eq!(
        acts.last().unwrap(),
        &set_fatal(A, FatalReason::RelayRejectedByServer)
    );
    assert_eq!(state_of(&p, t, 0), "fatal");
    p.handle(ended(1, StopReason::SourceStopped, true), t + 5.0);
    assert_eq!(p.handle(live(2, 1, 10), t + 6.0), vec![]);
}

#[test]
fn test_ownership_other_session_is_fatal() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(voice(A, "other", Some(CH)), t + 1.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown", "SetFatal"]);
    assert_eq!(
        acts[2],
        set_fatal(A, FatalReason::AccountOwnershipUncertain)
    );
}

#[test]
fn test_healthy_observation_and_joining_exempt() {
    let mut p = make(&[A]);
    p.handle(live(1, 1, 7), 0.0);
    p.handle(joined(A, 1, 1, "vs"), 0.1);
    assert_eq!(p.handle(voice(A, "other", None), 0.2), vec![]); // JOINING: exempt
    p.handle(ready(A, 1, 1, "H264"), 0.3);
    assert_eq!(p.handle(voice(A, "vs", Some(CH)), 0.4), vec![]);
}

#[test]
fn test_auth_rejected_is_fatal_and_blocks_start() {
    let mut p = make(&[A, B]);
    assert_eq!(
        p.handle(PoolEvent::OutAuthRejected { account: A }, 0.0),
        vec![set_fatal(A, FatalReason::AuthRejected)]
    );
    assert_eq!(p.handle(live(1, 1, 7), 1.0), vec![start(B, 1, 1, CH)]);
}

#[test]
fn test_persisted_fatal_not_started() {
    let mut p = make_with(Opts {
        persisted: HashMap::from([(
            A,
            AccountPersisted {
                fatal_reason: Some(FatalReason::AuthRejected),
                quarantine_incarnation: None,
                consecutive_rejections: 0,
            },
        )]),
        ..Opts::default()
    });
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(B, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 5.0), vec![]);
}

#[test]
fn test_backoff_retry_only_while_live() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(failed(A, 1, 1, StopReason::ConnectFailed, true), t + 1.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown"]);
    assert_eq!(p.handle(TICK, t + 1.5), vec![]);
    assert_eq!(p.handle(TICK, t + 2.0), vec![start(A, 1, 2, CH)]);
    // fail again, then the session ends during backoff: no restart
    p.handle(failed(A, 1, 2, StopReason::ConnectFailed, true), t + 3.0);
    p.handle(ended(1, StopReason::HoldExpired, false), t + 3.5);
    assert_eq!(p.handle(TICK, t + 100.0), vec![]);
    assert_eq!(state_of(&p, t, 0), "off");
}

#[test]
fn test_backoff_expiry_after_session_end_does_not_start() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    p.handle(health(A, 1, 1, false), t + 1.0);
    p.handle(ended(1, StopReason::SourceStopped, true), t + 1.2);
    assert_eq!(p.handle(TICK, t + 50.0), vec![]);
}

#[test]
fn test_health_ok_ignored() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    assert_eq!(p.handle(health(A, 1, 1, true), t + 1.0), vec![]);
}

#[test]
fn test_non_retryable_failure_off_until_next_session() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(failed(A, 1, 1, StopReason::ConnectFailed, false), t + 1.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown"]);
    assert_eq!(p.handle(TICK, t + 50.0), vec![]);
    assert_eq!(p.handle(live(1, 2, 7), t + 51.0), vec![]); // same session
    assert_eq!(p.handle(live(2, 1, 7), t + 52.0), vec![start(A, 2, 2, CH)]);
}

#[test]
fn test_stale_generation_ignored() {
    let mut p = make(&[A]);
    p.handle(live(1, 1, 7), 0.0);
    assert_eq!(p.handle(joined(A, 0, 1, "x"), 0.1), vec![]);
    assert_eq!(p.handle(ready(A, 0, 1, "H264"), 0.2), vec![]);
    assert_eq!(
        p.handle(failed(A, 0, 1, StopReason::ConnectFailed, true), 0.3),
        vec![]
    );
    assert_eq!(p.handle(health(A, 0, 1, false), 0.4), vec![]);
    assert_eq!(state_of(&p, 0.4, 0), "joining");
}

#[test]
fn test_new_generation_tears_down_older_bound_accounts_first() {
    let mut p = make(&[A]);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(live(2, 1, 8), t + 3.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown", "Start"]);
    assert_eq!(acts[1], teardown(A, 1, StopReason::SessionInvalid));
    assert_eq!(acts[2], start(A, 2, 2, CH));
}

#[test]
fn test_one_account_failure_leaves_others_live() {
    let mut p = make(&[A, B]);
    let t = bring(&mut p, &[A, B]);
    p.handle(failed(A, 1, 1, StopReason::OutputDead, true), t + 1.0);
    assert!(!p.account_live(A, 1, 1));
    assert!(p.account_live(B, 1, 1));
    p.handle(voice(A, "x", None), t + 2.0); // A has no voice: ignored
    assert!(p.account_live(B, 1, 1));
}

#[test]
fn test_mirror_account_not_displaced_in_its_channel() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    assert_eq!(p.channel_of(A), Some(CH));
    assert_eq!(p.channel_of(B), Some(MCH));
    let t = bring(&mut p, &[A, B]);
    assert_eq!(
        p.handle(voice(B, &format!("vs{B}"), Some(MCH)), t + 1.0),
        vec![]
    );
    assert_eq!(
        p.handle(voice(A, &format!("vs{A}"), Some(CH)), t + 1.0),
        vec![]
    );
    let acts = p.handle(voice(B, &format!("vs{B}"), Some(CH)), t + 2.0); // pulled elsewhere
    assert!(acts.contains(&set_quarantine(B, Some(7))));
}

#[test]
fn test_start_carries_assigned_channel() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 2.0), vec![start(B, 1, 1, MCH)]);
}

#[test]
fn test_set_routing_restarts_only_moved_account() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    let acts = p.set_routing(routing(CH, &[(MCH2, B)]), t);
    assert_eq!(
        acts,
        vec![
            close(B, "routing_changed"),
            teardown(B, 1, StopReason::RoutingChanged)
        ]
    );
    assert_eq!(p.handle(TICK, t + 2.0), vec![start(B, 1, 2, MCH2)]);
    let pool_ready = p.handle(joined(B, 1, 2, "vsB2"), t + 2.0);
    assert_eq!(pool_ready, vec![]);
    p.handle(ready(B, 1, 2, "H264"), t + 2.5);
    // removing the mirror stops B for good (an unassigned mirror never runs); A untouched
    let acts = p.set_routing(routing(CH, &[]), t + 2.5);
    assert_eq!(
        acts.iter().map(PoolAction::account).collect::<Vec<_>>(),
        [B, B]
    );
    assert_eq!(p.handle(TICK, t + 10.0), vec![]);
    assert_eq!(p.set_routing(routing(CH, &[]), t + 10.0), vec![]);
}

#[test]
fn test_main_channel_change_moves_non_mirror_accounts_next_session() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    assert_eq!(p.set_routing(routing(700, &[(MCH, B)]), t), vec![]);
    assert!(p.account_live(A, 1, 1) && p.account_live(B, 1, 1));
    p.handle(ended(1, StopReason::Disabled, false), t + 1.0);
    let acts = p.handle(live(2, 1, 8), t + 10.0);
    assert_eq!(acts, vec![start(A, 2, 2, 700)]);
    assert_eq!(p.handle(TICK, t + 12.0), vec![start(B, 2, 2, MCH)]);
}

#[test]
fn test_excluded_viewer_in_mirror_stops_only_that_account() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        excluded: vec![555],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    assert_eq!(p.handle(participants(B, &[1, 2], MCH), t + 1.0), vec![]);
    let acts = p.handle(participants(B, &[555], MCH), t + 2.0);
    assert_eq!(
        acts,
        vec![
            close(B, "excluded_viewer"),
            teardown(B, 1, StopReason::ExcludedViewer)
        ]
    );
    assert_eq!(p.handle(TICK, t + 60.0), vec![]); // no backoff restart while the viewer is present
    assert!(p.account_live(A, 1, 1));
    // main channel: supervisor's rule
    assert_eq!(p.handle(participants(A, &[555], MCH), t + 61.0), vec![]);
    assert_eq!(
        p.handle(participants(B, &[], MCH), t + 62.0),
        vec![start(B, 1, 2, MCH)]
    );
}

#[test]
fn test_fatal_mirror_account_keeps_mapping_and_is_unavailable() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        persisted: HashMap::from([(
            B,
            AccountPersisted {
                fatal_reason: Some(FatalReason::AuthRejected),
                quarantine_incarnation: None,
                consecutive_rejections: 0,
            },
        )]),
        ..Opts::default()
    });
    assert_eq!(p.channel_of(B), Some(MCH));
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 10.0), vec![]);
    assert_eq!(p.set_routing(routing(CH, &[(MCH, B)]), 10.0), vec![]);
    assert_eq!(state_of(&p, 10.0, 1), "fatal");
}

#[test]
fn test_stale_channel_participants_ignored_after_mirror_move() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        excluded: vec![555],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    assert_eq!(p.handle(participants(B, &[555], 999), t + 1.0), vec![]);
    // move B to a new channel; a stale event from the old channel must not block it
    let new_ch = 888;
    p.set_routing(routing(CH, &[(new_ch, B)]), t + 1.0);
    let mut acts = p.handle(participants(B, &[555], MCH), t + 2.0);
    acts.extend(p.handle(TICK, t + 4.0));
    assert_eq!(acts, vec![start(B, 1, 2, new_ch)]);
}

#[test]
fn test_events_of_superseded_attempt_in_same_generation_ignored() {
    // mirror then unmirror while the first join is still in flight: same generation, new attempt
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    p.set_routing(routing(CH, &[(MCH2, B)]), t);
    assert_eq!(p.handle(TICK, t + 2.0), vec![start(B, 1, 2, MCH2)]);
    let acts = p.set_routing(routing(CH, &[(MCH, B)]), t + 2.0);
    assert_eq!(kinds(&acts), ["CloseGate", "Teardown"]);
    assert_eq!(p.handle(TICK, t + 4.0), vec![start(B, 1, 3, MCH)]);
    // the MCH2 publish (attempt 2) completes late: neither arms nor goes live
    assert_eq!(p.handle(joined(B, 1, 2, "old"), t + 4.1), vec![]);
    assert_eq!(p.handle(ready(B, 1, 2, "H264"), t + 4.2), vec![]);
    assert_eq!(
        p.handle(failed(B, 1, 2, StopReason::ConnectFailed, true), t + 4.3),
        vec![]
    );
    assert_eq!(p.handle(health(B, 1, 2, false), t + 4.4), vec![]);
    assert!(!p.account_live(B, 1, 1) && state_of(&p, t, 1) == "joining");
    // JOINING: exempt, not displaced
    assert_eq!(p.handle(voice(B, "old", None), t + 4.5), vec![]);
    // the current attempt goes live normally
    p.handle(joined(B, 1, 3, "new"), t + 5.0);
    assert_eq!(
        p.handle(ready(B, 1, 3, "H264"), t + 5.1),
        vec![set_live(B, 1, 1)]
    );
    assert_eq!(p.handle(voice(B, "new", Some(MCH)), t + 5.2), vec![]);
}

#[test]
fn test_set_routing_starts_moved_account_without_waiting_for_tick() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    let acts = p.set_routing(routing(CH, &[(MCH2, B)]), t + 5.0);
    assert_eq!(
        acts,
        vec![
            close(B, "routing_changed"),
            teardown(B, 1, StopReason::RoutingChanged),
            start(B, 1, 2, MCH2),
        ]
    );
}

#[test]
fn test_skipped_generation_is_not_restarted_by_routing_or_unblock() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        excluded: vec![555],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    // non-retryable: wait for next session
    p.handle(failed(B, 1, 1, StopReason::ConnectFailed, false), t);
    assert_eq!(p.set_routing(routing(CH, &[(MCH2, B)]), t + 5.0), vec![]);
    assert_eq!(p.handle(participants(B, &[555], MCH2), t + 6.0), vec![]);
    assert_eq!(p.handle(participants(B, &[], MCH2), t + 7.0), vec![]);
    assert_eq!(p.handle(TICK, t + 20.0), vec![]);
}

#[test]
fn test_unblocked_mirror_account_starts_in_same_event() {
    let mut p = make_with(Opts {
        mirror_ids: vec![B],
        mirrors: vec![(MCH, B)],
        excluded: vec![555],
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    p.handle(participants(B, &[555], MCH), t + 1.0);
    assert_eq!(
        p.handle(participants(B, &[], MCH), t + 10.0),
        vec![start(B, 1, 2, MCH)]
    );
}

// ---------------------------------------------------------------- relay target and roles

#[test]
fn test_only_first_n_relays_start() {
    let mut p = make(&[A, B, C]);
    assert_eq!(p.set_relay_target(1, 0.0), vec![]); // no session yet
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 2.0), vec![]);
    assert_eq!(p.handle(TICK, 10.0), vec![]);
    assert_eq!(p.relay_summary(), summary(1, 1, 3, 0));
}

#[test]
fn test_raising_n_starts_next_relay_immediately() {
    let mut p = make(&[A, B, C]);
    p.set_relay_target(1, 0.0);
    let t = bring(&mut p, &[A]);
    assert_eq!(p.set_relay_target(2, t + 5.0), vec![start(B, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, t + 20.0), vec![]);
}

#[test]
fn test_lowering_n_stops_last_relay_without_penalty() {
    let mut p = make_with(Opts {
        persisted: HashMap::from([(B, persisted_rejections(1))]),
        ..Opts::default()
    });
    let t = bring(&mut p, &[A, B]);
    p.handle(viewers(A, &[900]), t);
    p.handle(viewers(B, &[]), t);
    // B has had no viewer for 1 s only
    assert_eq!(p.set_relay_target(1, t + 1.0), vec![]);
    let acts = p.handle(TICK, t + 30.0);
    assert_eq!(
        acts,
        vec![
            close(B, "relay_scaled_down"),
            teardown(B, 1, StopReason::RelayScaledDown)
        ]
    );
    let row = &p.status(t + 30.0)[1];
    assert!(
        row.state.as_str() == "off" && row.backoff_in.is_none() && row.consecutive_rejections == 1
    );
    assert!(p.account_live(A, 1, 1));
    assert_eq!(p.handle(TICK, t + 100.0), vec![]);
    assert_eq!(p.relay_summary(), summary(1, 1, 2, 0));
    // raising again restarts B in the same session
    assert_eq!(p.set_relay_target(2, t + 101.0), vec![start(B, 1, 2, CH)]);
}

#[test]
fn test_fatal_relay_replaced_by_next() {
    let mut p = make(&[A, B, C]);
    p.set_relay_target(2, 0.0);
    let t = bring(&mut p, &[A, B]);
    let acts = p.handle(PoolEvent::OutAuthRejected { account: A }, t + 5.0);
    assert_eq!(
        acts,
        vec![
            close(A, "auth_rejected"),
            teardown(A, 1, StopReason::AuthRejected),
            set_fatal(A, FatalReason::AuthRejected),
            start(C, 1, 1, CH),
        ]
    );
    assert_eq!(p.relay_summary(), summary(2, 2, 2, 0));
    p.handle(PoolEvent::OutAuthRejected { account: B }, t + 10.0);
    assert_eq!(p.relay_summary(), summary(2, 1, 1, 1));
}

#[test]
fn test_quarantined_relay_replaced_by_next() {
    let mut p = make(&[A, B]);
    p.set_relay_target(1, 0.0);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(voice(A, &format!("vs{A}"), None), t + 5.0);
    assert!(acts.contains(&set_quarantine(A, Some(7))));
    assert_eq!(acts.last().unwrap(), &start(B, 1, 1, CH));
}

#[test]
fn test_relay_failed_for_session_replaced_by_next() {
    let mut p = make(&[A, B]);
    p.set_relay_target(1, 0.0);
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    p.handle(failed(A, 1, 1, StopReason::ConfigInvalid, false), 0.5);
    // after the start stagger
    assert_eq!(p.handle(TICK, 2.0), vec![start(B, 1, 1, CH)]);
    assert_eq!(p.relay_summary(), summary(1, 1, 1, 0));
    p.set_relay_target(2, 1.0);
    assert_eq!(p.relay_summary(), summary(2, 1, 1, 1));
    // next session A gets a fresh chance
    p.handle(ended(1, StopReason::Shutdown, false), 2.0);
    assert_eq!(p.handle(live(2, 2, 7), 10.0), vec![start(A, 2, 2, CH)]);
}

#[test]
fn test_backoff_relay_replaced_and_live_relay_kept() {
    let mut p = make(&[A, B]);
    p.set_relay_target(1, 0.0);
    let t = bring(&mut p, &[A]);
    let acts = p.handle(health(A, 1, 1, false), t + 5.0);
    assert_eq!(acts.last().unwrap(), &start(B, 1, 1, CH));
    assert_eq!(p.relay_summary().short_by, 0);
    p.handle(joined(B, 1, 1, &format!("vs{B}")), t + 5.5);
    p.handle(ready(B, 1, 1, "H264"), t + 5.5);
    // A's backoff expires: B stays, A is not restarted
    assert_eq!(p.handle(TICK, t + 10.0), vec![]);
    assert!(p.account_live(B, 1, 1));
    // scale 2 -> 1 while A is in backoff keeps the live B
    assert_eq!(p.set_relay_target(2, t + 11.0), vec![start(A, 1, 2, CH)]);
    p.handle(health(A, 1, 2, false), t + 11.5);
    assert_eq!(state_of(&p, t + 11.5, 0), "backoff");
    assert_eq!(p.set_relay_target(1, t + 12.0), vec![]);
    assert!(p.account_live(B, 1, 1));
}

#[test]
fn test_unassigned_mirror_never_starts() {
    let mut p = make_with(Opts {
        ids: vec![A, B],
        mirror_ids: vec![B],
        excluded: vec![555],
        ..Opts::default()
    });
    assert_eq!(p.channel_of(B), None);
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 10.0), vec![]);
    assert_eq!(p.handle(participants(B, &[], MCH), 11.0), vec![]);
    assert_eq!(p.handle(voice(B, "x", Some(MCH)), 12.0), vec![]);
    assert_eq!(p.set_routing(routing(700, &[]), 13.0), vec![]);
    assert_eq!(state_of(&p, 13.0, 1), "off");
}

#[test]
fn test_relay_never_routed_to_mirror_channel() {
    let mut p = make_with(Opts {
        ids: vec![A, B],
        mirrors: vec![(MCH, A)],
        excluded: vec![555],
        ..Opts::default()
    });
    assert_eq!(p.channel_of(A), Some(CH));
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.handle(TICK, 2.0), vec![start(B, 1, 1, CH)]);
    assert_eq!(p.set_routing(routing(CH, &[(MCH2, A)]), 3.0), vec![]);
    assert_eq!(p.handle(participants(A, &[555], MCH2), 4.0), vec![]);
}

#[test]
fn test_excluded_viewer_blocks_relays_via_session() {
    let mut p = make(&[A, B]);
    assert_eq!(p.set_relay_target(2, 0.0), vec![]);
    assert_eq!(p.handle(TICK, 5.0), vec![]);
    let t = bring_live(&mut p, &[A, B], 7, 10.0);
    let acts = p.handle(ended(1, StopReason::ExcludedViewer, false), t + 1.0);
    assert_eq!(
        kinds(&acts),
        ["CloseGate", "Teardown", "CloseGate", "Teardown"]
    );
    assert_eq!(p.set_relay_target(2, t + 2.0), vec![]);
    assert_eq!(p.set_relay_target(1, t + 3.0), vec![]);
    assert_eq!(p.handle(TICK, t + 50.0), vec![]);
}

#[test]
fn test_surplus_relay_with_viewers_stays_and_zero_viewer_one_stops_instead() {
    let mut p = make(&[A, B]);
    let t = bring(&mut p, &[A, B]);
    p.handle(viewers(A, &[]), t);
    p.handle(viewers(B, &[900]), t);
    assert_eq!(p.set_relay_target(1, t + 1.0), vec![]);
    assert_eq!(p.status(t + 1.0)[0].stop_in, Some(29.0));
    assert_eq!(p.status(t + 1.0)[1].stop_in, None);
    assert_eq!(p.handle(TICK, t + 29.0), vec![]);
    // A has had no viewer for 30 s: it stops although it comes first in config order
    assert_eq!(
        p.handle(TICK, t + 30.0),
        vec![
            close(A, "relay_scaled_down"),
            teardown(A, 1, StopReason::RelayScaledDown)
        ]
    );
    assert_eq!(p.handle(TICK, t + 1000.0), vec![]);
    assert!(p.account_live(B, 1, 1));
}

#[test]
fn test_surplus_relay_kept_while_watched_and_stopped_after_idle() {
    let mut p = make(&[A, B]);
    let t = bring(&mut p, &[A, B]);
    p.handle(viewers(A, &[900]), t);
    p.handle(viewers(B, &[901]), t);
    assert_eq!(p.set_relay_target(1, t + 1.0), vec![]);
    assert_eq!(p.relay_summary().kept, 1);
    // the later one in config order is the extra
    assert_eq!(p.relay_summary().kept_ids, vec![B]);
    assert_eq!(p.handle(TICK, t + 500.0), vec![]);
    p.handle(viewers(B, &[]), t + 500.0);
    assert_eq!(p.relay_summary().kept, 0);
    // someone came back: the idle timer restarts
    p.handle(viewers(B, &[901]), t + 520.0);
    p.handle(viewers(B, &[]), t + 525.0);
    assert_eq!(p.handle(TICK, t + 554.0), vec![]);
    assert_eq!(p.status(t + 554.0)[1].viewers, Some(0));
    assert_eq!(
        p.handle(TICK, t + 555.0),
        vec![
            close(B, "relay_scaled_down"),
            teardown(B, 1, StopReason::RelayScaledDown)
        ]
    );
    assert_eq!(p.status(t + 555.0)[1].viewers, None);
}

#[test]
fn test_target_watching_a_relay_is_not_a_viewer() {
    let mut p = make(&[A, B]);
    let t = bring(&mut p, &[A, B]);
    p.handle(viewers(A, &[900]), t);
    p.handle(viewers(B, &[SRC]), t);
    assert_eq!(p.status(t)[1].viewers, Some(0));
    p.set_relay_target(1, t);
    assert_eq!(kinds(&p.handle(TICK, t + 30.0)), ["CloseGate", "Teardown"]);
}

#[test]
fn test_unknown_viewers_count_as_watched_for_four_idle_periods() {
    let mut p = make(&[A, B]);
    let t = bring(&mut p, &[A, B]); // B went live at t and never reports viewers
    p.handle(viewers(A, &[900]), t);
    assert_eq!(p.status(t)[1].viewers, None);
    assert_eq!(p.set_relay_target(1, t + 1.0), vec![]);
    assert_eq!(p.handle(TICK, t + 119.0), vec![]);
    assert_eq!(
        p.handle(TICK, t + 120.0),
        vec![
            close(B, "relay_scaled_down"),
            teardown(B, 1, StopReason::RelayScaledDown)
        ]
    );
}

#[test]
fn test_starting_relay_is_not_cut_and_queued_one_is_dropped() {
    let mut p = make(&[A, B]);
    p.set_relay_target(2, 0.0);
    // B waits for the stagger
    assert_eq!(p.handle(live(1, 1, 7), 0.0), vec![start(A, 1, 1, CH)]);
    assert_eq!(p.set_relay_target(0, 0.5), vec![]);
    assert_eq!(p.handle(TICK, 3.0), vec![]); // B never starts
    assert_eq!(state_of(&p, 3.0, 0), "joining");
}

#[test]
fn test_kept_relay_counts_toward_a_higher_target() {
    let mut p = make(&[A, B, C]);
    p.set_relay_target(2, 0.0);
    let t = bring(&mut p, &[A, B]);
    p.handle(viewers(A, &[900]), t);
    p.handle(viewers(B, &[901]), t);
    assert_eq!(p.set_relay_target(1, t + 1.0), vec![]);
    assert_eq!(p.set_relay_target(2, t + 2.0), vec![]); // B already runs
    assert_eq!(p.relay_summary().kept, 0);
    assert_eq!(p.set_relay_target(3, t + 3.0), vec![start(C, 1, 1, CH)]);
}

#[test]
fn test_status_has_no_stop_in_for_a_queued_relay() {
    let mut p = make(&[A, B]);
    p.set_relay_target(2, 0.0);
    p.handle(live(1, 1, 7), 0.0); // A starts, B waits for the stagger
    // Python pokes `_relay_target = 1` directly; the public call leaves B queued the same way
    p.set_relay_target(1, 0.0);
    assert!(state_of(&p, 0.0, 1) == "off" && p.status(0.0)[1].stop_in.is_none());
}

#[test]
fn test_zero_viewer_clock_starts_only_once_the_relay_is_live() {
    let mut p = make(&[A, B]);
    p.set_relay_target(2, 0.0);
    let t = bring(&mut p, &[A]);
    p.handle(viewers(A, &[900]), t);
    let acts = p.handle(TICK, t + 2.0);
    let n = attempt_of(&acts, B).unwrap();
    p.handle(joined(B, 1, n, &format!("vs{B}")), t + 2.0);
    p.handle(viewers(B, &[]), t + 2.0); // empty set arrives while B still joins
    p.set_relay_target(1, t + 3.0);
    assert_eq!(p.handle(TICK, t + 1000.0), vec![]);
    assert_eq!(p.status(t + 1000.0)[1].stop_in, None);
    p.handle(ready(B, 1, n, "H264"), t + 1000.0);
    assert_eq!(p.handle(TICK, t + 1029.0), vec![]);
    assert_eq!(
        kinds(&p.handle(TICK, t + 1030.0)),
        ["CloseGate", "Teardown"]
    );
}

#[test]
fn test_kept_counts_only_relays_with_known_viewers() {
    let mut p = make(&[A, B]);
    p.set_relay_target(2, 0.0);
    let t = bring(&mut p, &[A]);
    p.handle(viewers(A, &[900]), t);
    let acts = p.handle(TICK, t + 2.0);
    let n = attempt_of(&acts, B).unwrap();
    p.handle(joined(B, 1, n, &format!("vs{B}")), t + 2.0); // B is still joining: no viewer data
    p.set_relay_target(1, t + 3.0);
    assert_eq!(p.relay_summary().kept, 0);
    p.handle(ready(B, 1, n, "H264"), t + 4.0);
    p.handle(viewers(B, &[901]), t + 4.0);
    assert_eq!(p.relay_summary().kept, 1);
    assert_eq!(p.relay_summary().kept_ids, vec![B]);
}

#[test]
fn test_pool_adaptive_accepts_av1_ready() {
    let mut p = make_with(Opts {
        ids: vec![A],
        adaptive: true,
        ..Opts::default()
    });
    p.handle(live(1, 1, 7), 0.0);
    p.handle(joined(A, 1, 1, "vs"), 0.1);
    p.handle(ready(A, 1, 1, "AV1"), 0.2);
    assert_ne!(state_of(&p, 0.2, 0), "backoff");
    assert!(p.is_current(A, 1, 1) && !p.is_current(A, 1, 2));
    p.handle(joined(A, 1, 1, "vs"), 0.3);
    let mut q = make_with(Opts {
        ids: vec![A],
        adaptive: true,
        ..Opts::default()
    });
    q.handle(live(1, 1, 7), 0.0);
    q.handle(joined(A, 1, 1, "vs"), 0.1);
    assert_eq!(
        kinds(&q.handle(ready(A, 1, 1, "FOO"), 0.2)),
        ["CloseGate", "Teardown"]
    );
}

#[test]
fn account_view_and_set_account_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::new(tmp.path());
    assert_eq!(
        account_view(&store.load().unwrap(), 7),
        persisted_rejections(0)
    );
    store
        .update(|s| {
            set_account(s, 7, |r| {
                r.quarantine_incarnation = Some(3);
                r.consecutive_rejections = 1;
            })
        })
        .unwrap();
    let latch = json!({"reason": "AUTH_REJECTED", "detail": "", "at": 1.0});
    store
        .update(|s| set_account(s, 7, |r| r.fatal_latch = latch.as_object().cloned()))
        .unwrap();
    let st = StateStore::new(tmp.path()).load().unwrap();
    assert_eq!(
        account_view(&st, 7),
        AccountPersisted {
            fatal_reason: Some(FatalReason::AuthRejected),
            quarantine_incarnation: Some(3),
            consecutive_rejections: 1,
        }
    );
    assert_eq!(account_view(&st, 8), persisted_rejections(0));
}
