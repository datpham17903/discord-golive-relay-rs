//! OutputPool: pure state machine for extra output accounts (see `outputs_model.rs`). Port of
//! `supervisor/outputs.py`.
//!
//! `handle(event, now)` returns actions for the service to execute in order. The pool follows the
//! main session through SessionLive/SessionHold/SessionEnded and never performs I/O. Close always
//! precedes teardown.
//!
//! The Python code mutates account records through aliases while iterating; here every helper that
//! needs the whole pool takes the account's `uid`, and loops iterate over a snapshot of the uids.

use std::collections::{HashMap, HashSet, VecDeque};

use indexmap::IndexMap;
use relay_media::codec::CODEC_ORDER;

use crate::config::AccountRole;

use super::model::{FatalReason, RelayVoiceObserved, StopReason};
use super::outputs_model::{AccountPersisted, OutputAccount, OutputState, PoolAction, PoolEvent};
use super::ownership::{OwnershipDetector, OwnershipVerdict};
use super::retry::RetryPolicy;
use super::routing::Routing;

#[derive(Debug, Clone, Copy)]
struct Session {
    generation: u64,
    epoch: u64,
    incarnation: i64,
    holding: bool,
}

#[derive(Debug)]
struct Acct {
    account: OutputAccount,
    retry: RetryPolicy,
    state: OutputState,
    fatal: Option<FatalReason>,
    quarantine_inc: Option<i64>,
    rejections: i64,
    /// Session generation this account is bound to.
    generation: Option<u64>,
    incarnation: Option<i64>,
    /// Per-account `Start` counter; events carry it back.
    attempt: u64,
    backoff_until: Option<f64>,
    /// Non-retryable failure: wait for the next session.
    skip_generation: Option<u64>,
    /// Incarnation whose displacement was already counted.
    counted_inc: Option<i64>,
    streamed_inc: Option<i64>,
    queued: bool,
    /// An excluded viewer is in this mirror channel.
    blocked: bool,
    /// Go Live created (READY) in the current run.
    live_at: Option<f64>,
    /// Watchers of the current Go Live; `None` until the client reports them.
    viewers: Option<HashSet<u64>>,
    /// Viewers became empty.
    zero_since: Option<f64>,
    detector: OwnershipDetector,
}

impl Acct {
    fn uid(&self) -> u64 {
        self.account.user_id
    }

    fn is_mirror(&self) -> bool {
        self.account.role == AccountRole::Mirror
    }

    fn active(&self) -> bool {
        matches!(
            self.state,
            OutputState::Joining | OutputState::Ready | OutputState::Live
        )
    }

    fn usable(&self) -> bool {
        !matches!(self.state, OutputState::Fatal | OutputState::Quarantine)
    }

    /// Close the gate, then tear down; the account becomes unbound (caller sets the final state).
    fn stop(&mut self, why: &'static str, reason: StopReason, out: &mut Vec<PoolAction>) {
        out.push(PoolAction::CloseGate {
            account: self.uid(),
            reason: why,
        });
        out.push(PoolAction::Teardown {
            account: self.uid(),
            generation: self
                .generation
                .expect("a stopped account is bound to a generation"),
            reason,
        });
        self.detector.disarm();
        self.generation = None;
        self.state = OutputState::Off;
        self.live_at = None;
        self.viewers = None;
        self.zero_since = None;
    }

    fn backoff(&mut self, reason: StopReason, now: f64, out: &mut Vec<PoolAction>) {
        self.stop(reason.as_str(), reason, out);
        self.state = OutputState::Backoff;
        self.backoff_until = Some(now + self.retry.next_delay());
    }

    fn set_fatal(&mut self, reason: FatalReason, detail: &str, out: &mut Vec<PoolAction>) {
        self.fatal = Some(reason);
        self.state = OutputState::Fatal;
        out.push(PoolAction::SetFatal {
            account: self.uid(),
            reason,
            detail: detail.to_string(),
        });
    }

    /// When an extra relay may stop; `None` while it has viewers, or may have them (still
    /// connecting, or no viewer data yet within four idle periods of going live).
    fn stop_at(&self, idle_stop: f64) -> Option<f64> {
        if !self.active() {
            return Some(f64::NEG_INFINITY); // only queued: nothing to cut
        }
        let live_at = self.live_at?;
        let Some(viewers) = &self.viewers else {
            return Some(live_at + 4.0 * idle_stop);
        };
        if !viewers.is_empty() {
            return None;
        }
        let zero_since = self.zero_since?;
        Some(zero_since.max(live_at) + idle_stop)
    }
}

/// Counts for the status line (Python `relay_summary` dict).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelaySummary {
    pub wanted: usize,
    pub running: usize,
    pub available: usize,
    pub short_by: usize,
    /// Extra relays that stay because they have (or may still have) viewers.
    pub kept: usize,
    pub kept_ids: Vec<u64>,
}

/// One row of `OutputPool::status` (Python dict).
#[derive(Debug, Clone, PartialEq)]
pub struct OutputStatus {
    pub account: u64,
    pub state: OutputState,
    pub generation: Option<u64>,
    pub backoff_in: Option<f64>,
    pub fatal: Option<FatalReason>,
    pub quarantine_incarnation: Option<i64>,
    pub consecutive_rejections: i64,
    pub viewers: Option<usize>,
    pub stop_in: Option<f64>,
}

/// Keyword arguments of the Python constructor, with its defaults.
pub struct PoolConfig {
    pub stagger_s: f64,
    pub rejection_limit: i64,
    pub routing: Routing,
    pub excluded_viewer_ids: HashSet<u64>,
    pub relay_idle_stop_s: f64,
    pub adaptive: bool,
}

impl PoolConfig {
    pub fn new(routing: Routing) -> Self {
        Self {
            stagger_s: 2.0,
            rejection_limit: 3,
            routing,
            excluded_viewer_ids: HashSet::new(),
            relay_idle_stop_s: 30.0,
            adaptive: false,
        }
    }
}

pub struct OutputPool {
    adaptive: bool,
    stagger: f64,
    idle_stop: f64,
    limit: i64,
    routing: Routing,
    excluded: HashSet<u64>,
    accts: IndexMap<u64, Acct>,
    session: Option<Session>,
    queue: VecDeque<u64>,
    next_start: f64,
    /// Config order = replacement order.
    relays: Vec<u64>,
    relay_target: usize,
}

impl OutputPool {
    /// `retry` builds one `RetryPolicy` per account.
    pub fn new(
        accounts: &[OutputAccount],
        persisted: &HashMap<u64, AccountPersisted>,
        retry: impl Fn() -> RetryPolicy,
        cfg: PoolConfig,
    ) -> Self {
        let mut accts = IndexMap::new();
        for a in accounts {
            let p = persisted
                .get(&a.user_id)
                .cloned()
                .unwrap_or(AccountPersisted {
                    fatal_reason: None,
                    quarantine_incarnation: None,
                    consecutive_rejections: 0,
                });
            let state = if p.fatal_reason.is_some() {
                OutputState::Fatal
            } else if p.quarantine_incarnation.is_some() {
                OutputState::Quarantine
            } else {
                OutputState::Off
            };
            accts.insert(
                a.user_id,
                Acct {
                    account: a.clone(),
                    retry: retry(),
                    state,
                    fatal: p.fatal_reason,
                    quarantine_inc: p.quarantine_incarnation,
                    rejections: p.consecutive_rejections,
                    generation: None,
                    incarnation: None,
                    attempt: 0,
                    backoff_until: None,
                    skip_generation: None,
                    counted_inc: None,
                    streamed_inc: None,
                    queued: false,
                    blocked: false,
                    live_at: None,
                    viewers: None,
                    zero_since: None,
                    detector: OwnershipDetector::new(),
                },
            );
        }
        let relays: Vec<u64> = accounts
            .iter()
            .filter(|a| a.role != AccountRole::Mirror)
            .map(|a| a.user_id)
            .collect();
        Self {
            adaptive: cfg.adaptive,
            stagger: cfg.stagger_s,
            idle_stop: cfg.relay_idle_stop_s,
            limit: cfg.rejection_limit,
            routing: cfg.routing,
            excluded: cfg.excluded_viewer_ids,
            accts,
            session: None,
            queue: VecDeque::new(),
            next_start: f64::NEG_INFINITY,
            relay_target: relays.len(),
            relays,
        }
    }

    // ------------------------------------------------------------ queries

    /// Relay accounts publish in the main channel; a mirror account in its assigned channel, or
    /// `None` if unassigned.
    pub fn channel_of(&self, uid: u64) -> Option<u64> {
        match self.accts.get(&uid) {
            Some(rec) if rec.is_mirror() => self
                .routing
                .mirrors
                .iter()
                .find(|(_, acct)| **acct == uid)
                .map(|(ch, _)| *ch),
            _ => Some(self.routing.channel_id),
        }
    }

    /// Restart only the mirror accounts whose assignment moved them; a main-channel change waits for
    /// the next session.
    pub fn set_routing(&mut self, routing: Routing, now: f64) -> Vec<PoolAction> {
        let mut out = Vec::new();
        let uids = self.uids();
        let before: Vec<Option<u64>> = uids.iter().map(|&u| self.channel_of(u)).collect();
        self.routing = routing;
        for (uid, before) in uids.into_iter().zip(before) {
            if !self.rec(uid).is_mirror() || before == self.channel_of(uid) {
                continue;
            }
            self.rec_mut(uid).blocked = false;
            if self.rec(uid).active() {
                self.rec_mut(uid)
                    .stop("routing_changed", StopReason::RoutingChanged, &mut out);
            }
            if self.restartable(uid) && self.allowed(uid, now) {
                self.enqueue(uid);
            }
        }
        self.pump(now, &mut out);
        out
    }

    /// `n` usable relay accounts should run. More start at once; an extra running relay stops
    /// cleanly (no backoff, no rejection) only once its Go Live has had no viewer for
    /// `relay_idle_stop_s`.
    pub fn set_relay_target(&mut self, n: usize, now: f64) -> Vec<PoolAction> {
        let mut out = Vec::new();
        self.relay_target = n;
        self.reconcile_relays(now, &mut out);
        self.pump(now, &mut out);
        out
    }

    pub fn relay_summary(&self) -> RelaySummary {
        let available = self
            .relays
            .iter()
            .filter(|&&u| self.relay_candidate(u))
            .count();
        let running = self
            .relays
            .iter()
            .filter(|&&u| self.rec(u).active())
            .count();
        let waiting: HashSet<u64> = self
            .surplus_stops()
            .into_iter()
            .map(|(u, _)| u)
            .filter(|&u| self.rec(u).active())
            .collect();
        // relays with no known viewers (still joining) are not "kept because watched"
        let has_viewers = |u: u64| self.rec(u).viewers.as_ref().is_some_and(|v| !v.is_empty());
        let unknown = self
            .relays
            .iter()
            .filter(|&&u| self.rec(u).active() && !waiting.contains(&u) && !has_viewers(u))
            .count();
        let kept =
            (running as i64 - self.relay_target as i64 - waiting.len() as i64 - unknown as i64)
                .max(0) as usize;
        // the watched ones that would stop first, the last in config order (the tie order of
        // `surplus_stops`)
        let watched: Vec<u64> = self
            .running_relays()
            .into_iter()
            .filter(|&u| self.rec(u).active() && !waiting.contains(&u) && has_viewers(u))
            .collect();
        RelaySummary {
            wanted: self.relay_target,
            running,
            available,
            short_by: self.relay_target.saturating_sub(available),
            kept,
            kept_ids: if kept > 0 {
                watched[watched.len().saturating_sub(kept)..].to_vec()
            } else {
                Vec::new()
            },
        }
    }

    pub fn is_current(&self, account: u64, generation: u64, attempt: u64) -> bool {
        self.current(account, generation, attempt).is_some()
    }

    pub fn account_live(&self, account: u64, generation: u64, epoch: u64) -> bool {
        let Some(rec) = self.accts.get(&account) else {
            return false;
        };
        rec.state == OutputState::Live
            && rec.generation == Some(generation)
            && self
                .session
                .is_some_and(|s| !s.holding && s.generation == generation && s.epoch == epoch)
    }

    pub fn status(&self, now: f64) -> Vec<OutputStatus> {
        let stops: HashMap<u64, f64> = self.surplus_stops().into_iter().collect();
        self.accts
            .values()
            .map(|r| OutputStatus {
                account: r.uid(),
                state: r.state,
                generation: r.generation,
                backoff_in: r.backoff_until.map(|b| (b - now).max(0.0)),
                fatal: r.fatal,
                quarantine_incarnation: r.quarantine_inc,
                consecutive_rejections: r.rejections,
                viewers: r.viewers.as_ref().map(HashSet::len),
                stop_in: match stops.get(&r.uid()) {
                    Some(at) if r.active() => Some((at - now).max(0.0)),
                    _ => None,
                },
            })
            .collect()
    }

    // ------------------------------------------------------------ events

    pub fn handle(&mut self, event: PoolEvent, now: f64) -> Vec<PoolAction> {
        let mut out = Vec::new();
        match event {
            PoolEvent::SessionLive {
                generation,
                epoch,
                incarnation,
            } => self.on_live(generation, epoch, incarnation, now, &mut out),
            PoolEvent::SessionHold { generation, .. } => self.on_hold(generation, &mut out),
            PoolEvent::SessionEnded {
                generation,
                reason,
                incarnation_ended,
            } => self.on_ended(generation, reason, incarnation_ended, &mut out),
            PoolEvent::OutJoined {
                account,
                generation,
                attempt,
                voice_session_id,
            } => {
                if let Some(uid) = self.current(account, generation, attempt) {
                    let rec = self.rec_mut(uid);
                    if rec.state == OutputState::Joining {
                        rec.detector.arm(voice_session_id);
                    }
                }
            }
            PoolEvent::OutReady {
                account,
                generation,
                attempt,
                negotiated_video_codec,
            } => self.on_ready(
                account,
                generation,
                attempt,
                negotiated_video_codec.as_deref(),
                now,
                &mut out,
            ),
            PoolEvent::OutFailed {
                account,
                generation,
                attempt,
                reason,
                retryable,
                ..
            } => {
                if let Some(uid) = self.current(account, generation, attempt) {
                    let rec = self.rec_mut(uid);
                    if rec.active() {
                        if retryable {
                            rec.backoff(reason, now, &mut out);
                        } else {
                            rec.skip_generation = rec.generation;
                            rec.stop("failed", reason, &mut out);
                        }
                    }
                }
            }
            PoolEvent::OutHealth {
                account,
                generation,
                attempt,
                ok,
            } => {
                if let Some(uid) = self.current(account, generation, attempt) {
                    let rec = self.rec_mut(uid);
                    if !ok && rec.active() {
                        rec.backoff(StopReason::OutputDead, now, &mut out);
                    }
                }
            }
            PoolEvent::OutVoiceObserved {
                account,
                session_id,
                channel_id,
            } => self.on_voice(account, session_id, channel_id, now, &mut out),
            PoolEvent::MirrorParticipants {
                account,
                participants,
                channel_id,
            } => self.on_participants(account, &participants, channel_id, &mut out),
            PoolEvent::OutViewers { account, viewers } => self.on_viewers(account, viewers, now),
            PoolEvent::OutAuthRejected { account } => {
                if let Some(rec) = self.accts.get_mut(&account) {
                    if rec.active() {
                        rec.stop("auth_rejected", StopReason::AuthRejected, &mut out);
                    }
                    rec.set_fatal(FatalReason::AuthRejected, "", &mut out);
                }
            }
            PoolEvent::OutCodecChanged { .. } | PoolEvent::OutPli { .. } | PoolEvent::OutTick => {}
        }
        self.reconcile_relays(now, &mut out);
        self.pump(now, &mut out);
        out
    }

    // ------------------------------------------------------------ helpers

    fn uids(&self) -> Vec<u64> {
        self.accts.keys().copied().collect()
    }

    /// Record of an account known to the pool (uids come from `accts` or `relays`).
    fn rec(&self, uid: u64) -> &Acct {
        &self.accts[&uid]
    }

    fn rec_mut(&mut self, uid: u64) -> &mut Acct {
        self.accts
            .get_mut(&uid)
            .expect("uid taken from the account table")
    }

    fn current(&self, account: u64, generation: u64, attempt: u64) -> Option<u64> {
        let rec = self.accts.get(&account)?;
        (rec.generation == Some(generation) && rec.attempt == attempt).then_some(account)
    }

    fn restartable(&self, uid: u64) -> bool {
        let rec = self.rec(uid);
        self.session.is_some_and(|s| {
            rec.state == OutputState::Off && rec.skip_generation != Some(s.generation)
        })
    }

    /// A relay in BACKOFF or failed for this session cannot hold a slot, so the next relay takes over.
    fn relay_candidate(&self, uid: u64) -> bool {
        let rec = self.rec(uid);
        rec.usable()
            && rec.state != OutputState::Backoff
            && !self
                .session
                .is_some_and(|s| rec.skip_generation == Some(s.generation))
    }

    fn running_relays(&self) -> Vec<u64> {
        self.relays
            .iter()
            .copied()
            .filter(|&u| self.relay_candidate(u) && (self.rec(u).active() || self.rec(u).queued))
            .collect()
    }

    /// Extra relays chosen to stop, earliest first, with the time each may stop: only relays without
    /// viewers, in any position (the last one first on a tie).
    fn surplus_stops(&self) -> Vec<(u64, f64)> {
        let running = self.running_relays();
        let excess = running.len().saturating_sub(self.relay_target);
        if excess == 0 {
            return Vec::new();
        }
        let mut picks: Vec<(f64, i64, u64)> = running
            .iter()
            .enumerate()
            .filter_map(|(i, &u)| {
                self.rec(u)
                    .stop_at(self.idle_stop)
                    .map(|at| (at, -(i as i64), u))
            })
            .collect();
        picks.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .expect("stop times are not NaN")
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
        });
        picks
            .into_iter()
            .take(excess)
            .map(|(at, _, u)| (u, at))
            .collect()
    }

    /// Running (or queued) relays keep their slot unless they are extra and due to stop; idle ones
    /// fill up to the target in config order.
    fn wanted_relays(&self, now: f64) -> Vec<u64> {
        let running = self.running_relays();
        let due: HashSet<u64> = self
            .surplus_stops()
            .into_iter()
            .filter(|&(_, at)| at <= now)
            .map(|(u, _)| u)
            .collect();
        let mut keep: Vec<u64> = running
            .iter()
            .copied()
            .filter(|u| !due.contains(u))
            .collect();
        let room = self.relay_target.saturating_sub(keep.len());
        let idle = self
            .relays
            .iter()
            .copied()
            .filter(|&u| self.relay_candidate(u) && !running.contains(&u))
            .take(room);
        keep.extend(idle);
        keep
    }

    /// A mirror account runs only while assigned; a relay account only within the wanted relays.
    fn allowed(&self, uid: u64, now: f64) -> bool {
        if self.rec(uid).is_mirror() {
            return self.channel_of(uid).is_some();
        }
        self.wanted_relays(now).contains(&uid)
    }

    fn reconcile_relays(&mut self, now: f64, out: &mut Vec<PoolAction>) {
        let wanted: HashSet<u64> = self.wanted_relays(now).into_iter().collect();
        for uid in self.relays.clone() {
            if wanted.contains(&uid) {
                if self.restartable(uid) {
                    self.enqueue(uid);
                }
            } else if self.rec(uid).active() {
                self.rec_mut(uid)
                    .stop("relay_scaled_down", StopReason::RelayScaledDown, out);
            }
        }
    }

    fn enqueue(&mut self, uid: u64) {
        let rec = self.rec_mut(uid);
        if !rec.queued {
            rec.queued = true;
            self.queue.push_back(uid);
        }
    }

    fn on_live(
        &mut self,
        generation: u64,
        epoch: u64,
        incarnation: i64,
        now: f64,
        out: &mut Vec<PoolAction>,
    ) {
        let prev = self.session;
        self.session = Some(Session {
            generation,
            epoch,
            incarnation,
            holding: false,
        });
        for uid in self.uids() {
            let rec = self.rec_mut(uid);
            if rec.generation.is_some() && rec.generation != Some(generation) {
                rec.stop("session_replaced", StopReason::SessionInvalid, out);
            } else if rec.generation == Some(generation)
                && matches!(rec.state, OutputState::Ready | OutputState::Live)
            {
                rec.state = OutputState::Live;
                out.push(PoolAction::SetLive {
                    account: uid,
                    generation,
                    epoch,
                });
            }
            if rec.state == OutputState::Backoff && prev.is_none_or(|p| p.generation != generation)
            {
                rec.state = OutputState::Off;
                rec.backoff_until = None;
            }
            if rec.state == OutputState::Quarantine && rec.quarantine_inc != Some(incarnation) {
                rec.quarantine_inc = None;
                rec.state = OutputState::Off;
                out.push(PoolAction::SetQuarantine {
                    account: uid,
                    incarnation: None,
                });
            }
            let rec = self.rec(uid);
            if rec.state == OutputState::Off
                && rec.skip_generation != Some(generation)
                && self.allowed(uid, now)
            {
                self.enqueue(uid);
            }
        }
    }

    fn on_hold(&mut self, generation: u64, out: &mut Vec<PoolAction>) {
        let Some(s) = self.session.as_mut().filter(|s| s.generation == generation) else {
            return;
        };
        s.holding = true;
        for rec in self.accts.values() {
            if rec.generation == Some(generation) && rec.state == OutputState::Live {
                out.push(PoolAction::Slate {
                    account: rec.uid(),
                    generation,
                });
            }
        }
    }

    fn on_ended(
        &mut self,
        generation: u64,
        reason: StopReason,
        incarnation_ended: bool,
        out: &mut Vec<PoolAction>,
    ) {
        let inc = self
            .session
            .filter(|s| s.generation == generation)
            .map(|s| s.incarnation);
        if inc.is_some() {
            self.session = None;
        }
        for rec in self.accts.values_mut() {
            let uid = rec.uid();
            if rec.generation == Some(generation) && rec.active() {
                rec.stop(reason.as_str(), reason, out);
            } else if rec.state == OutputState::Backoff {
                rec.state = OutputState::Off;
                rec.backoff_until = None;
            }
            if incarnation_ended && let Some(inc) = inc {
                if rec.quarantine_inc == Some(inc) {
                    rec.quarantine_inc = None;
                    if rec.state == OutputState::Quarantine {
                        rec.state = OutputState::Off;
                    }
                    out.push(PoolAction::SetQuarantine {
                        account: uid,
                        incarnation: None,
                    });
                }
                if rec.streamed_inc == Some(inc) {
                    rec.streamed_inc = None;
                    if rec.counted_inc != Some(inc) && rec.rejections != 0 {
                        rec.rejections = 0;
                        out.push(PoolAction::SetRejections {
                            account: uid,
                            count: 0,
                        });
                    }
                }
            }
        }
    }

    fn on_ready(
        &mut self,
        account: u64,
        generation: u64,
        attempt: u64,
        negotiated: Option<&str>,
        now: f64,
        out: &mut Vec<PoolAction>,
    ) {
        let Some(uid) = self.current(account, generation, attempt) else {
            return;
        };
        let s = self.session;
        let mismatch = if self.adaptive {
            !CODEC_ORDER.contains(&negotiated.unwrap_or("").to_uppercase().as_str())
        } else {
            negotiated != Some("H264")
        };
        let rec = self.rec_mut(uid);
        if rec.state != OutputState::Joining {
            return;
        }
        if mismatch {
            rec.backoff(StopReason::CodecMismatch, now, out);
            return;
        }
        rec.retry.reset();
        rec.state = OutputState::Ready;
        rec.live_at = Some(now);
        rec.streamed_inc = rec.incarnation;
        if let Some(s) = s.filter(|s| Some(s.generation) == rec.generation) {
            rec.state = OutputState::Live;
            out.push(if s.holding {
                PoolAction::Slate {
                    account,
                    generation: s.generation,
                }
            } else {
                PoolAction::SetLive {
                    account,
                    generation: s.generation,
                    epoch: s.epoch,
                }
            });
        }
    }

    fn on_voice(
        &mut self,
        account: u64,
        session_id: Option<String>,
        channel_id: Option<u64>,
        now: f64,
        out: &mut Vec<PoolAction>,
    ) {
        let Some(rec) = self.accts.get(&account) else {
            return;
        };
        if !matches!(rec.state, OutputState::Ready | OutputState::Live) {
            return; // JOINING/STOPPING are exempt; other states hold no voice
        }
        let Some(expected) = self.channel_of(account) else {
            return; // unassigned mirror: set_routing already stopped it
        };
        let limit = self.limit;
        let rec = self.rec_mut(account);
        let verdict = rec.detector.classify(
            &RelayVoiceObserved {
                session_id,
                channel_id,
            },
            expected,
            now,
        );
        match verdict {
            OwnershipVerdict::Ok | OwnershipVerdict::Ignored => {}
            OwnershipVerdict::OwnershipUncertain => {
                rec.stop("ownership_uncertain", StopReason::OwnershipLost, out);
                rec.set_fatal(FatalReason::AccountOwnershipUncertain, "", out);
            }
            OwnershipVerdict::Displaced => {
                let inc = rec.incarnation;
                rec.stop("displaced", StopReason::RelayDisplaced, out);
                rec.state = OutputState::Quarantine;
                rec.quarantine_inc = inc;
                out.push(PoolAction::SetQuarantine {
                    account,
                    incarnation: inc,
                });
                if rec.counted_inc != inc {
                    rec.counted_inc = inc;
                    rec.rejections += 1;
                    out.push(PoolAction::SetRejections {
                        account,
                        count: rec.rejections,
                    });
                    if rec.rejections >= limit {
                        rec.set_fatal(FatalReason::RelayRejectedByServer, "", out);
                    }
                }
            }
            // do not guess, stop sending and retry
            OwnershipVerdict::MissingEvidence => {
                rec.backoff(StopReason::OwnershipLost, now, out);
            }
        }
    }

    fn on_participants(
        &mut self,
        account: u64,
        participants: &HashSet<u64>,
        channel_id: u64,
        out: &mut Vec<PoolAction>,
    ) {
        let Some(rec) = self.accts.get(&account) else {
            return;
        };
        if !rec.is_mirror() || Some(channel_id) != self.channel_of(account) {
            return;
        }
        let blocked = !self.excluded.is_disjoint(participants);
        if blocked == rec.blocked {
            return;
        }
        self.rec_mut(account).blocked = blocked;
        if blocked {
            let rec = self.rec_mut(account);
            if rec.active() {
                rec.stop("excluded_viewer", StopReason::ExcludedViewer, out);
            }
        } else if self.restartable(account) {
            self.enqueue(account);
        }
    }

    fn on_viewers(&mut self, account: u64, mut viewers: HashSet<u64>, now: f64) {
        let target = self.routing.target_user_id;
        let Some(rec) = self.accts.get_mut(&account) else {
            return;
        };
        if !rec.active() {
            return;
        }
        viewers.remove(&target);
        if !viewers.is_empty() {
            rec.zero_since = None;
        } else if rec.viewers.as_ref().is_none_or(|v| !v.is_empty()) {
            rec.zero_since = Some(now);
        }
        rec.viewers = Some(viewers);
    }

    fn pump(&mut self, now: f64, out: &mut Vec<PoolAction>) {
        let s = self.session;
        for uid in self.uids() {
            let rec = self.rec_mut(uid);
            if rec.state == OutputState::Backoff && rec.backoff_until.is_some_and(|b| now >= b) {
                rec.backoff_until = None;
                rec.state = OutputState::Off;
                if s.is_some() && self.allowed(uid, now) {
                    self.enqueue(uid);
                }
            }
        }
        let Some(s) = s else {
            for uid in std::mem::take(&mut self.queue) {
                self.rec_mut(uid).queued = false;
            }
            return;
        };
        while now >= self.next_start {
            let Some(uid) = self.queue.pop_front() else {
                break;
            };
            self.rec_mut(uid).queued = false;
            let rec = self.rec(uid);
            if rec.state != OutputState::Off || rec.blocked || !self.allowed(uid, now) {
                continue;
            }
            let channel_id = self
                .channel_of(uid)
                .expect("an allowed account has a channel");
            let rec = self.rec_mut(uid);
            rec.state = OutputState::Joining;
            rec.generation = Some(s.generation);
            rec.incarnation = Some(s.incarnation);
            rec.detector.disarm();
            rec.attempt += 1;
            out.push(PoolAction::Start {
                account: uid,
                generation: s.generation,
                attempt: rec.attempt,
                channel_id,
            });
            self.next_start = now + self.stagger;
        }
    }
}
