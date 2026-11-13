//! Supervisor state machine (spec §6, 7, 10, 12, 13). Port of `supervisor/machine.py`.
//!
//! `Supervisor::handle(event, now)` is pure apart from its own fields: it returns the actions the
//! service must execute in order. STOPPING is transient: the stop actions (`CloseGate` first, then
//! `Teardown`) are emitted and the next state is chosen in the same call, because the model has no
//! teardown-complete event.

use std::collections::HashSet;

use indexmap::IndexMap;
use relay_media::codec::CODEC_ORDER;

use crate::clock::wall_clock;

use super::liveness::LivenessTracker;
use super::model::{
    Action, Event, FatalReason, GateFacts, GatewayState, HoldReason, KeyframeMethod, Liveness,
    QuarantineRecord, RelayState, RelayVoiceObserved, SourceStream, StatusSnapshot, StopReason,
    TargetVoice,
};
use super::ownership::{OwnershipDetector, OwnershipVerdict};
use super::predicate::evaluate_gate;
use super::retry::{RetryPolicy, SplitMix64};
use super::routing::Routing;

/// Clauses that can only be true once a session exists; ignored when deciding whether to start one.
const PREFLIGHT_IGNORED: [&str; 7] = [
    "decodable_frame",
    "incarnation",
    "relay_channel",
    "ownership",
    "liveness",
    "rtc_ready",
    "generation_epoch",
];
/// Pre-join needs everything a start needs except a stream to watch.
const PREJOIN_IGNORED: [&str; 10] = [
    "decodable_frame",
    "incarnation",
    "relay_channel",
    "ownership",
    "liveness",
    "rtc_ready",
    "generation_epoch",
    "target_streaming",
    "stream_available",
    "not_paused",
];
const KEYFRAME_ORDER: [KeyframeMethod; 3] = [
    KeyframeMethod::Pli,
    KeyframeMethod::Repair,
    KeyframeMethod::Rewatch,
];
/// Consecutive fast watch retries before the parent voice client is rebuilt.
const FAST_WATCH_LEAVE_AFTER: u32 = 3;

/// `_SESSION`: CONNECTING, RELAYING, HOLD.
fn in_session(state: RelayState) -> bool {
    matches!(
        state,
        RelayState::Connecting | RelayState::Relaying | RelayState::Hold
    )
}

/// `not (set(failed) - ignored)`.
fn only_ignored(failed: &[&str], ignored: &[&str]) -> bool {
    failed.iter().all(|f| ignored.contains(f))
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupervisorConfig {
    pub target_user_id: u64,
    pub guild_id: u64,
    pub allowed_channel_id: u64,
    pub t_dead: f64,
    pub excluded_viewer_ids: HashSet<u64>,
    pub debounce: f64,
    pub hold_timeout: f64,
    pub idle_grace: f64,
    pub connect_timeout: f64,
    pub first_media_timeout: f64,
    pub retry_delays: Vec<f64>,
    pub retry_cooldown: f64,
    pub retry_jitter: f64,
    pub rejection_limit: u32,
    pub paused_capability_verified: bool,
    pub keyframe_methods: Vec<KeyframeMethod>,
    /// Wait per recovery step before trying the next one.
    pub keyframe_step_seconds: f64,
    pub prejoin_voice: bool,
    /// Watch failed while joined (slot race): retry this fast, jittered.
    pub watch_retry_interval: f64,
    /// After this long of fast watch retries, normal backoff resumes.
    pub watch_retry_cap: f64,
    /// Adaptive codec mode: the output may negotiate any codec in `CODEC_ORDER`.
    pub adaptive: bool,
}

impl SupervisorConfig {
    /// The Python dataclass with its field defaults; the four required fields are arguments.
    pub fn new(
        target_user_id: u64,
        guild_id: u64,
        allowed_channel_id: u64,
        t_dead: f64,
    ) -> SupervisorConfig {
        SupervisorConfig {
            target_user_id,
            guild_id,
            allowed_channel_id,
            t_dead,
            excluded_viewer_ids: HashSet::new(),
            debounce: 0.5,
            hold_timeout: 15.0,
            idle_grace: 30.0,
            connect_timeout: 15.0,
            first_media_timeout: 10.0,
            retry_delays: vec![1.0, 2.0, 4.0, 8.0, 15.0],
            retry_cooldown: 60.0,
            retry_jitter: 0.2,
            rejection_limit: 3,
            paused_capability_verified: false,
            // No rewatch by default: dropping the watch of a full stream gives its slot away.
            keyframe_methods: vec![KeyframeMethod::Pli, KeyframeMethod::Repair],
            keyframe_step_seconds: 3.0,
            prejoin_voice: false,
            watch_retry_interval: 1.0,
            watch_retry_cap: 300.0,
            adaptive: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PersistedStateView {
    pub desired_enabled: bool,
    pub fatal_reason: Option<FatalReason>,
    pub quarantine: Option<QuarantineRecord>,
    pub consecutive_rejections: u32,
}

/// Keyword arguments of Python `_stop`.
struct StopOpts<'a> {
    retryable: bool,
    detail: &'a str,
    keep_voice: bool,
    watch_retry: bool,
}

impl Default for StopOpts<'_> {
    fn default() -> Self {
        StopOpts {
            retryable: true,
            detail: "",
            keep_voice: false,
            watch_retry: false,
        }
    }
}

pub struct Supervisor {
    cfg: SupervisorConfig,
    mirrors: IndexMap<u64, u64>,
    desired_enabled: bool,
    fatal: Option<FatalReason>,
    quarantine: Option<QuarantineRecord>,
    rejections: u32,
    counted_inc: Option<i64>,
    gateway: GatewayState,
    target: Option<TargetVoice>,
    stream: Option<SourceStream>,
    participants: Option<HashSet<u64>>,
    generation: u64,
    epoch: u64,
    liveness: LivenessTracker,
    owner: OwnershipDetector,
    rng: SplitMix64,
    retry: RetryPolicy,
    watch_retry_since: Option<f64>,
    fast_fails: u32,
    prejoin_retry: RetryPolicy,
    prejoin_after: Option<f64>,
    /// When the start gate passed for the last `StartConnect`.
    gate_ready_at: Option<f64>,
    in_voice: bool,
    leave_at: Option<f64>,
    debounce_since: Option<f64>,
    backoff_until: Option<f64>,
    closed: bool,
    state: RelayState,
    // Per-session fields, reset by `reset_session`.
    baseline: Option<String>,
    cur_inc: Option<i64>,
    session_stream: Option<SourceStream>,
    joined: bool,
    watching: bool,
    has_frame: bool,
    inbound: bool,
    outbound: bool,
    create_requested: bool,
    relay_channel: Option<u64>,
    ownership_ok: bool,
    connect_started: f64,
    ready_at: Option<f64>,
    media_seen: bool,
    hold_reason: Option<HoldReason>,
    hold_deadline: Option<f64>,
    fresh: bool,
    kf_last: Option<f64>,
    kf_idx: usize,
}

impl Supervisor {
    pub fn new(
        cfg: SupervisorConfig,
        persisted: PersistedStateView,
        mirrors: IndexMap<u64, u64>,
    ) -> Supervisor {
        let retry =
            RetryPolicy::new(&cfg.retry_delays, cfg.retry_cooldown).jitter(cfg.retry_jitter);
        let prejoin_retry =
            RetryPolicy::new(&cfg.retry_delays, cfg.retry_cooldown).jitter(cfg.retry_jitter);
        let mut sup = Supervisor {
            liveness: LivenessTracker::new(cfg.t_dead),
            cfg,
            mirrors,
            desired_enabled: persisted.desired_enabled,
            fatal: persisted.fatal_reason.filter(|r| r.is_durable()),
            quarantine: persisted.quarantine,
            rejections: persisted.consecutive_rejections,
            counted_inc: None,
            // never trust anything from before the first synced snapshot
            gateway: GatewayState::Invalid,
            target: None,
            stream: None,
            participants: None,
            generation: 0,
            epoch: 0,
            owner: OwnershipDetector::new(),
            rng: SplitMix64::from_clock(),
            retry,
            watch_retry_since: None,
            fast_fails: 0,
            prejoin_retry,
            prejoin_after: None,
            gate_ready_at: None,
            in_voice: false,
            leave_at: None,
            debounce_since: None,
            backoff_until: None,
            closed: false,
            state: RelayState::Idle,
            baseline: None,
            cur_inc: None,
            session_stream: None,
            joined: false,
            watching: false,
            has_frame: false,
            inbound: false,
            outbound: false,
            create_requested: false,
            relay_channel: None,
            ownership_ok: false,
            connect_started: 0.0,
            ready_at: None,
            media_seen: false,
            hold_reason: None,
            hold_deadline: None,
            fresh: false,
            kf_last: None,
            kf_idx: 0,
        };
        sup.reset_session();
        sup.state = sup.enable_state();
        sup
    }

    // ------------------------------------------------------------ public

    pub fn prejoin_voice(&self) -> bool {
        self.cfg.prejoin_voice
    }

    pub fn routing(&self) -> Routing {
        Routing {
            target_user_id: self.cfg.target_user_id,
            channel_id: self.cfg.allowed_channel_id,
            mirrors: self.mirrors.clone(),
        }
    }

    pub fn state(&self) -> RelayState {
        self.state
    }

    pub fn gateway(&self) -> GatewayState {
        self.gateway
    }

    pub fn stream(&self) -> Option<&SourceStream> {
        self.stream.as_ref()
    }

    pub fn target(&self) -> Option<&TargetVoice> {
        self.target.as_ref()
    }

    pub fn desired_enabled(&self) -> bool {
        self.desired_enabled
    }

    pub fn fatal(&self) -> Option<FatalReason> {
        self.fatal
    }

    pub fn quarantine(&self) -> Option<&QuarantineRecord> {
        self.quarantine.as_ref()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn gate_ready_at(&self) -> Option<f64> {
        self.gate_ready_at
    }

    /// Participants of the main channel; `None` without a reliable view.
    pub fn participants(&self) -> Option<&HashSet<u64>> {
        self.participants.as_ref()
    }

    pub fn handle(&mut self, event: Event, now: f64) -> Vec<Action> {
        let mut acts = Vec::new();
        if self.closed {
            return acts;
        }
        self.dispatch(event, now, &mut acts);
        if !self.closed {
            self.settle(now, &mut acts);
        }
        acts
    }

    pub fn tick(&mut self, now: f64) -> Vec<Action> {
        self.handle(Event::Tick, now)
    }

    /// Publisher's per-packet check: the full predicate with current facts.
    pub fn gate_allows(&self, generation: u64, epoch: u64, now: f64) -> bool {
        if self.state != RelayState::Relaying
            || generation != self.generation
            || epoch != self.epoch
        {
            return false;
        }
        evaluate_gate(&self.facts(now)).open
    }

    pub fn status(&self, now: f64) -> StatusSnapshot {
        let failed = if in_session(self.state) {
            evaluate_gate(&self.facts(now)).failed
        } else {
            Vec::new()
        };
        StatusSnapshot {
            state: self.state,
            desired_enabled: self.desired_enabled,
            fatal: self.fatal.map(|f| f.as_str().to_string()),
            hold_reason: self.hold_reason.map(|h| h.as_str().to_string()),
            hold_deadline: self.hold_deadline,
            backoff_until: self.backoff_until,
            generation: self.generation,
            epoch: self.epoch,
            source_incarnation: self.cur_inc,
            gateway: self.gateway.as_str().to_string(),
            quarantine: self.quarantine.clone(),
            consecutive_rejections: self.rejections,
            voice_session_baseline: self.baseline.clone(),
            last_source_progress_age: self.liveness.age(now),
            gate_failed: failed,
        }
    }

    // ---------------------------------------------------------- session

    fn reset_session(&mut self) {
        self.owner.disarm();
        self.baseline = None;
        self.cur_inc = None;
        self.session_stream = None;
        self.joined = false;
        self.watching = false;
        self.has_frame = false;
        self.inbound = false;
        self.outbound = false;
        self.create_requested = false;
        self.relay_channel = None;
        self.ownership_ok = false;
        self.connect_started = 0.0;
        self.ready_at = None;
        self.media_seen = false;
        self.hold_reason = None;
        self.hold_deadline = None;
        self.fresh = false;
        self.kf_last = None;
        self.kf_idx = 0;
        self.liveness.reset();
    }

    fn enable_state(&self) -> RelayState {
        if self.fatal.is_some_and(|f| f.is_durable()) {
            return RelayState::Fatal;
        }
        if !self.desired_enabled {
            return RelayState::Disabled;
        }
        if self.quarantine.is_some() {
            return RelayState::Quarantine;
        }
        RelayState::Idle
    }

    fn facts(&self, now: f64) -> GateFacts {
        GateFacts {
            desired_enabled: self.desired_enabled,
            // no revocation event exists; consent loss arrives as disable
            source_authorization_valid: true,
            gateway_state: self.gateway,
            excluded_viewer_ids: self.cfg.excluded_viewer_ids.clone(),
            participant_view_fresh: self.participants.is_some(),
            participants: self.participants.clone().unwrap_or_default(),
            quarantined_incarnation: self.quarantine.as_ref().map(|q| q.incarnation),
            configured_target_user_id: self.cfg.target_user_id,
            allowed_voice_channel_id: self.cfg.allowed_channel_id,
            target: self.target,
            stream: self.stream.clone(),
            current_source_incarnation: self.cur_inc,
            paused_capability_verified: self.cfg.paused_capability_verified,
            has_verified_decodable_frame: self.has_frame,
            relay_channel_id: self.relay_channel,
            relay_ownership_valid: self.ownership_ok,
            input_liveness: self.liveness.state(now),
            inbound_ready: self.inbound,
            outbound_ready: self.outbound,
            generation_current: in_session(self.state),
            epoch_current: true,
        }
    }

    /// Everything knowable before join passes (used to start and to retry).
    fn source_ok(&self, now: f64) -> bool {
        only_ignored(&evaluate_gate(&self.facts(now)).failed, &PREFLIGHT_IGNORED)
    }

    fn start(&mut self, now: f64, acts: &mut Vec<Action>) {
        let stream = self
            .stream
            .clone()
            .expect("a start requires a stream (source_ok checked stream_available)");
        self.epoch += 1;
        self.reset_session();
        self.cur_inc = Some(stream.incarnation);
        self.session_stream = Some(stream.clone());
        self.connect_started = now;
        self.gate_ready_at = Some(self.debounce_since.unwrap_or(now));
        self.in_voice = true;
        self.leave_at = None;
        self.debounce_since = None;
        self.backoff_until = None;
        self.state = RelayState::Connecting;
        acts.push(Action::StartConnect {
            generation: self.generation,
            epoch: self.epoch,
            stream,
        });
    }

    // ---------------------------------------------------------- dispatch

    fn cur(&self, generation: u64) -> bool {
        in_session(self.state) && generation == self.generation
    }

    fn cur_epoch(&self, generation: u64, epoch: u64) -> bool {
        self.cur(generation) && epoch == self.epoch
    }

    fn dispatch(&mut self, event: Event, now: f64, acts: &mut Vec<Action>) {
        match event {
            Event::Tick => {}
            Event::EnableChanged { enabled } => {
                self.desired_enabled = enabled;
                if enabled {
                    if self.state == RelayState::Disabled {
                        self.state = self.enable_state();
                    }
                } else if !matches!(self.state, RelayState::Disabled | RelayState::Fatal) {
                    self.stop(StopReason::Disabled, now, acts, StopOpts::default());
                }
            }
            Event::RoutingChanged { routing } => self.apply_routing(&routing, now, acts),
            Event::PrejoinChanged { on } => {
                self.cfg.prejoin_voice = on;
                self.prejoin_after = None;
                self.prejoin_retry.reset();
                if !on && self.state == RelayState::Idle && self.in_voice && !self.source_ok(now) {
                    acts.push(Action::LeaveVoice {
                        reason: "prejoin_off".to_string(),
                    });
                    self.in_voice = false;
                    self.leave_at = None;
                }
            }
            Event::PrejoinFailed { .. } => {
                if !in_session(self.state) && self.in_voice {
                    self.in_voice = false;
                    self.leave_at = None;
                    self.prejoin_after = Some(now + self.prejoin_retry.next_delay());
                }
            }
            Event::VoiceLost {
                foreign_session,
                from_prejoin,
                ..
            } => {
                if in_session(self.state) {
                    if !from_prejoin {
                        self.stop(
                            StopReason::ConnectFailed,
                            now,
                            acts,
                            StopOpts {
                                detail: "voice lost",
                                ..StopOpts::default()
                            },
                        );
                    }
                } else if self.in_voice {
                    self.in_voice = false;
                    self.leave_at = None;
                    let delay = if foreign_session {
                        self.cfg.retry_cooldown
                    } else {
                        self.prejoin_retry.next_delay()
                    };
                    self.prejoin_after = Some(now + delay);
                }
            }
            Event::ResetFatal => {
                if self.state == RelayState::Fatal && self.fatal.is_some_and(|f| f.is_durable()) {
                    let reset = self.fatal == Some(FatalReason::RelayRejectedByServer);
                    acts.push(Action::ClearFatalLatch {
                        reset_rejections: reset,
                    });
                    if reset {
                        self.rejections = 0;
                    }
                    self.fatal = None;
                    self.state = self.enable_state();
                }
            }
            Event::GatewayChanged { state } => {
                self.gateway = state;
                if state == GatewayState::Fresh {
                    self.reconcile_quarantine(acts, false);
                }
            }
            Event::GatewaySnapshot {
                target,
                stream,
                participants,
                new_session,
            } => {
                self.target = target;
                self.stream = stream.filter(|s| s.owner_id == self.cfg.target_user_id);
                self.participants = participants;
                self.gateway = GatewayState::Fresh;
                if new_session && in_session(self.state) {
                    self.stop(StopReason::SessionInvalid, now, acts, StopOpts::default());
                }
                self.reconcile_quarantine(acts, true);
            }
            Event::TargetVoiceChanged { target } => {
                self.target = target;
                self.reconcile_quarantine(acts, false);
            }
            Event::StreamStarted { stream: s } | Event::StreamUpdated { stream: s } => {
                // Only the configured target's streams matter; anyone else's are ignored.
                if s.owner_id == self.cfg.target_user_id {
                    self.stream = Some(s);
                    self.reconcile_quarantine(acts, false);
                }
            }
            Event::StreamEnded { key, incarnation } => {
                if self
                    .stream
                    .as_ref()
                    .is_some_and(|s| s.key == key && s.incarnation == incarnation)
                {
                    self.stream = None;
                    self.reconcile_quarantine(acts, false);
                }
            }
            Event::ParticipantsChanged { participants } => self.participants = participants,
            Event::RelayVoiceObserved(e) => {
                if !in_session(self.state)
                    && self.in_voice
                    && e.channel_id == Some(self.cfg.allowed_channel_id)
                {
                    self.prejoin_retry.reset(); // the pre-join landed
                }
                self.on_relay_voice(&e, now, acts);
            }
            Event::Joined {
                generation,
                voice_session_id,
            } if self.cur(generation) => {
                self.owner.arm(voice_session_id.clone());
                self.ownership_ok = voice_session_id.is_some();
                self.baseline = voice_session_id;
                self.joined = true;
                self.relay_channel = Some(self.cfg.allowed_channel_id);
                self.mark_ready(now);
            }
            Event::Watching { generation } if self.cur(generation) => {
                self.watching = true;
                self.inbound = true;
                self.mark_ready(now);
            }
            Event::FirstFrame {
                generation, epoch, ..
            }
            | Event::FreshFrame { generation, epoch }
                if self.cur_epoch(generation, epoch) =>
            {
                self.has_frame = true;
                self.fresh = true;
            }
            Event::OutputReady {
                generation,
                negotiated_video_codec,
            } if self.cur(generation) => {
                self.outbound = true;
                if !self.codec_ok(negotiated_video_codec.as_deref()) {
                    self.stop(StopReason::CodecMismatch, now, acts, StopOpts::default());
                }
            }
            Event::CodecChanged {
                generation,
                negotiated_video_codec,
            } if self.cur(generation) && !self.cfg.adaptive => {
                if !self.codec_ok(negotiated_video_codec.as_deref()) {
                    self.stop(StopReason::CodecMismatch, now, acts, StopOpts::default());
                }
            }
            Event::ConnectFailed {
                generation,
                reason,
                retryable,
                detail,
                watch_retry,
            } if self.cur(generation) => {
                self.stop(
                    reason,
                    now,
                    acts,
                    StopOpts {
                        retryable,
                        detail: &detail,
                        watch_retry,
                        ..StopOpts::default()
                    },
                );
            }
            Event::InboundHealth { generation, ok } if self.cur(generation) => self.inbound = ok,
            Event::OutputHealth { generation, ok } if self.cur(generation) => {
                self.outbound = ok;
                if !ok {
                    self.stop(StopReason::OutputDead, now, acts, StopOpts::default());
                }
            }
            Event::SourceProgress { generation, at, .. } if self.cur(generation) => {
                self.liveness.progress(at);
                self.media_seen = true;
            }
            Event::WorkerFailed {
                generation, epoch, ..
            } if self.cur_epoch(generation, epoch) => self.worker_failed(now, acts),
            Event::AuthRejected => {
                if self.state != RelayState::Fatal {
                    self.stop(StopReason::AuthRejected, now, acts, StopOpts::default());
                }
            }
            Event::ConfigInvalid { detail } => {
                if self.state != RelayState::Fatal {
                    self.stop(
                        StopReason::ConfigInvalid,
                        now,
                        acts,
                        StopOpts {
                            detail: &detail,
                            ..StopOpts::default()
                        },
                    );
                }
            }
            Event::Shutdown => self.stop(StopReason::Shutdown, now, acts, StopOpts::default()),
            // stale or irrelevant for the current state
            _ => {}
        }
    }

    /// Not a rejection: no backoff, quarantine and fatal latch are left as they are.
    fn apply_routing(&mut self, r: &Routing, now: f64, acts: &mut Vec<Action>) {
        self.mirrors = r.mirrors.clone();
        if (r.target_user_id, r.channel_id)
            == (self.cfg.target_user_id, self.cfg.allowed_channel_id)
        {
            acts.push(Action::ApplyRouting {
                routing: self.routing(),
            });
            return;
        }
        let channel_changed = r.channel_id != self.cfg.allowed_channel_id;
        if in_session(self.state) {
            self.stop(
                StopReason::RoutingChanged,
                now,
                acts,
                StopOpts {
                    keep_voice: !channel_changed,
                    ..StopOpts::default()
                },
            );
        } else if channel_changed && self.in_voice {
            acts.push(Action::LeaveVoice {
                reason: StopReason::RoutingChanged.as_str().to_string(),
            });
            self.in_voice = false;
            self.leave_at = None;
        }
        self.cfg.target_user_id = r.target_user_id;
        self.cfg.allowed_channel_id = r.channel_id;
        if !matches!(self.state, RelayState::Disabled | RelayState::Fatal) {
            self.state = self.enable_state();
            self.debounce_since = None;
            self.backoff_until = None;
        }
        acts.push(Action::ApplyRouting {
            routing: self.routing(),
        });
    }

    fn codec_ok(&self, codec: Option<&str>) -> bool {
        let Some(codec) = codec else {
            return false;
        };
        let upper = codec.to_uppercase();
        if self.cfg.adaptive {
            CODEC_ORDER.contains(&upper.as_str())
        } else {
            upper == "H264"
        }
    }

    fn mark_ready(&mut self, now: f64) {
        if self.joined && self.watching && self.ready_at.is_none() {
            self.ready_at = Some(now);
        }
    }

    fn on_relay_voice(&mut self, e: &RelayVoiceObserved, now: f64, acts: &mut Vec<Action>) {
        match self.owner.classify(e, self.cfg.allowed_channel_id, now) {
            OwnershipVerdict::Ok => {
                self.relay_channel = e.channel_id;
                self.ownership_ok = true;
            }
            OwnershipVerdict::MissingEvidence => self.ownership_ok = false,
            OwnershipVerdict::Displaced => {
                self.stop(StopReason::RelayDisplaced, now, acts, StopOpts::default());
            }
            OwnershipVerdict::OwnershipUncertain => {
                self.stop(StopReason::OwnershipLost, now, acts, StopOpts::default());
            }
            OwnershipVerdict::Ignored => {}
        }
    }

    fn worker_failed(&mut self, now: f64, acts: &mut Vec<Action>) {
        if self.state == RelayState::Connecting {
            self.stop(
                StopReason::ConnectFailed,
                now,
                acts,
                StopOpts {
                    detail: "worker failed",
                    ..StopOpts::default()
                },
            );
            return;
        }
        if self.state == RelayState::Relaying {
            self.enter_hold(HoldReason::PipelineRebuild, now, acts);
        } else {
            self.hold_reason = Some(HoldReason::PipelineRebuild);
        }
        self.rebuild(acts, true);
    }

    fn rebuild(&mut self, acts: &mut Vec<Action>, reset_keyframe: bool) {
        self.epoch += 1;
        self.has_frame = false;
        self.fresh = false;
        if reset_keyframe {
            self.kf_last = None;
            self.kf_idx = 0;
        }
        acts.push(Action::RebuildPipeline {
            generation: self.generation,
            epoch: self.epoch,
        });
    }

    // -------------------------------------------------------- quarantine

    /// Clear the barrier once the quarantined incarnation is observed to be over.
    ///
    /// A snapshot with the same key and owner adopts its incarnation: after a restart an active
    /// stream is not a new incarnation (spec §12).
    fn reconcile_quarantine(&mut self, acts: &mut Vec<Action>, snapshot: bool) {
        let Some(q) = self.quarantine.clone() else {
            return;
        };
        if self.gateway != GatewayState::Fresh {
            return;
        }
        let allowed = self.cfg.allowed_channel_id;
        let gone = match (&self.target, &self.stream) {
            (Some(t), Some(s)) => {
                t.channel_id != Some(allowed)
                    || !t.self_stream
                    || s.key != q.stream_key
                    || s.owner_id != q.owner_id
                    || (s.incarnation != q.incarnation && !snapshot)
            }
            _ => true,
        };
        if gone {
            self.quarantine = None;
            acts.push(Action::SetQuarantine { record: None });
            if self.state == RelayState::Quarantine {
                self.state = RelayState::Idle;
            }
        } else if let Some(s) = &self.stream
            && s.incarnation != q.incarnation
        {
            let adopted = QuarantineRecord {
                incarnation: s.incarnation,
                ..q
            };
            self.quarantine = Some(adopted.clone());
            acts.push(Action::SetQuarantine {
                record: Some(adopted),
            });
        }
    }

    // -------------------------------------------------------------- stop

    fn stop(&mut self, reason: StopReason, now: f64, acts: &mut Vec<Action>, o: StopOpts<'_>) {
        let was_in_session = in_session(self.state);
        let ss = self.session_stream.clone();
        let media = self.media_seen;
        let fast = reason == StopReason::ConnectFailed
            && o.watch_retry
            && o.retryable
            && self.fast_watch_retry(now);
        let mut rebuild_voice = false;
        if fast {
            self.fast_fails += 1;
            if self.fast_fails >= FAST_WATCH_LEAVE_AFTER {
                self.fast_fails = 0;
                // the parent voice client may be dead: the next attempt joins again
                rebuild_voice = true;
            }
        } else if was_in_session {
            self.fast_fails = 0;
        }
        let keep_voice =
            (reason == StopReason::SourceStopped || o.keep_voice || (fast && !rebuild_voice))
                && self.in_voice;
        acts.push(Action::CloseGate {
            reason: reason.as_str().to_string(),
        });
        if was_in_session {
            acts.push(Action::Teardown {
                generation: self.generation,
                leave_voice: !keep_voice,
                reason,
            });
            // invalidates every late callback of the old session
            self.generation += 1;
            self.reset_session();
        } else if self.in_voice && !keep_voice {
            acts.push(Action::LeaveVoice {
                reason: reason.as_str().to_string(),
            });
        }
        if keep_voice {
            self.leave_at = Some(now + self.cfg.idle_grace);
        } else {
            self.in_voice = false;
            self.leave_at = None;
        }
        self.debounce_since = None;
        self.backoff_until = None;
        if was_in_session
            && media
            && ss.is_some()
            && matches!(
                reason,
                StopReason::SourceStopped | StopReason::SourceLeftWhitelist
            )
            && self.rejections != 0
        {
            self.rejections = 0;
            acts.push(Action::SetRejections { count: 0 });
        }

        match reason {
            StopReason::Shutdown => {
                acts.push(Action::Exit { code: 0 });
                self.closed = true;
            }
            StopReason::Disabled => self.state = RelayState::Disabled,
            StopReason::AuthRejected => self.latch(FatalReason::AuthRejected, o.detail, acts),
            StopReason::OwnershipLost => {
                self.latch(FatalReason::AccountOwnershipUncertain, o.detail, acts);
            }
            StopReason::ConfigInvalid => {
                self.fatal = Some(FatalReason::ConfigInvalid); // never latched
                self.state = RelayState::Fatal;
            }
            StopReason::RelayDisplaced if ss.is_some() => {
                if let Some(ss) = &ss {
                    self.displaced(ss, acts);
                }
            }
            StopReason::HoldExpired
            | StopReason::OutputDead
            | StopReason::CodecMismatch
            | StopReason::ConnectFailed => {
                let delay = if fast {
                    // not a rejection and no escalation: the next watch should win a freed slot quickly
                    let j = self.cfg.retry_jitter;
                    self.cfg.watch_retry_interval * (1.0 + (-j + 2.0 * j * self.rng.next_f64()))
                } else if o.retryable {
                    self.retry.next_delay()
                } else {
                    self.cfg.retry_cooldown
                };
                self.backoff_until = Some(now + delay);
                self.state = RelayState::Backoff;
            }
            _ => self.state = RelayState::Idle,
        }
    }

    fn fast_watch_retry(&mut self, now: f64) -> bool {
        let since = *self.watch_retry_since.get_or_insert(now);
        now - since < self.cfg.watch_retry_cap
    }

    fn latch(&mut self, reason: FatalReason, detail: &str, acts: &mut Vec<Action>) {
        acts.push(Action::SetFatalLatch {
            reason,
            detail: detail.to_string(),
        });
        self.fatal = Some(reason);
        self.state = RelayState::Fatal;
    }

    fn displaced(&mut self, ss: &SourceStream, acts: &mut Vec<Action>) {
        let rec = QuarantineRecord {
            owner_id: ss.owner_id,
            stream_key: ss.key.clone(),
            incarnation: ss.incarnation,
            since: wall_clock(),
        };
        self.quarantine = Some(rec.clone());
        acts.push(Action::SetQuarantine { record: Some(rec) });
        if Some(ss.incarnation) != self.counted_inc {
            // at most once per incarnation
            self.counted_inc = Some(ss.incarnation);
            self.rejections += 1;
            acts.push(Action::SetRejections {
                count: self.rejections,
            });
        }
        if self.rejections >= self.cfg.rejection_limit {
            let detail = format!("{} consecutive displaced incarnations", self.rejections);
            self.latch(FatalReason::RelayRejectedByServer, &detail, acts);
        } else {
            self.state = RelayState::Quarantine;
        }
    }

    // ------------------------------------------------------------ settle

    fn stop_reason(&self) -> Option<StopReason> {
        let allowed = self.cfg.allowed_channel_id;
        if self.gateway == GatewayState::Invalid {
            return Some(StopReason::SessionInvalid);
        }
        let t = match &self.target {
            Some(t) if t.channel_id == Some(allowed) => t,
            _ => return Some(StopReason::SourceLeftWhitelist),
        };
        let s = match &self.stream {
            Some(s) if t.self_stream && !s.unavailable => s,
            _ => return Some(StopReason::SourceStopped),
        };
        if s.channel_id != allowed {
            return Some(StopReason::SourceLeftWhitelist);
        }
        if Some(s.incarnation) != self.cur_inc {
            return Some(StopReason::SourceStopped);
        }
        let ex = &self.cfg.excluded_viewer_ids;
        if !ex.is_empty() {
            match &self.participants {
                None => return Some(StopReason::ExcludedViewer),
                Some(p) if !ex.is_disjoint(p) => return Some(StopReason::ExcludedViewer),
                Some(_) => {}
            }
        }
        None
    }

    fn settle(&mut self, now: f64, acts: &mut Vec<Action>) {
        if in_session(self.state)
            && let Some(reason) = self.stop_reason()
        {
            self.stop(reason, now, acts, StopOpts::default());
        }
        if self.closed {
            return;
        }
        if self.state == RelayState::Connecting {
            self.connecting(now, acts);
        } else if self.state == RelayState::Relaying {
            self.relaying(now, acts);
        }
        match self.state {
            RelayState::Hold => self.holding(now, acts),
            RelayState::Idle => self.idle(now, acts),
            RelayState::Backoff => self.backoff(now, acts),
            _ => {}
        }
    }

    fn idle(&mut self, now: f64, acts: &mut Vec<Action>) {
        if self.source_ok(now) {
            let since = *self.debounce_since.get_or_insert(now);
            if now - since >= self.cfg.debounce {
                self.start(now, acts);
            }
            return;
        }
        self.debounce_since = None;
        self.watch_retry_since = None;
        self.fast_fails = 0;
        let prejoin = self.cfg.prejoin_voice && self.prejoin_ok(now);
        if !prejoin {
            self.prejoin_after = None;
            self.prejoin_retry.reset();
        }
        if !self.in_voice {
            if prejoin && self.prejoin_after.is_none_or(|after| now >= after) {
                self.prejoin_after = None;
                acts.push(Action::JoinVoice);
                self.in_voice = true;
                self.leave_at = None;
            }
            return;
        }
        if self
            .target
            .is_none_or(|t| t.channel_id != Some(self.cfg.allowed_channel_id))
        {
            acts.push(Action::LeaveVoice {
                reason: "source_left_voice".to_string(),
            });
            self.in_voice = false;
        } else if prejoin {
            self.leave_at = None; // stay ready for the next stream start
        } else {
            match self.leave_at {
                // e.g. pre-join turned off while a start was debouncing
                None => self.leave_at = Some(now + self.cfg.idle_grace),
                Some(at) if now >= at => {
                    acts.push(Action::LeaveVoice {
                        reason: "idle_grace".to_string(),
                    });
                    self.in_voice = false;
                }
                Some(_) => {}
            }
        }
    }

    /// Every join gate passes; only the stream itself is missing.
    fn prejoin_ok(&self, now: f64) -> bool {
        only_ignored(&evaluate_gate(&self.facts(now)).failed, &PREJOIN_IGNORED)
    }

    fn backoff(&mut self, now: f64, acts: &mut Vec<Action>) {
        if !self.source_ok(now) {
            self.backoff_until = None;
            self.retry.reset();
            self.state = RelayState::Idle;
            self.idle(now, acts);
        } else if self.backoff_until.is_some_and(|until| now >= until) {
            self.start(now, acts);
        }
    }

    fn connecting(&mut self, now: f64, acts: &mut Vec<Action>) {
        if !(self.joined && self.watching) {
            if now - self.connect_started >= self.cfg.connect_timeout {
                if self.joined {
                    self.stop(
                        StopReason::ConnectFailed,
                        now,
                        acts,
                        StopOpts {
                            detail: "watch timeout",
                            watch_retry: true,
                            ..StopOpts::default()
                        },
                    );
                } else {
                    self.stop(
                        StopReason::ConnectFailed,
                        now,
                        acts,
                        StopOpts {
                            detail: "join timeout",
                            ..StopOpts::default()
                        },
                    );
                }
                return;
            }
        } else if !self.has_frame
            && self
                .ready_at
                .is_some_and(|at| now - at >= self.cfg.first_media_timeout)
        {
            self.stop(
                StopReason::ConnectFailed,
                now,
                acts,
                StopOpts {
                    detail: "no first media",
                    ..StopOpts::default()
                },
            );
            return;
        } else if self
            .ready_at
            .is_some_and(|at| now - at >= self.cfg.connect_timeout)
        {
            // Joined and watching but the gate never opened: output not ready or source not LIVE.
            self.stop(
                StopReason::ConnectFailed,
                now,
                acts,
                StopOpts {
                    detail: "output not ready",
                    ..StopOpts::default()
                },
            );
            return;
        }
        if !self.create_requested
            && self.joined
            && self.watching
            && self.has_frame
            && self.liveness.state(now) == Liveness::Live
        {
            self.create_requested = true;
            acts.push(Action::CreateOutput {
                generation: self.generation,
            });
        }
        if self.outbound && evaluate_gate(&self.facts(now)).open {
            self.open(acts);
        }
    }

    fn open(&mut self, acts: &mut Vec<Action>) {
        self.state = RelayState::Relaying;
        self.hold_reason = None;
        self.hold_deadline = None;
        self.retry.reset();
        self.watch_retry_since = None;
        self.fast_fails = 0;
        acts.push(Action::OpenGate {
            generation: self.generation,
            epoch: self.epoch,
        });
    }

    fn relaying(&mut self, now: f64, acts: &mut Vec<Action>) {
        let decision = evaluate_gate(&self.facts(now));
        if decision.open {
            return;
        }
        let failed = decision.failed;
        let reason = if failed.contains(&"gateway_fresh") {
            HoldReason::GatewayResume
        } else if failed.contains(&"not_paused") {
            HoldReason::Paused
        } else if failed.contains(&"liveness") {
            HoldReason::NoMedia
        } else {
            HoldReason::InputKeyframe
        };
        self.enter_hold(reason, now, acts);
    }

    fn enter_hold(&mut self, reason: HoldReason, now: f64, acts: &mut Vec<Action>) {
        acts.push(Action::CloseGate {
            reason: format!("hold_{}", reason.as_str()),
        });
        self.state = RelayState::Hold;
        self.hold_reason = Some(reason);
        // Fixed at entry; later causes or recovery steps never extend it. Verified pause has none.
        self.hold_deadline = if reason == HoldReason::Paused {
            None
        } else {
            Some(now + self.cfg.hold_timeout)
        };
        self.fresh = false;
        self.kf_last = None;
        self.kf_idx = 0;
        acts.push(Action::ShowSlate {
            generation: self.generation,
            reason,
        });
    }

    fn paused_now(&self) -> bool {
        self.cfg.paused_capability_verified
            && self.stream.as_ref().is_some_and(|s| s.paused == Some(true))
    }

    fn holding(&mut self, now: f64, acts: &mut Vec<Action>) {
        if self.hold_reason == Some(HoldReason::Paused) && !self.paused_now() {
            // Unpause must show liveness and a fresh frame again, within a normal deadline.
            self.hold_reason = Some(HoldReason::InputKeyframe);
            self.hold_deadline = Some(now + self.cfg.hold_timeout);
            self.fresh = false;
            self.kf_last = None;
            self.kf_idx = 0;
            acts.push(Action::ShowSlate {
                generation: self.generation,
                reason: HoldReason::InputKeyframe,
            });
        }
        if self.hold_deadline.is_some_and(|d| now >= d) {
            // Media still flows and only a keyframe is missing: the watch holds a viewer slot a
            // full stream may not give back, so keep it and start the keyframe recovery over.
            // Any other failed clause (relay voice, ownership, rtc) still tears down and
            // reconnects.
            let failed = evaluate_gate(&self.facts(now)).failed;
            if self.liveness.state(now) == Liveness::Live
                && failed.iter().all(|c| *c == "decodable_frame")
            {
                self.hold_deadline = Some(now + self.cfg.hold_timeout);
                self.kf_idx = 0;
                self.kf_last = None;
                self.recover(now, acts);
                return;
            }
            self.stop(StopReason::HoldExpired, now, acts, StopOpts::default());
            self.backoff(now, acts);
            return;
        }
        if self.fresh && evaluate_gate(&self.facts(now)).open {
            self.open(acts);
            return;
        }
        self.recover(now, acts);
    }

    /// Input keyframe recovery: pli, then repair, then rewatch, then wait for a natural frame.
    fn recover(&mut self, now: f64, acts: &mut Vec<Action>) {
        if self.hold_reason == Some(HoldReason::Paused)
            || self.gateway != GatewayState::Fresh
            || self.fresh
        {
            return;
        }
        if self
            .kf_last
            .is_some_and(|last| now - last < self.cfg.keyframe_step_seconds)
        {
            return;
        }
        let methods: Vec<KeyframeMethod> = KEYFRAME_ORDER
            .into_iter()
            .filter(|m| self.cfg.keyframe_methods.contains(m))
            .collect();
        while self.kf_idx < methods.len() {
            let method = methods[self.kf_idx];
            self.kf_idx += 1;
            if method == KeyframeMethod::Rewatch {
                if !self.source_ok(now) {
                    continue;
                }
                self.hold_reason = Some(HoldReason::Rewatch);
                self.rebuild(acts, false);
            }
            acts.push(Action::RequestInputKeyframe {
                generation: self.generation,
                method,
            });
            self.kf_last = Some(now);
            return;
        }
        // Every step was tried: while media still arrives, keep asking for a keyframe each step.
        if methods.contains(&KeyframeMethod::Pli) && self.liveness.state(now) == Liveness::Live {
            acts.push(Action::RequestInputKeyframe {
                generation: self.generation,
                method: KeyframeMethod::Pli,
            });
            self.kf_last = Some(now);
        }
    }
}
