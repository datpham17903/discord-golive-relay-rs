//! Shared types for the Supervisor (spec §6, 7, 12, 13). Port of `supervisor/model.py`.
//!
//! The Supervisor is a pure, serialized state machine: `Supervisor::handle(event, now)` returns a
//! list of actions, and the service executes them in order. Every event that refers to a session
//! carries its `generation` so late callbacks from a stopped session are discarded.
//!
//! Python dataclass -> Rust mapping. Python's `Event` and `Action` unions become the enums below;
//! each dataclass is the variant of the same name with the same fields (`Tick`, `ResetFatal`,
//! `AuthRejected`, `Shutdown`, `JoinVoice` are unit variants; `RelayVoiceObserved` is a struct used as the
//! payload of the variant of the same name, because the ownership detector takes it directly). Python defaults have no Rust
//! counterpart, so every field is spelled out at construction (`detail: String::new()`,
//! `foreign_session: false`, `from_prejoin: false`, `watch_retry: false`). Other dataclasses keep
//! their name and are structs (`TargetVoice`, `SourceStream`, `QuarantineRecord`, `GateFacts`,
//! `GateDecision`, `StatusSnapshot`). Types: ids are `u64`, `incarnation` is `i64` (as persisted),
//! `generation`/`epoch` are `u64`, frozensets are `HashSet`, `Optional[x]` is `Option<x>`, and a
//! negotiated codec is the codec name (`String`, see `relay-media::codec`). `RequestInputKeyframe.method`
//! is the `KeyframeMethod` enum.

use std::collections::HashSet;
use std::fmt;

use crate::control::state::JsonObject;

use super::routing::Routing;

/// Defines a fieldless enum whose variants map to the Python string values used in logs, status
/// and state.
macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $value:literal,)+ }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant,)+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }

            /// Inverse of `as_str` (Python `Enum(value)`).
            pub fn parse(s: &str) -> Option<Self> {
                match s {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

str_enum!(GatewayState {
    Fresh = "fresh",
    Resuming = "resuming",
    Invalid = "invalid",
});

str_enum!(RelayState {
    Disabled = "disabled",
    Idle = "idle",
    Connecting = "connecting",
    Relaying = "relaying",
    Hold = "hold",
    Stopping = "stopping",
    Backoff = "backoff",
    Quarantine = "quarantine",
    Fatal = "fatal",
});

str_enum!(HoldReason {
    GatewayResume = "gateway_resume",
    PipelineRebuild = "pipeline_rebuild",
    InputKeyframe = "input_keyframe",
    Rewatch = "rewatch",
    NoMedia = "no_media",
    Paused = "paused",
});

str_enum!(
    /// `AuthRejected`, `AccountOwnershipUncertain`, `RelayRejectedByServer` are durable latches
    /// (state.json `fatal_latch`), cleared only by reset-fatal. `ConfigInvalid` is not latched:
    /// fixing the config and restarting is enough.
    FatalReason {
        AuthRejected = "AUTH_REJECTED",
        AccountOwnershipUncertain = "ACCOUNT_OWNERSHIP_UNCERTAIN",
        RelayRejectedByServer = "RELAY_REJECTED_BY_SERVER",
        ConfigInvalid = "CONFIG_INVALID",
    }
);

impl FatalReason {
    /// Python `DURABLE_FATAL` membership.
    pub fn is_durable(self) -> bool {
        !matches!(self, Self::ConfigInvalid)
    }

    /// Reason of a persisted latch (Python `account_view`): `None` for no (or an empty) latch; an
    /// unknown reason stays fatal as `AccountOwnershipUncertain` rather than being cleared.
    pub fn from_latch(latch: Option<&JsonObject>) -> Option<Self> {
        let latch = latch.filter(|l| !l.is_empty())?;
        Some(
            latch
                .get("reason")
                .and_then(|r| r.as_str())
                .and_then(Self::parse)
                .unwrap_or(Self::AccountOwnershipUncertain),
        )
    }
}

str_enum!(StopReason {
    SourceStopped = "source_stopped",
    SourceLeftWhitelist = "source_left_whitelist",
    Disabled = "disabled",
    ExcludedViewer = "excluded_viewer",
    SessionInvalid = "session_invalid",
    OutputDead = "output_dead",
    HoldExpired = "hold_expired",
    CodecMismatch = "codec_mismatch",
    RelayDisplaced = "relay_displaced",
    OwnershipLost = "ownership_lost",
    AuthRejected = "auth_rejected",
    ConnectFailed = "connect_failed",
    ConfigInvalid = "config_invalid",
    Shutdown = "shutdown",
    RoutingChanged = "routing_changed",
    RelayScaledDown = "relay_scaled_down",
});

str_enum!(Liveness {
    Live = "live",
    Dead = "dead",
    Unknown = "unknown",
});

/// `RequestInputKeyframe.method` (Python `"pli" | "rewatch"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyframeMethod {
    Pli,
    Rewatch,
}

impl KeyframeMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pli => "pli",
            Self::Rewatch => "rewatch",
        }
    }
}

/// Target's voice state as seen on the Gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetVoice {
    pub channel_id: Option<u64>,
    pub self_stream: bool,
}

/// One Go Live incarnation of the target. `incarnation` is assigned by the adapter and increments on
/// every create, including stream key reuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceStream {
    pub key: String,
    pub owner_id: u64,
    pub channel_id: u64,
    pub incarnation: i64,
    /// `None` when the paused capability is not verified.
    pub paused: Option<bool>,
    pub unavailable: bool,
}

/// Persisted barrier against the interrupted incarnation (state.json `quarantine`).
#[derive(Debug, Clone, PartialEq)]
pub struct QuarantineRecord {
    pub owner_id: u64,
    pub stream_key: String,
    pub incarnation: i64,
    /// Wall clock, informational only.
    pub since: f64,
}

/// Every input of the source gate predicate, one field per clause of spec §6.
#[derive(Debug, Clone, PartialEq)]
pub struct GateFacts {
    pub desired_enabled: bool,
    pub source_authorization_valid: bool,
    pub gateway_state: GatewayState,
    pub excluded_viewer_ids: HashSet<u64>,
    pub participant_view_fresh: bool,
    pub participants: HashSet<u64>,
    pub quarantined_incarnation: Option<i64>,
    pub configured_target_user_id: u64,
    pub allowed_voice_channel_id: u64,
    pub target: Option<TargetVoice>,
    pub stream: Option<SourceStream>,
    pub current_source_incarnation: Option<i64>,
    pub paused_capability_verified: bool,
    pub has_verified_decodable_frame: bool,
    pub relay_channel_id: Option<u64>,
    pub relay_ownership_valid: bool,
    pub input_liveness: Liveness,
    pub inbound_ready: bool,
    pub outbound_ready: bool,
    pub generation_current: bool,
    pub epoch_current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateDecision {
    pub open: bool,
    /// Names of failed clauses, for status/logs.
    pub failed: Vec<&'static str>,
}

// ---------------------------------------------------------------- events

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Tick,
    EnableChanged {
        enabled: bool,
    },
    RoutingChanged {
        routing: Routing,
    },
    /// Runtime override of `runtime.prejoin_voice` (chat `prejoin on|off`).
    PrejoinChanged {
        on: bool,
    },
    /// The JoinVoice join failed: the relay is not in voice.
    PrejoinFailed {
        detail: String,
    },
    /// The relay's voice connection stayed dead past the grace period (or a join hit a foreign voice
    /// session). `from_prejoin`: a pre-join failed, which says nothing about a session that connects
    /// on its own.
    VoiceLost {
        detail: String,
        foreign_session: bool,
        from_prejoin: bool,
    },
    ResetFatal,
    /// RESUMING on disconnect; FRESH only after RESUMED with the replay fence passed; INVALID on
    /// invalid session.
    GatewayChanged {
        state: GatewayState,
    },
    /// Synced snapshot after READY (new session) or for restart reconciliation. `participants` is
    /// `None` when no reliable participant view exists.
    GatewaySnapshot {
        target: Option<TargetVoice>,
        stream: Option<SourceStream>,
        participants: Option<HashSet<u64>>,
        new_session: bool,
    },
    TargetVoiceChanged {
        target: Option<TargetVoice>,
    },
    StreamStarted {
        stream: SourceStream,
    },
    StreamUpdated {
        stream: SourceStream,
    },
    StreamEnded {
        key: String,
        incarnation: i64,
    },
    ParticipantsChanged {
        participants: Option<HashSet<u64>>,
    },
    /// The relay's own voice state, observed before the library applies it.
    RelayVoiceObserved(RelayVoiceObserved),
    Joined {
        generation: u64,
        voice_session_id: Option<String>,
    },
    Watching {
        generation: u64,
    },
    /// Worker decoded a frame and locked the canvas for `epoch`.
    FirstFrame {
        generation: u64,
        epoch: u64,
        width: u32,
        height: u32,
    },
    OutputReady {
        generation: u64,
        negotiated_video_codec: Option<String>,
    },
    /// `watch_retry`: joined, but the watch failed (e.g. stream_full): retry the watch fast.
    ConnectFailed {
        generation: u64,
        reason: StopReason,
        retryable: bool,
        detail: String,
        watch_retry: bool,
    },
    InboundHealth {
        generation: u64,
        ok: bool,
    },
    OutputHealth {
        generation: u64,
        ok: bool,
    },
    /// Valid new RTP of the target's stream (or a verified SR). `at` is monotonic.
    SourceProgress {
        generation: u64,
        at: f64,
        video: bool,
    },
    /// Worker decoded a new source frame in `epoch` (used to leave HOLD).
    FreshFrame {
        generation: u64,
        epoch: u64,
    },
    WorkerFailed {
        generation: u64,
        epoch: u64,
        detail: String,
    },
    CodecChanged {
        generation: u64,
        negotiated_video_codec: Option<String>,
    },
    AuthRejected,
    ConfigInvalid {
        detail: String,
    },
    Shutdown,
}

/// The relay's own voice state (`Event::RelayVoiceObserved` payload), shared with the ownership
/// detector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayVoiceObserved {
    pub session_id: Option<String>,
    pub channel_id: Option<u64>,
}

// --------------------------------------------------------------- actions

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Always the first action of any stop or hold path.
    CloseGate {
        reason: String,
    },
    /// Publisher may send source content for (generation, epoch), starting with an IDR.
    OpenGate {
        generation: u64,
        epoch: u64,
    },
    /// Gate closed but the output Go Live is kept: publisher sends slate only.
    ShowSlate {
        generation: u64,
        reason: HoldReason,
    },
    /// Join (if needed), watch `stream` and start the worker for `epoch`.
    StartConnect {
        generation: u64,
        epoch: u64,
        stream: SourceStream,
    },
    /// Prepare encoder/slate for the locked canvas, then create_stream.
    CreateOutput {
        generation: u64,
    },
    /// Replace or reset the worker/decoder/encoder; `epoch` is the new epoch.
    RebuildPipeline {
        generation: u64,
        epoch: u64,
    },
    RequestInputKeyframe {
        generation: u64,
        method: KeyframeMethod,
    },
    /// STOPPING steps 2-3: end output, drop buffers, stop worker, stop receiving, and leave voice
    /// when `leave_voice`.
    Teardown {
        generation: u64,
        leave_voice: bool,
        reason: StopReason,
    },
    LeaveVoice {
        reason: String,
    },
    /// Pre-join (`runtime.prejoin_voice`): join the main channel before the target streams.
    JoinVoice,
    SetFatalLatch {
        reason: FatalReason,
        detail: String,
    },
    ClearFatalLatch {
        reset_rejections: bool,
    },
    SetQuarantine {
        record: Option<QuarantineRecord>,
    },
    SetRejections {
        count: u32,
    },
    Exit {
        code: i32,
    },
    /// Emitted before the restart's `StartConnect`: the service points the adapter at the new
    /// routing.
    ApplyRouting {
        routing: Routing,
    },
}

/// What `relayctl status` shows; produced by `Supervisor::status()`.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusSnapshot {
    pub state: RelayState,
    pub desired_enabled: bool,
    pub fatal: Option<String>,
    pub hold_reason: Option<String>,
    pub hold_deadline: Option<f64>,
    pub backoff_until: Option<f64>,
    pub generation: u64,
    pub epoch: u64,
    pub source_incarnation: Option<i64>,
    pub gateway: String,
    pub quarantine: Option<QuarantineRecord>,
    pub consecutive_rejections: u32,
    pub voice_session_baseline: Option<String>,
    pub last_source_progress_age: Option<f64>,
    pub gate_failed: Vec<&'static str>,
}
