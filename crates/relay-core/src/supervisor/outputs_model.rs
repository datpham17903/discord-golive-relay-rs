//! Shared types for extra output accounts (multi-account fan-out). Port of `supervisor/outputs_model.py`.
//!
//! One ingest account watches the target and is driven by the main Supervisor (`machine.rs`).
//! Extra output accounts only join the allowed voice channel and publish their own Go Live carrying
//! the same access units. The `OutputPool` (`outputs.rs`) is a pure, serialized state machine like
//! the Supervisor: `handle(event, now) -> Vec<PoolAction>`. Every event and action names the account
//! (relay user ID) and the ingest `generation` it belongs to, so late callbacks from an earlier
//! session are discarded.
//!
//! An output account may send source content only when the main Supervisor's gate allows
//! (generation, epoch) AND the pool says that account is LIVE.
//!
//! Python dataclass -> Rust mapping: `PoolEvent` and `PoolAction` replace the Python unions; every
//! event dataclass is the variant of the same name. The action variants lose the `Out` prefix
//! (`OutStart` -> `PoolAction::Start`, ... , `OutSetRejections` -> `PoolAction::SetRejections`)
//! because every Python action name shares it. `OutputAccount` is `config::OutputAccount`
//! (re-exported). Types: ids are `u64`, `incarnation` is `i64` (as persisted), `generation`,
//! `epoch` and `attempt` are `u64`, rejection counts are `i64` (as persisted), frozensets are
//! `HashSet`.

use std::collections::HashSet;

pub use crate::config::{AccountRole, OutputAccount};
use crate::control::state::PersistedState;

use super::model::{FatalReason, HoldReason, StopReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputState {
    /// Not in voice.
    Off,
    /// Join + create_stream in flight.
    Joining,
    /// Go Live created with H264, waiting for the main gate.
    Ready,
    /// Publishing source content (or slate while the main session holds).
    Live,
    Stopping,
    Backoff,
    /// Displaced from voice; waits for the source incarnation to end.
    Quarantine,
    /// Durable latch for this account only.
    Fatal,
}

impl OutputState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Joining => "joining",
            Self::Ready => "ready",
            Self::Live => "live",
            Self::Stopping => "stopping",
            Self::Backoff => "backoff",
            Self::Quarantine => "quarantine",
            Self::Fatal => "fatal",
        }
    }
}

/// Per-account part of state.json (`accounts[user_id]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPersisted {
    pub fatal_reason: Option<FatalReason>,
    pub quarantine_incarnation: Option<i64>,
    pub consecutive_rejections: i64,
}

/// Python `account_view`: the persisted record of `user_id` as typed values.
pub fn account_view(st: &PersistedState, user_id: u64) -> AccountPersisted {
    let rec = st.account_record(user_id);
    AccountPersisted {
        fatal_reason: FatalReason::from_latch(rec.fatal_latch.as_ref()),
        quarantine_incarnation: rec.quarantine_incarnation,
        consecutive_rejections: rec.consecutive_rejections,
    }
}

// ---------------------------------------------------------------- events

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolEvent {
    // ---- inputs from the main session
    /// Main Supervisor opened the gate for (generation, epoch) on incarnation `incarnation`.
    SessionLive {
        generation: u64,
        epoch: u64,
        incarnation: i64,
    },
    SessionHold {
        generation: u64,
        reason: HoldReason,
    },
    /// Main session left RELAYING/HOLD for good (STOPPING); `incarnation_ended` when the source stopped.
    SessionEnded {
        generation: u64,
        reason: StopReason,
        incarnation_ended: bool,
    },
    // ---- per-account events
    OutJoined {
        account: u64,
        generation: u64,
        attempt: u64,
        voice_session_id: Option<String>,
    },
    OutReady {
        account: u64,
        generation: u64,
        attempt: u64,
        negotiated_video_codec: Option<String>,
    },
    OutFailed {
        account: u64,
        generation: u64,
        attempt: u64,
        reason: StopReason,
        retryable: bool,
        detail: String,
    },
    /// Server op 14 on the account's output: the negotiated video codec is now `codec`.
    OutCodecChanged {
        account: u64,
        generation: u64,
        attempt: u64,
        codec: String,
    },
    /// A viewer asked the output for a keyframe the library cannot answer; `account` None is the
    /// ingest output.
    OutPli {
        account: Option<u64>,
    },
    OutHealth {
        account: u64,
        generation: u64,
        attempt: u64,
        ok: bool,
    },
    /// The account's own voice state, observed before the library applies it.
    OutVoiceObserved {
        account: u64,
        session_id: Option<String>,
        channel_id: Option<u64>,
    },
    /// Voice participants of the channel the account is assigned to, seen by the account's own client.
    MirrorParticipants {
        account: u64,
        participants: HashSet<u64>,
        channel_id: u64,
    },
    /// Users watching the account's own current Go Live, bot accounts excluded (sent when the list
    /// changes).
    OutViewers {
        account: u64,
        viewers: HashSet<u64>,
    },
    OutAuthRejected {
        account: u64,
    },
    OutTick,
}

// ---------------------------------------------------------------- actions

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolAction {
    /// Join the allowed voice (self_mute), create_stream with the output codecs, play a MediaSource.
    ///
    /// `attempt` is per account and comes back in OutJoined/OutReady/OutFailed/OutHealth, so events
    /// of an earlier attempt still in flight in the same generation are ignored.
    Start {
        account: u64,
        generation: u64,
        attempt: u64,
        channel_id: u64,
    },
    /// The account's publisher may send source content for (generation, epoch), starting with an IDR.
    SetLive {
        account: u64,
        generation: u64,
        epoch: u64,
    },
    Slate {
        account: u64,
        generation: u64,
    },
    /// First action of any stop path for this account.
    CloseGate {
        account: u64,
        reason: &'static str,
    },
    /// End the Go Live, drop buffers, leave voice (always: output accounts never idle in voice).
    Teardown {
        account: u64,
        generation: u64,
        reason: StopReason,
    },
    SetFatal {
        account: u64,
        reason: FatalReason,
        detail: String,
    },
    SetQuarantine {
        account: u64,
        incarnation: Option<i64>,
    },
    SetRejections {
        account: u64,
        count: i64,
    },
}

impl PoolAction {
    /// The account every action names.
    pub fn account(&self) -> u64 {
        match self {
            Self::Start { account, .. }
            | Self::SetLive { account, .. }
            | Self::Slate { account, .. }
            | Self::CloseGate { account, .. }
            | Self::Teardown { account, .. }
            | Self::SetFatal { account, .. }
            | Self::SetQuarantine { account, .. }
            | Self::SetRejections { account, .. } => *account,
        }
    }
}
