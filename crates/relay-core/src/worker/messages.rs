//! IPC messages between the supervisor side and the passthrough worker thread (port of `worker/messages.py`).
//!
//! All times are monotonic seconds (`f64`). Every message carries generation and epoch; the receiver
//! drops anything whose pair is not current.

use serde_json::{Map, Value};

pub use crate::config::OutputProfile;

/// 20 ms, 48 kHz, stereo, s16le.
pub const AUDIO_FRAME_BYTES: usize = 3840;

// ---------------------------------------------------------- main -> worker

#[derive(Debug, Clone, PartialEq)]
pub struct IngressVideo {
    pub generation: u64,
    pub epoch: u64,
    /// "H264" | "H265" | "VP8" | "VP9" | "AV1" (anything else is a malformed packet the worker must skip).
    pub codec: String,
    pub ssrc: u32,
    pub rtp_ts: u32,
    pub sequence: Option<u16>,
    pub mono_received: f64,
    /// Presentation time from SR, mapped to monotonic.
    pub rtcp_time_mono: Option<f64>,
    pub data: Vec<u8>,
    /// The library dropped frame(s) just before this one (sequence hole).
    pub gap: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngressAudio {
    pub generation: u64,
    pub epoch: u64,
    pub ssrc: u32,
    pub rtp_ts: u32,
    pub sequence: Option<u16>,
    pub mono_received: f64,
    pub rtcp_time_mono: Option<f64>,
    /// One Opus packet.
    pub data: Vec<u8>,
}

/// Start a session pipeline for (generation, epoch).
#[derive(Debug, Clone, PartialEq)]
pub struct Configure {
    pub generation: u64,
    pub epoch: u64,
    pub profile: OutputProfile,
}

/// New epoch: drop buffered state, wait for an input keyframe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetPipeline {
    pub generation: u64,
    pub epoch: u64,
}

/// `enabled == false`: keep receiving, emit nothing, clear the retained frame.
/// `enabled == true`: emit again starting with a keyframe received after this message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetOutput {
    pub generation: u64,
    pub epoch: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForceIdr {
    pub generation: u64,
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopWorker;

/// Everything the worker accepts (Python dispatches on `isinstance`).
#[derive(Debug, Clone, PartialEq)]
pub enum Ingress {
    Video(IngressVideo),
    Audio(IngressAudio),
    Configure(Configure),
    ResetPipeline(ResetPipeline),
    SetOutput(SetOutput),
    ForceIdr(ForceIdr),
    StopWorker(StopWorker),
}

// ---------------------------------------------------------- worker -> main

#[derive(Debug, Clone, PartialEq)]
pub struct EgressVideo {
    pub generation: u64,
    pub epoch: u64,
    /// Annex B access unit (H.264/H.265) or the codec's own access unit.
    pub data: Vec<u8>,
    /// Keyframe.
    pub is_idr: bool,
    /// From the output schedule.
    pub frame_time_ms: f64,
    /// Monotonic time this AU is due.
    pub deadline: f64,
    /// Monotonic arrival of the source frame it shows.
    pub source_frame_at: Option<f64>,
    pub repeated: bool,
    /// Python default is "H264".
    pub codec: String,
}

/// Audio is always passthrough, so there is no PCM field.
#[derive(Debug, Clone, PartialEq)]
pub struct EgressAudio {
    pub generation: u64,
    pub epoch: u64,
    pub deadline: f64,
    /// One 20 ms Opus packet as received.
    pub opus: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerEventKind {
    FirstFrame,
    FreshFrame,
    SourceCodec,
    SourceIneligible,
    NeedKeyframe,
    /// Passthrough stopped forwarding until the next entry point; `reason` says why.
    KeyWait,
    /// A keyframe was forwarded; `bytes` is its size.
    Keyframe,
    DecodeError,
    Overflow,
    Stats,
    Fatal,
}

impl WorkerEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FirstFrame => "first_frame",
            Self::FreshFrame => "fresh_frame",
            Self::SourceCodec => "source_codec",
            Self::SourceIneligible => "source_ineligible",
            Self::NeedKeyframe => "need_keyframe",
            Self::KeyWait => "key_wait",
            Self::Keyframe => "keyframe",
            Self::DecodeError => "decode_error",
            Self::Overflow => "overflow",
            Self::Stats => "stats",
            Self::Fatal => "fatal",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerEvent {
    pub generation: u64,
    pub epoch: u64,
    pub kind: WorkerEventKind,
    pub data: Map<String, Value>,
}

/// Everything the worker emits.
#[derive(Debug, Clone, PartialEq)]
pub enum Egress {
    Video(EgressVideo),
    Audio(EgressAudio),
}

macro_rules! impl_from_ingress {
    ($($variant:ident($ty:ty)),+ $(,)?) => {$(
        impl From<$ty> for Ingress {
            fn from(m: $ty) -> Self {
                Self::$variant(m)
            }
        }
    )+};
}

impl_from_ingress!(
    Video(IngressVideo),
    Audio(IngressAudio),
    Configure(Configure),
    ResetPipeline(ResetPipeline),
    SetOutput(SetOutput),
    ForceIdr(ForceIdr),
    StopWorker(StopWorker),
);
