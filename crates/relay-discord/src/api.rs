//! What the service (S2) consumes: the [`Adapter`] of the ingest account and the [`OutputAdapter`]
//! of an extra output account, mirroring the Python adapter API, and the events they emit.
//!
//! Both are object safe (`dyn Adapter`) so the service can be tested with fakes; async methods
//! return boxed futures. Events leave through plain callbacks, called from whichever thread
//! produced them (gateway pump task, UDP dispatcher thread), after the adapter's own locks are
//! released, so a callback may call back into the adapter. They must not block.
//!
//! Crate dependency direction: `relay-discord` depends on `relay-voice`, `relay-core` and
//! `relay-media`; `relay-voice` never depends on `relay-discord` (it reaches the main gateway only
//! through `relay_voice::link`), and `relay-core` knows neither.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use relay_core::control::commands::ChatCommand;
use relay_core::supervisor::model::{Event, SourceStream};
use relay_core::supervisor::outputs_model::PoolEvent;
use relay_core::worker::messages::Ingress;
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;
use relay_voice::VoiceError;
use relay_voice::publish::VideoParams;
use serde_json::Value;

use crate::Token;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ApiFuture<'a, T> = BoxFuture<'a, Result<T, AdapterError>>;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Python `RuntimeError`: the call does not apply in the adapter's current state.
    #[error("{0}")]
    Failed(String),
    #[error(transparent)]
    Voice(#[from] VoiceError),
    #[error(transparent)]
    Discord(#[from] crate::Error),
    /// The token logs in as a different user than the configured one (`AccountMismatch`).
    #[error("logged-in account is not the configured output account")]
    AccountMismatch,
}

impl AdapterError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }

    /// The voice websocket close code behind a failed join (Python `ConnectionClosed.code`), e.g.
    /// 4006 for a session that is no longer valid.
    pub fn close_code(&self) -> Option<u16> {
        match self {
            Self::Voice(VoiceError::Closed { code, .. }) => *code,
            _ => None,
        }
    }
}

/// Everything the ingest adapter reports. `Model` is the supervisor's input (`Event` has no chat
/// variant, on purpose); the others are routed by the service itself.
#[derive(Clone, PartialEq)]
pub enum AdapterEvent {
    /// Gateway, target, stream, participants, relay voice state, source progress and codec events.
    Model(Event),
    /// A prefixed message in the configured guild (and command channel); the service decides who
    /// may use what.
    Chat(ChatCommand),
    /// An event of the output pool raised on the ingest account: a viewer PLI of the ingest output
    /// that the library cannot answer (`OutPli(None)`).
    Pool(PoolEvent),
}

// `ChatCommand` has no `Debug` (message text is never logged), so neither does a chat event.
impl fmt::Debug for AdapterEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(e) => f.debug_tuple("Model").field(e).finish(),
            Self::Chat(c) => write!(f, "Chat(author={}, channel={})", c.author_id, c.channel_id),
            Self::Pool(e) => f.debug_tuple("Pool").field(e).finish(),
        }
    }
}

pub type EventSink = Arc<dyn Fn(AdapterEvent) + Send + Sync>;
pub type PoolEventSink = Arc<dyn Fn(PoolEvent) + Send + Sync>;
/// Called on the UDP dispatcher thread for every packet of the target; must only enqueue.
pub type IngressSink = Arc<dyn Fn(Ingress) + Send + Sync>;
/// `log(name, **fields)`; `fields` is a JSON object.
pub type LogFn = Arc<dyn Fn(&str, Value) + Send + Sync>;

/// Builds the media source of an output for the video codec the server negotiated.
pub type SourceFactory = Box<dyn FnOnce(VoiceCodec) -> Box<dyn FrameSource> + Send>;

/// Called once the gateway echoed the relay's voice state for the channel, before the media
/// connection is made: from then on `watch` may run.
pub type VoiceStateCallback = Box<dyn FnOnce() + Send>;

/// The media an output account plays: ready, or (adaptive) built for the negotiated codec.
pub enum OutputSource {
    Ready(Box<dyn FrameSource>),
    ForCodec(SourceFactory),
}

/// The ingest account: gateway, voice join, watch of the target's Go Live, and its own output.
pub trait Adapter: Send + Sync {
    /// Logs in (a rejected token is reported as `Event::AuthRejected`, not an error) and starts
    /// the gateway.
    fn start(&self, token: Token) -> ApiFuture<'_, ()>;
    /// Ends output, watch and voice, then the gateway. Step errors are logged, not returned.
    fn close(&self) -> BoxFuture<'_, ()>;
    fn set_epoch(&self, epoch: u64);
    /// The routing the adapter follows (`target_id` / `channel_id` of the Python adapter).
    fn set_routing(&self, target_user_id: u64, channel_id: u64);
    fn target_id(&self) -> u64;
    fn channel_id(&self) -> u64;
    /// Synced view of target voice, target stream and participants (`Event::GatewaySnapshot`).
    fn snapshot(&self, new_session: bool) -> Event;

    // ---- chat
    /// Replies without pinging anyone.
    fn reply(&self, channel_id: u64, message_id: u64, text: String) -> ApiFuture<'_, ()>;
    fn voice_channel_ids(&self) -> HashSet<u64>;
    fn display_name(&self, user_id: u64) -> Option<String>;
    fn is_bot(&self, user_id: u64) -> bool;
    fn role_name(&self, role_id: u64) -> Option<String>;

    // ---- voice
    /// Joins the allowed channel muted; returns the voice session id.
    fn join(&self, on_voice_state: Option<VoiceStateCallback>) -> ApiFuture<'_, String>;
    fn voice_alive(&self) -> bool;
    /// Session id of the last own voice state seen.
    fn last_vsu_session(&self) -> Option<String>;
    /// The last own voice state carried a session id other than this gateway session's.
    fn foreign_voice_session(&self) -> bool;
    fn leave(&self) -> ApiFuture<'_, ()>;

    // ---- watch
    /// Watches exactly `stream` (key and incarnation must match the tracked one).
    fn watch(&self, generation: u64, epoch: u64, stream: SourceStream) -> ApiFuture<'_, ()>;
    /// Narrows or widens the codecs the target may send; `false` (no-op) when not watching.
    fn update_watch_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool>;
    fn request_pli(&self) -> ApiFuture<'_, ()>;
    /// Drops and re-establishes the watch of the same stream (forces a keyframe).
    fn rewatch(&self) -> ApiFuture<'_, ()>;
    fn stop_watch(&self) -> ApiFuture<'_, ()>;

    // ---- output
    /// Creates the output Go Live; returns the negotiated video codec. A viewer PLI becomes
    /// `AdapterEvent::Pool(OutPli)`.
    fn create_output(&self, generation: u64, fps: u32) -> ApiFuture<'_, Option<String>>;
    /// Creates the output offering every codec; op 14 becomes `Event::CodecChanged`, a viewer PLI
    /// the library cannot answer `AdapterEvent::Pool(OutPli)`.
    fn create_output_adaptive(&self, generation: u64, fps: u32) -> ApiFuture<'_, Option<String>>;
    /// The video codec negotiated for the output right now.
    fn output_codec(&self) -> Option<String>;
    fn update_output_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool>;
    /// Swaps the running output to a source for the currently negotiated codec; returns the bound
    /// codec, `None` when nothing could be swapped (logged).
    fn swap_output_source(&self, make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>>;
    fn play(&self, source: Box<dyn FrameSource>, params: VideoParams) -> Result<(), AdapterError>;
    fn end_output(&self) -> ApiFuture<'_, ()>;

    // ---- stats
    /// Target audio counters since the last call.
    fn take_audio_window(&self) -> Value;
    /// Loss/NACK/RTX counters of the watch and output connections (empty when disabled).
    fn network_stats(&self) -> Value;
}

/// An extra output account: it only joins the allowed channel and publishes its own Go Live.
/// Every call that can fail returns an error, which the service turns into `OutFailed`; `end` is
/// safe after a partial `publish`.
pub trait OutputAdapter: Send + Sync {
    fn user_id(&self) -> u64;
    /// Logs in and waits for READY. A rejected token is reported as `OutAuthRejected` (nothing is
    /// left running); a token of another account is `AdapterError::AccountMismatch`.
    fn start(&self, token: Token) -> ApiFuture<'_, ()>;
    /// Joins `channel_id`, creates the Go Live and plays `source`; returns the negotiated codec.
    /// The frame rate of the stream is `params.fps`.
    fn publish(
        &self,
        generation: u64,
        attempt: u64,
        source: OutputSource,
        params: VideoParams,
        channel_id: u64,
        adaptive: bool,
    ) -> ApiFuture<'_, Option<String>>;
    /// Offers the voice server a new codec list for this output; `false` (no-op) without one.
    fn update_codecs<'a>(&'a self, names: &'a [&'a str]) -> ApiFuture<'a, bool>;
    fn swap_source(&self, make_source: SourceFactory) -> BoxFuture<'_, Option<VoiceCodec>>;
    /// Stops the output first, then disconnects the stream and leaves voice.
    fn end(&self) -> ApiFuture<'_, ()>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn network_stats(&self) -> Value;
}
