//! The seam between the voice clients and the main Discord gateway.
//!
//! The voice join and the Go Live watch are a dance between this crate and the main gateway: the
//! client sends op 4 / op 20 and then waits for the gateway's answers (VOICE_STATE_UPDATE,
//! VOICE_SERVER_UPDATE, STREAM_CREATE, STREAM_SERVER_UPDATE, STREAM_DELETE). `relay-voice` does not
//! depend on `relay-discord`; the service wires the two with this trait and enum.
//!
//! Contract:
//! - Outgoing ([`GatewayLink`]): `voice_state` is op 4, `stream_create` op 18, `stream_watch` op 20,
//!   `stream_delete` op 19 and `stream_set_paused` op 22. The implementor does its own rate limiting; an `Err` aborts the connect in progress.
//! - Incoming ([`LinkEvent`], fed to `VoiceClient::handle_event`): the service forwards, in gateway
//!   order, only the events that concern this account: its own VOICE_STATE_UPDATE (any channel,
//!   `channel_id: None` meaning "left"), the VOICE_SERVER_UPDATE of the guild/channel the client is
//!   joined to, and every STREAM_* dispatch. Events for other users or other streams are ignored.
//!   Events may arrive before the client asks for them (a STREAM_SERVER_UPDATE routinely precedes
//!   the STREAM_CREATE it belongs to); the client keeps what it needs.

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

pub type LinkFuture<'a> = Pin<Box<dyn Future<Output = Result<(), LinkError>> + Send + 'a>>;

#[derive(Debug, Error)]
#[error("gateway link: {0}")]
pub struct LinkError(pub String);

/// A server token: never printed (`Debug` shows `***`).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

/// Op 4 `change_voice_state`: `channel_id: None` leaves voice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VoiceStateRequest {
    pub guild_id: Option<u64>,
    pub channel_id: Option<u64>,
    pub self_mute: bool,
    pub self_deaf: bool,
    pub self_video: bool,
}

/// Op 18 `stream_create`: the Go Live of our own voice channel (`guild_id: None` for a private call).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamCreateRequest {
    pub guild_id: Option<u64>,
    pub channel_id: u64,
}

/// Everything the clients send to the main gateway.
pub trait GatewayLink: Send + Sync {
    /// Op 4 VOICE_STATE_UPDATE.
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_>;
    /// Op 18 STREAM_CREATE: starts our own Go Live in the channel we are connected to.
    fn stream_create(&self, request: StreamCreateRequest) -> LinkFuture<'_>;
    /// Op 20 STREAM_WATCH for `stream_key` (`guild:<guild>:<channel>:<owner>`).
    fn stream_watch(&self, stream_key: &str) -> LinkFuture<'_>;
    /// Op 19 STREAM_DELETE: stops watching `stream_key` (deletes it when it is ours).
    fn stream_delete(&self, stream_key: &str) -> LinkFuture<'_>;
    /// Op 22 STREAM_SET_PAUSED for our own stream.
    fn stream_set_paused(&self, stream_key: &str, paused: bool) -> LinkFuture<'_>;
}

/// Why a stream went away (STREAM_DELETE `reason`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamDelete {
    pub stream_key: String,
    pub reason: String,
    /// The stream is only unavailable (server side hiccup) rather than deleted.
    pub unavailable: bool,
}

/// Gateway dispatches the clients consume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkEvent {
    /// Own VOICE_STATE_UPDATE.
    VoiceStateUpdate {
        channel_id: Option<u64>,
        session_id: Option<String>,
    },
    /// VOICE_SERVER_UPDATE; `endpoint` may be `None` while the server is being allocated.
    VoiceServerUpdate {
        token: Option<Secret>,
        endpoint: Option<String>,
        guild_id: Option<u64>,
        channel_id: Option<u64>,
    },
    /// STREAM_CREATE.
    StreamCreate {
        stream_key: String,
        rtc_server_id: u64,
        rtc_channel_id: u64,
    },
    /// STREAM_SERVER_UPDATE.
    StreamServerUpdate {
        stream_key: String,
        token: Option<Secret>,
        endpoint: Option<String>,
    },
    /// STREAM_UPDATE.
    StreamUpdate {
        stream_key: String,
        paused: Option<bool>,
    },
    /// STREAM_DELETE (also the "unavailable" form).
    StreamDelete(StreamDelete),
}
