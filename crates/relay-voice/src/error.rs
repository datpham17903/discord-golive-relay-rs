//! Errors of the voice and stream clients.

use std::io;

use crate::link::LinkError;
use crate::transport::CryptoError;
use crate::transport::packets::SendError;
use crate::voice_ws::VoiceWsError;

#[derive(Debug, thiserror::Error)]
pub enum VoiceError {
    #[error(transparent)]
    Ws(#[from] VoiceWsError),
    #[error("timed out {0}")]
    Timeout(&'static str),
    /// The voice websocket ended before the handshake finished.
    #[error("voice connection closed during the handshake (code {code:?}): {reason}")]
    Closed { code: Option<u16>, reason: String },
    #[error("no supported transport encryption mode among {0:?}")]
    NoSupportedMode(Vec<String>),
    #[error("voice udp: {0}")]
    Udp(#[from] io::Error),
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Send(#[from] SendError),
    #[error(transparent)]
    Link(#[from] LinkError),
    #[error("not connected to voice")]
    NotConnected,
    #[error("already listening to media")]
    AlreadyListening,
    #[error("{0}")]
    Invalid(String),
    /// STREAM_DELETE arrived while waiting for STREAM_CREATE.
    #[error("stream connection rejected: {0}")]
    StreamRejected(String),
}
