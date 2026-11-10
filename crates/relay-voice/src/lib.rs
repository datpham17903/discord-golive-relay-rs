//! Voice transport for the relay: transport crypto, UDP, RTP and the per-codec
//! (de)packetizers (vendored from discord-native-voice, see `NOTICE`), the voice websocket and
//! DAVE, and on top of them the receive pipeline, the voice client (join) and the stream client
//! (watch a Go Live). The main gateway is reached through the [`link`] module's trait.

pub mod connection;
pub mod dave;
pub mod error;
pub mod link;
pub mod publish;
pub mod receive;
pub mod rtcp;
pub mod rtp;
pub mod stream_client;
pub mod transport;
pub mod video;
pub mod voice_client;
pub mod voice_ws;

pub use error::VoiceError;
pub use stream_client::StreamClient;
pub use voice_client::{ClientEvent, JoinOptions, VoiceClient, VoiceClientConfig};
