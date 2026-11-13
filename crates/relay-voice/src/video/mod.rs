// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/video/mod.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       added the `send` module (src/python/video_send.rs), the re-exported
//                `strip_h264_filler_nalus` (src/python/video.rs) and `depacketizer_for`.

mod av1;
mod common;
mod h264;
mod h265;
mod h26x;
mod send;
mod vp8;
mod vp9;

pub use av1::{AV1Depacketizer, AV1Packetizer};
pub(crate) use common::random_u16;
pub use h26x::strip_h264_filler_nalus;
pub use h264::{H264Depacketizer, H264Packetizer};
pub use h265::{H265Depacketizer, H265Packetizer};
pub use send::{
    NackResendSummary, PacketSink, RtxSendSummary, VideoSendPipeline, VideoSendSummary,
};
pub use vp8::{VP8Depacketizer, VP8Packetizer};
pub use vp9::{VP9Depacketizer, VP9Packetizer};

use relay_media::codec::VoiceCodec;

use crate::rtp::{MediaPacket, PacketError};

pub trait VideoPacketizer {
    fn packetize(&self, frame: &[u8], frame_time_ms: f64) -> Result<Vec<MediaPacket>, PacketError>;
}

pub trait VideoDepacketizer {
    fn push_packet(
        &mut self,
        payload: &[u8],
        marker: bool,
        sequence: u16,
        timestamp: u32,
    ) -> Result<Option<Vec<u8>>, PacketError>;

    /// A payload-less (padding) packet: it carries no media but uses up `sequence`, so it must not
    /// read as a lost packet before the next frame.
    fn skip_packet(&mut self, sequence: u16);
}

/// Depacketizer for the codec of an incoming video stream.
pub fn depacketizer_for(codec: VoiceCodec) -> Box<dyn VideoDepacketizer + Send> {
    match codec {
        VoiceCodec::AV1 => Box::new(AV1Depacketizer::new()),
        VoiceCodec::H265 => Box::new(H265Depacketizer::new()),
        VoiceCodec::H264 => Box::new(H264Depacketizer::new()),
        VoiceCodec::VP8 => Box::new(VP8Depacketizer::new()),
        VoiceCodec::VP9 => Box::new(VP9Depacketizer::new()),
    }
}
