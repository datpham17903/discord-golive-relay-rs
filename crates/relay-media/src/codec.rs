//! Codec preference order (best first) and ranking helpers. Port of the top of
//! `golive_relay/media/bitstream.py`.

use std::str::FromStr;

pub const CODEC_ORDER: [&str; 5] = ["AV1", "H265", "H264", "VP8", "VP9"];

/// Opus RTP payload type in the voice codec table.
pub const OPUS_PAYLOAD_TYPE: u8 = 120;

/// A negotiable video codec, in `CODEC_ORDER` order. The `&str` helpers below stay the common API;
/// this is for code that needs the payload types or wants the codec checked once at the edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VoiceCodec {
    AV1,
    H265,
    H264,
    VP8,
    VP9,
}

impl VoiceCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            VoiceCodec::AV1 => "AV1",
            VoiceCodec::H265 => "H265",
            VoiceCodec::H264 => "H264",
            VoiceCodec::VP8 => "VP8",
            VoiceCodec::VP9 => "VP9",
        }
    }

    /// RTP payload type from the discord-native-voice codec table.
    pub fn payload_type(self) -> u8 {
        match self {
            VoiceCodec::AV1 => 101,
            VoiceCodec::H265 => 103,
            VoiceCodec::H264 => 105,
            VoiceCodec::VP8 => 107,
            VoiceCodec::VP9 => 109,
        }
    }

    pub fn rtx_payload_type(self) -> u8 {
        self.payload_type() + 1
    }
}

impl FromStr for VoiceCodec {
    type Err = UnknownCodec;

    fn from_str(s: &str) -> Result<Self, UnknownCodec> {
        match s {
            "AV1" => Ok(VoiceCodec::AV1),
            "H265" => Ok(VoiceCodec::H265),
            "H264" => Ok(VoiceCodec::H264),
            "VP8" => Ok(VoiceCodec::VP8),
            "VP9" => Ok(VoiceCodec::VP9),
            _ => Err(UnknownCodec(s.to_owned())),
        }
    }
}

/// Python `ValueError(f"unknown codec {codec!r}")`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownCodec(pub String);

impl std::fmt::Display for UnknownCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown codec {:?}", self.0)
    }
}

impl std::error::Error for UnknownCodec {}

pub fn rank(codec: &str) -> Result<usize, UnknownCodec> {
    CODEC_ORDER
        .iter()
        .position(|&c| c == codec)
        .ok_or_else(|| UnknownCodec(codec.to_owned()))
}

pub fn better<'a>(a: &'a str, b: &'a str) -> Result<&'a str, UnknownCodec> {
    Ok(if rank(a)? <= rank(b)? { a } else { b })
}

/// Worst-ranked codec of `codecs`; `None` when empty. Ties resolve to the first one (Python `max`).
pub fn lowest<'a>(codecs: &[&'a str]) -> Result<Option<&'a str>, UnknownCodec> {
    let mut best: Option<(usize, &'a str)> = None;
    for &c in codecs {
        let r = rank(c)?;
        if best.is_none_or(|(br, _)| r > br) {
            best = Some((r, c));
        }
    }
    Ok(best.map(|(_, c)| c))
}

pub fn down_from(codec: &str) -> Result<&'static [&'static str], UnknownCodec> {
    Ok(&CODEC_ORDER[rank(codec)?..])
}
