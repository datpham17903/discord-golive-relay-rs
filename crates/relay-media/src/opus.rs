//! Opus packet helpers shared by the adapter (ingress stats) and the worker (audio passthrough).
//! Port of `golive_relay/media/opus.py`.

/// The frame a sender emits while muted / silent.
pub const OPUS_SILENCE: [u8; 3] = [0xF8, 0xFF, 0xFE];

const SILK_MS: [f64; 4] = [10.0, 20.0, 40.0, 60.0];
const CELT_MS: [f64; 4] = [2.5, 5.0, 10.0, 20.0];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channels {
    Mono,
    Stereo,
}

impl Channels {
    pub fn as_str(self) -> &'static str {
        match self {
            Channels::Mono => "mono",
            Channels::Stereo => "stereo",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Celt,
    Silk,
    Hybrid,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Celt => "celt",
            Mode::Silk => "silk",
            Mode::Hybrid => "hybrid",
        }
    }
}

/// `(duration_ms, channels, mode, frames)` from the Opus TOC byte (RFC 6716 3.1).
pub fn opus_packet_info(payload: &[u8]) -> Option<(f64, Channels, Mode, u32)> {
    let toc = *payload.first()?;
    let config = toc >> 3;
    let (mode, ms) = if config < 12 {
        (Mode::Silk, SILK_MS[usize::from(config & 3)])
    } else if config < 16 {
        (Mode::Hybrid, if config < 14 { 10.0 } else { 20.0 })
    } else {
        (Mode::Celt, CELT_MS[usize::from(config & 3)])
    };
    let code = toc & 3;
    let frames = if code == 3 {
        let frames = u32::from(*payload.get(1)? & 0x3F);
        if frames == 0 {
            return None;
        }
        frames
    } else if code == 0 {
        1
    } else {
        2
    };
    let channels = if (toc >> 2) & 1 == 1 {
        Channels::Stereo
    } else {
        Channels::Mono
    };
    Some((ms * f64::from(frames), channels, mode, frames))
}
