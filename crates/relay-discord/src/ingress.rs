//! From a received media [`Packet`] to the worker's ingress messages, and the ingress-side audio
//! counters (`DiscordAdapter._on_packet`, `_count_audio`, `take_audio_window`).

use std::collections::HashMap;

use relay_core::worker::messages::{IngressAudio, IngressVideo};
use relay_media::opus::{OPUS_SILENCE, opus_packet_info};
use relay_voice::receive::Packet;
use serde_json::{Map, Value, json};

pub fn ingress_video(
    packet: &Packet,
    (generation, epoch): (u64, u64),
    mono_received: f64,
    rtcp_mono: Option<f64>,
    gap: bool,
) -> IngressVideo {
    IngressVideo {
        generation,
        epoch,
        codec: packet.codec.name().to_owned(),
        ssrc: packet.ssrc,
        rtp_ts: packet.timestamp,
        sequence: Some(packet.sequence),
        mono_received,
        rtcp_time_mono: rtcp_mono,
        data: packet.payload.clone(),
        gap,
    }
}

pub fn ingress_audio(
    packet: &Packet,
    (generation, epoch): (u64, u64),
    mono_received: f64,
    rtcp_mono: Option<f64>,
) -> IngressAudio {
    IngressAudio {
        generation,
        epoch,
        ssrc: packet.ssrc,
        rtp_ts: packet.timestamp,
        sequence: Some(packet.sequence),
        mono_received,
        rtcp_time_mono: rtcp_mono,
        data: packet.payload.clone(),
    }
}

/// Target audio counters since the last [`AudioStats::take`]: packets, Opus silence frames, and the
/// format of the rest (no decoding).
#[derive(Debug, Default)]
pub struct AudioStats {
    packets: u64,
    silence: u64,
    bytes: u64,
    bad: u64,
    seq_gaps: u64,
    seq_back: u64,
    multi_frame: u64,
    /// Duration (ms, as text) -> packets.
    durations: Vec<(String, u64)>,
    stereo: u64,
    mono: u64,
    celt: u64,
    silk: u64,
    hybrid: u64,
    /// Last in-order sequence number per SSRC; kept across windows, like `_audio_last_seq`.
    last_seq: HashMap<u32, u16>,
}

impl AudioStats {
    /// One audio packet of the target.
    pub fn count(&mut self, ssrc: u32, seq: u16, data: &[u8]) {
        self.packets += 1;
        let last = self.last_seq.get(&ssrc).copied();
        self.last_seq.insert(ssrc, last.unwrap_or(seq));
        if let Some(last) = last {
            let delta = seq.wrapping_sub(last);
            if delta == 0 || delta >= 0x8000 {
                self.seq_back += 1;
            } else {
                self.seq_gaps += u64::from(delta) - 1;
                self.last_seq.insert(ssrc, seq);
            }
        }
        if data == OPUS_SILENCE {
            self.silence += 1;
            return;
        }
        let Some((ms, channels, mode, frames)) = opus_packet_info(data) else {
            self.bad += 1;
            return;
        };
        self.bytes += data.len() as u64;
        let key = format!("{ms}");
        match self.durations.iter_mut().find(|(k, _)| *k == key) {
            Some((_, n)) => *n += 1,
            None => self.durations.push((key, 1)),
        }
        match channels.as_str() {
            "stereo" => self.stereo += 1,
            _ => self.mono += 1,
        }
        match mode.as_str() {
            "celt" => self.celt += 1,
            "silk" => self.silk += 1,
            _ => self.hybrid += 1,
        }
        if frames > 1 {
            self.multi_frame += 1;
        }
    }

    /// The window so far as the status object (`kbps` over `elapsed_s`), and a fresh window.
    pub fn take(&mut self, elapsed_s: f64) -> Value {
        let w = std::mem::take(self);
        self.last_seq = w.last_seq.clone();
        let kbps = if elapsed_s > 0.0 {
            round_half_even(w.bytes as f64 * 8.0 / elapsed_s / 1000.0)
        } else {
            0
        };
        let durations: Map<String, Value> = w
            .durations
            .iter()
            .map(|(k, n)| (k.clone(), json!(n)))
            .collect();
        json!({
            "packets": w.packets,
            "silence": w.silence,
            "bytes": w.bytes,
            "bad": w.bad,
            "seq_gaps": w.seq_gaps,
            "seq_back": w.seq_back,
            "multi_frame": w.multi_frame,
            "durations": durations,
            "channels": {"stereo": w.stereo, "mono": w.mono},
            "modes": {"celt": w.celt, "silk": w.silk, "hybrid": w.hybrid},
            "kbps": kbps,
        })
    }
}

/// Python `round()` to an integer.
pub(crate) fn round_half_even(x: f64) -> i64 {
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 && r as i64 % 2 != 0 {
        (r - x.signum()) as i64
    } else {
        r as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_counts_silence_and_formats_and_restarts() {
        let mut s = AudioStats::default();
        let celt20 = [&[0xFCu8][..], &[0; 9]].concat();
        s.count(8, 1, &celt20);
        s.count(8, 2, &OPUS_SILENCE);
        let w = s.take(2.0);
        assert_eq!(
            (w["packets"].as_u64(), w["silence"].as_u64()),
            (Some(2), Some(1))
        );
        assert_eq!(w["bytes"], 10);
        assert_eq!(w["kbps"], 0, "40 bit/s rounds to 0 kbps");
        let w = s.take(1.0);
        assert_eq!(
            (w["packets"].as_u64(), w["silence"].as_u64()),
            (Some(0), Some(0))
        );
        // sequence memory survives the window
        s.count(8, 3, &celt20);
        assert_eq!(s.take(1.0)["seq_gaps"], 0);
    }

    #[test]
    fn python_rounding_is_half_to_even() {
        assert_eq!(round_half_even(2.5), 2);
        assert_eq!(round_half_even(3.5), 4);
        assert_eq!(round_half_even(7.4), 7);
        assert_eq!(round_half_even(0.5), 0);
        assert_eq!(round_half_even(-0.5), 0);
        assert_eq!(round_half_even(-1.5), -2);
        assert_eq!(round_half_even(-4.6), -5);
    }
}
