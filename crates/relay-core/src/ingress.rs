//! Pure helpers between the receive side and the worker/service: what `DiscordAdapter._video_gap`,
//! `_note_payload_type` and `service._request_pli` / `_need_idr` do, with no knowledge of the voice
//! crate (packets are handed in as plain numbers).

use std::collections::{HashMap, HashSet};

use relay_media::codec::VoiceCodec;

/// `service.py` `PLI_MIN_INTERVAL_S`.
pub const PLI_MIN_INTERVAL_S: f64 = 0.5;
/// `service.py` `OVERFLOW_MIN_INTERVAL_S`.
pub const OVERFLOW_MIN_INTERVAL_S: f64 = 1.0;

/// `service.py` `_request_pli`: at most one PLI per interval; a request inside the interval is not
/// dropped but deferred, once, to the end of the interval (`poll` fires it).
#[derive(Debug)]
pub struct PliLimiter {
    min_interval: f64,
    last: f64,
    pending: bool,
}

impl PliLimiter {
    pub fn new(min_interval: f64) -> Self {
        Self {
            min_interval,
            last: f64::NEG_INFINITY,
            pending: false,
        }
    }

    /// `true`: send a PLI now.
    pub fn request(&mut self, now: f64) -> bool {
        if now - self.last >= self.min_interval {
            self.last = now;
            self.pending = false;
            true
        } else {
            self.pending = true;
            false
        }
    }

    /// `true`: the deferred PLI is due now.
    pub fn poll(&mut self, now: f64) -> bool {
        if self.pending && now - self.last >= self.min_interval {
            self.last = now;
            self.pending = false;
            true
        } else {
            false
        }
    }
}

/// `service.py` `_need_idr`: feedback IDR requests at most once per interval, none deferred.
#[derive(Debug)]
pub struct IntervalGate {
    min_interval: f64,
    last: f64,
}

impl IntervalGate {
    pub fn new(min_interval: f64) -> Self {
        Self {
            min_interval,
            last: f64::NEG_INFINITY,
        }
    }

    pub fn allow(&mut self, now: f64) -> bool {
        if now - self.last < self.min_interval {
            return false;
        }
        self.last = now;
        true
    }
}

/// Per-SSRC bookkeeping of `DiscordAdapter._video_gap` and `_note_payload_type`.
#[derive(Debug, Default)]
pub struct GapTracker {
    end: HashMap<(u32, VoiceCodec), u16>,
    payload_types: HashMap<u32, u8>,
}

impl GapTracker {
    /// RTP sequence numbers are missing between the previous frame of this SSRC and codec and this
    /// one. `sequence` is the last RTP sequence number of the frame, `packet_sequences` those of every
    /// RTP packet that built it.
    pub fn gap(
        &mut self,
        ssrc: u32,
        codec: VoiceCodec,
        sequence: u16,
        packet_sequences: impl IntoIterator<Item = u16>,
    ) -> bool {
        let Some(&prev) = self.end.get(&(ssrc, codec)) else {
            self.end.insert((ssrc, codec), sequence);
            return false;
        };
        let span = sequence.wrapping_sub(prev);
        if span == 0 || span >= 0x8000 {
            return false;
        }
        self.end.insert((ssrc, codec), sequence);
        let seqs: HashSet<u16> = packet_sequences.into_iter().collect();
        !seqs.is_empty() && usize::from(span) > seqs.len()
    }

    /// The payload type of `ssrc` changed from a previous one: `true` when the receive state of the
    /// codec left behind must be dropped (and its sequence memory is forgotten here).
    pub fn payload_type_switched(
        &mut self,
        ssrc: u32,
        codec: VoiceCodec,
        payload_type: u8,
    ) -> bool {
        match self.payload_types.insert(ssrc, payload_type) {
            Some(old) if old != payload_type => {
                self.end.retain(|&(s, c), _| s != ssrc || c == codec);
                true
            }
            _ => false,
        }
    }

    /// Forget everything: a watch starts or ends (`watch` and `stop_watch` clear `_video_end` and
    /// `_video_pt`).
    pub fn clear(&mut self) {
        self.end.clear();
        self.payload_types.clear();
    }
}

/// `rtcp_time` (sender clock, Unix seconds) on the monotonic clock.
pub fn rtcp_to_mono(rtcp_time: Option<f64>, wall_now: f64, mono_now: f64) -> Option<f64> {
    rtcp_time.map(|t| t - wall_now + mono_now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pli_goes_out_at_once_then_at_most_every_half_second() {
        let mut l = PliLimiter::new(PLI_MIN_INTERVAL_S);
        assert!(l.request(10.0));
        assert!(!l.request(10.1), "inside the interval");
        assert!(!l.poll(10.4), "not due yet");
        assert!(
            l.poll(10.5),
            "the deferred one fires at the end of the interval"
        );
        assert!(!l.poll(11.5), "and only once");
        assert!(l.request(11.5));
    }

    #[test]
    fn many_requests_inside_the_interval_defer_a_single_pli() {
        let mut l = PliLimiter::new(0.5);
        assert!(l.request(0.0));
        for t in [0.1, 0.2, 0.3] {
            assert!(!l.request(t));
        }
        assert!(l.poll(0.5));
        assert!(!l.poll(0.6));
    }

    #[test]
    fn an_immediate_pli_clears_a_deferred_one() {
        let mut l = PliLimiter::new(0.5);
        assert!(l.request(0.0));
        assert!(!l.request(0.1));
        assert!(l.request(0.7));
        assert!(!l.poll(1.3), "nothing left to fire");
    }

    #[test]
    fn idr_feedback_is_gated_without_deferral() {
        let mut g = IntervalGate::new(0.25);
        assert!(g.allow(5.0));
        assert!(!g.allow(5.2));
        assert!(g.allow(5.25));
        assert!(!g.allow(5.3));
    }

    #[test]
    fn first_frame_of_an_ssrc_has_no_gap() {
        let mut t = GapTracker::default();
        assert!(!t.gap(1, VoiceCodec::H264, 100, [100]));
    }

    #[test]
    fn a_frame_spanning_more_sequence_numbers_than_it_carried_has_a_gap() {
        let mut t = GapTracker::default();
        t.gap(1, VoiceCodec::H264, 100, [100]);
        // 101..=104: four numbers elapsed, the frame has two packets
        assert!(t.gap(1, VoiceCodec::H264, 104, [103, 104]));
        // contiguous: 105, 106 with two packets
        assert!(!t.gap(1, VoiceCodec::H264, 106, [105, 106]));
    }

    #[test]
    fn sequence_wrap_and_old_frames_are_handled() {
        let mut t = GapTracker::default();
        t.gap(1, VoiceCodec::H264, 0xFFFF, [0xFFFF]);
        assert!(!t.gap(1, VoiceCodec::H264, 0, [0]), "wrap, contiguous");
        // a late frame (backwards) is neither a gap nor does it move the memory
        assert!(!t.gap(1, VoiceCodec::H264, 0xFFF0, [0xFFF0]));
        assert!(!t.gap(1, VoiceCodec::H264, 1, [1]));
        // an empty packet list never reports a gap
        assert!(!t.gap(1, VoiceCodec::H264, 50, []));
    }

    #[test]
    fn gaps_are_tracked_per_ssrc_and_codec() {
        let mut t = GapTracker::default();
        t.gap(1, VoiceCodec::H264, 10, [10]);
        assert!(!t.gap(2, VoiceCodec::H264, 500, [500]));
        assert!(!t.gap(1, VoiceCodec::VP9, 900, [900]));
    }

    #[test]
    fn a_payload_type_switch_forgets_the_other_codecs_sequence() {
        let mut t = GapTracker::default();
        assert!(
            !t.payload_type_switched(1, VoiceCodec::H264, 105),
            "first sight"
        );
        assert!(!t.payload_type_switched(1, VoiceCodec::H264, 105));
        t.gap(1, VoiceCodec::H264, 10, [10]);
        t.gap(1, VoiceCodec::VP9, 20, [20]);
        assert!(t.payload_type_switched(1, VoiceCodec::VP9, 109));
        // H264 was dropped: its next frame is a first frame again, VP9's memory stays
        assert!(!t.gap(1, VoiceCodec::H264, 900, [900]));
        assert!(t.gap(1, VoiceCodec::VP9, 25, [24, 25]));
    }

    #[test]
    fn clear_starts_a_watch_from_scratch() {
        let mut t = GapTracker::default();
        t.payload_type_switched(1, VoiceCodec::H264, 105);
        t.gap(1, VoiceCodec::H264, 10, [10]);
        t.clear();
        assert!(!t.gap(1, VoiceCodec::H264, 20, [20]), "first frame again");
        assert!(
            !t.payload_type_switched(1, VoiceCodec::H264, 109),
            "first sight again"
        );
    }

    #[test]
    fn rtcp_time_maps_onto_the_monotonic_clock() {
        assert_eq!(rtcp_to_mono(None, 1000.0, 5.0), None);
        let mapped = rtcp_to_mono(Some(999.5), 1000.0, 5.0).unwrap();
        assert!((mapped - 4.5).abs() < 1e-9);
    }
}
