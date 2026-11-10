//! Pure accounting of a watch probe: what arrived, when, and the final summary object.

use std::collections::BTreeMap;

use relay_media::bitstream::{is_keyframe, probe_dimensions};
use relay_media::codec::{CODEC_ORDER, OPUS_PAYLOAD_TYPE, VoiceCodec};
use serde_json::{Value, json};

/// Ask for a keyframe when none arrived this long after the first video packet.
pub const PLI_AFTER_S: f64 = 2.0;
/// At most one PLI per this interval.
pub const PLI_MIN_INTERVAL_S: f64 = 1.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RtpKind {
    Audio,
    Video,
}

impl RtpKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }
}

/// Primary payload types only: RTX retransmissions and unknown types are not counted.
pub fn classify_payload_type(payload_type: u8) -> Option<RtpKind> {
    if payload_type == OPUS_PAYLOAD_TYPE {
        return Some(RtpKind::Audio);
    }
    CODEC_ORDER
        .iter()
        .filter_map(|name| name.parse::<VoiceCodec>().ok())
        .any(|codec| codec.payload_type() == payload_type)
        .then_some(RtpKind::Video)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirstFrame {
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl FirstFrame {
    pub fn to_json(&self) -> Value {
        json!({"codec": self.codec, "width": self.width, "height": self.height})
    }
}

/// Things worth a log line the first time they happen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Milestone {
    VideoPacket {
        ssrc: u32,
        payload_type: u8,
    },
    AccessUnit {
        ssrc: u32,
        codec: String,
        keyframe: bool,
        bytes: usize,
    },
    Frame(FirstFrame),
}

#[derive(Clone, Debug)]
struct SsrcStats {
    kind: RtpKind,
    rtp_packets: u64,
    frames: u64,
    /// Arrival time of the previous RTP packet and the largest gap so far (input for T_dead
    /// calibration, `scripts/m0/session.py::ReceiveStats`).
    last_rtp_at: Option<f64>,
    max_gap_s: f64,
}

#[derive(Debug, Default)]
pub struct WatchAccount {
    video_packets: u64,
    audio_packets: u64,
    frames: u64,
    keyframes: u64,
    first_video_packet_at: Option<f64>,
    first_au_at: Option<f64>,
    last_au_at: Option<f64>,
    first_frame: Option<FirstFrame>,
    video_ssrc: Option<u32>,
    video_ssrc_requested: bool,
    last_pli_at: Option<f64>,
    pli_requests: u64,
    per_ssrc: BTreeMap<u32, SsrcStats>,
    window: Option<(f64, u64)>,
}

impl WatchAccount {
    pub fn new() -> Self {
        Self::default()
    }

    fn ssrc_entry(&mut self, ssrc: u32, kind: RtpKind) -> &mut SsrcStats {
        self.per_ssrc.entry(ssrc).or_insert(SsrcStats {
            kind,
            rtp_packets: 0,
            frames: 0,
            last_rtp_at: None,
            max_gap_s: 0.0,
        })
    }

    /// One transport-decrypted RTP packet (before DAVE and depacketizing).
    pub fn on_rtp(&mut self, ssrc: u32, payload_type: u8, now: f64) -> Option<Milestone> {
        let kind = classify_payload_type(payload_type)?;
        let entry = self.ssrc_entry(ssrc, kind);
        entry.rtp_packets += 1;
        if let Some(last) = entry.last_rtp_at {
            entry.max_gap_s = entry.max_gap_s.max(now - last);
        }
        entry.last_rtp_at = Some(now);
        match kind {
            RtpKind::Audio => {
                self.audio_packets += 1;
                None
            }
            RtpKind::Video => {
                self.video_packets += 1;
                if self.first_video_packet_at.is_some() {
                    return None;
                }
                self.first_video_packet_at = Some(now);
                self.video_ssrc = Some(ssrc);
                Some(Milestone::VideoPacket { ssrc, payload_type })
            }
        }
    }

    /// One assembled, DAVE-decrypted video access unit.
    pub fn on_access_unit(
        &mut self,
        ssrc: u32,
        codec: &str,
        au: &[u8],
        now: f64,
    ) -> Vec<Milestone> {
        let mut out = Vec::new();
        let keyframe = is_keyframe(codec, au);
        self.ssrc_entry(ssrc, RtpKind::Video).frames += 1;
        self.frames += 1;
        if keyframe {
            self.keyframes += 1;
        }
        if self.first_au_at.is_none() {
            self.first_au_at = Some(now);
            out.push(Milestone::AccessUnit {
                ssrc,
                codec: codec.to_owned(),
                keyframe,
                bytes: au.len(),
            });
        }
        self.last_au_at = Some(now);
        if keyframe && self.first_frame.is_none() {
            let dims = probe_dimensions(codec, au);
            let first = FirstFrame {
                codec: codec.to_owned(),
                width: dims.map(|d| d.0),
                height: dims.map(|d| d.1),
            };
            self.first_frame = Some(first.clone());
            out.push(Milestone::Frame(first));
        }
        out
    }

    /// One delivered Opus packet.
    pub fn on_audio_frame(&mut self, ssrc: u32) {
        self.ssrc_entry(ssrc, RtpKind::Audio).frames += 1;
    }

    /// The video SSRC to `request_video` for, once.
    pub fn take_video_ssrc_to_request(&mut self) -> Option<u32> {
        if self.video_ssrc_requested {
            return None;
        }
        let ssrc = self.video_ssrc?;
        self.video_ssrc_requested = true;
        Some(ssrc)
    }

    /// The SSRC to send a PLI for: no keyframe yet, `PLI_AFTER_S` after the first video packet, and at
    /// most one per `PLI_MIN_INTERVAL_S`.
    pub fn pli_due(&self, now: f64) -> Option<u32> {
        let first = self.first_video_packet_at?;
        if self.keyframes > 0 || now - first < PLI_AFTER_S {
            return None;
        }
        if self
            .last_pli_at
            .is_some_and(|t| now - t < PLI_MIN_INTERVAL_S)
        {
            return None;
        }
        self.video_ssrc
    }

    pub fn note_pli(&mut self, now: f64) {
        self.last_pli_at = Some(now);
        self.pli_requests += 1;
    }

    /// Frames per second since the previous call (the first call measures since `since`).
    pub fn window_fps(&mut self, now: f64, since: f64) -> f64 {
        let (t0, f0) = self.window.unwrap_or((since, 0));
        self.window = Some((now, self.frames));
        let dt = now - t0;
        if dt > 0.0 {
            (self.frames - f0) as f64 / dt
        } else {
            0.0
        }
    }

    /// Access units assembled so far.
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// The video SSRC of the first video packet seen.
    pub fn video_ssrc(&self) -> Option<u32> {
        self.video_ssrc
    }

    /// Average frame rate between the first and the last assembled access unit.
    pub fn fps_avg(&self) -> f64 {
        match (self.first_au_at, self.last_au_at) {
            (Some(first), Some(last)) if self.frames >= 2 && last > first => {
                (self.frames - 1) as f64 / (last - first)
            }
            _ => 0.0,
        }
    }

    /// Fields of the periodic `stats` log line.
    pub fn stats_fields(&self, fps: f64, transport_failures: u64, dave_failures: u64) -> Value {
        let per_ssrc: serde_json::Map<String, Value> = self
            .per_ssrc
            .iter()
            .map(|(ssrc, s)| {
                (
                    ssrc.to_string(),
                    json!({"kind": s.kind.as_str(), "rtp_packets": s.rtp_packets, "frames": s.frames}),
                )
            })
            .collect();
        // `streams["<ssrc>/<kind>"]`, the shape `relayctl calibrate-tdead` reads (cumulative per run)
        let streams: serde_json::Map<String, Value> = self
            .per_ssrc
            .iter()
            .map(|(ssrc, s)| {
                (
                    format!("{ssrc}/{}", s.kind.as_str()),
                    json!({"packets": s.rtp_packets, "max_gap_s": round6(s.max_gap_s)}),
                )
            })
            .collect();
        json!({
            "video_packets": self.video_packets,
            "frames_assembled": self.frames,
            "keyframes": self.keyframes,
            "fps": round2(fps),
            "audio_packets": self.audio_packets,
            "transport_decrypt_failures": transport_failures,
            "dave_decrypt_failures": dave_failures,
            "pli_requests": self.pli_requests,
            "per_ssrc": per_ssrc,
            "streams": streams,
        })
    }
}

fn round6(v: f64) -> f64 {
    (v * 1e6).round() / 1e6
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// What the probe reached, besides the counters.
#[derive(Debug, Default)]
pub struct Progress {
    pub login_user_id: Option<u64>,
    pub ready: bool,
    pub voice_joined: bool,
    pub stream_key_found: bool,
    pub watch_connected: bool,
    pub errors: Vec<String>,
}

/// The single JSON object printed on stdout at exit.
pub fn summary_json(
    progress: &Progress,
    account: &WatchAccount,
    dave_protocol_version: Option<u16>,
    decrypt_failures: u64,
) -> Value {
    json!({
        "login_user_id": progress.login_user_id,
        "ready": progress.ready,
        "voice_joined": progress.voice_joined,
        "stream_key_found": progress.stream_key_found,
        "watch_connected": progress.watch_connected,
        "dave_protocol_version": dave_protocol_version,
        "video_packets": account.video_packets,
        "frames_assembled": account.frames,
        "keyframes": account.keyframes,
        "first_frame": account.first_frame.as_ref().map(FirstFrame::to_json),
        "fps_avg": round2(account.fps_avg()),
        "audio_packets": account.audio_packets,
        "decrypt_failures": decrypt_failures,
        "errors": progress.errors,
    })
}

/// 0 only when at least one frame was assembled.
pub fn exit_code(account: &WatchAccount) -> u8 {
    u8::from(account.frames == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// H.264 IDR slice NAL (type 5) behind a 4-byte start code; no SPS, so no dimensions.
    const IDR: &[u8] = &[0, 0, 0, 1, 0x65, 0x88, 0x84, 0x00];
    /// Non-IDR slice (type 1).
    const DELTA: &[u8] = &[0, 0, 0, 1, 0x41, 0x9A, 0x00, 0x10];

    #[test]
    fn payload_types_classify_by_codec_table() {
        assert_eq!(classify_payload_type(120), Some(RtpKind::Audio));
        for pt in [101, 103, 105, 107, 109] {
            assert_eq!(classify_payload_type(pt), Some(RtpKind::Video), "{pt}");
        }
        for pt in [102, 104, 106, 108, 110, 0, 96, 121] {
            assert_eq!(classify_payload_type(pt), None, "{pt}");
        }
    }

    #[test]
    fn rtp_counts_split_by_kind_and_ssrc() {
        let mut a = WatchAccount::new();
        assert_eq!(
            a.on_rtp(11, 105, 1.0),
            Some(Milestone::VideoPacket {
                ssrc: 11,
                payload_type: 105
            })
        );
        assert_eq!(a.on_rtp(11, 105, 1.1), None);
        assert_eq!(a.on_rtp(22, 120, 1.1), None);
        assert_eq!(a.on_rtp(11, 106, 1.2), None, "RTX is not counted");
        assert_eq!((a.video_packets, a.audio_packets), (2, 1));
        let stats = a.stats_fields(0.0, 0, 0);
        assert_eq!(stats["per_ssrc"]["11"]["rtp_packets"], 2);
        assert_eq!(stats["per_ssrc"]["11"]["kind"], "video");
        assert_eq!(stats["per_ssrc"]["22"]["kind"], "audio");
        // calibrate-tdead input: packets and the largest inter-packet gap per stream
        assert_eq!(stats["streams"]["11/video"]["packets"], 2);
        assert_eq!(stats["streams"]["11/video"]["max_gap_s"], 0.1);
        assert_eq!(stats["streams"]["22/audio"]["max_gap_s"], 0.0);
    }

    #[test]
    fn access_units_count_frames_and_keyframes() {
        let mut a = WatchAccount::new();
        let m = a.on_access_unit(11, "H264", DELTA, 1.0);
        assert_eq!(
            m,
            vec![Milestone::AccessUnit {
                ssrc: 11,
                codec: "H264".into(),
                keyframe: false,
                bytes: DELTA.len()
            }]
        );
        assert!(
            a.first_frame.as_ref().is_none(),
            "first_frame waits for a keyframe"
        );
        let m = a.on_access_unit(11, "H264", IDR, 1.5);
        assert_eq!(
            m,
            vec![Milestone::Frame(FirstFrame {
                codec: "H264".into(),
                width: None,
                height: None
            })]
        );
        assert!(a.on_access_unit(11, "H264", IDR, 2.0).is_empty());
        assert_eq!((a.frames, a.keyframes), (3, 2));
    }

    #[test]
    fn first_frame_carries_probed_dimensions() {
        let slate = relay_media::slate::build_slate(1280, 720, 30, 2.0, 2000).unwrap();
        let au: Vec<u8> = slate.variants[0][0].clone();
        let mut a = WatchAccount::new();
        let m = a.on_access_unit(1, "H264", &au, 0.0);
        let Some(Milestone::Frame(f)) = m.last() else {
            panic!("{m:?}");
        };
        assert_eq!((f.width, f.height), (Some(1280), Some(720)));
    }

    #[test]
    fn fps_average_spans_first_to_last_access_unit() {
        let mut a = WatchAccount::new();
        assert_eq!(a.fps_avg(), 0.0);
        a.on_access_unit(1, "H264", IDR, 10.0);
        assert_eq!(a.fps_avg(), 0.0, "one frame has no rate");
        for i in 1..=30 {
            a.on_access_unit(1, "H264", DELTA, 10.0 + f64::from(i) / 30.0);
        }
        assert!((a.fps_avg() - 30.0).abs() < 1e-9, "{}", a.fps_avg());
    }

    #[test]
    fn window_fps_counts_frames_since_the_previous_call() {
        let mut a = WatchAccount::new();
        for i in 0..50 {
            a.on_access_unit(1, "H264", DELTA, f64::from(i) * 0.1);
        }
        assert!((a.window_fps(5.0, 0.0) - 10.0).abs() < 1e-9);
        for i in 0..25 {
            a.on_access_unit(1, "H264", DELTA, 5.0 + f64::from(i) * 0.2);
        }
        assert!((a.window_fps(10.0, 0.0) - 5.0).abs() < 1e-9);
        assert_eq!(a.window_fps(10.0, 0.0), 0.0, "zero-length window");
    }

    #[test]
    fn video_ssrc_is_requested_once() {
        let mut a = WatchAccount::new();
        assert_eq!(a.take_video_ssrc_to_request(), None);
        a.on_rtp(7, 105, 0.0);
        a.on_rtp(8, 105, 0.1);
        assert_eq!(a.take_video_ssrc_to_request(), Some(7));
        assert_eq!(a.take_video_ssrc_to_request(), None);
    }

    #[test]
    fn pli_waits_two_seconds_then_is_rate_limited() {
        let mut a = WatchAccount::new();
        assert_eq!(a.pli_due(100.0), None, "no video yet");
        a.on_rtp(7, 105, 10.0);
        assert_eq!(a.pli_due(11.9), None);
        assert_eq!(a.pli_due(12.0), Some(7));
        a.note_pli(12.0);
        assert_eq!(a.pli_due(12.9), None);
        assert_eq!(a.pli_due(13.0), Some(7));
        a.note_pli(13.0);
        assert_eq!(a.pli_requests, 2);
    }

    #[test]
    fn pli_stops_once_a_keyframe_arrived() {
        let mut a = WatchAccount::new();
        a.on_rtp(7, 105, 10.0);
        a.on_access_unit(7, "H264", IDR, 11.0);
        assert_eq!(a.pli_due(30.0), None);
    }

    fn progress() -> Progress {
        Progress {
            login_user_id: Some(42),
            ready: true,
            voice_joined: true,
            stream_key_found: true,
            watch_connected: true,
            errors: vec![],
        }
    }

    #[test]
    fn summary_has_the_documented_fields_in_order() {
        let mut a = WatchAccount::new();
        a.on_rtp(7, 105, 0.0);
        a.on_rtp(8, 120, 0.0);
        a.on_access_unit(7, "H264", IDR, 1.0);
        a.on_access_unit(7, "H264", DELTA, 2.0);
        let s = summary_json(&progress(), &a, Some(1), 3);
        let keys: Vec<&str> = s.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "login_user_id",
                "ready",
                "voice_joined",
                "stream_key_found",
                "watch_connected",
                "dave_protocol_version",
                "video_packets",
                "frames_assembled",
                "keyframes",
                "first_frame",
                "fps_avg",
                "audio_packets",
                "decrypt_failures",
                "errors"
            ]
        );
        assert_eq!(s["login_user_id"], 42);
        assert_eq!(s["dave_protocol_version"], 1);
        assert_eq!(s["video_packets"], 1);
        assert_eq!(s["frames_assembled"], 2);
        assert_eq!(s["keyframes"], 1);
        assert_eq!(s["audio_packets"], 1);
        assert_eq!(s["decrypt_failures"], 3);
        assert_eq!(s["fps_avg"], 1.0);
        assert_eq!(s["first_frame"]["codec"], "H264");
        assert_eq!(s["errors"], json!([]));
    }

    #[test]
    fn summary_of_an_empty_run_uses_nulls() {
        let s = summary_json(&Progress::default(), &WatchAccount::new(), None, 0);
        assert_eq!(s["login_user_id"], Value::Null);
        assert_eq!(s["dave_protocol_version"], Value::Null);
        assert_eq!(s["first_frame"], Value::Null);
        assert_eq!(s["ready"], false);
        assert_eq!(s["frames_assembled"], 0);
    }

    #[test]
    fn exit_code_follows_frames_assembled() {
        let mut a = WatchAccount::new();
        a.on_rtp(7, 105, 0.0);
        assert_eq!(exit_code(&a), 1, "packets alone are not enough");
        a.on_access_unit(7, "H264", DELTA, 1.0);
        assert_eq!(exit_code(&a), 0);
    }
}
