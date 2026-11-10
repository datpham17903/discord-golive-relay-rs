//! Port of the audio tests of `tests/worker/test_worker.py` for the passthrough-only worker.
//!
//! Opus input is synthetic: the worker only reads the TOC byte (`opus_packet_info`), so a 20 ms CELT stereo
//! one-frame packet (`0xFC ..`) stands in for `gen_opus` (no libopus encoder needed). Python's `Sink` is a real
//! `BoundedChannel` that the helpers drain.
//!
//! Dropped (transcode/PCM): `test_opus_to_pcm_blocks`; the transcode half of
//! `test_audio_passthrough_forwards_original_bytes_with_transcode_deadlines`; the `Publisher` playback half of
//! the two `*_timed_by_arrival_not_sender_clock` tests (the publisher is ported in task S1).

use std::sync::Arc;

use relay_core::config::{CodecMode, OutputProfile, QualityMode};
use relay_core::worker::ipc::BoundedChannel;
use relay_core::worker::messages::{
    Configure, Egress, EgressAudio, Ingress, IngressAudio, SetOutput, WorkerEvent, WorkerEventKind,
};
use relay_core::worker::process::{Worker, egress_channel, events_channel, ingress_channel};
use relay_media::opus::OPUS_SILENCE;

fn profile() -> OutputProfile {
    OutputProfile {
        mode: QualityMode::Auto,
        max_width: 1280,
        max_height: 720,
        fps: 30,
        fixed_width: 0,
        fixed_height: 0,
        video_bitrate_kbps: 1500,
        keyframe_interval_s: 2.0,
        max_input_pixels: 8_000_000,
        audio_enabled: true,
        codec_mode: CodecMode::H264,
    }
}

struct Rig {
    w: Worker,
    egress: Arc<BoundedChannel<Egress>>,
    events: Arc<BoundedChannel<WorkerEvent>>,
    out: Vec<EgressAudio>,
    seen: Vec<WorkerEvent>,
}

impl Rig {
    fn audio(&mut self) -> Vec<EgressAudio> {
        while let Ok(item) = self.egress.get_nowait() {
            if let Egress::Audio(a) = item {
                self.out.push(a);
            }
        }
        self.out.clone()
    }

    fn events(&mut self, kind: WorkerEventKind) -> Vec<WorkerEvent> {
        while let Ok(e) = self.events.get_nowait() {
            self.seen.push(e);
        }
        self.seen
            .iter()
            .filter(|e| e.kind == kind)
            .cloned()
            .collect()
    }
}

fn make_worker(profile: OutputProfile) -> Rig {
    let egress = Arc::new(BoundedChannel::new(&[
        ("video", 100_000, None),
        ("audio", 100_000, None),
    ]));
    let events = Arc::new(events_channel());
    let mut w = Worker::new(Arc::new(ingress_channel()), egress.clone(), events.clone());
    w.handle(
        Ingress::Configure(Configure {
            generation: 1,
            epoch: 0,
            profile,
        }),
        0.0,
    );
    Rig {
        w,
        egress,
        events,
        out: Vec::new(),
        seen: Vec::new(),
    }
}

/// A 20 ms CELT stereo single-frame Opus packet.
fn gen_opus(packets: usize) -> Vec<Vec<u8>> {
    (0..packets + 5)
        .map(|n| vec![0xFC, n as u8, (n * 7) as u8, 0x55, 0xAA])
        .collect()
}

fn amsg(ssrc: u32, rtp_ts: u32, now: f64, sender: Option<f64>, data: &[u8]) -> Ingress {
    Ingress::Audio(IngressAudio {
        generation: 1,
        epoch: 0,
        ssrc,
        rtp_ts,
        sequence: None,
        mono_received: now,
        rtcp_time_mono: sender,
        data: data.to_vec(),
    })
}

/// Python `_audio_run`: one packet every 20 ms from `t0`, optional sender clock offset.
fn audio_run(
    packets: &[Vec<u8>],
    sender_offset: Option<f64>,
    jitter_s: f64,
    t0: f64,
) -> (Rig, Vec<EgressAudio>) {
    let mut r = make_worker(profile());
    r.w.jitter_s = jitter_s;
    for (i, p) in packets.iter().enumerate() {
        let now = t0 + i as f64 * 0.02;
        r.w.handle(
            amsg(7, (i * 960) as u32, now, sender_offset.map(|o| now + o), p),
            now,
        );
    }
    let audio = r.audio();
    (r, audio)
}

fn approx(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

#[test]
fn test_audio_disabled_profile_emits_nothing() {
    let mut r = make_worker(OutputProfile {
        audio_enabled: false,
        ..profile()
    });
    let now = 1000.0;
    r.w.handle(amsg(7, 0, now, None, &gen_opus(2)[0]), now);
    assert!(r.audio().is_empty());
}

#[test]
fn test_audio_deadline_follows_video_jitter_buffer() {
    // Audio must wait as long as video sits in the jitter buffer, or it leads the picture.
    let packets = gen_opus(20);
    let first_deadline = |jitter_s: f64| {
        let (_, audio) = audio_run(&packets, None, jitter_s, 1000.0);
        audio[0].deadline
    };
    assert!(approx(first_deadline(0.4) - first_deadline(0.0), 0.4, 1e-6));
}

#[test]
fn test_audio_timed_by_arrival_not_sender_clock() {
    // Live: the sender's clock (RTCP / abs-send-time) was seconds off local arrival, so every deadline fell
    // outside the publisher's play window and the relayed audio was silent.
    for sender_offset in [1.0, -0.5, 5.0] {
        let t0 = 1000.0;
        let (_, audio) = audio_run(&gen_opus(40), Some(sender_offset), 0.2, t0);
        assert!(audio.len() >= 30);
        for (k, a) in audio.iter().enumerate() {
            let lead = a.deadline - (t0 + k as f64 * 0.02);
            assert!(
                (0.0..=0.2 + 0.1).contains(&lead),
                "offset {sender_offset} k {k} lead {lead}"
            );
        }
    }
}

#[test]
fn test_audio_does_not_disturb_video_jitter_state() {
    let mut r = make_worker(profile());
    let t0 = 1000.0;
    for (i, p) in gen_opus(40).iter().enumerate() {
        // bursty, late audio arrivals
        let now = t0 + i as f64 * 0.02 + if i % 5 == 0 { 0.3 } else { 0.0 };
        r.w.handle(amsg(7, (i * 960) as u32, now, None, p), now);
    }
    // audio went through its own timing state
    assert!(!r.audio().is_empty() && r.w.audio_offsets.get(&7).is_some_and(|q| !q.is_empty()));
    assert_eq!(r.w.jitter_s, 0.0);
    assert!(r.w.offsets.is_empty() && r.w.lateness.is_empty());
}

#[test]
fn test_audio_passthrough_forwards_original_bytes_with_arrival_deadlines() {
    let packets = gen_opus(40);
    let (_, pt) = audio_run(&packets, None, 0.2, 1000.0);
    assert_eq!(
        pt.iter().map(|a| a.opus.clone()).collect::<Vec<_>>(),
        packets
    );
    // deadline = arrival offset (t0) + rtp seconds + 0.05 playout delay + jitter_s
    for (k, a) in pt.iter().enumerate() {
        assert!(approx(
            a.deadline,
            1000.0 + k as f64 * 0.02 + 0.05 + 0.2,
            1e-9
        ));
        assert_eq!((a.generation, a.epoch), (1, 0));
    }
}

#[test]
fn test_audio_passthrough_timed_by_arrival_not_sender_clock() {
    for sender_offset in [1.0, -0.5, 5.0] {
        let t0 = 1000.0;
        let (_, audio) = audio_run(&gen_opus(40), Some(sender_offset), 0.2, t0);
        assert!(audio.len() >= 40);
        for (k, a) in audio.iter().enumerate() {
            let lead = a.deadline - (t0 + k as f64 * 0.02);
            assert!((0.0..=0.2 + 0.1).contains(&lead), "k {k} lead {lead}");
        }
    }
}

#[test]
fn test_audio_rtp_timestamp_wrap_keeps_deadlines_monotonic() {
    // The 32-bit RTP timestamp wraps after the 5th packet; deadlines must keep advancing by 20 ms.
    let packets = gen_opus(40);
    let base = u32::MAX - 5 * 960 + 1;
    let t0 = 1000.0;
    let mut r = make_worker(profile());
    r.w.jitter_s = 0.2;
    for (i, p) in packets.iter().enumerate() {
        let now = t0 + i as f64 * 0.02;
        let ts = base.wrapping_add((i * 960) as u32);
        r.w.handle(amsg(7, ts, now, None, p), now);
    }
    let audio = r.audio();
    assert_eq!(audio.len(), packets.len());
    assert!(base.wrapping_add((5 * 960) as u32) < base); // the wrap really happens
    for w in audio.windows(2) {
        assert!(w[1].deadline >= w[0].deadline);
        assert!(approx(w[1].deadline - w[0].deadline, 0.02, 1e-6));
    }
    assert!(approx(audio[0].deadline, t0 + 0.05 + 0.2, 1e-9));
}

#[test]
fn test_audio_passthrough_drops_and_counts_unsupported_packets() {
    let ok = vec![0xFC, 1, 2]; // CELT 20 ms stereo, one frame
    let packets: Vec<Vec<u8>> = vec![
        ok.clone(),
        vec![0xF4, 1],         // CELT 10 ms
        vec![0x14, 1],         // SILK 40 ms
        vec![0xFD, 1, 2],      // two 20 ms frames
        vec![],                // no TOC
        OPUS_SILENCE.to_vec(), // the source's own silence frame is forwarded as-is
        vec![0x7C, 1],         // hybrid 20 ms stereo
    ];
    let (mut r, audio) = audio_run(&packets, None, 0.2, 1000.0);
    assert_eq!(
        audio.iter().map(|a| a.opus.clone()).collect::<Vec<_>>(),
        vec![ok, OPUS_SILENCE.to_vec(), vec![0x7C, 1]]
    );
    assert_eq!(r.w.stats.audio_unsupported, 4);
    let deadlines: Vec<f64> = audio.iter().map(|a| a.deadline).collect();
    let mut sorted = deadlines.clone();
    sorted.sort_by(f64::total_cmp);
    assert_eq!(deadlines, sorted);
    r.w.last_stats = Some(0.0);
    r.w.tick(2000.0);
    let stats = r.events(WorkerEventKind::Stats);
    assert_eq!(stats.last().unwrap().data["audio_unsupported"], 4);
}

#[test]
fn test_audio_stale_or_muted_output_is_not_forwarded() {
    let mut r = make_worker(profile());
    let p = gen_opus(1);
    // older than INGRESS_MAX_AGE_S when handled: dropped and counted
    r.w.handle(amsg(7, 0, 1000.0, None, &p[0]), 1000.3);
    assert!(r.audio().is_empty());
    assert_eq!(r.w.stats.audio_dropped, 1);
    // output disabled: nothing is forwarded, nothing is counted
    r.w.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 0,
            enabled: false,
        }),
        1001.0,
    );
    r.w.handle(amsg(7, 960, 1001.0, None, &p[1]), 1001.0);
    assert!(r.audio().is_empty());
    assert_eq!(r.w.stats.audio_dropped, 1);
}

#[test]
fn test_audio_egress_full_counts_dropped() {
    let egress = Arc::new(egress_channel());
    let events = Arc::new(events_channel());
    let mut w = Worker::new(Arc::new(ingress_channel()), egress.clone(), events);
    w.handle(
        Ingress::Configure(Configure {
            generation: 1,
            epoch: 0,
            profile: profile(),
        }),
        0.0,
    );
    for (i, p) in gen_opus(14).iter().enumerate() {
        let now = 1000.0 + i as f64 * 0.02;
        w.handle(amsg(7, (i * 960) as u32, now, None, p), now);
    }
    assert_eq!(egress.pending("audio"), 10);
    assert_eq!(w.stats.audio_dropped, 9);
}
