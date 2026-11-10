//! Port of `tests/media/test_publisher.py` (Opus-only; no PCM mode).
//!
//! The slate is the M3 gray GOP (`build_slate(64, 64, 10, 1.0, 100)`), not an x264 encode.
//!
//! `test_media_source_contract` and `test_opus_source_passes_source_silence_and_never_empty` are ported
//! against `PublisherSource` (the `relay_media::FrameSource` the V4 sender pulls from); the
//! `VideoConfig` / `has_audio` / `read_preview` parts of `RelayMediaSource` belong to the Python
//! `native_voice` package and have no counterpart.
//! `test_audio_queue_bounded_and_silence`, `test_audio_held_until_deadline`,
//! `test_audio_window_counts_played_held_late_and_evicted` and `test_close_purges` are ported in Opus mode with
//! `OPUS_SILENCE` as silence. `test_h265_waits_for_entry_point` uses the real H.265 fixtures instead of
//! monkeypatching `is_entry_point` (so the "only H265 was asked" assertion has no counterpart).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::Mutex;
use relay_core::publisher::{
    CATCHUP_FACTOR, MAX_EGRESS_AUS, MAX_PCM_BLOCKS, Publisher, PublisherSource, SendSnapshot,
    SendWindow, freezes_since, rates,
};
use relay_core::worker::messages::{EgressAudio, EgressVideo};
use relay_media::FrameSource;
use relay_media::h264::{idr_pic_id, is_idr};
use relay_media::opus::OPUS_SILENCE;
use relay_media::slate::{SlateGop, build_slate};

fn slate() -> Arc<SlateGop> {
    Arc::new(build_slate(64, 64, 10, 1.0, 100).unwrap())
}

fn approx(a: f64, b: f64) {
    assert!((a - b).abs() < 1e-6, "{a} != {b}");
}

struct Env {
    now: Arc<Mutex<f64>>,
    gate_open: Arc<AtomicBool>,
    idr_requests: Arc<AtomicUsize>,
    slate: Arc<SlateGop>,
    pub_: Publisher,
}

impl Env {
    fn new() -> Self {
        let now = Arc::new(Mutex::new(100.0));
        let gate_open = Arc::new(AtomicBool::new(true));
        let idr_requests = Arc::new(AtomicUsize::new(0));
        let (g, c, r) = (gate_open.clone(), now.clone(), idr_requests.clone());
        let pub_ = Publisher::new(
            Box::new(move |_, _, _| g.load(Ordering::SeqCst)),
            Box::new(move || *c.lock()),
            0.2,
            Box::new(move || {
                r.fetch_add(1, Ordering::SeqCst);
            }),
            "H264",
        );
        Env {
            now,
            gate_open,
            idr_requests,
            slate: slate(),
            pub_,
        }
    }

    fn now(&self) -> f64 {
        *self.now.lock()
    }

    fn advance(&self, dt: f64) {
        *self.now.lock() += dt;
    }

    fn set_gate(&self, open: bool) {
        self.gate_open.store(open, Ordering::SeqCst);
    }

    fn idr_requests(&self) -> usize {
        self.idr_requests.load(Ordering::SeqCst)
    }

    fn reset_idr_requests(&self) {
        self.idr_requests.store(0, Ordering::SeqCst);
    }

    fn au(&self, data: &[u8], is_idr: bool, generation: u64, epoch: u64, late: f64) -> EgressVideo {
        EgressVideo {
            generation,
            epoch,
            data: data.to_vec(),
            is_idr,
            frame_time_ms: 100.0,
            deadline: self.now() - late,
            source_frame_at: None,
            repeated: false,
            codec: "H264".to_owned(),
        }
    }

    fn au1(&self, data: &[u8], is_idr: bool) -> EgressVideo {
        self.au(data, is_idr, 1, 1, 0.0)
    }

    fn codec_au(&self, data: &[u8], codec: &str, is_idr: bool) -> EgressVideo {
        EgressVideo {
            codec: codec.to_owned(),
            ..self.au1(data, is_idr)
        }
    }

    fn audio(&self, data: &[u8], deadline: f64) -> EgressAudio {
        EgressAudio {
            generation: 1,
            epoch: 1,
            deadline,
            opus: data.to_vec(),
        }
    }
}

fn silence() -> Vec<u8> {
    OPUS_SILENCE.to_vec()
}

fn fixture(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../relay-media/tests/fixtures/codecs")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("fixture {}: {e}", p.display()))
}

#[test]
fn test_gate_false_only_slate_or_none() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    e.set_gate(false);
    assert!(e.pub_.next_video().is_none());
    e.pub_.set_slate(Some(s.clone()));
    let f = e.pub_.next_video().unwrap();
    assert_eq!(f.data, s.variants[0][0]);
    assert_eq!(e.pub_.next_video().unwrap().data, s.variants[0][1]);
}

#[test]
fn test_first_source_au_non_idr_discarded() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.au1(&s.variants[0][1], false));
    assert!(e.pub_.next_video().is_none());
    assert_eq!(e.idr_requests(), 1);
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    assert_eq!(e.pub_.next_video().unwrap().data, s.variants[0][0]);
}

#[test]
fn test_stale_epoch_and_generation_dropped() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(2, 5);
    e.pub_.offer_video(e.au(&s.variants[0][0], true, 2, 4, 0.0));
    e.pub_.offer_video(e.au(&s.variants[0][0], true, 1, 5, 0.0));
    assert!(e.pub_.next_video().is_none());
    e.pub_.offer_video(e.au(&s.variants[0][0], true, 2, 5, 0.0));
    assert!(e.pub_.next_video().is_some());
}

#[test]
fn test_late_au_dropped_until_next_idr() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    assert!(e.pub_.next_video().is_some());
    e.pub_
        .offer_video(e.au(&s.variants[0][1], false, 1, 1, 0.5));
    assert!(e.pub_.next_video().is_none());
    assert_eq!(e.idr_requests(), 1);
    e.pub_.offer_video(e.au1(&s.variants[0][2], false));
    assert!(e.pub_.next_video().is_none());
    e.pub_.offer_video(e.au1(&s.variants[1][0], true));
    assert_eq!(e.pub_.next_video().unwrap().data, s.variants[1][0]);
}

#[test]
fn test_adjacency_inserts_slate_p_frame() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.set_slate(Some(s.clone()));
    e.set_gate(false);
    let f = e.pub_.next_video().unwrap(); // slate IDR, id A
    assert!(is_idr(&f.data));
    e.pub_.set_slate(None);
    e.pub_.set_slate(Some(s.clone()));
    // source IDR with the same id as the last sent IDR must not follow it directly
    let last_id = idr_pic_id(&f.data);
    let same = s
        .variants
        .iter()
        .map(|v| &v[0])
        .find(|d| idr_pic_id(d) == last_id)
        .unwrap();
    e.set_gate(true);
    e.pub_.offer_video(e.au1(same, true));
    let out = e.pub_.next_video().unwrap();
    assert!(!is_idr(&out.data));
    assert_eq!(&e.pub_.next_video().unwrap().data, same);
}

#[test]
fn test_source_idr_then_same_id_source_idr_sent_directly() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    let idr = &s.variants[0][0];
    e.pub_.offer_video(e.au1(idr, true));
    assert_eq!(&e.pub_.next_video().unwrap().data, idr);
    e.pub_
        .offer_video(e.au(&s.variants[0][1], false, 1, 1, 0.5)); // dropped as late
    assert!(e.pub_.next_video().is_none());
    e.pub_.offer_video(e.au1(idr, true));
    let out = e.pub_.next_video().unwrap();
    assert_eq!(&out.data, idr);
}

#[test]
fn test_different_idr_id_sent_directly() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.set_slate(Some(s.clone()));
    e.set_gate(false);
    let f = e.pub_.next_video().unwrap();
    let other = s
        .variants
        .iter()
        .map(|v| &v[0])
        .find(|d| idr_pic_id(d) != idr_pic_id(&f.data))
        .unwrap();
    e.set_gate(true);
    e.pub_.offer_video(e.au1(other, true));
    assert_eq!(&e.pub_.next_video().unwrap().data, other);
}

#[test]
fn test_small_burst_does_not_force_idr() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    for _ in 0..4 {
        e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    }
    assert_eq!(e.idr_requests(), 0);
    assert!(e.pub_.next_video().is_some());
}

#[test]
fn test_video_overflow_clears_and_requests_idr() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    for _ in 0..=MAX_EGRESS_AUS {
        e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    }
    assert_eq!(e.idr_requests(), 1);
    assert!(e.pub_.next_video().is_none());
}

#[test]
fn test_audio_queue_bounded_and_silence() {
    let e = Env::new();
    e.pub_.set_source(1, 1);
    assert_eq!(e.pub_.next_audio(), silence());
    for i in 0..MAX_PCM_BLOCKS + 10 {
        e.pub_.offer_audio(e.audio(&[i as u8 + 1; 3], e.now()));
    }
    let first = e.pub_.next_audio();
    assert_eq!(first[0], 11); // oldest 10 frames dropped: the queue keeps MAX_PCM_BLOCKS
    e.set_gate(false);
    assert_eq!(e.pub_.next_audio(), silence());
}

#[test]
fn test_close_purges() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.set_slate(Some(s.clone()));
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    e.pub_.offer_audio(e.audio(&[1; 3], e.now()));
    e.pub_.close();
    assert!(e.pub_.next_video().is_none());
    assert_eq!(e.pub_.next_audio(), silence());
}

#[test]
fn test_media_source_contract() {
    let e = Env::new();
    let s = e.slate.clone();
    let publisher = Arc::new(Publisher::new(
        Box::new(|_, _, _| true),
        Box::new(|| 100.0),
        0.2,
        Box::new(|| {}),
        "H264",
    ));
    let mut src = PublisherSource(publisher.clone());
    assert!(!src.next_audio().is_empty());
    assert!(src.next_video().is_none());
    publisher.set_source(1, 1);
    publisher.offer_video(e.au1(&s.variants[0][0], true));
    let frame = src.next_video().unwrap();
    assert_eq!(frame.data, s.variants[0][0]);
    assert_eq!(frame.frame_time_ms, 100.0);
    assert!(src.next_video().is_none());
}

#[test]
fn test_opus_source_passes_source_silence_and_never_empty() {
    let e = Env::new();
    let mut src = PublisherSource(Arc::new(Publisher::new(
        Box::new(|_, _, _| true),
        Box::new(|| 100.0),
        0.2,
        Box::new(|| {}),
        "H264",
    )));
    assert_eq!(src.next_audio(), silence());
    // the publisher itself is a source too
    e.pub_.set_source(1, 1);
    e.pub_.offer_audio(e.audio(&OPUS_SILENCE, e.now()));
    let mut direct = e.pub_;
    assert_eq!(FrameSource::next_audio(&mut direct), silence());
    assert!((0..5).all(|_| !src.next_audio().is_empty()));
}

#[test]
fn test_audio_held_until_deadline() {
    let e = Env::new();
    e.pub_.set_source(1, 1);
    e.pub_.offer_audio(e.audio(&[1; 3], e.now() + 0.1));
    assert_eq!(e.pub_.next_audio(), silence());
    e.advance(0.1);
    assert_eq!(e.pub_.next_audio()[0], 1);
}

#[test]
fn test_backlog_drains_faster_then_returns_to_normal_pace() {
    // Live G2b: with an 8-AU queue at a 60 fps source and 60 fps sender, a burst left a standing
    // backlog (~+40 ms latency); the sender must speed up while frames are queued.
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    for p in &s.variants[0][1..4] {
        e.pub_.offer_video(e.au1(p, false));
    }
    let times: Vec<f64> = (0..4)
        .map(|_| e.pub_.next_video().unwrap().frame_time_ms)
        .collect();
    approx(times[0], 100.0 * CATCHUP_FACTOR); // 3 still queued
    approx(times[1], 100.0 * CATCHUP_FACTOR); // 2 still queued
    approx(times[2], 100.0); // 1 queued: normal pace
    approx(times[3], 100.0);
}

#[test]
fn test_send_gap_metrics_count_stalls_over_50ms() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    for (i, gap) in [0.0, 0.016, 0.2, 0.016].into_iter().enumerate() {
        e.advance(gap);
        let data = if i == 0 {
            &s.variants[0][0]
        } else {
            &s.variants[0][1]
        };
        e.pub_.offer_video(e.au1(data, i == 0));
        assert!(e.pub_.next_video().is_some());
    }
    let w = e.pub_.take_window();
    assert!(w.sent == 4 && w.stalls == 1);
    approx(w.max_send_gap_ms, 200.0);
    assert_eq!(e.pub_.take_window(), SendWindow::default());
}

#[test]
fn test_audio_window_counts_played_held_late_and_evicted() {
    let e = Env::new();
    e.pub_.set_source(1, 1);
    e.pub_.offer_audio(e.audio(&[1; 3], e.now() - 1.0)); // too late
    e.pub_.offer_audio(e.audio(&[2; 3], e.now()));
    e.pub_.offer_audio(e.audio(&[3; 3], e.now() + 0.5)); // head in the future
    assert_eq!(e.pub_.next_audio()[0], 2);
    assert_eq!(e.pub_.next_audio(), silence());
    for _ in 0..MAX_PCM_BLOCKS + 2 {
        e.pub_.offer_audio(e.audio(&[4; 3], e.now() + 1.0));
    }
    let w = e.pub_.take_window();
    assert_eq!(
        (
            w.audio_played,
            w.audio_held_reads,
            w.audio_late_dropped,
            w.audio_evicted
        ),
        (1, 1, 1, 3)
    );
    assert_eq!(e.pub_.take_window().audio_played, 0);
}

fn opus(e: &Env, i: u8, deadline: f64) -> EgressAudio {
    e.audio(&[0xFC, i], deadline)
}

#[test]
fn test_opus_publisher_plays_frames_at_deadline_and_silence_otherwise() {
    let e = Env::new();
    assert_eq!(e.pub_.next_audio(), silence()); // no source: gate closed
    e.pub_.set_source(1, 1);
    assert_eq!(e.pub_.next_audio(), silence()); // nothing queued
    e.pub_.offer_audio(opus(&e, 1, e.now() - 1.0)); // too late
    e.pub_.offer_audio(opus(&e, 2, e.now()));
    e.pub_.offer_audio(opus(&e, 3, e.now() + 0.1));
    assert_eq!(e.pub_.next_audio(), vec![0xFC, 2]);
    assert_eq!(e.pub_.next_audio(), silence()); // holding for a future deadline
    e.advance(0.1);
    assert_eq!(e.pub_.next_audio(), vec![0xFC, 3]);
    for i in 0..MAX_PCM_BLOCKS + 2 {
        e.pub_.offer_audio(opus(&e, i as u8, e.now() + 1.0));
    }
    let w = e.pub_.take_window();
    assert_eq!(
        (
            w.audio_played,
            w.audio_held_reads,
            w.audio_late_dropped,
            w.audio_evicted
        ),
        (2, 1, 1, 2)
    );
    e.set_gate(false);
    assert_eq!(e.pub_.next_audio(), silence());
    e.pub_.close();
    assert_eq!(e.pub_.next_audio(), silence());
}

#[test]
fn test_drops_other_codec_aus() {
    let e = Env::new();
    e.pub_.set_source(1, 1);
    e.pub_
        .offer_video(e.codec_au(&[0, 0, 1, 0x40], "H265", false));
    assert!(e.pub_.next_video().is_none());
    assert_eq!(e.idr_requests(), 0);
}

#[test]
fn test_h265_waits_for_entry_point() {
    let e = Env::new();
    let (key, delta) = (
        fixture("h265_1280x720_key.bin"),
        fixture("h265_1280x720_delta.bin"),
    );
    e.pub_.set_codec("H265");
    e.reset_idr_requests();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.codec_au(&delta, "H265", false));
    assert!(e.pub_.next_video().is_none());
    assert_eq!(e.idr_requests(), 1);
    e.pub_.offer_video(e.codec_au(&key, "H265", true));
    e.pub_.offer_video(e.codec_au(&delta, "H265", false));
    assert_eq!(e.pub_.next_video().unwrap().data, key);
    assert_eq!(e.pub_.next_video().unwrap().data, delta);
}

#[test]
fn test_no_slate_when_codec_not_h264() {
    let e = Env::new();
    e.pub_.set_slate(Some(e.slate.clone()));
    e.pub_.set_source(1, 1);
    e.set_gate(false);
    assert!(e.pub_.next_video().is_some());
    e.pub_.set_codec("H265");
    assert!(e.pub_.next_video().is_none());
}

#[test]
fn test_set_codec_clears_queue_and_awaits_entry() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    e.pub_.set_codec("H265");
    assert_eq!(e.pub_.codec(), "H265");
    assert_eq!(e.idr_requests(), 1); // the new codec needs an entry point from the worker
    assert!(!e.pub_.slate_iter_active());
    assert!(e.pub_.next_video().is_none());
    e.pub_.set_codec("H264");
    e.pub_.offer_video(e.au1(&s.variants[0][1], false));
    assert!(e.pub_.next_video().is_none()); // non-entry discarded
    e.pub_.offer_video(e.au1(&s.variants[0][0], true));
    assert_eq!(e.pub_.next_video().unwrap().data, s.variants[0][0]);
}

#[test]
fn test_snapshot_counts_frames_bytes_and_freezes_and_is_not_reset() {
    let e = Env::new();
    let s = e.slate.clone();
    e.pub_.set_source(1, 1);
    let mut size = 0;
    for (i, gap) in [0.0, 0.016, 0.3, 0.016].into_iter().enumerate() {
        e.advance(gap);
        let au = if i == 0 {
            &s.variants[0][0]
        } else {
            &s.variants[0][1]
        };
        size += au.len() as u64;
        e.pub_.offer_video(e.au1(au, i == 0));
        assert!(e.pub_.next_video().is_some());
    }
    e.pub_.take_window();
    let snap = e.pub_.snapshot();
    assert_eq!((snap.frames, snap.bytes, snap.freezes), (4, size, 1));
    approx(snap.frozen_s, 0.3);
    let hist = [
        (0.0, SendSnapshot::default()),
        (
            5.0,
            SendSnapshot {
                frames: 10,
                bytes: 1000,
                freezes: 0,
                frozen_s: 0.0,
            },
        ),
    ];
    let cur = SendSnapshot {
        frames: 20,
        bytes: 3000,
        freezes: 2,
        frozen_s: 0.6,
    };
    let (kbps, fps) = rates(&hist, 10.0, &cur, 10.0).unwrap();
    approx(kbps, 2.4);
    approx(fps, 2.0);
    assert!(rates(&[], 10.0, &cur, 10.0).is_none());
    assert_eq!(freezes_since(&hist, 10.0, &cur, 300.0), 2);
    assert_eq!(freezes_since(&hist, 10.0, &cur, 5.0), 2);
}
