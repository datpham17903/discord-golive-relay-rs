//! Port of the video tests of `tests/worker/test_worker.py` for the passthrough-only worker.
//!
//! Sans-IO: `Worker::handle`/`tick` take a fake clock. Inputs are real keyframes from the shared fixtures plus
//! synthetic P-frame access units (the worker only looks at NAL types, SPS size and the codec's entry-point
//! markers, so no encoder is needed). Python's `Sink` becomes a real `BoundedChannel` that the helpers drain.
//!
//! Semantics that differ from Python (passthrough only, no transcode):
//! - an ineligible source (over `max_*`, not `fixed_*`, over `max_input_pixels`, or not H.264 outside adaptive
//!   mode) emits `source_ineligible` once per epoch and forwards nothing until an eligible entry point arrives;
//! - the tests that exercised the decoder/encoder/canvas/PCM paths are dropped (see the task report).

use std::path::PathBuf;
use std::sync::Arc;

use relay_core::clock::monotonic;
use relay_core::config::{CodecMode, OutputProfile, QualityMode};
use relay_core::worker::ipc::BoundedChannel;
use relay_core::worker::messages::{
    Configure, Egress, EgressVideo, ForceIdr, Ingress, IngressVideo, ResetPipeline, SetOutput,
    StopWorker, WorkerEvent, WorkerEventKind,
};
use relay_core::worker::process::{
    ADAPT_MAX_S, ADAPT_RELEASE, ADAPT_WINDOW_S, JITTER_MAX_FRAMES, Worker, WorkerHandle,
    egress_channel, events_channel, h264_is_idr, ingress_channel,
};
use relay_media::h264::{ensure_low_delay_vui, nal_units};
use serde_json::{Value, json};

const FPS: usize = 30;
const T0: f64 = 1000.0;

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

fn small() -> OutputProfile {
    OutputProfile {
        max_width: 320,
        max_height: 240,
        ..profile()
    }
}

fn adaptive() -> OutputProfile {
    OutputProfile {
        codec_mode: CodecMode::Adaptive,
        ..profile()
    }
}

fn adaptive_small() -> OutputProfile {
    OutputProfile {
        codec_mode: CodecMode::Adaptive,
        ..small()
    }
}

// ------------------------------------------------------------------ fixtures

fn media_fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../relay-media/tests/fixtures")
}

fn fixture(rel: &str) -> Vec<u8> {
    std::fs::read(media_fixtures().join(rel)).unwrap_or_else(|e| panic!("fixture {rel}: {e}"))
}

/// A 320x240 H.264 IDR access unit with SPS/PPS.
fn idr240() -> Vec<u8> {
    fixture("idr_320x240.bin")
}

fn idr180() -> Vec<u8> {
    fixture("idr_320x180.bin")
}

fn idr720() -> Vec<u8> {
    fixture("codecs/h264_1280x720_key.bin")
}

fn p_frame(i: usize) -> Vec<u8> {
    vec![
        0,
        0,
        0,
        1,
        0x41,
        0x9a,
        0x80 | (i % 128) as u8,
        0xa5,
        0xa5,
        0xa5,
        0xa5,
    ]
}

/// H.264 stream: `idr` at every 60th AU (keyint = 2 s at 30 fps), synthetic P-frames in between.
fn h264_stream(idr: &[u8], n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            if i % (FPS * 2) == 0 {
                idr.to_vec()
            } else {
                p_frame(i)
            }
        })
        .collect()
}

/// One keyframe/entry point followed by delta AUs of a 1280x720 fixture codec.
fn codec_stream(codec: &str, n: usize) -> Vec<Vec<u8>> {
    let name = codec.to_lowercase();
    let key = fixture(&format!("codecs/{name}_1280x720_key.bin"));
    let delta = fixture(&format!("codecs/{name}_1280x720_delta.bin"));
    (0..n)
        .map(|i| if i == 0 { key.clone() } else { delta.clone() })
        .collect()
}

fn nal_types(au: &[u8]) -> Vec<u8> {
    nal_units(au).iter().map(|&(t, _)| t).collect()
}

// ------------------------------------------------------------------ rig

struct Rig {
    w: Worker,
    egress: Arc<BoundedChannel<Egress>>,
    events: Arc<BoundedChannel<WorkerEvent>>,
    out: Vec<EgressVideo>,
    seen: Vec<WorkerEvent>,
}

impl Rig {
    fn video(&mut self) -> Vec<EgressVideo> {
        while let Ok(item) = self.egress.get_nowait() {
            match item {
                Egress::Video(v) => self.out.push(v),
                Egress::Audio(_) => {}
            }
        }
        self.out.clone()
    }

    fn all_events(&mut self) -> Vec<WorkerEvent> {
        while let Ok(e) = self.events.get_nowait() {
            self.seen.push(e);
        }
        self.seen.clone()
    }

    fn events(&mut self, kind: WorkerEventKind) -> Vec<WorkerEvent> {
        self.all_events()
            .into_iter()
            .filter(|e| e.kind == kind)
            .collect()
    }

    fn kinds(&mut self, wanted: &[WorkerEventKind]) -> Vec<WorkerEventKind> {
        self.all_events()
            .into_iter()
            .map(|e| e.kind)
            .filter(|k| wanted.contains(k))
            .collect()
    }

    fn handle(&mut self, msg: impl Into<Ingress>, now: f64) -> bool {
        self.w.handle(msg.into(), now)
    }
}

fn rig_with(profile: OutputProfile, egress: Arc<BoundedChannel<Egress>>) -> Rig {
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

/// Python's `Sink` without a capacity: nothing is ever full.
fn roomy_egress() -> Arc<BoundedChannel<Egress>> {
    Arc::new(BoundedChannel::new(&[
        ("video", 100_000, None),
        ("audio", 100_000, None),
    ]))
}

fn make_worker(profile: OutputProfile) -> Rig {
    rig_with(profile, roomy_egress())
}

fn vmsg(
    generation: u64,
    epoch: u64,
    codec: &str,
    ssrc: u32,
    rtp_ts: u32,
    now: f64,
    data: Vec<u8>,
) -> IngressVideo {
    IngressVideo {
        generation,
        epoch,
        codec: codec.to_string(),
        ssrc,
        rtp_ts,
        sequence: None,
        mono_received: now,
        rtcp_time_mono: None,
        data,
        gap: false,
    }
}

/// Deliver AUs at `FPS` on a fake clock, ticking at every arrival.
fn feed_codec(
    r: &mut Rig,
    codec: &str,
    aus: &[Vec<u8>],
    t0: f64,
    epoch: u64,
    start: usize,
    ssrc: u32,
) -> f64 {
    let mut now = t0;
    for (i, au) in aus.iter().enumerate() {
        now = t0 + i as f64 / FPS as f64;
        let rtp = ((start + i) * 3000) as u32;
        r.handle(vmsg(1, epoch, codec, ssrc, rtp, now, au.clone()), now);
        r.w.tick(now);
    }
    now
}

fn feed(r: &mut Rig, aus: &[Vec<u8>], t0: f64) -> f64 {
    feed_codec(r, "H264", aus, t0, 0, 0, 1)
}

fn feed_from(r: &mut Rig, aus: &[Vec<u8>], t0: f64, epoch: u64, start: usize) -> f64 {
    feed_codec(r, "H264", aus, t0, epoch, start, 1)
}

fn drain(r: &mut Rig, now: f64) {
    for k in 0..80 {
        r.w.tick(now + 0.001 * (k + 1) as f64);
    }
}

fn data_of(e: &WorkerEvent) -> Value {
    Value::Object(e.data.clone())
}

fn datas(v: &[EgressVideo]) -> Vec<&[u8]> {
    v.iter().map(|x| x.data.as_slice()).collect()
}

use WorkerEventKind::{
    FirstFrame, FreshFrame, KeyWait, Keyframe, NeedKeyframe, Overflow, SourceCodec,
    SourceIneligible, Stats,
};

// ------------------------------------------------------------------ passthrough

#[test]
fn test_passthrough_forwards_au_unchanged_without_codecs() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus, T0);
    drain(&mut r, now);
    let out = r.video();
    let expected: Vec<Vec<u8>> = aus
        .iter()
        .map(|a| {
            if h264_is_idr(a) {
                ensure_low_delay_vui(a)
            } else {
                a.clone()
            }
        })
        .collect();
    assert_eq!(
        datas(&out),
        expected.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert_eq!(
        out.iter().map(|v| v.is_idr).collect::<Vec<_>>(),
        aus.iter().map(|a| h264_is_idr(a)).collect::<Vec<_>>()
    );
    assert!(out.iter().all(|v| v.codec == "H264" && !v.repeated));
    assert!(r.w.pt_active);
    assert_eq!(r.w.stats.passthrough_frames, aus.len() as u64);
    let ff = r.events(FirstFrame);
    assert_eq!(ff.len(), 1);
    assert_eq!(ff[0].data["encoder"], "passthrough");
    assert_eq!(ff[0].data["decode"], "none");
    assert_eq!(ff[0].data["frame_format"], "h264");
    assert_eq!(
        (ff[0].data["width"].as_u64(), ff[0].data["height"].as_u64()),
        (Some(320), Some(240))
    );
}

#[test]
fn test_passthrough_waits_for_idr_with_sps_and_requests_keyframe() {
    let idr = idr240();
    let aus = h264_stream(&idr, 60);
    let mut r = make_worker(profile());
    let now = feed_from(&mut r, &aus[1..6], T0, 0, 1);
    drain(&mut r, now);
    assert!(r.video().is_empty() && !r.events(NeedKeyframe).is_empty());
    assert!(r.events(FirstFrame).is_empty());
    let mut sps_less = vec![0, 0, 0, 1];
    for (t, n) in nal_units(&idr) {
        if t == 5 {
            sps_less.extend_from_slice(&[0, 0, 0, 1]);
            sps_less.extend_from_slice(n);
        }
    }
    let now = feed_from(&mut r, &[sps_less], now + 0.1, 0, 6);
    drain(&mut r, now);
    assert!(r.video().is_empty() && r.w.need_key);
    let mut again = vec![idr.clone()];
    again.extend_from_slice(&aus[1..3]);
    feed_from(&mut r, &again, now + 0.2, 0, 7);
    drain(&mut r, now + 1.0);
    let out = r.video();
    assert!(!out.is_empty() && out[0].is_idr && !r.w.need_key);
}

/// Python: oversize IDR falls back to transcode and returns. Passthrough only: the oversized source is not
/// relayed (`source_ineligible`, nothing forwarded); an eligible entry point resumes forwarding.
#[test]
fn test_passthrough_oversized_idr_falls_back_to_transcode_and_returns() {
    let a = h264_stream(&idr240(), 30);
    let b = h264_stream(&idr720(), 30);
    let c = h264_stream(&idr240(), 30);
    let mut r = make_worker(small());
    let now = feed(&mut r, &a, T0);
    drain(&mut r, now);
    assert!(r.w.pt_active);
    let n_a = r.video().len();
    assert_eq!(n_a, a.len());

    let now = feed_from(&mut r, &b, now + 0.5, 0, 100);
    drain(&mut r, now);
    assert!(!r.w.pt_active);
    assert_eq!(
        r.video().len(),
        n_a,
        "nothing of the oversized source is forwarded"
    );
    assert_eq!(r.events(FirstFrame).len(), 1);
    let ineligible = r.events(SourceIneligible);
    assert_eq!(ineligible.len(), 1);
    assert_eq!(
        data_of(&ineligible[0]),
        json!({"codec": "H264", "width": 1280, "height": 720})
    );

    let now = feed_from(&mut r, &c, now + 0.5, 0, 200);
    drain(&mut r, now);
    assert!(r.w.pt_active);
    let all = r.video();
    let out_c = &all[n_a..];
    assert!(out_c[0].is_idr);
    assert_eq!(
        datas(&out_c[1..]),
        c[1..].iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert_eq!(r.events(SourceIneligible).len(), 1);
}

#[test]
fn test_passthrough_jbuf_overflow_clears_and_requests_keyframe() {
    let idr = idr240();
    let aus = h264_stream(&idr, JITTER_MAX_FRAMES + 7);
    let mut r = make_worker(profile());
    for (i, au) in aus.iter().enumerate() {
        r.handle(vmsg(1, 0, "H264", 1, (i * 3000) as u32, T0, au.clone()), T0); // never ticked
    }
    assert!(r.w.jbuf.is_empty() && r.w.need_key && !r.events(NeedKeyframe).is_empty());
    let rtp = ((JITTER_MAX_FRAMES + 7) * 3000) as u32;
    r.handle(vmsg(1, 0, "H264", 1, rtp, T0, idr), T0);
    assert!(!r.w.need_key && r.w.jbuf.len() == 1);
}

#[test]
fn test_passthrough_egress_full_waits_without_dropping() {
    let egress = Arc::new(BoundedChannel::new(&[
        ("video", 1, None),
        ("audio", 10, None),
    ]));
    let aus = h264_stream(&idr240(), 60);
    let mut r = rig_with(profile(), egress);
    let now = feed(&mut r, &aus[..5], T0);
    let out = r.video(); // the main process polled
    assert!(out.len() == 1 && out[0].is_idr);
    assert!(!r.w.need_key && r.w.jbuf.len() == 4 && r.events(NeedKeyframe).is_empty());
    for k in 0..80 {
        r.w.tick(now + 0.001 * (k + 1) as f64);
        r.video(); // the main process keeps polling
    }
    assert!(r.video().len() == 5 && !r.w.need_key);
}

#[test]
fn test_passthrough_egress_byte_overflow_drops_au_requests_keyframe_and_emits_event() {
    // full() stays false (items) but put_nowait rejects the AU on the byte limit.
    let egress = Arc::new(BoundedChannel::new(&[
        ("video", 2, Some(10)),
        ("audio", 10, None),
    ]));
    let aus = h264_stream(&idr240(), 60);
    let mut r = rig_with(profile(), egress);
    feed(&mut r, &aus[..5], T0);
    assert!(r.video().is_empty());
    assert!(r.w.stats.egress_dropped > 0);
    assert!(r.w.need_key && r.w.jbuf.is_empty());
    assert_eq!(r.events(Overflow).len(), 1);
    assert_eq!(r.events(Overflow)[0].data["channel"], "egress_video");
}

#[test]
fn test_passthrough_does_not_request_periodic_keyframe() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..59], T0); // 1.9 s of P-frames after the IDR (> 1.7 s)
    drain(&mut r, now);
    assert!(r.events(NeedKeyframe).is_empty());
    assert!(!r.w.need_key && r.video().len() == 59);
}

#[test]
fn test_passthrough_force_idr_requests_keyframe() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..3], T0);
    r.handle(
        Ingress::ForceIdr(ForceIdr {
            generation: 1,
            epoch: 0,
        }),
        now,
    );
    assert_eq!(r.events(NeedKeyframe).len(), 1);
}

/// Python: other codecs go through the transcode path. Passthrough only: outside adaptive mode a non-H.264
/// source is ineligible (`source_ineligible`), never forwarded.
#[test]
fn test_passthrough_mode_uses_transcode_for_other_codecs() {
    let mut r = make_worker(profile());
    let now = T0;
    // not an entry point: nothing to decide yet, a keyframe is requested
    r.handle(
        vmsg(1, 0, "H265", 1, 0, now, vec![0, 0, 0, 1, 0x40, 0x01, 0x00]),
        now,
    );
    assert!(!r.w.pt_active && r.video().is_empty());
    assert!(r.events(FirstFrame).is_empty() && !r.events(NeedKeyframe).is_empty());
    assert!(r.events(SourceIneligible).is_empty());

    let aus = codec_stream("H265", 5);
    feed_codec(&mut r, "H265", &aus, now + 0.1, 0, 1, 1);
    drain(&mut r, now + 1.0);
    assert!(!r.w.pt_active && r.video().is_empty());
    assert!(r.events(FirstFrame).is_empty());
    let ineligible = r.events(SourceIneligible);
    assert_eq!(ineligible.len(), 1);
    assert_eq!(
        data_of(&ineligible[0]),
        json!({"codec": "H265", "width": 1280, "height": 720})
    );
    assert!(
        r.events(SourceCodec).is_empty(),
        "source_codec is adaptive-only"
    );
}

// ------------------------------------------------------------------ ineligible sources (new)

#[test]
fn ineligible_source_emits_once_per_epoch_and_forwards_nothing() {
    let aus = h264_stream(&idr720(), 130); // IDRs at 0, 60, 120: three entry points
    let mut r = make_worker(small());
    let now = feed(&mut r, &aus, T0);
    drain(&mut r, now);
    assert!(r.video().is_empty() && !r.w.pt_active && r.w.canvas.is_none());
    assert!(r.events(FirstFrame).is_empty() && r.events(NeedKeyframe).is_empty());
    assert_eq!(r.events(SourceIneligible).len(), 1);

    // a new epoch reports again, once
    r.handle(
        Ingress::ResetPipeline(ResetPipeline {
            generation: 1,
            epoch: 1,
        }),
        now,
    );
    let now = feed_from(&mut r, &aus[..70], now + 0.1, 1, 200);
    drain(&mut r, now);
    assert!(r.video().is_empty());
    let events = r.events(SourceIneligible);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].epoch, 1);
    assert_eq!(
        data_of(&events[1]),
        json!({"codec": "H264", "width": 1280, "height": 720})
    );
}

#[test]
fn eligible_entry_point_after_ineligible_resumes_forwarding() {
    let big = h264_stream(&idr720(), 10);
    let idr = idr240();
    let mut r = make_worker(small());
    let now = feed(&mut r, &big, T0);
    // the eligible IDR starts the relay
    let mut resume = vec![idr.clone()];
    resume.extend((1..10).map(p_frame));
    let now = feed_from(&mut r, &resume, now + 0.1, 0, 100);
    drain(&mut r, now);
    let out = r.video();
    assert!(r.w.pt_active && out.len() == resume.len());
    assert!(out[0].is_idr);
    assert_eq!(
        datas(&out[1..]),
        resume[1..].iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert_eq!(r.events(FirstFrame).len(), 1);
    assert_eq!(r.events(SourceIneligible).len(), 1);
    assert_eq!(r.w.canvas, Some((320, 240)));
}

#[test]
fn test_max_input_pixels_enforced() {
    let prof = OutputProfile {
        max_input_pixels: 50_000,
        ..profile()
    };
    let mut r = make_worker(prof);
    feed(&mut r, &h264_stream(&idr240(), 10), T0);
    assert!(r.w.canvas.is_none() && r.video().is_empty());
    let ev = r.events(SourceIneligible);
    assert_eq!(ev.len(), 1);
    assert_eq!(
        data_of(&ev[0]),
        json!({"codec": "H264", "width": 320, "height": 240})
    );
}

#[test]
fn test_fixed_mode_requires_exact_size() {
    let prof = OutputProfile {
        mode: QualityMode::Fixed,
        fixed_width: 320,
        fixed_height: 240,
        ..profile()
    };
    let mut r = make_worker(prof.clone());
    feed(&mut r, &h264_stream(&idr240(), 5), T0);
    assert!(r.w.pt_active && r.events(SourceIneligible).is_empty());
    let mut r = make_worker(prof);
    feed(&mut r, &h264_stream(&idr180(), 5), T0); // fits max_* but is not the fixed size
    assert!(!r.w.pt_active && r.video().is_empty());
    assert_eq!(r.events(SourceIneligible).len(), 1);
}

#[test]
fn test_empty_and_truncated_au_are_skipped_and_request_keyframe() {
    let idr = idr240();
    for profile in [profile(), adaptive()] {
        let mut r = make_worker(profile);
        for (i, data) in [
            vec![],
            vec![0],
            vec![0, 0, 1],
            idr[..12].to_vec(), // cut inside the SPS
            idr[..6].to_vec(),
        ]
        .into_iter()
        .enumerate()
        {
            let now = T0 + i as f64 * 0.01;
            r.handle(vmsg(1, 0, "H264", 1, 0, now, data), now);
            r.w.tick(now);
        }
        assert!(r.video().is_empty() && !r.w.pt_active);
        assert!(r.events(FirstFrame).is_empty());
        assert!(!r.events(NeedKeyframe).is_empty());
    }
    for codec in ["H265", "AV1", "VP8", "VP9"] {
        let mut r = make_worker(adaptive());
        for data in [vec![], vec![0x12], vec![0, 0, 1, 0x40]] {
            r.handle(vmsg(1, 0, codec, 1, 0, T0, data), T0);
        }
        assert!(r.video().is_empty() && !r.w.pt_active, "{codec}");
        assert!(!r.events(NeedKeyframe).is_empty(), "{codec}");
    }
}

// ------------------------------------------------------------------ output / epoch / stale

#[test]
fn test_canvas_locked_on_resize() {
    let mut r = make_worker(profile());
    let now = feed(&mut r, &h264_stream(&idr240(), 60), T0);
    let now = feed_from(
        &mut r,
        &h264_stream(&idr180(), 30),
        now + 1.0 / FPS as f64,
        0,
        60,
    );
    drain(&mut r, now);
    assert_eq!(r.w.canvas, Some((320, 240)));
    assert_eq!(r.events(FirstFrame).len(), 1);
    assert_eq!(
        r.video().len(),
        90,
        "the smaller source is still eligible and forwarded"
    );
}

#[test]
fn test_set_output_false_stops_egress_and_true_waits_for_fresh_frame() {
    let aus = h264_stream(&idr240(), 180);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..60], T0);
    let n = r.video().len();
    assert!(n > 0);
    r.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 0,
            enabled: false,
        }),
        now,
    );
    let now = feed_from(&mut r, &aus[60..120], now + 1.0 / FPS as f64, 0, 60);
    assert_eq!(r.video().len(), n); // receiving continues, nothing emitted
    assert!(r.w.jbuf.is_empty());
    r.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 0,
            enabled: true,
        }),
        now,
    );
    r.w.tick(now + 0.5);
    assert_eq!(r.video().len(), n); // no source frame since the message
    feed_from(&mut r, &aus[120..180], now + 1.0 / FPS as f64, 0, 120);
    let video = r.video();
    assert!(video.len() > n && video[n].is_idr);
    let types = nal_types(&video[n].data);
    assert!(types.contains(&7) && types.contains(&8));
    assert!(!r.events(FreshFrame).is_empty());
}

#[test]
fn test_stale_generation_and_epoch_ignored() {
    let aus = h264_stream(&idr240(), 30);
    let mut r = make_worker(profile());
    for (i, au) in aus.iter().enumerate() {
        let now = T0 + i as f64 / FPS as f64;
        r.handle(
            vmsg(2, 0, "H264", 1, (i * 3000) as u32, now, au.clone()),
            now,
        );
        r.w.tick(now);
    }
    for (i, au) in aus.iter().enumerate() {
        let now = T0 + i as f64 / FPS as f64;
        r.handle(
            vmsg(1, 5, "H264", 1, (i * 3000) as u32, now, au.clone()),
            now,
        );
        r.w.tick(now);
    }
    assert!(r.video().is_empty() && r.w.stats.passthrough_frames == 0);
    assert_eq!(r.w.stats.stale_ignored, 60);
    r.handle(
        Ingress::ForceIdr(ForceIdr {
            generation: 9,
            epoch: 0,
        }),
        T0,
    );
    assert_eq!(r.w.stats.stale_ignored, 61);
}

#[test]
fn test_reset_pipeline_new_epoch_keeps_canvas_waits_for_keyframe() {
    let aus = h264_stream(&idr240(), 150);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..60], T0);
    r.handle(
        Ingress::ResetPipeline(ResetPipeline {
            generation: 1,
            epoch: 1,
        }),
        now,
    );
    assert_eq!(r.w.canvas, Some((320, 240)));
    let before = r.w.stats.passthrough_frames;
    feed_from(&mut r, &aus[61..70], now + 1.0 / FPS as f64, 0, 61); // old epoch: ignored
    feed_from(&mut r, &aus[61..70], now + 1.0 / FPS as f64, 1, 61); // P frames only: waits for IDR
    assert_eq!(r.w.stats.passthrough_frames, before);
    assert_eq!(r.events(NeedKeyframe).len(), 1);
    let mut gop = vec![idr240()];
    gop.extend_from_slice(&aus[121..150]);
    feed_from(&mut r, &gop, now + 2.0, 1, 120); // starts at an IDR
    drain(&mut r, now + 3.0);
    assert!(r.w.stats.passthrough_frames > before);
    assert!(!r.events(FreshFrame).is_empty());
    assert_eq!(r.video().last().unwrap().epoch, 1);
}

#[test]
fn test_stale_ingress_resets_decoder_and_requests_keyframe() {
    let mut r = make_worker(profile());
    r.handle(vmsg(1, 0, "H264", 1, 0, T0 - 0.5, idr240()), T0);
    assert_eq!(r.w.stats.ingress_dropped, 1);
    assert!(r.w.need_key && r.w.jbuf.is_empty() && !r.w.pt_active);
    assert_eq!(r.events(NeedKeyframe).len(), 1);
}

#[test]
fn test_stale_ingress_while_active_clears_jitter_buffer() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    r.handle(vmsg(1, 0, "H264", 1, 0, T0, aus[0].clone()), T0);
    r.handle(vmsg(1, 0, "H264", 1, 3000, T0, aus[1].clone()), T0);
    assert_eq!(r.w.jbuf.len(), 2);
    r.handle(vmsg(1, 0, "H264", 1, 6000, T0 - 0.5, aus[2].clone()), T0);
    assert!(r.w.jbuf.is_empty() && r.w.need_key);
    // an access unit is never dropped alone: P-frames wait for the next IDR
    r.handle(
        vmsg(1, 0, "H264", 1, 9000, T0 + 0.01, aus[3].clone()),
        T0 + 0.01,
    );
    assert!(r.w.jbuf.is_empty());
    r.handle(
        vmsg(1, 0, "H264", 1, 12000, T0 + 0.02, aus[0].clone()),
        T0 + 0.02,
    );
    assert!(!r.w.need_key && r.w.jbuf.len() == 1);
}

#[test]
fn test_fresh_frame_announced_while_output_disabled() {
    let aus = h264_stream(&idr240(), 150);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..60], T0);
    r.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 0,
            enabled: false,
        }),
        now,
    );
    r.handle(
        Ingress::ResetPipeline(ResetPipeline {
            generation: 1,
            epoch: 1,
        }),
        now,
    );
    let n = r.video().len();
    let mut gop = vec![idr240()];
    gop.extend_from_slice(&aus[121..150]);
    feed_from(&mut r, &gop, now + 1.0 / FPS as f64, 1, 120);
    assert!(!r.events(FreshFrame).is_empty());
    assert_eq!(r.video().len(), n); // announced, but no source frame is emitted
}

#[test]
fn test_unsupported_codec_fatal_once_per_epoch() {
    let mut r = make_worker(profile());
    for i in 0..3 {
        r.handle(vmsg(1, 0, "XYZ", 1, i, T0, b"x".to_vec()), T0);
    }
    let fatal = r.events(WorkerEventKind::Fatal);
    assert_eq!(fatal.len(), 1);
    assert_eq!(
        data_of(&fatal[0]),
        json!({"reason": "unsupported_codec", "codec": "XYZ"})
    );
    r.handle(
        Ingress::ResetPipeline(ResetPipeline {
            generation: 1,
            epoch: 1,
        }),
        T0,
    );
    r.handle(vmsg(1, 1, "XYZ", 1, 0, T0, b"x".to_vec()), T0);
    assert_eq!(r.events(WorkerEventKind::Fatal).len(), 2);
    assert!(r.video().is_empty());
}

// ------------------------------------------------------------------ adaptive codec passthrough

fn check_forwards_unchanged(codec: &str) {
    let aus = codec_stream(codec, 45);
    let mut r = make_worker(adaptive());
    let now = feed_codec(&mut r, codec, &aus, T0, 0, 0, 1);
    drain(&mut r, now);
    let out = r.video();
    assert_eq!(
        datas(&out),
        aus.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert!(out.iter().all(|v| v.codec == codec));
    assert!(out[0].is_idr && r.w.pt_active);
    let ff = r.events(FirstFrame);
    assert_eq!(ff.len(), 1);
    assert_eq!(ff[0].data["encoder"], "passthrough");
    assert_eq!(ff[0].data["frame_format"], codec.to_lowercase().as_str());
    assert_eq!(
        (ff[0].data["width"].as_u64(), ff[0].data["height"].as_u64()),
        (Some(1280), Some(720))
    );
    let sc = r.events(SourceCodec);
    assert_eq!(sc.len(), 1);
    assert_eq!(
        data_of(&sc[0]),
        json!({"codec": codec, "width": 1280, "height": 720, "eligible": true})
    );
}

#[test]
fn test_adaptive_forwards_h265_unchanged() {
    check_forwards_unchanged("H265");
}

#[test]
fn test_adaptive_forwards_av1_unchanged() {
    check_forwards_unchanged("AV1");
}

#[test]
fn test_adaptive_forwards_vp9_unchanged() {
    check_forwards_unchanged("VP9");
}

#[test]
fn test_adaptive_forwards_vp8_unchanged() {
    check_forwards_unchanged("VP8");
}

#[test]
fn test_source_codec_before_first_frame() {
    let mut r = make_worker(adaptive());
    feed_codec(&mut r, "H264", &h264_stream(&idr240(), 45), T0, 0, 0, 1);
    assert_eq!(
        r.kinds(&[SourceCodec, FirstFrame]),
        vec![SourceCodec, FirstFrame]
    );
}

#[test]
fn test_source_codec_on_change_and_each_epoch() {
    let h264 = h264_stream(&idr240(), 45);
    let h265 = codec_stream("H265", 45);
    let mut r = make_worker(adaptive());
    let now = feed_codec(&mut r, "H265", &h265, T0, 0, 0, 1);
    let now = feed_codec(&mut r, "H264", &h264, now + 0.1, 0, 100, 1);
    let codecs: Vec<Value> = r
        .events(SourceCodec)
        .iter()
        .map(|e| e.data["codec"].clone())
        .collect();
    assert_eq!(codecs, vec![json!("H265"), json!("H264")]);
    assert_eq!(r.events(FirstFrame).len(), 1);
    r.handle(
        Ingress::ResetPipeline(ResetPipeline {
            generation: 1,
            epoch: 1,
        }),
        now,
    );
    let now = feed_codec(&mut r, "H264", &h264, now + 0.1, 1, 200, 1);
    assert_eq!(r.events(SourceCodec).len(), 3);
    r.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 1,
            enabled: false,
        }),
        now,
    );
    r.handle(
        Ingress::SetOutput(SetOutput {
            generation: 1,
            epoch: 1,
            enabled: true,
        }),
        now,
    );
    feed_codec(&mut r, "H264", &h264, now + 0.1, 1, 300, 1);
    assert_eq!(r.events(SourceCodec).len(), 4);
    assert_eq!(r.events(FirstFrame).len(), 1);
}

#[test]
fn test_adaptive_waits_for_entry_after_codec_change() {
    let h265 = codec_stream("H265", 45);
    let h264 = h264_stream(&idr240(), 45);
    let mut r = make_worker(adaptive());
    let now = feed_codec(&mut r, "H265", &h265, T0, 0, 0, 1);
    drain(&mut r, now);
    let before = r.video().len();
    let now = feed_codec(&mut r, "H264", &h264[1..5], now + 0.1, 0, 100, 1); // P-frames: no entry point yet
    drain(&mut r, now);
    assert!(r.video().len() == before && !r.events(NeedKeyframe).is_empty() && r.w.jbuf.is_empty());
    let codecs = |r: &mut Rig| -> Vec<Value> {
        r.events(SourceCodec)
            .iter()
            .map(|e| e.data["codec"].clone())
            .collect()
    };
    assert_eq!(codecs(&mut r), vec![json!("H265")]);
    let now = feed_codec(&mut r, "H264", &h264[..3], now + 0.1, 0, 200, 1);
    drain(&mut r, now);
    let all = r.video();
    let new = &all[before..];
    assert!(new[0].is_idr && new.iter().all(|v| v.codec == "H264"));
    assert_eq!(
        datas(&new[1..]),
        h264[1..3].iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert_eq!(codecs(&mut r), vec![json!("H265"), json!("H264")]);
}

/// Python: oversize adaptive source is transcoded (`eligible: false`). Passthrough only: `source_codec` with
/// `eligible: false`, then `source_ineligible`; nothing is forwarded.
#[test]
fn test_adaptive_oversize_transcodes_eligible_false() {
    let aus = codec_stream("H265", 30);
    let mut r = make_worker(adaptive_small());
    let now = feed_codec(&mut r, "H265", &aus, T0, 0, 0, 1);
    drain(&mut r, now);
    assert!(!r.w.pt_active && r.video().is_empty());
    let sc = r.events(SourceCodec);
    assert_eq!(sc.len(), 1);
    assert_eq!(
        data_of(&sc[0]),
        json!({"codec": "H265", "width": 1280, "height": 720, "eligible": false})
    );
    let ineligible = r.events(SourceIneligible);
    assert_eq!(ineligible.len(), 1);
    assert_eq!(
        data_of(&ineligible[0]),
        json!({"codec": "H265", "width": 1280, "height": 720})
    );
    assert_eq!(
        r.kinds(&[SourceCodec, SourceIneligible, FirstFrame]),
        vec![SourceCodec, SourceIneligible]
    );
}

#[test]
fn test_adaptive_ineligible_inter_frames_are_dropped_without_keyframe_requests() {
    let aus = codec_stream("AV1", 30);
    let mut r = make_worker(adaptive_small());
    let now = feed_codec(&mut r, "AV1", &aus, T0, 0, 0, 1);
    drain(&mut r, now);
    assert!(!r.w.pt_active && r.video().is_empty() && r.events(NeedKeyframe).is_empty());
}

#[test]
fn test_vui_rewrite_only_for_h264() {
    let mut r = make_worker(adaptive());
    let h265 = codec_stream("H265", 3);
    let now = feed_codec(&mut r, "H265", &h265, T0, 0, 0, 1);
    drain(&mut r, now);
    assert_eq!(
        datas(&r.video()),
        h265.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );

    let no_vui = fixture("idr_320x240_no_vui.bin");
    let expected = fixture("idr_320x240_no_vui.expected.bin");
    assert_ne!(no_vui, expected);
    let mut r = make_worker(adaptive());
    let now = feed_codec(&mut r, "H264", &[no_vui], T0, 0, 0, 1);
    drain(&mut r, now);
    assert_eq!(r.video()[0].data, expected);
}

// ------------------------------------------------------------------ jitter buffer / pacing

#[test]
fn test_present_at_follows_max_lateness_capped_and_releases_slowly() {
    let mut r = make_worker(profile());
    let w = &mut r.w;
    // steady stream: present_at is rtp + constant offset, no jitter
    for i in 0..5 {
        let t = i as f64 / 30.0;
        let p = w.present_at(t, 10.0 + t);
        assert!((p - (10.0 + t)).abs() < 1e-9);
    }
    assert_eq!(w.jitter_s, 0.0);
    // a 100 ms late frame: fast attack (the buffer follows the largest recent lateness)
    let t = 1.0;
    let p = w.present_at(t, 10.0 + t + 0.1);
    assert!((w.jitter_s - 0.1).abs() < 1e-9 && (p - (11.0 + 0.1)).abs() < 1e-9);
    // extreme lateness is capped
    w.present_at(1.1, 10.0 + 1.1 + 2.0);
    assert!((w.jitter_s - ADAPT_MAX_S).abs() < 1e-9);
    // once the late samples age out of the window the target is 0 but release is slow per frame
    let before = w.jitter_s;
    w.present_at(
        1.1 + ADAPT_WINDOW_S + 5.0,
        10.0 + 1.1 + ADAPT_WINDOW_S + 5.0,
    );
    assert!((1.0 - ADAPT_RELEASE) * before - 1e-9 <= w.jitter_s && w.jitter_s < before);
}

#[test]
fn test_frame_time_is_spacing_to_next_buffered_frame() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    // RTP gaps 33 ms then 100 ms then 33 ms; all four arrive at once
    let rtps = [0u32, 3000, 12000, 15000];
    for (i, au) in aus[..4].iter().enumerate() {
        r.handle(vmsg(1, 0, "H264", 1, rtps[i], T0, au.clone()), T0);
    }
    for k in 0..200 {
        r.w.tick(T0 + 0.3 + k as f64 * 0.005);
    }
    let ft: Vec<f64> = r
        .video()
        .iter()
        .filter(|v| !v.repeated)
        .map(|v| v.frame_time_ms)
        .collect();
    assert_eq!(ft.len(), 4);
    let nominal = 1000.0 / FPS as f64;
    assert!((ft[0] - nominal).abs() < 1e-6); // 33.3 ms spacing
    assert!((ft[1] - 3.0 * nominal).abs() < 1e-6); // 100 ms clamped to 3x
    assert!((ft[2] - nominal).abs() < 1e-6);
    assert!((ft[3] - nominal).abs() < 1e-6); // no next frame: nominal
}

#[test]
fn test_new_ssrc_resets_jitter_buffer_and_waits_for_entry_point() {
    let idr = idr240();
    let aus = h264_stream(&idr, 60);
    let mut r = make_worker(profile());
    r.handle(vmsg(1, 0, "H264", 1, 0, T0, idr.clone()), T0);
    r.handle(vmsg(1, 0, "H264", 1, 3000, T0, aus[1].clone()), T0);
    assert_eq!(r.w.jbuf.len(), 2);
    // a P-frame of another SSRC: old queue is meaningless, a keyframe is requested
    r.handle(
        vmsg(1, 0, "H264", 2, 5, T0 + 0.01, aus[2].clone()),
        T0 + 0.01,
    );
    assert!(r.w.jbuf.is_empty() && r.w.need_key);
    // its IDR resumes
    r.handle(vmsg(1, 0, "H264", 2, 3005, T0 + 0.02, idr), T0 + 0.02);
    assert!(!r.w.need_key && r.w.jbuf.len() == 1);
}

#[test]
fn test_rtp_timestamp_wrap_keeps_frame_time_nominal() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let base = u32::MAX - 3000; // the second AU wraps past 2^32
    for (i, au) in aus[..3].iter().enumerate() {
        let ts = base.wrapping_add((i * 3000) as u32);
        r.handle(vmsg(1, 0, "H264", 1, ts, T0, au.clone()), T0);
    }
    for k in 0..200 {
        r.w.tick(T0 + 0.3 + k as f64 * 0.005);
    }
    let ft: Vec<f64> = r.video().iter().map(|v| v.frame_time_ms).collect();
    let nominal = 1000.0 / FPS as f64;
    assert_eq!(ft.len(), 3);
    assert!(ft.iter().all(|f| (f - nominal).abs() < 1e-6), "{ft:?}");
}

#[test]
fn test_bursty_arrival_one_au_per_source_frame_no_repeats() {
    // Python test_bursty_arrival_one_au_per_source_frame_no_repeats, passthrough: 4 AUs arrive at once
    // every 66 ms while the source RTP spacing is a steady 60 fps.
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(OutputProfile {
        fps: 60,
        ..profile()
    });
    let arrivals: Vec<f64> = (0..aus.len())
        .map(|i| T0 + (i / 4) as f64 * 4.0 / 60.0)
        .collect();
    for (i, (au, &now)) in aus.iter().zip(&arrivals).enumerate() {
        r.handle(
            vmsg(1, 0, "H264", 1, (i * 1500) as u32, now, au.clone()),
            now,
        );
        r.w.tick(now);
    }
    let last = *arrivals.last().unwrap();
    for k in 1..100 {
        r.w.tick(last + k as f64 * 0.005); // drain the jitter buffer
    }
    let video = r.video();
    assert_eq!(video.len(), aus.len());
    assert!(!video.iter().any(|v| v.repeated));
    assert_eq!(
        datas(&video),
        aus.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert!(
        video
            .iter()
            .all(|v| (v.frame_time_ms - 1000.0 / 60.0).abs() < 1e-6),
        "{:?}",
        video.iter().map(|v| v.frame_time_ms).collect::<Vec<_>>()
    );
}

#[test]
fn test_rtp_sequence_wrap_65535_to_0_is_not_a_gap() {
    // The producer flags holes (`gap`); the 16-bit sequence only wraps 65535 -> 0 and must change nothing.
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let mut now = T0;
    for (i, au) in aus[..8].iter().enumerate() {
        now = T0 + i as f64 / FPS as f64;
        let mut m = vmsg(1, 0, "H264", 1, (i * 3000) as u32, now, au.clone());
        m.sequence = Some(65532u16.wrapping_add(i as u16));
        r.handle(m, now);
        r.w.tick(now);
    }
    drain(&mut r, now);
    assert_eq!(r.w.stats.gap_frames, 0);
    assert!(!r.w.need_key && r.events(NeedKeyframe).is_empty());
    assert_eq!(r.video().len(), 8);
}

#[test]
fn test_adaptive_h265_joining_mid_gop_waits_for_irap() {
    let key = fixture("codecs/h265_1280x720_key.bin");
    let delta = fixture("codecs/h265_1280x720_delta.bin");
    assert!(!relay_media::bitstream::is_entry_point("H265", &delta));
    assert!(relay_media::bitstream::is_entry_point("H265", &key));
    let mut r = make_worker(adaptive());
    // joined mid-GOP: only delta AUs, nothing may be forwarded and a keyframe is requested
    let mut now = T0;
    for i in 0..10 {
        now = T0 + i as f64 / FPS as f64;
        r.handle(vmsg(1, 0, "H265", 1, i * 3000, now, delta.clone()), now);
        r.w.tick(now);
    }
    drain(&mut r, now);
    assert!(r.video().is_empty() && !r.w.pt_active);
    assert!(!r.events(NeedKeyframe).is_empty());
    assert!(r.events(FirstFrame).is_empty());
    // the IRAP arrives: it is the first forwarded AU, deltas follow
    let mut aus = vec![key.clone()];
    aus.extend(std::iter::repeat_n(delta.clone(), 5));
    for (j, au) in aus.iter().enumerate() {
        now = T0 + (10 + j) as f64 / FPS as f64;
        r.handle(
            vmsg(1, 0, "H265", 1, ((10 + j) * 3000) as u32, now, au.clone()),
            now,
        );
        r.w.tick(now);
    }
    drain(&mut r, now);
    let out = r.video();
    assert_eq!(
        datas(&out),
        aus.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
    );
    assert!(out[0].is_idr && out[0].codec == "H265" && r.w.pt_active);
    assert!(out[1..].iter().all(|v| !v.is_idr));
    assert_eq!(r.events(FirstFrame).len(), 1);
}

// ------------------------------------------------------------------ stats / keyframe rate limit

#[test]
fn test_stats_report_jitter_buffer_ms() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    feed(&mut r, &aus[..5], T0);
    r.w.jitter_s = 0.0421;
    r.w.last_stats = Some(T0 - 10.0);
    r.w.tick(T0 + 1.0);
    let stats = r.events(Stats);
    let last = stats.last().expect("stats event");
    let ms = last.data["jitter_buffer_ms"].as_f64().unwrap();
    assert!((ms - 42.1).abs() < 1e-9);
}

#[test]
fn test_stats_event_keys_are_passthrough_only() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..10], T0);
    drain(&mut r, now);
    r.w.last_stats = Some(now - 10.0);
    r.w.tick(now + 1.0);
    let stats = r.events(Stats);
    let mut keys: Vec<&str> = stats[0].data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "audio_dropped",
            "audio_unsupported",
            "backpressure_skips",
            "egress_dropped",
            "frames_out",
            "gap_frames",
            "idr",
            "ingress_dropped",
            "jitter_buffer_ms",
            "passthrough_frames",
            "stale_ignored",
        ]
    );
    assert_eq!(stats[0].data["frames_out"], 10);
    assert_eq!(stats[0].data["passthrough_frames"], 10);
    assert_eq!(stats[0].data["idr"], 1);
}

#[test]
fn test_stats_not_emitted_before_interval() {
    let mut r = make_worker(profile());
    r.w.tick(T0);
    r.w.tick(T0 + 4.9);
    assert!(r.events(Stats).is_empty());
    r.w.tick(T0 + 5.1);
    assert_eq!(r.events(Stats).len(), 1);
}

#[test]
fn test_need_keyframe_repeats_at_most_once_per_second() {
    let mut r = make_worker(profile());
    for _ in 0..3 {
        r.w.request_keyframe(100.0);
    }
    assert_eq!(r.events(NeedKeyframe).len(), 1);
    r.w.request_keyframe(101.1);
    assert_eq!(r.events(NeedKeyframe).len(), 2);
}

#[test]
fn test_h264_is_idr_helper() {
    let idr = idr240();
    assert!(h264_is_idr(&idr) && !h264_is_idr(&p_frame(1)));
    assert!(!h264_is_idr(&[]) && !h264_is_idr(&[0, 0, 1]));
    assert!(h264_is_idr(&[0, 0, 1, 0x65]));
}

// ------------------------------------------------------------------ ingest gaps and drain loop

#[test]
fn test_gap_frame_is_discarded_and_keyframe_requested() {
    let aus = h264_stream(&idr240(), 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..5], T0);
    drain(&mut r, now);
    assert!(!r.w.need_key && r.events(NeedKeyframe).is_empty());
    let before = r.video().len();
    let n = r.w.stats.passthrough_frames;
    let now = now + 1.0 / FPS as f64;
    let mut m = vmsg(1, 0, "H264", 1, 5 * 3000, now, aus[5].clone());
    m.gap = true;
    r.handle(m, now);
    drain(&mut r, now);
    assert_eq!(r.w.stats.gap_frames, 1);
    assert!(r.w.need_key);
    assert_eq!(r.events(NeedKeyframe).len(), 1);
    assert!(r.w.stats.passthrough_frames == n && r.video().len() == before);
}

#[test]
fn test_gap_on_entry_point_is_forwarded_without_keyframe_request() {
    let idr = idr240();
    let aus = h264_stream(&idr, 60);
    let mut r = make_worker(profile());
    let now = feed(&mut r, &aus[..5], T0);
    drain(&mut r, now);
    let before = r.video().len();
    let now = now + 1.0 / FPS as f64;
    let mut m = vmsg(1, 0, "H264", 1, 5 * 3000, now, idr);
    m.gap = true;
    r.handle(m, now);
    drain(&mut r, now + 1.0);
    assert_eq!(r.w.stats.gap_frames, 1);
    assert!(!r.w.need_key && r.events(NeedKeyframe).is_empty());
    assert_eq!(r.video().len(), before + 1);
}

#[test]
fn test_run_never_drops_the_65th_message() {
    // profile is None, so every ForceIdr is counted as stale: that counts the messages handled
    let ingress = Arc::new(BoundedChannel::new(&[("control", 128, None)]));
    for _ in 0..70 {
        assert!(ingress.put_nowait(
            Ingress::ForceIdr(ForceIdr {
                generation: 1,
                epoch: 0
            }),
            "control",
            0
        ));
    }
    assert!(ingress.put_nowait(Ingress::StopWorker(StopWorker), "control", 0));
    let mut w = Worker::new(
        ingress,
        Arc::new(egress_channel()),
        Arc::new(events_channel()),
    );
    assert!(w.run_once(&monotonic));
    assert_eq!(w.stats.stale_ignored, 64); // a tick runs after the 64th message
    assert!(!w.run_once(&monotonic)); // the 65th..70th are still handled, then StopWorker
    assert_eq!(w.stats.stale_ignored, 70);
}

// ------------------------------------------------------------------ handle

#[test]
fn test_handle_end_to_end() {
    let aus = h264_stream(&idr240(), 120);
    let mut h = WorkerHandle::start(profile()).expect("spawn worker");
    let mut egress: Vec<Egress> = Vec::new();
    let mut events: Vec<WorkerEvent> = Vec::new();
    assert!(h.send(Ingress::Configure(Configure {
        generation: 1,
        epoch: 0,
        profile: profile()
    })));
    let t0 = monotonic();
    for (i, au) in aus.iter().enumerate() {
        let due = t0 + i as f64 / FPS as f64;
        while monotonic() < due {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let msg = vmsg(1, 0, "H264", 1, (i * 3000) as u32, monotonic(), au.clone());
        assert!(h.send(Ingress::Video(msg)));
        egress.extend(h.poll_egress(10));
        events.extend(h.poll_events());
    }
    let end = monotonic() + 1.0;
    while monotonic() < end {
        std::thread::sleep(std::time::Duration::from_millis(20));
        egress.extend(h.poll_egress(10));
        events.extend(h.poll_events());
    }
    assert!(h.alive());
    h.stop();
    assert!(!h.alive());
    assert!(!events.iter().any(|e| e.kind == WorkerEventKind::Fatal));
    let video: Vec<&EgressVideo> = egress
        .iter()
        .filter_map(|e| {
            if let Egress::Video(v) = e {
                Some(v)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(video.len(), aus.len());
    assert!(video[0].is_idr);
    let types = nal_types(&video[0].data);
    assert!([5, 7, 8].iter().all(|t| types.contains(t)));
    assert!(video.iter().all(|v| v.generation == 1 && v.epoch == 0));
    assert!(events.iter().any(|e| e.kind == FirstFrame));
}

#[test]
fn test_handle_stop_joins_worker_thread() {
    let mut h = WorkerHandle::start(profile()).expect("spawn worker");
    assert!(h.alive());
    h.stop();
    assert!(!h.alive());
    // after stop the worker no longer accepts or answers anything
    assert!(h.poll_events().is_empty());
}

/// Live gate V-RELAY follow-up: a 60 fps source whose arrival rate swings between 52 and 69 fps
/// (and stalls 400 ms every 10 s), through the real egress limits, with the worker woken only by
/// arrivals or its 5 ms receive timeout rounded up to the 15.6 ms Windows timer, and the service
/// draining egress at the same coarse interval. Nothing may be skipped or dropped.
#[test]
fn bursty_52_to_69_fps_source_through_real_egress_is_not_skipped() {
    const SOURCE_FPS: f64 = 60.0;
    const WAKE_S: f64 = 0.0156;
    let n = 60 * 60;
    let aus = h264_stream(&idr720(), n);
    let mut r = rig_with(
        OutputProfile {
            fps: 60,
            ..profile()
        },
        Arc::new(egress_channel()),
    );
    // network delay: up 0 -> 150 ms over one second (52 fps), back down over the next (69 fps)
    let delay = |i: usize| {
        let pos = (i % 120) as f64 / 60.0;
        0.15 * if pos < 1.0 { pos } else { 2.0 - pos }
    };
    let mut arrivals = Vec::with_capacity(n);
    let mut last = T0;
    for i in 0..n {
        let produced = T0 + i as f64 / SOURCE_FPS;
        let mut at = produced + delay(i);
        let stall_start = T0 + (i as f64 / SOURCE_FPS / 10.0).floor() * 10.0 + 5.0;
        if produced >= stall_start && produced < stall_start + 0.4 {
            at = at.max(stall_start + 0.4); // held, then delivered at once
        }
        last = at.max(last);
        arrivals.push(last);
    }
    let (mut next, mut now, mut last_wake, mut next_pump) = (0, T0, T0, T0);
    let mut out = 0;
    let end = arrivals[n - 1] + 2.0;
    while now < end {
        now += 0.001;
        let mut woke = false;
        while next < n && arrivals[next] <= now {
            let msg = vmsg(
                1,
                0,
                "H264",
                1,
                (next * 1500) as u32,
                arrivals[next],
                aus[next].clone(),
            );
            r.handle(msg, now);
            next += 1;
            woke = true;
        }
        if woke || now - last_wake >= WAKE_S {
            r.w.tick(now);
            last_wake = now;
        }
        if now >= next_pump {
            for _ in 0..16 {
                match r.egress.get_nowait() {
                    Ok(Egress::Video(_)) => out += 1,
                    Ok(Egress::Audio(_)) => {}
                    Err(_) => break,
                }
            }
            next_pump = now + WAKE_S;
        }
    }
    let s = &r.w.stats;
    assert_eq!(
        (s.backpressure_skips, s.egress_dropped, s.ingress_dropped),
        (0, 0, 0),
        "{:?}",
        s
    );
    assert_eq!(out, n);
}

/// A source that stalls 850 ms (longer than the 600 ms jitter cap) every 10 s and then delivers the
/// stall at once: the whole burst stays in the jitter buffer (no resync), and the overdue part leaves at
/// the publisher's catch-up pace, so the real publisher (8 AUs, 200 ms late limit) never drops to an
/// IDR and the latency returns to the jitter buffer after each stall. Worker woken by arrivals or by its
/// receive timeout (5 ms, or 15.6 ms as on Windows), pump every 10 ms, sender paced by the publisher.
#[test]
fn stall_of_850ms_through_real_egress_and_publisher_is_not_skipped() {
    use parking_lot::Mutex;
    use relay_core::publisher::Publisher;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SOURCE_FPS: f64 = 60.0;
    const STALL_S: f64 = 0.85;
    let n = 60 * 60;
    let aus = h264_stream(&idr720(), n);
    let mut arrivals = Vec::with_capacity(n);
    for i in 0..n {
        let produced = T0 + i as f64 / SOURCE_FPS;
        let stall_start = T0 + (i as f64 / SOURCE_FPS / 10.0).floor() * 10.0 + 5.0;
        let held = produced >= stall_start && produced < stall_start + STALL_S;
        arrivals.push(if held {
            stall_start + STALL_S
        } else {
            produced
        });
    }
    for wake_s in [0.005, 0.0156] {
        let mut r = rig_with(
            OutputProfile {
                fps: 60,
                ..profile()
            },
            Arc::new(egress_channel()),
        );
        let clock = Arc::new(Mutex::new(T0));
        let idr_requests = Arc::new(AtomicUsize::new(0));
        let (c, q) = (clock.clone(), idr_requests.clone());
        let publisher = Publisher::new(
            Box::new(|_, _, _| true),
            Box::new(move || *c.lock()),
            0.2, // the service's MAX_LATE_S
            Box::new(move || {
                q.fetch_add(1, Ordering::SeqCst);
            }),
            "H264",
        );
        publisher.set_source(1, 0);
        let (mut next, mut now, mut last_wake, mut next_pump, mut next_send) = (0, T0, T0, T0, T0);
        let mut latency = Vec::with_capacity(n);
        let end = arrivals[n - 1] + 3.0;
        while now < end {
            now += 0.001;
            *clock.lock() = now;
            let mut woke = false;
            while next < n && arrivals[next] <= now {
                let rtp = (next * 1500) as u32;
                let msg = vmsg(1, 0, "H264", 1, rtp, arrivals[next], aus[next].clone());
                r.handle(msg, now);
                next += 1;
                woke = true;
            }
            if woke || now - last_wake >= wake_s {
                r.w.tick(now);
                last_wake = now;
            }
            if now >= next_pump {
                for _ in 0..16 {
                    match r.egress.get_nowait() {
                        Ok(Egress::Video(v)) => publisher.offer_video(v),
                        Ok(Egress::Audio(_)) => {}
                        Err(_) => break,
                    }
                }
                next_pump = now + 0.01;
            }
            if now >= next_send {
                // relay-voice `next_video_time`, retrying every 2 ms while nothing is due
                next_send = match publisher.next_video() {
                    Some(f) => {
                        latency.push(now - (T0 + latency.len() as f64 / SOURCE_FPS));
                        let ft = f.frame_time_ms / 1000.0;
                        let target = next_send + ft;
                        if now - target > ft { now } else { target }
                    }
                    None => now + 0.002,
                };
            }
        }
        let s = &r.w.stats;
        assert_eq!(
            (s.backpressure_skips, s.egress_dropped, s.ingress_dropped),
            (0, 0, 0),
            "wake {wake_s}: {s:?}"
        );
        assert_eq!(idr_requests.load(Ordering::SeqCst), 0, "wake {wake_s}");
        assert_eq!(latency.len(), n, "wake {wake_s}");
        // after each stall's catch-up the delay is back at the 600 ms cap (plus pump and send slots)
        for stall in 1..6 {
            let before_next = (stall * 10 + 5) * 60 - 1;
            let l = latency[before_next];
            assert!(l < ADAPT_MAX_S + 0.05, "wake {wake_s}, stall {stall}: {l}");
        }
    }
}

#[test]
fn test_forwarded_keyframe_and_key_wait_are_reported() {
    let idr = idr240();
    let aus = h264_stream(&idr, JITTER_MAX_FRAMES + 7);
    let mut r = make_worker(profile());
    feed(&mut r, &aus[..3], T0);
    let keys = r.events(Keyframe);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].data["bytes"], idr.len());
    assert!(r.events(KeyWait).is_empty());
    for (i, au) in aus.iter().enumerate().skip(3) {
        r.handle(vmsg(1, 0, "H264", 1, (i * 3000) as u32, T0, au.clone()), T0); // never ticked
    }
    let waits = r.events(KeyWait);
    assert_eq!(waits.len(), 1, "reported once per wait");
    assert_eq!(waits[0].data["reason"], "jitter_overflow");
}
