//! Passthrough worker: forwards the target's access units unchanged, paced by a jitter buffer (port of
//! `worker/process.py`, passthrough and adaptive branches only; no decode, scale or encode).
//!
//! The worker receives no token and writes no media to disk. All queues are bounded; late input is
//! dropped, never queued. `Worker::handle`/`tick` take `now` (monotonic seconds, `f64`) so tests drive
//! the core with a fake clock; [`crate::clock::monotonic`] is the clock the message producers and `run` share.
//!
//! Differs from Python on purpose: a source that cannot be passed through (over `max_*`, not `fixed_*`,
//! over `max_input_pixels`, or a non-H.264 codec outside adaptive mode) is never transcoded. The worker
//! reports `source_ineligible` once per epoch and drops access units until an eligible entry point arrives.

use std::collections::{HashMap, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use relay_media::bitstream;
use relay_media::codec::CODEC_ORDER;
use relay_media::h264::{ensure_low_delay_vui, has_parameter_sets, sps_dimensions};
use relay_media::opus::opus_packet_info;
use serde_json::{Map, Value, json};

use crate::clock::monotonic;
use crate::config::{CodecMode, OutputProfile, QualityMode};
use crate::publisher::CATCHUP_FACTOR;
use crate::worker::ipc::BoundedChannel;
use crate::worker::messages::{
    AUDIO_FRAME_BYTES, Egress, EgressAudio, EgressVideo, Ingress, IngressAudio, IngressVideo,
    WorkerEvent, WorkerEventKind,
};
use crate::worker::timeline::RtpUnwrapper;

pub const VIDEO_CLOCK_RATE: u32 = 90_000;
pub const AUDIO_CLOCK_RATE: u32 = 48_000;
pub const INGRESS_MAX_AGE_S: f64 = 0.2;
/// Video is sent as soon as it is forwarded, so audio must not trail it by a longer playout delay.
pub const AUDIO_PLAYOUT_DELAY_S: f64 = 0.05;
/// Arrival-offset minimum is tracked over this window (absorbs clock drift).
pub const JITTER_WINDOW_S: f64 = 3.0;
// Smoothness first (owner's choice: latency is acceptable, hitches are not). Live G2c: a p95/150 ms
// buffer let ~1 hitch per 5 s through with arrival gaps of 0.3-1.2 s; following the largest lateness
// of the last 20 s, capped at 600 ms and shrinking slowly, removed most of them.
pub const ADAPT_WINDOW_S: f64 = 20.0;
pub const ADAPT_PERCENTILE: f64 = 1.0;
pub const ADAPT_MAX_S: f64 = 0.6;
/// Per-frame step towards a lower target (fast attack, slow release).
pub const ADAPT_RELEASE: f64 = 0.002;
/// 2 s at 60 fps (Python: 48). A source that stalls longer than the 600 ms cap delivers the whole stall
/// in one burst; 48 turned a 0.8 s stall into a resync and a keyframe wait (live: 245 skips / 90 s).
pub const JITTER_MAX_FRAMES: usize = 120;
/// `need_keyframe` is repeated at this rate while still waiting.
pub const KEYFRAME_REREQUEST_S: f64 = 1.0;
pub const STATS_INTERVAL_S: f64 = 5.0;
pub const OVERFLOW_EVENT_INTERVAL_S: f64 = 1.0;

/// Generation/epoch reported before the first `Configure` (never equals a real one).
const UNCONFIGURED: u64 = u64::MAX;

/// Python `_h264_is_idr`: a start code followed by an IDR slice NAL (type 5).
pub fn h264_is_idr(data: &[u8]) -> bool {
    let mut i = 0;
    while let Some(p) = find_start_code(data, i) {
        if p + 3 < data.len() && data[p + 3] & 0x1F == 5 {
            return true;
        }
        i = p + 3;
    }
    false
}

fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    data.get(from..)?
        .windows(3)
        .position(|w| w == [0, 0, 1])
        .map(|p| p + from)
}

/// Can the source be passed through? `(w, h)` must fit the profile and the input pixel limit.
fn pt_eligible(p: &OutputProfile, w: u32, h: u32) -> bool {
    let (w, h) = (u64::from(w), u64::from(h));
    let fits = if p.mode == QualityMode::Fixed {
        (w, h) == (p.fixed_width, p.fixed_height)
    } else {
        w <= p.max_width && h <= p.max_height
    };
    fits && w * h <= p.max_input_pixels
}

/// One buffered access unit: shown at `present_at`.
#[derive(Debug, Clone)]
pub struct JbufEntry {
    pub present_at: f64,
    pub au: Vec<u8>,
    pub mono_received: f64,
    pub rtp_s: f64,
}

/// Counters reported by the `stats` event (passthrough-relevant ones only).
#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub frames_out: u64,
    pub idr: u64,
    pub backpressure_skips: u64,
    pub ingress_dropped: u64,
    pub egress_dropped: u64,
    pub audio_dropped: u64,
    pub audio_unsupported: u64,
    pub stale_ignored: u64,
    pub passthrough_frames: u64,
    pub gap_frames: u64,
}

/// Bounded channels with the limits the worker and its handle agree on.
pub fn ingress_channel() -> BoundedChannel<Ingress> {
    BoundedChannel::new(&[
        ("video", 256, Some(8 << 20)),
        ("audio", 256, Some(1 << 20)),
        ("control", 64, None),
    ])
}

pub fn egress_channel() -> BoundedChannel<Egress> {
    BoundedChannel::new(&[
        ("video", 2, Some(8 << 20)),
        ("audio", 10, Some(10 * AUDIO_FRAME_BYTES)),
    ])
}

pub fn events_channel() -> BoundedChannel<WorkerEvent> {
    BoundedChannel::new(&[("event", 1024, None)])
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

/// Single-threaded worker core. `handle`/`tick` take `now` so tests can drive it directly.
pub struct Worker {
    ingress: Arc<BoundedChannel<Ingress>>,
    egress: Arc<BoundedChannel<Egress>>,
    events: Arc<BoundedChannel<WorkerEvent>>,
    /// Set by [`WorkerHandle`] to end `run` even when the control queue is full.
    stop_flag: Arc<AtomicBool>,
    pub generation: u64,
    pub epoch: u64,
    pub profile: Option<OutputProfile>,
    /// `None` until the first tick, which starts the stats interval.
    pub last_stats: Option<f64>,
    last_overflow_event: f64,
    // per generation
    /// Size announced by `first_frame`; locked for the generation.
    pub canvas: Option<(u32, u32)>,
    pub jitter_s: f64,
    output_enabled: bool,
    dims_cache: HashMap<(String, Vec<u8>), (u32, u32)>,
    pub stats: WorkerStats,
    // per epoch
    /// Forwarding the source's access units (decided at an entry point).
    pub pt_active: bool,
    pt_decided: bool,
    /// Codec of the passthrough lane (adaptive mode).
    pt_codec: String,
    /// Last `source_codec` announced.
    src_state: Option<(String, u32, u32, bool)>,
    pub need_key: bool,
    last_key_request: f64,
    unsupported_reported: bool,
    ineligible_reported: bool,
    unwrappers: HashMap<u32, RtpUnwrapper>,
    /// Access units are shown at their source spacing.
    pub jbuf: VecDeque<JbufEntry>,
    pub offsets: VecDeque<(f64, f64)>,
    pub lateness: VecDeque<(f64, f64)>,
    jbuf_ssrc: Option<u32>,
    /// An overdue access unit is not forwarded before this time.
    paced_until: f64,
    announce_fresh: bool,
    last_audio_deadline: f64,
    // Audio is timed by local arrival like video, never by the sender's clock (RTCP / abs-send-time:
    // seconds off live, which put every packet outside the publisher's play window). Kept apart from
    // video's offsets/lateness so audio never moves video's jitter buffer.
    audio_unwrappers: HashMap<u32, RtpUnwrapper>,
    pub audio_offsets: HashMap<u32, VecDeque<(f64, f64)>>,
}

impl Worker {
    pub fn new(
        ingress: Arc<BoundedChannel<Ingress>>,
        egress: Arc<BoundedChannel<Egress>>,
        events: Arc<BoundedChannel<WorkerEvent>>,
    ) -> Self {
        let mut w = Self {
            ingress,
            egress,
            events,
            stop_flag: Arc::new(AtomicBool::new(false)),
            generation: UNCONFIGURED,
            epoch: UNCONFIGURED,
            profile: None,
            last_stats: None,
            last_overflow_event: f64::NEG_INFINITY,
            canvas: None,
            jitter_s: 0.0,
            output_enabled: true,
            dims_cache: HashMap::new(),
            stats: WorkerStats::default(),
            pt_active: false,
            pt_decided: false,
            pt_codec: String::new(),
            src_state: None,
            need_key: true,
            last_key_request: f64::NEG_INFINITY,
            unsupported_reported: false,
            ineligible_reported: false,
            unwrappers: HashMap::new(),
            jbuf: VecDeque::new(),
            offsets: VecDeque::new(),
            lateness: VecDeque::new(),
            jbuf_ssrc: None,
            paced_until: f64::NEG_INFINITY,
            announce_fresh: false,
            last_audio_deadline: f64::NEG_INFINITY,
            audio_unwrappers: HashMap::new(),
            audio_offsets: HashMap::new(),
        };
        w.reset_generation();
        w
    }

    // ------------------------------------------------------------ state

    fn reset_generation(&mut self) {
        self.canvas = None;
        self.jitter_s = 0.0;
        self.output_enabled = true;
        self.dims_cache.clear();
        self.stats = WorkerStats::default();
        self.reset_epoch();
    }

    fn reset_epoch(&mut self) {
        self.pt_active = false;
        self.pt_decided = false;
        self.pt_codec.clear();
        self.src_state = None;
        self.need_key = true;
        self.last_key_request = f64::NEG_INFINITY;
        self.unsupported_reported = false;
        self.ineligible_reported = false;
        self.unwrappers.clear();
        self.jbuf.clear();
        self.offsets.clear();
        self.lateness.clear();
        self.jbuf_ssrc = None;
        self.paced_until = f64::NEG_INFINITY;
        self.announce_fresh = false;
        self.last_audio_deadline = f64::NEG_INFINITY;
        self.audio_unwrappers.clear();
        self.audio_offsets.clear();
    }

    fn event(&self, kind: WorkerEventKind, data: Value) {
        self.events.put_nowait(
            WorkerEvent {
                generation: self.generation,
                epoch: self.epoch,
                kind,
                data: object(data),
            },
            "event",
            0,
        );
    }

    fn adaptive(&self) -> bool {
        self.profile
            .as_ref()
            .is_some_and(|p| p.codec_mode == CodecMode::Adaptive)
    }

    // ---------------------------------------------------------- control

    /// Process one ingress message; `false` means the worker must exit.
    pub fn handle(&mut self, msg: Ingress, now: f64) -> bool {
        let (generation, epoch) = match &msg {
            Ingress::StopWorker(_) => return false,
            Ingress::Configure(c) => {
                self.generation = c.generation;
                self.epoch = c.epoch;
                self.profile = Some(c.profile.clone());
                self.reset_generation();
                return true;
            }
            Ingress::Video(m) => (m.generation, m.epoch),
            Ingress::Audio(m) => (m.generation, m.epoch),
            Ingress::ResetPipeline(m) => (m.generation, m.epoch),
            Ingress::SetOutput(m) => (m.generation, m.epoch),
            Ingress::ForceIdr(m) => (m.generation, m.epoch),
        };
        if self.profile.is_none() || generation != self.generation {
            self.stats.stale_ignored += 1;
            return true;
        }
        if matches!(msg, Ingress::ResetPipeline(_)) {
            if epoch > self.epoch {
                self.epoch = epoch;
                self.reset_epoch();
                self.announce_fresh = true;
            } else {
                self.stats.stale_ignored += 1;
            }
            return true;
        }
        if epoch != self.epoch {
            self.stats.stale_ignored += 1;
            return true;
        }
        match msg {
            Ingress::SetOutput(m) => {
                self.jbuf.clear();
                self.output_enabled = m.enabled;
                if m.enabled {
                    self.src_state = None; // source_codec is announced again at the next entry point
                    self.announce_fresh = true;
                    if self.pt_active {
                        self.request_keyframe(now); // output restarts from a source IDR
                    }
                }
            }
            Ingress::ForceIdr(_) => {
                if self.pt_active {
                    self.last_key_request = f64::NEG_INFINITY; // the service already rate-limits ForceIdr
                    self.emit_need_keyframe(now);
                }
            }
            Ingress::Video(m) => self.video(m, now),
            Ingress::Audio(m) => self.audio(m, now),
            Ingress::StopWorker(_) | Ingress::Configure(_) | Ingress::ResetPipeline(_) => {}
        }
        true
    }

    // ------------------------------------------------------------ video

    pub fn request_keyframe(&mut self, now: f64) {
        self.need_key = true;
        self.emit_need_keyframe(now);
    }

    fn emit_need_keyframe(&mut self, now: f64) {
        if now - self.last_key_request >= KEYFRAME_REREQUEST_S {
            self.last_key_request = now;
            self.event(WorkerEventKind::NeedKeyframe, json!({}));
        }
    }

    fn video(&mut self, m: IngressVideo, now: f64) {
        if now - m.mono_received > INGRESS_MAX_AGE_S {
            self.stats.ingress_dropped += 1;
            if self.pt_active {
                self.jbuf.clear(); // an access unit is never dropped alone
            }
            self.request_keyframe(now);
            return;
        }
        if m.gap {
            // frames were lost before this one: nothing but an entry point may follow
            self.stats.gap_frames += 1;
            self.need_key = true;
            if !bitstream::is_entry_point(&m.codec, &m.data) {
                // an entry point already repairs the stream
                self.last_key_request = f64::NEG_INFINITY;
                self.emit_need_keyframe(now);
            }
        }
        if !CODEC_ORDER.contains(&m.codec.as_str()) {
            if !self.unsupported_reported {
                self.unsupported_reported = true;
                self.event(
                    WorkerEventKind::Fatal,
                    json!({"reason": "unsupported_codec", "codec": m.codec}),
                );
            }
            self.request_keyframe(now);
            return;
        }
        self.passthrough(m, now);
    }

    /// Adaptive mode: tell the main process the source codec, size and passthrough eligibility when they change.
    fn announce_source(&mut self, codec: &str, dims: (u32, u32), eligible: bool) {
        let state = (codec.to_string(), dims.0, dims.1, eligible);
        if self.src_state.as_ref() != Some(&state) {
            self.src_state = Some(state);
            self.event(
                WorkerEventKind::SourceCodec,
                json!({"codec": codec, "width": dims.0, "height": dims.1, "eligible": eligible}),
            );
        }
    }

    fn probe_dims(&mut self, codec: &str, data: &[u8]) -> Option<(u32, u32)> {
        // the headers that carry the size sit at the start of an entry AU
        let key = (codec.to_string(), data[..data.len().min(128)].to_vec());
        if let Some(&dims) = self.dims_cache.get(&key) {
            return Some(dims);
        }
        let dims = bitstream::probe_dimensions(codec, data)?;
        if self.dims_cache.len() >= 16 {
            self.dims_cache.clear();
        }
        self.dims_cache.insert(key, dims);
        Some(dims)
    }

    fn passthrough(&mut self, m: IngressVideo, now: f64) {
        let adaptive = self.adaptive();
        if adaptive && m.codec != self.pt_codec {
            if self.pt_active || self.pt_decided {
                // the target switched codec: wait for an entry point
                self.jbuf.clear();
                self.offsets.clear();
                self.lateness.clear();
            }
            self.pt_active = false;
            self.pt_decided = false;
            self.pt_codec.clone_from(&m.codec);
        }
        let h264 = m.codec == "H264";
        let key = if h264 && !adaptive {
            h264_is_idr(&m.data) && has_parameter_sets(&m.data)
        } else {
            bitstream::is_entry_point(&m.codec, &m.data)
        };
        if key {
            let dims = if h264 && !adaptive {
                sps_dimensions(&m.data)
            } else {
                self.probe_dims(&m.codec, &m.data)
            };
            let Some(dims) = dims else {
                // an entry point whose size cannot be read: unusable, wait for a clean one
                self.pt_resync(now);
                return;
            };
            // outside adaptive mode only H.264 is relayed
            let eligible = (h264 || adaptive) && self.fits(dims);
            if adaptive {
                self.announce_source(&m.codec, dims, eligible);
            }
            if !eligible {
                if self.pt_active {
                    self.jbuf.clear(); // leave passthrough: nothing more is forwarded
                }
                self.pt_active = false;
                self.pt_decided = true;
                if !self.ineligible_reported {
                    self.ineligible_reported = true;
                    self.event(
                        WorkerEventKind::SourceIneligible,
                        json!({"codec": m.codec, "width": dims.0, "height": dims.1}),
                    );
                }
                return;
            }
            self.pt_enter(&m, dims);
        } else if !self.pt_decided || (self.pt_active && self.need_key) {
            self.request_keyframe(now);
            return;
        } else if !self.pt_active {
            return; // ineligible source: drop until an eligible entry point
        }
        if !self.output_enabled {
            return;
        }
        let unwrapper = self
            .unwrappers
            .entry(m.ssrc)
            .or_insert_with(|| RtpUnwrapper::new(VIDEO_CLOCK_RATE));
        let rtp_s = unwrapper.unwrap(m.rtp_ts) as f64 / f64::from(VIDEO_CLOCK_RATE);
        if self.jbuf_ssrc != Some(m.ssrc) {
            // a new RTP clock: old offsets and queued units are meaningless
            self.jbuf_ssrc = Some(m.ssrc);
            self.jbuf.clear();
            self.offsets.clear();
            self.lateness.clear();
            if !key {
                self.pt_resync(now);
                return;
            }
        }
        let mut data = m.data;
        if m.codec == "H264" && h264_is_idr(&data) {
            data = ensure_low_delay_vui(&data);
        }
        let present_at = self.present_at(rtp_s, m.mono_received);
        self.jbuf.push_back(JbufEntry {
            present_at,
            au: data,
            mono_received: m.mono_received,
            rtp_s,
        });
        if self.jbuf.len() > JITTER_MAX_FRAMES {
            self.stats.backpressure_skips += self.jbuf.len() as u64;
            self.pt_resync(now);
        }
    }

    fn fits(&self, dims: (u32, u32)) -> bool {
        self.profile
            .as_ref()
            .is_some_and(|p| pt_eligible(p, dims.0, dims.1))
    }

    fn pt_enter(&mut self, m: &IngressVideo, dims: (u32, u32)) {
        self.pt_active = true;
        self.pt_decided = true;
        self.need_key = false;
        self.last_key_request = f64::NEG_INFINITY;
        if self.canvas.is_none() {
            self.canvas = Some(dims);
            self.announce_fresh = false;
            self.event(
                WorkerEventKind::FirstFrame,
                json!({
                    "width": dims.0, "height": dims.1,
                    "source_width": dims.0, "source_height": dims.1,
                    "codec": m.codec, "ssrc": m.ssrc,
                    "encoder": "passthrough", "decode": "none",
                    "frame_format": m.codec.to_lowercase(),
                }),
            );
        } else if self.announce_fresh {
            self.announce_fresh = false;
            self.event(WorkerEventKind::FreshFrame, json!({}));
        }
    }

    fn pt_resync(&mut self, now: f64) {
        self.jbuf.clear();
        self.request_keyframe(now);
    }

    /// rtp_s + minimum recent (arrival - rtp_s) + adaptive jitter.
    pub fn present_at(&mut self, rtp_s: f64, arrival: f64) -> f64 {
        let off = arrival - rtp_s;
        let q = &mut self.offsets;
        while q.back().is_some_and(|&(_, o)| o >= off) {
            q.pop_back();
        }
        q.push_back((arrival, off));
        while q
            .front()
            .is_some_and(|&(t, _)| t < arrival - JITTER_WINDOW_S)
        {
            q.pop_front();
        }
        let min_off = q.front().expect("just pushed").1;
        rtp_s + min_off + self.jitter(arrival, off - min_off)
    }

    fn jitter(&mut self, arrival: f64, lateness: f64) -> f64 {
        let lq = &mut self.lateness;
        lq.push_back((arrival, lateness));
        while lq
            .front()
            .is_some_and(|&(t, _)| t < arrival - ADAPT_WINDOW_S)
        {
            lq.pop_front();
        }
        let mut vals: Vec<f64> = lq.iter().map(|&(_, v)| v).collect();
        vals.sort_by(f64::total_cmp);
        let idx = (vals.len() - 1).min((vals.len() as f64 * ADAPT_PERCENTILE) as usize);
        let target = vals[idx].min(ADAPT_MAX_S);
        if target > self.jitter_s {
            self.jitter_s = target;
        } else {
            self.jitter_s -= (self.jitter_s - target) * ADAPT_RELEASE;
        }
        self.jitter_s
    }

    /// Forward the oldest due access unit as received; any loss is repaired by a new source IDR.
    pub fn tick(&mut self, now: f64) {
        self.maybe_stats(now);
        if self.profile.is_none() || !self.output_enabled || !self.pt_active {
            return;
        }
        // A full egress only means the main process has not polled yet: the AU waits in jbuf
        // (dropping it would freeze viewers until the next source IDR, ~0.7 s live).
        let Some(front) = self.jbuf.front() else {
            return;
        };
        if front.present_at > now || self.egress.full("video") {
            return;
        }
        // A stall longer than the jitter cap leaves part of its burst overdue. It goes out at the
        // publisher's catch-up pace: all at once it would overflow the publisher's queue
        // (MAX_EGRESS_AUS) and late limit, which drop to the next source IDR.
        let fps = self.profile.as_ref().map_or(30, |p| p.fps).max(1);
        let overdue = front.present_at < now - 1.0 / fps as f64;
        if overdue && now < self.paced_until {
            return;
        }
        let entry = self.jbuf.pop_front().expect("checked above");
        let adaptive = self.adaptive();
        let idr = if adaptive {
            bitstream::is_keyframe(&self.pt_codec, &entry.au)
        } else {
            h264_is_idr(&entry.au)
        };
        let size = entry.au.len();
        let frame_time_ms = self.frame_time_ms(entry.rtp_s);
        let item = EgressVideo {
            generation: self.generation,
            epoch: self.epoch,
            frame_time_ms,
            deadline: now,
            source_frame_at: Some(entry.mono_received),
            repeated: false,
            codec: if adaptive {
                self.pt_codec.clone()
            } else {
                "H264".to_string()
            },
            data: entry.au,
            is_idr: idr,
        };
        if self.egress.put_nowait(Egress::Video(item), "video", size) {
            self.stats.frames_out += 1;
            self.stats.passthrough_frames += 1;
            self.stats.idr += u64::from(idr);
            if overdue {
                // from the previous slot, not from `now`: a late tick must not slow the catch-up
                let step = frame_time_ms / 1000.0 * CATCHUP_FACTOR;
                self.paced_until = self.paced_until.max(now - step) + step;
            }
        } else {
            self.stats.egress_dropped += 1;
            self.pt_resync(now);
            if now - self.last_overflow_event >= OVERFLOW_EVENT_INTERVAL_S {
                self.last_overflow_event = now;
                self.event(
                    WorkerEventKind::Overflow,
                    json!({"channel": "egress_video"}),
                );
            }
        }
    }

    /// RTP spacing to the next buffered frame, clamped; never from the previous gap, which would
    /// slow the sender after a stall and back up the queue.
    fn frame_time_ms(&self, rtp_s: f64) -> f64 {
        let fps = self.profile.as_ref().map_or(30, |p| p.fps).max(1);
        let nominal = 1000.0 / fps as f64;
        match self.jbuf.front() {
            None => nominal,
            Some(next) => ((next.rtp_s - rtp_s) * 1000.0).clamp(nominal * 0.5, nominal * 3.0),
        }
    }

    // ------------------------------------------------------------ audio

    fn audio(&mut self, m: IngressAudio, now: f64) {
        let audio_enabled = self.profile.as_ref().is_some_and(|p| p.audio_enabled);
        if !audio_enabled || !self.output_enabled {
            return;
        }
        if now - m.mono_received > INGRESS_MAX_AGE_S {
            self.stats.audio_dropped += 1;
            return;
        }
        self.audio_passthrough(m);
    }

    /// Forward one 20 ms Opus packet unchanged: the sender reads one frame per 20 ms and advances RTP by 960,
    /// so any other packet is dropped, never re-packetized.
    fn audio_passthrough(&mut self, m: IngressAudio) {
        match opus_packet_info(&m.data) {
            Some((20.0, _, _, 1)) => {}
            _ => {
                self.stats.audio_unsupported += 1;
                return;
            }
        }
        let unwrapper = self
            .audio_unwrappers
            .entry(m.ssrc)
            .or_insert_with(|| RtpUnwrapper::new(AUDIO_CLOCK_RATE));
        let rtp_s = unwrapper.unwrap(m.rtp_ts) as f64 / f64::from(AUDIO_CLOCK_RATE);
        let base =
            rtp_s + self.audio_offset(m.ssrc, rtp_s, m.mono_received) + AUDIO_PLAYOUT_DELAY_S;
        let deadline = base + self.jitter_s;
        let deadline = deadline.max(self.last_audio_deadline);
        self.last_audio_deadline = deadline;
        let size = m.data.len();
        let item = EgressAudio {
            generation: self.generation,
            epoch: self.epoch,
            deadline,
            opus: m.data,
        };
        if !self.egress.put_nowait(Egress::Audio(item), "audio", size) {
            self.stats.audio_dropped += 1;
        }
    }

    /// Minimum recent (arrival - rtp_s) of this audio ssrc, like video's `present_at`.
    fn audio_offset(&mut self, ssrc: u32, rtp_s: f64, arrival: f64) -> f64 {
        let off = arrival - rtp_s;
        let q = self.audio_offsets.entry(ssrc).or_default();
        while q.back().is_some_and(|&(_, o)| o >= off) {
            q.pop_back();
        }
        q.push_back((arrival, off));
        while q
            .front()
            .is_some_and(|&(t, _)| t < arrival - JITTER_WINDOW_S)
        {
            q.pop_front();
        }
        q.front().map_or(off, |&(_, o)| o)
    }

    // ------------------------------------------------------------ stats

    fn maybe_stats(&mut self, now: f64) {
        let last = *self.last_stats.get_or_insert(now);
        if now - last < STATS_INTERVAL_S {
            return;
        }
        self.last_stats = Some(now);
        let s = &self.stats;
        let jitter_ms = (self.jitter_s * 1000.0 * 10.0).round() / 10.0;
        self.event(
            WorkerEventKind::Stats,
            json!({
                "jitter_buffer_ms": jitter_ms,
                "frames_out": s.frames_out,
                "idr": s.idr,
                "backpressure_skips": s.backpressure_skips,
                "ingress_dropped": s.ingress_dropped,
                "egress_dropped": s.egress_dropped,
                "audio_dropped": s.audio_dropped,
                "audio_unsupported": s.audio_unsupported,
                "stale_ignored": s.stale_ignored,
                "passthrough_frames": s.passthrough_frames,
                "gap_frames": s.gap_frames,
            }),
        );
    }

    // ------------------------------------------------------------- loop

    /// One pass of the worker loop: wait up to 5 ms for a message, handle up to 64, then tick.
    /// `false` means the worker must exit.
    pub fn run_once(&mut self, clock: &dyn Fn() -> f64) -> bool {
        if self.stop_flag.load(Ordering::Relaxed) {
            return false;
        }
        let mut msg = self.ingress.get(Some(0.005)).ok();
        let mut drained = 0;
        while let Some(m) = msg {
            if !self.handle(m, clock()) {
                return false;
            }
            drained += 1;
            if drained >= 64 {
                break;
            }
            msg = self.ingress.get_nowait().ok();
        }
        self.tick(clock());
        true
    }

    pub fn run(&mut self) {
        while self.run_once(&monotonic) {}
    }
}

fn worker_main(mut w: Worker) {
    if panic::catch_unwind(AssertUnwindSafe(|| w.run())).is_err() {
        w.event(
            WorkerEventKind::Fatal,
            json!({"reason": "worker_crash", "error": "panic"}),
        );
    }
}

// ------------------------------------------------------------------ handle

#[derive(Debug, thiserror::Error)]
pub enum WorkerStartError {
    #[error("token-like data would reach the worker")]
    TokenLikeData,
    #[error("cannot spawn the worker thread: {0}")]
    Spawn(#[from] std::io::Error),
}

/// The worker never gets credentials: a profile that mentions a token is a bug.
pub fn check_no_token(profile: &OutputProfile) -> Result<(), WorkerStartError> {
    if format!("{profile:?}").to_lowercase().contains("token") {
        return Err(WorkerStartError::TokenLikeData);
    }
    Ok(())
}

/// Main-thread side of the worker: spawn, bounded send, polling, stop.
pub struct WorkerHandle {
    thread: Option<JoinHandle<()>>,
    ingress: Arc<BoundedChannel<Ingress>>,
    egress: Arc<BoundedChannel<Egress>>,
    events: Arc<BoundedChannel<WorkerEvent>>,
    stop_flag: Arc<AtomicBool>,
}

impl WorkerHandle {
    pub fn start(profile: OutputProfile) -> Result<Self, WorkerStartError> {
        check_no_token(&profile)?;
        let ingress = Arc::new(ingress_channel());
        let egress = Arc::new(egress_channel());
        let events = Arc::new(events_channel());
        let worker = Worker::new(ingress.clone(), egress.clone(), events.clone());
        let stop_flag = worker.stop_flag.clone();
        let thread = thread::Builder::new()
            .name("relay-worker".to_string())
            .spawn(move || worker_main(worker))?;
        Ok(Self {
            thread: Some(thread),
            ingress,
            egress,
            events,
            stop_flag,
        })
    }

    /// `false` when the bounded ingress is full; the caller bumps the epoch.
    pub fn send(&self, msg: Ingress) -> bool {
        match &msg {
            Ingress::Video(m) => {
                let size = m.data.len();
                self.ingress.put_nowait(msg, "video", size)
            }
            Ingress::Audio(m) => {
                let size = m.data.len();
                self.ingress.put_nowait(msg, "audio", size)
            }
            _ => self.ingress.put_nowait(msg, "control", 0),
        }
    }

    pub fn poll_egress(&self, max_items: usize) -> Vec<Egress> {
        poll(&self.egress, max_items)
    }

    pub fn poll_events(&self) -> Vec<WorkerEvent> {
        poll(&self.events, 1024)
    }

    pub fn alive(&self) -> bool {
        self.thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// Ask the worker to exit and wait up to 3 s (then force the loop to end and wait 2 s more).
    pub fn stop(&mut self) {
        self.stop_within(3.0);
    }

    pub fn stop_within(&mut self, timeout: f64) {
        self.send(Ingress::StopWorker(crate::worker::messages::StopWorker));
        if !self.wait_finished(timeout) {
            self.stop_flag.store(true, Ordering::Relaxed);
            self.wait_finished(2.0);
        }
        if let Some(thread) = self.thread.take_if(|t| t.is_finished()) {
            // a panic was already reported as a worker_crash event
            let _ = thread.join();
        }
    }

    fn wait_finished(&self, timeout: f64) -> bool {
        let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
        while self.alive() {
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(2));
        }
        true
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}

fn poll<T>(channel: &BoundedChannel<T>, max_items: usize) -> Vec<T> {
    let mut out = Vec::new();
    while out.len() < max_items {
        match channel.get_nowait() {
            Ok(item) => out.push(item),
            Err(_) => break,
        }
    }
    out
}
