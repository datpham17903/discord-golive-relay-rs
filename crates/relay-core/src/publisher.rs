//! Single send path: chooses between source content and slate (spec 6, 9, 11). Port of
//! `golive_relay/media/publisher.py`, Opus audio only.
//!
//! `next_video`/`next_audio` are called from the voice sender thread while offers and control calls come
//! from the service, so all state is guarded by one lock. Frames leave as [`OutFrame`] (data plus the wait
//! before the next one); `RelayMediaSource` of the Python library has no counterpart here, the sender in
//! `relay-voice` pulls these directly.
//!
//! Differs from Python on purpose: there is no PCM mode (`opus` flag and `pcm` field are gone), so silence is
//! always `OPUS_SILENCE`, and `next_slate` returns no frame (instead of raising) when the slate has no variant
//! with an `idr_pic_id` different from the last sent one, which `build_slate` never produces.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use relay_media::OutFrame;
use relay_media::bitstream::is_entry_point;
use relay_media::h264::{idr_pic_id, is_idr};
use relay_media::opus::OPUS_SILENCE;
use relay_media::slate::SlateGop;

use crate::worker::messages::{EgressAudio, EgressVideo};

/// ~133 ms at 60 fps; absorbs source bursts (6 dropped frames live), adds latency only while bursting.
pub const MAX_EGRESS_AUS: usize = 8;
pub const MAX_EGRESS_BYTES: usize = 8 * 1024 * 1024;
/// The sender's pace equals the source rate, so a burst would leave a standing backlog (+~40 ms live).
/// While CATCHUP_BACKLOG or more AUs stay queued, frames go out ~10% faster.
pub const CATCHUP_BACKLOG: usize = 2;
pub const CATCHUP_FACTOR: f64 = 0.9;
/// A gap between sends that a viewer notices as a hitch.
pub const STALL_S: f64 = 0.05;
/// A gap between sends that reads as a visible freeze.
pub const FREEZE_S: f64 = 0.25;
/// 800 ms: audio is held as long as video sits in the 600 ms jitter buffer. (Python name; the unit is Opus
/// frames here.)
pub const MAX_PCM_BLOCKS: usize = 40;

/// Send counters since the last [`Publisher::take_window`]: sent, stalls (gaps over 50 ms),
/// `max_send_gap_ms`, audio frames played / late-dropped / evicted (queue overflow) and `audio_held_reads`
/// (silent reads while the head frame was not yet due).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SendWindow {
    pub sent: u64,
    pub stalls: u64,
    pub max_send_gap_ms: f64,
    pub audio_played: u64,
    pub audio_held_reads: u64,
    pub audio_late_dropped: u64,
    pub audio_evicted: u64,
}

/// Cumulative, never-reset video send counters.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SendSnapshot {
    pub frames: u64,
    pub bytes: u64,
    pub freezes: u64,
    pub frozen_s: f64,
}

/// (kbps, fps) over the last `window_s`: `current` against the newest sample at least that old, else the
/// oldest sample. `None` without a sample or a positive time span.
pub fn rates(
    history: &[(f64, SendSnapshot)],
    now: f64,
    current: &SendSnapshot,
    window_s: f64,
) -> Option<(f64, f64)> {
    let first = history.first()?;
    let (base_t, base) = history
        .iter()
        .rev()
        .find(|h| h.0 <= now - window_s)
        .unwrap_or(first);
    let dt = now - base_t;
    if dt <= 0.0 {
        return None;
    }
    Some((
        (current.bytes - base.bytes) as f64 * 8.0 / 1000.0 / dt,
        (current.frames - base.frames) as f64 / dt,
    ))
}

/// Freezes in the last `window_s`; a history younger than that counts from the publisher's start.
pub fn freezes_since(
    history: &[(f64, SendSnapshot)],
    now: f64,
    current: &SendSnapshot,
    window_s: f64,
) -> u64 {
    let base = history
        .iter()
        .rev()
        .find(|h| h.0 <= now - window_s)
        .map_or(0, |h| h.1.freezes);
    current.freezes - base
}

pub type Gate = Box<dyn Fn(u64, u64, f64) -> bool + Send + Sync>;
pub type Clock = Box<dyn Fn() -> f64 + Send + Sync>;
pub type NeedIdr = Box<dyn Fn() + Send + Sync>;

struct State {
    codec: String,
    source: Option<(u64, u64)>,
    slate: Option<Arc<SlateGop>>,
    /// (variant index, next position) of the slate GOP being played.
    slate_iter: Option<(usize, usize)>,
    video: VecDeque<EgressVideo>,
    video_bytes: usize,
    audio: VecDeque<EgressAudio>,
    /// Discard source AUs until an IDR with SPS/PPS.
    await_idr: bool,
    /// `idr_pic_id` of the last sent AU if it was an IDR, else `None`.
    last_idr_id: Option<u32>,
    /// The pad is only valid after a slate IDR (x264 CABAC vs source SPS/PPS).
    last_idr_from_slate: bool,
    last_send: Option<f64>,
    window: SendWindow,
    total: SendSnapshot,
}

impl State {
    fn purge(&mut self) {
        self.video.clear();
        self.video_bytes = 0;
        self.audio.clear();
    }

    fn note_send(&mut self, now: f64, size: usize) {
        self.window.sent += 1;
        let t = self.total;
        let mut total = SendSnapshot {
            frames: t.frames + 1,
            bytes: t.bytes + size as u64,
            ..t
        };
        if let Some(last) = self.last_send {
            let gap = now - last;
            self.window.stalls += u64::from(gap > STALL_S);
            if gap >= FREEZE_S {
                total.freezes += 1;
                total.frozen_s += gap;
            }
            self.window.max_send_gap_ms = self.window.max_send_gap_ms.max(gap * 1000.0);
        }
        self.total = total;
        self.last_send = Some(now);
    }

    fn paced(&self, frame_time_ms: f64) -> f64 {
        if self.video.len() >= CATCHUP_BACKLOG {
            frame_time_ms * CATCHUP_FACTOR
        } else {
            frame_time_ms
        }
    }

    fn slate_p_frame(&self) -> Option<Vec<u8>> {
        let slate = self.slate.as_ref()?;
        slate
            .variants
            .iter()
            .find(|gop| gop.len() > 1)
            .map(|gop| gop[1].clone())
    }

    fn next_slate(&mut self) -> Option<OutFrame> {
        let slate = self.slate.clone()?;
        if self.codec != "H264" {
            return None;
        }
        let next = self
            .slate_iter
            .and_then(|(v, pos)| slate.variants[v].get(pos).map(|au| (v, pos, au)));
        let (v, pos, au) = match next {
            Some(n) => n,
            None => {
                let last = self.last_idr_id;
                let v = slate.variants.iter().position(|gop| {
                    gop.first()
                        .is_some_and(|au| last.is_none() || idr_pic_id(au) != last)
                })?;
                (v, 0, &slate.variants[v][0])
            }
        };
        self.slate_iter = Some((v, pos + 1));
        self.last_idr_id = if is_idr(au) { idr_pic_id(au) } else { None };
        self.last_idr_from_slate = self.last_idr_id.is_some();
        Some(OutFrame {
            data: au.clone(),
            frame_time_ms: slate.frame_time_ms,
        })
    }
}

pub struct Publisher {
    gate: Gate,
    clock: Clock,
    max_late: f64,
    on_need_idr: NeedIdr,
    state: Mutex<State>,
}

impl Publisher {
    pub fn new(
        gate: Gate,
        clock: Clock,
        max_late_s: f64,
        on_need_idr: NeedIdr,
        codec: &str,
    ) -> Self {
        Publisher {
            gate,
            clock,
            max_late: max_late_s,
            on_need_idr,
            state: Mutex::new(State {
                codec: codec.to_owned(),
                source: None,
                slate: None,
                slate_iter: None,
                video: VecDeque::new(),
                video_bytes: 0,
                audio: VecDeque::new(),
                await_idr: false,
                last_idr_id: None,
                last_idr_from_slate: false,
                last_send: None,
                window: SendWindow::default(),
                total: SendSnapshot::default(),
            }),
        }
    }

    pub fn codec(&self) -> String {
        self.state.lock().codec.clone()
    }

    /// Whether a slate GOP is mid-play (Python's `_slate_iter is not None`).
    #[doc(hidden)]
    pub fn slate_iter_active(&self) -> bool {
        self.state.lock().slate_iter.is_some()
    }

    /// The slate set now (Python's `_slate`).
    pub fn slate(&self) -> Option<Arc<SlateGop>> {
        self.state.lock().slate.clone()
    }

    /// Whether a source is set, i.e. the publisher is not closed (Python's `_source is not None`).
    pub fn has_source(&self) -> bool {
        self.state.lock().source.is_some()
    }

    // ------------------------------------------------------------ control

    pub fn set_source(&self, generation: u64, epoch: u64) {
        let mut st = self.state.lock();
        st.purge();
        st.source = Some((generation, epoch));
        st.await_idr = true;
    }

    pub fn set_codec(&self, codec: &str) {
        {
            let mut st = self.state.lock();
            st.codec = codec.to_owned();
            st.video.clear();
            st.video_bytes = 0;
            st.last_idr_id = None;
            st.last_idr_from_slate = false;
            st.slate_iter = None;
            st.await_idr = true;
        }
        (self.on_need_idr)();
    }

    pub fn set_slate(&self, slate: Option<Arc<SlateGop>>) {
        let mut st = self.state.lock();
        st.slate = slate;
        st.slate_iter = None;
    }

    pub fn close(&self) {
        let mut st = self.state.lock();
        st.purge();
        st.source = None;
        st.slate = None;
        st.slate_iter = None;
    }

    // ------------------------------------------------------------- offers

    pub fn offer_video(&self, au: EgressVideo) {
        let mut need = false;
        {
            let mut st = self.state.lock();
            if st.source != Some((au.generation, au.epoch)) {
                return;
            }
            if au.codec != st.codec {
                return;
            }
            if st.video.len() >= MAX_EGRESS_AUS || st.video_bytes + au.data.len() > MAX_EGRESS_BYTES
            {
                st.video.clear();
                st.video_bytes = 0;
                st.await_idr = true;
                need = true;
            } else {
                st.video_bytes += au.data.len();
                st.video.push_back(au);
            }
        }
        if need {
            (self.on_need_idr)();
        }
    }

    pub fn offer_audio(&self, au: EgressAudio) {
        let mut st = self.state.lock();
        if st.source != Some((au.generation, au.epoch)) {
            return;
        }
        st.audio.push_back(au);
        while st.audio.len() > MAX_PCM_BLOCKS {
            st.audio.pop_front();
            st.window.audio_evicted += 1;
        }
    }

    // -------------------------------------------------------------- reads

    pub fn next_video(&self) -> Option<OutFrame> {
        let (frame, need) = {
            let mut st = self.state.lock();
            let (frame, need) = self.next_video_locked(&mut st);
            if let Some(f) = &frame {
                let now = (self.clock)();
                st.note_send(now, f.data.len());
            }
            (frame, need)
        };
        if need {
            (self.on_need_idr)();
        }
        frame
    }

    /// Clock time of the last video frame handed to the sender.
    pub fn last_send_at(&self) -> Option<f64> {
        self.state.lock().last_send
    }

    /// Cumulative send counters; unlike [`Self::take_window`] they are never reset.
    pub fn snapshot(&self) -> SendSnapshot {
        self.state.lock().total
    }

    pub fn take_window(&self) -> SendWindow {
        std::mem::take(&mut self.state.lock().window)
    }

    fn gate_open_locked(&self, st: &State, now: f64) -> bool {
        st.source.is_some_and(|(g, e)| (self.gate)(g, e, now))
    }

    fn next_video_locked(&self, st: &mut State) -> (Option<OutFrame>, bool) {
        let now = (self.clock)();
        let mut need = false;
        if !self.gate_open_locked(st, now) {
            // Anything queued is stale and the source must restart on an IDR.
            st.purge();
            st.await_idr = true;
            return (st.next_slate(), false);
        }
        st.slate_iter = None;
        while let Some(au) = st.video.pop_front() {
            st.video_bytes -= au.data.len();
            if now - au.deadline > self.max_late {
                st.await_idr = true;
                need = true;
                continue;
            }
            let idr = st.codec == "H264" && is_idr(&au.data);
            if st.await_idr {
                if !is_entry_point(&st.codec, &au.data) {
                    need = true;
                    continue;
                }
                st.await_idr = false;
            }
            if !idr {
                st.last_idr_id = None;
                let frame_time_ms = st.paced(au.frame_time_ms);
                return (
                    Some(OutFrame {
                        data: au.data,
                        frame_time_ms,
                    }),
                    need,
                );
            }
            let new_id = idr_pic_id(&au.data);
            if st.last_idr_from_slate && st.last_idr_id.is_some() && new_id == st.last_idr_id {
                let Some(pad) = st.slate_p_frame() else {
                    st.await_idr = true;
                    need = true;
                    continue;
                };
                let frame_time_ms = st.slate.as_ref().map_or(0.0, |s| s.frame_time_ms);
                // Keep the IDR for the next call; one slate P-frame separates the equal ids.
                st.video_bytes += au.data.len();
                st.video.push_front(au);
                st.last_idr_id = None;
                return (
                    Some(OutFrame {
                        data: pad,
                        frame_time_ms,
                    }),
                    need,
                );
            }
            st.last_idr_id = new_id;
            st.last_idr_from_slate = false;
            let frame_time_ms = st.paced(au.frame_time_ms);
            return (
                Some(OutFrame {
                    data: au.data,
                    frame_time_ms,
                }),
                need,
            );
        }
        (None, need)
    }

    /// One 20 ms Opus frame, or `OPUS_SILENCE` when none is due.
    pub fn next_audio(&self) -> Vec<u8> {
        let mut st = self.state.lock();
        let now = (self.clock)();
        if !self.gate_open_locked(&st, now) {
            st.audio.clear();
            return OPUS_SILENCE.to_vec();
        }
        while let Some(head) = st.audio.front() {
            if head.deadline > now {
                st.window.audio_held_reads += 1;
                return OPUS_SILENCE.to_vec(); // hold until the shared-timeline deadline, like video
            }
            let au = st.audio.pop_front().expect("front checked");
            if now - au.deadline <= self.max_late {
                st.window.audio_played += 1;
                return au.opus;
            }
            st.window.audio_late_dropped += 1;
        }
        OPUS_SILENCE.to_vec()
    }
}

impl relay_media::FrameSource for Publisher {
    fn next_video(&mut self) -> Option<OutFrame> {
        Publisher::next_video(self)
    }

    fn next_audio(&mut self) -> Vec<u8> {
        Publisher::next_audio(self)
    }
}

/// The voice sender thread pulls from the publisher; the service keeps its own `Arc` to feed it.
/// (A newtype because `FrameSource for Arc<Publisher>` would break the orphan rule.)
pub struct PublisherSource(pub Arc<Publisher>);

impl relay_media::FrameSource for PublisherSource {
    fn next_video(&mut self) -> Option<OutFrame> {
        self.0.next_video()
    }

    fn next_audio(&mut self) -> Vec<u8> {
        self.0.next_audio()
    }
}
