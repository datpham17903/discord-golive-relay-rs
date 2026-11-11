//! Publishing side of a Go Live stream: the send pipeline of `native_voice.VoiceClient`
//! (`start_video` / `_apply_selected_video_stream`, `send_audio_packet`, `send_video_frame`, the RTCP
//! sender reports, NACK / PLI handling) and of `native_voice.player.MediaPlayer._do_run` (the 20 ms
//! audio cadence and the video pacing), driven from one dedicated OS thread so a paced burst never
//! blocks a tokio worker.
//!
//! The source is a [`FrameSource`]. Per video access unit the thread strips H.264 filler NAL units,
//! applies DAVE (passthrough when DAVE is not active), packetizes for the negotiated codec, adds the
//! RTP extensions, encrypts with the transport cipher and sends in bursts (`VideoSendPipeline`). Per
//! 20 ms it sends one Opus frame through `Dave::encrypt_opus`.
//!
//! Deliberate differences from the Python library:
//! - No keyframe cache: a viewer's PLI goes to a callback ([`PliHandler`]) so the owner can ask the
//!   source for a keyframe; replaying a cached GOP on the shared SSRC would rewind every viewer.
//! - FIR is treated like PLI.
//! - A DAVE encrypt error or a failed send drops that frame or packet and is counted
//!   ([`SendStats`]); Python raised and stopped the player thread.
//! - Only one (non-simulcast) stream is sent, so MEDIA_SINK_WANTS (op 15) is ignored.
//! - When the connection closes the thread waits up to [`RECONNECT_TIMEOUT`] for a new one, like
//!   `MediaPlayer` waits for a reconnect, then ends. The stream client hands it the new connection
//!   (`Sender::rebind`), which restarts video on that connection's SSRC and codec like a source
//!   swap; Python's player kept the old video state.
//! - The audio RTP timestamp wraps at 2^32; Python reset it to 0 there.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use relay_media::FrameSource;
use relay_media::codec::VoiceCodec;
use relay_media::opus::OPUS_SILENCE;
use serde_json::Value;

use crate::connection::{RECONNECT_TIMEOUT, RtcConnection, SendSettings};
use crate::error::VoiceError;
use crate::receive::Feedback;
use crate::rtcp;
use crate::rtp::RtpPacket;
use crate::transport::packets::{
    RTP_AUDIO_LEVEL_SILENCE, build_audio_extension_payload, header_with_one_byte_extensions,
    wait_until,
};
use crate::video::{PacketSink, VideoSendPipeline, strip_h264_filler_nalus};
use crate::voice_ws::{CodecSpec, StreamResolution, StreamSpec, speaking};

/// `MediaPlayer.DELAY` / `opus.Encoder.FRAME_LENGTH`.
const AUDIO_FRAME: Duration = Duration::from_millis(20);
const AUDIO_SAMPLES_PER_FRAME: u32 = 960;
const AUDIO_CLOCK_RATE: u64 = 48_000;
/// `MediaPlayer.SILENCE_PACKET_COUNT`.
const SILENCE_PACKET_COUNT: usize = 10;
/// `RelayMediaSource.video_retry_delay`.
const VIDEO_RETRY: Duration = Duration::from_millis(2);
/// Retry when the video transport is not started (`MediaPlayer`: `+ 0.02`).
const VIDEO_NOT_READY_RETRY: Duration = Duration::from_millis(20);
/// The longest the loop sleeps in one go.
const MAX_SLEEP: Duration = Duration::from_millis(20);
const MEDIA_STREAM_TYPE: &str = "screen";

/// Called on the UDP dispatcher thread when a viewer asks for a keyframe of our stream.
pub type PliHandler = Arc<dyn Fn() + Send + Sync>;

/// What `start_video` announces (`_VideoStartParams`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VideoParams {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    /// Bits per second; 0 picks [`native_video_bitrate`].
    pub bitrate: u32,
}

/// `_native_video_bitrate`.
pub fn native_video_bitrate(height: u32, fps: u32) -> u32 {
    if height >= 721 || fps >= 31 {
        8_000_000
    } else {
        4_000_000
    }
}

/// Counters of the send side, since the stream was created.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SendStats {
    pub video_frames: u64,
    pub video_packets: u64,
    pub video_octets: u64,
    pub audio_packets: u64,
    /// Frames or Opus packets dropped because DAVE could not encrypt them.
    pub dave_encrypt_errors: u64,
    /// Frames or packets dropped by a packetizer or socket error.
    pub send_errors: u64,
    pub nack_packets_received: u64,
    pub rtx_packets_resent: u64,
    pub pli_received: u64,
}

#[derive(Default)]
struct Counters {
    video_frames: AtomicU64,
    video_packets: AtomicU64,
    video_octets: AtomicU64,
    audio_packets: AtomicU64,
    dave_encrypt_errors: AtomicU64,
    send_errors: AtomicU64,
    nack_packets_received: AtomicU64,
    rtx_packets_resent: AtomicU64,
    pli_received: AtomicU64,
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// The negotiated video stream we send on (`_video_ssrc`, `_video_codec`...).
struct VideoStream {
    codec: VoiceCodec,
    ssrc: u32,
    /// Seconds of media sent, for the once-per-second sender report (`_video_media_times_ms`).
    media_time_ms: f64,
    last_rtcp_media_ms: f64,
    packet_count: u32,
    octet_count: u32,
}

struct AudioState {
    /// `VoiceClient.sequence`: incremented before each packet.
    sequence: u16,
    timestamp: u32,
    packet_count: u32,
    octet_count: u32,
    last_rtcp_media_ms: Option<u64>,
    last_was_silence: bool,
}

struct Active {
    source: Option<Box<dyn FrameSource>>,
    video: Option<VideoStream>,
    audio: AudioState,
    /// What the last `play` announced; a swap or restart reuses it.
    params: Option<VideoParams>,
}

pub(crate) struct Sender {
    /// Replaced by [`Sender::rebind`]; lock order is `active` before `conn`.
    conn: Mutex<Arc<RtcConnection>>,
    pipeline: VideoSendPipeline,
    cfg: SendSettings,
    /// The stream descriptors we IDENTIFYed with, merged with READY's at start.
    streams: Vec<StreamSpec>,
    codecs: Vec<CodecSpec>,
    rtx_enabled: bool,
    active: Mutex<Active>,
    /// Bumped by a source swap; the thread then restarts its clocks and announces SPEAKING again.
    generation: AtomicU64,
    stop: AtomicBool,
    running: AtomicBool,
    video_ssrc: AtomicU32,
    pli: Mutex<Option<PliHandler>>,
    counters: Counters,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Sender {
    pub(crate) fn new(
        conn: Arc<RtcConnection>,
        streams: Vec<StreamSpec>,
        codecs: Vec<CodecSpec>,
        rtx_enabled: bool,
        cfg: SendSettings,
        pli: Option<PliHandler>,
    ) -> Arc<Self> {
        Arc::new(Self {
            conn: Mutex::new(conn),
            pipeline: VideoSendPipeline::new(),
            cfg,
            streams,
            codecs,
            rtx_enabled,
            active: Mutex::new(Active {
                source: None,
                video: None,
                audio: AudioState {
                    sequence: 0,
                    timestamp: 0,
                    packet_count: 0,
                    octet_count: 0,
                    last_rtcp_media_ms: None,
                    last_was_silence: true,
                },
                params: None,
            }),
            generation: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            running: AtomicBool::new(false),
            video_ssrc: AtomicU32::new(0),
            pli: Mutex::new(pli),
            counters: Counters::default(),
            thread: Mutex::new(None),
        })
    }

    pub(crate) fn set_pli_handler(&self, handler: Option<PliHandler>) {
        *self.pli.lock() = handler;
    }

    fn conn(&self) -> Arc<RtcConnection> {
        Arc::clone(&self.conn.lock())
    }

    /// Moves the sender to `conn` (the stream's new server). A running sender restarts video on the
    /// new connection's SSRC and negotiated codec (`start_video`, op 12) and announces SPEAKING
    /// again; the audio sender report counts start over for the new SSRC. Returns `true` when the
    /// negotiated video codec changed: no video is sent until the owner swaps in a source for it.
    pub(crate) fn rebind(self: &Arc<Self>, conn: Arc<RtcConnection>) -> Result<bool, VoiceError> {
        let mut active = self.active.lock();
        *self.conn.lock() = Arc::clone(&conn);
        active.audio.packet_count = 0;
        active.audio.octet_count = 0;
        active.audio.last_rtcp_media_ms = None;
        if !self.is_running() {
            return Ok(false);
        }
        let mut codec_changed = false;
        if let Some(params) = active.params {
            let before = active.video.as_ref().map(|v| v.codec);
            let codec = self.apply_video_start(&mut active, params)?;
            if before.is_some_and(|before| before != codec) {
                active.video = None;
                codec_changed = true;
            }
        }
        self.install_feedback();
        conn.ensure_rtcp_listener();
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(codec_changed)
    }

    /// What the last `play` announced.
    pub(crate) fn video_params(&self) -> Option<VideoParams> {
        self.active.lock().params
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn stats(&self) -> SendStats {
        let c = &self.counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        SendStats {
            video_frames: get(&c.video_frames),
            video_packets: get(&c.video_packets),
            video_octets: get(&c.video_octets),
            audio_packets: get(&c.audio_packets),
            dave_encrypt_errors: get(&c.dave_encrypt_errors),
            send_errors: get(&c.send_errors),
            nack_packets_received: get(&c.nack_packets_received),
            rtx_packets_resent: get(&c.rtx_packets_resent),
            pli_received: get(&c.pli_received),
        }
    }

    /// Starts video on the negotiated codec and spawns the sender thread (`VoiceClient.play`).
    pub(crate) fn start(
        self: &Arc<Self>,
        source: Box<dyn FrameSource>,
        params: VideoParams,
    ) -> Result<(), VoiceError> {
        if self.is_running() {
            return Err(VoiceError::Invalid("already playing media".into()));
        }
        let conn = self.conn();
        if conn.is_closed() {
            return Err(VoiceError::NotConnected);
        }
        {
            let mut active = self.active.lock();
            active.params = Some(params);
            self.apply_video_start(&mut active, params)?;
            active.source = Some(source);
        }
        self.stop.store(false, Ordering::Release);
        self.install_feedback();
        conn.ensure_rtcp_listener();
        self.running.store(true, Ordering::Release);
        let core = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("golive-send".into())
            .spawn(move || core.run());
        match spawned {
            Ok(handle) => {
                *self.thread.lock() = Some(handle);
                Ok(())
            }
            Err(e) => {
                self.running.store(false, Ordering::Release);
                Err(VoiceError::Udp(e))
            }
        }
    }

    /// Replaces the source in place (`set_source`): video restarts on the codec negotiated now
    /// (`start_video`, which clears the packetizer state and announces the video state again) and
    /// the thread restarts its clocks. Everything happens under the lock the thread holds while it
    /// reads and sends a frame, so no frame of the old source is sent on the new codec.
    pub(crate) fn swap(&self, source: Box<dyn FrameSource>) -> Result<(), VoiceError> {
        let mut active = self.active.lock();
        let params = active
            .params
            .ok_or_else(|| VoiceError::Invalid("no media was played yet".into()))?;
        self.apply_video_start(&mut active, params)?;
        active.source = Some(source);
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Stops the thread and waits for it (it sends Opus silence and stops video on its way out).
    /// Blocks for at most a frame, so call it from `spawn_blocking` in async code.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        let handle = self.thread.lock().take();
        if let Some(handle) = handle
            && handle.thread().id() != std::thread::current().id()
        {
            let _ = handle.join();
        }
        self.conn().receiver().set_feedback_handler(None);
    }

    /// `_apply_selected_video_stream` with `reset=True`, then the op 12 announcement.
    fn apply_video_start(
        &self,
        active: &mut Active,
        params: VideoParams,
    ) -> Result<VoiceCodec, VoiceError> {
        let conn = self.conn();
        let codec = conn
            .session_info()
            .video_codec
            .ok_or_else(|| {
                VoiceError::Invalid("voice server did not negotiate a video codec".into())
            })?
            .to_ascii_uppercase()
            .parse::<VoiceCodec>()
            .map_err(|e| VoiceError::Invalid(e.to_string()))?;
        let streams = negotiated_streams(&self.streams, conn.ready_streams());
        let selected = streams
            .iter()
            .filter(|s| s.ssrc.is_some())
            .max_by_key(|s| s.quality)
            .ok_or_else(|| {
                VoiceError::Invalid(
                    "voice server did not negotiate a screen SSRC for this connection".into(),
                )
            })?;
        let ssrc = selected.ssrc.expect("filtered on ssrc");
        let spec = self
            .codecs
            .iter()
            .find(|c| c.kind == "video" && c.name.eq_ignore_ascii_case(codec.as_str()))
            .ok_or_else(|| VoiceError::Invalid(format!("unsupported video codec: {codec:?}")))?;
        let rtx_payload_type = if self.rtx_enabled {
            spec.rtx_payload_type
        } else {
            None
        };
        let rtx_ssrc = if rtx_payload_type.is_some() {
            selected.rtx_ssrc.unwrap_or(ssrc.wrapping_add(1))
        } else {
            0
        };

        // reset: a fresh packetizer, counters and transport sequence
        self.pipeline.clear()?;
        self.pipeline.retain_streams(&[ssrc])?;
        self.pipeline.configure_stream(
            codec,
            ssrc,
            u16::from(spec.payload_type),
            rtx_payload_type.map(u16::from),
            rtx_ssrc,
            &selected.rid,
        )?;
        active.video = Some(VideoStream {
            codec,
            ssrc,
            media_time_ms: 0.0,
            last_rtcp_media_ms: -1.0,
            packet_count: 0,
            octet_count: 0,
        });
        self.video_ssrc.store(ssrc, Ordering::Release);

        let bitrate = if params.bitrate > 0 {
            params.bitrate
        } else {
            native_video_bitrate(params.height, params.fps)
        };
        let mut state = selected.clone();
        state.active = true;
        state.rtx_ssrc = (rtx_ssrc != 0).then_some(rtx_ssrc);
        state.max_bitrate = Some(state.max_bitrate.unwrap_or(if state.quality >= 100 {
            u64::from(bitrate)
        } else {
            u64::from(bitrate / 4).max(1)
        }));
        state.max_framerate = Some(state.max_framerate.unwrap_or(params.fps));
        state.max_resolution = Some(
            state
                .max_resolution
                .unwrap_or_else(|| StreamResolution::fixed(params.width, params.height)),
        );
        conn.ws().video_state(ssrc, rtx_ssrc, &[state])?;
        Ok(codec)
    }

    /// `_ensure_rtcp_feedback_listener` + `_handle_rtcp_nack` / `_handle_rtcp_pli`.
    fn install_feedback(self: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        self.conn()
            .receiver()
            .set_feedback_handler(Some(Arc::new(move |feedback| {
                if let Some(core) = weak.upgrade() {
                    core.on_feedback(feedback);
                }
            })));
    }

    fn on_feedback(&self, feedback: Feedback) {
        match feedback {
            Feedback::Nack { payload } => self.on_nack(&payload),
            Feedback::KeyframeRequest { media_ssrc, .. } => {
                if media_ssrc != self.video_ssrc.load(Ordering::Acquire)
                    || !self.pipeline.has_stream(media_ssrc).unwrap_or(false)
                {
                    return;
                }
                add(&self.counters.pli_received, 1);
                let handler = self.pli.lock().clone();
                if let Some(handler) = handler {
                    handler();
                }
            }
        }
    }

    fn on_nack(&self, payload: &[u8]) {
        if payload.len() < 8 {
            return;
        }
        let media_ssrc = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if !self.pipeline.has_stream(media_ssrc).unwrap_or(false) {
            return;
        }
        add(&self.counters.nack_packets_received, 1);
        let conn = self.conn();
        let udp = conn.udp();
        match self.pipeline.send_video_rtx_for_nack(
            conn.crypto_tx(),
            &**udp as &dyn PacketSink,
            payload,
            MEDIA_STREAM_TYPE,
            self.cfg.burst_size,
            self.cfg.burst_interval,
        ) {
            Ok(summary) => add(&self.counters.rtx_packets_resent, summary.resent as u64),
            Err(_) => add(&self.counters.send_errors, 1),
        }
    }

    // ------------------------------------------------------------------ the sender thread

    fn run(self: Arc<Self>) {
        let start = Instant::now();
        let mut next_audio = start;
        let mut next_video = start;
        let mut seen_generation = u64::MAX;
        let mut closed_since: Option<Instant> = None;
        while !self.stop.load(Ordering::Acquire) {
            let conn = self.conn();
            if conn.is_closed() {
                // `MediaPlayer`: wait for a reconnect (`rebind` bumps the generation)
                let since = *closed_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= RECONNECT_TIMEOUT {
                    break;
                }
                std::thread::sleep(MAX_SLEEP);
                continue;
            }
            closed_since = None;
            let generation = self.generation.load(Ordering::Acquire);
            if generation != seen_generation {
                seen_generation = generation;
                let now = Instant::now();
                next_audio = now;
                next_video = now;
                let _ = conn.ws().speak(speaking::SOUNDSHARE, 0);
            }
            let now = Instant::now();
            if now >= next_audio {
                self.send_audio_tick();
                next_audio += AUDIO_FRAME;
            }
            if now >= next_video {
                next_video = match self.send_video_tick() {
                    VideoTick::Sent(frame_time) => next_video_time(next_video, frame_time),
                    VideoTick::Idle => Instant::now() + VIDEO_RETRY,
                    VideoTick::NotReady => Instant::now() + VIDEO_NOT_READY_RETRY,
                };
            }
            let now = Instant::now();
            let wake = next_audio.min(next_video);
            let delay = wake.saturating_duration_since(now).min(MAX_SLEEP);
            if !delay.is_zero() {
                wait_until(now + delay);
            }
        }
        self.finish();
    }

    /// End of playback: trailing Opus silence, then `stop_video`.
    fn finish(&self) {
        let conn = self.conn();
        if !conn.is_closed() {
            let needs_silence = !self.active.lock().audio.last_was_silence;
            if needs_silence {
                for _ in 0..SILENCE_PACKET_COUNT {
                    self.send_audio_packet(&OPUS_SILENCE);
                }
            }
            let _ = conn.ws().video_state(0, 0, &[]);
        }
        let mut active = self.active.lock();
        active.video = None;
        active.source = None;
        let _ = self.pipeline.clear();
        self.video_ssrc.store(0, Ordering::Release);
        self.running.store(false, Ordering::Release);
    }

    fn send_audio_tick(&self) {
        let data = {
            let mut active = self.active.lock();
            match active.source.as_mut() {
                Some(source) => source.next_audio(),
                None => return,
            }
        };
        if !data.is_empty() {
            self.send_audio_packet(&data);
        }
    }

    /// `send_audio_packet(data, encode=False)` + `_get_voice_packet`.
    fn send_audio_packet(&self, data: &[u8]) {
        let audio_level = if data == OPUS_SILENCE {
            RTP_AUDIO_LEVEL_SILENCE
        } else {
            0
        };
        let conn = self.conn();
        let encrypted = match conn.dave().encrypt_opus(data) {
            Ok(encrypted) => encrypted,
            Err(_) => {
                add(&self.counters.dave_encrypt_errors, 1);
                return;
            }
        };
        let (sequence, timestamp, report) = {
            let mut active = self.active.lock();
            let audio = &mut active.audio;
            audio.sequence = audio.sequence.wrapping_add(1);
            let timestamp = audio.timestamp;
            audio.timestamp = audio.timestamp.wrapping_add(AUDIO_SAMPLES_PER_FRAME);
            audio.last_was_silence = data == OPUS_SILENCE;
            // `_maybe_send_audio_rtcp_sender_report`: once per second of media time
            let media_ms = u64::from(timestamp) * 1000 / AUDIO_CLOCK_RATE;
            let due = audio
                .last_rtcp_media_ms
                .is_none_or(|last| media_ms / 1000 > last / 1000);
            if due {
                audio.last_rtcp_media_ms = Some(media_ms);
            }
            let report = due.then_some((audio.packet_count, audio.octet_count));
            (audio.sequence, timestamp, report)
        };
        let ssrc = conn.ssrc();
        if let Some((packets, octets)) = report {
            self.send_sender_report(&conn, ssrc, timestamp, packets, octets);
        }
        let packet = (|| {
            let mut header = RtpPacket::new_header(
                relay_media::codec::OPUS_PAYLOAD_TYPE,
                sequence,
                timestamp,
                ssrc,
                false,
                false,
            );
            let extension = build_audio_extension_payload(speaking::SOUNDSHARE, audio_level)?;
            header = header_with_one_byte_extensions(&header, &extension)?;
            let mut payload = extension;
            payload.extend_from_slice(&encrypted);
            conn.crypto_tx().encrypt_rtp(&header, &payload)
        })();
        let sent = match packet {
            Ok(packet) => conn.udp().send_packet(&packet).is_ok(),
            Err(_) => false,
        };
        if sent {
            add(&self.counters.audio_packets, 1);
            let mut active = self.active.lock();
            active.audio.packet_count = active.audio.packet_count.wrapping_add(1);
            active.audio.octet_count = active
                .audio
                .octet_count
                .wrapping_add(u32::try_from(data.len()).unwrap_or(u32::MAX));
        } else {
            add(&self.counters.send_errors, 1);
        }
    }

    /// `send_video_frame` for the next access unit of the source.
    fn send_video_tick(&self) -> VideoTick {
        let mut active = self.active.lock();
        let Active { source, video, .. } = &mut *active;
        let (Some(source), Some(video)) = (source.as_mut(), video.as_mut()) else {
            return VideoTick::NotReady;
        };
        let Some(frame) = source.next_video() else {
            return VideoTick::Idle;
        };
        let frame_time_ms = frame.frame_time_ms.max(0.001);
        let frame_time = Duration::from_secs_f64(frame_time_ms / 1000.0);
        let codec = video.codec;
        let original = if codec == VoiceCodec::H264 {
            strip_h264_filler_nalus(&frame.data)
        } else {
            frame.data
        };
        let conn = self.conn();
        let encrypted = match conn.dave().encrypt_video(codec, &original) {
            Ok(encrypted) => encrypted,
            Err(_) => {
                add(&self.counters.dave_encrypt_errors, 1);
                return VideoTick::Sent(frame_time);
            }
        };
        let udp = conn.udp();
        let metadata = (codec == VoiceCodec::VP9).then_some(original.as_slice());
        let sent = self.pipeline.send_video_frame(
            conn.crypto_tx(),
            &**udp as &dyn PacketSink,
            video.ssrc,
            &encrypted,
            metadata,
            frame_time_ms,
            MEDIA_STREAM_TYPE,
            self.cfg.burst_size,
            self.cfg.burst_interval,
        );
        match sent {
            Ok(summary) => {
                add(&self.counters.video_frames, 1);
                add(&self.counters.video_packets, summary.sent as u64);
                add(&self.counters.video_octets, summary.octets as u64);
                video.packet_count = video.packet_count.wrapping_add(summary.sent as u32);
                video.octet_count = video.octet_count.wrapping_add(summary.octets as u32);
                // `_maybe_send_video_rtcp_sender_report`: media time before this frame
                if summary.sent > 0
                    && (video.media_time_ms / 1000.0).floor()
                        > (video.last_rtcp_media_ms / 1000.0).floor()
                {
                    self.send_sender_report(
                        &conn,
                        video.ssrc,
                        summary.rtp_timestamp,
                        video.packet_count,
                        video.octet_count,
                    );
                    video.last_rtcp_media_ms = video.media_time_ms;
                }
                video.media_time_ms += frame_time_ms;
            }
            Err(_) => add(&self.counters.send_errors, 1),
        }
        VideoTick::Sent(frame_time)
    }

    /// `_send_rtcp_sender_report`.
    fn send_sender_report(
        &self,
        conn: &RtcConnection,
        ssrc: u32,
        rtp_timestamp: u32,
        packets: u32,
        octets: u32,
    ) {
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        let (header, body) = rtcp::sender_report(ssrc, unix, rtp_timestamp, packets, octets);
        let sent = conn
            .crypto_tx()
            .encrypt_rtcp(&header, &body)
            .map_err(|_| ())
            .and_then(|packet| conn.udp().send_packet(&packet).map_err(|_| ()));
        if sent.is_err() {
            add(&self.counters.send_errors, 1);
        }
    }
}

enum VideoTick {
    /// A frame went out (or was dropped); the next is due after this long.
    Sent(Duration),
    /// The source had nothing due.
    Idle,
    /// Video is not started (no codec or SSRC yet).
    NotReady,
}

/// `MediaPlayer._next_video_time` for a non-realtime source: the next slot, or now when more than a
/// frame behind.
fn next_video_time(next_video: Instant, frame_time: Duration) -> Instant {
    let target = next_video + frame_time;
    let now = Instant::now();
    if now.saturating_duration_since(target) > frame_time {
        now
    } else {
        target
    }
}

/// `connection.video_streams` after READY: the descriptors we IDENTIFYed with, updated by the
/// payload of the same `rid`, plus any stream only the server named.
fn negotiated_streams(settings: &[StreamSpec], ready: &[Value]) -> Vec<StreamSpec> {
    let mut streams = settings.to_vec();
    for payload in ready {
        let Some(rid) = payload.get("rid").and_then(Value::as_str) else {
            continue;
        };
        let index = match streams.iter().position(|s| s.rid == rid) {
            Some(index) => index,
            None => {
                let kind = if payload.get("type").and_then(Value::as_str) == Some("video") {
                    "video"
                } else {
                    "screen"
                };
                let mut spec = StreamSpec::screen(rid, 100, None);
                spec.kind = kind;
                streams.push(spec);
                streams.len() - 1
            }
        };
        let spec = &mut streams[index];
        if let Some(v) = payload.get("quality").and_then(Value::as_u64) {
            spec.quality = u32::try_from(v).unwrap_or(spec.quality);
        }
        if let Some(v) = payload.get("active").and_then(Value::as_bool) {
            spec.active = v;
        }
        if let Some(v) = payload.get("max_bitrate").and_then(Value::as_u64) {
            spec.max_bitrate = Some(v);
        }
        if let Some(v) = payload.get("max_framerate").and_then(Value::as_u64) {
            spec.max_framerate = u32::try_from(v).ok();
        }
        if let Some(r) = payload.get("max_resolution").and_then(Value::as_object) {
            let kind = r.get("type").and_then(Value::as_str).unwrap_or("fixed");
            let dim = |k: &str| {
                r.get(k)
                    .and_then(Value::as_u64)
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(0)
            };
            spec.max_resolution = Some(StreamResolution {
                kind: kind.to_owned(),
                width: dim("width"),
                height: dim("height"),
            });
        }
        if let Some(v) = payload.get("ssrc").and_then(Value::as_u64) {
            spec.ssrc = u32::try_from(v).ok();
        }
        if let Some(v) = payload.get("rtx_ssrc").and_then(Value::as_u64) {
            spec.rtx_ssrc = u32::try_from(v).ok();
        }
    }
    streams
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn bitrate_follows_resolution_and_fps() {
        assert_eq!(native_video_bitrate(1080, 60), 8_000_000);
        assert_eq!(native_video_bitrate(720, 30), 4_000_000);
        assert_eq!(native_video_bitrate(720, 31), 8_000_000);
        assert_eq!(native_video_bitrate(721, 30), 8_000_000);
    }

    #[test]
    fn ready_stream_fills_in_the_ssrcs_of_the_identified_one() {
        let settings = [StreamSpec::screen("100", 100, Some(60))];
        let ready = [json!({"type": "screen", "rid": "100", "quality": 100,
                            "ssrc": 11, "rtx_ssrc": 12, "active": false})];
        let streams = negotiated_streams(&settings, &ready);
        assert_eq!(streams.len(), 1);
        assert_eq!(
            (
                streams[0].ssrc,
                streams[0].rtx_ssrc,
                streams[0].max_framerate
            ),
            (Some(11), Some(12), Some(60))
        );
        assert!(!streams[0].active);
    }

    #[test]
    fn unknown_ready_stream_is_added_and_malformed_ones_ignored() {
        let streams = negotiated_streams(&[], &[json!({"rid": "50", "ssrc": 5}), json!({"x": 1})]);
        assert_eq!(streams.len(), 1);
        assert_eq!((streams[0].rid.as_str(), streams[0].ssrc), ("50", Some(5)));
    }

    #[test]
    fn video_clock_catches_up_only_when_more_than_a_frame_late() {
        let frame = Duration::from_millis(16);
        let on_time = Instant::now();
        let next = next_video_time(on_time, frame);
        assert_eq!(next, on_time + frame);
        let late = Instant::now()
            .checked_sub(Duration::from_millis(100))
            .expect("monotonic clock far enough from its origin");
        assert!(next_video_time(late, frame) > late + frame);
    }
}
