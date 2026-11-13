//! Media receive pipeline: transport decrypt, RTX recovery, reordering, NACK/PLI/receiver-report
//! feedback, depacketizing and DAVE decrypt, producing [`Packet`]s.
//!
//! Port of the receive half of `discord/ext/native_voice/client.py` (`_decode_media_packets`,
//! `_decode_decoded_media_packet`, `_decode_video_packets`, `_decode_video_frame`, the pending DAVE
//! frame queue, `_track_video_receive_sequence`, the NACK bookkeeping and the RTCP handlers) and of
//! `rtp.py::_VideoReceiveReorderBuffer`.
//!
//! The pipeline is synchronous. `UdpTransport` calls [`Receiver::process_batch`] from its dispatcher
//! thread and hands the result to the application callback, so nothing here needs an async runtime.
//!
//! Deliberate differences from Python:
//! - A depacketizer error drops that packet (and its frame) only; Python threw away the whole batch.
//! - Debug output is replaced by counters ([`Receiver::stats`]); there is no logging dependency.
//! - `raw` (the encrypted datagram) is not kept on [`Packet`]; nothing downstream uses it.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use relay_media::codec::VoiceCodec;
use relay_media::opus::OPUS_SILENCE;

use crate::dave::{Dave, DaveDecryptError, DaveMedia};
use crate::rtcp::{
    self, RTCP_GENERIC_NACK, RTCP_NACK_MAX_SEQUENCES, RTCP_PAYLOAD_FEEDBACK,
    RTCP_PICTURE_LOSS_INDICATION, RTCP_PROTECTED_HEADER_LEN, RTCP_RECEIVER_REPORT,
    RTCP_RTP_FEEDBACK, RTCP_SENDER_REPORT, RTP_EXT_ABSOLUTE_SEND_TIME, RTP_EXT_AUDIO_LEVEL,
    RTP_EXT_DISCORD_SPEAKING, ReceiveReportState, ReportBlock, VIDEO_NACK_MAX_OUTSTANDING,
    VIDEO_NACK_MAX_RETRIES, VIDEO_NACK_RETRY_INTERVAL,
};
use crate::transport::packets::{SendError, SharedTransportCrypto};
use crate::video::{PacketSink, VideoDepacketizer, depacketizer_for};
use crate::voice_ws::{CodecSpec, speaking};

/// RTCP payload-specific feedback format 4: full intra request.
const RTCP_FIR: u8 = 4;
const DAVE_PENDING_VIDEO_MAX_FRAMES: usize = 128;
const DAVE_PENDING_VIDEO_MAX_AGE: Duration = Duration::from_secs(10);
const DAVE_TAG_SIZE: usize = 8;
const DAVE_SIZE_SIZE: usize = 1;
const DAVE_MARKER: [u8; 2] = [0xFA, 0xFA];
const DAVE_MIN_SIZE: usize = DAVE_TAG_SIZE + DAVE_SIZE_SIZE + DAVE_MARKER.len();
const VIDEO_CLOCK_RATE: u32 = 90_000;
const AUDIO_CLOCK_RATE: u32 = 48_000;

// ---------------------------------------------------------------------------------------------
// public types

/// RTCP feedback a viewer sent about the stream we publish (handled by the send pipeline).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Feedback {
    /// Generic NACK: `payload` is the media SSRC followed by PID/BLP pairs.
    Nack { payload: Vec<u8> },
    /// Picture loss indication (or full intra request) for `media_ssrc`.
    KeyframeRequest { media_ssrc: u32, fir: bool },
}

/// Called on the UDP dispatcher thread, outside the receiver's lock.
pub type FeedbackHandler = Arc<dyn Fn(Feedback) + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpExtension {
    pub id: u8,
    pub data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaType {
    Audio,
    Video,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PacketCodec {
    Opus,
    Video(VoiceCodec),
}

impl PacketCodec {
    /// `"opus"` or the video codec name (`"H264"`...), as `MediaPacket.codec` in Python.
    pub fn name(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::Video(codec) => codec.as_str(),
        }
    }

    pub fn video(self) -> Option<VoiceCodec> {
        match self {
            Self::Opus => None,
            Self::Video(codec) => Some(codec),
        }
    }
}

/// One RTP packet that went into a [`Packet`] (`RTPPacket`, without the raw bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtpPacketInfo {
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub payload_type: u8,
    pub marker: bool,
    /// Recovered through RTX retransmission.
    pub rtx: bool,
    pub payload_len: usize,
}

/// A received media unit: one Opus packet, or one complete video access unit (`MediaPacket`).
///
/// For video, `payload` is the depacketized, DAVE-decrypted AU (Annex-B for H264/H265), `sequence`,
/// `timestamp`, `payload_type` and `rtp_extensions` are those of the last RTP packet of the frame,
/// and `rtp_packets` lists every packet that built it.
#[derive(Clone, Debug)]
pub struct Packet {
    pub user_id: Option<u64>,
    pub media_type: MediaType,
    pub codec: PacketCodec,
    pub ssrc: u32,
    pub timestamp: u32,
    pub sequence: u16,
    pub payload: Vec<u8>,
    pub payload_type: u8,
    pub marker: bool,
    pub rtp_extensions: Vec<RtpExtension>,
    pub rtp_packets: Vec<RtpPacketInfo>,
    /// Local monotonic time the datagram batch arrived.
    pub received_at: Instant,
    /// Sender clock as Unix-epoch seconds, from RTCP sender reports (or abs-send-time).
    pub rtcp_time: Option<f64>,
    /// Audio only: [`speaking`] flags from the Discord speaking extension.
    pub speaking_flags: Option<u32>,
    /// Audio only: RTP audio level (0 loudest, 127 silence) and its voice-activity bit.
    pub audio_level: Option<u8>,
    pub audio_voice_activity: Option<bool>,
}

/// What [`ReceiveConfig::rtp_tap`] sees of every decrypted RTP packet, before any filtering. This
/// is where `relay-discord` records the transport-wide sequence number (extension id 5) for TWCC.
#[derive(Debug)]
pub struct RtpObservation<'a> {
    pub ssrc: u32,
    pub sequence: u16,
    pub timestamp: u32,
    pub payload_type: u8,
    pub rtp_extensions: &'a [RtpExtension],
    pub received_at: Instant,
}

pub type RtpTap = Arc<dyn Fn(&RtpObservation<'_>) + Send + Sync>;

#[derive(Clone)]
pub struct ReceiveConfig {
    /// The codecs advertised in SELECT_PROTOCOL; they define the payload type lookups.
    pub codecs: Vec<CodecSpec>,
    pub rtx_enabled: bool,
    /// Our SSRC from READY: the sender SSRC of every RTCP packet we emit.
    pub own_ssrc: u32,
    /// `video_receive_reorder_max_packets`.
    pub reorder_max_packets: usize,
    /// `video_receive_reorder_max_delay`.
    pub reorder_max_delay: Duration,
    /// `rtcp_receiver_report_interval` (zero disables the periodic reports).
    pub receiver_report_interval: Duration,
    /// `rtcp_receiver_report_ttl`.
    pub receiver_report_ttl: Duration,
    /// `enable_debug_stats`: collect the counters of [`Receiver::stats`].
    pub debug_stats: bool,
    pub rtp_tap: Option<RtpTap>,
}

impl ReceiveConfig {
    pub fn new(codecs: Vec<CodecSpec>, own_ssrc: u32) -> Self {
        Self {
            codecs,
            rtx_enabled: true,
            own_ssrc,
            reorder_max_packets: 8192,
            reorder_max_delay: Duration::from_millis(500),
            receiver_report_interval: Duration::from_secs(1),
            receiver_report_ttl: Duration::from_secs(3),
            debug_stats: false,
            rtp_tap: None,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// SSRC bookkeeping (`ssrc_user_ids`, `video_ssrcs`, `rtx_ssrc_media_ssrcs`, `video_states`)

/// SSRCs of one stream descriptor inside an op 12 VIDEO message.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamSsrcs {
    pub rid: String,
    pub ssrc: Option<u32>,
    pub rtx_ssrc: Option<u32>,
}

/// An op 12 VIDEO message, already merged with the user's previous one
/// (`DiscordVoiceWebSocket._update_video_state_streams`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VideoState {
    pub audio_ssrc: Option<u32>,
    pub video_ssrc: Option<u32>,
    pub rtx_ssrc: Option<u32>,
    pub streams: Vec<StreamSsrcs>,
}

impl VideoState {
    /// `_video_state_ssrcs`.
    fn ssrcs(&self, include_audio: bool) -> Vec<u32> {
        let mut out = Vec::new();
        if include_audio {
            out.extend(self.audio_ssrc);
        }
        out.extend(self.video_ssrc);
        out.extend(self.rtx_ssrc);
        for stream in &self.streams {
            out.extend(stream.ssrc);
            out.extend(stream.rtx_ssrc);
        }
        out.retain(|&s| s != 0);
        out
    }

    /// `_video_state_rtx_ssrcs`: rtx ssrc to media ssrc.
    fn rtx_pairs(&self) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        if let (Some(video), Some(rtx)) = (self.video_ssrc, self.rtx_ssrc)
            && video != 0
            && rtx != 0
        {
            out.push((rtx, video));
        }
        for stream in &self.streams {
            if let (Some(ssrc), Some(rtx)) = (stream.ssrc, stream.rtx_ssrc)
                && ssrc != 0
                && rtx != 0
            {
                out.push((rtx, ssrc));
            }
        }
        out
    }
}

#[derive(Default)]
struct SsrcInner {
    ssrc_user: HashMap<u32, u64>,
    rtx_media: HashMap<u32, u32>,
    video_ssrcs: HashSet<u32>,
    video_states: HashMap<u64, VideoState>,
}

/// SSRC to user mapping of one voice connection, filled from SPEAKING and VIDEO messages by the
/// connection task and read by the receive thread.
#[derive(Default)]
pub struct SsrcMap {
    inner: Mutex<SsrcInner>,
}

impl SsrcMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// `clear_ssrc_mappings` (on READY).
    pub fn clear(&self) {
        *self.inner.lock() = SsrcInner::default();
    }

    /// op 5 SPEAKING.
    pub fn set_speaking(&self, ssrc: u32, user_id: u64) {
        self.inner.lock().ssrc_user.insert(ssrc, user_id);
    }

    /// The previous op 12 state of `user_id`, to merge a new message with.
    pub fn video_state(&self, user_id: u64) -> Option<VideoState> {
        self.inner.lock().video_states.get(&user_id).cloned()
    }

    /// `update_video_state`.
    pub fn update_video_state(&self, user_id: u64, state: VideoState) {
        let mut inner = self.inner.lock();
        if let Some(previous) = inner.video_states.get(&user_id).cloned() {
            for ssrc in previous.ssrcs(false) {
                if inner.ssrc_user.get(&ssrc) == Some(&user_id) {
                    inner.ssrc_user.remove(&ssrc);
                }
                inner.video_ssrcs.remove(&ssrc);
            }
            for (rtx, media) in previous.rtx_pairs() {
                if inner.rtx_media.get(&rtx) == Some(&media) {
                    inner.rtx_media.remove(&rtx);
                }
            }
        }
        for ssrc in state.ssrcs(true) {
            inner.ssrc_user.insert(ssrc, user_id);
        }
        inner.video_ssrcs.extend(state.ssrcs(false));
        inner.rtx_media.extend(state.rtx_pairs());
        inner.video_states.insert(user_id, state);
    }

    pub fn user_for(&self, ssrc: u32) -> Option<u64> {
        self.inner.lock().ssrc_user.get(&ssrc).copied()
    }

    fn media_for_rtx(&self, ssrc: u32) -> Option<u32> {
        self.inner.lock().rtx_media.get(&ssrc).copied()
    }

    fn is_video(&self, ssrc: u32) -> bool {
        self.inner.lock().video_ssrcs.contains(&ssrc)
    }
}

// ---------------------------------------------------------------------------------------------
// internal state

#[derive(Clone)]
struct Decoded {
    payload_type: u8,
    marker: bool,
    sequence: u16,
    timestamp: u32,
    ssrc: u32,
    rtp_extensions: Vec<RtpExtension>,
    payload: Vec<u8>,
}

struct Received {
    decoded: Decoded,
    codec: VoiceCodec,
    ssrc: u32,
    user_id: Option<u64>,
    rtp_packet: RtpPacketInfo,
    received_at: Instant,
}

/// `_VideoReceiveReorderBuffer`.
struct ReorderBuffer {
    max_packets: usize,
    max_delay: Duration,
    expected: Option<i64>,
    blocked_at: Option<Instant>,
    packets: BTreeMap<i64, Received>,
    pushed: u64,
    duplicates: u64,
    old: u64,
    waits: u64,
    flushes: u64,
    skipped: u64,
    ready: u64,
    max_buffered: u64,
}

impl ReorderBuffer {
    fn new(max_packets: usize, max_delay: Duration) -> Self {
        Self {
            max_packets: max_packets.max(1),
            max_delay,
            expected: None,
            blocked_at: None,
            packets: BTreeMap::new(),
            pushed: 0,
            duplicates: 0,
            old: 0,
            waits: 0,
            flushes: 0,
            skipped: 0,
            ready: 0,
            max_buffered: 0,
        }
    }

    fn push(&mut self, packet: Received, now: Instant) -> Vec<Received> {
        let sequence = packet.decoded.sequence;
        self.pushed += 1;
        let (expected, unwrapped) = match self.expected {
            None => {
                self.expected = Some(i64::from(sequence));
                (i64::from(sequence), i64::from(sequence))
            }
            Some(expected) => (expected, rtcp::unwrap_sequence(sequence, expected)),
        };
        if unwrapped < expected {
            self.old += 1;
            return Vec::new();
        }
        if self.packets.contains_key(&unwrapped) {
            self.duplicates += 1;
        }
        self.packets.entry(unwrapped).or_insert(packet);
        self.max_buffered = self.max_buffered.max(self.packets.len() as u64);
        let ready = self.pop_ready();
        if !ready.is_empty() {
            self.blocked_at = None;
            self.ready += ready.len() as u64;
            return ready;
        }
        let blocked_at = *self.blocked_at.get_or_insert(now);
        if self.packets.len() <= self.max_packets
            && now.saturating_duration_since(blocked_at) < self.max_delay
        {
            self.waits += 1;
            return Vec::new();
        }
        // Waited long enough for the missing packet(s): skip ahead to the oldest one held.
        let skip_to = *self
            .packets
            .keys()
            .next()
            .expect("a packet was just inserted");
        self.skipped += (skip_to - expected).max(0) as u64;
        self.expected = Some(skip_to);
        self.blocked_at = None;
        self.flushes += 1;
        let ready = self.pop_ready();
        self.ready += ready.len() as u64;
        ready
    }

    fn pop_ready(&mut self) -> Vec<Received> {
        let Some(mut expected) = self.expected else {
            return Vec::new();
        };
        let mut ready = Vec::new();
        while let Some(packet) = self.packets.remove(&expected) {
            ready.push(packet);
            expected += 1;
        }
        self.expected = Some(expected);
        ready
    }
}

#[derive(Default)]
struct PendingNack {
    sent_at: Option<Instant>,
    retries: u32,
}

struct PendingDaveFrame {
    decoded: Decoded,
    codec: VoiceCodec,
    ssrc: u32,
    user_id: Option<u64>,
    payload: Vec<u8>,
    rtp_packets: Vec<RtpPacketInfo>,
    received_at: Instant,
    queued_at: Instant,
    /// A decrypt of this frame already failed and was counted in `Dave::decrypt_failures`.
    counted: bool,
}

/// Receive health of one sender, as `Instant`s (the receiver's creation until the first event).
#[derive(Clone, Copy)]
struct UserHealth {
    packet: Instant,
    decrypted: Instant,
    undecryptable: Instant,
}

struct Health {
    rtp: Instant,
    users: HashMap<u64, UserHealth>,
}

/// What [`Receiver::health`] reports about one sender. Ages count from the receiver's creation
/// while the event has not happened yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiveHealth {
    /// Any RTP packet that passed the transport decrypt.
    pub rtp_age: Duration,
    /// Any media packet mapped to the sender, before the DAVE decrypt.
    pub packet_age: Duration,
    /// The last media unit of the sender that decrypted.
    pub decrypted_age: Duration,
    /// The last media unit of the sender that was stashed or failed the DAVE decrypt.
    pub undecryptable_age: Duration,
    /// A DAVE session exists that cannot decrypt the sender yet.
    pub dave_stuck: bool,
}

#[derive(Default)]
struct RxState {
    depacketizers: HashMap<(u32, u8), Box<dyn VideoDepacketizer + Send>>,
    frame_rtp: HashMap<(u32, u8), Vec<RtpPacketInfo>>,
    last_sequence: HashMap<u32, u16>,
    pending_nacks: HashMap<u32, HashMap<u16, PendingNack>>,
    reorder: HashMap<(u32, u8), ReorderBuffer>,
    startup_plis: HashSet<u32>,
    pending_dave: VecDeque<PendingDaveFrame>,
    /// ssrc to (sender NTP unix seconds, RTP timestamp, clock rate).
    sender_reports: HashMap<u32, (f64, u32, u32)>,
    /// ssrc to (compact NTP of its last SR, when it arrived).
    sender_report_lsr: HashMap<u32, (u32, Instant)>,
    receiver_reports: HashMap<(u32, u32), (ReportBlock, Instant)>,
    report_states: HashMap<u32, ReceiveReportState>,
    stats: BTreeMap<&'static str, u64>,
    /// Receive-side retired reorder-buffer counters (`_record_video_reorder_stats`).
    retired_reorder: BTreeMap<&'static str, u64>,
}

// ---------------------------------------------------------------------------------------------
// the receiver

pub struct Receiver {
    cfg: ReceiveConfig,
    /// payload type to codec for every advertised codec.
    payload_types: HashMap<u8, PacketCodec>,
    /// rtx payload type to (codec, associated payload type); empty without RTX.
    rtx_types: HashMap<u8, (VoiceCodec, u8)>,
    crypto_rx: SharedTransportCrypto,
    crypto_tx: SharedTransportCrypto,
    sink: Arc<dyn PacketSink + Send + Sync>,
    dave: Arc<Dave>,
    ssrcs: Arc<SsrcMap>,
    anchor: Instant,
    state: Mutex<RxState>,
    health: Mutex<Health>,
    feedback: Mutex<Option<FeedbackHandler>>,
}

impl Receiver {
    pub fn new(
        cfg: ReceiveConfig,
        crypto_rx: SharedTransportCrypto,
        crypto_tx: SharedTransportCrypto,
        sink: Arc<dyn PacketSink + Send + Sync>,
        dave: Arc<Dave>,
        ssrcs: Arc<SsrcMap>,
    ) -> Self {
        let mut payload_types = HashMap::new();
        let mut rtx_types = HashMap::new();
        for spec in &cfg.codecs {
            let codec = if spec.kind == "video" {
                match VoiceCodec::from_str(&spec.name.to_ascii_uppercase()) {
                    Ok(codec) => PacketCodec::Video(codec),
                    Err(_) => continue,
                }
            } else if spec.name == "opus" {
                PacketCodec::Opus
            } else {
                continue;
            };
            payload_types.insert(spec.payload_type, codec);
            if let (PacketCodec::Video(video), Some(rtx)) = (codec, spec.rtx_payload_type) {
                rtx_types.insert(rtx, (video, spec.payload_type));
            }
        }
        if !cfg.rtx_enabled {
            rtx_types.clear();
        }
        let anchor = Instant::now();
        Self {
            cfg,
            payload_types,
            rtx_types,
            crypto_rx,
            crypto_tx,
            sink,
            dave,
            ssrcs,
            anchor,
            state: Mutex::new(RxState::default()),
            health: Mutex::new(Health {
                rtp: anchor,
                users: HashMap::new(),
            }),
            feedback: Mutex::new(None),
        }
    }

    /// Always-on receive health of `user_id` (not gated by `debug_stats`).
    pub fn health(&self, user_id: u64) -> ReceiveHealth {
        let now = Instant::now();
        let h = self.health.lock();
        let u = h.users.get(&user_id).copied().unwrap_or(UserHealth {
            packet: self.anchor,
            decrypted: self.anchor,
            undecryptable: self.anchor,
        });
        let age = |at: Instant| now.saturating_duration_since(at);
        ReceiveHealth {
            rtp_age: age(h.rtp),
            packet_age: age(u.packet),
            decrypted_age: age(u.decrypted),
            undecryptable_age: age(u.undecryptable),
            dave_stuck: self.dave.has_session() && !self.dave.can_decrypt(user_id),
        }
    }

    fn note(&self, user: Option<u64>, update: impl FnOnce(&mut UserHealth, Instant)) {
        let Some(user) = user else { return };
        let now = Instant::now();
        let anchor = self.anchor;
        let mut h = self.health.lock();
        let u = h.users.entry(user).or_insert(UserHealth {
            packet: anchor,
            decrypted: anchor,
            undecryptable: anchor,
        });
        update(u, now);
    }

    fn note_decrypted(&self, user: Option<u64>) {
        self.note(user, |u, now| u.decrypted = now);
    }

    fn note_undecryptable(&self, user: Option<u64>) {
        self.note(user, |u, now| u.undecryptable = now);
    }

    /// Routes the NACK / PLI / FIR packets of viewers to `handler` (`None` ignores them).
    pub fn set_feedback_handler(&self, handler: Option<FeedbackHandler>) {
        *self.feedback.lock() = handler;
    }

    pub fn own_ssrc(&self) -> u32 {
        self.cfg.own_ssrc
    }

    pub fn rtx_enabled(&self) -> bool {
        self.cfg.rtx_enabled
    }

    fn secs(&self, at: Instant) -> f64 {
        at.saturating_duration_since(self.anchor).as_secs_f64()
    }

    /// Decrypts, reorders and depacketizes a batch of RTP datagrams. Frames held back for DAVE keys
    /// that have arrived since are released first, like Python.
    pub fn process_batch(&self, batch: &[Vec<u8>], received_at: Instant) -> Vec<Packet> {
        let mut st = self.state.lock();
        let mut out = self.retry_pending_dave(&mut st, received_at);
        let (failed, decrypted) = match self.crypto_rx.decrypt_rtp_packets(batch) {
            Ok(result) => result,
            Err(_) => {
                self.stat(&mut st, "decrypt_batch_errors", 1);
                return out;
            }
        };
        if failed > 0 {
            self.stat(&mut st, "rtp_decrypt_failures", failed as u64);
        }
        if !decrypted.is_empty() {
            self.health.lock().rtp = Instant::now();
        }
        for packet in decrypted {
            let header = &packet.packet;
            let decoded = Decoded {
                payload_type: header.payload_type,
                marker: header.marker,
                sequence: header.sequence,
                timestamp: header.timestamp,
                ssrc: header.ssrc,
                rtp_extensions: packet
                    .extensions
                    .into_iter()
                    .map(|(id, data)| RtpExtension { id, data })
                    .collect(),
                payload: packet.media_payload,
            };
            self.decode_one(&mut st, decoded, received_at, &mut out);
        }
        out
    }

    fn stat(&self, st: &mut RxState, key: &'static str, amount: u64) {
        if self.cfg.debug_stats {
            *st.stats.entry(key).or_default() += amount;
        }
    }

    /// `_decode_decoded_media_packet`.
    fn decode_one(
        &self,
        st: &mut RxState,
        mut d: Decoded,
        received_at: Instant,
        out: &mut Vec<Packet>,
    ) {
        if let Some(tap) = &self.cfg.rtp_tap {
            tap(&RtpObservation {
                ssrc: d.ssrc,
                sequence: d.sequence,
                timestamp: d.timestamp,
                payload_type: d.payload_type,
                rtp_extensions: &d.rtp_extensions,
                received_at,
            });
        }
        let mut ssrc = d.ssrc;
        let mut rtx = false;
        let codec = if let Some(&(video, associated)) = self.rtx_types.get(&d.payload_type) {
            self.stat(st, "rtx_packets_received", 1);
            if d.payload.len() < 2 {
                self.stat(st, "rtx_packets_too_short", 1);
                return;
            }
            rtx = true;
            if let Some(media) = self
                .ssrcs
                .media_for_rtx(ssrc)
                .or_else(|| ssrc.checked_sub(1).filter(|_| ssrc > 0))
            {
                ssrc = media;
            }
            d = Decoded {
                payload_type: associated,
                sequence: u16::from_be_bytes([d.payload[0], d.payload[1]]),
                payload: d.payload[2..].to_vec(),
                ssrc,
                ..d
            };
            PacketCodec::Video(video)
        } else {
            let Some(&codec) = self.payload_types.get(&d.payload_type) else {
                self.stat(st, "unknown_payload_type", 1);
                return;
            };
            if matches!(codec, PacketCodec::Video(_)) {
                self.stat(st, "primary_packets_received", 1);
            }
            codec
        };
        let user_id = self.ssrcs.user_for(ssrc);
        self.note(user_id, |u, now| u.packet = now);
        let rtp_packet = RtpPacketInfo {
            sequence: d.sequence,
            timestamp: d.timestamp,
            ssrc,
            payload_type: d.payload_type,
            marker: d.marker,
            rtx,
            payload_len: d.payload.len(),
        };
        match codec {
            PacketCodec::Video(video) => {
                self.stat(st, "video_packets_received", 1);
                self.maybe_send_startup_pli(st, video, ssrc);
                if !rtx {
                    self.maybe_send_receiver_report(st, ssrc, d.sequence, d.timestamp, received_at);
                }
                self.track_sequence(st, ssrc, d.sequence, rtx, received_at);
                let received = Received {
                    decoded: d,
                    codec: video,
                    ssrc,
                    user_id,
                    rtp_packet,
                    received_at,
                };
                self.decode_video_packets(st, received, out);
            }
            PacketCodec::Opus => {
                if let Some(packet) =
                    self.decode_audio(st, d, ssrc, user_id, rtp_packet, received_at)
                {
                    out.push(packet);
                }
            }
        }
    }

    // ----- audio

    fn decode_audio(
        &self,
        st: &mut RxState,
        d: Decoded,
        ssrc: u32,
        user_id: Option<u64>,
        rtp_packet: RtpPacketInfo,
        received_at: Instant,
    ) -> Option<Packet> {
        let mut payload = d.payload;
        let silence = is_unencrypted_opus_silence(&payload);
        if !silence && self.dave.has_session() {
            let looks_like_dave = looks_like_dave_protocol_frame(&payload);
            match user_id {
                None if looks_like_dave => return None,
                None => {}
                Some(user) if self.dave.can_decrypt(user) => {
                    match self.dave.decrypt(user, DaveMedia::Audio, &payload) {
                        Ok(plain) => {
                            payload = plain;
                            self.note_decrypted(user_id);
                        }
                        Err(_) => {
                            self.stat(st, "audio_decrypt_failures", 1);
                            self.note_undecryptable(user_id);
                            return None;
                        }
                    }
                }
                Some(_) if looks_like_dave => {
                    self.note_undecryptable(user_id);
                    return None;
                }
                Some(_) => {}
            }
        }
        let (audio_level, voice_activity) = audio_level(&d.rtp_extensions);
        let rtcp_time = self.rtcp_time(st, ssrc, d.timestamp, &d.rtp_extensions);
        Some(Packet {
            user_id,
            media_type: MediaType::Audio,
            codec: PacketCodec::Opus,
            ssrc,
            timestamp: d.timestamp,
            sequence: d.sequence,
            payload,
            payload_type: d.payload_type,
            marker: d.marker,
            speaking_flags: Some(if silence {
                speaking::NONE
            } else {
                speaking_flags(&d.rtp_extensions)
            }),
            audio_level,
            audio_voice_activity: voice_activity,
            rtp_extensions: d.rtp_extensions,
            rtp_packets: vec![rtp_packet],
            received_at,
            rtcp_time,
        })
    }

    // ----- video

    /// `_decode_video_packets`.
    fn decode_video_packets(&self, st: &mut RxState, received: Received, out: &mut Vec<Packet>) {
        let ready = if self.cfg.rtx_enabled {
            let key = (received.ssrc, received.decoded.payload_type);
            let now = received.received_at;
            let (max_packets, max_delay) =
                (self.cfg.reorder_max_packets, self.cfg.reorder_max_delay);
            let buffer = st
                .reorder
                .entry(key)
                .or_insert_with(|| ReorderBuffer::new(max_packets, max_delay));
            buffer.push(received, now)
        } else {
            vec![received]
        };
        for packet in ready {
            self.stat(st, "video_packets_reorder_ready", 1);
            if let Some(frame) = self.decode_video_frame(st, packet) {
                self.stat(st, "video_frames_completed", 1);
                out.push(frame);
            }
        }
    }

    /// `_decode_video_frame`.
    fn decode_video_frame(&self, st: &mut RxState, p: Received) -> Option<Packet> {
        let key = (p.ssrc, p.decoded.payload_type);
        let depacketizer = st
            .depacketizers
            .entry(key)
            .or_insert_with(|| depacketizer_for(p.codec));
        let pushed = depacketizer.push_packet(
            &p.decoded.payload,
            p.decoded.marker,
            p.decoded.sequence,
            p.decoded.timestamp,
        );
        st.frame_rtp
            .entry(key)
            .or_default()
            .push(p.rtp_packet.clone());
        let frame = match pushed {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                if p.decoded.marker {
                    self.stat(st, "video_frames_dropped_at_marker", 1);
                    st.frame_rtp.insert(key, Vec::new());
                } else {
                    self.stat(st, "video_frame_packets_buffered", 1);
                }
                return None;
            }
            Err(_) => {
                self.stat(st, "video_depacketize_errors", 1);
                st.frame_rtp.insert(key, Vec::new());
                return None;
            }
        };
        let rtp_packets = std::mem::take(st.frame_rtp.entry(key).or_default());

        let looks_like_dave = looks_like_dave_protocol_frame(&frame);
        if looks_like_dave && !self.can_attempt_dave(p.user_id) {
            self.note_undecryptable(p.user_id);
            self.stash_dave_frame(st, &p, frame, rtp_packets, false);
            return None;
        }
        match self.decrypt_video(p.user_id, frame.clone(), true) {
            Ok(payload) => {
                self.note_decrypted(p.user_id);
                Some(self.video_packet(
                    st,
                    &p.decoded,
                    p.codec,
                    p.ssrc,
                    p.user_id,
                    payload,
                    rtp_packets,
                    p.received_at,
                ))
            }
            Err(e) if looks_like_dave && e.is_retryable() => {
                self.note_undecryptable(p.user_id);
                self.stash_dave_frame(st, &p, frame, rtp_packets, true);
                None
            }
            Err(_) => {
                self.stat(st, "video_frame_decrypt_failures", 1);
                self.note_undecryptable(p.user_id);
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn video_packet(
        &self,
        st: &RxState,
        d: &Decoded,
        codec: VoiceCodec,
        ssrc: u32,
        user_id: Option<u64>,
        payload: Vec<u8>,
        rtp_packets: Vec<RtpPacketInfo>,
        received_at: Instant,
    ) -> Packet {
        Packet {
            user_id,
            media_type: MediaType::Video,
            codec: PacketCodec::Video(codec),
            ssrc,
            timestamp: d.timestamp,
            sequence: d.sequence,
            payload,
            payload_type: d.payload_type,
            marker: true,
            rtp_extensions: d.rtp_extensions.clone(),
            rtp_packets,
            received_at,
            rtcp_time: self.rtcp_time(st, ssrc, d.timestamp, &d.rtp_extensions),
            speaking_flags: None,
            audio_level: None,
            audio_voice_activity: None,
        }
    }

    // ----- DAVE

    /// `_can_attempt_video_dave_decrypt`.
    fn can_attempt_dave(&self, user_id: Option<u64>) -> bool {
        user_id.is_some_and(|user| self.dave.has_session() && self.dave.can_decrypt(user))
    }

    /// `_decrypt_video_frame`: the payload untouched when no key can exist for the sender. A failure
    /// is counted in `Dave::decrypt_failures` only when `count`.
    fn decrypt_video(
        &self,
        user_id: Option<u64>,
        payload: Vec<u8>,
        count: bool,
    ) -> Result<Vec<u8>, DaveDecryptError> {
        match user_id {
            Some(user) if self.can_attempt_dave(Some(user)) => {
                self.dave
                    .decrypt_counting(user, DaveMedia::Video, &payload, count)
            }
            _ => Ok(payload),
        }
    }

    /// `_stash_dave_video_frame`.
    fn stash_dave_frame(
        &self,
        st: &mut RxState,
        p: &Received,
        payload: Vec<u8>,
        rtp_packets: Vec<RtpPacketInfo>,
        counted: bool,
    ) {
        let now = Instant::now();
        let mut expired = 0;
        while st.pending_dave.front().is_some_and(|f| {
            now.saturating_duration_since(f.queued_at) > DAVE_PENDING_VIDEO_MAX_AGE
        }) {
            st.pending_dave.pop_front();
            expired += 1;
        }
        if expired > 0 {
            self.stat(st, "dave_pending_expired", expired);
        }
        if st.pending_dave.len() >= DAVE_PENDING_VIDEO_MAX_FRAMES {
            st.pending_dave.pop_front();
            self.stat(st, "dave_pending_dropped", 1);
        }
        st.pending_dave.push_back(PendingDaveFrame {
            decoded: p.decoded.clone(),
            codec: p.codec,
            ssrc: p.ssrc,
            user_id: p.user_id,
            payload,
            rtp_packets,
            received_at: p.received_at,
            queued_at: now,
            counted,
        });
    }

    /// `_retry_pending_dave_video_frames`, at every batch (`now` is its arrival). A frame's decrypt
    /// failure is counted once, not at every retry; frames that age out are counted as
    /// `dave_pending_expired`.
    fn retry_pending_dave(&self, st: &mut RxState, now: Instant) -> Vec<Packet> {
        if st.pending_dave.is_empty() {
            return Vec::new();
        }
        let mut ready = Vec::new();
        let mut remaining = VecDeque::new();
        let mut expired = 0;
        while let Some(mut frame) = st.pending_dave.pop_front() {
            if now.saturating_duration_since(frame.queued_at) > DAVE_PENDING_VIDEO_MAX_AGE {
                expired += 1;
                continue;
            }
            if !self.can_attempt_dave(frame.user_id) {
                remaining.push_back(frame);
                continue;
            }
            match self.decrypt_video(frame.user_id, frame.payload.clone(), !frame.counted) {
                Ok(payload) => {
                    self.note_decrypted(frame.user_id);
                    ready.push(self.video_packet(
                        st,
                        &frame.decoded,
                        frame.codec,
                        frame.ssrc,
                        frame.user_id,
                        payload,
                        frame.rtp_packets,
                        frame.received_at,
                    ));
                }
                Err(e) if e.is_retryable() => {
                    frame.counted = true;
                    remaining.push_back(frame);
                }
                Err(_) => {}
            }
        }
        st.pending_dave = remaining;
        if expired > 0 {
            self.stat(st, "dave_pending_expired", expired);
        }
        if !ready.is_empty() {
            self.stat(st, "dave_pending_retried", ready.len() as u64);
        }
        ready
    }

    // ----- feedback

    /// `_maybe_send_startup_video_pli`: once per SSRC, for H264/H265.
    fn maybe_send_startup_pli(&self, st: &mut RxState, codec: VoiceCodec, ssrc: u32) {
        if !matches!(codec, VoiceCodec::H264 | VoiceCodec::H265) || !st.startup_plis.insert(ssrc) {
            return;
        }
        let _ = self.send_pli(ssrc);
    }

    /// `_maybe_send_video_rtcp_receiver_report`.
    fn maybe_send_receiver_report(
        &self,
        st: &mut RxState,
        ssrc: u32,
        sequence: u16,
        timestamp: u32,
        received_at: Instant,
    ) {
        let arrival = self.secs(received_at);
        let is_new = !st.report_states.contains_key(&ssrc);
        let state = st
            .report_states
            .entry(ssrc)
            .or_insert_with(|| ReceiveReportState::new(sequence));
        if is_new {
            state.note_transit(timestamp, arrival);
        } else {
            state.on_packet(sequence, timestamp, arrival);
        }
        let interval = self.cfg.receiver_report_interval.as_secs_f64();
        if interval <= 0.0 || arrival - state.last_report_at < interval {
            return;
        }
        state.last_report_at = arrival;
        let last_sr = st.sender_report_lsr.get(&ssrc).map(|(lsr, at)| {
            (
                *lsr,
                received_at.saturating_duration_since(*at).as_secs_f64(),
            )
        });
        let block = state.block(ssrc, last_sr);
        self.stat(st, "rtcp_receiver_reports_sent", 1);
        let _ = self.send_receiver_report(&[block]);
    }

    /// `_track_video_receive_sequence`.
    fn track_sequence(
        &self,
        st: &mut RxState,
        ssrc: u32,
        sequence: u16,
        repaired: bool,
        now: Instant,
    ) {
        if let Some(pending) = st.pending_nacks.get_mut(&ssrc) {
            pending.remove(&sequence);
            if pending.is_empty() {
                st.pending_nacks.remove(&ssrc);
            }
        }
        let rtx = self.cfg.rtx_enabled;
        if repaired {
            self.stat(st, "video_sequence_repaired_packets", 1);
            if rtx {
                self.retry_pending(st, ssrc, now);
            }
            return;
        }
        let Some(previous) = st.last_sequence.get(&ssrc).copied() else {
            st.last_sequence.insert(ssrc, sequence);
            if rtx {
                self.retry_pending(st, ssrc, now);
            }
            return;
        };
        let delta = rtcp::sequence_delta(sequence, previous);
        if delta == 0 || delta > 0x7FFF {
            self.stat(
                st,
                if delta == 0 {
                    "video_sequence_duplicates"
                } else {
                    "video_sequence_old_or_reordered"
                },
                1,
            );
            if rtx {
                self.retry_pending(st, ssrc, now);
            }
            return;
        }
        if sequence < previous {
            self.stat(st, "video_sequence_wraps", 1);
        }
        if delta > 1 {
            self.stat(st, "video_sequence_gaps", 1);
            if self.cfg.debug_stats {
                let max = st.stats.entry("video_sequence_gap_max").or_default();
                *max = (*max).max(u64::from(delta));
            }
        }
        st.last_sequence.insert(ssrc, sequence);
        if !rtx {
            return;
        }
        if delta == 1 {
            self.retry_pending(st, ssrc, now);
            return;
        }
        let missing: Vec<u16> = (1..delta)
            .map(|offset| previous.wrapping_add(offset))
            .collect();
        let added = self.add_pending_nacks(st, ssrc, &missing);
        if !added.is_empty() {
            self.stat(st, "video_nack_sequences_added", added.len() as u64);
            self.send_pending_nacks(st, ssrc, &added, now);
        }
        self.retry_pending(st, ssrc, now);
    }

    /// `_add_pending_video_nacks`: the sequences newly pending; on overflow the list is dropped and
    /// a keyframe requested instead.
    fn add_pending_nacks(&self, st: &mut RxState, ssrc: u32, sequences: &[u16]) -> Vec<u16> {
        let mut unique: Vec<u16> = Vec::with_capacity(sequences.len());
        for &s in sequences {
            if !unique.contains(&s) {
                unique.push(s);
            }
        }
        if unique.is_empty() {
            return Vec::new();
        }
        let pending = st.pending_nacks.entry(ssrc).or_default();
        let added: Vec<u16> = unique
            .into_iter()
            .filter(|s| !pending.contains_key(s))
            .collect();
        if added.is_empty() {
            return Vec::new();
        }
        if pending.len() + added.len() > VIDEO_NACK_MAX_OUTSTANDING {
            st.pending_nacks.remove(&ssrc);
            self.stat(st, "video_nack_overflows", 1);
            let _ = self.send_pli(ssrc);
            return Vec::new();
        }
        for s in &added {
            pending.insert(*s, PendingNack::default());
        }
        added
    }

    /// `_send_pending_video_nacks`.
    fn send_pending_nacks(&self, st: &mut RxState, ssrc: u32, sequences: &[u16], now: Instant) {
        let Some(pending) = st.pending_nacks.get(&ssrc) else {
            return;
        };
        let mut seen = HashSet::new();
        let sendable: Vec<u16> = sequences
            .iter()
            .copied()
            .filter(|s| pending.contains_key(s) && seen.insert(*s))
            .collect();
        for chunk in sendable.chunks(RTCP_NACK_MAX_SEQUENCES) {
            self.stat(st, "video_nack_packets_sent", 1);
            self.stat(st, "video_nack_sequences_sent", chunk.len() as u64);
            if let Some((header, body)) = rtcp::nack(self.cfg.own_ssrc, ssrc, chunk) {
                let _ = self.send_rtcp(&header, &body);
            }
            if let Some(pending) = st.pending_nacks.get_mut(&ssrc) {
                for s in chunk {
                    if let Some(nack) = pending.get_mut(s) {
                        nack.sent_at = Some(now);
                        nack.retries += 1;
                    }
                }
            }
        }
    }

    /// `_retry_pending_video_nacks`.
    fn retry_pending(&self, st: &mut RxState, ssrc: u32, now: Instant) {
        let Some(pending) = st.pending_nacks.get_mut(&ssrc) else {
            return;
        };
        let interval = Duration::from_secs_f64(VIDEO_NACK_RETRY_INTERVAL);
        let mut expired = Vec::new();
        let mut due = Vec::new();
        for (&sequence, nack) in pending.iter() {
            if nack.retries >= VIDEO_NACK_MAX_RETRIES {
                expired.push(sequence);
            } else if nack
                .sent_at
                .is_none_or(|at| now.saturating_duration_since(at) >= interval)
            {
                due.push(sequence);
            }
        }
        for sequence in &expired {
            pending.remove(sequence);
        }
        due.sort_unstable();
        if !expired.is_empty() {
            self.stat(st, "video_nack_sequences_expired", expired.len() as u64);
        }
        if !due.is_empty() {
            self.send_pending_nacks(st, ssrc, &due, now);
        }
        if st.pending_nacks.get(&ssrc).is_some_and(HashMap::is_empty) {
            st.pending_nacks.remove(&ssrc);
        }
    }

    /// Timer entry (every [`VIDEO_NACK_RETRY_INTERVAL`] seconds while RTX is on): resend overdue NACKs.
    pub fn retry_pending_nacks(&self, now: Instant) {
        let mut st = self.state.lock();
        let ssrcs: Vec<u32> = st.pending_nacks.keys().copied().collect();
        for ssrc in ssrcs {
            self.retry_pending(&mut st, ssrc, now);
        }
    }

    // ----- RTCP in/out

    /// `_handle_rtcp_packet`: sender reports (for [`Packet::rtcp_time`]) and receiver reports.
    /// NACK/PLI aimed at our own send pipeline are the publish side's business (V4).
    pub fn handle_rtcp(&self, datagram: &[u8]) {
        let Ok(packets) = self
            .crypto_rx
            .decrypt_rtcp_packets(datagram, RTCP_PROTECTED_HEADER_LEN)
        else {
            let mut st = self.state.lock();
            self.stat(&mut st, "rtcp_decrypt_failures", 1);
            return;
        };
        let now = Instant::now();
        let mut feedback = Vec::new();
        let mut st = self.state.lock();
        for packet in packets {
            match (packet.packet_type, packet.count) {
                (RTCP_SENDER_REPORT, _) => {
                    let Some(report) = rtcp::parse_sender_report(&packet.payload) else {
                        continue;
                    };
                    let clock = if self.ssrcs.is_video(packet.sender_ssrc) {
                        VIDEO_CLOCK_RATE
                    } else {
                        AUDIO_CLOCK_RATE
                    };
                    st.sender_reports.insert(
                        packet.sender_ssrc,
                        (report.ntp_time, report.rtp_timestamp, clock),
                    );
                    st.sender_report_lsr
                        .insert(packet.sender_ssrc, (report.compact_ntp, now));
                }
                (RTCP_RECEIVER_REPORT, count) => {
                    let ttl = self.cfg.receiver_report_ttl;
                    st.receiver_reports
                        .retain(|_, (_, at)| now.saturating_duration_since(*at) <= ttl);
                    for block in
                        rtcp::parse_receiver_report(packet.sender_ssrc, count, &packet.payload)
                    {
                        st.receiver_reports
                            .insert((block.sender_ssrc, block.source_ssrc), (block, now));
                    }
                }
                (RTCP_RTP_FEEDBACK, RTCP_GENERIC_NACK) => feedback.push(Feedback::Nack {
                    payload: packet.payload,
                }),
                (RTCP_PAYLOAD_FEEDBACK, RTCP_PICTURE_LOSS_INDICATION) => {
                    if let Some(media_ssrc) = be_u32(&packet.payload, 0) {
                        feedback.push(Feedback::KeyframeRequest {
                            media_ssrc,
                            fir: false,
                        });
                    }
                }
                // FIR: the media SSRC field is 0; the target is in the first FCI entry.
                (RTCP_PAYLOAD_FEEDBACK, RTCP_FIR) => {
                    if let Some(media_ssrc) = be_u32(&packet.payload, 4) {
                        feedback.push(Feedback::KeyframeRequest {
                            media_ssrc,
                            fir: true,
                        });
                    }
                }
                _ => {}
            }
        }
        drop(st);
        if feedback.is_empty() {
            return;
        }
        let handler = self.feedback.lock().clone();
        if let Some(handler) = handler {
            for item in feedback {
                handler(item);
            }
        }
    }

    /// Receiver reports other participants sent about us, newest per (sender, source), not older
    /// than the TTL (`rtcp_receiver_reports`).
    pub fn receiver_reports(&self) -> Vec<ReportBlock> {
        let ttl = self.cfg.receiver_report_ttl;
        let now = Instant::now();
        self.state
            .lock()
            .receiver_reports
            .values()
            .filter(|(_, at)| now.saturating_duration_since(*at) <= ttl)
            .map(|(block, _)| *block)
            .collect()
    }

    /// `send_video_pli`.
    pub fn send_pli(&self, media_ssrc: u32) -> Result<(), SendError> {
        let (header, body) = rtcp::pli(self.cfg.own_ssrc, media_ssrc);
        self.send_rtcp(&header, &body)
    }

    /// `_send_rtcp_receiver_report` with `blocks` (the first one sent at connect time has none).
    pub fn send_receiver_report(&self, blocks: &[[u8; 24]]) -> Result<(), SendError> {
        let (header, body) = rtcp::receiver_report(self.cfg.own_ssrc, blocks);
        self.send_rtcp(&header, &body)
    }

    /// Encrypts `body` under `header` and sends it (how TWCC feedback leaves).
    pub fn send_rtcp(&self, header: &[u8], body: &[u8]) -> Result<(), SendError> {
        let packet = self.crypto_tx.encrypt_rtcp(header, body)?;
        self.sink.send_packet(&packet)?;
        Ok(())
    }

    // ----- housekeeping

    /// `_reset_receive_transport_state`: forget everything tied to the old transport.
    pub fn reset(&self) {
        let mut st = self.state.lock();
        Self::retire_reorder_stats(&mut st);
        st.depacketizers.clear();
        st.last_sequence.clear();
        st.pending_nacks.clear();
        st.reorder.clear();
        st.startup_plis.clear();
        st.frame_rtp.clear();
        st.pending_dave.clear();
    }

    /// Drops the depacketizer, reorder buffer and half-built frame of every payload type of `ssrc`
    /// except `keep`. After a codec switch the old codec's sequence numbers run on, so keeping its
    /// reorder buffer would make it wait out (then skip) every number sent meanwhile.
    pub fn forget_other_payload_types(&self, ssrc: u32, keep: u8) {
        let mut st = self.state.lock();
        st.depacketizers.retain(|k, _| k.0 != ssrc || k.1 == keep);
        st.frame_rtp.retain(|k, _| k.0 != ssrc || k.1 == keep);
        st.reorder.retain(|k, _| k.0 != ssrc || k.1 == keep);
    }

    fn retire_reorder_stats(st: &mut RxState) {
        let totals = reorder_totals(&st.reorder);
        merge_stats(&mut st.retired_reorder, totals);
    }

    /// `video_receive_stats`: empty unless `debug_stats` was enabled.
    pub fn stats(&self) -> BTreeMap<&'static str, u64> {
        if !self.cfg.debug_stats {
            return BTreeMap::new();
        }
        let st = self.state.lock();
        let mut out = st.stats.clone();
        merge_stats(&mut out, st.retired_reorder.clone());
        merge_stats(&mut out, reorder_totals(&st.reorder));
        out
    }

    // ----- clocks

    /// `_rtcp_time_for_packet`.
    fn rtcp_time(
        &self,
        st: &RxState,
        ssrc: u32,
        timestamp: u32,
        extensions: &[RtpExtension],
    ) -> Option<f64> {
        let Some(&(ntp_time, report_timestamp, clock_rate)) = st.sender_reports.get(&ssrc) else {
            return absolute_send_time(extensions);
        };
        match rtcp::rtp_timestamp_delta(timestamp, report_timestamp) {
            Some(delta) => Some(ntp_time + f64::from(delta) / f64::from(clock_rate)),
            None => absolute_send_time(extensions),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// helpers

fn reorder_totals(reorder: &HashMap<(u32, u8), ReorderBuffer>) -> BTreeMap<&'static str, u64> {
    let mut total: BTreeMap<&'static str, u64> = BTreeMap::new();
    for buffer in reorder.values() {
        for (key, value) in [
            ("reorder_packets_pushed", buffer.pushed),
            ("reorder_duplicate_packets", buffer.duplicates),
            ("reorder_old_packets", buffer.old),
            ("reorder_waits", buffer.waits),
            ("reorder_flushes", buffer.flushes),
            ("reorder_skipped_packets", buffer.skipped),
            ("reorder_ready_packets", buffer.ready),
        ] {
            *total.entry(key).or_default() += value;
        }
        let max = total.entry("reorder_max_buffered").or_default();
        *max = (*max).max(buffer.max_buffered);
    }
    total
}

/// Adds `src` into `dst`; `reorder_max_buffered` is a maximum, everything else a sum.
fn merge_stats(dst: &mut BTreeMap<&'static str, u64>, src: BTreeMap<&'static str, u64>) {
    for (key, value) in src {
        let slot = dst.entry(key).or_default();
        if key == "reorder_max_buffered" {
            *slot = (*slot).max(value);
        } else {
            *slot += value;
        }
    }
}

/// `_absolute_send_time_from_extensions`.
fn absolute_send_time(extensions: &[RtpExtension]) -> Option<f64> {
    let ext = extensions
        .iter()
        .find(|e| e.id == RTP_EXT_ABSOLUTE_SEND_TIME && e.data.len() == 3)?;
    let value = u32::from_be_bytes([0, ext.data[0], ext.data[1], ext.data[2]]);
    let mod64 = f64::from(value) / f64::from(1u32 << 18);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs_f64();
    let mut send_time = now - (now % 64.0) + mod64;
    if send_time - now > 32.0 {
        send_time -= 64.0;
    } else if now - send_time > 32.0 {
        send_time += 64.0;
    }
    Some(send_time)
}

/// `_audio_speaking_flags_from_extensions`.
fn speaking_flags(extensions: &[RtpExtension]) -> u32 {
    for ext in extensions {
        if ext.id == RTP_EXT_DISCORD_SPEAKING && ext.data.len() == 1 {
            let value = u32::from(ext.data[0]);
            if value == 0 {
                return speaking::VOICE;
            }
            let mut flags = (value >> 1) & (speaking::VOICE | speaking::SOUNDSHARE);
            if value & 1 != 0 {
                flags |= speaking::VOICE | speaking::PRIORITY;
            }
            return flags;
        }
    }
    speaking::VOICE
}

/// `_audio_level_from_extensions`.
fn audio_level(extensions: &[RtpExtension]) -> (Option<u8>, Option<bool>) {
    for ext in extensions {
        if ext.id == RTP_EXT_AUDIO_LEVEL && ext.data.len() == 1 {
            let value = ext.data[0];
            return (Some(value & 0x7F), Some(value & 0x80 != 0));
        }
    }
    (None, None)
}

/// `_is_unencrypted_opus_silence_payload`: the 3-byte Opus silence frame, optionally followed by
/// RTP-style padding whose bytes all equal the padding length.
pub fn is_unencrypted_opus_silence(payload: &[u8]) -> bool {
    if payload == OPUS_SILENCE {
        return true;
    }
    if !payload.starts_with(&OPUS_SILENCE) {
        return false;
    }
    let padding = &payload[OPUS_SILENCE.len()..];
    !padding.is_empty()
        && padding.len() <= 255
        && padding.iter().all(|&b| usize::from(b) == padding.len())
}

/// `_read_leb128_uint64`: the value and the offset after it.
fn read_leb128(buffer: &[u8], mut offset: usize, end: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut fill_bits = 0u32;
    while offset != end && fill_bits < 64 - 7 {
        let byte = buffer[offset];
        value |= u64::from(byte & 0x7F) << fill_bits;
        offset += 1;
        fill_bits += 7;
        if byte & 0x80 == 0 {
            return Some((value, offset));
        }
    }
    if offset != end && buffer[offset] <= 1 {
        value |= u64::from(buffer[offset]) << fill_bits;
        return Some((value, offset + 1));
    }
    None
}

/// `_valid_unencrypted_ranges`.
fn valid_unencrypted_ranges(
    buffer: &[u8],
    mut offset: usize,
    end: usize,
    frame_size: usize,
) -> bool {
    let mut previous_end = 0u64;
    while offset != end {
        let Some((range_offset, next)) = read_leb128(buffer, offset, end) else {
            return false;
        };
        let Some((range_size, next)) = read_leb128(buffer, next, end) else {
            return false;
        };
        offset = next;
        let Some(range_end) = range_offset.checked_add(range_size) else {
            return false;
        };
        if range_offset < previous_end || range_end > frame_size as u64 {
            return false;
        }
        previous_end = range_end;
    }
    true
}

/// `_looks_like_dave_protocol_frame`: does `payload` end in a well-formed DAVE supplemental block
/// (tag, nonce, unencrypted ranges, size byte, `FA FA` marker)?
pub fn looks_like_dave_protocol_frame(payload: &[u8]) -> bool {
    if payload.len() < DAVE_MIN_SIZE || !payload.ends_with(&DAVE_MARKER) {
        return false;
    }
    let supplemental_size =
        usize::from(payload[payload.len() - (DAVE_SIZE_SIZE + DAVE_MARKER.len())]);
    if supplemental_size < DAVE_MIN_SIZE || supplemental_size > payload.len() {
        return false;
    }
    let supplemental = &payload[payload.len() - supplemental_size..];
    let ranges_end = supplemental_size - DAVE_SIZE_SIZE - DAVE_MARKER.len();
    if usize::from(supplemental[ranges_end]) != supplemental_size
        || supplemental[ranges_end + DAVE_SIZE_SIZE..] != DAVE_MARKER
    {
        return false;
    }
    let Some((_nonce, offset)) = read_leb128(supplemental, DAVE_TAG_SIZE, ranges_end) else {
        return false;
    };
    valid_unencrypted_ranges(supplemental, offset, ranges_end, payload.len())
}

fn be_u32(data: &[u8], at: usize) -> Option<u32> {
    let bytes = data.get(at..at + 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn received(sequence: u16) -> Received {
        Received {
            decoded: Decoded {
                payload_type: 105,
                marker: false,
                sequence,
                timestamp: 0,
                ssrc: 1,
                rtp_extensions: Vec::new(),
                payload: Vec::new(),
            },
            codec: VoiceCodec::H264,
            ssrc: 1,
            user_id: None,
            rtp_packet: RtpPacketInfo {
                sequence,
                timestamp: 0,
                ssrc: 1,
                payload_type: 105,
                marker: false,
                rtx: false,
                payload_len: 0,
            },
            received_at: Instant::now(),
        }
    }

    fn seqs(packets: &[Received]) -> Vec<u16> {
        packets.iter().map(|p| p.decoded.sequence).collect()
    }

    #[test]
    fn reorder_releases_in_order_and_holds_a_gap() {
        let t0 = Instant::now();
        let mut buffer = ReorderBuffer::new(8, Duration::from_millis(500));
        assert_eq!(seqs(&buffer.push(received(10), t0)), [10]);
        assert!(buffer.push(received(12), t0).is_empty());
        assert!(buffer.push(received(13), t0).is_empty());
        assert_eq!(seqs(&buffer.push(received(11), t0)), [11, 12, 13]);
        // duplicate and stale packets are dropped
        assert!(buffer.push(received(12), t0).is_empty());
        assert_eq!((buffer.old, buffer.waits), (1, 2));
    }

    #[test]
    fn reorder_skips_a_gap_after_the_delay() {
        let t0 = Instant::now();
        let mut buffer = ReorderBuffer::new(8, Duration::from_millis(500));
        buffer.push(received(1), t0);
        assert!(buffer.push(received(3), t0).is_empty());
        let late = t0 + Duration::from_millis(501);
        assert_eq!(seqs(&buffer.push(received(4), late)), [3, 4]);
        assert_eq!((buffer.skipped, buffer.flushes), (1, 1));
    }

    #[test]
    fn reorder_skips_when_the_buffer_is_full() {
        let t0 = Instant::now();
        let mut buffer = ReorderBuffer::new(2, Duration::from_secs(60));
        buffer.push(received(1), t0);
        assert!(buffer.push(received(3), t0).is_empty());
        assert!(buffer.push(received(4), t0).is_empty());
        assert_eq!(seqs(&buffer.push(received(5), t0)), [3, 4, 5]);
    }

    #[test]
    fn reorder_survives_sequence_wraparound() {
        let t0 = Instant::now();
        let mut buffer = ReorderBuffer::new(8, Duration::from_millis(500));
        assert_eq!(seqs(&buffer.push(received(65_534), t0)), [65_534]);
        assert!(buffer.push(received(0), t0).is_empty());
        assert_eq!(seqs(&buffer.push(received(65_535), t0)), [65_535, 0]);
        assert_eq!(seqs(&buffer.push(received(1), t0)), [1]);
    }

    #[test]
    fn silence_detection_accepts_rtp_padding() {
        assert!(is_unencrypted_opus_silence(&[0xF8, 0xFF, 0xFE]));
        assert!(is_unencrypted_opus_silence(&[0xF8, 0xFF, 0xFE, 2, 2]));
        assert!(!is_unencrypted_opus_silence(&[0xF8, 0xFF, 0xFE, 2, 3]));
        assert!(!is_unencrypted_opus_silence(&[0xF8, 0xFF, 0xFE, 0]));
        assert!(!is_unencrypted_opus_silence(&[1, 2, 3]));
    }

    /// tag(8) + nonce leb128(1) + ranges + size + marker.
    fn dave_frame(body: &[u8], ranges: &[u8]) -> Vec<u8> {
        let size = DAVE_TAG_SIZE + 1 + ranges.len() + DAVE_SIZE_SIZE + DAVE_MARKER.len();
        let mut out = body.to_vec();
        out.extend_from_slice(&[0xAA; DAVE_TAG_SIZE]);
        out.push(0x05); // nonce
        out.extend_from_slice(ranges);
        out.push(size as u8);
        out.extend_from_slice(&DAVE_MARKER);
        out
    }

    #[test]
    fn dave_frame_detection() {
        let frame = dave_frame(&[1; 20], &[]);
        assert!(looks_like_dave_protocol_frame(&frame));
        // one unencrypted range (offset 2, size 3)
        assert!(looks_like_dave_protocol_frame(&dave_frame(
            &[1; 20],
            &[2, 3]
        )));
        // range past the end of the frame
        assert!(!looks_like_dave_protocol_frame(&dave_frame(
            &[1; 4],
            &[200, 1, 50, 1]
        )));
        // not a DAVE frame
        assert!(!looks_like_dave_protocol_frame(&[0u8; 40]));
        assert!(!looks_like_dave_protocol_frame(&[]));
        // size byte that does not point back at itself
        let mut bad = frame.clone();
        let n = bad.len();
        bad[n - 3] = DAVE_MIN_SIZE as u8; // consistent length, but the byte it points at is not
        assert!(!looks_like_dave_protocol_frame(&bad));
    }

    #[test]
    fn leb128_reads_multibyte_and_rejects_truncation() {
        assert_eq!(read_leb128(&[0xE5, 0x8E, 0x26], 0, 3), Some((624_485, 3)));
        assert_eq!(read_leb128(&[0x80], 0, 1), None);
        assert_eq!(read_leb128(&[], 0, 0), None);
    }

    #[test]
    fn speaking_and_level_extensions() {
        let ext = |id, byte| {
            vec![RtpExtension {
                id,
                data: vec![byte],
            }]
        };
        assert_eq!(speaking_flags(&[]), speaking::VOICE);
        assert_eq!(
            speaking_flags(&ext(RTP_EXT_DISCORD_SPEAKING, 0)),
            speaking::VOICE
        );
        assert_eq!(
            speaking_flags(&ext(RTP_EXT_DISCORD_SPEAKING, 0b101)),
            speaking::VOICE | speaking::PRIORITY | speaking::SOUNDSHARE
        );
        assert_eq!(
            audio_level(&ext(RTP_EXT_AUDIO_LEVEL, 0x85)),
            (Some(5), Some(true))
        );
        assert_eq!(audio_level(&[]), (None, None));
    }

    #[test]
    fn ssrc_map_replaces_a_users_previous_video_ssrcs() {
        let map = SsrcMap::new();
        map.set_speaking(10, 7);
        map.update_video_state(
            7,
            VideoState {
                audio_ssrc: Some(10),
                video_ssrc: Some(20),
                rtx_ssrc: Some(21),
                streams: Vec::new(),
            },
        );
        assert_eq!(map.user_for(20), Some(7));
        assert_eq!(map.user_for(21), Some(7));
        assert_eq!(map.media_for_rtx(21), Some(20));
        assert!(map.is_video(20) && map.is_video(21) && !map.is_video(10));
        map.update_video_state(
            7,
            VideoState {
                audio_ssrc: Some(10),
                video_ssrc: Some(30),
                rtx_ssrc: Some(31),
                streams: Vec::new(),
            },
        );
        assert_eq!(map.user_for(20), None);
        assert_eq!(map.media_for_rtx(21), None);
        assert_eq!(map.user_for(10), Some(7));
        assert_eq!(map.media_for_rtx(31), Some(30));
        map.clear();
        assert_eq!(map.user_for(30), None);
    }
}
