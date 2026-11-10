// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/python/video_send.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       pyo3 layer removed. `PyVideoSendPipeline` became `VideoSendPipeline`
//                (plain struct, same RTX cache / NACK / extension logic); the codec is the
//                typed `relay_media::codec::VoiceCodec` instead of a string; frames go out
//                through a `PacketSink` instead of a raw socket handle.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use relay_media::codec::VoiceCodec;

use crate::rtp::{MediaPacket, PacketError, RtpPacket};
use crate::transport::net::UdpTransport;
use crate::transport::packets::{
    SendError, SharedTransportCrypto, build_video_extension_payload,
    header_with_one_byte_extensions, wait_until,
};

use super::{
    AV1Packetizer, H264Packetizer, H265Packetizer, VP8Packetizer, VP9Packetizer, VideoPacketizer,
};

const RTX_CACHE_MAX_PACKETS: usize = 16_384;

enum PipelinePacketizer {
    AV1(AV1Packetizer),
    H264(H264Packetizer),
    H265(H265Packetizer),
    VP8(VP8Packetizer),
    VP9(VP9Packetizer),
}

impl PipelinePacketizer {
    fn new(codec: VoiceCodec, ssrc: u32, payload_type: u8) -> Self {
        match codec {
            VoiceCodec::AV1 => Self::AV1(AV1Packetizer::new(ssrc, payload_type)),
            VoiceCodec::H264 => Self::H264(H264Packetizer::new(ssrc, payload_type)),
            VoiceCodec::H265 => Self::H265(H265Packetizer::new(ssrc, payload_type)),
            VoiceCodec::VP8 => Self::VP8(VP8Packetizer::new(ssrc, payload_type)),
            VoiceCodec::VP9 => Self::VP9(VP9Packetizer::new(ssrc, payload_type)),
        }
    }

    fn packetize(
        &self,
        frame: &[u8],
        metadata_frame: Option<&[u8]>,
        frame_time_ms: f64,
    ) -> Result<Vec<MediaPacket>, PacketError> {
        match self {
            Self::AV1(packetizer) => packetizer.packetize(frame, frame_time_ms),
            Self::H264(packetizer) => packetizer.packetize(frame, frame_time_ms),
            Self::H265(packetizer) => packetizer.packetize(frame, frame_time_ms),
            Self::VP8(packetizer) => packetizer.packetize(frame, frame_time_ms),
            Self::VP9(packetizer) => packetizer.packetize_with_metadata_frame(
                frame,
                metadata_frame.unwrap_or(frame),
                frame_time_ms,
            ),
        }
    }

    fn timestamp(&self) -> u32 {
        match self {
            Self::AV1(packetizer) => packetizer.timestamp(),
            Self::H264(packetizer) => packetizer.timestamp(),
            Self::H265(packetizer) => packetizer.timestamp(),
            Self::VP8(packetizer) => packetizer.timestamp(),
            Self::VP9(packetizer) => packetizer.timestamp(),
        }
    }
}

struct SentVideoPacket {
    payload: Vec<u8>,
    marker: bool,
    sequence: u16,
    timestamp: u32,
}

struct PipelineStream {
    codec: VoiceCodec,
    payload_type: u8,
    rid: String,
    rtx_payload_type: Option<u8>,
    rtx_ssrc: u32,
    rtx_sequence: u16,
    packetizer: PipelinePacketizer,
    rtx_cache: HashMap<u16, SentVideoPacket>,
    rtx_order: VecDeque<u16>,
}

impl PipelineStream {
    fn new(
        codec: VoiceCodec,
        media_ssrc: u32,
        payload_type: u8,
        rtx_payload_type: Option<u8>,
        rtx_ssrc: u32,
        rid: &str,
    ) -> Self {
        Self {
            codec,
            payload_type,
            rid: rid.to_owned(),
            rtx_payload_type,
            rtx_ssrc,
            rtx_sequence: 0,
            packetizer: PipelinePacketizer::new(codec, media_ssrc, payload_type),
            rtx_cache: HashMap::new(),
            rtx_order: VecDeque::new(),
        }
    }

    fn update(&mut self, rtx_payload_type: Option<u8>, rtx_ssrc: u32, rid: &str) {
        self.rtx_payload_type = rtx_payload_type;
        self.rtx_ssrc = rtx_ssrc;
        self.rid.clear();
        self.rid.push_str(rid);
        if self.rtx_payload_type.is_none() || self.rtx_ssrc == 0 {
            self.rtx_cache.clear();
            self.rtx_order.clear();
            self.rtx_sequence = 0;
        }
    }

    fn rtx_active(&self) -> bool {
        self.rtx_payload_type.is_some() && self.rtx_ssrc != 0
    }

    fn cache_media_packet(
        &mut self,
        payload: Vec<u8>,
        marker: bool,
        sequence: u16,
        timestamp: u32,
    ) {
        if !self.rtx_active() {
            return;
        }

        self.rtx_cache.insert(
            sequence,
            SentVideoPacket {
                payload,
                marker,
                sequence,
                timestamp,
            },
        );
        self.rtx_order.push_back(sequence);
        while self.rtx_order.len() > RTX_CACHE_MAX_PACKETS {
            if let Some(expired) = self.rtx_order.pop_front() {
                self.rtx_cache.remove(&expired);
            }
        }
    }
}

/// Result of [`VideoSendPipeline::send_video_rtx_for_nack`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackResendSummary {
    pub resent: usize,
    pub media_ssrc: u32,
    pub rtx_ssrc: u32,
    pub last_rtx_sequence: u16,
    pub transport_sequence: u16,
}

struct PreparedRTPPacket {
    header: Vec<u8>,
    payload: Vec<u8>,
}

struct PreparedVideoSend {
    packets: Vec<PreparedRTPPacket>,
    transport_sequence: u16,
    rtp_timestamp: u32,
    last_sequence: Option<u16>,
}

struct PreparedRTXSend {
    packet: Option<PreparedRTPPacket>,
    rtx_ssrc: u32,
    rtx_sequence: u16,
    transport_sequence: u16,
}

struct PreparedNackResend {
    packets: Vec<PreparedRTPPacket>,
    summary: NackResendSummary,
}

/// Where encrypted datagrams go. `UdpTransport` is the production sink; tests
/// collect packets instead.
pub trait PacketSink {
    fn send_packet(&self, packet: &[u8]) -> io::Result<()>;
}

impl PacketSink for UdpTransport {
    fn send_packet(&self, packet: &[u8]) -> io::Result<()> {
        UdpTransport::send_packet(self, packet)
    }
}

fn validate_payload_type(payload_type: u16, name: &str) -> Result<u8, SendError> {
    if payload_type > 0x7F {
        return Err(SendError::Value(format!("{name} must be in range 0..=127")));
    }
    Ok(payload_type as u8)
}

fn packet_error(error: PacketError) -> SendError {
    SendError::Value(error.to_string())
}

/// Encrypts and sends `packets`, pacing them in bursts of `burst_size` every
/// `burst_interval` seconds when both are positive. Returns `(sent, octets)`.
/// Blocks the calling thread while pacing.
fn send_prepared_packets(
    crypto: &SharedTransportCrypto,
    packets: Vec<PreparedRTPPacket>,
    sink: &dyn PacketSink,
    burst_size: usize,
    burst_interval: f64,
) -> Result<(usize, usize), SendError> {
    let should_burst = burst_size > 0 && burst_interval > 0.0;
    let burst_started_at = Instant::now();
    let burst_interval = if should_burst {
        Some(Duration::from_secs_f64(burst_interval))
    } else {
        None
    };
    let mut sent = 0usize;
    let mut octets = 0usize;

    for packet in packets {
        let packet = crypto.encrypt_rtp(&packet.header, &packet.payload)?;

        if let Some(duration) = burst_interval
            && sent > 0
            && sent.is_multiple_of(burst_size)
        {
            let target_burst = sent / burst_size;
            wait_until(burst_started_at + duration.mul_f64(target_burst as f64));
        }

        sink.send_packet(packet.as_slice())?;
        sent += 1;
        octets += packet.len();
    }

    Ok((sent, octets))
}

/// Result of [`VideoSendPipeline::send_video_frame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSendSummary {
    pub sent: usize,
    pub octets: usize,
    pub transport_sequence: u16,
    pub rtp_timestamp: u32,
    pub last_sequence: Option<u16>,
}

/// Result of [`VideoSendPipeline::send_video_rtx_packet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtxSendSummary {
    pub sent: bool,
    pub rtx_ssrc: u32,
    pub rtx_sequence: u16,
    pub transport_sequence: u16,
}

/// Per-SSRC packetizers, RTX history and the shared transport-wide sequence
/// counter. Calls are serialised by an internal send lock.
pub struct VideoSendPipeline {
    inner: Mutex<VideoSendPipelineState>,
    send_lock: Mutex<()>,
}

struct VideoSendPipelineState {
    streams: HashMap<u32, PipelineStream>,
    transport_sequence: u16,
}

impl VideoSendPipelineState {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            transport_sequence: super::random_u16(),
        }
    }
}

fn lock_state(
    inner: &Mutex<VideoSendPipelineState>,
) -> Result<MutexGuard<'_, VideoSendPipelineState>, SendError> {
    inner
        .lock()
        .map_err(|_| SendError::Value("Video send pipeline lock poisoned".to_owned()))
}

fn lock_send(inner: &Mutex<()>) -> Result<MutexGuard<'_, ()>, SendError> {
    inner
        .lock()
        .map_err(|_| SendError::Value("Video send lock poisoned".to_owned()))
}

impl Default for VideoSendPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl VideoSendPipeline {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VideoSendPipelineState::new()),
            send_lock: Mutex::new(()),
        }
    }

    /// Adds a stream, or updates RTX/RID of an existing one with the same
    /// codec and payload type (keeping its sequence numbers and RTX history).
    pub fn configure_stream(
        &self,
        codec: VoiceCodec,
        media_ssrc: u32,
        payload_type: u16,
        rtx_payload_type: Option<u16>,
        rtx_ssrc: u32,
        rid: &str,
    ) -> Result<(), SendError> {
        let payload_type = validate_payload_type(payload_type, "payload_type")?;
        let rtx_payload_type = rtx_payload_type
            .map(|payload_type| validate_payload_type(payload_type, "rtx_payload_type"))
            .transpose()?;

        let mut state = lock_state(&self.inner)?;
        if let Some(stream) = state.streams.get_mut(&media_ssrc)
            && stream.codec == codec
            && stream.payload_type == payload_type
        {
            stream.update(rtx_payload_type, rtx_ssrc, rid);
            return Ok(());
        }

        state.streams.insert(
            media_ssrc,
            PipelineStream::new(
                codec,
                media_ssrc,
                payload_type,
                rtx_payload_type,
                rtx_ssrc,
                rid,
            ),
        );
        Ok(())
    }

    pub fn retain_streams(&self, media_ssrcs: &[u32]) -> Result<(), SendError> {
        let media_ssrcs: HashSet<u32> = media_ssrcs.iter().copied().collect();
        lock_state(&self.inner)?
            .streams
            .retain(|media_ssrc, _stream| media_ssrcs.contains(media_ssrc));
        Ok(())
    }

    pub fn clear(&self) -> Result<(), SendError> {
        let mut state = lock_state(&self.inner)?;
        state.streams.clear();
        state.transport_sequence = super::random_u16();
        Ok(())
    }

    pub fn has_stream(&self, media_ssrc: u32) -> Result<bool, SendError> {
        Ok(lock_state(&self.inner)?.streams.contains_key(&media_ssrc))
    }

    pub fn current_timestamp(&self, media_ssrc: u32) -> Result<u32, SendError> {
        lock_state(&self.inner)?
            .streams
            .get(&media_ssrc)
            .map(|stream| stream.packetizer.timestamp())
            .ok_or_else(|| SendError::Value(format!("Unknown video stream SSRC: {media_ssrc}")))
    }

    /// Packetizes `frame` (for VP9, `metadata_frame` supplies the layer
    /// metadata, defaulting to `frame` itself), encrypts and sends it.
    #[allow(clippy::too_many_arguments)]
    pub fn send_video_frame(
        &self,
        crypto: &SharedTransportCrypto,
        sink: &dyn PacketSink,
        media_ssrc: u32,
        frame: &[u8],
        metadata_frame: Option<&[u8]>,
        frame_time_ms: f64,
        media_stream_type: &str,
        burst_size: usize,
        burst_interval: f64,
    ) -> Result<VideoSendSummary, SendError> {
        let _send_guard = lock_send(&self.send_lock)?;
        let prepared = {
            let mut state = lock_state(&self.inner)?;
            state.prepare_video_frame_inner(
                media_ssrc,
                frame,
                metadata_frame,
                frame_time_ms,
                media_stream_type,
            )?
        };
        let (sent, octets) =
            send_prepared_packets(crypto, prepared.packets, sink, burst_size, burst_interval)?;
        Ok(VideoSendSummary {
            sent,
            octets,
            transport_sequence: prepared.transport_sequence,
            rtp_timestamp: prepared.rtp_timestamp,
            last_sequence: prepared.last_sequence,
        })
    }

    /// Resends one cached media packet (by original sequence) over RTX.
    pub fn send_video_rtx_packet(
        &self,
        crypto: &SharedTransportCrypto,
        sink: &dyn PacketSink,
        media_ssrc: u32,
        sequence: u16,
        media_stream_type: &str,
    ) -> Result<RtxSendSummary, SendError> {
        let _send_guard = lock_send(&self.send_lock)?;
        let prepared = {
            let mut state = lock_state(&self.inner)?;
            state.prepare_video_rtx_packet_inner(media_ssrc, sequence, media_stream_type)?
        };
        let PreparedRTXSend {
            packet,
            rtx_ssrc,
            rtx_sequence,
            transport_sequence,
        } = prepared;
        let sent = match packet {
            Some(packet) => {
                let (sent, _) = send_prepared_packets(crypto, vec![packet], sink, 0, 0.0)?;
                sent > 0
            }
            None => false,
        };
        Ok(RtxSendSummary {
            sent,
            rtx_ssrc,
            rtx_sequence,
            transport_sequence,
        })
    }

    /// Answers an RTCP generic NACK. `payload` is the decrypted RTCP payload
    /// (media SSRC followed by PID/BLP pairs).
    pub fn send_video_rtx_for_nack(
        &self,
        crypto: &SharedTransportCrypto,
        sink: &dyn PacketSink,
        payload: &[u8],
        media_stream_type: &str,
        burst_size: usize,
        burst_interval: f64,
    ) -> Result<NackResendSummary, SendError> {
        let _send_guard = lock_send(&self.send_lock)?;
        let prepared = {
            let mut state = lock_state(&self.inner)?;
            state.prepare_video_rtx_for_nack_inner(payload, media_stream_type)?
        };
        let summary = prepared.summary;
        let _ = send_prepared_packets(crypto, prepared.packets, sink, burst_size, burst_interval)?;
        Ok(summary)
    }
}

impl VideoSendPipelineState {
    fn prepare_video_frame_inner(
        &mut self,
        media_ssrc: u32,
        frame: &[u8],
        metadata_frame: Option<&[u8]>,
        frame_time_ms: f64,
        media_stream_type: &str,
    ) -> Result<PreparedVideoSend, SendError> {
        let stream = self
            .streams
            .get_mut(&media_ssrc)
            .ok_or_else(|| SendError::Value(format!("Unknown video stream SSRC: {media_ssrc}")))?;
        let rtp_timestamp = stream.packetizer.timestamp();
        let packets = stream
            .packetizer
            .packetize(frame, metadata_frame, frame_time_ms)
            .map_err(packet_error)?;
        let mut prepared_packets = Vec::with_capacity(packets.len());
        let mut last_sequence = None;

        for packet in packets {
            let MediaPacket {
                header,
                payload: media_payload,
                marker,
                sequence,
                timestamp,
                ..
            } = packet;
            self.transport_sequence = self.transport_sequence.wrapping_add(1);
            let extension_payload = build_video_extension_payload(
                self.transport_sequence,
                &stream.rid,
                media_stream_type,
                false,
            )?;
            let header = header_with_one_byte_extensions(&header, &extension_payload)?;
            let mut payload = Vec::with_capacity(extension_payload.len() + media_payload.len());
            payload.extend_from_slice(&extension_payload);
            payload.extend_from_slice(&media_payload);

            last_sequence = Some(sequence);
            stream.cache_media_packet(media_payload, marker, sequence, timestamp);
            prepared_packets.push(PreparedRTPPacket { header, payload });
        }

        Ok(PreparedVideoSend {
            packets: prepared_packets,
            transport_sequence: self.transport_sequence,
            rtp_timestamp,
            last_sequence,
        })
    }

    fn prepare_video_rtx_packet_inner(
        &mut self,
        media_ssrc: u32,
        sequence: u16,
        media_stream_type: &str,
    ) -> Result<PreparedRTXSend, SendError> {
        let stream = self
            .streams
            .get_mut(&media_ssrc)
            .ok_or_else(|| SendError::Value(format!("Unknown video stream SSRC: {media_ssrc}")))?;
        let Some(rtx_payload_type) = stream.rtx_payload_type else {
            return Ok(PreparedRTXSend {
                packet: None,
                rtx_ssrc: 0,
                rtx_sequence: 0,
                transport_sequence: self.transport_sequence,
            });
        };
        if stream.rtx_ssrc == 0 {
            return Ok(PreparedRTXSend {
                packet: None,
                rtx_ssrc: 0,
                rtx_sequence: 0,
                transport_sequence: self.transport_sequence,
            });
        }
        let Some(packet) = stream.rtx_cache.get(&sequence) else {
            return Ok(PreparedRTXSend {
                packet: None,
                rtx_ssrc: 0,
                rtx_sequence: 0,
                transport_sequence: self.transport_sequence,
            });
        };

        stream.rtx_sequence = stream.rtx_sequence.wrapping_add(1);
        self.transport_sequence = self.transport_sequence.wrapping_add(1);
        let rtx_sequence = stream.rtx_sequence;
        let extension_payload = build_video_extension_payload(
            self.transport_sequence,
            &stream.rid,
            media_stream_type,
            true,
        )?;

        let header = RtpPacket::new_header(
            rtx_payload_type,
            rtx_sequence,
            packet.timestamp,
            stream.rtx_ssrc,
            packet.marker,
            false,
        );
        let header = header_with_one_byte_extensions(&header, &extension_payload)?;
        let mut payload = Vec::with_capacity(extension_payload.len() + 2 + packet.payload.len());
        payload.extend_from_slice(&extension_payload);
        payload.extend_from_slice(&packet.sequence.to_be_bytes());
        payload.extend_from_slice(&packet.payload);
        Ok(PreparedRTXSend {
            packet: Some(PreparedRTPPacket { header, payload }),
            rtx_ssrc: stream.rtx_ssrc,
            rtx_sequence,
            transport_sequence: self.transport_sequence,
        })
    }

    fn prepare_video_rtx_for_nack_inner(
        &mut self,
        payload: &[u8],
        media_stream_type: &str,
    ) -> Result<PreparedNackResend, SendError> {
        if payload.len() < 8 {
            return Ok(PreparedNackResend {
                packets: Vec::new(),
                summary: NackResendSummary {
                    resent: 0,
                    media_ssrc: 0,
                    rtx_ssrc: 0,
                    last_rtx_sequence: 0,
                    transport_sequence: self.transport_sequence,
                },
            });
        }
        let media_ssrc = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if !self.streams.contains_key(&media_ssrc) {
            return Ok(PreparedNackResend {
                packets: Vec::new(),
                summary: NackResendSummary {
                    resent: 0,
                    media_ssrc,
                    rtx_ssrc: 0,
                    last_rtx_sequence: 0,
                    transport_sequence: self.transport_sequence,
                },
            });
        }

        let mut prepared_packets = Vec::new();
        let mut resent = 0usize;
        let mut rtx_ssrc = 0u32;
        let mut last_rtx_sequence = 0u16;
        for chunk in payload[4..].as_chunks::<4>().0 {
            let pid = u16::from_be_bytes([chunk[0], chunk[1]]);
            let blp = u16::from_be_bytes([chunk[2], chunk[3]]);
            let prepared =
                self.prepare_video_rtx_packet_inner(media_ssrc, pid, media_stream_type)?;
            if let Some(packet) = prepared.packet {
                resent += 1;
                rtx_ssrc = prepared.rtx_ssrc;
                last_rtx_sequence = prepared.rtx_sequence;
                prepared_packets.push(packet);
            }
            for bit in 0..16 {
                if blp & (1 << bit) != 0 {
                    let prepared = self.prepare_video_rtx_packet_inner(
                        media_ssrc,
                        pid.wrapping_add(bit + 1),
                        media_stream_type,
                    )?;
                    if let Some(packet) = prepared.packet {
                        resent += 1;
                        rtx_ssrc = prepared.rtx_ssrc;
                        last_rtx_sequence = prepared.rtx_sequence;
                        prepared_packets.push(packet);
                    }
                }
            }
        }

        Ok(PreparedNackResend {
            packets: prepared_packets,
            summary: NackResendSummary {
                resent,
                media_ssrc,
                rtx_ssrc,
                last_rtx_sequence,
                transport_sequence: self.transport_sequence,
            },
        })
    }
}
