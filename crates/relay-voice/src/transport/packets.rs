// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/python/transport.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       pyo3 layer removed. `PyTransportCrypto` became the plain
//                `SharedTransportCrypto`; decrypted packets and RTCP packets are
//                returned as structs instead of Python tuples; `PyErr` results
//                became `SendError`. Added `build_audio_extension_payload` (a port of
//                `discord/voice_media.py::_audio_rtp_extension_payload`, which has no native
//                counterpart).

use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::rtp::RtpPacket;

use super::udp::is_rtcp_packet;
use super::{CryptoError, TransportCrypto, TransportEncryptionMode};

const RTP_ONE_BYTE_EXTENSION_PROFILE: [u8; 2] = [0xBE, 0xDE];
const RTP_EXT_TRANSMISSION_OFFSET: u8 = 2;
const RTP_EXT_ABS_SEND_TIME: u8 = 3;
const RTP_EXT_VIDEO_ORIENTATION: u8 = 4;
const RTP_EXT_TRANSPORT_SEQUENCE_NUMBER: u8 = 5;
const RTP_EXT_PLAYOUT_DELAY: u8 = 6;
const RTP_EXT_VIDEO_CONTENT_TYPE: u8 = 7;
const RTP_EXT_VIDEO_TIMING: u8 = 8;
const RTP_EXT_MEDIA_STREAM_TYPE: u8 = 10;
const RTP_EXT_RID: u8 = 11;
const RTP_EXT_REPAIRED_RID: u8 = 12;
const PRECISE_SLEEP_MARGIN: Duration = Duration::from_millis(1);
const PRECISE_SPIN_THRESHOLD: Duration = Duration::from_micros(250);

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Transport crypto lock poisoned")]
    Poisoned,
    #[error("{0}")]
    Value(String),
}

/// One decrypted RTP packet: `extension_payload` is the header-extension block
/// (empty if the packet had none) and `media_payload` the codec payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecryptedRtpPacket {
    /// Position of the packet in the input batch.
    pub index: usize,
    pub packet: RtpPacket,
    pub extension_payload: Vec<u8>,
    /// `(id, data)` pairs parsed out of `extension_payload`.
    pub extensions: Vec<(u8, Vec<u8>)>,
    pub media_payload: Vec<u8>,
}

/// One RTCP packet of a decrypted compound packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcpPacket {
    /// RTCP packet type (byte 1 of the header).
    pub packet_type: u8,
    /// Report count / feedback format (low 5 bits of byte 0).
    pub count: u8,
    pub sender_ssrc: u32,
    /// Bytes after the protected header (the sender SSRC is part of the header).
    pub payload: Vec<u8>,
}

/// Cloneable handle to the transport cipher; encrypt and decrypt share the lock
/// and therefore the outgoing nonce counter.
#[derive(Clone)]
pub struct SharedTransportCrypto {
    inner: Arc<Mutex<TransportCrypto>>,
}

impl SharedTransportCrypto {
    pub fn new(mode: TransportEncryptionMode, key: &[u8]) -> Result<Self, CryptoError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(TransportCrypto::new(mode, key)?)),
        })
    }

    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, TransportCrypto>, SendError> {
        self.inner.lock().map_err(|_| SendError::Poisoned)
    }

    pub fn encrypt_rtp(&self, header: &[u8], payload: &[u8]) -> Result<Vec<u8>, SendError> {
        Ok(self.lock()?.encrypt_rtp(header, payload)?)
    }

    /// RTCP uses the same AEAD framing and nonce counter as RTP.
    pub fn encrypt_rtcp(&self, header: &[u8], payload: &[u8]) -> Result<Vec<u8>, SendError> {
        Ok(self.lock()?.encrypt_rtp(header, payload)?)
    }

    /// Decrypts one RTCP datagram whose first `header_len` bytes are the
    /// unencrypted header, then splits the compound packet.
    pub fn decrypt_rtcp_packets(
        &self,
        packet: &[u8],
        header_len: usize,
    ) -> Result<Vec<RtcpPacket>, SendError> {
        let payload = decrypt_rtcp_payload(self, packet, header_len)?;
        parse_decrypted_rtcp_packets(packet, &payload, header_len)
    }

    /// Parses and decrypts a batch of RTP datagrams. Returns the number of
    /// packets that failed to parse or decrypt, and the decrypted ones.
    pub fn decrypt_rtp_packets<P: AsRef<[u8]>>(
        &self,
        packets: &[P],
    ) -> Result<(usize, Vec<DecryptedRtpPacket>), SendError> {
        let mut parsed_packets = Vec::new();
        let mut parse_failed = 0usize;
        for (index, item) in packets.iter().enumerate() {
            match RtpPacket::parse(item.as_ref()) {
                Ok(packet) => parsed_packets.push((index, packet)),
                Err(_) => parse_failed += 1,
            }
        }

        let (decrypt_failed, decoded) = decrypt_rtp_packet_batch(self, parsed_packets)?;
        Ok((parse_failed + decrypt_failed, decoded))
    }
}

impl std::fmt::Debug for SharedTransportCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedTransportCrypto(***)")
    }
}

fn strip_rtp_padding_native(payload: &mut Vec<u8>, padded: bool) -> Result<(), SendError> {
    if !padded {
        return Ok(());
    }

    let padding_len = payload
        .last()
        .copied()
        .ok_or_else(|| SendError::Value("RTP padding is missing a padding length".to_owned()))?
        as usize;
    if padding_len == 0 || padding_len > payload.len() {
        return Err(SendError::Value(
            "RTP padding length exceeds decrypted payload length".to_owned(),
        ));
    }

    payload.truncate(payload.len() - padding_len);
    Ok(())
}

fn rtp_extension_string(value: &str, fallback: &str) -> Vec<u8> {
    let encoded: Vec<u8> = value.bytes().filter(u8::is_ascii).take(16).collect();
    if !encoded.is_empty() {
        return encoded;
    }
    fallback.as_bytes()[..fallback.len().min(16)].to_vec()
}

fn abs_send_time() -> [u8; 3] {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0);
    let value = (((seconds % 64.0) * 262_144.0) as u32) & 0xFF_FFFF;
    [
        ((value >> 16) & 0xFF) as u8,
        ((value >> 8) & 0xFF) as u8,
        (value & 0xFF) as u8,
    ]
}

fn push_one_byte_extension(payload: &mut Vec<u8>, id: u8, data: &[u8]) -> Result<(), SendError> {
    if !(1..=14).contains(&id) {
        return Err(SendError::Value(format!(
            "RTP extension ID must be in [1, 14], got {id}"
        )));
    }
    if data.is_empty() || data.len() > 16 {
        return Err(SendError::Value(format!(
            "RTP extension data length must be in [1, 16], got {}",
            data.len()
        )));
    }
    payload.push((id << 4) | ((data.len() - 1) as u8));
    payload.extend_from_slice(data);
    Ok(())
}

pub fn build_video_extension_payload(
    transport_sequence: u16,
    rid: &str,
    media_stream_type: &str,
    repaired: bool,
) -> Result<Vec<u8>, SendError> {
    let mut payload = Vec::with_capacity(48);
    push_one_byte_extension(&mut payload, RTP_EXT_TRANSMISSION_OFFSET, &[0, 0, 0])?;
    push_one_byte_extension(&mut payload, RTP_EXT_ABS_SEND_TIME, &abs_send_time())?;
    push_one_byte_extension(&mut payload, RTP_EXT_VIDEO_ORIENTATION, &[0])?;
    push_one_byte_extension(
        &mut payload,
        RTP_EXT_TRANSPORT_SEQUENCE_NUMBER,
        &transport_sequence.to_be_bytes(),
    )?;
    push_one_byte_extension(&mut payload, RTP_EXT_PLAYOUT_DELAY, &[0, 0, 0])?;
    push_one_byte_extension(
        &mut payload,
        RTP_EXT_VIDEO_CONTENT_TYPE,
        &[if media_stream_type == "screen" { 1 } else { 0 }],
    )?;
    push_one_byte_extension(&mut payload, RTP_EXT_VIDEO_TIMING, &[0; 13])?;
    push_one_byte_extension(
        &mut payload,
        RTP_EXT_MEDIA_STREAM_TYPE,
        &rtp_extension_string(media_stream_type, "video"),
    )?;
    if repaired {
        push_one_byte_extension(
            &mut payload,
            RTP_EXT_REPAIRED_RID,
            &rtp_extension_string(rid, "100"),
        )?;
    } else {
        push_one_byte_extension(&mut payload, RTP_EXT_RID, &rtp_extension_string(rid, "100"))?;
    }

    let padding = (4 - (payload.len() % 4)) % 4;
    payload.extend(std::iter::repeat_n(0, padding));
    Ok(payload)
}

const RTP_EXT_AUDIO_LEVEL: u8 = 1;
const RTP_EXT_DISCORD_SPEAKING: u8 = 9;
/// RTP audio level of digital silence (`RTP_AUDIO_LEVEL_SILENCE`, dBov).
pub const RTP_AUDIO_LEVEL_SILENCE: u8 = 127;

/// `_audio_rtp_extension_payload`: audio level + voice activity, absolute send time, the Discord
/// speaking flags and the `audio` media stream type. `speaking` is a `voice_ws::speaking` bit set.
pub fn build_audio_extension_payload(speaking: u32, audio_level: u8) -> Result<Vec<u8>, SendError> {
    let level = audio_level.min(RTP_AUDIO_LEVEL_SILENCE);
    let voice_activity = if speaking != 0 { 0x80 } else { 0 };
    let mut speaking_value = 0u8;
    if speaking & 4 != 0 {
        speaking_value |= 0b001;
    }
    if speaking & 1 != 0 {
        speaking_value |= 0b010;
    }
    if speaking & 2 != 0 {
        speaking_value |= 0b100;
    }
    let mut payload = Vec::with_capacity(16);
    push_one_byte_extension(&mut payload, RTP_EXT_AUDIO_LEVEL, &[voice_activity | level])?;
    push_one_byte_extension(&mut payload, RTP_EXT_ABS_SEND_TIME, &abs_send_time())?;
    push_one_byte_extension(&mut payload, RTP_EXT_DISCORD_SPEAKING, &[speaking_value])?;
    push_one_byte_extension(&mut payload, RTP_EXT_MEDIA_STREAM_TYPE, b"audio")?;
    let padding = (4 - (payload.len() % 4)) % 4;
    payload.extend(std::iter::repeat_n(0, padding));
    Ok(payload)
}

/// Blocks the calling thread until `deadline`, sleeping first and spinning for
/// the last stretch. Call it from a dedicated send thread, not an async task.
pub fn wait_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return;
        }

        let remaining = deadline - now;
        if remaining > PRECISE_SLEEP_MARGIN {
            thread::sleep(remaining - PRECISE_SLEEP_MARGIN);
        } else if remaining > PRECISE_SPIN_THRESHOLD {
            thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}

pub fn header_with_one_byte_extensions(
    header: &[u8],
    extension_payload: &[u8],
) -> Result<Vec<u8>, SendError> {
    if header.len() < 12 {
        return Err(SendError::Value(
            "RTP header must be at least 12 bytes".to_owned(),
        ));
    }
    let mut header_buffer = header[..12].to_vec();
    header_buffer[0] |= 0x10;
    header_buffer.extend_from_slice(&RTP_ONE_BYTE_EXTENSION_PROFILE);
    header_buffer.extend_from_slice(&((extension_payload.len() / 4) as u16).to_be_bytes());
    Ok(header_buffer)
}

/// Splits an RTP header-extension block into `(id, data)` pairs. Understands
/// the one-byte (`0xBEDE`) and two-byte (`0x100x`) profiles; any other profile
/// yields nothing.
pub fn parse_rtp_extensions(profile: u16, payload: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut extensions = Vec::new();
    if profile == 0xBEDE {
        parse_one_byte_extensions(payload, &mut extensions);
    } else if profile & 0xFFF0 == 0x1000 {
        parse_two_byte_extensions(payload, &mut extensions);
    }
    extensions
}

fn parse_one_byte_extensions(payload: &[u8], extensions: &mut Vec<(u8, Vec<u8>)>) {
    let mut index = 0usize;

    while index < payload.len() {
        let header = payload[index];
        index += 1;
        if header == 0 {
            continue;
        }

        let extension_id = header >> 4;
        if extension_id == 15 {
            break;
        }

        let length = ((header & 0x0F) as usize) + 1;
        let end = index + length;
        if end > payload.len() {
            break;
        }

        extensions.push((extension_id, payload[index..end].to_vec()));
        index = end;
    }
}

fn parse_two_byte_extensions(payload: &[u8], extensions: &mut Vec<(u8, Vec<u8>)>) {
    let mut index = 0usize;

    while index < payload.len() {
        let extension_id = payload[index];
        index += 1;
        if extension_id == 0 {
            continue;
        }
        if index >= payload.len() {
            break;
        }

        let length = payload[index] as usize;
        index += 1;
        let end = index + length;
        if end > payload.len() {
            break;
        }

        extensions.push((extension_id, payload[index..end].to_vec()));
        index = end;
    }
}

fn decrypt_rtcp_payload(
    crypto: &SharedTransportCrypto,
    packet: &[u8],
    header_len: usize,
) -> Result<Vec<u8>, SendError> {
    if header_len == 0 || packet.len() < header_len + 4 {
        return Err(SendError::Value(
            "RTCP packet is too short for the requested header length".to_owned(),
        ));
    }

    let nonce_offset = packet.len() - 4;
    let mut nonce = [0u8; 4];
    nonce.copy_from_slice(&packet[nonce_offset..]);
    let header = &packet[..header_len];
    let encrypted_payload = &packet[header_len..nonce_offset];
    Ok(crypto
        .lock()?
        .decrypt_packet(header, encrypted_payload, &nonce)?)
}

fn decrypt_rtp_packet_inner(
    crypto: &mut TransportCrypto,
    index: usize,
    packet: RtpPacket,
) -> Result<DecryptedRtpPacket, SendError> {
    let mut payload = crypto.decrypt_rtp(&packet)?;
    strip_rtp_padding_native(&mut payload, packet.padded)?;
    if packet.extension_payload_len > payload.len() {
        return Err(SendError::Value(
            "RTP extension length exceeds decrypted payload length".to_owned(),
        ));
    }

    let extension_payload_len = packet.extension_payload_len;
    let extension_payload = payload[..extension_payload_len].to_vec();
    let extensions = parse_rtp_extensions(packet.extension_profile, &extension_payload);
    let media_payload = payload[extension_payload_len..].to_vec();
    Ok(DecryptedRtpPacket {
        index,
        packet,
        extension_payload,
        extensions,
        media_payload,
    })
}

fn decrypt_rtp_packet_batch(
    crypto: &SharedTransportCrypto,
    packets: Vec<(usize, RtpPacket)>,
) -> Result<(usize, Vec<DecryptedRtpPacket>), SendError> {
    let mut crypto = crypto.lock()?;
    let mut failed = 0usize;
    let mut decoded = Vec::with_capacity(packets.len());
    for (index, packet) in packets {
        match decrypt_rtp_packet_inner(&mut crypto, index, packet) {
            Ok(packet) => decoded.push(packet),
            Err(_) => failed += 1,
        }
    }
    Ok((failed, decoded))
}

fn parse_decrypted_rtcp_packets(
    packet: &[u8],
    payload: &[u8],
    header_len: usize,
) -> Result<Vec<RtcpPacket>, SendError> {
    if packet.len() < header_len || header_len < 8 {
        return Err(SendError::Value(
            "RTCP packet is too short for the requested header length".to_owned(),
        ));
    }

    let first_packet_length = (u16::from_be_bytes([packet[2], packet[3]]) as usize + 1) * 4;
    if first_packet_length < header_len {
        return Err(SendError::Value(
            "RTCP packet length is shorter than the protected header".to_owned(),
        ));
    }

    let first_payload_length = first_packet_length - header_len;
    if first_payload_length > payload.len() {
        return Err(SendError::Value(
            "RTCP payload is shorter than the compound packet length".to_owned(),
        ));
    }

    let mut packets = Vec::new();
    let sender_ssrc = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    packets.push(RtcpPacket {
        packet_type: packet[1],
        count: packet[0] & 0x1F,
        sender_ssrc,
        payload: payload[..first_payload_length].to_vec(),
    });

    let mut offset = first_payload_length;
    while offset + header_len <= payload.len() {
        let compound = &payload[offset..];
        if !is_rtcp_packet(compound) {
            break;
        }

        let packet_length = (u16::from_be_bytes([compound[2], compound[3]]) as usize + 1) * 4;
        if packet_length < header_len || packet_length > compound.len() {
            break;
        }

        let sender_ssrc = u32::from_be_bytes([compound[4], compound[5], compound[6], compound[7]]);
        packets.push(RtcpPacket {
            packet_type: compound[1],
            count: compound[0] & 0x1F,
            sender_ssrc,
            payload: compound[header_len..packet_length].to_vec(),
        });
        offset += packet_length;
    }

    Ok(packets)
}
