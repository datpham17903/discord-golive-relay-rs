//! RTCP constants, builders and parsers for the receive side, ported from
//! `discord/ext/native_voice/client.py` (`_send_rtcp_receiver_report`, `_rtcp_receiver_report_block`,
//! `_send_rtcp_nack`, `send_video_pli`, `_handle_rtcp_sender_report`, `_handle_rtcp_receiver_report`)
//! and `rtp.py` (sequence/timestamp helpers).
//!
//! Every builder returns `(header, body)`: the 8-byte header stays in the clear and `body` is what
//! `SharedTransportCrypto::encrypt_rtcp(header, body)` encrypts.

pub const RTCP_SENDER_REPORT: u8 = 200;
pub const RTCP_RECEIVER_REPORT: u8 = 201;
pub const RTCP_RTP_FEEDBACK: u8 = 205;
pub const RTCP_PAYLOAD_FEEDBACK: u8 = 206;
pub const RTCP_GENERIC_NACK: u8 = 1;
pub const RTCP_PICTURE_LOSS_INDICATION: u8 = 1;
pub const RTCP_PROTECTED_HEADER_LEN: usize = 8;
pub const RTCP_NACK_MAX_SEQUENCES: usize = 0xFD;
pub const VIDEO_NACK_MAX_OUTSTANDING: usize = 1000;
pub const VIDEO_NACK_MAX_RETRIES: u32 = 100;
/// Seconds between retries of an unanswered NACK (and the cadence of the retry timer).
pub const VIDEO_NACK_RETRY_INTERVAL: f64 = 0.1;

pub const RTP_EXT_AUDIO_LEVEL: u8 = 1;
pub const RTP_EXT_ABSOLUTE_SEND_TIME: u8 = 3;
/// Transport-wide congestion control sequence number (what `relay-discord`'s TWCC feedback needs).
pub const RTP_EXT_TRANSPORT_SEQUENCE: u8 = 5;
pub const RTP_EXT_DISCORD_SPEAKING: u8 = 9;

const RTP_TIMESTAMP_HALF_RANGE: u32 = 1 << 31;
/// Seconds between the NTP epoch (1900) and the Unix epoch (1970).
const NTP_UNIX_OFFSET: f64 = 2_208_988_800.0;

/// `_sequence_delta`.
pub fn sequence_delta(sequence: u16, previous: u16) -> u16 {
    sequence.wrapping_sub(previous)
}

/// `_unwrap_sequence`: `sequence` as the 16-bit number nearest to `reference` on the unrolled line.
pub fn unwrap_sequence(sequence: u16, reference: i64) -> i64 {
    let mut delta = i64::from(sequence.wrapping_sub(reference as u16));
    if delta > 0x7FFF {
        delta -= 0x1_0000;
    }
    reference + delta
}

/// `_rtp_timestamp_delta`: `None` when `timestamp` is behind `base` by half the range or more.
pub fn rtp_timestamp_delta(timestamp: u32, base: u32) -> Option<u32> {
    let delta = timestamp.wrapping_sub(base);
    (delta < RTP_TIMESTAMP_HALF_RANGE).then_some(delta)
}

/// `send_video_pli`: picture loss indication for `media_ssrc`.
pub fn pli(sender_ssrc: u32, media_ssrc: u32) -> (Vec<u8>, Vec<u8>) {
    (
        header(
            0x80 | RTCP_PICTURE_LOSS_INDICATION,
            RTCP_PAYLOAD_FEEDBACK,
            2,
            sender_ssrc,
        ),
        media_ssrc.to_be_bytes().to_vec(),
    )
}

/// `_send_rtcp_sender_report`: `unix_time` is the wall clock in seconds since 1970.
pub fn sender_report(
    ssrc: u32,
    unix_time: f64,
    rtp_timestamp: u32,
    packet_count: u32,
    octet_count: u32,
) -> (Vec<u8>, Vec<u8>) {
    let ntp = unix_time + NTP_UNIX_OFFSET;
    let ntp_seconds = ntp as u32;
    let ntp_fraction = ((ntp - f64::from(ntp_seconds)) * 4_294_967_296.0) as u32;
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&ntp_seconds.to_be_bytes());
    body.extend_from_slice(&ntp_fraction.to_be_bytes());
    body.extend_from_slice(&rtp_timestamp.to_be_bytes());
    body.extend_from_slice(&packet_count.to_be_bytes());
    body.extend_from_slice(&octet_count.to_be_bytes());
    (header(0x80, RTCP_SENDER_REPORT, 6, ssrc), body)
}

/// `_send_rtcp_receiver_report`: at most 31 report blocks of 24 bytes each.
pub fn receiver_report(ssrc: u32, blocks: &[[u8; 24]]) -> (Vec<u8>, Vec<u8>) {
    let blocks = &blocks[..blocks.len().min(31)];
    let header = header(
        0x80 | blocks.len() as u8,
        RTCP_RECEIVER_REPORT,
        1 + (blocks.len() as u16 * 6),
        ssrc,
    );
    (header, blocks.concat())
}

/// `_send_rtcp_nack`: generic NACK for `sequences` (deduplicated, grouped into PID + BLP blocks).
/// `None` when there is nothing to request.
pub fn nack(sender_ssrc: u32, media_ssrc: u32, sequences: &[u16]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut pending: Vec<u16> = Vec::with_capacity(sequences.len());
    for &sequence in sequences {
        if !pending.contains(&sequence) {
            pending.push(sequence);
        }
    }
    let mut blocks: Vec<(u16, u16)> = Vec::new();
    while !pending.is_empty() {
        let pid = pending.remove(0);
        let mut blp = 0u16;
        pending.retain(|&sequence| {
            let distance = sequence.wrapping_sub(pid);
            if (1..=16).contains(&distance) {
                blp |= 1 << (distance - 1);
                false
            } else {
                true
            }
        });
        blocks.push((pid, blp));
    }
    if blocks.is_empty() {
        return None;
    }
    let header = header(
        0x80 | RTCP_GENERIC_NACK,
        RTCP_RTP_FEEDBACK,
        2 + blocks.len() as u16,
        sender_ssrc,
    );
    let mut body = media_ssrc.to_be_bytes().to_vec();
    for (pid, blp) in blocks {
        body.extend_from_slice(&pid.to_be_bytes());
        body.extend_from_slice(&blp.to_be_bytes());
    }
    Some((header, body))
}

fn header(first: u8, packet_type: u8, length: u16, ssrc: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(RTCP_PROTECTED_HEADER_LEN);
    out.push(first);
    out.push(packet_type);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&ssrc.to_be_bytes());
    out
}

/// What a sender report tells us about the sender's clock (`_handle_rtcp_sender_report`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SenderReport {
    /// NTP time as Unix-epoch seconds.
    pub ntp_time: f64,
    pub rtp_timestamp: u32,
    /// Middle 32 bits of the NTP timestamp: the `last_sender_report` field of our report blocks.
    pub compact_ntp: u32,
}

/// Parses the sender info of an SR body (the bytes after the protected header); `None` if short.
pub fn parse_sender_report(payload: &[u8]) -> Option<SenderReport> {
    if payload.len() < 20 {
        return None;
    }
    let word =
        |i: usize| u32::from_be_bytes([payload[i], payload[i + 1], payload[i + 2], payload[i + 3]]);
    let (seconds, fraction, rtp_timestamp) = (word(0), word(4), word(8));
    Some(SenderReport {
        ntp_time: f64::from(seconds) - NTP_UNIX_OFFSET + f64::from(fraction) / 4_294_967_296.0,
        rtp_timestamp,
        compact_ntp: ((seconds & 0xFFFF) << 16) | (fraction >> 16),
    })
}

/// One receiver-report block (`RTCPReceiverReport`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReportBlock {
    pub sender_ssrc: u32,
    pub source_ssrc: u32,
    pub fraction_lost: u8,
    pub cumulative_lost: i32,
    pub extended_high_sequence: u32,
    pub jitter: u32,
    pub last_sender_report: u32,
    pub delay_since_last_sender_report: u32,
}

/// `_handle_rtcp_receiver_report`: the complete 24-byte blocks among the first `report_count`.
pub fn parse_receiver_report(
    sender_ssrc: u32,
    report_count: u8,
    payload: &[u8],
) -> Vec<ReportBlock> {
    let word =
        |i: usize| u32::from_be_bytes([payload[i], payload[i + 1], payload[i + 2], payload[i + 3]]);
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < payload.len().min(usize::from(report_count) * 24) {
        if offset + 24 > payload.len() {
            break;
        }
        let loss = word(offset + 4);
        let mut cumulative_lost = (loss & 0xFF_FFFF) as i32;
        if cumulative_lost & 0x80_0000 != 0 {
            cumulative_lost -= 0x100_0000;
        }
        out.push(ReportBlock {
            sender_ssrc,
            source_ssrc: word(offset),
            fraction_lost: (loss >> 24) as u8,
            cumulative_lost,
            extended_high_sequence: word(offset + 8),
            jitter: word(offset + 12),
            last_sender_report: word(offset + 16),
            delay_since_last_sender_report: word(offset + 20),
        });
        offset += 24;
    }
    out
}

/// `_RTCPReceiveReportState`: what the receiver report we send about one video SSRC is built from.
#[derive(Clone, Debug)]
pub struct ReceiveReportState {
    base_extended_sequence: i64,
    max_extended_sequence: i64,
    packets_received: i64,
    expected_prior: i64,
    received_prior: i64,
    jitter: f64,
    transit: Option<i64>,
    /// Seconds (receiver clock) of the last report sent.
    pub last_report_at: f64,
}

impl ReceiveReportState {
    pub fn new(sequence: u16) -> Self {
        Self {
            base_extended_sequence: i64::from(sequence),
            max_extended_sequence: i64::from(sequence),
            packets_received: 1,
            expected_prior: 0,
            received_prior: 0,
            jitter: 0.0,
            transit: None,
            // Python starts at 0.0 on a perf_counter clock that is far past it: the first packet
            // reports at once.
            last_report_at: f64::NEG_INFINITY,
        }
    }

    /// A further packet: `arrival_secs` is the receiver clock, `timestamp` the packet's RTP timestamp.
    pub fn on_packet(&mut self, sequence: u16, timestamp: u32, arrival_secs: f64) {
        let extended = unwrap_sequence(sequence, self.max_extended_sequence);
        if extended > self.max_extended_sequence {
            self.max_extended_sequence = extended;
        }
        self.packets_received += 1;
        self.note_transit(timestamp, arrival_secs);
    }

    /// Jitter estimator (RFC 3550 A.8) at the 90 kHz video clock; also run for the first packet.
    pub fn note_transit(&mut self, timestamp: u32, arrival_secs: f64) {
        let arrival = (arrival_secs * 90_000.0).round() as i64;
        let transit = arrival - i64::from(timestamp);
        let previous = self.transit.replace(transit);
        if let Some(previous) = previous {
            let delta = (transit - previous).abs() as f64;
            self.jitter += (delta - self.jitter) / 16.0;
        }
    }

    /// `_rtcp_receiver_report_block` for `ssrc`; `last_sr` is `(compact NTP, seconds since that SR
    /// arrived)` of the sender's latest report for it.
    pub fn block(&mut self, ssrc: u32, last_sr: Option<(u32, f64)>) -> [u8; 24] {
        let expected = self.max_extended_sequence - self.base_extended_sequence + 1;
        let lost = expected - self.packets_received;
        let expected_interval = expected - self.expected_prior;
        let received_interval = self.packets_received - self.received_prior;
        let lost_interval = expected_interval - received_interval;
        let mut fraction_lost = 0i64;
        if expected_interval > 0 && lost_interval > 0 {
            fraction_lost = ((lost_interval << 8) / expected_interval).min(255);
        }
        self.expected_prior = expected;
        self.received_prior = self.packets_received;

        let mut cumulative = lost.clamp(-(1 << 23), (1 << 23) - 1);
        if cumulative < 0 {
            cumulative += 1 << 24;
        }
        let loss = (((fraction_lost & 0xFF) as u32) << 24) | (cumulative as u32 & 0xFF_FFFF);

        let (lsr, delay) = last_sr.map_or((0, 0), |(lsr, age_secs)| {
            (
                lsr,
                ((age_secs * 65536.0).round().clamp(0.0, f64::from(u32::MAX))) as u32,
            )
        });

        let mut out = [0u8; 24];
        out[0..4].copy_from_slice(&ssrc.to_be_bytes());
        out[4..8].copy_from_slice(&loss.to_be_bytes());
        out[8..12].copy_from_slice(&(self.max_extended_sequence as u32).to_be_bytes());
        out[12..16].copy_from_slice(&(self.jitter.round() as u32).to_be_bytes());
        out[16..20].copy_from_slice(&lsr.to_be_bytes());
        out[20..24].copy_from_slice(&delay.to_be_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_sequence_crosses_the_16_bit_boundary_both_ways() {
        assert_eq!(unwrap_sequence(1, 65_535), 65_537);
        assert_eq!(unwrap_sequence(65_535, 65_537), 65_535);
        assert_eq!(unwrap_sequence(5, 3), 5);
        assert_eq!(unwrap_sequence(3, 5), 3);
    }

    #[test]
    fn rtp_timestamp_delta_rejects_the_past() {
        assert_eq!(rtp_timestamp_delta(10, 4), Some(6));
        assert_eq!(rtp_timestamp_delta(2, u32::MAX - 1), Some(4));
        assert_eq!(rtp_timestamp_delta(4, 10), None);
    }

    #[test]
    fn pli_layout() {
        let (header, body) = pli(0x0102_0304, 0xAABB_CCDD);
        assert_eq!(header, [0x81, 206, 0, 2, 1, 2, 3, 4]);
        assert_eq!(body, [0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn empty_receiver_report_is_a_bare_header() {
        let (header, body) = receiver_report(7, &[]);
        assert_eq!(header, [0x80, 201, 0, 1, 0, 0, 0, 7]);
        assert!(body.is_empty());
    }

    #[test]
    fn nack_groups_sequences_into_pid_blp_blocks() {
        let (header, body) = nack(0x11, 0x22, &[100, 101, 103, 100, 200, 65_535, 0]).unwrap();
        // blocks: (100, 101 and 103 -> bits 0 and 2), (200, 0), (65535, bit 0 for 0)
        assert_eq!(header, [0x81, 205, 0, 5, 0, 0, 0, 0x11]);
        assert_eq!(&body[..4], 0x22u32.to_be_bytes());
        assert_eq!(&body[4..8], [0, 100, 0, 0b101]);
        assert_eq!(&body[8..12], [0, 200, 0, 0]);
        assert_eq!(&body[12..16], [0xFF, 0xFF, 0, 1]);
        assert!(nack(1, 2, &[]).is_none());
    }

    #[test]
    fn sender_report_gives_unix_time_and_compact_ntp() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(2_208_988_800u32 + 1000).to_be_bytes());
        payload.extend_from_slice(&0x8000_0000u32.to_be_bytes());
        payload.extend_from_slice(&90_000u32.to_be_bytes());
        payload.extend_from_slice(&[0; 8]);
        let sr = parse_sender_report(&payload).unwrap();
        assert!((sr.ntp_time - 1000.5).abs() < 1e-9);
        assert_eq!(sr.rtp_timestamp, 90_000);
        assert_eq!(
            sr.compact_ntp,
            ((2_208_988_800u32 + 1000) & 0xFFFF) << 16 | 0x8000
        );
        assert!(parse_sender_report(&payload[..19]).is_none());
    }

    #[test]
    fn receiver_report_blocks_decode_signed_loss() {
        let mut state = ReceiveReportState::new(10);
        state.on_packet(13, 0, 0.0);
        let block = state.block(0xAB, Some((0x1234_5678, 0.5)));
        let parsed = parse_receiver_report(9, 1, &block);
        assert_eq!(parsed.len(), 1);
        let b = parsed[0];
        assert_eq!((b.sender_ssrc, b.source_ssrc), (9, 0xAB));
        // 4 expected (10..=13), 2 received: 2 lost of 4 = 128/256
        assert_eq!((b.fraction_lost, b.cumulative_lost), (128, 2));
        assert_eq!(b.extended_high_sequence, 13);
        assert_eq!(b.last_sender_report, 0x1234_5678);
        assert_eq!(b.delay_since_last_sender_report, 32_768);
        // a duplicate-heavy stream reports negative loss
        let mut dup = ReceiveReportState::new(1);
        dup.on_packet(1, 0, 0.0);
        dup.on_packet(1, 0, 0.0);
        let parsed = parse_receiver_report(9, 1, &dup.block(1, None));
        assert_eq!(parsed[0].cumulative_lost, -2);
        assert_eq!(parsed[0].fraction_lost, 0);
    }

    #[test]
    fn receiver_report_parse_stops_at_a_truncated_block() {
        let mut state = ReceiveReportState::new(1);
        let block = state.block(1, None);
        let mut payload = block.to_vec();
        payload.extend_from_slice(&block[..10]);
        assert_eq!(parse_receiver_report(1, 2, &payload).len(), 1);
    }
}
