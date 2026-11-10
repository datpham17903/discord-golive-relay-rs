//! The receive pipeline on its own: encrypted RTP in (the T4 fixture captures and hand-built
//! packets), assembled, decrypted media units and RTCP feedback out. No network.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{CODEC_NAMES, CollectSink, codec_of, hex, read_wire_packets, rtp_manifest};
use relay_media::codec::VoiceCodec;
use relay_voice::dave::Dave;
use relay_voice::receive::{
    MediaType, Packet, PacketCodec, ReceiveConfig, Receiver, SsrcMap, VideoState,
};
use relay_voice::rtcp::{RTCP_PAYLOAD_FEEDBACK, RTCP_RECEIVER_REPORT, RTCP_RTP_FEEDBACK};
use relay_voice::rtp::RtpPacket;
use relay_voice::transport::TransportEncryptionMode;
use relay_voice::transport::packets::SharedTransportCrypto;
use relay_voice::voice_ws::{speaking, video_codec_list};
use serde_json::Value;

const MEDIA_SSRC: u32 = 287_454_020;
const RTX_SSRC: u32 = 1_432_778_632;
const AUDIO_SSRC: u32 = 777;
const OWN_SSRC: u32 = 0x0A0B_0C0D;
const USER: u64 = 42;

fn key() -> Vec<u8> {
    (0u8..32).collect()
}

fn crypto() -> SharedTransportCrypto {
    SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &key()).unwrap()
}

struct Rig {
    rx: Receiver,
    sink: Arc<CollectSink>,
    /// A second cipher instance to read what the receiver sent.
    reader: SharedTransportCrypto,
    ssrcs: Arc<SsrcMap>,
}

fn rig(tweak: impl FnOnce(&mut ReceiveConfig)) -> Rig {
    let codecs = video_codec_list(
        &["AV1", "H265", "H264", "VP8", "VP9"].map(|c| c.parse::<VoiceCodec>().unwrap()),
        false,
    );
    let mut cfg = ReceiveConfig::new(codecs, OWN_SSRC);
    cfg.debug_stats = true;
    tweak(&mut cfg);
    let ssrcs = Arc::new(SsrcMap::new());
    ssrcs.update_video_state(
        USER,
        VideoState {
            audio_ssrc: Some(AUDIO_SSRC),
            video_ssrc: Some(MEDIA_SSRC),
            rtx_ssrc: Some(RTX_SSRC),
            streams: Vec::new(),
        },
    );
    let sink = Arc::new(CollectSink::default());
    let rx = Receiver::new(
        cfg,
        crypto(),
        crypto(),
        sink.clone(),
        Arc::new(Dave::new(1, 2)),
        ssrcs.clone(),
    );
    Rig {
        rx,
        sink,
        reader: crypto(),
        ssrcs,
    }
}

impl Rig {
    /// Everything the receiver sent, as `(packet_type, count, sender_ssrc, payload)`.
    fn sent_rtcp(&self) -> Vec<(u8, u8, u32, Vec<u8>)> {
        self.sink
            .take()
            .iter()
            .flat_map(|wire| self.reader.decrypt_rtcp_packets(wire, 8).unwrap())
            .map(|p| (p.packet_type, p.count, p.sender_ssrc, p.payload))
            .collect()
    }
}

fn feed(rig: &Rig, wire: &[Vec<u8>]) -> Vec<Packet> {
    rig.rx.process_batch(wire, Instant::now())
}

struct Frame {
    au_sha: String,
    packets: Vec<Value>,
}

/// `(wire packets, frames)` of one codec's capture: a key frame then a delta frame. The capture
/// file also ends with a few RTX retransmissions of already sent packets; `capture` leaves them
/// out and `capture_with_rtx` keeps them.
fn capture(codec: &str) -> (Vec<Vec<u8>>, Vec<Frame>) {
    let (mut wire, frames) = capture_with_rtx(codec);
    let primary: usize = frames.iter().map(|f| f.packets.len()).sum();
    wire.truncate(primary);
    (wire, frames)
}

fn capture_with_rtx(codec: &str) -> (Vec<Vec<u8>>, Vec<Frame>) {
    let manifest = rtp_manifest();
    let entry = &manifest["codecs"][codec];
    let wire = read_wire_packets(entry["wire_file"].as_str().unwrap());
    let frames = entry["frames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| Frame {
            au_sha: f["depacketized_sha256"].as_str().unwrap().to_owned(),
            packets: f["packets"].as_array().unwrap().clone(),
        })
        .collect();
    (wire, frames)
}

fn assert_frame(packet: &Packet, codec: &str, frame: &Frame) {
    assert_eq!(packet.media_type, MediaType::Video);
    assert_eq!(packet.codec, PacketCodec::Video(codec_of(codec)));
    assert_eq!(packet.user_id, Some(USER));
    assert_eq!(packet.ssrc, MEDIA_SSRC);
    assert!(packet.marker);
    assert_eq!(
        common::sha256_hex(&packet.payload),
        frame.au_sha,
        "{codec} AU"
    );
    let last = frame.packets.last().unwrap();
    assert_eq!(
        packet.payload_type as u64,
        last["payload_type"].as_u64().unwrap()
    );
    assert_eq!(packet.sequence as u64, last["sequence"].as_u64().unwrap());
    assert_eq!(packet.timestamp as u64, last["timestamp"].as_u64().unwrap());
    // TWCC transport sequence (extension 5) of the frame's last packet
    let twcc = packet
        .rtp_extensions
        .iter()
        .find(|e| e.id == 5)
        .unwrap_or_else(|| panic!("{codec}: no TWCC extension"));
    let expected = last["transport_sequence"].as_u64().unwrap() as u16;
    assert_eq!(
        twcc.data,
        expected.to_be_bytes(),
        "{codec} transport sequence"
    );
    // and the sender's one-byte extensions, as recorded for packets that list them
    if let Some(listed) = last["extensions"].as_array() {
        let got: Vec<(u8, String)> = packet
            .rtp_extensions
            .iter()
            .map(|e| (e.id, hex(&e.data)))
            .collect();
        let want: Vec<(u8, String)> = listed
            .iter()
            .map(|e| {
                (
                    e[0].as_u64().unwrap() as u8,
                    e[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(got, want, "{codec} rtp_extensions");
    }
    assert_eq!(
        packet.rtp_packets.len(),
        frame.packets.len(),
        "{codec} rtp_packets"
    );
    assert!(packet.rtp_packets.iter().all(|p| !p.rtx));
}

#[test]
fn test_every_codec_assembles_one_au_per_frame() {
    for codec in CODEC_NAMES {
        let rig = rig(|_| {});
        // the whole capture in one datagram batch, as the UDP dispatcher delivers them; its RTX
        // tail repeats packets that were already delivered and must not produce further frames
        let (wire, frames) = capture_with_rtx(codec);
        let out = feed(&rig, &wire);
        assert_eq!(out.len(), frames.len(), "{codec}");
        for (packet, frame) in out.iter().zip(&frames) {
            assert_frame(packet, codec, frame);
        }
        let stats = rig.rx.stats();
        assert_eq!(stats["video_frames_completed"], frames.len() as u64);
        assert!(!stats.contains_key("rtp_decrypt_failures"));
    }
}

#[test]
fn test_one_packet_per_batch_gives_the_same_frames() {
    let rig = rig(|_| {});
    let (wire, frames) = capture("h264");
    let mut out = Vec::new();
    for datagram in &wire {
        out.extend(feed(&rig, std::slice::from_ref(datagram)));
    }
    assert_eq!(out.len(), 2);
    for (packet, frame) in out.iter().zip(&frames) {
        assert_frame(packet, "h264", frame);
    }
}

#[test]
fn test_reordered_packets_still_assemble_the_frame() {
    for codec in CODEC_NAMES {
        let rig = rig(|_| {});
        let (mut wire, frames) = capture(codec);
        // swap neighbours, keeping the first packet in place: it fixes where the reorder buffer
        // starts (as in the Python library), so a packet older than it is dropped as stale
        for pair in wire[1..].chunks_mut(2) {
            pair.reverse();
        }
        let out = feed(&rig, &wire);
        assert_eq!(out.len(), frames.len(), "{codec}");
        for (packet, frame) in out.iter().zip(&frames) {
            assert_eq!(common::sha256_hex(&packet.payload), frame.au_sha, "{codec}");
        }
    }
}

#[test]
fn test_lost_packet_is_nacked_and_the_late_packet_completes_the_frame() {
    let rig = rig(|_| {});
    let (wire, frames) = capture("h264");
    let lost = 3usize;
    let lost_sequence = frames[0].packets[lost]["sequence"].as_u64().unwrap() as u16;
    let mut held_back: Vec<Vec<u8>> = wire.clone();
    let late = held_back.remove(lost);

    let out = feed(&rig, &held_back);
    // the key frame waits for the gap; the delta frame behind it waits too (ordered delivery)
    assert!(
        out.is_empty(),
        "{:?}",
        out.iter()
            .map(|p| (p.sequence, p.payload.len()))
            .collect::<Vec<_>>()
    );
    let nacks: Vec<_> = rig
        .sent_rtcp()
        .into_iter()
        .filter(|(t, ..)| *t == RTCP_RTP_FEEDBACK)
        .collect();
    assert!(!nacks.is_empty());
    let (_, count, sender, payload) = &nacks[0];
    assert_eq!((*count, *sender), (1, OWN_SSRC));
    assert_eq!(
        u32::from_be_bytes(payload[..4].try_into().unwrap()),
        MEDIA_SSRC
    );
    assert_eq!(
        u16::from_be_bytes(payload[4..6].try_into().unwrap()),
        lost_sequence
    );

    let out = feed(&rig, &[late]);
    assert_eq!(out.len(), 2);
    for (packet, frame) in out.iter().zip(&frames) {
        assert_eq!(common::sha256_hex(&packet.payload), frame.au_sha);
    }
    assert_eq!(rig.rx.stats()["video_nack_sequences_added"], 1);
}

#[test]
fn test_unanswered_nack_is_repeated_on_the_retry_timer() {
    let rig = rig(|_| {});
    let (wire, _) = capture("vp8");
    let mut held_back = wire.clone();
    held_back.remove(2);
    feed(&rig, &held_back);
    let first = rig
        .sent_rtcp()
        .into_iter()
        .filter(|(t, ..)| *t == RTCP_RTP_FEEDBACK)
        .count();
    assert_eq!(first, 1);
    // not due yet
    rig.rx.retry_pending_nacks(Instant::now());
    assert!(
        rig.sent_rtcp()
            .iter()
            .all(|(t, ..)| *t != RTCP_RTP_FEEDBACK)
    );
    rig.rx
        .retry_pending_nacks(Instant::now() + Duration::from_millis(150));
    let again = rig.sent_rtcp();
    assert_eq!(
        again
            .iter()
            .filter(|(t, ..)| *t == RTCP_RTP_FEEDBACK)
            .count(),
        1
    );
}

#[test]
fn test_rtx_retransmission_fills_the_gap() {
    let rig = rig(|_| {});
    let (wire, frames) = capture("h264");
    let lost = 5usize;
    let mut held_back = wire.clone();
    let lost_wire = held_back.remove(lost);
    assert!(feed(&rig, &held_back).is_empty());

    // the lost packet comes back as an RTX packet: 2 bytes of original sequence + the payload
    let (_, decoded) = crypto().decrypt_rtp_packets(&[lost_wire]).unwrap();
    let original = &decoded[0];
    let mut payload = original.packet.sequence.to_be_bytes().to_vec();
    payload.extend_from_slice(&original.media_payload);
    let header = RtpPacket::new_header(106, 1, original.packet.timestamp, RTX_SSRC, false, false);
    let rtx = crypto().encrypt_rtp(&header, &payload).unwrap();

    let out = feed(&rig, &[rtx]);
    assert_eq!(out.len(), 2);
    for (packet, frame) in out.iter().zip(&frames) {
        assert_eq!(common::sha256_hex(&packet.payload), frame.au_sha);
    }
    assert_eq!(out[0].rtp_packets.iter().filter(|p| p.rtx).count(), 1);
    assert!(out[0].rtp_packets.iter().all(|p| p.ssrc == MEDIA_SSRC));
}

#[test]
fn test_startup_pli_is_sent_once_for_h26x_only() {
    for (codec, expect) in [
        ("h264", true),
        ("h265", true),
        ("vp8", false),
        ("av1", false),
    ] {
        let rig = rig(|_| {});
        let (wire, frames) = capture(codec);
        let out = feed(&rig, &wire);
        assert_eq!(out.len(), frames.len());
        let plis: Vec<_> = rig
            .sent_rtcp()
            .into_iter()
            .filter(|(t, ..)| *t == RTCP_PAYLOAD_FEEDBACK)
            .collect();
        assert_eq!(plis.len(), usize::from(expect), "{codec}");
        if expect {
            let (_, count, sender, payload) = &plis[0];
            assert_eq!((*count, *sender), (1, OWN_SSRC));
            assert_eq!(
                u32::from_be_bytes(payload[..4].try_into().unwrap()),
                MEDIA_SSRC
            );
        }
    }
}

#[test]
fn test_explicit_pli_and_empty_receiver_report() {
    let rig = rig(|_| {});
    rig.rx.send_pli(MEDIA_SSRC).unwrap();
    rig.rx.send_receiver_report(&[]).unwrap();
    let sent = rig.sent_rtcp();
    assert_eq!(sent[0].0, RTCP_PAYLOAD_FEEDBACK);
    assert_eq!(
        (sent[1].0, sent[1].1, sent[1].2),
        (RTCP_RECEIVER_REPORT, 0, OWN_SSRC)
    );
    assert!(sent[1].3.is_empty());
}

#[test]
fn test_first_video_packet_gets_a_receiver_report_at_once() {
    let rig = rig(|_| {});
    let (wire, _) = capture("vp8");
    rig.rx.process_batch(&wire[..3], Instant::now());
    let reports: Vec<_> = rig
        .sent_rtcp()
        .into_iter()
        .filter(|(t, ..)| *t == RTCP_RECEIVER_REPORT)
        .collect();
    assert_eq!(reports.len(), 1);
}

#[test]
fn test_receiver_report_is_sent_once_per_interval() {
    let rig = rig(|_| {});
    let (wire, _) = capture("vp8");
    let base = Instant::now() + Duration::from_secs(5);
    rig.rx.process_batch(&wire[..10], base);
    let reports = |rig: &Rig| {
        rig.sent_rtcp()
            .into_iter()
            .filter(|(t, ..)| *t == RTCP_RECEIVER_REPORT)
            .collect::<Vec<_>>()
    };
    let first = reports(&rig);
    assert_eq!(first.len(), 1);
    let (_, count, sender, body) = &first[0];
    assert_eq!((*count, *sender), (1, OWN_SSRC));
    assert_eq!(
        u32::from_be_bytes(body[..4].try_into().unwrap()),
        MEDIA_SSRC
    );
    // within the same second: no second report
    rig.rx
        .process_batch(&wire[10..20], base + Duration::from_millis(500));
    assert!(reports(&rig).is_empty());
    rig.rx
        .process_batch(&wire[20..30], base + Duration::from_millis(1100));
    assert_eq!(reports(&rig).len(), 1);
}

// ---------------------------------------------------------------------------------------------
// hand-built packets

/// One encrypted RTP packet with a one-word header extension carrying a TWCC sequence number.
fn rtp(
    pt: u8,
    seq: u16,
    ts: u32,
    ssrc: u32,
    marker: bool,
    twcc: Option<u16>,
    payload: &[u8],
) -> Vec<u8> {
    let header = RtpPacket::new_header(pt, seq, ts, ssrc, marker, twcc.is_some());
    let mut body = Vec::new();
    if let Some(twcc) = twcc {
        body.extend_from_slice(&[0x51, (twcc >> 8) as u8, twcc as u8, 0]);
    }
    body.extend_from_slice(payload);
    crypto().encrypt_rtp(&header, &body).unwrap()
}

/// A single-NAL H264 frame: an IDR slice NAL.
fn idr(tag: u8) -> Vec<u8> {
    vec![0x65, 0x88, 0x84, tag]
}

#[test]
fn test_sequence_wraparound_keeps_frames_in_order() {
    let rig = rig(|_| {});
    let seqs = [65_533u16, 65_534, 65_535, 0, 1, 2];
    let wire: Vec<Vec<u8>> = seqs
        .iter()
        .enumerate()
        .map(|(i, &s)| {
            rtp(
                105,
                s,
                1000 + i as u32 * 3000,
                MEDIA_SSRC,
                true,
                Some(s),
                &idr(i as u8),
            )
        })
        .collect();
    let out = feed(&rig, &wire);
    assert_eq!(out.len(), 6);
    assert_eq!(out.iter().map(|p| p.sequence).collect::<Vec<_>>(), seqs);
    for (i, packet) in out.iter().enumerate() {
        assert_eq!(packet.payload, [&[0, 0, 0, 1][..], &idr(i as u8)].concat());
    }
    let stats = rig.rx.stats();
    assert_eq!(stats["video_sequence_wraps"], 1);
    assert!(!stats.contains_key("video_sequence_gaps"));
}

#[test]
fn test_wraparound_gap_is_nacked_across_the_boundary() {
    let rig = rig(|_| {});
    let a = rtp(105, 65_535, 1000, MEDIA_SSRC, true, None, &idr(0));
    let c = rtp(105, 1, 7000, MEDIA_SSRC, true, None, &idr(2));
    feed(&rig, &[a]);
    rig.sink.take();
    let out = feed(&rig, &[c]);
    assert!(out.is_empty(), "frame 2 waits for sequence 0");
    let nacks: Vec<_> = rig
        .sent_rtcp()
        .into_iter()
        .filter(|(t, ..)| *t == RTCP_RTP_FEEDBACK)
        .collect();
    assert_eq!(nacks.len(), 1);
    assert_eq!(u16::from_be_bytes(nacks[0].3[4..6].try_into().unwrap()), 0);
}

#[test]
fn test_unknown_payload_type_is_ignored() {
    let rig = rig(|_| {});
    let out = feed(&rig, &[rtp(99, 1, 0, MEDIA_SSRC, true, None, &idr(0))]);
    assert!(out.is_empty());
    assert_eq!(rig.rx.stats()["unknown_payload_type"], 1);
}

#[test]
fn test_packet_with_a_wrong_key_is_counted_not_fatal() {
    let rig = rig(|_| {});
    let other = SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &[9; 32])
        .unwrap();
    let header = RtpPacket::new_header(105, 1, 0, MEDIA_SSRC, true, false);
    let bad = other.encrypt_rtp(&header, &idr(0)).unwrap();
    let good = rtp(105, 1, 0, MEDIA_SSRC, true, None, &idr(1));
    let out = feed(&rig, &[bad, good]);
    assert_eq!(out.len(), 1);
    assert_eq!(rig.rx.stats()["rtp_decrypt_failures"], 1);
}

#[test]
fn test_malformed_h264_payload_drops_only_that_frame() {
    let rig = rig(|_| {});
    // FU-A with nal type 0 is invalid; the next frame must still come through
    let bad = rtp(105, 1, 1000, MEDIA_SSRC, true, None, &[0x7C, 0x80]);
    let good = rtp(105, 2, 4000, MEDIA_SSRC, true, None, &idr(5));
    let out = feed(&rig, &[bad, good]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].sequence, 2);
}

#[test]
fn test_dave_shaped_video_frame_without_a_key_is_held_back() {
    let rig = rig(|_| {});
    // an H264 NAL followed by a well-formed DAVE supplemental block
    let mut nal = idr(1);
    nal.extend_from_slice(&[0xAA; 8]); // tag
    nal.push(0x05); // nonce
    nal.push(12); // supplemental size (8 + 1 + 0 + 1 + 2)
    nal.extend_from_slice(&[0xFA, 0xFA]);
    let out = feed(&rig, &[rtp(105, 1, 1000, MEDIA_SSRC, true, None, &nal)]);
    assert!(
        out.is_empty(),
        "an undecryptable DAVE frame must not reach the application"
    );
}

#[test]
fn test_audio_packet_gets_user_extensions_and_speaking_flags() {
    let rig = rig(|_| {});
    let opus = vec![0x78, 1, 2, 3, 4];
    // audio level extension: id 1 -> bytes: header (id 1, len 1) value 0x85, pad
    let header = RtpPacket::new_header(120, 9, 4800, AUDIO_SSRC, false, true);
    let mut body = vec![0x10, 0x85, 0, 0];
    body.extend_from_slice(&opus);
    let wire = crypto().encrypt_rtp(&header, &body).unwrap();
    let out = feed(&rig, &[wire]);
    assert_eq!(out.len(), 1);
    let p = &out[0];
    assert_eq!(p.media_type, MediaType::Audio);
    assert_eq!(p.codec, PacketCodec::Opus);
    assert_eq!(
        (p.user_id, p.ssrc, p.sequence, p.timestamp),
        (Some(USER), AUDIO_SSRC, 9, 4800)
    );
    assert_eq!(p.payload, opus);
    assert_eq!(p.speaking_flags, Some(speaking::VOICE));
    assert_eq!(
        (p.audio_level, p.audio_voice_activity),
        (Some(5), Some(true))
    );
    assert_eq!(p.payload_type, 120);
}

#[test]
fn test_unencrypted_opus_silence_is_flagged_not_speaking() {
    let rig = rig(|_| {});
    let out = feed(
        &rig,
        &[rtp(
            120,
            1,
            960,
            AUDIO_SSRC,
            false,
            None,
            &[0xF8, 0xFF, 0xFE],
        )],
    );
    assert_eq!(out[0].speaking_flags, Some(speaking::NONE));
    assert_eq!(out[0].payload, [0xF8, 0xFF, 0xFE]);
}

#[test]
fn test_packet_from_an_unknown_ssrc_has_no_user() {
    let rig = rig(|_| {});
    let out = feed(&rig, &[rtp(105, 1, 0, 5555, true, None, &idr(0))]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].user_id, None);
    rig.ssrcs.set_speaking(5555, 8);
    let out = feed(&rig, &[rtp(105, 2, 0, 5555, true, None, &idr(0))]);
    assert_eq!(out[0].user_id, Some(8));
}

#[test]
fn test_rtp_tap_sees_every_packet_with_its_twcc_extension() {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let rig = rig(move |cfg| {
        cfg.rtp_tap = Some(Arc::new(move |o| {
            let twcc = o
                .rtp_extensions
                .iter()
                .find(|e| e.id == 5)
                .map(|e| u16::from_be_bytes([e.data[0], e.data[1]]));
            seen2.lock().push((o.ssrc, o.sequence, twcc));
        }));
    });
    let wire = [
        rtp(105, 10, 0, MEDIA_SSRC, true, Some(500), &idr(0)),
        rtp(99, 11, 0, MEDIA_SSRC, true, Some(501), &idr(1)), // unknown payload type: tapped anyway
    ];
    feed(&rig, &wire);
    assert_eq!(
        *seen.lock(),
        [(MEDIA_SSRC, 10, Some(500)), (MEDIA_SSRC, 11, Some(501))]
    );
}

#[test]
fn test_forget_other_payload_types_drops_the_old_codec_state() {
    let rig = rig(|_| {});
    // H264 (105) packets 1 and 3 arrive: 2 is missing, the buffer holds 3
    feed(
        &rig,
        &[
            rtp(105, 1, 0, MEDIA_SSRC, true, None, &idr(0)),
            rtp(105, 3, 9000, MEDIA_SSRC, true, None, &idr(2)),
        ],
    );
    // codec switch to VP8 (107): forget the H264 state, then H264 sequence 2 arriving is a fresh start
    rig.rx.forget_other_payload_types(MEDIA_SSRC, 107);
    let out = feed(&rig, &[rtp(105, 2, 6000, MEDIA_SSRC, true, None, &idr(1))]);
    assert_eq!(
        out.len(),
        1,
        "a new reorder buffer starts at the first packet it sees"
    );
}

#[test]
fn test_sender_report_gives_rtcp_time() {
    let rig = rig(|_| {});
    let (wire, frames) = capture("h264");
    // SR for the media SSRC: NTP = 1_000_000 s since 1970, RTP ts = the first frame's timestamp
    let ts = frames[0].packets[0]["timestamp"].as_u64().unwrap() as u32;
    let mut sender_info = Vec::new();
    sender_info.extend_from_slice(&(2_208_988_800u32 + 1_000_000).to_be_bytes());
    sender_info.extend_from_slice(&0u32.to_be_bytes());
    sender_info.extend_from_slice(&ts.to_be_bytes());
    sender_info.extend_from_slice(&[0; 8]);
    let header = {
        let mut h = vec![0x80, 200, 0, 6];
        h.extend_from_slice(&MEDIA_SSRC.to_be_bytes());
        h
    };
    let sr = crypto().encrypt_rtcp(&header, &sender_info).unwrap();
    rig.rx.handle_rtcp(&sr);
    let out = feed(&rig, &wire);
    let t0 = out[0].rtcp_time.expect("rtcp time from the sender report");
    assert!((t0 - 1_000_000.0).abs() < 1e-6, "{t0}");
    let delta_ts = out[1].timestamp.wrapping_sub(ts);
    let t1 = out[1].rtcp_time.unwrap();
    assert!((t1 - (1_000_000.0 + f64::from(delta_ts) / 90_000.0)).abs() < 1e-6);
}

#[test]
fn test_without_a_sender_report_abs_send_time_is_used() {
    let rig = rig(|_| {});
    let (wire, _) = capture("vp8");
    // the fixture packets carry abs-send-time (ext 3) from the native sender
    let out = feed(&rig, &wire);
    let t = out[0].rtcp_time.expect("abs-send-time fallback");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    // within a 64 s wrap window of now
    assert!((t - now).abs() <= 64.0, "{t} vs {now}");
}

#[test]
fn test_rtx_disabled_ignores_rtx_payload_types_and_skips_reordering() {
    let rig = rig(|cfg| cfg.rtx_enabled = false);
    let (wire, frames) = capture("h264");
    let mut shuffled = wire.clone();
    shuffled.swap(0, 1);
    let out = feed(&rig, &shuffled);
    // no reorder buffer: the swapped head breaks that frame, later frames still come out
    assert!(out.len() < frames.len() + 1);
    assert!(
        rig.sent_rtcp()
            .iter()
            .all(|(t, ..)| *t != RTCP_RTP_FEEDBACK)
    );
}

#[test]
fn test_codec_specs_define_the_payload_types() {
    // an AV1-only client does not know H264's payload type
    let rig = rig(|cfg| {
        cfg.codecs = video_codec_list(&[VoiceCodec::AV1], false);
    });
    let (wire, _) = capture("h264");
    assert!(feed(&rig, &wire).is_empty());
    assert!(rig.rx.stats()["unknown_payload_type"] > 0);
}
