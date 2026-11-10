//! Send pipeline (packetize -> RTP extensions -> encrypt -> sink): stream bookkeeping, RTX/NACK,
//! pacing, and an end-to-end loopback through the UDP transport.

mod common;

use std::io;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

use common::{CODEC_NAMES, CollectSink, annex_b_nalus, codec_fixture, codec_of};
use relay_media::codec::VoiceCodec;
use relay_voice::transport::TransportEncryptionMode;
use relay_voice::transport::net::UdpTransport;
use relay_voice::transport::packets::{DecryptedRtpPacket, SendError, SharedTransportCrypto};
use relay_voice::video::{PacketSink, VideoSendPipeline, depacketizer_for};

const SSRC: u32 = 0x1000_0001;
const RTX_SSRC: u32 = 0x1000_0002;
const FRAME_MS: f64 = 1000.0 / 30.0;

fn crypto() -> SharedTransportCrypto {
    SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &[3; 32]).unwrap()
}

fn pipeline_for(codec: VoiceCodec, rtx: bool) -> VideoSendPipeline {
    let pipeline = VideoSendPipeline::new();
    let payload_type = codec.payload_type() as u16;
    pipeline
        .configure_stream(
            codec,
            SSRC,
            payload_type,
            rtx.then_some(payload_type + 1),
            if rtx { RTX_SSRC } else { 0 },
            "100",
        )
        .unwrap();
    pipeline
}

fn send(
    pipeline: &VideoSendPipeline,
    crypto: &SharedTransportCrypto,
    sink: &dyn PacketSink,
    frame: &[u8],
) -> relay_voice::video::VideoSendSummary {
    pipeline
        .send_video_frame(crypto, sink, SSRC, frame, None, FRAME_MS, "screen", 0, 0.0)
        .unwrap()
}

fn decrypt(crypto: &SharedTransportCrypto, wire: &[Vec<u8>]) -> Vec<DecryptedRtpPacket> {
    let (failed, decoded) = crypto.decrypt_rtp_packets(wire).unwrap();
    assert_eq!(failed, 0);
    decoded
}

fn transport_sequence(packet: &DecryptedRtpPacket) -> u16 {
    let (_, data) = packet.extensions.iter().find(|(id, _)| *id == 5).unwrap();
    u16::from_be_bytes([data[0], data[1]])
}

#[test]
fn test_every_codec_goes_through_the_pipeline_and_back() {
    for codec_name in CODEC_NAMES {
        let codec = codec_of(codec_name);
        let pipeline = pipeline_for(codec, false);
        let crypto = crypto();
        let sink = CollectSink::default();
        let mut depacketizer = depacketizer_for(codec);
        let mut previous_transport = None;
        let mut previous_ts = None;

        for kind in ["key", "delta", "delta"] {
            let input = codec_fixture(codec_name, "1920x1080", kind);
            let summary = send(&pipeline, &crypto, &sink, &input);
            let packets = decrypt(&crypto, &sink.take());
            assert_eq!(packets.len(), summary.sent);
            assert_eq!(
                summary.octets,
                packets.iter().map(|p| p.media_payload.len()).sum::<usize>()
                    + packets
                        .iter()
                        .map(|p| p.extension_payload.len())
                        .sum::<usize>()
                    + packets.len() * (12 + 4 + 16 + 4),
                "octets = header(12+4 ext hdr) + extension + payload + tag(16) + nonce(4)"
            );

            let mut rebuilt = None;
            for (index, packet) in packets.iter().enumerate() {
                assert_eq!(packet.packet.ssrc, SSRC);
                assert_eq!(packet.packet.payload_type, codec.payload_type());
                assert!(packet.packet.extended);
                assert_eq!(packet.packet.extension_profile, 0xBEDE);
                assert_eq!(packet.packet.marker, index + 1 == packets.len());
                if let Some(previous) = previous_transport {
                    assert_eq!(transport_sequence(packet), u16::wrapping_add(previous, 1));
                }
                previous_transport = Some(transport_sequence(packet));
                let out = depacketizer
                    .push_packet(
                        &packet.media_payload,
                        packet.packet.marker,
                        packet.packet.sequence,
                        packet.packet.timestamp,
                    )
                    .unwrap();
                if out.is_some() {
                    rebuilt = out;
                }
            }
            assert_eq!(summary.transport_sequence, previous_transport.unwrap());
            assert_eq!(
                summary.last_sequence,
                Some(packets.last().unwrap().packet.sequence)
            );
            assert_eq!(summary.rtp_timestamp, packets[0].packet.timestamp);
            if let Some(previous) = previous_ts {
                assert_eq!(summary.rtp_timestamp, u32::wrapping_add(previous, 3000));
            }
            previous_ts = Some(summary.rtp_timestamp);

            let rebuilt = rebuilt.unwrap_or_else(|| panic!("{codec_name} {kind}: no AU"));
            match codec_name {
                "vp8" | "vp9" => assert_eq!(rebuilt, input),
                "h264" | "h265" => assert_eq!(annex_b_nalus(&rebuilt), annex_b_nalus(&input)),
                _ => assert!(!rebuilt.is_empty()),
            }
        }
        // next frame's timestamp is exposed without sending
        assert_eq!(
            pipeline.current_timestamp(SSRC).unwrap(),
            u32::wrapping_add(previous_ts.unwrap(), 3000)
        );
    }
}

#[test]
fn test_stream_bookkeeping() {
    let pipeline = VideoSendPipeline::new();
    assert!(!pipeline.has_stream(SSRC).unwrap());
    assert!(matches!(
        pipeline.current_timestamp(SSRC),
        Err(SendError::Value(_))
    ));

    pipeline
        .configure_stream(VoiceCodec::H264, SSRC, 105, Some(106), RTX_SSRC, "100")
        .unwrap();
    pipeline
        .configure_stream(VoiceCodec::VP8, 7, 107, None, 0, "101")
        .unwrap();
    assert!(pipeline.has_stream(SSRC).unwrap() && pipeline.has_stream(7).unwrap());

    pipeline.retain_streams(&[7]).unwrap();
    assert!(!pipeline.has_stream(SSRC).unwrap() && pipeline.has_stream(7).unwrap());
    pipeline.clear().unwrap();
    assert!(!pipeline.has_stream(7).unwrap());

    let crypto = crypto();
    let sink = CollectSink::default();
    let err = pipeline
        .send_video_frame(&crypto, &sink, 99, b"x", None, FRAME_MS, "screen", 0, 0.0)
        .unwrap_err();
    assert!(err.to_string().contains("Unknown video stream SSRC: 99"));
}

#[test]
fn test_payload_types_above_127_are_rejected() {
    let pipeline = VideoSendPipeline::new();
    let err = pipeline
        .configure_stream(VoiceCodec::VP8, SSRC, 128, None, 0, "100")
        .unwrap_err();
    assert!(err.to_string().contains("payload_type"));
    let err = pipeline
        .configure_stream(VoiceCodec::VP8, SSRC, 107, Some(200), RTX_SSRC, "100")
        .unwrap_err();
    assert!(err.to_string().contains("rtx_payload_type"));
    assert!(!pipeline.has_stream(SSRC).unwrap());
}

#[test]
fn test_reconfiguring_same_codec_keeps_state_and_changing_it_resets() {
    let pipeline = pipeline_for(VoiceCodec::VP8, true);
    let crypto = crypto();
    let sink = CollectSink::default();
    let frame = codec_fixture("vp8", "1280x720", "delta");

    let first = send(&pipeline, &crypto, &sink, &frame);
    // same codec + payload type: only RTX/RID change, sequence numbers carry on
    pipeline
        .configure_stream(VoiceCodec::VP8, SSRC, 107, Some(108), RTX_SSRC, "101")
        .unwrap();
    let second = send(&pipeline, &crypto, &sink, &frame);
    assert_eq!(
        second
            .last_sequence
            .unwrap()
            .wrapping_sub(first.last_sequence.unwrap()) as usize,
        second.sent
    );
    let packets = decrypt(&crypto, &sink.take());
    let last = packets.last().unwrap();
    let (_, rid) = last.extensions.iter().find(|(id, _)| *id == 11).unwrap();
    assert_eq!(rid, b"101");

    // different payload type: a fresh packetizer (new random sequence / timestamp)
    pipeline
        .configure_stream(VoiceCodec::VP8, SSRC, 109, None, 0, "100")
        .unwrap();
    let third = send(&pipeline, &crypto, &sink, &frame);
    assert_eq!(decrypt(&crypto, &sink.take())[0].packet.payload_type, 109);
    assert_ne!(
        third
            .last_sequence
            .unwrap()
            .wrapping_sub(second.last_sequence.unwrap()) as usize,
        third.sent,
        "a new stream does not continue the old sequence (1 in 65536 flake)"
    );
}

#[test]
fn test_rtx_resends_cached_packet_with_original_sequence_prefix() {
    let pipeline = pipeline_for(VoiceCodec::H264, true);
    let crypto = crypto();
    let sink = CollectSink::default();
    send(
        &pipeline,
        &crypto,
        &sink,
        &codec_fixture("h264", "1280x720", "delta"),
    );
    let media = decrypt(&crypto, &sink.take());
    assert!(media.len() > 3);
    let wanted = &media[2];

    let summary = pipeline
        .send_video_rtx_packet(&crypto, &sink, SSRC, wanted.packet.sequence, "screen")
        .unwrap();
    assert!(summary.sent);
    assert_eq!((summary.rtx_ssrc, summary.rtx_sequence), (RTX_SSRC, 1));
    let rtx = decrypt(&crypto, &sink.take());
    assert_eq!(rtx.len(), 1);
    let rtx = &rtx[0];
    assert_eq!(rtx.packet.payload_type, VoiceCodec::H264.rtx_payload_type());
    assert_eq!(rtx.packet.ssrc, RTX_SSRC);
    assert_eq!(rtx.packet.timestamp, wanted.packet.timestamp);
    assert_eq!(rtx.packet.marker, wanted.packet.marker);
    assert_eq!(rtx.media_payload[..2], wanted.packet.sequence.to_be_bytes());
    assert_eq!(rtx.media_payload[2..], wanted.media_payload[..]);
    assert_eq!(transport_sequence(rtx), summary.transport_sequence);
    assert!(
        rtx.extensions
            .iter()
            .any(|(id, data)| *id == 12 && data == b"100")
    );
    assert!(rtx.extensions.iter().all(|(id, _)| *id != 11));

    // a sequence we never sent
    let none = pipeline
        .send_video_rtx_packet(
            &crypto,
            &sink,
            SSRC,
            wanted.packet.sequence.wrapping_add(500),
            "screen",
        )
        .unwrap();
    assert!(!none.sent);
    assert!(sink.take().is_empty());
}

#[test]
fn test_rtx_is_inert_without_rtx_payload_type_or_ssrc() {
    for (rtx_pt, rtx_ssrc) in [(None, RTX_SSRC), (Some(108u16), 0)] {
        let pipeline = VideoSendPipeline::new();
        pipeline
            .configure_stream(VoiceCodec::VP8, SSRC, 107, rtx_pt, rtx_ssrc, "100")
            .unwrap();
        let crypto = crypto();
        let sink = CollectSink::default();
        let summary = send(
            &pipeline,
            &crypto,
            &sink,
            &codec_fixture("vp8", "1280x720", "delta"),
        );
        sink.take();
        let result = pipeline
            .send_video_rtx_packet(
                &crypto,
                &sink,
                SSRC,
                summary.last_sequence.unwrap(),
                "screen",
            )
            .unwrap();
        assert!(!result.sent);
        assert!(sink.take().is_empty());
    }
}

#[test]
fn test_rtx_history_is_bounded() {
    let pipeline = pipeline_for(VoiceCodec::VP8, true);
    let crypto = crypto();
    let sink = CollectSink::default();
    // tiny VP8 frames: one packet each
    let frame = [
        0x10, 0x02, 0x00, 0x9D, 0x01, 0x2A, 0x10, 0x00, 0x10, 0x00, 1, 2, 3,
    ];
    let first = send(&pipeline, &crypto, &sink, &frame)
        .last_sequence
        .unwrap();
    let mut last = first;
    for _ in 0..16_400 {
        last = send(&pipeline, &crypto, &sink, &frame)
            .last_sequence
            .unwrap();
        sink.take();
    }
    // 16 384 packets are kept: the oldest ones are gone, recent ones can still be resent
    assert!(
        !pipeline
            .send_video_rtx_packet(&crypto, &sink, SSRC, first, "screen")
            .unwrap()
            .sent
    );
    assert!(
        pipeline
            .send_video_rtx_packet(&crypto, &sink, SSRC, last, "screen")
            .unwrap()
            .sent
    );
}

#[test]
fn test_nack_resends_pid_and_blp_bits_and_skips_unknown_packets() {
    let pipeline = pipeline_for(VoiceCodec::VP9, true);
    let crypto = crypto();
    let sink = CollectSink::default();
    send(
        &pipeline,
        &crypto,
        &sink,
        &codec_fixture("vp9", "1280x720", "key"),
    );
    let media = decrypt(&crypto, &sink.take());
    assert!(media.len() > 4);
    let first = media[0].packet.sequence;

    // media SSRC + (PID = first, BLP bits 0 and 2) + (PID = never sent)
    let mut payload = SSRC.to_be_bytes().to_vec();
    payload.extend_from_slice(&first.to_be_bytes());
    payload.extend_from_slice(&0b101u16.to_be_bytes());
    payload.extend_from_slice(&first.wrapping_add(900).to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());

    let summary = pipeline
        .send_video_rtx_for_nack(&crypto, &sink, &payload, "screen", 0, 0.0)
        .unwrap();
    assert_eq!(summary.resent, 3);
    assert_eq!(
        (
            summary.media_ssrc,
            summary.rtx_ssrc,
            summary.last_rtx_sequence
        ),
        (SSRC, RTX_SSRC, 3)
    );
    let rtx = decrypt(&crypto, &sink.take());
    let originals: Vec<[u8; 2]> = rtx
        .iter()
        .map(|p| p.media_payload[..2].try_into().unwrap())
        .collect();
    assert_eq!(
        originals,
        [
            first.to_be_bytes(),
            first.wrapping_add(1).to_be_bytes(),
            first.wrapping_add(3).to_be_bytes()
        ]
    );
    assert_eq!(
        summary.transport_sequence,
        transport_sequence(rtx.last().unwrap())
    );

    // too short, or for an SSRC we do not send: nothing goes out
    let short = pipeline
        .send_video_rtx_for_nack(&crypto, &sink, &[0; 7], "screen", 0, 0.0)
        .unwrap();
    assert_eq!(short.resent, 0);
    let mut unknown = 0xDEAD_BEEFu32.to_be_bytes().to_vec();
    unknown.extend_from_slice(&[0, 1, 0, 0]);
    let unknown = pipeline
        .send_video_rtx_for_nack(&crypto, &sink, &unknown, "screen", 0, 0.0)
        .unwrap();
    assert_eq!((unknown.resent, unknown.media_ssrc), (0, 0xDEAD_BEEF));
    assert!(sink.take().is_empty());
}

#[test]
fn test_burst_pacing_spreads_packets_over_time() {
    let pipeline = pipeline_for(VoiceCodec::VP8, false);
    let crypto = crypto();
    let sink = CollectSink::default();
    let frame = codec_fixture("vp8", "1280x720", "key");

    let started = Instant::now();
    let summary = pipeline
        .send_video_frame(
            &crypto, &sink, SSRC, &frame, None, FRAME_MS, "screen", 15, 0.02,
        )
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(summary.sent, 45);
    // bursts of 15: waits before packet 15 (20 ms) and packet 30 (40 ms)
    assert!(
        elapsed >= Duration::from_millis(40),
        "paced send took {elapsed:?}"
    );
    assert_eq!(sink.take().len(), 45);
}

struct FailingSink;

impl PacketSink for FailingSink {
    fn send_packet(&self, _packet: &[u8]) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "socket gone"))
    }
}

#[test]
fn test_sink_errors_surface_as_send_errors() {
    let pipeline = pipeline_for(VoiceCodec::VP8, false);
    let err = pipeline
        .send_video_frame(
            &crypto(),
            &FailingSink,
            SSRC,
            &codec_fixture("vp8", "1280x720", "delta"),
            None,
            FRAME_MS,
            "screen",
            0,
            0.0,
        )
        .unwrap_err();
    assert!(matches!(err, SendError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe));
}

#[test]
fn test_empty_frame_is_an_error_not_a_panic() {
    let pipeline = pipeline_for(VoiceCodec::H264, false);
    let sink = CollectSink::default();
    let err = pipeline
        .send_video_frame(
            &crypto(),
            &sink,
            SSRC,
            &[],
            None,
            FRAME_MS,
            "screen",
            0,
            0.0,
        )
        .unwrap_err();
    assert!(matches!(err, SendError::Value(_)));
    assert!(sink.take().is_empty());
}

#[test]
fn test_pipeline_to_udp_loopback_and_back() {
    let server = UdpSocket::bind("127.0.0.1:0").unwrap();
    server
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let port = server.local_addr().unwrap().port();
    // read concurrently: the whole keyframe is larger than a default socket receive buffer
    let reader = std::thread::spawn(move || {
        let mut wire = Vec::new();
        let mut buffer = [0u8; 2048];
        while let Ok((length, _)) = server.recv_from(&mut buffer) {
            wire.push(buffer[..length].to_vec());
        }
        wire
    });
    let transport = UdpTransport::new(None, None).unwrap();
    transport.connect("127.0.0.1", port).unwrap();

    let pipeline = pipeline_for(VoiceCodec::VP8, false);
    let crypto = crypto();
    let frame = codec_fixture("vp8", "1280x720", "key");
    let summary = pipeline
        .send_video_frame(
            &crypto, &transport, SSRC, &frame, None, FRAME_MS, "screen", 0, 0.0,
        )
        .unwrap();
    assert_eq!(transport.stats().packets_sent, summary.sent);

    let wire = reader.join().unwrap();
    assert_eq!(wire.len(), summary.sent);
    let packets = decrypt(&crypto, &wire);
    let mut depacketizer = depacketizer_for(VoiceCodec::VP8);
    let rebuilt = packets
        .iter()
        .filter_map(|p| {
            depacketizer
                .push_packet(
                    &p.media_payload,
                    p.packet.marker,
                    p.packet.sequence,
                    p.packet.timestamp,
                )
                .unwrap()
        })
        .last()
        .unwrap();
    assert_eq!(rebuilt, frame);
}
