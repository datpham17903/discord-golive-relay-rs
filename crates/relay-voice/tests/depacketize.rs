//! Packetize (vendored send side) -> depacketize round trips on the T4 fixtures, plus the
//! depacketizer edge cases a lossy network produces.

mod common;

use common::{CODEC_NAMES, annex_b_nalus, codec_fixture, codec_of, rtp_manifest, sha256_hex};
use relay_media::codec::VoiceCodec;
use relay_voice::rtp::{MediaPacket, PacketError};
use relay_voice::video::{
    AV1Packetizer, H264Packetizer, H265Packetizer, VP8Packetizer, VP9Packetizer, VideoDepacketizer,
    VideoPacketizer, depacketizer_for,
};

const SSRC: u32 = 0xA1B2_C3D4;
const FRAME_MS: f64 = 1000.0 / 30.0;
const MTU: usize = 1200;

fn packetizer_for(codec: VoiceCodec) -> Box<dyn VideoPacketizer> {
    let payload_type = codec.payload_type();
    match codec {
        VoiceCodec::AV1 => Box::new(AV1Packetizer::new(SSRC, payload_type)),
        VoiceCodec::H265 => Box::new(H265Packetizer::new(SSRC, payload_type)),
        VoiceCodec::H264 => Box::new(H264Packetizer::new(SSRC, payload_type)),
        VoiceCodec::VP8 => Box::new(VP8Packetizer::new(SSRC, payload_type)),
        VoiceCodec::VP9 => Box::new(VP9Packetizer::new(SSRC, payload_type)),
    }
}

/// Feeds a frame's packets and returns the rebuilt AU; it must appear exactly on the marker packet.
fn depacketize(
    depacketizer: &mut dyn VideoDepacketizer,
    packets: &[MediaPacket],
) -> Result<Vec<u8>, PacketError> {
    let mut rebuilt = None;
    for (index, packet) in packets.iter().enumerate() {
        let out = depacketizer.push_packet(
            &packet.payload,
            packet.marker,
            packet.sequence,
            packet.timestamp,
        )?;
        if index + 1 < packets.len() {
            assert!(out.is_none(), "AU emitted before the marker packet");
        } else {
            rebuilt = out;
        }
    }
    Ok(rebuilt.expect("marker packet completes the AU"))
}

/// Native depacketizer output (recorded from the compiled Python module) for 1280x720 frames.
fn native_depacketized_sha(codec: &str, kind: &str) -> String {
    let manifest = rtp_manifest();
    let au_file = format!("{codec}_1280x720_{kind}.bin");
    manifest["codecs"][codec]["frames"]
        .as_array()
        .unwrap()
        .iter()
        .find(|frame| frame["au_file"] == au_file.as_str())
        .unwrap_or_else(|| panic!("{au_file} not in manifest"))["depacketized_sha256"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_au_matches_input(
    codec: &str,
    resolution: &str,
    kind: &str,
    input: &[u8],
    rebuilt: &[u8],
) {
    match codec {
        // Raw frames survive unchanged.
        "vp8" | "vp9" => assert_eq!(rebuilt, input, "{codec} {resolution} {kind}"),
        // Annex-B: same NAL units; start codes are normalised to 4 bytes.
        "h264" | "h265" => {
            assert_eq!(
                annex_b_nalus(rebuilt),
                annex_b_nalus(input),
                "{codec} {resolution} {kind}"
            );
            assert!(rebuilt.starts_with(&[0, 0, 0, 1]));
        }
        // AV1 rebuilds RTP-form OBUs (temporal delimiter dropped, no size field): checked against
        // the native module's output below instead of the input bytes.
        "av1" => {
            assert!(!rebuilt.is_empty());
            assert!(rebuilt.len() <= input.len(), "{codec} {resolution} {kind}");
        }
        other => panic!("unknown codec {other}"),
    }
    if resolution == "1280x720" {
        assert_eq!(
            sha256_hex(rebuilt),
            native_depacketized_sha(codec, kind),
            "{codec} {resolution} {kind} differs from the native module"
        );
    }
}

#[test]
fn test_roundtrip_key_and_delta_for_every_codec_and_size() {
    for codec_name in CODEC_NAMES {
        for resolution in ["1920x1080", "1280x720"] {
            let codec = codec_of(codec_name);
            let packetizer = packetizer_for(codec);
            let mut depacketizer = depacketizer_for(codec);
            let mut last_timestamp = None;
            // key, delta, key, delta on one packetizer/depacketizer pair (continuous sequence)
            for kind in ["key", "delta", "key", "delta"] {
                let input = codec_fixture(codec_name, resolution, kind);
                let packets = packetizer.packetize(&input, FRAME_MS).unwrap();
                assert!(!packets.is_empty());

                for (index, packet) in packets.iter().enumerate() {
                    assert_eq!(packet.marker, index + 1 == packets.len(), "marker");
                    assert_eq!(packet.payload_type, codec.payload_type());
                    assert_eq!(packet.ssrc, SSRC);
                    assert!(packet.payload.len() <= MTU, "payload exceeds the MTU");
                    assert_eq!(packet.timestamp, packets[0].timestamp);
                    if index > 0 {
                        assert_eq!(packet.sequence, packets[index - 1].sequence.wrapping_add(1));
                    }
                }
                if let Some(last) = last_timestamp {
                    assert_eq!(packets[0].timestamp, u32::wrapping_add(last, 3000));
                }
                last_timestamp = Some(packets[0].timestamp);

                let rebuilt = depacketize(depacketizer.as_mut(), &packets).unwrap();
                assert_au_matches_input(codec_name, resolution, kind, &input, &rebuilt);
            }
        }
    }
}

#[test]
fn test_h264_multi_nal_au_uses_one_packet_per_nal_with_marker_on_last() {
    let au = [
        &[0, 0, 0, 1, 0x67, 0x42, 0xC0, 0x1F, 0xAA][..],
        &[0, 0, 0, 1, 0x68, 0xCE, 0x3C, 0x80][..],
        &[0, 0, 0, 1, 0x65, 0x88, 0x84, 0x00, 0x10][..],
    ]
    .concat();
    let packets = H264Packetizer::new(SSRC, 105)
        .packetize(&au, FRAME_MS)
        .unwrap();
    assert_eq!(packets.len(), 3);
    assert_eq!(
        packets.iter().map(|p| p.marker).collect::<Vec<_>>(),
        [false, false, true]
    );
    let rebuilt = depacketize(&mut relay_voice::video::H264Depacketizer::new(), &packets).unwrap();
    assert_eq!(rebuilt, au);
}

#[test]
fn test_h264_large_nal_is_fragmented_as_fu_a() {
    let mut au = vec![0, 0, 0, 1, 0x65];
    au.extend((0..5000u32).map(|i| (i % 251) as u8 + 1));
    let packets = H264Packetizer::new(SSRC, 105)
        .packetize(&au, FRAME_MS)
        .unwrap();
    assert!(packets.len() > 1);
    assert!(packets.iter().all(|p| p.payload[0] & 0x1F == 28));
    assert_eq!(
        packets[0].payload[1] & 0xC0,
        0x80,
        "start bit on the first fragment"
    );
    assert_eq!(
        packets.last().unwrap().payload[1] & 0xC0,
        0x40,
        "end bit on the last fragment"
    );
    let rebuilt = depacketize(&mut relay_voice::video::H264Depacketizer::new(), &packets).unwrap();
    assert_eq!(rebuilt, au);
}

#[test]
fn test_empty_frame_is_rejected_by_every_packetizer() {
    for codec_name in CODEC_NAMES {
        let packetizer = packetizer_for(codec_of(codec_name));
        assert!(
            packetizer.packetize(&[], FRAME_MS).is_err(),
            "{codec_name} accepted an empty frame"
        );
    }
}

#[test]
fn test_empty_payload_is_invalid_for_every_depacketizer() {
    for codec_name in CODEC_NAMES {
        let mut depacketizer = depacketizer_for(codec_of(codec_name));
        assert_eq!(
            depacketizer.push_packet(&[], true, 1, 1),
            Err(PacketError::InvalidPayload),
            "{codec_name}"
        );
    }
}

#[test]
fn test_lost_packet_drops_the_frame_and_the_next_frame_recovers() {
    for codec_name in CODEC_NAMES {
        let codec = codec_of(codec_name);
        let packetizer = packetizer_for(codec);
        let mut depacketizer = depacketizer_for(codec);

        let key = codec_fixture(codec_name, "1280x720", "key");
        let delta = codec_fixture(codec_name, "1280x720", "delta");
        let key_packets = packetizer.packetize(&key, FRAME_MS).unwrap();
        let delta_packets = packetizer.packetize(&delta, FRAME_MS).unwrap();
        assert!(key_packets.len() > 2);

        let mut emitted = None;
        for (index, packet) in key_packets.iter().enumerate() {
            if index == 1 {
                continue; // lost on the wire
            }
            let out = depacketizer
                .push_packet(
                    &packet.payload,
                    packet.marker,
                    packet.sequence,
                    packet.timestamp,
                )
                .unwrap();
            if out.is_some() {
                emitted = out;
            }
        }
        assert!(
            emitted.is_none(),
            "{codec_name}: a frame with a gap was delivered"
        );

        let rebuilt = depacketize(depacketizer.as_mut(), &delta_packets).unwrap();
        assert_au_matches_input(codec_name, "1280x720", "delta", &delta, &rebuilt);
    }
}

#[test]
fn test_sequence_number_wraps_from_65535_to_0() {
    for codec_name in CODEC_NAMES {
        let codec = codec_of(codec_name);
        let input = codec_fixture(codec_name, "1280x720", "key");
        let packets = packetizer_for(codec).packetize(&input, FRAME_MS).unwrap();
        assert!(packets.len() > 8);

        let mut reference = depacketizer_for(codec);
        let expected = depacketize(reference.as_mut(), &packets).unwrap();

        let mut depacketizer = depacketizer_for(codec);
        let mut rebuilt = None;
        for (index, packet) in packets.iter().enumerate() {
            let sequence = 65_530u16.wrapping_add(index as u16);
            let out = depacketizer
                .push_packet(&packet.payload, packet.marker, sequence, packet.timestamp)
                .unwrap();
            if out.is_some() {
                rebuilt = out;
            }
        }
        assert_eq!(
            rebuilt.as_deref(),
            Some(expected.as_slice()),
            "{codec_name}"
        );
    }
}

#[test]
fn test_timestamp_wraps_around_u32() {
    for codec_name in CODEC_NAMES {
        let codec = codec_of(codec_name);
        let packetizer = packetizer_for(codec);
        let mut depacketizer = depacketizer_for(codec);
        let key = codec_fixture(codec_name, "1280x720", "key");
        let delta = codec_fixture(codec_name, "1280x720", "delta");

        let mut next_timestamp = u32::MAX - 1000;
        let mut sequence = 100u16;
        for (input, kind) in [(&key, "key"), (&delta, "delta")] {
            let packets = packetizer.packetize(input, FRAME_MS).unwrap();
            let mut rebuilt = None;
            for packet in &packets {
                let out = depacketizer
                    .push_packet(&packet.payload, packet.marker, sequence, next_timestamp)
                    .unwrap();
                sequence = sequence.wrapping_add(1);
                if out.is_some() {
                    rebuilt = out;
                }
            }
            next_timestamp = next_timestamp.wrapping_add(3000); // crosses u32::MAX on the second frame
            let rebuilt = rebuilt.unwrap_or_else(|| panic!("{codec_name} {kind}: no AU"));
            assert_au_matches_input(codec_name, "1280x720", kind, input, &rebuilt);
        }
    }
}

#[test]
fn test_h264_fu_a_continuation_without_start_is_discarded() {
    let mut depacketizer = relay_voice::video::H264Depacketizer::new();
    // middle + end fragments of an FU-A whose start packet was never seen
    let middle = [0x7C, 0x05, 1, 2, 3];
    let end = [0x7C, 0x45, 4, 5, 6];
    assert_eq!(depacketizer.push_packet(&middle, false, 10, 900), Ok(None));
    assert_eq!(depacketizer.push_packet(&end, true, 11, 900), Ok(None));

    // the next, complete frame still decodes
    let single = [0x65, 9, 9, 9];
    assert_eq!(
        depacketizer.push_packet(&single, true, 12, 1800),
        Ok(Some([&[0, 0, 0, 1][..], &single[..]].concat()))
    );
}

#[test]
fn test_h264_unsupported_and_malformed_payloads_are_errors() {
    let mut depacketizer = relay_voice::video::H264Depacketizer::new();
    // type 29 = FU-B, not supported
    assert_eq!(
        depacketizer.push_packet(&[0x7D, 0, 0], true, 1, 1),
        Err(PacketError::UnsupportedCodec)
    );
    // STAP-A whose NAL length runs past the payload
    assert_eq!(
        depacketizer.push_packet(&[0x78, 0x00, 0x09, 0x67, 0x42], true, 2, 2),
        Err(PacketError::InvalidPayload)
    );
    // STAP-A with a trailing garbage byte
    assert_eq!(
        depacketizer.push_packet(&[0x78, 0x00, 0x02, 0x67, 0x42, 0xFF], true, 3, 3),
        Err(PacketError::InvalidPayload)
    );
    // FU-A with reserved NAL type 0
    assert_eq!(
        depacketizer.push_packet(&[0x7C, 0x80, 1], true, 4, 4),
        Err(PacketError::InvalidPayload)
    );
}

#[test]
fn test_h264_stap_a_aggregates_nalus() {
    let mut depacketizer = relay_voice::video::H264Depacketizer::new();
    let stap = [0x78, 0x00, 0x03, 0x67, 0x42, 0xC0, 0x00, 0x02, 0x68, 0xCE];
    let out = depacketizer
        .push_packet(&stap, true, 1, 1)
        .unwrap()
        .unwrap();
    assert_eq!(out, [0, 0, 0, 1, 0x67, 0x42, 0xC0, 0, 0, 0, 1, 0x68, 0xCE]);
}

#[test]
fn test_h265_mid_gop_delta_au_is_depacketized_without_a_keyframe() {
    // a viewer that joins mid-GOP starts with a delta AU: still rebuilt, keyframe handling is the
    // caller's job (the worker waits for an entry point)
    let packetizer = H265Packetizer::new(SSRC, 103);
    let delta = codec_fixture("h265", "1280x720", "delta");
    let packets = packetizer.packetize(&delta, FRAME_MS).unwrap();
    let mut depacketizer = depacketizer_for(VoiceCodec::H265);
    let rebuilt = depacketize(depacketizer.as_mut(), &packets).unwrap();
    assert_eq!(annex_b_nalus(&rebuilt), annex_b_nalus(&delta));
}

#[test]
fn test_packet_burst_in_one_call_sequence_yields_one_au_per_frame() {
    // several frames' packets pushed back to back (a burst after a network stall)
    for codec_name in CODEC_NAMES {
        let codec = codec_of(codec_name);
        let packetizer = packetizer_for(codec);
        let mut depacketizer = depacketizer_for(codec);
        let delta = codec_fixture(codec_name, "1280x720", "delta");
        let mut count = 0;
        for _ in 0..4 {
            for packet in packetizer.packetize(&delta, FRAME_MS).unwrap() {
                if depacketizer
                    .push_packet(
                        &packet.payload,
                        packet.marker,
                        packet.sequence,
                        packet.timestamp,
                    )
                    .unwrap()
                    .is_some()
                {
                    count += 1;
                }
            }
        }
        assert_eq!(count, 4, "{codec_name}");
    }
}
