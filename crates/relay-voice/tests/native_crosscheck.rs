//! Cross-checks against the compiled `_native_voice` module of the reference venv. The fixtures
//! under `tests/fixtures/rtp` were recorded once by `tools/gen_voice_fixtures.py`: encrypted wire
//! packets produced by the native send pipeline, plus what the native crypto / depacketizer made
//! of them. Rust must decrypt and rebuild the same bytes, and its own packetizer / crypto must
//! produce the same payloads and the same ciphertext.

mod common;

use common::{
    CODEC_NAMES, CollectSink, codec_fixture, codec_of, hex, read_wire_packets, rtp_manifest,
    sha256_hex, unhex,
};
use relay_voice::transport::packets::{DecryptedRtpPacket, SharedTransportCrypto};
use relay_voice::transport::{TransportCrypto, TransportEncryptionMode};
use relay_voice::video::{VideoSendPipeline, depacketizer_for, strip_h264_filler_nalus};
use serde_json::Value;

fn crypto_from(manifest: &Value, mode: &str) -> SharedTransportCrypto {
    let key = unhex(manifest["key_hex"].as_str().unwrap());
    SharedTransportCrypto::new(mode.parse().unwrap(), &key).unwrap()
}

fn extension(packet: &DecryptedRtpPacket, id: u8) -> Option<&[u8]> {
    packet
        .extensions
        .iter()
        .find(|(ext_id, _)| *ext_id == id)
        .map(|(_, data)| data.as_slice())
}

fn transport_sequence(packet: &DecryptedRtpPacket) -> u16 {
    let data = extension(packet, 5).expect("transport-wide sequence extension");
    u16::from_be_bytes([data[0], data[1]])
}

fn assert_packet_matches_row(packet: &DecryptedRtpPacket, row: &Value, context: &str) {
    assert_eq!(
        packet.packet.payload_type as u64, row["payload_type"],
        "{context} pt"
    );
    assert_eq!(packet.packet.marker, row["marker"], "{context} marker");
    assert_eq!(packet.packet.padded, row["padded"], "{context} padded");
    assert_eq!(
        packet.packet.sequence as u64, row["sequence"],
        "{context} seq"
    );
    assert_eq!(
        packet.packet.timestamp as u64, row["timestamp"],
        "{context} ts"
    );
    assert_eq!(packet.packet.ssrc as u64, row["ssrc"], "{context} ssrc");
    assert_eq!(
        packet.packet.extended, row["extended"],
        "{context} extended"
    );
    assert_eq!(
        packet.media_payload.len() as u64,
        row["media_len"],
        "{context} len"
    );
    assert_eq!(
        sha256_hex(&packet.media_payload),
        row["media_sha256"].as_str().unwrap(),
        "{context} payload"
    );
    assert_eq!(
        transport_sequence(packet) as u64,
        row["transport_sequence"],
        "{context} transport seq"
    );
    if let Some(expected) = row.get("extensions") {
        let expected: Vec<(u8, String)> = expected
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                (
                    pair[0].as_u64().unwrap() as u8,
                    pair[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let actual: Vec<(u8, String)> = packet
            .extensions
            .iter()
            .map(|(id, data)| (*id, hex(data)))
            .collect();
        assert_eq!(actual, expected, "{context} extensions");
    }
}

#[test]
fn test_native_wire_packets_decrypt_and_rebuild_the_same_access_units() {
    let manifest = rtp_manifest();
    for codec_name in CODEC_NAMES {
        let entry = &manifest["codecs"][codec_name];
        let crypto = crypto_from(&manifest, manifest["mode"].as_str().unwrap());
        let wire = read_wire_packets(entry["wire_file"].as_str().unwrap());
        let (failed, decrypted) = crypto.decrypt_rtp_packets(&wire).unwrap();
        assert_eq!(failed, 0, "{codec_name}: native packets failed to decrypt");
        assert_eq!(decrypted.len(), wire.len());

        let mut depacketizer = depacketizer_for(codec_of(codec_name));
        let mut cursor = 0usize;
        for frame in entry["frames"].as_array().unwrap() {
            let count = frame["packet_count"].as_u64().unwrap() as usize;
            let packets = &decrypted[cursor..cursor + count];
            cursor += count;
            let rows = frame["packets"].as_array().unwrap();
            assert_eq!(rows.len(), count);

            let mut rebuilt = None;
            for (index, (packet, row)) in packets.iter().zip(rows).enumerate() {
                let context = format!("{codec_name} {} #{index}", frame["au_file"]);
                assert_packet_matches_row(packet, row, &context);
                assert_eq!(
                    extension(packet, 10),
                    Some(&b"screen"[..]),
                    "{context} media stream type"
                );
                assert_eq!(extension(packet, 11), Some(&b"100"[..]), "{context} rid");
                assert_eq!(
                    extension(packet, 7),
                    Some(&[1u8][..]),
                    "{context} content type"
                );
                if index > 0 {
                    assert_eq!(
                        transport_sequence(packet),
                        transport_sequence(&packets[index - 1]).wrapping_add(1)
                    );
                }
                let out = depacketizer
                    .push_packet(
                        &packet.media_payload,
                        packet.packet.marker,
                        packet.packet.sequence,
                        packet.packet.timestamp,
                    )
                    .unwrap();
                if index + 1 < count {
                    assert!(out.is_none(), "{context}: early AU");
                } else {
                    rebuilt = out;
                }
            }
            assert_eq!(
                transport_sequence(&packets[count - 1]) as u64,
                frame["transport_sequence"],
                "summary transport sequence"
            );
            assert_eq!(
                packets[count - 1].packet.sequence as u64,
                frame["last_sequence"]
            );
            assert_eq!(packets[0].packet.timestamp as u64, frame["rtp_timestamp"]);

            let rebuilt = rebuilt.unwrap_or_else(|| panic!("{codec_name}: no AU"));
            assert_eq!(rebuilt.len() as u64, frame["depacketized_len"]);
            assert_eq!(
                sha256_hex(&rebuilt),
                frame["depacketized_sha256"].as_str().unwrap(),
                "{codec_name} {}: AU differs from the native depacketizer",
                frame["au_file"]
            );
            if frame["depacketized_equals_input"] == true {
                let input = std::fs::read(
                    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../relay-media/tests/fixtures/codecs")
                        .join(frame["au_file"].as_str().unwrap()),
                )
                .unwrap();
                assert_eq!(rebuilt, input, "{codec_name} AU bytes equal the T4 fixture");
            }
        }
    }
}

#[test]
fn test_rust_packetizers_emit_the_same_payloads_as_the_native_pipeline() {
    let manifest = rtp_manifest();
    for codec_name in CODEC_NAMES {
        let entry = &manifest["codecs"][codec_name];
        let codec = codec_of(codec_name);
        let pipeline = VideoSendPipeline::new();
        let ssrc = entry["media_ssrc"].as_u64().unwrap() as u32;
        let payload_type = entry["payload_type"].as_u64().unwrap() as u16;
        pipeline
            .configure_stream(
                codec,
                ssrc,
                payload_type,
                Some(payload_type + 1),
                entry["rtx_ssrc"].as_u64().unwrap() as u32,
                entry["rid"].as_str().unwrap(),
            )
            .unwrap();
        let crypto = crypto_from(&manifest, manifest["mode"].as_str().unwrap());
        let sink = CollectSink::default();

        let mut previous_timestamp = None;
        for frame in entry["frames"].as_array().unwrap() {
            let au_file = frame["au_file"].as_str().unwrap();
            let kind = if au_file.contains("_key") {
                "key"
            } else {
                "delta"
            };
            let input = codec_fixture(codec_name, "1280x720", kind);
            let summary = pipeline
                .send_video_frame(
                    &crypto,
                    &sink,
                    ssrc,
                    &input,
                    None,
                    entry["frame_time_ms"].as_f64().unwrap(),
                    entry["media_stream_type"].as_str().unwrap(),
                    0,
                    0.0,
                )
                .unwrap();
            assert_eq!(
                summary.sent as u64, frame["packet_count"],
                "{codec_name} {au_file}"
            );
            assert_eq!(summary.sent, sink.packets.lock().unwrap().len());

            let (failed, ours) = crypto.decrypt_rtp_packets(&sink.take()).unwrap();
            assert_eq!(failed, 0);
            // Sequence / timestamp / transport sequence start at random values; everything else
            // (payload bytes, markers, sizes, extension layout) is deterministic.
            for (index, (packet, row)) in ours
                .iter()
                .zip(frame["packets"].as_array().unwrap())
                .enumerate()
            {
                let context = format!("{codec_name} {au_file} #{index}");
                assert_eq!(
                    packet.packet.payload_type as u64, row["payload_type"],
                    "{context}"
                );
                assert_eq!(packet.packet.marker, row["marker"], "{context} marker");
                assert_eq!(packet.packet.ssrc as u64, row["ssrc"], "{context}");
                assert_eq!(
                    packet.media_payload.len() as u64,
                    row["media_len"],
                    "{context}"
                );
                assert_eq!(
                    sha256_hex(&packet.media_payload),
                    row["media_sha256"].as_str().unwrap(),
                    "{context} payload bytes"
                );
                assert_eq!(packet.packet.timestamp, ours[0].packet.timestamp);
                let ids: Vec<u8> = packet.extensions.iter().map(|(id, _)| *id).collect();
                assert_eq!(
                    ids,
                    [2, 3, 4, 5, 6, 7, 8, 10, 11],
                    "{context} extension ids"
                );
                assert_eq!(extension(packet, 7), Some(&[1u8][..]));
                assert_eq!(extension(packet, 10), Some(&b"screen"[..]));
                assert_eq!(extension(packet, 11), Some(&b"100"[..]));
                assert_eq!(extension(packet, 8), Some(&[0u8; 13][..]));
            }
            // the timestamp step per 33.33 ms frame is 3000 ticks in both implementations
            if let Some(previous) = previous_timestamp {
                assert_eq!(summary.rtp_timestamp, u32::wrapping_add(previous, 3000));
            }
            previous_timestamp = Some(summary.rtp_timestamp);
        }
    }
}

#[test]
fn test_native_rtx_packets_have_the_layout_the_rust_pipeline_produces() {
    let manifest = rtp_manifest();
    for codec_name in CODEC_NAMES {
        let entry = &manifest["codecs"][codec_name];
        let crypto = crypto_from(&manifest, manifest["mode"].as_str().unwrap());
        let wire = read_wire_packets(entry["wire_file"].as_str().unwrap());
        let media_count: usize = entry["frames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|frame| frame["packet_count"].as_u64().unwrap() as usize)
            .sum();
        let (_, decrypted) = crypto.decrypt_rtp_packets(&wire).unwrap();
        let media = &decrypted[..media_count];
        let rtx = &decrypted[media_count..];
        assert_eq!(rtx.len(), 1 + 3, "single RTX + three NACK retransmissions");

        let originals: Vec<u16> = std::iter::once(&entry["rtx_single"]["original_sequence"])
            .chain(entry["nack"]["original_sequences"].as_array().unwrap())
            .map(|value| value.as_u64().unwrap() as u16)
            .collect();
        let rtx_ssrc = entry["rtx_ssrc"].as_u64().unwrap() as u32;
        let first_media_seq = media[0].packet.sequence;
        for (index, (packet, original)) in rtx.iter().zip(&originals).enumerate() {
            let source = media
                .iter()
                .find(|candidate| candidate.packet.sequence == *original)
                .unwrap();
            assert_eq!(packet.packet.payload_type as u64, entry["rtx_payload_type"]);
            assert_eq!(packet.packet.ssrc, rtx_ssrc);
            assert_eq!(
                packet.packet.sequence as usize,
                index + 1,
                "RTX sequence counts from 1"
            );
            assert_eq!(packet.packet.timestamp, source.packet.timestamp);
            assert_eq!(packet.packet.marker, source.packet.marker);
            // RTX payload = original sequence (2 bytes, big endian) + original media payload
            assert_eq!(packet.media_payload[..2], original.to_be_bytes());
            assert_eq!(packet.media_payload[2..], source.media_payload[..]);
            assert!(extension(packet, 12).is_some(), "repaired-rid extension");
            assert!(extension(packet, 11).is_none());
        }
        assert_eq!(originals[0], first_media_seq.wrapping_add(1));
        assert_eq!(
            originals[1..],
            [
                first_media_seq,
                first_media_seq.wrapping_add(1),
                first_media_seq.wrapping_add(3)
            ]
        );
        assert_eq!(
            entry["nack"]["last_rtx_sequence"].as_u64().unwrap() as u16,
            rtx.last().unwrap().packet.sequence
        );
    }
}

#[test]
fn test_transport_crypto_matches_native_ciphertext_for_both_modes() {
    let manifest = rtp_manifest();
    let key = unhex(manifest["key_hex"].as_str().unwrap());
    for mode_name in ["aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize"] {
        let mode: TransportEncryptionMode = mode_name.parse().unwrap();
        assert_eq!(mode.as_str(), mode_name);
        let vectors = manifest["crypto"][mode_name].as_array().unwrap();
        let mut ours = TransportCrypto::new(mode, &key).unwrap();
        let mut theirs = TransportCrypto::new(mode, &key).unwrap();
        for vector in vectors {
            let header = unhex(vector["header_hex"].as_str().unwrap());
            let payload = unhex(vector["payload_hex"].as_str().unwrap());
            let wire = unhex(vector["wire_hex"].as_str().unwrap());

            // Rust encrypts to the exact same bytes (nonce counter starts at 0x80000000 in both)
            assert_eq!(
                ours.encrypt_rtp(&header, &payload).unwrap(),
                wire,
                "{mode_name}"
            );

            // and decrypts the native ciphertext
            let nonce: [u8; 4] = wire[wire.len() - 4..].try_into().unwrap();
            let plain = theirs
                .decrypt_packet(&header, &wire[header.len()..wire.len() - 4], &nonce)
                .unwrap();
            assert_eq!(plain, payload, "{mode_name}");
        }
    }
}

#[test]
fn test_rtcp_compound_decrypts_like_native() {
    let manifest = rtp_manifest();
    let key = unhex(manifest["key_hex"].as_str().unwrap());
    for mode_name in ["aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize"] {
        let vector = &manifest["rtcp"][mode_name];
        let crypto = SharedTransportCrypto::new(mode_name.parse().unwrap(), &key).unwrap();
        let header = unhex(vector["header_hex"].as_str().unwrap());
        let plaintext = unhex(vector["plaintext_hex"].as_str().unwrap());
        let wire = unhex(vector["wire_hex"].as_str().unwrap());

        assert_eq!(crypto.encrypt_rtcp(&header, &plaintext).unwrap(), wire);

        let packets = crypto.decrypt_rtcp_packets(&wire, 8).unwrap();
        let expected = vector["packets"].as_array().unwrap();
        assert_eq!(packets.len(), expected.len());
        for (packet, expected) in packets.iter().zip(expected) {
            assert_eq!(packet.packet_type as u64, expected["packet_type"]);
            assert_eq!(packet.count as u64, expected["count"]);
            assert_eq!(packet.sender_ssrc as u64, expected["sender_ssrc"]);
            assert_eq!(
                hex(&packet.payload),
                expected["payload_hex"].as_str().unwrap()
            );
        }
    }
}

#[test]
fn test_strip_h264_filler_nalus_matches_native() {
    let manifest = rtp_manifest();
    for case in manifest["strip_h264_filler"].as_array().unwrap() {
        let input = unhex(case["input_hex"].as_str().unwrap());
        let expected = unhex(case["output_hex"].as_str().unwrap());
        assert_eq!(
            strip_h264_filler_nalus(&input),
            expected,
            "{}",
            case["name"]
        );
    }
}
