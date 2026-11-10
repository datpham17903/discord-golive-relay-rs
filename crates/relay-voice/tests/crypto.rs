//! Transport crypto: round trips for each supported mode, wire layout, tamper detection and the
//! RTP parse + decrypt batch helper. (Byte-for-byte agreement with the native module is checked in
//! `native_crosscheck.rs`.)

use relay_voice::rtp::{PacketError, RtpPacket};
use relay_voice::transport::packets::{SendError, SharedTransportCrypto, parse_rtp_extensions};
use relay_voice::transport::{CryptoError, TransportCrypto, TransportEncryptionMode};

const MODES: [TransportEncryptionMode; 2] = [
    TransportEncryptionMode::AeadAes256GcmRtpSize,
    TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize,
];
const KEY: [u8; 32] = [7; 32];
const TAG_LEN: usize = 16;

fn header(sequence: u16) -> Vec<u8> {
    RtpPacket::new_header(
        120,
        sequence,
        960 * sequence as u32,
        0xCAFE_F00D,
        false,
        false,
    )
}

#[test]
fn test_mode_names_round_trip() {
    for (mode, name) in [
        (
            TransportEncryptionMode::AeadAes256GcmRtpSize,
            "aead_aes256_gcm_rtpsize",
        ),
        (
            TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize,
            "aead_xchacha20_poly1305_rtpsize",
        ),
    ] {
        assert_eq!(mode.as_str(), name);
        assert_eq!(name.parse::<TransportEncryptionMode>(), Ok(mode));
    }
}

#[test]
fn test_unsupported_mode_name_is_rejected() {
    let err = "xsalsa20_poly1305"
        .parse::<TransportEncryptionMode>()
        .unwrap_err();
    assert_eq!(
        err,
        CryptoError::UnsupportedMode("xsalsa20_poly1305".to_owned())
    );
    assert!(err.to_string().contains("xsalsa20_poly1305"));
}

#[test]
fn test_encrypt_then_decrypt_round_trip_in_every_mode() {
    for mode in MODES {
        let mut sender = TransportCrypto::new(mode, &KEY).unwrap();
        let mut receiver = TransportCrypto::new(mode, &KEY).unwrap();
        for (index, payload) in [&b""[..], b"x", b"opus frame bytes", &[0xAB; 1100][..]]
            .into_iter()
            .enumerate()
        {
            let header = header(index as u16);
            let wire = sender.encrypt_rtp(&header, payload).unwrap();
            let packet = RtpPacket::parse(&wire).unwrap();
            assert_eq!(packet.header, header);
            assert_eq!(receiver.decrypt_rtp(&packet).unwrap(), payload, "{mode:?}");
        }
    }
}

#[test]
fn test_wire_layout_is_header_ciphertext_tag_nonce_and_nonce_counts_up() {
    for mode in MODES {
        let mut crypto = TransportCrypto::new(mode, &KEY).unwrap();
        let header = header(1);
        let payload = b"0123456789";
        let first = crypto.encrypt_rtp(&header, payload).unwrap();
        let second = crypto.encrypt_rtp(&header, payload).unwrap();

        assert_eq!(first.len(), header.len() + payload.len() + TAG_LEN + 4);
        assert_eq!(&first[..header.len()], &header[..]);
        assert_eq!(first[first.len() - 4..], 0x8000_0000u32.to_le_bytes());
        assert_eq!(second[second.len() - 4..], 0x8000_0001u32.to_le_bytes());
        // a fresh nonce changes the ciphertext even for identical input
        assert_ne!(first[header.len()..], second[header.len()..]);
    }
}

#[test]
fn test_tampering_or_wrong_key_fails_to_decrypt() {
    for mode in MODES {
        let mut sender = TransportCrypto::new(mode, &KEY).unwrap();
        let mut receiver = TransportCrypto::new(mode, &KEY).unwrap();
        let wire = sender.encrypt_rtp(&header(5), b"secret payload").unwrap();

        // flipped ciphertext byte, flipped header byte (the header is authenticated), changed nonce
        for position in [14, 3, wire.len() - 1] {
            let mut bad = wire.clone();
            bad[position] ^= 0x01;
            let packet = RtpPacket::parse(&bad).unwrap();
            assert_eq!(
                receiver.decrypt_rtp(&packet),
                Err(CryptoError::DecryptFailed),
                "{mode:?} byte {position}"
            );
        }

        let mut other = TransportCrypto::new(mode, &[8; 32]).unwrap();
        let packet = RtpPacket::parse(&wire).unwrap();
        assert_eq!(other.decrypt_rtp(&packet), Err(CryptoError::DecryptFailed));
    }
}

#[test]
fn test_key_must_be_32_bytes() {
    for mode in MODES {
        for length in [0usize, 16, 31, 33, 64] {
            assert_eq!(
                TransportCrypto::new(mode, &vec![1; length]).err(),
                Some(CryptoError::InvalidKeyLength(length))
            );
        }
    }
}

#[test]
fn test_rtp_parse_rejects_short_and_non_rtp_packets() {
    assert_eq!(RtpPacket::parse(&[0x80; 8]), Err(PacketError::TooShort));
    let mut wire = vec![0u8; 20];
    wire[0] = 0x40; // version 1
    assert_eq!(RtpPacket::parse(&wire), Err(PacketError::InvalidVersion(1)));
    // extension flag set but the extension body is cut off
    let mut wire = vec![0u8; 20];
    wire[0] = 0x90;
    wire[14..16].copy_from_slice(&10u16.to_be_bytes());
    assert_eq!(
        RtpPacket::parse(&wire),
        Err(PacketError::TruncatedExtension)
    );
}

#[test]
fn test_batch_decrypt_splits_extension_and_media_and_counts_failures() {
    let shared =
        SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &KEY).unwrap();

    // extended header: one 4-byte extension word (id 5, 2 bytes: transport sequence 0x0102)
    let extension = [0x51, 0x01, 0x02, 0x00];
    let extended = RtpPacket::new_header(105, 9, 90_000, 0x1234, true, true);
    let mut plain = extension.to_vec();
    plain.extend_from_slice(b"media bytes");
    let extended_wire = shared.encrypt_rtp(&extended, &plain).unwrap();

    let plain_wire = shared.encrypt_rtp(&header(1), b"no extension").unwrap();
    let mut corrupt = plain_wire.clone();
    corrupt[13] ^= 0xFF;
    let garbage = vec![0x01; 30];

    let (failed, decoded) = shared
        .decrypt_rtp_packets(&[plain_wire, extended_wire, corrupt, garbage])
        .unwrap();
    assert_eq!(
        failed, 2,
        "one corrupt ciphertext and one unparsable datagram"
    );
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].index, 0);
    assert_eq!(decoded[0].media_payload, b"no extension");
    assert!(decoded[0].extension_payload.is_empty() && decoded[0].extensions.is_empty());

    assert_eq!(decoded[1].index, 1);
    assert_eq!(decoded[1].media_payload, b"media bytes");
    assert_eq!(decoded[1].extension_payload, extension);
    assert_eq!(decoded[1].extensions, [(5u8, vec![0x01, 0x02])]);
    assert!(decoded[1].packet.marker && decoded[1].packet.extended);
    assert_eq!(decoded[1].packet.payload_type, 105);
    assert_eq!(decoded[1].packet.sequence, 9);
    assert_eq!(decoded[1].packet.timestamp, 90_000);
}

#[test]
fn test_padded_packets_are_stripped_and_bad_padding_is_a_failure() {
    let shared =
        SharedTransportCrypto::new(TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize, &KEY)
            .unwrap();
    let mut padded = header(2);
    padded[0] |= 0x20;

    let good = shared
        .encrypt_rtp(&padded, &[b'a', b'b', b'c', 0, 0, 3])
        .unwrap();
    let zero_len = shared.encrypt_rtp(&padded, &[b'a', 0]).unwrap();
    let too_long = shared.encrypt_rtp(&padded, &[b'a', 9]).unwrap();

    let (failed, decoded) = shared
        .decrypt_rtp_packets(&[good, zero_len, too_long])
        .unwrap();
    assert_eq!(failed, 2);
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].media_payload, b"abc");
    assert!(decoded[0].packet.padded);
}

#[test]
fn test_one_and_two_byte_extension_profiles() {
    let one_byte = [0x10, 0xAA, 0x00, 0x21, 0x01, 0x02, 0xF0, 0xFF];
    assert_eq!(
        parse_rtp_extensions(0xBEDE, &one_byte),
        [(1u8, vec![0xAA]), (2, vec![0x01, 0x02])]
    );
    let two_byte = [0x03, 0x02, 0xDE, 0xAD, 0x00, 0x04, 0x00];
    assert_eq!(
        parse_rtp_extensions(0x1000, &two_byte),
        [(3u8, vec![0xDE, 0xAD]), (4, vec![])]
    );
    assert!(parse_rtp_extensions(0x1234, &one_byte).is_empty());
    // truncated element stops parsing instead of reading past the end
    assert!(parse_rtp_extensions(0xBEDE, &[0x1F, 0x00]).is_empty());
}

#[test]
fn test_rtcp_rejects_short_packets() {
    let shared =
        SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &KEY).unwrap();
    assert!(matches!(
        shared.decrypt_rtcp_packets(&[0x81, 205, 0, 1, 0, 0, 0, 1], 8),
        Err(SendError::Value(_))
    ));
    assert!(matches!(
        shared.decrypt_rtcp_packets(&[0x81; 20], 0),
        Err(SendError::Value(_))
    ));
}

#[test]
fn test_key_never_appears_in_debug_output() {
    let shared =
        SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &[0x5A; 32])
            .unwrap();
    let shown = format!("{shared:?}");
    assert!(shown.contains("***"));
    assert!(!shown.to_lowercase().contains("5a"));
}
