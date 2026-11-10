//! Port of the `opus_packet_info` assertions in `$PY/tests/adapter/test_adapter_fence.py`
//! plus coverage of every TOC branch of `opus.py`.

use relay_media::opus::{Channels, Mode, OPUS_SILENCE, opus_packet_info};

fn info(payload: &[u8]) -> Option<(f64, &'static str, &'static str, u32)> {
    opus_packet_info(payload).map(|(ms, ch, mode, frames)| (ms, ch.as_str(), mode.as_str(), frames))
}

#[test]
fn test_opus_packet_info_parses_toc() {
    assert_eq!(info(&[0xFC]), Some((20.0, "stereo", "celt", 1)));
    assert_eq!(info(&[0xF0]), Some((10.0, "mono", "celt", 1))); // config 30
    assert_eq!(info(&[0x18]), Some((60.0, "mono", "silk", 1)));
    assert_eq!(info(&[0x7B, 0x03]), Some((60.0, "mono", "hybrid", 3)));
    assert_eq!(info(b"\xf8\xff\xfe"), Some((20.0, "mono", "celt", 1)));
    assert_eq!(info(b""), None);
    assert_eq!(info(&[0x03]), None);
}

#[test]
fn test_opus_silence_constant() {
    assert_eq!(OPUS_SILENCE, [0xF8, 0xFF, 0xFE]);
    assert_eq!(info(&OPUS_SILENCE), Some((20.0, "mono", "celt", 1)));
}

#[test]
fn test_enum_string_values() {
    assert_eq!(Channels::Mono.as_str(), "mono");
    assert_eq!(Channels::Stereo.as_str(), "stereo");
    assert_eq!(Mode::Silk.as_str(), "silk");
    assert_eq!(Mode::Hybrid.as_str(), "hybrid");
    assert_eq!(Mode::Celt.as_str(), "celt");
}

// Cases used by the worker audio-passthrough test (`test_worker.py`
// `test_audio_passthrough_drops_and_counts_unsupported_packets`).
#[test]
fn test_worker_passthrough_packets() {
    assert_eq!(info(&[0xFC, 1, 2]), Some((20.0, "stereo", "celt", 1)));
    assert_eq!(info(&[0xF4, 1]), Some((10.0, "stereo", "celt", 1)));
    assert_eq!(info(&[0x14, 1]), Some((40.0, "stereo", "silk", 1)));
    assert_eq!(info(&[0xFD, 1, 2]), Some((40.0, "stereo", "celt", 2)));
    assert_eq!(info(&[0x7C, 1]), Some((20.0, "stereo", "hybrid", 1)));
}

#[test]
fn test_every_config_duration_and_mode() {
    // RFC 6716 3.1 table: config -> (mode, frame ms).
    let silk = [10.0, 20.0, 40.0, 60.0];
    let celt = [2.5, 5.0, 10.0, 20.0];
    for config in 0u8..32 {
        let toc = config << 3;
        let (mode, ms) = match config {
            0..=11 => ("silk", silk[usize::from(config & 3)]),
            12..=13 => ("hybrid", 10.0),
            14..=15 => ("hybrid", 20.0),
            _ => ("celt", celt[usize::from(config & 3)]),
        };
        assert_eq!(info(&[toc]), Some((ms, "mono", mode, 1)), "config {config}");
        assert_eq!(
            info(&[toc | 4]),
            Some((ms, "stereo", mode, 1)),
            "config {config} stereo"
        );
    }
}

#[test]
fn test_frame_count_codes() {
    // code 0: one frame; code 1 and 2: two frames; code 3: count in the next byte (low 6 bits).
    assert_eq!(info(&[0xF8]), Some((20.0, "mono", "celt", 1)));
    assert_eq!(info(&[0xF9]), Some((40.0, "mono", "celt", 2)));
    assert_eq!(info(&[0xFA]), Some((40.0, "mono", "celt", 2)));
    assert_eq!(info(&[0xFB]), None); // code 3 without the count byte
    assert_eq!(info(&[0xFB, 0x00]), None); // zero frames
    assert_eq!(info(&[0xFB, 0xC0]), None); // VBR/padding flags are masked: still zero frames
    assert_eq!(info(&[0xFB, 0x01]), Some((20.0, "mono", "celt", 1)));
    assert_eq!(info(&[0xFB, 0xC2]), Some((40.0, "mono", "celt", 2)));
    assert_eq!(info(&[0xFB, 0x3F]), Some((20.0 * 63.0, "mono", "celt", 63)));
    assert_eq!(info(&[0x03, 0x30]), Some((10.0 * 48.0, "mono", "silk", 48)));
    assert_eq!(info(&[0x83, 0x30]), Some((2.5 * 48.0, "mono", "celt", 48)));
}

/// Exhaustive cross-check against the Python implementation: every TOC byte, and for code 3 several
/// count bytes (`tools/gen_fixtures.py::gen_opus_table`).
#[test]
fn test_matches_python_for_every_toc_byte() {
    let table = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/opus_toc_table.tsv"
    ))
    .unwrap();
    let mut n = 0;
    for line in table.lines() {
        let cols: Vec<&str> = line.split('\t').collect();
        let payload: Vec<u8> = (0..cols[0].len() / 2)
            .map(|i| u8::from_str_radix(&cols[0][2 * i..2 * i + 2], 16).unwrap())
            .collect();
        let expected = if cols[1] == "-" {
            None
        } else {
            let mode = match cols[3] {
                "silk" => "silk",
                "hybrid" => "hybrid",
                _ => "celt",
            };
            let ch = if cols[2] == "stereo" {
                "stereo"
            } else {
                "mono"
            };
            Some((
                cols[1].parse::<f64>().unwrap(),
                ch,
                mode,
                cols[4].parse::<u32>().unwrap(),
            ))
        };
        assert_eq!(info(&payload), expected, "payload {}", cols[0]);
        n += 1;
    }
    assert_eq!(n, 256 + 64 * 6);
}
