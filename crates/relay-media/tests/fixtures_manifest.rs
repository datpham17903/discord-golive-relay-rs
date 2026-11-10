//! Smoke test for the T4 binary fixtures (`tools/gen_fixtures.py`): the manifest parses and every
//! listed file exists with the recorded size and a plausible bitstream prefix.

use std::path::PathBuf;

use serde_json::Value;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codecs")
}

fn manifest() -> Vec<Value> {
    let text = std::fs::read_to_string(dir().join("manifest.json")).expect("manifest.json");
    let v: Value = serde_json::from_str(&text).expect("manifest parses");
    v["files"].as_array().expect("files array").clone()
}

#[test]
fn test_manifest_files_exist_with_recorded_size() {
    let files = manifest();
    assert!(!files.is_empty());
    for e in &files {
        let name = e["file"].as_str().unwrap();
        let data = std::fs::read(dir().join(name)).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert!(!data.is_empty(), "{name} is empty");
        assert_eq!(
            data.len() as u64,
            e["bytes"].as_u64().unwrap(),
            "{name} size"
        );
        assert!(
            e["codec"].is_string() && e["keyframe"].is_boolean(),
            "{name} fields"
        );
        match e["codec"].as_str().unwrap() {
            "h264" | "h265" => assert_eq!(data[..4], [0, 0, 0, 1], "{name} Annex-B start code"),
            "av1" => assert_eq!(data[..2], [0x12, 0x00], "{name} temporal delimiter"),
            _ => {}
        }
    }
}

#[test]
fn test_every_video_codec_has_key_and_delta_at_both_sizes() {
    let files = manifest();
    for codec in ["h264", "h265", "av1", "vp8", "vp9"] {
        for (w, h) in [(1920u64, 1080u64), (1280, 720)] {
            let hits: Vec<bool> = files
                .iter()
                .filter(|e| {
                    e["media"] == "video"
                        && e["codec"] == codec
                        && e["width"] == w
                        && e["height"] == h
                })
                .map(|e| e["keyframe"].as_bool().unwrap())
                .collect();
            assert_eq!(
                hits.iter().filter(|k| **k).count(),
                1,
                "{codec} {w}x{h} keyframes"
            );
            assert_eq!(
                hits.iter().filter(|k| !**k).count(),
                1,
                "{codec} {w}x{h} deltas"
            );
        }
    }
}

#[test]
fn test_opus_samples_cover_durations_and_channels() {
    let files = manifest();
    let opus: Vec<&Value> = files.iter().filter(|e| e["media"] == "audio").collect();
    for ch in ["mono", "stereo"] {
        for ms in [10.0, 20.0, 40.0] {
            assert!(
                opus.iter()
                    .any(|e| e["channels"] == ch && e["duration_ms"] == ms),
                "opus {ch} {ms} ms"
            );
        }
    }
    assert!(
        opus.iter().any(|e| e["frames"].as_u64().unwrap() > 1),
        "multi-frame opus packet"
    );
}
