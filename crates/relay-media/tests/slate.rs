//! Port of `$PY/tests/media/test_slate.py`. The Rust slate is a hand-written Constrained Baseline
//! bitstream (gray Y=U=V=128, Python's libx264 slate is Y=110), so `test_every_variant_decodes_with_fresh_decoder`
//! (needs a decoder) is replaced by `tools/verify_slate.py`, which decodes the committed fixtures with PyAV and
//! writes `fixtures/slate/slate_decoded.json`; `test_fixtures_match_pyav_decode` checks that manifest.
//! Extra tests cover the SPS (dimensions, cropping, level, low-delay VUI).

use relay_media::h264::{
    NAL_SPS, ensure_low_delay_vui, has_parameter_sets, idr_pic_id, is_idr, nal_units,
    parse_sps_full, sps_dimensions,
};
use relay_media::slate::{SlateError, SlateGop, build_slate};
use serde_json::Value;

const FIX: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/slate/");

fn slate() -> SlateGop {
    build_slate(160, 96, 15, 1.0, 300).unwrap()
}

#[test]
fn test_variants_distinct_first_idr() {
    let slate = slate();
    assert!(slate.variants.len() >= 2);
    let ids: Vec<Option<u32>> = slate.variants.iter().map(|v| idr_pic_id(&v[0])).collect();
    assert!(!ids.contains(&None));
    let mut uniq = ids.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), ids.len());
    assert!(
        slate
            .variants
            .iter()
            .all(|v| is_idr(&v[0]) && has_parameter_sets(&v[0]))
    );
    assert!(
        slate
            .variants
            .iter()
            .all(|v| v[1..].iter().all(|a| !is_idr(a)))
    );
}

#[test]
fn test_variant_ids_are_zero_and_one() {
    let slate = slate();
    assert_eq!(slate.variants.len(), 2);
    assert_eq!(idr_pic_id(&slate.variants[0][0]), Some(0));
    assert_eq!(idr_pic_id(&slate.variants[1][0]), Some(1));
}

#[test]
fn test_every_variant_parses_with_expected_size() {
    // Decoder half of the Python test is in tools/verify_slate.py; here only the parsed geometry.
    let slate = slate();
    assert_eq!((slate.width, slate.height), (160, 96));
    for v in &slate.variants {
        assert_eq!(v.len(), 15);
        assert_eq!(sps_dimensions(&v[0]), Some((160, 96)));
        for au in &v[1..] {
            assert_eq!(nal_units(au).len(), 1);
        }
    }
}

#[test]
fn test_gop_duration_within_limit() {
    let slate = slate();
    assert!((slate.frame_time_ms - 1000.0 / 15.0).abs() < 1e-9);
    for v in &slate.variants {
        assert!(v.len() as f64 * slate.frame_time_ms <= 1000.0 + 1e-6);
    }
}

#[test]
fn test_gop_length_follows_interval_and_cap() {
    let len = |fps, interval| build_slate(64, 64, fps, interval, 300).unwrap().variants[0].len();
    assert_eq!(len(15, 1.0), 15);
    assert_eq!(len(30, 0.5), 15);
    assert_eq!(len(30, 4.0), 30); // capped at 1 s
    assert_eq!(len(30, 0.01), 1); // at least one frame
}

#[test]
fn test_next_au_picks_different_id() {
    let slate = slate();
    for v in &slate.variants {
        let last = idr_pic_id(&v[0]);
        let gop: Vec<&[u8]> = slate.next_au(last).unwrap().collect();
        assert_ne!(idr_pic_id(gop[0]), last);
    }
    let first: Vec<&[u8]> = slate.next_au(None).unwrap().collect();
    let expected: Vec<&[u8]> = slate.variants[0].iter().map(Vec::as_slice).collect();
    assert_eq!(first, expected);
}

#[test]
fn test_next_au_without_distinct_variant_errors() {
    let mut slate = slate();
    slate.variants.truncate(1);
    let last = idr_pic_id(&slate.variants[0][0]);
    assert!(matches!(
        slate.next_au(last),
        Err(SlateError::NoDistinctVariant)
    ));
}

#[test]
fn test_odd_canvas_rejected() {
    assert_eq!(
        build_slate(161, 96, 15, 1.0, 300).unwrap_err(),
        SlateError::OddCanvas
    );
    assert_eq!(
        build_slate(160, 95, 15, 1.0, 300).unwrap_err(),
        SlateError::OddCanvas
    );
}

#[test]
fn test_zero_params_rejected() {
    for (w, h, fps) in [(0, 96, 15), (160, 0, 15), (160, 96, 0)] {
        assert_eq!(
            build_slate(w, h, fps, 1.0, 300).unwrap_err(),
            SlateError::InvalidParams
        );
    }
}

#[test]
fn test_sps_dimensions_with_cropping() {
    for (w, h) in [
        (1920, 1080),
        (1280, 720),
        (202, 118),
        (16, 16),
        (2, 2),
        (426, 240),
    ] {
        let slate = build_slate(w, h, 30, 1.0, 300).unwrap();
        for v in &slate.variants {
            assert_eq!(sps_dimensions(&v[0]), Some((w, h)), "{w}x{h}");
        }
    }
}

fn sps_of(au: &[u8]) -> Vec<u8> {
    nal_units(au)
        .into_iter()
        .find(|&(t, _)| t == NAL_SPS)
        .unwrap()
        .1
        .to_vec()
}

#[test]
fn test_sps_is_baseline_with_low_delay_vui() {
    let slate = build_slate(1920, 1080, 30, 1.0, 300).unwrap();
    let nal = sps_of(&slate.variants[0][0]);
    assert_eq!(nal[1], 66);
    assert_eq!(nal[2], 0xC0);
    let info = parse_sps_full(&nal).unwrap();
    assert!(info.restriction);
    assert_eq!(
        (info.reorder, info.dec_buf, info.max_num_ref_frames),
        (0, 1, 1)
    );
    assert_eq!((info.width, info.height), (1920, 1080));
}

#[test]
fn test_ensure_low_delay_vui_is_noop_on_slate_idr() {
    for (w, h, fps) in [(160, 96, 15), (1920, 1080, 30), (202, 118, 25)] {
        let slate = build_slate(w, h, fps, 1.0, 300).unwrap();
        for v in &slate.variants {
            assert_eq!(ensure_low_delay_vui(&v[0]), v[0]);
        }
    }
}

#[test]
fn test_level_idc_by_frame_size_and_rate() {
    let level = |w, h, fps| sps_of(&build_slate(w, h, fps, 1.0, 300).unwrap().variants[0][0])[3];
    assert_eq!(level(160, 96, 15), 31);
    assert_eq!(level(1280, 720, 30), 31);
    assert_eq!(level(1280, 720, 60), 40);
    assert_eq!(level(1920, 1080, 30), 40);
    assert_eq!(level(1920, 1080, 60), 42);
    assert_eq!(level(3840, 2160, 30), 51);
    assert_eq!(level(3840, 2160, 60), 52);
}

#[test]
fn test_idr_has_one_byte_per_macroblock_and_p_is_skip_run() {
    let slate = build_slate(64, 48, 10, 1.0, 300).unwrap(); // 4x3 = 12 MBs
    let idr = nal_units(&slate.variants[0][0])
        .into_iter()
        .find(|&(t, _)| t == 5)
        .unwrap()
        .1;
    assert_eq!(idr[0], 0x65);
    assert_eq!(&idr[idr.len() - 13..idr.len() - 1], &[0x27; 12]);
    let p = &slate.variants[0][1];
    assert_eq!(p[..5], [0, 0, 0, 1, 0x41]);
}

fn read_aus(name: &str) -> Vec<Vec<u8>> {
    let data = std::fs::read(format!("{FIX}{name}")).unwrap();
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let n = u32::from_be_bytes(data[i..i + 4].try_into().unwrap()) as usize;
        out.push(data[i + 4..i + 4 + n].to_vec());
        i += 4 + n;
    }
    out
}

fn manifest() -> Vec<Value> {
    let text = std::fs::read_to_string(format!("{FIX}slate_decoded.json")).unwrap();
    serde_json::from_str::<Value>(&text).unwrap()["cases"]
        .as_array()
        .unwrap()
        .clone()
}

fn case_name(w: u64, h: u64, fps: u64) -> String {
    format!("slate_{w}x{h}_{fps}fps")
}

/// Regenerate the length-prefixed fixtures with `UPDATE_SLATE_FIXTURES=1 cargo test -p relay-media --test slate`
/// and then run `tools/verify_slate.py`.
#[test]
fn test_fixtures_match_generated_slate() {
    for (w, h, fps) in [
        (160, 96, 15),
        (202, 118, 25),
        (1280, 720, 60),
        (1920, 1080, 30),
    ] {
        let slate = build_slate(w, h, fps, 1.0, 300).unwrap();
        for (n, v) in slate.variants.iter().enumerate() {
            let name = format!(
                "{}_v{n}.bin",
                case_name(u64::from(w), u64::from(h), u64::from(fps))
            );
            if std::env::var_os("UPDATE_SLATE_FIXTURES").is_some() {
                let mut data = Vec::new();
                for au in v {
                    data.extend((au.len() as u32).to_be_bytes());
                    data.extend_from_slice(au);
                }
                std::fs::create_dir_all(FIX).unwrap();
                std::fs::write(format!("{FIX}{name}"), data).unwrap();
            }
            assert_eq!(&read_aus(&name), v, "{name}");
        }
    }
}

/// `tools/verify_slate.py` decoded every fixture with PyAV (libavcodec h264): size and plane values.
#[test]
fn test_fixtures_match_pyav_decode() {
    let cases = manifest();
    assert_eq!(cases.len(), 4);
    for c in &cases {
        let (w, h, fps) = (
            c["width"].as_u64().unwrap(),
            c["height"].as_u64().unwrap(),
            c["fps"].as_u64().unwrap(),
        );
        let slate = build_slate(w as u32, h as u32, fps as u32, 1.0, 300).unwrap();
        let variants = c["variants"].as_array().unwrap();
        assert_eq!(variants.len(), slate.variants.len());
        for (dec, v) in variants.iter().zip(&slate.variants) {
            assert_eq!(dec["frames"].as_u64().unwrap() as usize, v.len());
            assert_eq!(dec["decoded_width"].as_u64().unwrap(), w);
            assert_eq!(dec["decoded_height"].as_u64().unwrap(), h);
            for plane in ["y", "u", "v"] {
                assert_eq!(dec[plane]["min"], 128, "{plane}");
                assert_eq!(dec[plane]["max"], 128, "{plane}");
            }
        }
    }
}
