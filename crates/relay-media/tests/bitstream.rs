//! Port of `$PY/tests/media/test_bitstream.py`. The Python tests encode with PyAV at test time; here the
//! access units come from the T4 fixtures (`tests/fixtures/codecs`, one keyframe + one delta per codec and
//! size). `probe_dimensions` has no decoder to lean on, so each fixture's parsed size is cross-checked against
//! the manifest (sizes recorded from PyAV decoding), plus hand-built headers for the branches the encoders
//! never produce (H.265 conformance-window cropping, sub-layers, AV1 timing/decoder-model fields, VP9 profiles).

use std::path::PathBuf;

use relay_media::bitstream::{is_entry_point, is_keyframe, probe_dimensions};
use relay_media::codec::{CODEC_ORDER, better, down_from, lowest, rank};
use relay_media::h264::escape;
use serde_json::Value;

struct Fixture {
    codec: &'static str,
    width: u32,
    height: u32,
    key: bool,
    data: Vec<u8>,
}

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codecs")
}

fn fixtures() -> Vec<Fixture> {
    let text = std::fs::read_to_string(dir().join("manifest.json")).unwrap();
    let v: Value = serde_json::from_str(&text).unwrap();
    let mut out = Vec::new();
    for e in v["files"].as_array().unwrap() {
        if e["media"] != "video" {
            continue;
        }
        let codec = match e["codec"].as_str().unwrap() {
            "h264" => "H264",
            "h265" => "H265",
            "av1" => "AV1",
            "vp8" => "VP8",
            "vp9" => "VP9",
            other => panic!("unexpected codec {other}"),
        };
        out.push(Fixture {
            codec,
            width: e["width"].as_u64().unwrap() as u32,
            height: e["height"].as_u64().unwrap() as u32,
            key: e["keyframe"].as_bool().unwrap(),
            data: std::fs::read(dir().join(e["file"].as_str().unwrap())).unwrap(),
        });
    }
    out
}

#[test]
fn test_entry_points_and_dimensions_per_codec() {
    for codec in CODEC_ORDER {
        let fx: Vec<_> = fixtures()
            .into_iter()
            .filter(|f| f.codec == codec)
            .collect();
        assert_eq!(fx.len(), 4, "{codec}: 2 sizes x (key + delta)");
        for f in &fx {
            let tag = format!("{codec} {}x{} key={}", f.width, f.height, f.key);
            assert_eq!(is_keyframe(codec, &f.data), f.key, "{tag}");
            assert_eq!(is_entry_point(codec, &f.data), f.key, "{tag}");
            if f.key {
                assert_eq!(
                    probe_dimensions(codec, &f.data),
                    Some((f.width, f.height)),
                    "{tag}"
                );
            }
        }
    }
}

fn h265_au(nal_type: u8, extra: &[u8]) -> Vec<u8> {
    let mut v = vec![0, 0, 0, 1, nal_type << 1, 1];
    v.extend_from_slice(extra);
    v
}

#[test]
fn test_h265_irap_without_parameter_sets_not_entry() {
    let au = h265_au(19, &[0x80; 8]); // IDR_W_RADL only
    assert!(is_keyframe("H265", &au));
    assert!(!is_entry_point("H265", &au));
}

#[test]
fn test_av1_obu_without_size_field() {
    let seq_no_size = [&[1u8 << 3][..], b"\x0a\x0b\x00"].concat(); // type 1, has_size=0, runs to end
    assert!(is_keyframe("AV1", &seq_no_size));
    assert!(is_entry_point("AV1", &seq_no_size));
    let td = b"\x12\x00"; // temporal delimiter, has_size=1, size 0
    assert!(!is_keyframe(
        "AV1",
        &[&td[..], &[(6 << 3) | 2, 1, 0]].concat()
    ));
    assert!(is_keyframe("AV1", &[&td[..], &seq_no_size].concat()));
}

#[test]
fn test_vp9_handcrafted_headers() {
    let cases: [([u8; 2], bool); 7] = [
        ([0x80, 0x00], true),  // profile 0, show_existing_frame=0, frame_type=0 (key)
        ([0x84, 0x00], false), // frame_type=1 (inter)
        ([0x88, 0x00], false), // show_existing_frame=1
        ([0xb0, 0x00], true),  // profile 3: reserved bit, then show_existing=0, key
        ([0xb2, 0x00], false), // profile 3 inter: read without the reserved bit it would look like a key
        ([0xb4, 0x00], false), // profile 3, show_existing_frame=1
        ([0x00, 0x00], false), // bad frame marker
    ];
    for (header, key) in cases {
        assert_eq!(is_keyframe("VP9", &header), key, "{header:02x?}");
        assert_eq!(is_entry_point("VP9", &header), key, "{header:02x?}");
    }
}

#[test]
fn test_order_helpers() {
    assert_eq!(CODEC_ORDER, ["AV1", "H265", "H264", "VP8", "VP9"]);
    assert!(rank("AV1").unwrap() < rank("VP9").unwrap());
    assert_eq!(better("H264", "H265").unwrap(), "H265");
    assert_eq!(lowest(&["H265", "VP8", "AV1"]).unwrap(), Some("VP8"));
    assert_eq!(lowest(&[]).unwrap(), None);
    assert_eq!(
        down_from("H265").unwrap(),
        ["H265", "H264", "VP8", "VP9"].as_slice()
    );
    let err = rank("MPEG2").unwrap_err();
    assert_eq!(err.to_string(), "unknown codec \"MPEG2\"");
    assert!(better("AV1", "MPEG2").is_err());
    assert!(lowest(&["AV1", "MPEG2"]).is_err());
    assert!(down_from("MPEG2").is_err());
}

#[test]
fn test_truncated_input_never_raises() {
    for codec in CODEC_ORDER {
        let key = fixtures()
            .into_iter()
            .find(|f| f.codec == codec && f.key)
            .unwrap();
        let mut samples: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0xff; 7], (0..40).collect()];
        for n in [1, 2, 3, 5, 9, 20, 40, 100, 200] {
            samples.push(key.data[..n].to_vec());
        }
        for au in &samples {
            is_keyframe(codec, au);
            is_entry_point(codec, au);
            probe_dimensions(codec, au);
        }
        // Every prefix of the headers must be handled too, not only a few lengths.
        for n in 0..300.min(key.data.len()) {
            is_keyframe(codec, &key.data[..n]);
            is_entry_point(codec, &key.data[..n]);
            probe_dimensions(codec, &key.data[..n]);
        }
    }
}

#[test]
fn test_unknown_codec_is_never_a_keyframe_and_has_no_dimensions() {
    let f = &fixtures()[0];
    assert!(!is_keyframe("MPEG2", &f.data));
    assert!(!is_entry_point("MPEG2", &f.data));
    assert_eq!(probe_dimensions("MPEG2", &f.data), None);
}

// ---- hand-built headers for probe_dimensions ----

#[derive(Default)]
struct Bits {
    bytes: Vec<u8>,
    n: usize,
}

impl Bits {
    fn put(&mut self, nbits: u32, v: u64) -> &mut Self {
        for i in (0..nbits).rev() {
            if self.n.is_multiple_of(8) {
                self.bytes.push(0);
            }
            if (v >> i) & 1 == 1 {
                *self.bytes.last_mut().unwrap() |= 0x80 >> (self.n % 8);
            }
            self.n += 1;
        }
        self
    }

    fn ue(&mut self, v: u64) -> &mut Self {
        let x = v + 1;
        let len = 64 - x.leading_zeros();
        self.put(len - 1, 0).put(len, x)
    }
}

struct H265Sps {
    sub_layers_minus1: u32,
    chroma: u64,
    separate: bool,
    width: u64,
    height: u64,
    crop: Option<(u64, u64, u64, u64)>, // left, right, top, bottom
}

fn h265_sps(s: &H265Sps) -> Vec<u8> {
    let mut b = Bits::default();
    b.put(4, 0).put(3, s.sub_layers_minus1 as u64).put(1, 1);
    b.put(2, 0).put(1, 0).put(5, 1).put(32, 0x6000_0000);
    b.put(48, 0x9000_0000_0000).put(8, 93);
    // sublayer 0 carries profile+level, sublayer 1 level only, the rest neither
    for i in 0..s.sub_layers_minus1 {
        b.put(1, u64::from(i == 0)).put(1, u64::from(i <= 1));
    }
    if s.sub_layers_minus1 > 0 {
        for _ in s.sub_layers_minus1..8 {
            b.put(2, 0);
        }
    }
    for i in 0..s.sub_layers_minus1 {
        if i == 0 {
            b.put(44, 0xabcdef).put(44, 0x123);
        }
        if i <= 1 {
            b.put(8, 90);
        }
    }
    b.ue(0).ue(s.chroma);
    if s.chroma == 3 {
        b.put(1, u64::from(s.separate));
    }
    b.ue(s.width).ue(s.height);
    match s.crop {
        None => {
            b.put(1, 0);
        }
        Some((l, r, t, bo)) => {
            b.put(1, 1).ue(l).ue(r).ue(t).ue(bo);
        }
    }
    b.put(1, 1).put(8, 0x80); // whatever follows (bit depths, ...)
    let mut au = vec![0, 0, 0, 1, 33 << 1, 1];
    au.extend(escape(&b.bytes));
    au
}

fn sps(sub_layers_minus1: u32, chroma: u64, width: u64, height: u64) -> H265Sps {
    H265Sps {
        sub_layers_minus1,
        chroma,
        separate: false,
        width,
        height,
        crop: None,
    }
}

#[test]
fn test_h265_probe_applies_conformance_window() {
    let au = h265_sps(&H265Sps {
        crop: Some((0, 0, 0, 4)),
        ..sps(0, 1, 1920, 1088)
    });
    assert_eq!(probe_dimensions("H265", &au), Some((1920, 1080)));
    // 4:2:0: SubWidthC = SubHeightC = 2
    let au = h265_sps(&H265Sps {
        crop: Some((1, 3, 2, 5)),
        ..sps(0, 1, 1024, 768)
    });
    assert_eq!(probe_dimensions("H265", &au), Some((1016, 754)));
    // 4:2:2: SubWidthC = 2, SubHeightC = 1
    let au = h265_sps(&H265Sps {
        crop: Some((1, 3, 2, 5)),
        ..sps(0, 2, 1024, 768)
    });
    assert_eq!(probe_dimensions("H265", &au), Some((1016, 761)));
    // 4:4:4 and monochrome: 1x1
    for chroma in [0, 3] {
        let au = h265_sps(&H265Sps {
            crop: Some((1, 3, 2, 5)),
            ..sps(0, chroma, 1024, 768)
        });
        assert_eq!(probe_dimensions("H265", &au), Some((1020, 761)), "{chroma}");
    }
    // separate_colour_plane_flag makes ChromaArrayType 0
    let au = h265_sps(&H265Sps {
        separate: true,
        crop: Some((1, 3, 2, 5)),
        ..sps(0, 3, 1024, 768)
    });
    assert_eq!(probe_dimensions("H265", &au), Some((1020, 761)));
}

#[test]
fn test_h265_probe_without_window_and_with_sub_layers() {
    assert_eq!(
        probe_dimensions("H265", &h265_sps(&sps(0, 1, 320, 240))),
        Some((320, 240))
    );
    for layers in 1..=7 {
        let au = h265_sps(&H265Sps {
            crop: Some((0, 0, 0, 2)),
            ..sps(layers, 1, 640, 368)
        });
        assert_eq!(probe_dimensions("H265", &au), Some((640, 364)), "{layers}");
    }
}

#[test]
fn test_h265_probe_rejects_bad_sps() {
    // window larger than the picture
    let au = h265_sps(&H265Sps {
        crop: Some((100, 100, 0, 0)),
        ..sps(0, 1, 320, 240)
    });
    assert_eq!(probe_dimensions("H265", &au), None);
    // zero-size picture
    assert_eq!(
        probe_dimensions("H265", &h265_sps(&sps(0, 1, 0, 240))),
        None
    );
    // no SPS NAL (an IDR only)
    assert_eq!(probe_dimensions("H265", &h265_au(19, &[0x80; 8])), None);
}

#[test]
fn test_h265_probe_finds_sps_after_vps() {
    let mut au = h265_au(32, &[0x0c, 0x01, 0xff, 0xff]); // VPS
    au.extend(h265_sps(&H265Sps {
        crop: Some((0, 0, 0, 4)),
        ..sps(0, 1, 1920, 1088)
    }));
    au.extend(h265_au(34, &[0xc0, 0xf3])); // PPS
    au.extend(h265_au(19, &[0x80; 8]));
    assert_eq!(probe_dimensions("H265", &au), Some((1920, 1080)));
}

fn leb128(mut v: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

fn obu(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![(kind << 3) | 2];
    v.extend(leb128(payload.len()));
    v.extend_from_slice(payload);
    v
}

fn av1_seq(b: &Bits) -> Vec<u8> {
    [obu(2, &[]), obu(1, &b.bytes), obu(6, &[1, 2, 3])].concat()
}

#[test]
fn test_av1_probe_reduced_still_picture_header() {
    let mut b = Bits::default();
    b.put(3, 0).put(1, 1).put(1, 1).put(5, 8);
    b.put(4, 11).put(4, 10).put(12, 1919).put(11, 1079);
    assert_eq!(probe_dimensions("AV1", &av1_seq(&b)), Some((1920, 1080)));
}

#[test]
fn test_av1_probe_timing_decoder_model_and_operating_points() {
    let mut b = Bits::default();
    b.put(3, 0).put(1, 0).put(1, 0);
    b.put(1, 1); // timing_info_present_flag
    b.put(32, 1001).put(32, 60000).put(1, 1).ue(0); // timing_info + num_ticks_per_picture_minus_1
    b.put(1, 1); // decoder_model_info_present_flag
    b.put(5, 9).put(32, 1).put(5, 4).put(5, 6); // buffer_delay_length_minus_1 = 9 (10 bit delays)
    b.put(1, 1); // initial_display_delay_present_flag
    b.put(5, 2); // 3 operating points
    // op 0: level > 7 -> tier bit; decoder model present; display delay present
    b.put(12, 0x101).put(5, 9).put(1, 0);
    b.put(1, 1).put(10, 5).put(10, 6).put(1, 0);
    b.put(1, 1).put(4, 3);
    // op 1: level <= 7 -> no tier; no decoder model; no display delay
    b.put(12, 0x103).put(5, 4);
    b.put(1, 0);
    b.put(1, 0);
    // op 2: decoder model present, display delay not present
    b.put(12, 0x107).put(5, 15).put(1, 1);
    b.put(1, 1).put(10, 7).put(10, 8).put(1, 1);
    b.put(1, 0);
    b.put(4, 12).put(4, 11).put(13, 3839).put(12, 2159);
    assert_eq!(probe_dimensions("AV1", &av1_seq(&b)), Some((3840, 2160)));
}

#[test]
fn test_av1_probe_without_sequence_header() {
    let au = [obu(2, &[]), obu(6, &[1, 2, 3])].concat();
    assert_eq!(probe_dimensions("AV1", &au), None);
    // header with no size field runs to the end of the buffer
    let mut b = Bits::default();
    b.put(3, 0).put(1, 1).put(1, 1).put(5, 8);
    b.put(4, 7).put(4, 7).put(8, 255).put(8, 99);
    let au = [&[1u8 << 3][..], &b.bytes[..]].concat();
    assert_eq!(probe_dimensions("AV1", &au), Some((256, 100)));
}

fn vp9_key(profile: u32, build_color: impl Fn(&mut Bits), w: u64, h: u64) -> Vec<u8> {
    let mut b = Bits::default();
    b.put(2, 2)
        .put(1, u64::from(profile & 1))
        .put(1, u64::from(profile >> 1));
    if profile == 3 {
        b.put(1, 0);
    }
    b.put(1, 0).put(1, 0).put(1, 1).put(1, 0); // show_existing, frame_type=key, show_frame, error_res
    b.put(24, 0x498342);
    build_color(&mut b);
    b.put(16, w - 1).put(16, h - 1).put(1, 0).put(8, 0xaa);
    b.bytes
}

#[test]
fn test_vp9_probe_across_profiles() {
    // profile 0: color_space(3) color_range(1)
    let au = vp9_key(
        0,
        |b| {
            b.put(3, 2).put(1, 0);
        },
        1280,
        720,
    );
    assert_eq!(probe_dimensions("VP9", &au), Some((1280, 720)));
    // profile 1: + subsampling_x/y + reserved
    let au = vp9_key(
        1,
        |b| {
            b.put(3, 2).put(1, 0).put(2, 0).put(1, 0);
        },
        641,
        479,
    );
    assert_eq!(probe_dimensions("VP9", &au), Some((641, 479)));
    // profile 2: ten_or_twelve_bit first
    let au = vp9_key(
        2,
        |b| {
            b.put(1, 1).put(3, 2).put(1, 1);
        },
        1920,
        1080,
    );
    assert_eq!(probe_dimensions("VP9", &au), Some((1920, 1080)));
    // profile 3: reserved bit in the head, ten_or_twelve_bit, subsampling
    let au = vp9_key(
        3,
        |b| {
            b.put(1, 0).put(3, 2).put(1, 0).put(2, 0).put(1, 0);
        },
        3840,
        2160,
    );
    assert_eq!(probe_dimensions("VP9", &au), Some((3840, 2160)));
    // RGB (color_space 7): no color_range; profiles 1/3 carry subsampling-free reserved bit
    let au = vp9_key(
        1,
        |b| {
            b.put(3, 7).put(1, 0);
        },
        64,
        48,
    );
    assert_eq!(probe_dimensions("VP9", &au), Some((64, 48)));
}

#[test]
fn test_vp9_probe_rejects_non_keyframes_and_bad_sync() {
    // inter frame, show_existing_frame, bad frame marker
    for first in [0x86u8, 0x88, 0x00] {
        let mut header = [0u8; 10];
        header[0] = first;
        assert_eq!(probe_dimensions("VP9", &header), None, "{first:#x}");
    }
    let mut au = vp9_key(
        0,
        |b| {
            b.put(3, 2).put(1, 0);
        },
        1280,
        720,
    );
    au[2] ^= 0xff; // corrupt the sync code
    assert_eq!(probe_dimensions("VP9", &au), None);
}

#[test]
fn test_vp8_and_h264_probe_dimensions() {
    for f in fixtures() {
        if f.key && (f.codec == "VP8" || f.codec == "H264") {
            assert_eq!(
                probe_dimensions(f.codec, &f.data),
                Some((f.width, f.height))
            );
        } else if !f.key && f.codec == "VP8" {
            assert_eq!(probe_dimensions("VP8", &f.data), None);
        }
    }
}
