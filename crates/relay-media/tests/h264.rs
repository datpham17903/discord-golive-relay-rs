//! Port of `$PY/tests/media/test_h264.py`. Inputs that Python builds with PyAV/libx264 are fixtures
//! generated once by `tools/gen_fixtures.py`; decoder assertions are dropped (no decoder in this crate)
//! and replaced by a byte-exact comparison with the output of the Python implementation.

use relay_media::h264::{
    BitWriter, NAL_SPS, ensure_low_delay_vui, escape, has_parameter_sets, idr_pic_id, is_idr,
    nal_units, parse_sps_full, sps_dimensions, unescape,
};

const FIX: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/");

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIX}{name}")).unwrap()
}

/// `_encode()` replacement: 40 AUs of 64x64, keyint 10.
fn encode() -> Vec<Vec<u8>> {
    let data = fixture("h264_64x64_keyint10.bin");
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let n = u32::from_be_bytes(data[i..i + 4].try_into().unwrap()) as usize;
        out.push(data[i + 4..i + 4 + n].to_vec());
        i += 4 + n;
    }
    out
}

fn idr(w: u32, h: u32) -> Vec<u8> {
    fixture(&format!("idr_{w}x{h}.bin"))
}

fn sps(au: &[u8]) -> Vec<u8> {
    nal_units(au)
        .into_iter()
        .find(|(t, _)| *t == NAL_SPS)
        .unwrap()
        .1
        .to_vec()
}

fn replace_sps(au: &[u8], rbsp: &[u8]) -> Vec<u8> {
    let old = sps(au);
    let i = au.windows(old.len()).position(|w| w == old).unwrap();
    let mut out = au[..i].to_vec();
    out.push(old[0]);
    out.extend(escape(rbsp));
    out.extend(&au[i + old.len()..]);
    out
}

/// Rebuild the SPS without VUI (`drop_vui`) or with VUI but without bitstream_restriction.
fn strip(au: &[u8], drop_vui: bool) -> Vec<u8> {
    let sps = sps(au);
    let info = parse_sps_full(&sps).unwrap();
    let rbsp = unescape(&sps[1..]);
    let mut w = BitWriter::new();
    if drop_vui {
        w.copy(&rbsp, info.vui_flag_pos);
    } else {
        w.copy(&rbsp, info.restriction_flag_pos.unwrap());
    }
    w.u(1, 0);
    replace_sps(au, &w.finish())
}

#[test]
fn test_nal_split_and_types() {
    let au = b"\x00\x00\x00\x01\x67\xaa\x00\x00\x01\x68\xbb\x00\x00\x00\x01\x65\xcc\x00";
    assert_eq!(
        nal_units(au),
        vec![
            (7, &b"\x67\xaa"[..]),
            (8, &b"\x68\xbb"[..]),
            (5, &b"\x65\xcc"[..])
        ]
    );
    assert!(is_idr(au));
    assert!(has_parameter_sets(au));
    assert!(!has_parameter_sets(b"\x00\x00\x01\x65\xcc"));
    assert!(nal_units(b"").is_empty());
}

#[test]
fn test_real_idrs_differ_and_p_frames_have_none() {
    let aus = encode();
    let idrs: Vec<&Vec<u8>> = aus.iter().filter(|a| is_idr(a)).collect();
    assert!(idrs.len() >= 3);
    assert!(idrs.iter().all(|a| has_parameter_sets(a)));
    let ids: Vec<Option<u32>> = idrs.iter().map(|a| idr_pic_id(a)).collect();
    assert!(!ids.contains(&None));
    assert!(ids.windows(2).all(|w| w[0] != w[1]));
    assert!(
        aus.iter()
            .filter(|a| !is_idr(a))
            .all(|a| idr_pic_id(a).is_none())
    );
}

#[test]
fn test_idr_without_sps_is_none() {
    let aus = encode();
    let idr = aus.iter().find(|a| is_idr(a)).unwrap();
    let mut stripped = Vec::new();
    for (t, n) in nal_units(idr) {
        if t == 5 {
            stripped.extend_from_slice(b"\x00\x00\x00\x01");
            stripped.extend_from_slice(n);
        }
    }
    assert_eq!(idr_pic_id(&stripped), None);
}

#[test]
fn test_emulation_prevention_removed() {
    assert_eq!(
        unescape(b"\x00\x00\x03\x01\x00\x00\x03"),
        b"\x00\x00\x01\x00\x00"
    );
}

#[test]
fn test_escape_inserts_emulation_prevention() {
    assert_eq!(escape(b"\x00\x00\x01\x00\x00"), b"\x00\x00\x03\x01\x00\x00");
    assert_eq!(
        escape(b"\x00\x00\x00\x00\x00\x00"),
        b"\x00\x00\x03\x00\x00\x03\x00\x00"
    );
    assert_eq!(escape(b"\x00\x00\x04"), b"\x00\x00\x04");
}

#[test]
fn test_bitwriter() {
    let mut w = BitWriter::new();
    w.u(3, 0b101);
    w.ue(0); // 1
    w.ue(1); // 010
    w.ue(2); // 011
    // 101 1 010 011 + stop bit 1 -> 1011 0100 111(0 0000)
    assert_eq!(w.finish(), vec![0b1011_0100, 0b1110_0000]);
}

#[test]
fn test_rewrite_vui_absent() {
    let au = idr(320, 240);
    assert!(parse_sps_full(&sps(&au)).unwrap().restriction);
    let stripped = strip(&au, true);
    assert_eq!(stripped, fixture("idr_320x240_no_vui.bin")); // BitWriter/escape match Python
    assert!(!parse_sps_full(&sps(&stripped)).unwrap().restriction);
    let out = ensure_low_delay_vui(&stripped);
    let info = parse_sps_full(&sps(&out)).unwrap();
    assert!(info.restriction && info.reorder == 0 && info.dec_buf == info.max_num_ref_frames);
    assert!(out.starts_with(b"\x00\x00\x00\x01") && has_parameter_sets(&out) && is_idr(&out));
    // Python asserted decoded frames equal the original's; here: byte-identical to Python's output.
    assert_eq!(out, fixture("idr_320x240_no_vui.expected.bin"));
    assert_eq!(ensure_low_delay_vui(&stripped), out); // cached path
}

#[test]
fn test_rewrite_vui_present_without_restriction() {
    let au = idr(320, 240);
    let stripped = strip(&au, false);
    assert_eq!(stripped, fixture("idr_320x240_vui_no_restriction.bin"));
    let info = parse_sps_full(&sps(&stripped)).unwrap();
    assert!(info.restriction_flag_pos.is_some() && !info.restriction);
    let out = ensure_low_delay_vui(&stripped);
    let info = parse_sps_full(&sps(&out)).unwrap();
    assert!(info.restriction && info.reorder == 0 && info.dec_buf == info.max_num_ref_frames);
    assert_eq!(out, fixture("idr_320x240_vui_no_restriction.expected.bin"));
}

#[test]
fn test_unchanged_when_restriction_present_or_no_sps() {
    let au = idr(320, 240);
    assert_eq!(ensure_low_delay_vui(&au), au);
    let aus = encode();
    let p = aus.iter().find(|a| !is_idr(a)).unwrap();
    assert_eq!(ensure_low_delay_vui(p), *p);
}

#[test]
fn test_sps_dimensions() {
    assert_eq!(sps_dimensions(&idr(320, 240)), Some((320, 240)));
    let au = idr(320, 180);
    assert_eq!(sps_dimensions(&au), Some((320, 180)));
    assert_eq!(sps_dimensions(&sps(&au)), Some((320, 180)));
    assert_eq!(sps_dimensions(b"\x00\x00\x01\x65\xcc"), None);
}

#[test]
fn test_truncated_and_empty_inputs_do_not_panic() {
    assert_eq!(sps_dimensions(b""), None);
    assert_eq!(sps_dimensions(b"\x67"), None);
    let au = idr(320, 240);
    let sps = sps(&au);
    for n in 0..sps.len() {
        let _ = sps_dimensions(&sps[..n]);
        let _ = ensure_low_delay_vui(&[&b"\x00\x00\x00\x01"[..], &sps[..n]].concat());
    }
    for n in 0..au.len().min(80) {
        let _ = idr_pic_id(&au[..n]);
    }
}
