//! Codec-generic helpers: keyframe / entry-point detection and dimension probing. Port of
//! `golive_relay/media/bitstream.py`; the codec ranking helpers live in [`crate::codec`] (the only public path).
//!
//! Difference from Python: `probe_dimensions` for AV1 / H.265 / VP9 decodes the frame with PyAV there; this
//! crate has no decoder, so the width/height come from a minimal header parser instead (AV1 sequence header
//! `max_frame_{width,height}_minus_1`, H.265 SPS with conformance-window cropping, VP9 uncompressed keyframe
//! header). They match what the decoder reports for streams where the frame size equals the sequence maximum
//! (always true for the single-size streams the relay forwards).

use crate::h264;

const H265_IRAP: std::ops::RangeInclusive<u8> = 16..=21;
const H265_VPS: u8 = 32;
const H265_SPS: u8 = 33;
const H265_PPS: u8 = 34;
const AV1_SEQUENCE_HEADER: u8 = 1;

/// MSB-first bit reader; every read returns `None` once the data runs out.
struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Bits<'a> {
    fn new(data: &'a [u8]) -> Self {
        Bits { data, pos: 0 }
    }

    fn u(&mut self, n: u32) -> Option<u64> {
        let mut v = 0u64;
        for _ in 0..n {
            let byte = *self.data.get(self.pos >> 3)?;
            v = (v << 1) | u64::from((byte >> (7 - (self.pos & 7))) & 1);
            self.pos += 1;
        }
        Some(v)
    }

    fn flag(&mut self) -> Option<bool> {
        Some(self.u(1)? == 1)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        self.pos = self.pos.checked_add(n)?;
        (self.pos <= self.data.len() * 8).then_some(())
    }

    /// Exp-Golomb `ue(v)` (H.265) / `uvlc()` (AV1).
    fn ue(&mut self) -> Option<u64> {
        let mut zeros = 0u32;
        while self.u(1)? == 0 {
            zeros += 1;
            if zeros > 32 {
                return None;
            }
        }
        let tail = if zeros > 0 { self.u(zeros)? } else { 0 };
        Some((1u64 << zeros) - 1 + tail)
    }
}

/// Start-code positions (index of the first byte after `00 00 01`) of an Annex B buffer.
fn start_code_ends(au: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(p) = au
        .get(i..)
        .and_then(|s| s.windows(3).position(|w| w == [0, 0, 1]))
    {
        out.push(i + p + 3);
        i += p + 3;
    }
    out
}

/// Python `_h265_nal_types`: the type byte after each start code (a start code ending the buffer is ignored).
fn h265_nal_types(au: &[u8]) -> Vec<u8> {
    start_code_ends(au)
        .into_iter()
        .filter_map(|p| au.get(p).map(|b| (b >> 1) & 0x3F))
        .collect()
}

/// OBUs of a low-overhead AV1 temporal unit as (type, payload). The flag is false when the data is malformed;
/// the OBUs read before the fault are still returned.
fn av1_obus(data: &[u8]) -> (Vec<(u8, &[u8])>, bool) {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < data.len() {
        let h = data[i];
        let kind = (h >> 3) & 0xF;
        let (ext, has_size) = (usize::from((h >> 2) & 1), (h >> 1) & 1 == 1);
        i += 1 + ext;
        if !has_size {
            // the OBU runs to the end of the buffer
            return match data.get(i..) {
                Some(rest) => {
                    out.push((kind, rest));
                    (out, true)
                }
                None => (out, false),
            };
        }
        let (mut size, mut shift) = (0u64, 0u32);
        loop {
            if i >= data.len() || shift > 56 {
                return (out, false);
            }
            let b = data[i];
            i += 1;
            size |= u64::from(b & 0x7F) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        let end = usize::try_from(size)
            .ok()
            .and_then(|s| i.checked_add(s))
            .filter(|&e| e <= data.len());
        match end {
            Some(end) => {
                out.push((kind, &data[i..end]));
                i = end;
            }
            None => return (out, false),
        }
    }
    (out, true)
}

/// OBU types of a low-overhead AV1 temporal unit; None if malformed.
fn av1_obu_types(data: &[u8]) -> Option<Vec<u8>> {
    let (obus, ok) = av1_obus(data);
    ok.then(|| obus.into_iter().map(|(k, _)| k).collect())
}

fn vp8_keyframe(au: &[u8]) -> bool {
    au.len() >= 10 && au[0] & 1 == 0 && au[3..6] == [0x9d, 0x01, 0x2a]
}

fn vp9_keyframe(au: &[u8]) -> bool {
    if au.is_empty() {
        return false;
    }
    // Like Python, work on the first two bytes zero-padded; the length check below rejects what the
    // padding invented.
    let bits = u16::from_be_bytes([au[0], au.get(1).copied().unwrap_or(0)]);
    let take = |pos: usize, n: usize| (bits >> (16 - pos - n)) & ((1 << n) - 1);
    if take(0, 2) != 2 {
        return false;
    }
    let profile = take(3, 1) << 1 | take(2, 1);
    let pos = 4 + usize::from(profile == 3);
    if au.len() * 8 < pos + 2 {
        return false;
    }
    if take(pos, 1) == 1 {
        // show_existing_frame
        return false;
    }
    take(pos + 1, 1) == 0
}

pub fn is_keyframe(codec: &str, au: &[u8]) -> bool {
    match codec {
        "H264" => h264::is_idr(au),
        "H265" => h265_nal_types(au).iter().any(|t| H265_IRAP.contains(t)),
        "AV1" => av1_obu_types(au).is_some_and(|t| t.contains(&AV1_SEQUENCE_HEADER)),
        "VP8" => vp8_keyframe(au),
        "VP9" => vp9_keyframe(au),
        _ => false,
    }
}

pub fn is_entry_point(codec: &str, au: &[u8]) -> bool {
    match codec {
        "H264" => h264::is_idr(au) && h264::has_parameter_sets(au),
        "H265" => {
            let types = h265_nal_types(au);
            types.iter().any(|t| H265_IRAP.contains(t))
                && [H265_VPS, H265_SPS, H265_PPS]
                    .iter()
                    .all(|t| types.contains(t))
        }
        _ => is_keyframe(codec, au),
    }
}

/// Cropped (width, height) of the first parseable H.265 SPS NAL in an Annex B access unit.
fn h265_dimensions(au: &[u8]) -> Option<(u32, u32)> {
    let ends = start_code_ends(au);
    for (n, &start) in ends.iter().enumerate() {
        let end = ends.get(n + 1).map_or(au.len(), |&e| e - 3);
        let nal = &au[start..end];
        if nal.len() > 2
            && (nal[0] >> 1) & 0x3F == H265_SPS
            && let Some(dims) = h265_sps_dimensions(&h264::unescape(&nal[2..]))
        {
            return Some(dims);
        }
    }
    None
}

/// seq_parameter_set_rbsp up to the conformance window (ITU-T H.265 7.3.2.2.1, 7.3.3).
fn h265_sps_dimensions(rbsp: &[u8]) -> Option<(u32, u32)> {
    let mut b = Bits::new(rbsp);
    b.skip(4)?; // sps_video_parameter_set_id
    let max_sub_layers_minus1 = b.u(3)? as usize;
    b.skip(1)?; // sps_temporal_id_nesting_flag
    // profile_tier_level(1, max_sub_layers_minus1): general part is 2+1+5+32+4+43+1+8 bits
    b.skip(96)?;
    let mut flags = [(false, false); 7];
    for f in flags.iter_mut().take(max_sub_layers_minus1) {
        *f = (b.flag()?, b.flag()?);
    }
    if max_sub_layers_minus1 > 0 {
        b.skip(2 * (8 - max_sub_layers_minus1))?; // reserved_zero_2bits
    }
    for &(profile, level) in flags.iter().take(max_sub_layers_minus1) {
        if profile {
            b.skip(88)?;
        }
        if level {
            b.skip(8)?;
        }
    }
    b.ue()?; // sps_seq_parameter_set_id
    let chroma_format_idc = b.ue()?;
    if chroma_format_idc > 3 {
        return None;
    }
    let separate_colour_plane = chroma_format_idc == 3 && b.flag()?;
    let width = b.ue()?;
    let height = b.ue()?;
    let (mut left, mut right, mut top, mut bottom) = (0, 0, 0, 0);
    if b.flag()? {
        // conformance_window_flag
        left = b.ue()?;
        right = b.ue()?;
        top = b.ue()?;
        bottom = b.ue()?;
    }
    // SubWidthC / SubHeightC (Table 6-1); ChromaArrayType is 0 with separate colour planes
    let (sub_w, sub_h) = match (separate_colour_plane, chroma_format_idc) {
        (false, 1) => (2, 2),
        (false, 2) => (2, 1),
        _ => (1, 1),
    };
    let w = width
        .checked_sub(sub_w * (left + right))
        .filter(|&w| w > 0)?;
    let h = height
        .checked_sub(sub_h * (top + bottom))
        .filter(|&h| h > 0)?;
    Some((u32::try_from(w).ok()?, u32::try_from(h).ok()?))
}

/// max_frame_{width,height}_minus_1 + 1 of the first sequence header OBU (AV1 spec 5.5.1).
fn av1_dimensions(au: &[u8]) -> Option<(u32, u32)> {
    let (obus, _) = av1_obus(au);
    let payload = obus.into_iter().find(|&(k, _)| k == AV1_SEQUENCE_HEADER)?.1;
    let mut b = Bits::new(payload);
    b.skip(3)?; // seq_profile
    b.skip(1)?; // still_picture
    let reduced_still_picture_header = b.flag()?;
    if reduced_still_picture_header {
        b.skip(5)?; // seq_level_idx[0]
    } else {
        let timing_info_present = b.flag()?;
        let mut decoder_model_info_present = false;
        let mut buffer_delay_length = 0;
        if timing_info_present {
            b.skip(64)?; // num_units_in_display_tick, time_scale
            if b.flag()? {
                b.ue()?; // num_ticks_per_picture_minus_1
            }
            decoder_model_info_present = b.flag()?;
            if decoder_model_info_present {
                buffer_delay_length = b.u(5)? as u32 + 1;
                b.skip(32)?; // num_units_in_decoding_tick
                b.skip(10)?; // buffer_removal_time_length_minus_1, frame_presentation_time_length_minus_1
            }
        }
        let initial_display_delay_present = b.flag()?;
        let operating_points = b.u(5)? + 1;
        for _ in 0..operating_points {
            b.skip(12)?; // operating_point_idc
            if b.u(5)? > 7 {
                b.skip(1)?; // seq_tier
            }
            if decoder_model_info_present && b.flag()? {
                b.skip(2 * buffer_delay_length as usize + 1)?; // operating_parameters_info
            }
            if initial_display_delay_present && b.flag()? {
                b.skip(4)?;
            }
        }
    }
    let width_bits = b.u(4)? as u32 + 1;
    let height_bits = b.u(4)? as u32 + 1;
    let width = b.u(width_bits)? + 1;
    let height = b.u(height_bits)? + 1;
    Some((u32::try_from(width).ok()?, u32::try_from(height).ok()?))
}

/// frame_width/height of a VP9 keyframe's uncompressed header (VP9 bitstream spec 6.2).
fn vp9_dimensions(au: &[u8]) -> Option<(u32, u32)> {
    let mut b = Bits::new(au);
    if b.u(2)? != 2 {
        return None; // frame_marker
    }
    let profile = b.u(1)? | (b.u(1)? << 1);
    if profile == 3 {
        b.skip(1)?; // reserved_zero
    }
    if b.flag()? {
        return None; // show_existing_frame
    }
    if b.u(1)? != 0 {
        return None; // not a keyframe
    }
    b.skip(2)?; // show_frame, error_resilient_mode
    if b.u(24)? != 0x49_83_42 {
        return None; // frame_sync_code
    }
    // color_config()
    if profile >= 2 {
        b.skip(1)?; // ten_or_twelve_bit
    }
    let color_space = b.u(3)?;
    if color_space != 7 {
        b.skip(1)?; // color_range
        if profile == 1 || profile == 3 {
            b.skip(3)?; // subsampling_x, subsampling_y, reserved_zero
        }
    } else if profile == 1 || profile == 3 {
        b.skip(1)?; // reserved_zero
    }
    let width = b.u(16)? as u32 + 1;
    let height = b.u(16)? as u32 + 1;
    Some((width, height))
}

fn vp8_dimensions(au: &[u8]) -> Option<(u32, u32)> {
    if !vp8_keyframe(au) {
        return None;
    }
    let dim = |i: usize| u32::from(u16::from_le_bytes([au[i], au[i + 1]]) & 0x3FFF);
    Some((dim(6), dim(8)))
}

pub fn probe_dimensions(codec: &str, au: &[u8]) -> Option<(u32, u32)> {
    match codec {
        "H264" => h264::sps_dimensions(au),
        "VP8" => vp8_dimensions(au),
        "H265" => h265_dimensions(au),
        "AV1" => av1_dimensions(au),
        "VP9" => vp9_dimensions(au),
        _ => None,
    }
}
