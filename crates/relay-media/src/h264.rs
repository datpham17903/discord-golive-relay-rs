//! H.264 Annex B helpers: NAL splitting, IDR detection, idr_pic_id parsing, SPS dimensions and the
//! low-delay VUI rewrite (spec §9, 11). Port of `golive_relay/media/h264.py`.

use std::sync::Mutex;

pub const NAL_IDR: u8 = 5;
pub const NAL_SPS: u8 = 7;
pub const NAL_PPS: u8 = 8;

const HIGH_PROFILES: [u64; 13] = [100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135];

/// Truncated or malformed bitstream (Python `ValueError`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitstreamError;

impl std::fmt::Display for BitstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bitstream truncated or malformed")
    }
}

impl std::error::Error for BitstreamError {}

type Result<T> = std::result::Result<T, BitstreamError>;

fn find_start_code(au: &[u8], from: usize) -> Option<usize> {
    au.get(from..)?
        .windows(3)
        .position(|w| w == [0, 0, 1])
        .map(|p| p + from)
}

/// Split an Annex B access unit into (nal_unit_type, nal bytes incl. header, no start code).
pub fn nal_units(au: &[u8]) -> Vec<(u8, &[u8])> {
    // (start code position, payload position)
    let mut starts: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while let Some(p) = find_start_code(au, i) {
        let pos = if p > 0 && au[p - 1] == 0 { p - 1 } else { p };
        starts.push((pos, p + 3));
        i = p + 3;
    }
    let mut out = Vec::new();
    for (k, &(_, payload)) in starts.iter().enumerate() {
        let end = starts.get(k + 1).map_or(au.len(), |s| s.0);
        let mut nal = &au[payload..end];
        while let [rest @ .., 0] = nal {
            nal = rest;
        }
        if let Some(&first) = nal.first() {
            out.push((first & 0x1F, nal));
        }
    }
    out
}

pub fn is_idr(au: &[u8]) -> bool {
    nal_units(au).iter().any(|&(t, _)| t == NAL_IDR)
}

pub fn has_parameter_sets(au: &[u8]) -> bool {
    let units = nal_units(au);
    units.iter().any(|&(t, _)| t == NAL_SPS) && units.iter().any(|&(t, _)| t == NAL_PPS)
}

/// Remove emulation prevention bytes (00 00 03 -> 00 00).
pub fn unescape(rbsp: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rbsp.len());
    let mut zeros = 0;
    for &b in rbsp {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        out.push(b);
        zeros = if b == 0 { zeros + 1 } else { 0 };
    }
    out
}

/// Insert emulation prevention bytes (00 00 0x -> 00 00 03 0x for x <= 3).
pub fn escape(rbsp: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rbsp.len() + rbsp.len() / 64);
    let mut zeros = 0;
    for &b in rbsp {
        if zeros >= 2 && b <= 3 {
            out.push(3);
            zeros = 0;
        }
        out.push(b);
        zeros = if b == 0 { zeros + 1 } else { 0 };
    }
    out
}

struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Bits<'a> {
    fn new(data: &'a [u8]) -> Self {
        Bits { data, pos: 0 }
    }

    fn u(&mut self, n: u32) -> Result<u64> {
        let mut v = 0u64;
        for _ in 0..n {
            let byte = *self.data.get(self.pos >> 3).ok_or(BitstreamError)?;
            v = (v << 1) | u64::from((byte >> (7 - (self.pos & 7))) & 1);
            self.pos += 1;
        }
        Ok(v)
    }

    fn flag(&mut self) -> Result<bool> {
        Ok(self.u(1)? == 1)
    }

    fn ue(&mut self) -> Result<u64> {
        let mut zeros = 0u32;
        while self.u(1)? == 0 {
            zeros += 1;
            if zeros > 32 {
                return Err(BitstreamError);
            }
        }
        let tail = if zeros > 0 { self.u(zeros)? } else { 0 };
        Ok((1u64 << zeros) - 1 + tail)
    }

    fn se(&mut self) -> Result<i64> {
        let k = self.ue()? as i64;
        Ok(if k & 1 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }
}

fn skip_scaling_list(b: &mut Bits, size: u32) -> Result<()> {
    let (mut last, mut next) = (8i64, 8i64);
    for _ in 0..size {
        if next != 0 {
            next = (last + b.se()?).rem_euclid(256);
        }
        if next != 0 {
            last = next;
        }
    }
    Ok(())
}

fn nal_body(nal: &[u8]) -> Vec<u8> {
    unescape(nal.get(1..).unwrap_or(&[]))
}

/// profile_idc .. seq_scaling_matrix, shared by both SPS parsers. Returns (chroma_format_idc, separate_colour_plane).
fn parse_sps_head(b: &mut Bits) -> Result<(u64, bool)> {
    let profile = b.u(8)?;
    b.u(16)?; // constraint flags + level_idc
    b.ue()?; // sps_id
    let (mut chroma, mut separate) = (1, false);
    if HIGH_PROFILES.contains(&profile) {
        chroma = b.ue()?;
        if chroma == 3 {
            separate = b.flag()?;
        }
        b.ue()?; // bit_depth_luma_minus8
        b.ue()?; // bit_depth_chroma_minus8
        b.u(1)?; // qpprime_y_zero_transform_bypass_flag
        if b.flag()? {
            // seq_scaling_matrix_present_flag
            for i in 0..(if chroma != 3 { 8 } else { 12 }) {
                if b.flag()? {
                    skip_scaling_list(b, if i < 6 { 16 } else { 64 })?;
                }
            }
        }
    }
    Ok((chroma, separate))
}

/// pic_order_cnt_type .. onwards up to and including max_num_ref_frames; returns max_num_ref_frames.
fn parse_poc_and_refs(b: &mut Bits) -> Result<u64> {
    let poc_type = b.ue()?;
    if poc_type == 0 {
        b.ue()?;
    } else if poc_type == 1 {
        b.u(1)?;
        b.se()?;
        b.se()?;
        for _ in 0..b.ue()? {
            b.se()?;
        }
    }
    b.ue() // max_num_ref_frames
}

/// (log2_max_frame_num, frame_mbs_only_flag, separate_colour_plane_flag)
fn parse_sps(nal: &[u8]) -> Result<(u32, bool, bool)> {
    let body = nal_body(nal);
    let mut b = Bits::new(&body);
    let (_, separate) = parse_sps_head(&mut b)?;
    let log2_frame_num = b.ue()? + 4;
    parse_poc_and_refs(&mut b)?;
    b.u(1)?; // gaps_in_frame_num_value_allowed_flag
    b.ue()?; // pic_width_in_mbs_minus1
    b.ue()?; // pic_height_in_map_units_minus1
    let frame_mbs_only = b.flag()?;
    // log2_frame_num <= 36 here; `u()` handles any count and the loop below stays bounded by the data.
    Ok((
        u32::try_from(log2_frame_num).map_err(|_| BitstreamError)?,
        frame_mbs_only,
        separate,
    ))
}

/// idr_pic_id of the IDR slice in `au`, or None when there is no IDR slice or no SPS to parse it.
pub fn idr_pic_id(au: &[u8]) -> Option<u32> {
    let units = nal_units(au);
    let sps = units.iter().find(|&&(t, _)| t == NAL_SPS)?.1;
    let slice_nal = units.iter().find(|&&(t, _)| t == NAL_IDR)?.1;
    let parse = || -> Result<u64> {
        let (log2_frame_num, frame_mbs_only, separate) = parse_sps(sps)?;
        let body = nal_body(slice_nal);
        let mut b = Bits::new(&body);
        b.ue()?; // first_mb_in_slice
        b.ue()?; // slice_type
        b.ue()?; // pps_id
        if separate {
            b.u(2)?; // colour_plane_id
        }
        b.u(log2_frame_num)?; // frame_num
        if !frame_mbs_only && b.flag()? {
            b.u(1)?; // field_pic_flag set: bottom_field_flag
        }
        b.ue()
    };
    parse().ok().and_then(|v| u32::try_from(v).ok())
}

#[derive(Clone, Debug, Default)]
pub struct BitWriter {
    bits: Vec<u8>,
}

impl BitWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append the first `nbits` bits of `data`.
    pub fn copy(&mut self, data: &[u8], nbits: usize) {
        for p in 0..nbits {
            self.bits.push((data[p >> 3] >> (7 - (p & 7))) & 1);
        }
    }

    pub fn u(&mut self, n: u32, v: u64) {
        self.bits
            .extend((0..n).map(|i| ((v >> (n - 1 - i)) & 1) as u8));
    }

    pub fn ue(&mut self, v: u64) {
        let v = v + 1;
        let n = u64::BITS - v.leading_zeros();
        self.u(n - 1, 0);
        self.u(n, v);
    }

    /// Append rbsp_stop_one_bit, zero-align and return the bytes.
    pub fn finish(mut self) -> Vec<u8> {
        self.bits.push(1);
        while !self.bits.len().is_multiple_of(8) {
            self.bits.push(0);
        }
        self.bits
            .chunks(8)
            .map(|c| c.iter().fold(0u8, |a, &bit| (a << 1) | bit))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sps {
    /// bit position of vui_parameters_present_flag
    pub vui_flag_pos: usize,
    /// bit position of bitstream_restriction_flag (None without VUI)
    pub restriction_flag_pos: Option<usize>,
    pub restriction: bool,
    /// max_num_reorder_frames (0 when no restriction)
    pub reorder: u64,
    /// max_dec_frame_buffering (0 when no restriction)
    pub dec_buf: u64,
    pub max_num_ref_frames: u64,
    pub width: u32,
    pub height: u32,
}

fn skip_hrd(b: &mut Bits) -> Result<()> {
    let cpb_cnt = b.ue()? + 1;
    b.u(8)?; // bit_rate_scale + cpb_size_scale
    for _ in 0..cpb_cnt {
        b.ue()?;
        b.ue()?;
        b.u(1)?;
    }
    b.u(20)?; // delays and lengths
    Ok(())
}

pub fn parse_sps_full(nal: &[u8]) -> Result<Sps> {
    let body = nal_body(nal);
    let mut b = Bits::new(&body);
    let (chroma, separate) = parse_sps_head(&mut b)?;
    b.ue()?; // log2_max_frame_num_minus4
    let num_ref = parse_poc_and_refs(&mut b)?;
    b.u(1)?;
    let mbs_w = b.ue()? + 1;
    let map_h = b.ue()? + 1;
    let fmo = b.u(1)?;
    if fmo == 0 {
        b.u(1)?; // mb_adaptive_frame_field_flag
    }
    b.u(1)?; // direct_8x8_inference_flag
    let mut crop = [0u64; 4];
    if b.flag()? {
        for c in &mut crop {
            *c = b.ue()?;
        }
    }
    let (unit_x, unit_y) = if separate || chroma == 0 {
        (1, 2 - fmo)
    } else {
        let ux = if chroma == 1 || chroma == 2 { 2 } else { 1 };
        (ux, (if chroma == 1 { 2 } else { 1 }) * (2 - fmo))
    };
    // Python lets cropped sizes go negative on garbage; here that is a parse error.
    let width = (mbs_w * 16)
        .checked_sub(unit_x * (crop[0] + crop[1]))
        .ok_or(BitstreamError)?;
    let height = (map_h * 16 * (2 - fmo))
        .checked_sub(unit_y * (crop[2] + crop[3]))
        .ok_or(BitstreamError)?;
    let (width, height) = (
        u32::try_from(width).map_err(|_| BitstreamError)?,
        u32::try_from(height).map_err(|_| BitstreamError)?,
    );
    let vui_pos = b.pos;
    let sps = |restriction_flag_pos, restriction, reorder, dec_buf| Sps {
        vui_flag_pos: vui_pos,
        restriction_flag_pos,
        restriction,
        reorder,
        dec_buf,
        max_num_ref_frames: num_ref,
        width,
        height,
    };
    if !b.flag()? {
        return Ok(sps(None, false, 0, 0));
    }
    if b.flag()? {
        // aspect_ratio_info_present_flag
        if b.u(8)? == 255 {
            b.u(32)?;
        }
    }
    if b.flag()? {
        // overscan_info_present_flag
        b.u(1)?;
    }
    if b.flag()? {
        // video_signal_type_present_flag
        b.u(4)?;
        if b.flag()? {
            b.u(24)?;
        }
    }
    if b.flag()? {
        // chroma_loc_info_present_flag
        b.ue()?;
        b.ue()?;
    }
    if b.flag()? {
        // timing_info_present_flag
        b.u(32)?;
        b.u(32)?;
        b.u(1)?;
    }
    let hrd = b.flag()?;
    if hrd {
        skip_hrd(&mut b)?;
    }
    let vcl = b.flag()?;
    if vcl {
        skip_hrd(&mut b)?;
    }
    if hrd || vcl {
        b.u(1)?; // low_delay_hrd_flag
    }
    b.u(1)?; // pic_struct_present_flag
    let flag_pos = b.pos;
    if !b.flag()? {
        return Ok(sps(Some(flag_pos), false, 0, 0));
    }
    b.u(1)?;
    for _ in 0..4 {
        b.ue()?;
    }
    let reorder = b.ue()?;
    let dec_buf = b.ue()?;
    Ok(sps(Some(flag_pos), true, reorder, dec_buf))
}

fn split_sps(data: &[u8]) -> Option<&[u8]> {
    if data.starts_with(&[0, 0, 1]) || data.starts_with(&[0, 0, 0, 1]) {
        return nal_units(data)
            .into_iter()
            .find(|&(t, _)| t == NAL_SPS)
            .map(|(_, n)| n);
    }
    Some(data)
}

/// Cropped (width, height) from the SPS of an Annex B AU or a bare SPS NAL; None if absent/unparseable.
pub fn sps_dimensions(au_or_sps: &[u8]) -> Option<(u32, u32)> {
    let info = parse_sps_full(split_sps(au_or_sps)?).ok()?;
    Some((info.width, info.height))
}

/// `nal` with bitstream_restriction (reorder 0) added, or `nal` itself when already present.
fn rewrite_sps(nal: &[u8]) -> Result<Vec<u8>> {
    let info = parse_sps_full(nal)?;
    if info.restriction {
        return Ok(nal.to_vec());
    }
    let rbsp = nal_body(nal);
    let mut w = BitWriter::new();
    match info.restriction_flag_pos {
        None => {
            w.copy(&rbsp, info.vui_flag_pos);
            w.u(1, 1); // vui_parameters_present_flag
            w.u(8, 0); // aspect/overscan/video_signal/chroma_loc/timing/nal_hrd/vcl_hrd/pic_struct all absent
        }
        Some(pos) => w.copy(&rbsp, pos),
    }
    w.u(1, 1); // bitstream_restriction_flag
    w.u(1, 1); // motion_vectors_over_pic_boundaries_flag
    w.ue(2); // max_bytes_per_pic_denom
    w.ue(1); // max_bits_per_mb_denom
    w.ue(16); // log2_max_mv_length_horizontal
    w.ue(16); // log2_max_mv_length_vertical
    w.ue(0); // max_num_reorder_frames
    w.ue(info.max_num_ref_frames); // max_dec_frame_buffering
    let mut out = vec![nal[0]];
    out.extend(escape(&w.finish()));
    Ok(out)
}

/// (input SPS, output SPS); called on every IDR.
static LAST_REWRITE: Mutex<Option<(Vec<u8>, Vec<u8>)>> = Mutex::new(None);

/// Make viewers' decoders output frames immediately: add VUI bitstream_restriction (reorder 0) to the SPS
/// when it has none. Returns a copy of `au` when there is no SPS or nothing to change.
pub fn ensure_low_delay_vui(au: &[u8]) -> Vec<u8> {
    let units = nal_units(au);
    let Some(&(_, sps)) = units.iter().find(|&&(t, _)| t == NAL_SPS) else {
        return au.to_vec();
    };
    let new = {
        let mut cache = LAST_REWRITE.lock().unwrap_or_else(|e| e.into_inner());
        match cache.as_ref() {
            Some((input, output)) if input == sps => output.clone(),
            _ => match rewrite_sps(sps) {
                Ok(new) => {
                    *cache = Some((sps.to_vec(), new.clone()));
                    new
                }
                Err(BitstreamError) => return au.to_vec(),
            },
        }
    };
    if new == sps {
        return au.to_vec();
    }
    let i = au.windows(sps.len()).position(|w| w == sps).unwrap_or(0);
    let mut out = Vec::with_capacity(au.len() + new.len() - sps.len());
    out.extend_from_slice(&au[..i]);
    out.extend_from_slice(&new);
    out.extend_from_slice(&au[i + sps.len()..]);
    out
}
