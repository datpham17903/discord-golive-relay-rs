//! Pre-built neutral slate: closed GOP variants with distinct IDR ids (spec §11). Port of
//! `golive_relay/media/slate.py`.
//!
//! Sai khác so với Python: không có libx264. Slate là bitstream Constrained Baseline tự viết (không
//! encoder): IDR chỉ gồm macroblock I_16x16 DC không residual, P-frame chỉ gồm P_Skip, nên mọi frame là
//! xám đồng nhất Y=U=V=128 (Python dùng Y=110, U=V=128). Luôn dùng 2 variant với `idr_pic_id` 0 và 1.

use crate::h264::{BitWriter, escape, idr_pic_id};

const VARIANTS: u32 = 2; // idr_pic_id 0/1
const MAX_GOP_S: f64 = 1.0; // the publisher needs only each variant's IDR and one P-frame
const START_CODE: [u8; 4] = [0, 0, 0, 1];
const LOG2_MAX_FRAME_NUM: u32 = 8;
/// One I_16x16 DC macroblock: mb_type ue(3), intra_chroma_pred_mode ue(0), mb_qp_delta se(0), coeff_token "1".
const MB_I16X16_DC: u8 = 0x27;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlateError {
    /// Width or height is odd (4:2:0 needs an even canvas).
    OddCanvas,
    /// Zero width, height or fps.
    InvalidParams,
    /// No variant with an `idr_pic_id` different from the last sent one.
    NoDistinctVariant,
}

impl std::fmt::Display for SlateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SlateError::OddCanvas => "slate canvas must be even",
            SlateError::InvalidParams => "slate width, height and fps must be positive",
            SlateError::NoDistinctVariant => "no slate variant with a different idr_pic_id",
        })
    }
}

impl std::error::Error for SlateError {}

/// Each variant is a closed GOP (IDR with SPS/PPS, then P-frames) spaced `frame_time_ms` apart.
#[derive(Clone, Debug, Default)]
pub struct SlateGop {
    pub variants: Vec<Vec<Vec<u8>>>,
    pub frame_time_ms: f64,
    pub width: u32,
    pub height: u32,
}

impl SlateGop {
    /// One GOP whose first IDR id differs from `last_sent_idr_pic_id`.
    pub fn next_au(
        &self,
        last_sent_idr_pic_id: Option<u32>,
    ) -> Result<impl Iterator<Item = &[u8]> + '_, SlateError> {
        let gop = self
            .variants
            .iter()
            .find(|gop| {
                last_sent_idr_pic_id.is_none() || idr_pic_id(&gop[0]) != last_sent_idr_pic_id
            })
            .ok_or(SlateError::NoDistinctVariant)?;
        Ok(gop.iter().map(Vec::as_slice))
    }
}

/// Smallest of levels 3.1/4.0/4.2/5.1/5.2 whose MaxFS and MaxMBPS fit (5.2 when none does).
fn level_idc(mbs: u64, fps: u32) -> u8 {
    // (level_idc, MaxFS, MaxMBPS)
    const LEVELS: [(u8, u64, u64); 5] = [
        (31, 3600, 108_000),
        (40, 8192, 245_760),
        (42, 8704, 522_240),
        (51, 36_864, 983_040),
        (52, 36_864, 2_073_600),
    ];
    let rate = mbs * u64::from(fps);
    LEVELS
        .iter()
        .find(|&&(_, max_fs, max_mbps)| mbs <= max_fs && rate <= max_mbps)
        .map_or(52, |l| l.0)
}

fn nal(header: u8, rbsp: Vec<u8>) -> Vec<u8> {
    let mut out = START_CODE.to_vec();
    out.push(header);
    out.extend(escape(&rbsp));
    out
}

fn sps(width: u32, height: u32, fps: u32) -> Vec<u8> {
    let (mbs_w, mbs_h) = (width.div_ceil(16), height.div_ceil(16));
    let mut w = BitWriter::new();
    w.u(8, 66); // profile_idc: Constrained Baseline
    w.u(8, 0xC0); // constraint_set0/1
    w.u(
        8,
        u64::from(level_idc(u64::from(mbs_w) * u64::from(mbs_h), fps)),
    );
    w.ue(0); // seq_parameter_set_id
    w.ue(u64::from(LOG2_MAX_FRAME_NUM - 4));
    w.ue(2); // pic_order_cnt_type
    w.ue(1); // max_num_ref_frames
    w.u(1, 0); // gaps_in_frame_num_value_allowed_flag
    w.ue(u64::from(mbs_w - 1));
    w.ue(u64::from(mbs_h - 1));
    w.u(1, 1); // frame_mbs_only_flag
    w.u(1, 1); // direct_8x8_inference_flag
    let (crop_x, crop_y) = ((mbs_w * 16 - width) / 2, (mbs_h * 16 - height) / 2);
    if crop_x | crop_y != 0 {
        w.u(1, 1); // frame_cropping_flag (4:2:0 crop unit is 2 pixels)
        w.ue(0);
        w.ue(u64::from(crop_x));
        w.ue(0);
        w.ue(u64::from(crop_y));
    } else {
        w.u(1, 0);
    }
    w.u(1, 1); // vui_parameters_present_flag
    w.u(4, 0); // aspect_ratio_info, overscan_info, video_signal_type, chroma_loc_info absent
    w.u(1, 1); // timing_info_present_flag
    w.u(32, 1); // num_units_in_tick
    w.u(32, 2 * u64::from(fps)); // time_scale
    w.u(1, 1); // fixed_frame_rate_flag
    w.u(3, 0); // nal_hrd, vcl_hrd, pic_struct_present_flag absent
    w.u(1, 1); // bitstream_restriction_flag
    w.u(1, 1); // motion_vectors_over_pic_boundaries_flag
    w.ue(2); // max_bytes_per_pic_denom
    w.ue(1); // max_bits_per_mb_denom
    w.ue(16); // log2_max_mv_length_horizontal
    w.ue(16); // log2_max_mv_length_vertical
    w.ue(0); // max_num_reorder_frames
    w.ue(1); // max_dec_frame_buffering
    nal(0x67, w.finish())
}

fn pps() -> Vec<u8> {
    let mut w = BitWriter::new();
    w.ue(0); // pic_parameter_set_id
    w.ue(0); // seq_parameter_set_id
    w.u(1, 0); // entropy_coding_mode_flag: CAVLC
    w.u(1, 0); // bottom_field_pic_order_in_frame_present_flag
    w.ue(0); // num_slice_groups_minus1
    w.ue(0); // num_ref_idx_l0_default_active_minus1
    w.ue(0); // num_ref_idx_l1_default_active_minus1
    w.u(1, 0); // weighted_pred_flag
    w.u(2, 0); // weighted_bipred_idc
    w.ue(0); // pic_init_qp_minus26
    w.ue(0); // pic_init_qs_minus26
    w.ue(0); // chroma_qp_index_offset
    w.u(1, 1); // deblocking_filter_control_present_flag
    w.u(1, 0); // constrained_intra_pred_flag
    w.u(1, 0); // redundant_pic_cnt_present_flag
    nal(0x68, w.finish())
}

fn idr_slice(mbs: u32, idr_pic_id: u64) -> Vec<u8> {
    let mut w = BitWriter::new();
    w.ue(0); // first_mb_in_slice
    w.ue(7); // slice_type: I (all slices)
    w.ue(0); // pic_parameter_set_id
    w.u(LOG2_MAX_FRAME_NUM, 0); // frame_num
    w.ue(idr_pic_id);
    w.u(1, 0); // no_output_of_prior_pics_flag
    w.u(1, 0); // long_term_reference_flag
    w.ue(0); // slice_qp_delta
    w.ue(1); // disable_deblocking_filter_idc
    for _ in 0..mbs {
        w.u(8, u64::from(MB_I16X16_DC));
    }
    nal(0x65, w.finish())
}

fn p_slice(mbs: u32, frame_num: u32) -> Vec<u8> {
    let mut w = BitWriter::new();
    w.ue(0); // first_mb_in_slice
    w.ue(5); // slice_type: P (all slices)
    w.ue(0); // pic_parameter_set_id
    w.u(
        LOG2_MAX_FRAME_NUM,
        u64::from(frame_num % (1 << LOG2_MAX_FRAME_NUM)),
    );
    w.u(1, 0); // num_ref_idx_active_override_flag
    w.u(1, 0); // ref_pic_list_modification_flag_l0
    w.u(1, 0); // adaptive_ref_pic_marking_mode_flag
    w.ue(0); // slice_qp_delta
    w.ue(1); // disable_deblocking_filter_idc
    w.ue(u64::from(mbs)); // mb_skip_run: every macroblock is P_Skip
    nal(0x41, w.finish())
}

/// Build the gray GOP variants. `bitrate_kbps` is unused (there is no rate control).
pub fn build_slate(
    width: u32,
    height: u32,
    fps: u32,
    keyframe_interval_s: f64,
    _bitrate_kbps: u32,
) -> Result<SlateGop, SlateError> {
    if !width.is_multiple_of(2) || !height.is_multiple_of(2) {
        return Err(SlateError::OddCanvas);
    }
    if width == 0 || height == 0 || fps == 0 {
        return Err(SlateError::InvalidParams);
    }
    let gop_len = ((keyframe_interval_s.min(MAX_GOP_S) * f64::from(fps)) as u32).max(1);
    let mbs = width.div_ceil(16) * height.div_ceil(16);
    let (sps, pps) = (sps(width, height, fps), pps());
    let variants = (0..VARIANTS)
        .map(|id| {
            let mut idr = sps.clone();
            idr.extend_from_slice(&pps);
            idr.extend(idr_slice(mbs, u64::from(id)));
            let mut gop = vec![idr];
            gop.extend((1..gop_len).map(|n| p_slice(mbs, n)));
            gop
        })
        .collect();
    Ok(SlateGop {
        variants,
        frame_time_ms: 1000.0 / f64::from(fps),
        width,
        height,
    })
}
