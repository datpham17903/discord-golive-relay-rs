// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/video/h26x.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       added `strip_h264_filler_nalus` (from src/python/video.rs, bytes in/out).

use crate::rtp::PacketError;

pub(super) const ANNEX_B_START_CODE: [u8; 4] = [0, 0, 0, 1];

pub(super) fn split_annex_b(frame: &[u8]) -> Result<Vec<Vec<u8>>, PacketError> {
    if frame.is_empty() {
        return Err(PacketError::EmptyFrame);
    }

    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= frame.len() {
        if i + 4 <= frame.len() && frame[i..i + 4] == [0, 0, 0, 1] {
            starts.push((i, 4));
            i += 4;
        } else if frame[i..i + 3] == [0, 0, 1] {
            starts.push((i, 3));
            i += 3;
        } else {
            i += 1;
        }
    }

    if starts.is_empty() {
        return Ok(vec![frame.to_vec()]);
    }

    let mut nalus = Vec::new();
    for (index, (start, prefix_len)) in starts.iter().copied().enumerate() {
        let payload_start = start + prefix_len;
        let payload_end = starts
            .get(index + 1)
            .map(|(next, _)| *next)
            .unwrap_or(frame.len());
        if payload_start < payload_end {
            nalus.push(frame[payload_start..payload_end].to_vec());
        }
    }

    if nalus.is_empty() {
        Err(PacketError::EmptyFrame)
    } else {
        Ok(nalus)
    }
}

/// Removes H.264 filler NAL units (type 12) from an Annex-B frame. Returns the
/// frame unchanged when it has none, has no start codes, or would end up empty.
pub fn strip_h264_filler_nalus(frame: &[u8]) -> Vec<u8> {
    let mut starts = Vec::new();
    let mut index = 0;
    while index + 3 <= frame.len() {
        if index + 4 <= frame.len() && frame[index..index + 4] == [0, 0, 0, 1] {
            starts.push((index, 4));
            index += 4;
        } else if frame[index..index + 3] == [0, 0, 1] {
            starts.push((index, 3));
            index += 3;
        } else {
            index += 1;
        }
    }

    if starts.is_empty() {
        return frame.to_vec();
    }

    let mut output = Vec::with_capacity(frame.len());
    let mut removed = false;
    for (position, (start, prefix_len)) in starts.iter().copied().enumerate() {
        let payload_start = start + prefix_len;
        let payload_end = starts
            .get(position + 1)
            .map(|(next, _)| *next)
            .unwrap_or(frame.len());
        if payload_start >= payload_end {
            continue;
        }
        if frame[payload_start] & 0x1F == 12 {
            removed = true;
            continue;
        }
        output.extend_from_slice(&frame[start..payload_end]);
    }

    if !removed || output.is_empty() {
        return frame.to_vec();
    }
    output
}
