//! Shared helpers for the relay-voice integration tests.
#![allow(dead_code)]

pub mod mls;

use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use relay_media::codec::VoiceCodec;
use relay_voice::video::PacketSink;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const CODEC_NAMES: [&str; 5] = ["h264", "h265", "av1", "vp8", "vp9"];

pub fn codec_of(name: &str) -> VoiceCodec {
    match name {
        "h264" => VoiceCodec::H264,
        "h265" => VoiceCodec::H265,
        "av1" => VoiceCodec::AV1,
        "vp8" => VoiceCodec::VP8,
        "vp9" => VoiceCodec::VP9,
        other => panic!("unknown codec {other}"),
    }
}

/// T4 fixtures shared with relay-media (`codecs/<codec>_<WxH>_<key|delta>.bin`).
pub fn codec_fixture(name: &str, resolution: &str, kind: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../relay-media/tests/fixtures/codecs")
        .join(format!("{name}_{resolution}_{kind}.bin"));
    std::fs::read(&path).unwrap_or_else(|err| panic!("{}: {err}", path.display()))
}

pub fn rtp_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rtp")
}

pub fn rtp_manifest() -> Value {
    let text = std::fs::read_to_string(rtp_fixture_dir().join("manifest.json"))
        .expect("tests/fixtures/rtp/manifest.json (run tools/gen_voice_fixtures.py)");
    serde_json::from_str(&text).expect("manifest parses")
}

/// Splits a `u16 length + bytes` capture into datagrams.
pub fn read_wire_packets(file: &str) -> Vec<Vec<u8>> {
    let data = std::fs::read(rtp_fixture_dir().join(file)).expect("wire fixture");
    let mut packets = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        packets.push(data[offset..offset + len].to_vec());
        offset += len;
    }
    packets
}

pub fn hex(data: &[u8]) -> String {
    data.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex digit"))
        .collect()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// Annex-B NAL units without their start codes.
pub fn annex_b_nalus(frame: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut index = 0;
    while index + 3 <= frame.len() {
        if frame[index..index + 3] == [0, 0, 1] {
            starts.push((index, 3));
            index += 3;
        } else {
            index += 1;
        }
    }
    let mut nalus = Vec::new();
    for (position, (start, prefix)) in starts.iter().copied().enumerate() {
        let mut end = starts
            .get(position + 1)
            .map(|(next, _)| *next)
            .unwrap_or(frame.len());
        // a 4-byte start code leaves one zero byte at the end of the previous NAL
        if position + 1 < starts.len() && end > start + prefix && frame[end - 1] == 0 {
            end -= 1;
        }
        nalus.push(&frame[start + prefix..end]);
    }
    nalus
}

/// Collects datagrams instead of sending them.
#[derive(Default)]
pub struct CollectSink {
    pub packets: Mutex<Vec<Vec<u8>>>,
}

impl CollectSink {
    pub fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.packets.lock().unwrap())
    }
}

impl PacketSink for CollectSink {
    fn send_packet(&self, packet: &[u8]) -> io::Result<()> {
        self.packets.lock().unwrap().push(packet.to_vec());
        Ok(())
    }
}
