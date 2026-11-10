//! A video access unit handed to the voice sender (replaces the Python library's `VideoFrame`).

/// One access unit plus the time the sender must wait before the next one.
#[derive(Clone, Debug, PartialEq)]
pub struct OutFrame {
    pub data: Vec<u8>,
    pub frame_time_ms: f64,
}
