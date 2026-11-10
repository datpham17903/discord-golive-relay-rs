//! What the voice sender pulls its media from (the counterpart of the Python library's
//! `MediaSource`, reduced to the two reads the relay uses).

use crate::frame::OutFrame;

/// A pull-based source of one video access unit at a time and 20 ms Opus frames.
///
/// The sender calls both reads from one dedicated thread, so implementations need no locking of their
/// own against the sender, but must be `Send`.
pub trait FrameSource: Send {
    /// The next access unit, or `None` when nothing is due (the sender asks again after ~2 ms).
    /// `OutFrame::frame_time_ms` is the pause before the following frame.
    fn next_video(&mut self) -> Option<OutFrame>;

    /// The next 20 ms Opus frame; the sender calls it every 20 ms. An empty vector means "nothing to
    /// send this tick" (the 20 ms cadence continues).
    fn next_audio(&mut self) -> Vec<u8>;
}

/// A boxed source is a source, so a caller that only has a `Box<dyn FrameSource>` can play it.
impl FrameSource for Box<dyn FrameSource> {
    fn next_video(&mut self) -> Option<OutFrame> {
        (**self).next_video()
    }

    fn next_audio(&mut self) -> Vec<u8> {
        (**self).next_audio()
    }
}
