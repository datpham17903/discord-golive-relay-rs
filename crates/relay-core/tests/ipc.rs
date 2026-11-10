//! Port of `test_bounded_channel_limits_and_accounting` (`tests/worker/test_worker.py`) plus checks of the
//! in-process channel semantics the worker relies on (blocking `get`, timeout, accounting on `get`).

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use relay_core::worker::ipc::{BoundedChannel, Empty};
use relay_core::worker::messages::{AUDIO_FRAME_BYTES, WorkerEventKind};

#[test]
fn test_bounded_channel_limits_and_accounting() {
    let ch = BoundedChannel::new(&[("v", 2, Some(10)), ("c", 1, None)]);
    assert!(ch.put_nowait("a", "v", 6));
    assert!(!ch.put_nowait("b", "v", 6)); // bytes
    assert!(ch.put_nowait("c", "v", 4));
    assert!(!ch.put_nowait("d", "v", 0)); // items
    assert!(ch.put_nowait("x", "c", 0) && !ch.put_nowait("y", "c", 0));
    assert_eq!(ch.get(Some(2.0)), Ok("a"));
    assert!(ch.put_nowait("e", "v", 6));
}

#[test]
fn test_full_pending_and_fifo_across_kinds() {
    let ch = BoundedChannel::new(&[("v", 1, Some(10)), ("c", 2, None)]);
    assert!(!ch.full("v") && ch.pending("v") == 0);
    assert!(ch.put_nowait(1, "v", 3));
    assert!(ch.put_nowait(2, "c", 0));
    assert!(ch.put_nowait(3, "c", 0));
    assert!(ch.full("v") && ch.full("c"));
    assert_eq!((ch.pending("v"), ch.pending("c")), (1, 2));
    assert_eq!(ch.get_nowait(), Ok(1));
    assert!(!ch.full("v") && ch.pending("v") == 0);
    assert_eq!(ch.get_nowait(), Ok(2));
    assert_eq!(ch.get_nowait(), Ok(3));
    assert_eq!(ch.get_nowait(), Err(Empty));
    assert_eq!(ch.pending("c"), 0);
}

#[test]
fn test_get_times_out_when_empty() {
    let ch: BoundedChannel<u8> = BoundedChannel::new(&[("v", 1, None)]);
    let t0 = Instant::now();
    assert_eq!(ch.get(Some(0.05)), Err(Empty));
    assert!(t0.elapsed() >= Duration::from_millis(45));
}

#[test]
fn test_get_blocks_until_put_from_another_thread() {
    let ch = Arc::new(BoundedChannel::new(&[("v", 4, None)]));
    let producer = {
        let ch = Arc::clone(&ch);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            assert!(ch.put_nowait("hello", "v", 0));
        })
    };
    assert_eq!(ch.get(Some(5.0)), Ok("hello"));
    producer.join().unwrap();
    assert!(ch.put_nowait("again", "v", 0));
    assert_eq!(ch.get(None), Ok("again"));
}

#[test]
#[should_panic(expected = "unknown channel kind")]
fn test_unknown_kind_panics() {
    let ch = BoundedChannel::new(&[("v", 1, None)]);
    ch.put_nowait(1, "nope", 0);
}

#[test]
fn test_message_constants_and_event_kind_strings() {
    assert_eq!(AUDIO_FRAME_BYTES, 3840);
    let kinds = [
        (WorkerEventKind::FirstFrame, "first_frame"),
        (WorkerEventKind::FreshFrame, "fresh_frame"),
        (WorkerEventKind::SourceCodec, "source_codec"),
        (WorkerEventKind::SourceIneligible, "source_ineligible"),
        (WorkerEventKind::NeedKeyframe, "need_keyframe"),
        (WorkerEventKind::DecodeError, "decode_error"),
        (WorkerEventKind::Overflow, "overflow"),
        (WorkerEventKind::Stats, "stats"),
        (WorkerEventKind::Fatal, "fatal"),
    ];
    for (kind, text) in kinds {
        assert_eq!(kind.as_str(), text);
    }
}
