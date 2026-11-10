//! Port of `tests/worker/test_timeline.py`. The `Timeline` class is unused in the Python sources and is not
//! ported, so its four tests (`test_arrival_anchored_is_estimated`,
//! `test_rtcp_anchor_not_estimated_and_shared_axis`, `test_deadline_never_goes_backwards`,
//! `test_estimated_is_per_track_any_active_estimated`) are dropped.

use relay_core::worker::timeline::RtpUnwrapper;

#[test]
fn test_unwrap_across_2_32() {
    let mut u = RtpUnwrapper::new(90_000);
    let a = u.unwrap(0xFFFF_FF00);
    let b = u.unwrap(0x0000_0100);
    assert_eq!(b - a, 0x200);
    assert_eq!(u.unwrap(0x0000_0200) - b, 0x100);
}

#[test]
fn test_unwrap_reordered_packet_steps_back() {
    let mut u = RtpUnwrapper::new(90_000);
    let a = u.unwrap(1000);
    let b = u.unwrap(4000);
    let c = u.unwrap(2500);
    assert!(c < b && c > a);
}
