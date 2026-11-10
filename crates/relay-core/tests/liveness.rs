//! Port of `tests/supervisor/test_liveness.py`.

use relay_core::supervisor::liveness::LivenessTracker;
use relay_core::supervisor::model::Liveness;

#[test]
fn unknown_before_progress() {
    let t = LivenessTracker::new(10.0);
    assert_eq!(t.state(100.0), Liveness::Unknown);
    assert_eq!(t.age(100.0), None);
}

#[test]
fn live_until_t_dead_then_dead() {
    let mut t = LivenessTracker::new(10.0);
    t.progress(100.0);
    assert_eq!(t.state(100.0), Liveness::Live);
    assert_eq!(t.state(110.0), Liveness::Live);
    assert_eq!(t.state(110.01), Liveness::Dead);
    assert_eq!(t.age(105.0), Some(5.0));
}

#[test]
fn progress_never_goes_backwards() {
    let mut t = LivenessTracker::new(10.0);
    t.progress(100.0);
    t.progress(90.0);
    assert_eq!(t.age(100.0), Some(0.0));
}

#[test]
fn reset() {
    let mut t = LivenessTracker::new(10.0);
    t.progress(100.0);
    t.reset();
    assert_eq!(t.state(101.0), Liveness::Unknown);
}
