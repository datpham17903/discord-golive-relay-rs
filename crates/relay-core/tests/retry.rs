//! Port of `tests/supervisor/test_retry.py`. Python seeds `random.Random`; the Rust policy has its
//! own PRNG (`seed`), so only the bounds are asserted, as in the Python tests.

use relay_core::supervisor::retry::RetryPolicy;

const DELAYS: [f64; 5] = [1.0, 2.0, 4.0, 8.0, 15.0];

#[test]
fn sequence_within_jitter_then_cooldown() {
    let mut p = RetryPolicy::new(&DELAYS, 60.0).seed(1);
    for base in DELAYS {
        let d = p.next_delay();
        assert!(base * 0.8 <= d && d <= base * 1.2, "{base} -> {d}");
    }
    assert_eq!(p.next_delay(), 60.0);
}

#[test]
fn restarts_after_cooldown() {
    let mut p = RetryPolicy::new(&DELAYS, 60.0).seed(1);
    for _ in 0..6 {
        p.next_delay();
    }
    let d = p.next_delay();
    assert!((0.8..=1.2).contains(&d), "{d}");
}

#[test]
fn reset() {
    let mut p = RetryPolicy::new(&DELAYS, 60.0).seed(1);
    p.next_delay();
    p.next_delay();
    p.reset();
    let d = p.next_delay();
    assert!((0.8..=1.2).contains(&d), "{d}");
}

#[test]
fn zero_jitter_exact() {
    let mut p = RetryPolicy::new(&DELAYS, 60.0).jitter(0.0);
    let got: Vec<f64> = (0..6).map(|_| p.next_delay()).collect();
    assert_eq!(got, vec![1.0, 2.0, 4.0, 8.0, 15.0, 60.0]);
}

#[test]
fn short_delays_list_repeats_last() {
    let mut p = RetryPolicy::new(&[1.0, 2.0], 60.0)
        .max_failures(4)
        .jitter(0.0);
    let got: Vec<f64> = (0..5).map(|_| p.next_delay()).collect();
    assert_eq!(got, vec![1.0, 2.0, 2.0, 2.0, 60.0]);
}

#[test]
fn jitter_bounds() {
    for seed in 0..20 {
        let mut p = RetryPolicy::new(&[10.0], 60.0).max_failures(1).seed(seed);
        let d = p.next_delay();
        assert!((8.0..=12.0).contains(&d), "seed {seed}: {d}");
    }
}

#[test]
fn default_policy_stays_within_jitter_without_seed() {
    let mut p = RetryPolicy::new(&[10.0], 60.0);
    let d = p.next_delay();
    assert!((8.0..=12.0).contains(&d), "{d}");
}
