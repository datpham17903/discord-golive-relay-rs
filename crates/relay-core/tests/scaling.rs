//! Port of `tests/supervisor/test_scaling.py`.

use relay_core::supervisor::scaling::relays_needed;

#[test]
fn formula_examples() {
    let with: Vec<usize> = [99, 100, 150, 200]
        .into_iter()
        .map(|n| relays_needed(n, 50, true))
        .collect();
    assert_eq!(with, vec![0, 1, 2, 3]);
    let without: Vec<usize> = [49, 50, 99, 100]
        .into_iter()
        .map(|n| relays_needed(n, 50, false))
        .collect();
    assert_eq!(without, vec![0, 1, 1, 2]);
}
