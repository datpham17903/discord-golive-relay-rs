//! Port of `supervisor/scaling.py`.

/// Number of output relays needed for `viewers` when each Go Live holds `cap` viewers.
pub fn relays_needed(viewers: usize, cap: usize, ingest_publishes: bool) -> usize {
    let base = (cap - 1) + if ingest_publishes { cap } else { 0 };
    viewers.saturating_sub(base).div_ceil(cap)
}
