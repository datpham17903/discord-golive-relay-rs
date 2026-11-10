//! The two clocks of the crate. They are not interchangeable: values from one must never be compared
//! with or subtracted from values of the other.
//!
//! - [`wall_clock`]: Unix-epoch seconds. Expected by `Supervisor::handle` (its `now` argument) and by every
//!   persisted or reported `since` field (state.json, status), because those outlive the process.
//! - [`monotonic`]: seconds since the first call in this process, never goes backwards. Expected by all
//!   worker and publisher timing (`Worker::handle`/`tick` `now`, `mono_received`, presentation times, liveness
//!   progress timestamps, cooldowns).

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Monotonic seconds since the first call in this process. Every `mono_received` and worker/publisher `now`
/// must use it.
pub fn monotonic() -> f64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// Unix-epoch seconds (`time.time()`); `0.0` if the system clock is before the epoch. Used for
/// `Supervisor::handle` timestamps and state `since` fields, never for worker/publisher timing.
pub fn wall_clock() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_never_decreases() {
        let a = monotonic();
        let b = monotonic();
        assert!(a >= 0.0 && b >= a);
    }

    #[test]
    fn wall_clock_is_unix_epoch_seconds() {
        // 2020-01-01 as a lower bound; monotonic() is process-relative and far below it.
        assert!(wall_clock() > 1_577_836_800.0);
        assert!(monotonic() < wall_clock());
    }
}
