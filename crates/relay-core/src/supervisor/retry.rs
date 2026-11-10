//! Retry schedule: delays with jitter, then one cooldown (spec §13). Port of `supervisor/retry.py`.

use std::time::{SystemTime, UNIX_EPOCH};

/// Small deterministic PRNG (SplitMix64); jitter does not need more, and it avoids a dependency.
#[derive(Debug, Clone)]
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn from_clock() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self(seed)
    }

    /// Uniform in `[0, 1)`.
    pub(super) fn next_f64(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// `next_delay()` yields `delays` (with jitter) for `max_failures` calls, then `cooldown` once, then
/// starts over.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    delays: Vec<f64>,
    cooldown: f64,
    max: u32,
    jitter: f64,
    rng: SplitMix64,
    failures: u32,
}

impl RetryPolicy {
    /// Defaults of the Python constructor: `max_failures=5`, `jitter=0.2`, clock-seeded RNG.
    pub fn new(delays: &[f64], cooldown: f64) -> Self {
        Self {
            delays: delays.to_vec(),
            cooldown,
            max: 5,
            jitter: 0.2,
            rng: SplitMix64::from_clock(),
            failures: 0,
        }
    }

    pub fn max_failures(mut self, max: u32) -> Self {
        self.max = max;
        self
    }

    pub fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter;
        self
    }

    /// Fixed RNG seed, for reproducible schedules (tests).
    pub fn seed(mut self, seed: u64) -> Self {
        self.rng = SplitMix64(seed);
        self
    }

    pub fn next_delay(&mut self) -> f64 {
        if self.failures >= self.max {
            self.failures = 0;
            return self.cooldown;
        }
        let idx = (self.failures as usize).min(self.delays.len() - 1);
        let base = self.delays[idx];
        self.failures += 1;
        let uniform = -self.jitter + (2.0 * self.jitter) * self.rng.next_f64();
        base * (1.0 + uniform)
    }

    pub fn reset(&mut self) {
        self.failures = 0;
    }
}
