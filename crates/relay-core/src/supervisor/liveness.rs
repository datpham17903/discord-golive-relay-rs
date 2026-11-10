//! Source liveness from monotonic progress timestamps (spec §10). Port of `supervisor/liveness.py`.

use super::model::Liveness;

/// LIVE while the last progress is at most `t_dead` seconds old.
#[derive(Debug, Clone)]
pub struct LivenessTracker {
    t_dead: f64,
    last: Option<f64>,
}

impl LivenessTracker {
    pub fn new(t_dead: f64) -> Self {
        Self { t_dead, last: None }
    }

    pub fn progress(&mut self, at: f64) {
        if self.last.is_none_or(|last| at > last) {
            self.last = Some(at);
        }
    }

    pub fn age(&self, now: f64) -> Option<f64> {
        self.last.map(|last| (now - last).max(0.0))
    }

    pub fn state(&self, now: f64) -> Liveness {
        match self.age(now) {
            None => Liveness::Unknown,
            Some(age) if age <= self.t_dead => Liveness::Live,
            Some(_) => Liveness::Dead,
        }
    }

    pub fn reset(&mut self) {
        self.last = None;
    }
}
