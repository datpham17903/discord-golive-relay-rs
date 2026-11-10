//! Chat-command rate limits. Pure: `now` (monotonic seconds) is passed in. Port of
//! `control/cooldown.py`.

use std::collections::{HashMap, HashSet};

/// Per-author window (one notice, then silence) and one global window between applied changes.
pub struct ChatCooldowns {
    author_s: f64,
    change_s: f64,
    /// author -> (window start, notice sent)
    authors: HashMap<u64, (f64, bool)>,
    last_change: Option<f64>,
    change_notified: HashSet<u64>,
}

impl ChatCooldowns {
    pub fn new(author_s: f64, change_s: f64) -> Self {
        Self {
            author_s,
            change_s,
            authors: HashMap::new(),
            last_change: None,
            change_notified: HashSet::new(),
        }
    }

    /// `None`: allowed, a new window starts. Else the seconds left to tell the author once; 0 =
    /// stay silent.
    pub fn author(&mut self, author_id: u64, now: f64) -> Option<u64> {
        let author_s = self.author_s;
        self.authors.retain(|_, v| now - v.0 < author_s);
        let Some(&(start, notified)) = self.authors.get(&author_id) else {
            if self.author_s > 0.0 {
                self.authors.insert(author_id, (now, false));
            }
            return None;
        };
        if notified {
            return Some(0);
        }
        self.authors.insert(author_id, (start, true));
        Some(((start + self.author_s - now).ceil() as u64).max(1))
    }

    /// Seconds until the next change may apply; 0 = now.
    pub fn change_wait(&self, now: f64) -> u64 {
        let Some(last) = self.last_change else {
            return 0;
        };
        let left = last + self.change_s - now;
        if left > 0.0 { left.ceil() as u64 } else { 0 }
    }

    /// True once per author per change window: the one reply a non-operator gets while the window
    /// runs.
    pub fn change_notice(&mut self, author_id: u64) -> bool {
        self.change_notified.insert(author_id)
    }

    pub fn changed(&mut self, now: f64) {
        self.last_change = Some(now);
        self.change_notified.clear();
    }

    /// Authors with a live window (Python tests read `_authors`).
    pub fn tracked_authors(&self) -> usize {
        self.authors.len()
    }
}
