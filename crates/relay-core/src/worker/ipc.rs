//! Bounded in-process channels for the worker IPC (port of `worker/ipc.py`).
//!
//! Limits are enforced per kind, so a full channel makes `put_nowait` return `false` instead of
//! blocking or growing without bound. Items are handed over as-is (the worker is a thread, not a process).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// `get` found nothing (Python `queue.Empty`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("channel is empty")]
pub struct Empty;

#[derive(Debug, Clone, Copy)]
struct Limit {
    max_items: usize,
    max_bytes: Option<usize>,
}

struct Inner<T> {
    queue: VecDeque<(&'static str, usize, T)>,
    /// kind -> (items, bytes) currently queued.
    used: HashMap<&'static str, (usize, usize)>,
}

/// A FIFO with per-kind item and byte limits.
pub struct BoundedChannel<T> {
    limits: HashMap<&'static str, Limit>,
    inner: Mutex<Inner<T>>,
    ready: Condvar,
}

impl<T> BoundedChannel<T> {
    /// `limits`: (kind, max_items, max_bytes or `None` for unlimited bytes).
    pub fn new(limits: &[(&'static str, usize, Option<usize>)]) -> Self {
        Self {
            limits: limits
                .iter()
                .map(|&(kind, max_items, max_bytes)| {
                    (
                        kind,
                        Limit {
                            max_items,
                            max_bytes,
                        },
                    )
                })
                .collect(),
            inner: Mutex::new(Inner {
                queue: VecDeque::new(),
                used: limits.iter().map(|&(kind, ..)| (kind, (0, 0))).collect(),
            }),
            ready: Condvar::new(),
        }
    }

    fn limit(&self, kind: &str) -> Limit {
        *self
            .limits
            .get(kind)
            .unwrap_or_else(|| panic!("unknown channel kind {kind:?}"))
    }

    /// Queue `item` unless its kind is at its item or byte limit. Panics on a kind not in the limits.
    pub fn put_nowait(&self, item: T, kind: &'static str, size: usize) -> bool {
        let limit = self.limit(kind);
        let mut inner = self.inner.lock();
        let used = inner.used.get_mut(kind).expect("kind has a counter");
        if used.0 + 1 > limit.max_items || limit.max_bytes.is_some_and(|max| used.1 + size > max) {
            return false;
        }
        used.0 += 1;
        used.1 += size;
        inner.queue.push_back((kind, size, item));
        drop(inner);
        self.ready.notify_one();
        true
    }

    fn pop(inner: &mut Inner<T>) -> Option<T> {
        let (kind, size, item) = inner.queue.pop_front()?;
        let used = inner.used.get_mut(kind).expect("kind has a counter");
        used.0 -= 1;
        used.1 -= size;
        Some(item)
    }

    /// Next item; waits up to `timeout` seconds (`None`: forever), `Err(Empty)` on timeout.
    pub fn get(&self, timeout: Option<f64>) -> Result<T, Empty> {
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t.max(0.0)));
        let mut inner = self.inner.lock();
        loop {
            if let Some(item) = Self::pop(&mut inner) {
                return Ok(item);
            }
            match deadline {
                None => self.ready.wait(&mut inner),
                Some(deadline) => {
                    if self.ready.wait_until(&mut inner, deadline).timed_out() {
                        return Self::pop(&mut inner).ok_or(Empty);
                    }
                }
            }
        }
    }

    pub fn get_nowait(&self) -> Result<T, Empty> {
        self.get(Some(0.0))
    }

    pub fn full(&self, kind: &str) -> bool {
        let limit = self.limit(kind);
        self.inner.lock().used[kind].0 >= limit.max_items
    }

    pub fn pending(&self, kind: &str) -> usize {
        self.limit(kind);
        self.inner.lock().used[kind].0
    }
}
