//! RTP timestamp unwrapping (port of `worker/timeline.py`; the unused `Timeline` class is not ported).

const MOD: i64 = 1 << 32;

/// Extends a 32-bit RTP timestamp to a monotonic-ish integer axis.
#[derive(Debug, Clone)]
pub struct RtpUnwrapper {
    pub clock_rate: u32,
    last_raw: Option<u32>,
    value: i64,
}

impl RtpUnwrapper {
    pub fn new(clock_rate: u32) -> Self {
        Self {
            clock_rate,
            last_raw: None,
            value: 0,
        }
    }

    pub fn unwrap(&mut self, ts: u32) -> i64 {
        match self.last_raw {
            None => self.value = i64::from(ts),
            Some(last) => {
                let mut delta = i64::from(ts.wrapping_sub(last));
                if delta >= MOD / 2 {
                    delta -= MOD; // reordered packet: step backwards
                }
                self.value += delta;
            }
        }
        self.last_raw = Some(ts);
        self.value
    }
}
