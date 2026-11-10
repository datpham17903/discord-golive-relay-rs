//! Relay voice ownership classification (spec §12). Port of `supervisor/ownership.py`.

use super::model::RelayVoiceObserved;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OwnershipVerdict {
    Ok,
    /// Same session, moved or disconnected -> QUARANTINE.
    Displaced,
    /// Other session -> FATAL.
    OwnershipUncertain,
    /// Close the gate, do not guess.
    MissingEvidence,
    /// Disarmed or inside an exemption window.
    Ignored,
}

impl OwnershipVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Displaced => "displaced",
            Self::OwnershipUncertain => "ownership_uncertain",
            Self::MissingEvidence => "missing_evidence",
            Self::Ignored => "ignored",
        }
    }
}

/// Compares the relay's own voice state with the baseline voice session.
#[derive(Debug, Clone)]
pub struct OwnershipDetector {
    armed: bool,
    baseline: Option<String>,
    exempt_until: f64,
}

impl Default for OwnershipDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnershipDetector {
    pub fn new() -> Self {
        Self {
            armed: false,
            baseline: None,
            exempt_until: f64::NEG_INFINITY,
        }
    }

    /// Start detecting against `baseline` (a voice session id).
    pub fn arm(&mut self, baseline: Option<String>) {
        self.armed = true;
        self.baseline = baseline;
    }

    pub fn disarm(&mut self) {
        self.armed = false;
        self.baseline = None;
    }

    /// Ignore events until `now + seconds` (connect, move, disconnect, cleanup we issued).
    pub fn exempt(&mut self, seconds: f64, now: f64) {
        self.exempt_until = self.exempt_until.max(now + seconds);
    }

    pub fn classify(
        &self,
        obs: &RelayVoiceObserved,
        allowed_channel: u64,
        now: f64,
    ) -> OwnershipVerdict {
        if !self.armed || now < self.exempt_until {
            return OwnershipVerdict::Ignored;
        }
        let (Some(session), Some(baseline)) = (obs.session_id.as_deref(), self.baseline.as_deref())
        else {
            return OwnershipVerdict::MissingEvidence;
        };
        if session != baseline {
            return OwnershipVerdict::OwnershipUncertain;
        }
        if obs.channel_id != Some(allowed_channel) {
            return OwnershipVerdict::Displaced;
        }
        OwnershipVerdict::Ok
    }
}
