//! Port of `tests/supervisor/test_ownership.py`.

use relay_core::supervisor::model::RelayVoiceObserved;
use relay_core::supervisor::ownership::{OwnershipDetector, OwnershipVerdict};

const CHAN: u64 = 20;

fn obs(session: Option<&str>, channel: Option<u64>) -> RelayVoiceObserved {
    RelayVoiceObserved {
        session_id: session.map(str::to_string),
        channel_id: channel,
    }
}

fn armed() -> OwnershipDetector {
    let mut d = OwnershipDetector::new();
    d.arm(Some("s1".to_string()));
    d
}

#[test]
fn ok() {
    assert_eq!(
        armed().classify(&obs(Some("s1"), Some(CHAN)), CHAN, 0.0),
        OwnershipVerdict::Ok
    );
}

#[test]
fn same_session_null_channel_displaced() {
    assert_eq!(
        armed().classify(&obs(Some("s1"), None), CHAN, 0.0),
        OwnershipVerdict::Displaced
    );
}

#[test]
fn same_session_other_channel_displaced() {
    assert_eq!(
        armed().classify(&obs(Some("s1"), Some(99)), CHAN, 0.0),
        OwnershipVerdict::Displaced
    );
}

#[test]
fn other_session_uncertain() {
    assert_eq!(
        armed().classify(&obs(Some("s2"), Some(CHAN)), CHAN, 0.0),
        OwnershipVerdict::OwnershipUncertain
    );
}

#[test]
fn missing_session_missing_evidence() {
    assert_eq!(
        armed().classify(&obs(None, Some(CHAN)), CHAN, 0.0),
        OwnershipVerdict::MissingEvidence
    );
}

#[test]
fn missing_baseline_missing_evidence() {
    let mut d = OwnershipDetector::new();
    d.arm(None);
    assert_eq!(
        d.classify(&obs(Some("s1"), Some(CHAN)), CHAN, 0.0),
        OwnershipVerdict::MissingEvidence
    );
}

#[test]
fn disarmed_ignored() {
    let mut d = armed();
    d.disarm();
    assert_eq!(
        d.classify(&obs(Some("s2"), None), CHAN, 0.0),
        OwnershipVerdict::Ignored
    );
}

#[test]
fn never_armed_ignored() {
    assert_eq!(
        OwnershipDetector::new().classify(&obs(Some("s2"), None), CHAN, 0.0),
        OwnershipVerdict::Ignored
    );
}

#[test]
fn exempt_window() {
    let mut d = armed();
    d.exempt(5.0, 100.0);
    assert_eq!(
        d.classify(&obs(Some("s2"), None), CHAN, 104.9),
        OwnershipVerdict::Ignored
    );
    assert_eq!(
        d.classify(&obs(Some("s2"), None), CHAN, 105.0),
        OwnershipVerdict::OwnershipUncertain
    );
}

#[test]
fn verdict_strings_match_python() {
    assert_eq!(OwnershipVerdict::Ok.as_str(), "ok");
    assert_eq!(OwnershipVerdict::Displaced.as_str(), "displaced");
    assert_eq!(
        OwnershipVerdict::OwnershipUncertain.as_str(),
        "ownership_uncertain"
    );
    assert_eq!(
        OwnershipVerdict::MissingEvidence.as_str(),
        "missing_evidence"
    );
    assert_eq!(OwnershipVerdict::Ignored.as_str(), "ignored");
}
