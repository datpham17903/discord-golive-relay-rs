//! Direct tests for `supervisor/routing.py` and the string values of `supervisor/model.py` enums
//! (Python covers them indirectly through test_state/test_machine/test_service).

use indexmap::IndexMap;
use relay_core::control::state::PersistedRouting;
use relay_core::supervisor::model::{
    FatalReason, GatewayState, HoldReason, KeyframeMethod, Liveness, RelayState, StopReason,
};
use relay_core::supervisor::routing::Routing;
use serde_json::json;

fn routing(mirrors: &[(u64, u64)]) -> Routing {
    Routing {
        target_user_id: 10,
        channel_id: 20,
        mirrors: mirrors.iter().copied().collect(),
    }
}

#[test]
fn to_persisted_stringifies_mirror_channels_in_order() {
    let p = routing(&[(31, 41), (30, 40)]).to_persisted();
    assert_eq!(p.target_user_id, 10);
    assert_eq!(p.channel_id, 20);
    let keys: Vec<&str> = p.mirrors.keys().map(String::as_str).collect();
    assert_eq!(keys, ["31", "30"]);
    assert_eq!(p.mirrors["31"], 41);
}

#[test]
fn roundtrip() {
    for rt in [
        routing(&[]),
        routing(&[(30, 40)]),
        routing(&[(31, 41), (30, 40), (u64::MAX, 7)]),
    ] {
        let default = routing(&[(1, 2)]);
        assert_eq!(
            Routing::from_persisted(Some(&rt.to_persisted()), &default),
            rt
        );
    }
}

#[test]
fn from_persisted_none_returns_default() {
    let default = routing(&[(5, 6)]);
    assert_eq!(Routing::from_persisted(None, &default), default);
}

#[test]
fn from_persisted_reads_stored_values() {
    let p = PersistedRouting {
        target_user_id: 1,
        channel_id: 2,
        mirrors: IndexMap::from([("30".to_string(), 40), ("31".to_string(), 41)]),
    };
    let r = Routing::from_persisted(Some(&p), &routing(&[]));
    assert_eq!(
        r,
        Routing {
            target_user_id: 1,
            channel_id: 2,
            mirrors: IndexMap::from([(30, 40), (31, 41)]),
        }
    );
}

#[test]
fn mirror_order_does_not_affect_equality() {
    assert_eq!(routing(&[(1, 2), (3, 4)]), routing(&[(3, 4), (1, 2)]));
}

#[test]
fn enum_strings_match_python_values() {
    assert_eq!(GatewayState::Resuming.as_str(), "resuming");
    assert_eq!(RelayState::Quarantine.as_str(), "quarantine");
    assert_eq!(HoldReason::PipelineRebuild.as_str(), "pipeline_rebuild");
    assert_eq!(
        FatalReason::AccountOwnershipUncertain.as_str(),
        "ACCOUNT_OWNERSHIP_UNCERTAIN"
    );
    assert_eq!(StopReason::RelayScaledDown.as_str(), "relay_scaled_down");
    assert_eq!(
        StopReason::SourceLeftWhitelist.to_string(),
        "source_left_whitelist"
    );
    assert_eq!(Liveness::Unknown.as_str(), "unknown");
    assert_eq!(KeyframeMethod::Rewatch.as_str(), "rewatch");
    assert_eq!(
        StopReason::parse("hold_expired"),
        Some(StopReason::HoldExpired)
    );
    assert_eq!(StopReason::parse("nope"), None);
}

#[test]
fn durable_fatal_set() {
    assert!(FatalReason::AuthRejected.is_durable());
    assert!(FatalReason::AccountOwnershipUncertain.is_durable());
    assert!(FatalReason::RelayRejectedByServer.is_durable());
    assert!(!FatalReason::ConfigInvalid.is_durable());
}

#[test]
fn fatal_reason_from_latch() {
    let latch = |v: serde_json::Value| v.as_object().cloned();
    assert_eq!(FatalReason::from_latch(None), None);
    assert_eq!(FatalReason::from_latch(latch(json!({})).as_ref()), None);
    assert_eq!(
        FatalReason::from_latch(latch(json!({"reason": "RELAY_REJECTED_BY_SERVER"})).as_ref()),
        Some(FatalReason::RelayRejectedByServer)
    );
    // Unknown latch stays fatal rather than being cleared.
    assert_eq!(
        FatalReason::from_latch(latch(json!({"reason": "???"})).as_ref()),
        Some(FatalReason::AccountOwnershipUncertain)
    );
}
