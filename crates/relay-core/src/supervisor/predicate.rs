//! Source gate predicate (spec §6): a pure conjunction of named clauses. Port of
//! `supervisor/predicate.py`.

use super::model::{GateDecision, GateFacts, GatewayState, Liveness};

/// Evaluate every clause; `failed` lists the names of those that are false, in clause order.
///
/// Clauses about the target or the stream pass vacuously when that object is absent, because
/// `target_id` / `stream_available` already fail then.
pub fn evaluate_gate(f: &GateFacts) -> GateDecision {
    let t = f.target.as_ref();
    let s = f.stream.as_ref();
    let excluded_ok = if f.excluded_viewer_ids.is_empty() {
        true
    } else {
        f.participant_view_fresh && f.excluded_viewer_ids.is_disjoint(&f.participants)
    };
    let quarantined = f.quarantined_incarnation.is_some_and(|q| {
        Some(q) == f.current_source_incarnation || Some(q) == s.map(|s| s.incarnation)
    });
    let paused = f.paused_capability_verified && s.is_some_and(|s| s.paused == Some(true));
    let incarnation_ok = match s {
        None => true,
        Some(s) => f
            .current_source_incarnation
            .is_some_and(|cur| s.incarnation == cur),
    };

    let clauses: [(&'static str, bool); 19] = [
        ("enabled", f.desired_enabled),
        ("authorization", f.source_authorization_valid),
        ("gateway_fresh", f.gateway_state == GatewayState::Fresh),
        ("excluded_viewers", excluded_ok),
        ("not_quarantined", !quarantined),
        ("target_id", t.is_some()),
        (
            "target_channel",
            t.is_none_or(|t| t.channel_id == Some(f.allowed_voice_channel_id)),
        ),
        ("target_streaming", t.is_none_or(|t| t.self_stream)),
        (
            "stream_owner",
            s.is_none_or(|s| s.owner_id == f.configured_target_user_id),
        ),
        (
            "stream_channel",
            s.is_none_or(|s| s.channel_id == f.allowed_voice_channel_id),
        ),
        ("not_paused", !paused),
        ("stream_available", s.is_some_and(|s| !s.unavailable)),
        ("decodable_frame", f.has_verified_decodable_frame),
        ("incarnation", incarnation_ok),
        (
            "relay_channel",
            f.relay_channel_id == Some(f.allowed_voice_channel_id),
        ),
        ("ownership", f.relay_ownership_valid),
        ("liveness", f.input_liveness == Liveness::Live),
        ("rtc_ready", f.inbound_ready && f.outbound_ready),
        ("generation_epoch", f.generation_current && f.epoch_current),
    ];
    let failed: Vec<&'static str> = clauses
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(name, _)| *name)
        .collect();
    GateDecision {
        open: failed.is_empty(),
        failed,
    }
}
