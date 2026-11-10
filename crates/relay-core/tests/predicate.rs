//! Port of `tests/supervisor/test_predicate.py`.

use std::collections::HashSet;

use relay_core::supervisor::model::{GateFacts, GatewayState, Liveness, SourceStream, TargetVoice};
use relay_core::supervisor::predicate::evaluate_gate;

const TARGET: u64 = 10;
const CHAN: u64 = 20;

fn set(ids: &[u64]) -> HashSet<u64> {
    ids.iter().copied().collect()
}

fn good_stream() -> SourceStream {
    SourceStream {
        key: "k".to_string(),
        owner_id: TARGET,
        channel_id: CHAN,
        incarnation: 3,
        paused: Some(false),
        unavailable: false,
    }
}

fn good() -> GateFacts {
    GateFacts {
        desired_enabled: true,
        source_authorization_valid: true,
        gateway_state: GatewayState::Fresh,
        excluded_viewer_ids: set(&[99]),
        participant_view_fresh: true,
        participants: set(&[TARGET, 5]),
        quarantined_incarnation: None,
        configured_target_user_id: TARGET,
        allowed_voice_channel_id: CHAN,
        target: Some(TargetVoice {
            channel_id: Some(CHAN),
            self_stream: true,
        }),
        stream: Some(good_stream()),
        current_source_incarnation: Some(3),
        paused_capability_verified: true,
        has_verified_decodable_frame: true,
        relay_channel_id: Some(CHAN),
        relay_ownership_valid: true,
        input_liveness: Liveness::Live,
        inbound_ready: true,
        outbound_ready: true,
        generation_current: true,
        epoch_current: true,
    }
}

fn stream(f: impl FnOnce(&mut SourceStream)) -> Option<SourceStream> {
    let mut s = good_stream();
    f(&mut s);
    Some(s)
}

fn breaks() -> Vec<(&'static str, GateFacts)> {
    vec![
        (
            "enabled",
            GateFacts {
                desired_enabled: false,
                ..good()
            },
        ),
        (
            "authorization",
            GateFacts {
                source_authorization_valid: false,
                ..good()
            },
        ),
        (
            "gateway_fresh",
            GateFacts {
                gateway_state: GatewayState::Resuming,
                ..good()
            },
        ),
        (
            "excluded_viewers",
            GateFacts {
                participants: set(&[TARGET, 99]),
                ..good()
            },
        ),
        (
            "not_quarantined",
            GateFacts {
                quarantined_incarnation: Some(3),
                ..good()
            },
        ),
        (
            "target_id",
            GateFacts {
                target: None,
                ..good()
            },
        ),
        (
            "target_channel",
            GateFacts {
                target: Some(TargetVoice {
                    channel_id: Some(77),
                    self_stream: true,
                }),
                ..good()
            },
        ),
        (
            "target_streaming",
            GateFacts {
                target: Some(TargetVoice {
                    channel_id: Some(CHAN),
                    self_stream: false,
                }),
                ..good()
            },
        ),
        (
            "stream_owner",
            GateFacts {
                stream: stream(|s| s.owner_id = 1),
                ..good()
            },
        ),
        (
            "stream_channel",
            GateFacts {
                stream: stream(|s| s.channel_id = 77),
                ..good()
            },
        ),
        (
            "not_paused",
            GateFacts {
                stream: stream(|s| s.paused = Some(true)),
                ..good()
            },
        ),
        (
            "stream_available",
            GateFacts {
                stream: stream(|s| s.unavailable = true),
                ..good()
            },
        ),
        (
            "decodable_frame",
            GateFacts {
                has_verified_decodable_frame: false,
                ..good()
            },
        ),
        (
            "incarnation",
            GateFacts {
                current_source_incarnation: Some(2),
                ..good()
            },
        ),
        (
            "relay_channel",
            GateFacts {
                relay_channel_id: None,
                ..good()
            },
        ),
        (
            "ownership",
            GateFacts {
                relay_ownership_valid: false,
                ..good()
            },
        ),
        (
            "liveness",
            GateFacts {
                input_liveness: Liveness::Unknown,
                ..good()
            },
        ),
        (
            "rtc_ready",
            GateFacts {
                outbound_ready: false,
                ..good()
            },
        ),
        (
            "generation_epoch",
            GateFacts {
                epoch_current: false,
                ..good()
            },
        ),
    ]
}

#[test]
fn all_good_opens() {
    let d = evaluate_gate(&good());
    assert!(d.open && d.failed.is_empty());
}

#[test]
fn each_clause_flips_alone() {
    for (clause, facts) in breaks() {
        let d = evaluate_gate(&facts);
        assert!(!d.open, "{clause}");
        assert_eq!(d.failed, vec![clause], "{clause}");
    }
}

#[test]
fn covers_all_19_clauses() {
    assert_eq!(breaks().len(), 19);
}

#[test]
fn empty_excluded_needs_no_view() {
    let f = GateFacts {
        excluded_viewer_ids: HashSet::new(),
        participant_view_fresh: false,
        participants: HashSet::new(),
        ..good()
    };
    assert!(evaluate_gate(&f).open);
}

#[test]
fn excluded_needs_fresh_view() {
    let f = GateFacts {
        participant_view_fresh: false,
        ..good()
    };
    assert_eq!(evaluate_gate(&f).failed, vec!["excluded_viewers"]);
}

#[test]
fn pause_ignored_when_capability_unverified() {
    let f = GateFacts {
        paused_capability_verified: false,
        stream: stream(|s| s.paused = Some(true)),
        ..good()
    };
    assert!(evaluate_gate(&f).open);
}

#[test]
fn pause_unknown_value_passes() {
    let f = GateFacts {
        stream: stream(|s| s.paused = None),
        ..good()
    };
    assert!(evaluate_gate(&f).open);
}

#[test]
fn no_stream_closes() {
    let f = GateFacts {
        stream: None,
        ..good()
    };
    assert!(!evaluate_gate(&f).open);
}
