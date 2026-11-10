//! Port of `tests/supervisor/test_codec.py`. `test_pinned_mode_only_h264` is dropped: the Rust build
//! is always passthrough, so the controller has no `pinned` mode.

use relay_core::supervisor::codec::{CodecAction, CodecController, CodecEvent, OutputId};
use relay_media::codec::down_from;

const A: OutputId = OutputId::Account(1);
const B: OutputId = OutputId::Account(2);
const V: OutputId = OutputId::Account(3);

fn dn(codec: &str) -> &'static [&'static str] {
    down_from(codec).unwrap()
}

fn h(c: &mut CodecController, ev: CodecEvent, now: f64) -> Vec<CodecAction> {
    c.handle(ev, now).unwrap()
}

fn source(codec: &str, eligible: bool) -> CodecEvent {
    CodecEvent::SourceCodec {
        codec: codec.to_string(),
        eligible,
    }
}

fn added(id: OutputId, codec: &str, offer_top: &str) -> CodecEvent {
    CodecEvent::OutputAdded {
        output_id: id,
        codec: codec.to_string(),
        offer_top: offer_top.to_string(),
        bound: None,
    }
}

fn out_codec(id: OutputId, codec: &str) -> CodecEvent {
    CodecEvent::OutputCodec {
        output_id: id,
        codec: codec.to_string(),
    }
}

fn bound(id: OutputId, codec: Option<&str>) -> CodecEvent {
    CodecEvent::OutputBound {
        output_id: id,
        codec: codec.map(str::to_string),
    }
}

fn update_watch(codec: &str) -> CodecAction {
    CodecAction::UpdateWatch { codecs: dn(codec) }
}

fn update_output(id: OutputId, codec: &str) -> CodecAction {
    CodecAction::UpdateOutput {
        output_id: id,
        codecs: dn(codec),
    }
}

fn swap(id: OutputId, codec: &str) -> CodecAction {
    CodecAction::SwapOutput {
        output_id: id,
        codec: codec.to_string(),
    }
}

fn is_swap(a: &CodecAction) -> bool {
    matches!(a, CodecAction::SwapOutput { .. })
}

fn settle_h264(c: &mut CodecController) {
    h(c, source("AV1", true), 0.0);
    h(c, added(A, "AV1", "AV1"), 0.0);
    h(c, added(B, "H264", "AV1"), 0.0);
    h(c, source("H264", true), 1.0);
    h(c, out_codec(A, "H264"), 1.1);
}

#[test]
fn test_initial_outputs_pinned_to_source_and_swapped() {
    let mut c = CodecController::new();
    assert_eq!(h(&mut c, source("H264", true), 0.0), vec![]);
    let acts = h(&mut c, added(A, "AV1", "AV1"), 0.0);
    assert_eq!(acts, vec![update_output(A, "H264")]);
    assert_eq!(h(&mut c, out_codec(A, "H264"), 0.1), vec![swap(A, "H264")]);
    assert_eq!(c.effective(), Some("H264"));
}

#[test]
fn test_divergent_outputs_converge() {
    let mut c = CodecController::new();
    h(&mut c, source("AV1", true), 0.0);
    assert_eq!(h(&mut c, added(A, "AV1", "AV1"), 0.0), vec![swap(A, "AV1")]);
    let acts = h(&mut c, added(B, "H264", "AV1"), 0.0);
    assert_eq!(acts, vec![update_watch("H264")]);
    assert_eq!(c.switching(), Some(("AV1", "H264")));
    let acts = h(&mut c, source("H264", true), 1.0);
    assert_eq!(acts, vec![update_output(A, "H264"), swap(B, "H264")]);
    assert_eq!(h(&mut c, out_codec(A, "H264"), 1.1), vec![swap(A, "H264")]);
    assert_eq!(c.switching(), None);
}

#[test]
fn test_weak_viewer_leaves_upgrades() {
    let mut c = CodecController::new();
    settle_h264(&mut c);
    let acts = h(&mut c, out_codec(B, "AV1"), 10.0);
    assert_eq!(acts, vec![update_watch("AV1")]);
    assert_eq!(c.switching(), Some(("H264", "AV1")));
    let acts = h(&mut c, source("AV1", true), 10.5);
    assert!(acts.contains(&update_output(A, "AV1")));
    assert_eq!(h(&mut c, out_codec(A, "AV1"), 10.6), vec![swap(A, "AV1")]);
    assert_eq!(c.effective(), Some("AV1"));
    assert_eq!(c.switching(), None);
}

#[test]
fn test_partial_upgrade_settles() {
    let mut c = CodecController::new();
    settle_h264(&mut c);
    h(&mut c, out_codec(B, "AV1"), 10.0);
    h(&mut c, source("AV1", true), 10.5);
    // b only follows to H265 after the raised offer
    let mut acts = h(&mut c, out_codec(B, "H265"), 10.6);
    acts.extend(h(&mut c, out_codec(A, "H265"), 10.6));
    acts.extend(h(&mut c, source("H265", true), 11.0));
    assert!(acts.contains(&update_watch("H265")));
    let swaps_b: Vec<_> = acts
        .iter()
        .filter(|x| matches!(x, CodecAction::SwapOutput { output_id, .. } if *output_id == B))
        .cloned()
        .collect();
    assert_eq!(swaps_b, vec![swap(B, "H265")]);
    assert_eq!(c.effective(), Some("H265"));
    let mut acts = vec![];
    for i in 0..10 {
        acts.extend(h(&mut c, CodecEvent::Tick, f64::from(12 + i)));
    }
    assert_eq!(acts, vec![]);
    assert_eq!(c.effective(), Some("H265"));
}

#[test]
fn test_target_side_drop_and_recovery() {
    let mut c = CodecController::new();
    h(&mut c, source("AV1", true), 0.0);
    h(&mut c, added(A, "AV1", "AV1"), 0.0);
    let acts = h(&mut c, source("H264", true), 5.0);
    assert_eq!(acts, vec![update_output(A, "H264")]);
    assert_eq!(h(&mut c, out_codec(A, "H264"), 5.1), vec![swap(A, "H264")]);
    let acts = h(&mut c, source("AV1", true), 9.0);
    assert_eq!(acts, vec![update_output(A, "AV1")]);
    assert_eq!(h(&mut c, out_codec(A, "AV1"), 9.1), vec![swap(A, "AV1")]);
}

#[test]
fn test_upgrade_wait_expires_pins_outputs() {
    let mut c = CodecController::new();
    h(&mut c, source("H264", true), 0.0);
    h(&mut c, added(A, "H264", "AV1"), 0.0);
    h(&mut c, added(B, "AV1", "AV1"), 0.0);
    h(&mut c, out_codec(A, "AV1"), 10.0); // viewer left: watch widened at 10
    assert_eq!(h(&mut c, CodecEvent::Tick, 11.0), vec![]);
    let acts = h(&mut c, CodecEvent::Tick, 13.5);
    assert!(acts.contains(&update_output(A, "H264")));
}

#[test]
fn test_swap_only_when_reported_equals_e() {
    let mut c = CodecController::new();
    h(&mut c, source("H265", true), 0.0);
    let acts = h(&mut c, added(A, "H264", "AV1"), 0.0);
    assert!(!acts.iter().any(is_swap));
    let acts = h(&mut c, out_codec(A, "H265"), 1.0);
    assert!(acts.contains(&swap(A, "H265")));
}

#[test]
fn test_short_hold_no_renegotiation() {
    let mut c = CodecController::new();
    h(&mut c, source("H265", true), 0.0);
    h(&mut c, added(A, "H265", "H265"), 0.0);
    assert_eq!(h(&mut c, CodecEvent::Hold, 1.0), vec![]);
    assert_eq!(h(&mut c, CodecEvent::Tick, 2.5), vec![]);
    assert_eq!(h(&mut c, CodecEvent::Live, 2.6), vec![]);
    assert_eq!(c.effective(), Some("H265"));
}

#[test]
fn test_hold_then_live_restores() {
    let mut c = CodecController::new();
    h(&mut c, source("H265", true), 0.0);
    h(&mut c, added(A, "H265", "H265"), 0.0);
    h(&mut c, CodecEvent::Hold, 1.0);
    let acts = h(&mut c, CodecEvent::Tick, 3.5);
    assert_eq!(acts, vec![update_output(A, "H264")]);
    assert_eq!(c.effective(), Some("H264"));
    assert_eq!(h(&mut c, out_codec(A, "H264"), 3.6), vec![swap(A, "H264")]);
    let acts = h(&mut c, CodecEvent::Live, 5.0);
    assert_eq!(c.effective(), Some("H265"));
    assert!(acts.contains(&update_output(A, "H265")));
    assert_eq!(h(&mut c, out_codec(A, "H265"), 5.1), vec![swap(A, "H265")]);
}

#[test]
fn test_ineligible_source_forces_h264() {
    let mut c = CodecController::new();
    h(&mut c, source("AV1", false), 0.0);
    assert_eq!(c.effective(), Some("H264"));
    let acts = h(&mut c, added(A, "AV1", "AV1"), 0.0);
    assert_eq!(acts, vec![update_output(A, "H264")]);
}

#[test]
fn test_output_gone_recomputes_watch() {
    let mut c = CodecController::new();
    h(&mut c, source("H264", true), 0.0);
    let acts = h(&mut c, added(B, "H264", "AV1"), 0.0);
    assert!(acts.contains(&update_watch("H264")));
    assert_eq!(
        h(&mut c, CodecEvent::OutputGone { output_id: B }, 1.0),
        vec![update_watch("AV1")]
    );
}

#[test]
fn test_vp8_natural_viewer_drops_below_h264() {
    let mut c = CodecController::new();
    h(&mut c, source("H264", true), 0.0);
    let acts = h(&mut c, added(V, "VP8", "AV1"), 0.0);
    assert_eq!(acts, vec![update_watch("VP8")]);
    assert_eq!(h(&mut c, source("VP8", true), 1.0), vec![swap(V, "VP8")]);
    assert_eq!(c.effective(), Some("VP8"));
}

#[test]
fn test_idempotent_no_duplicate_actions() {
    let mut c = CodecController::new();
    h(&mut c, source("H264", true), 0.0);
    h(&mut c, added(A, "AV1", "AV1"), 0.0);
    h(&mut c, added(B, "VP8", "AV1"), 0.0);
    for t in 1..8 {
        assert_eq!(h(&mut c, CodecEvent::Tick, f64::from(t)), vec![]);
    }
    assert_eq!(h(&mut c, source("H264", true), 9.0), vec![]);
}

#[test]
fn test_swap_bound_elsewhere_is_reemitted() {
    let mut c = CodecController::new();
    h(&mut c, source("H265", true), 0.0);
    h(&mut c, added(A, "H264", "AV1"), 0.0);
    assert!(h(&mut c, out_codec(A, "H265"), 1.0).contains(&swap(A, "H265")));
    // op 14 moved the output to H264 between emission and execution of the swap
    h(&mut c, out_codec(A, "H264"), 1.4);
    assert_eq!(h(&mut c, bound(A, Some("H264")), 1.5), vec![]);
    assert!(h(&mut c, out_codec(A, "H265"), 2.0).contains(&swap(A, "H265")));
    assert_eq!(h(&mut c, bound(A, None), 2.5), vec![]); // failed swap: no immediate retry
    assert_eq!(h(&mut c, CodecEvent::Tick, 3.0), vec![]); // retry cooldown
    assert_eq!(h(&mut c, CodecEvent::Tick, 7.6), vec![swap(A, "H265")]);
}

#[test]
fn test_action_failed_reemits_on_next_reconcile() {
    let mut c = CodecController::new();
    h(&mut c, source("H264", true), 0.0);
    let acts = h(&mut c, added(B, "H264", "AV1"), 0.0);
    assert_eq!(acts.len(), 2);
    let w = acts[0].clone();
    assert!(matches!(w, CodecAction::UpdateWatch { .. }));
    assert_eq!(h(&mut c, CodecEvent::ActionFailed(w.clone()), 0.5), vec![]);
    assert_eq!(h(&mut c, CodecEvent::Tick, 1.0), vec![w]);
    let u = update_output(A, "H264");
    assert_eq!(h(&mut c, added(A, "AV1", "AV1"), 1.0), vec![u.clone()]);
    h(&mut c, CodecEvent::ActionFailed(u.clone()), 1.1);
    assert_eq!(h(&mut c, CodecEvent::Tick, 1.2), vec![u]);
    let s = swap(A, "H264");
    assert_eq!(h(&mut c, out_codec(A, "H264"), 1.3), vec![s.clone()]);
    h(&mut c, CodecEvent::ActionFailed(s.clone()), 1.4);
    assert_eq!(h(&mut c, CodecEvent::Tick, 1.5), vec![]); // retry cooldown
    assert_eq!(h(&mut c, CodecEvent::Tick, 6.5), vec![s]);
}

#[test]
fn test_failed_swap_retried_after_cooldown() {
    let mut c = CodecController::new();
    h(&mut c, source("H265", true), 0.0);
    let acts = h(&mut c, added(A, "H265", "AV1"), 0.0);
    assert!(acts.contains(&swap(A, "H265")));
    h(&mut c, bound(A, None), 0.1);
    assert!(!h(&mut c, CodecEvent::Tick, 1.0).iter().any(is_swap));
    assert!(h(&mut c, CodecEvent::Tick, 5.2).contains(&swap(A, "H265")));
}
