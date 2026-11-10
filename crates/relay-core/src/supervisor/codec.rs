//! Pure, level-triggered controller that keeps the watch list, each output's codec offer, each
//! output's bound codec and the status codec consistent. Port of `supervisor/codec.py`.
//!
//! The Rust build is always passthrough, so Python's `pinned` mode (H.264-only offers, no watch
//! updates) does not exist here. Python's `output_id: object` is `OutputId`: the ingest output or
//! an output account.
//!
//! Python dataclass -> Rust mapping: `CodecEvent` is the union of `SourceCodec`, `OutputAdded`,
//! `OutputCodec`, `OutputGone`, `OutputBound`, `ActionFailed`, `Hold`, `Live` and `Tick`;
//! `CodecAction` is the union of `UpdateWatch`, `UpdateOutput` and `SwapOutput`. Offers are the
//! `relay_media::codec::down_from` slices.

use indexmap::IndexMap;
use relay_media::codec::{CODEC_ORDER, UnknownCodec, down_from, lowest, rank};

const NEVER: f64 = f64::NEG_INFINITY;

type Codecs = &'static [&'static str];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputId {
    /// The ingest account's own output (Python `"ingest"`).
    Ingest,
    /// An extra output account, by relay user id.
    Account(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecEvent {
    SourceCodec {
        codec: String,
        eligible: bool,
    },
    OutputAdded {
        output_id: OutputId,
        codec: String,
        offer_top: String,
        /// Codec of the source it was started with, if any.
        bound: Option<String>,
    },
    OutputCodec {
        output_id: OutputId,
        codec: String,
    },
    OutputGone {
        output_id: OutputId,
    },
    /// What the output's source is bound to after a swap; `None` = the swap failed (re-emitted
    /// while reported == E).
    OutputBound {
        output_id: OutputId,
        codec: Option<String>,
    },
    /// An emitted action could not be carried out: forget its memo so the next reconcile emits it
    /// again.
    ActionFailed(CodecAction),
    Hold,
    Live,
    Tick,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecAction {
    UpdateWatch { codecs: Codecs },
    UpdateOutput { output_id: OutputId, codecs: Codecs },
    SwapOutput { output_id: OutputId, codec: String },
}

#[derive(Debug)]
struct Out {
    reported: String,
    offer_top: String,
    requested: Option<Codecs>,
    bound: Option<String>,
    /// A raised offer needs a server op 14 before the output can be judged "natural"; without one
    /// (viewers unchanged) the wait simply expires.
    awaiting_until: f64,
    /// A failed swap is retried no sooner than this.
    swap_retry_at: f64,
}

#[derive(Debug)]
pub struct CodecController {
    pub upgrade_wait_s: f64,
    pub hold_force_s: f64,
    pub swap_retry_s: f64,
    source: Option<String>,
    eligible: bool,
    held_since: Option<f64>,
    last_watch: Option<Codecs>,
    watch_changed_at: f64,
    w: String,
    outs: IndexMap<OutputId, Out>,
    e: Option<String>,
    switching: Option<(String, String)>,
}

impl Default for CodecController {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecController {
    pub fn new() -> Self {
        Self {
            upgrade_wait_s: 3.0,
            hold_force_s: 2.0,
            swap_retry_s: 5.0,
            source: None,
            eligible: true,
            held_since: None,
            last_watch: Some(&CODEC_ORDER[..]),
            watch_changed_at: NEVER,
            w: "AV1".to_string(),
            outs: IndexMap::new(),
            e: None,
            switching: None,
        }
    }

    /// The codec the pipeline should produce now.
    pub fn effective(&self) -> Option<&str> {
        self.e.as_deref()
    }

    /// Outputs currently bound to the effective codec.
    pub fn bound_outputs(&self) -> Vec<OutputId> {
        let Some(e) = &self.e else {
            return Vec::new();
        };
        self.outs
            .iter()
            .filter(|(_, o)| o.bound.as_ref() == Some(e))
            .map(|(id, _)| *id)
            .collect()
    }

    /// The codec `output_id` last reported; `None` when the output is not registered.
    pub fn reported(&self, output_id: OutputId) -> Option<&str> {
        self.outs.get(&output_id).map(|o| o.reported.as_str())
    }

    /// `(effective, watch)` while the pipeline is being moved between two codecs.
    pub fn switching(&self) -> Option<(&str, &str)> {
        self.switching
            .as_ref()
            .map(|(e, w)| (e.as_str(), w.as_str()))
    }

    /// Errors on a codec name outside the codec table (Python `ValueError`).
    pub fn handle(
        &mut self,
        event: CodecEvent,
        now: f64,
    ) -> Result<Vec<CodecAction>, UnknownCodec> {
        match event {
            CodecEvent::SourceCodec { codec, eligible } => {
                self.source = Some(codec);
                self.eligible = eligible;
            }
            CodecEvent::OutputAdded {
                output_id,
                codec,
                offer_top,
                bound,
            } => {
                let requested = down_from(&offer_top)?;
                self.outs.insert(
                    output_id,
                    Out {
                        reported: codec,
                        offer_top,
                        requested: Some(requested),
                        bound,
                        awaiting_until: NEVER,
                        swap_retry_at: 0.0,
                    },
                );
            }
            CodecEvent::OutputCodec { output_id, codec } => {
                if let Some(o) = self.outs.get_mut(&output_id) {
                    o.reported = codec;
                    o.awaiting_until = NEVER;
                }
            }
            CodecEvent::OutputBound { output_id, codec } => {
                if let Some(o) = self.outs.get_mut(&output_id) {
                    if codec.is_none() {
                        o.swap_retry_at = now + self.swap_retry_s;
                    }
                    o.bound = codec;
                }
                // no reconcile: a failing swap would otherwise re-emit itself in a tight loop
                return Ok(Vec::new());
            }
            CodecEvent::ActionFailed(action) => {
                match action {
                    CodecAction::UpdateWatch { .. } => self.last_watch = None,
                    CodecAction::UpdateOutput { output_id, .. } => {
                        if let Some(o) = self.outs.get_mut(&output_id) {
                            o.requested = None;
                        }
                    }
                    CodecAction::SwapOutput { output_id, .. } => {
                        if let Some(o) = self.outs.get_mut(&output_id) {
                            o.bound = None;
                            o.swap_retry_at = now + self.swap_retry_s;
                        }
                    }
                }
                // emitted again by the next event or Tick, not immediately
                return Ok(Vec::new());
            }
            CodecEvent::OutputGone { output_id } => {
                self.outs.shift_remove(&output_id);
            }
            CodecEvent::Hold => {
                if self.held_since.is_none() {
                    self.held_since = Some(now);
                }
            }
            CodecEvent::Live => self.held_since = None,
            CodecEvent::Tick => {}
        }
        self.reconcile(now)
    }

    fn reconcile(&mut self, now: f64) -> Result<Vec<CodecAction>, UnknownCodec> {
        let mut actions = Vec::new();
        let held = self
            .held_since
            .is_some_and(|since| now - since >= self.hold_force_s);
        let e: Option<String> = match &self.source {
            None => None,
            Some(_) if !self.eligible || held => Some("H264".to_string()),
            Some(source) => Some(source.clone()),
        };
        self.e.clone_from(&e);

        let mut natural: Vec<&str> = Vec::new();
        for o in self.outs.values() {
            if rank(&o.reported)? > rank(&o.offer_top)? && now >= o.awaiting_until {
                natural.push(&o.reported);
            }
        }
        let any_natural = !natural.is_empty();
        let w: String = lowest(&natural)?.unwrap_or("AV1").to_string();
        let watch = down_from(&w)?;
        if Some(watch) != self.last_watch {
            if rank(&w)? < rank(&self.w)? {
                self.watch_changed_at = now;
            }
            self.last_watch = Some(watch);
            actions.push(CodecAction::UpdateWatch { codecs: watch });
        }
        self.w.clone_from(&w);

        let pending = match &e {
            Some(e) => rank(&w)? < rank(e)? && now - self.watch_changed_at < self.upgrade_wait_s,
            None => false,
        };

        self.switching = None;
        if let Some(e) = &e
            && (pending || (any_natural && rank(&w)? > rank(e)?))
        {
            self.switching = Some((e.clone(), w.clone()));
        }

        let Some(e) = e else {
            return Ok(actions);
        };
        let wanted = down_from(&e)?;
        for (oid, o) in &mut self.outs {
            if rank(&o.reported)? < rank(&e)? && !pending {
                if o.requested != Some(wanted) {
                    actions.push(CodecAction::UpdateOutput {
                        output_id: *oid,
                        codecs: wanted,
                    });
                    o.requested = Some(wanted);
                }
                o.offer_top.clone_from(&e);
            } else if rank(&o.offer_top)? > rank(&e)? {
                if o.requested != Some(wanted) {
                    actions.push(CodecAction::UpdateOutput {
                        output_id: *oid,
                        codecs: wanted,
                    });
                    o.requested = Some(wanted);
                }
                o.offer_top.clone_from(&e);
                o.awaiting_until = now + self.upgrade_wait_s;
            }
            if o.reported == e && o.bound.as_ref() != Some(&e) && now >= o.swap_retry_at {
                actions.push(CodecAction::SwapOutput {
                    output_id: *oid,
                    codec: e.clone(),
                });
                o.bound = Some(e.clone());
            }
        }
        Ok(actions)
    }
}
