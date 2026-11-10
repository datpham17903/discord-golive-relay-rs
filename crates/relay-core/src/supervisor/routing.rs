//! Runtime routing: which user is relayed, the main voice channel, and the mirror table. Port of
//! `supervisor/routing.py`.

use indexmap::IndexMap;

use crate::control::state::PersistedRouting;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routing {
    pub target_user_id: u64,
    pub channel_id: u64,
    /// channel_id -> output account user_id
    pub mirrors: IndexMap<u64, u64>,
}

impl Routing {
    /// `default` (from config) when nothing is persisted.
    ///
    /// `StateStore::load` only yields mirror keys that are canonical decimal channel ids, so parsing
    /// them cannot fail for a loaded state.
    pub fn from_persisted(d: Option<&PersistedRouting>, default: &Routing) -> Routing {
        let Some(d) = d else {
            return default.clone();
        };
        Routing {
            target_user_id: d.target_user_id,
            channel_id: d.channel_id,
            mirrors: d
                .mirrors
                .iter()
                .map(|(ch, acct)| {
                    let ch = ch
                        .parse::<u64>()
                        .expect("persisted routing mirror key is a canonical channel id");
                    (ch, *acct)
                })
                .collect(),
        }
    }

    pub fn to_persisted(&self) -> PersistedRouting {
        PersistedRouting {
            target_user_id: self.target_user_id,
            channel_id: self.channel_id,
            mirrors: self
                .mirrors
                .iter()
                .map(|(ch, acct)| (ch.to_string(), *acct))
                .collect(),
        }
    }
}
