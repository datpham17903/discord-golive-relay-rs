//! Port of `tests/adapter/test_discord_adapter.py`, `test_adapter_fence.py` and the adapter half of
//! `test_adaptive_codec.py`: event translation, chat intake, cache lookups, the receive path and the
//! join/leave bookkeeping, all without a network (the cache is fed by hand, the voice clients talk
//! to a fake gateway link).

use std::sync::Mutex as StdMutex;
use std::time::Instant;

use relay_core::control::commands::ChatCommand;
use relay_core::worker::messages::Ingress;
use relay_voice::link::{
    GatewayLink, LinkError, LinkFuture, StreamCreateRequest, VoiceStateRequest,
};
use relay_voice::receive::{PacketCodec, RtpPacketInfo};
use relay_voice::voice_ws::SessionInfo;
use tokio::sync::Notify;

use super::*;

const GUILD: u64 = 1;
const CHAN: u64 = 2;
const TARGET: u64 = 3;
const RELAY: u64 = 4;
const OUT: u64 = 5;
const OPERATOR: u64 = 50;
const CMD_CHAN: u64 = 60;
const ROLE: u64 = 700;

fn real_key() -> String {
    format!("guild:{GUILD}:{CHAN}:{TARGET}")
}

fn options(control: ControlConfig) -> AdapterOptions {
    AdapterOptions {
        guild_id: GUILD,
        target_id: TARGET,
        channel_id: CHAN,
        relay_id: RELAY,
        output_account_ids: vec![OUT],
        control,
        paused_verified: false,
        adaptive: false,
        parent_codecs: vec![VoiceCodec::H264],
        watch_codecs: vec![VoiceCodec::H264],
        network_stats: false,
        self_deaf: true,
        urls: Urls::default(),
        connect_timeout: Duration::from_millis(300),
    }
}

fn control(chat: bool, command_channel: Option<u64>, roles: &[u64]) -> ControlConfig {
    ControlConfig {
        chat_commands: chat,
        operator_user_ids: [OPERATOR, RELAY, OUT].into(),
        operator_role_ids: roles.iter().copied().collect(),
        command_channel_id: command_channel,
        ..ControlConfig::default()
    }
}

struct Rig {
    adapter: DiscordAdapter,
    cache: SharedCache,
    events: Arc<StdMutex<Vec<AdapterEvent>>>,
    ingress: Arc<StdMutex<Vec<Ingress>>>,
    logs: Arc<StdMutex<Vec<(String, Value)>>>,
}

impl Rig {
    fn new() -> Self {
        Self::with(options(control(true, Some(CMD_CHAN), &[])))
    }

    fn with(opts: AdapterOptions) -> Self {
        let cache = Cache::shared();
        {
            let mut c = cache.write();
            c.apply(
                "READY",
                &json!({"user": {"id": RELAY.to_string()}, "guilds": []}),
            );
            c.apply(
                "GUILD_CREATE",
                &json!({"id": GUILD.to_string(), "channels": [{"id": CHAN.to_string(), "type": 2}]}),
            );
        }
        let events = Arc::new(StdMutex::new(Vec::new()));
        let ingress = Arc::new(StdMutex::new(Vec::new()));
        let logs = Arc::new(StdMutex::new(Vec::new()));
        let (e, i, l) = (Arc::clone(&events), Arc::clone(&ingress), Arc::clone(&logs));
        let adapter = DiscordAdapter::with_cache(
            opts,
            Arc::clone(&cache),
            Arc::new(move |ev| e.lock().unwrap().push(ev)),
            Arc::new(move |m| i.lock().unwrap().push(m)),
            Arc::new(move |name, fields| l.lock().unwrap().push((name.to_owned(), fields))),
        );
        Self {
            adapter,
            cache,
            events,
            ingress,
            logs,
        }
    }

    /// The cache applies the dispatch first, then the adapter sees it with the change, as the
    /// gateway task does.
    fn dispatch(&self, name: &str, data: Value) {
        let change = self.cache.write().apply(name, &data);
        self.adapter.handle_dispatch(&Dispatch {
            name: name.to_owned(),
            seq: None,
            data,
            change,
        });
    }

    /// Changes the cache without telling the adapter.
    fn seed(&self, name: &str, data: Value) {
        self.cache.write().apply(name, &data);
    }

    fn events(&self) -> Vec<AdapterEvent> {
        self.events.lock().unwrap().clone()
    }

    fn model(&self) -> Vec<Event> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                AdapterEvent::Model(e) => Some(e),
                _ => None,
            })
            .collect()
    }

    fn chats(&self) -> Vec<ChatCommand> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                AdapterEvent::Chat(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn logs(&self) -> Vec<(String, Value)> {
        self.logs.lock().unwrap().clone()
    }

    fn vsu(&self, user: u64, channel: Option<u64>, self_stream: bool) {
        self.dispatch(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": GUILD.to_string(), "user_id": user.to_string(),
                   "channel_id": channel.map(|c| c.to_string()), "self_stream": self_stream}),
        );
    }

    fn stream_data(owner: u64, channel: u64) -> Value {
        json!({"stream_key": format!("guild:{GUILD}:{channel}:{owner}"),
               "rtc_server_id": "900", "rtc_channel_id": "901", "paused": false, "viewer_ids": []})
    }
}

fn message(author: u64, guild: Option<u64>, channel: u64, text: &str) -> Value {
    let mut m = json!({"id": "900", "channel_id": channel.to_string(),
                       "author": {"id": author.to_string()}, "content": text});
    if let Some(guild) = guild {
        m["guild_id"] = json!(guild.to_string());
    }
    m
}

fn chat(author: u64, text: &str) -> ChatCommand {
    ChatCommand {
        author_id: author,
        channel_id: CMD_CHAN,
        message_id: 900,
        text: text.to_owned(),
        author_role_ids: Default::default(),
        author_voice_channel_id: None,
    }
}

// ------------------------------------------------------------------ chat intake

#[test]
fn operator_message_becomes_chat_command() {
    let rig = Rig::new();
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    assert!(rig.chats() == vec![chat(OPERATOR, "!relay status")]);
    // owners too: only prefixed messages reach the service
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), CMD_CHAN, "hello"),
    );
    assert_eq!(rig.chats().len(), 1);
}

#[test]
fn non_operator_command_is_passed_on() {
    let rig = Rig::new();
    rig.dispatch(
        "MESSAGE_CREATE",
        message(51, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    assert!(rig.chats() == vec![chat(51, "!relay status")]);
    // not a command: never reaches the service
    for text in ["hello", "", "!relayx status"] {
        rig.dispatch("MESSAGE_CREATE", message(51, Some(GUILD), CMD_CHAN, text));
    }
    assert_eq!(rig.chats().len(), 1);
    // the service decides
    rig.dispatch(
        "MESSAGE_CREATE",
        message(51, Some(GUILD), CMD_CHAN, "!RELAY unmirror all"),
    );
    assert_eq!(rig.chats().last().unwrap().text, "!RELAY unmirror all");
}

#[test]
fn chat_from_elsewhere_ignored_no_reply() {
    let rig = Rig::new();
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(99), CMD_CHAN, "!relay status"),
    ); // other guild
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, None, CMD_CHAN, "!relay status"),
    ); // DM
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), 61, "!relay status"),
    ); // not the command channel
    // the relay accounts themselves, even if listed as operators
    rig.dispatch(
        "MESSAGE_CREATE",
        message(RELAY, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OUT, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    assert!(rig.chats().is_empty());

    let off = Rig::with(options(control(false, Some(CMD_CHAN), &[])));
    off.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    assert!(off.chats().is_empty());

    let any_channel = Rig::with(options(control(true, None, &[])));
    any_channel.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), 61, "!relay status"),
    );
    assert_eq!(any_channel.chats().len(), 1);
}

#[test]
fn role_ids_are_passed() {
    let rig = Rig::with(options(control(true, Some(CMD_CHAN), &[ROLE])));
    let mut m = message(51, Some(GUILD), CMD_CHAN, "!relay status");
    m["member"] = json!({"roles": [ROLE.to_string(), "701"]});
    rig.dispatch("MESSAGE_CREATE", m);
    let chats = rig.chats();
    assert_eq!(chats.len(), 1);
    assert_eq!(chats[0].author_role_ids, [ROLE, 701].into());
}

#[test]
fn role_holder_elsewhere_is_ignored() {
    let rig = Rig::with(options(control(true, Some(CMD_CHAN), &[ROLE])));
    let mut wrong_guild = message(51, Some(99), CMD_CHAN, "!relay status");
    wrong_guild["member"] = json!({"roles": [ROLE.to_string()]});
    rig.dispatch("MESSAGE_CREATE", wrong_guild);
    let mut relay = message(RELAY, Some(GUILD), CMD_CHAN, "!relay status");
    relay["member"] = json!({"roles": [ROLE.to_string()]});
    rig.dispatch("MESSAGE_CREATE", relay);
    assert!(rig.chats().is_empty());
}

#[test]
fn author_voice_channel_filled_from_the_cached_voice_state() {
    let rig = Rig::new();
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "50", "channel_id": "77"}),
    );
    rig.dispatch(
        "MESSAGE_CREATE",
        message(OPERATOR, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    // in voice of another guild: not looked up; no voice state at all
    rig.seed("GUILD_CREATE", json!({"id": "99"}));
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "99", "user_id": "51", "channel_id": "78"}),
    );
    rig.dispatch(
        "MESSAGE_CREATE",
        message(51, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    rig.dispatch(
        "MESSAGE_CREATE",
        message(52, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    // left voice
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "53", "channel_id": null}),
    );
    rig.dispatch(
        "MESSAGE_CREATE",
        message(53, Some(GUILD), CMD_CHAN, "!relay status"),
    );
    let voice: Vec<_> = rig
        .chats()
        .iter()
        .map(|c| c.author_voice_channel_id)
        .collect();
    assert_eq!(voice, [Some(77), None, None, None]);
}

#[test]
fn cache_lookups_through_the_adapter_api() {
    let rig = Rig::new();
    rig.seed(
        "GUILD_CREATE",
        json!({"id": "1",
               "channels": [{"id": "70", "type": 2}, {"id": "71", "type": 0}],
               "roles": [{"id": "500", "name": "Mods"}],
               "members": [{"user": {"id": "8", "username": "eight"}, "nick": "Nick"},
                           {"user": {"id": "9", "username": "robot", "bot": true}}]}),
    );
    let adapter: &dyn Adapter = &rig.adapter;
    assert_eq!(adapter.voice_channel_ids(), [70].into());
    assert_eq!(adapter.display_name(8).as_deref(), Some("Nick"));
    assert_eq!(adapter.display_name(10), None);
    assert!(adapter.is_bot(9) && !adapter.is_bot(8) && !adapter.is_bot(10));
    assert_eq!(adapter.role_name(500).as_deref(), Some("Mods"));
    assert_eq!(adapter.role_name(501), None);
}

// ------------------------------------------------------------ voice state, streams

fn started(events: &[Event]) -> Vec<&SourceStream> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::StreamStarted { stream } => Some(stream),
            _ => None,
        })
        .collect()
}

#[test]
fn voice_updates_then_resumed_orders_fresh_last() {
    let rig = Rig::new();
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "9", "channel_id": "2"}),
    );
    rig.adapter
        .inner
        .on_gateway_event(GatewayEvent::Disconnected {
            code: Some(1006),
            reason: "x".into(),
            resume: true,
        });
    rig.vsu(TARGET, Some(CHAN), true);
    // another channel, so a different key than the one synthesized from the voice state
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, 9));
    rig.dispatch("RESUMED", json!({}));
    let events = rig.model();
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| match e {
            Event::GatewayChanged { .. } => "gateway",
            Event::TargetVoiceChanged { .. } => "target",
            Event::StreamStarted { .. } => "started",
            Event::ParticipantsChanged { .. } => "participants",
            Event::StreamEnded { .. } => "ended",
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "gateway",
            "target",
            "started",
            "participants",
            "ended",
            "started",
            "gateway"
        ]
    );
    assert!(matches!(
        events[0],
        Event::GatewayChanged {
            state: GatewayState::Resuming
        }
    ));
    assert!(matches!(
        events[6],
        Event::GatewayChanged {
            state: GatewayState::Fresh
        }
    ));
    assert!(matches!(
        events[1],
        Event::TargetVoiceChanged {
            target: Some(TargetVoice {
                self_stream: true,
                ..
            })
        }
    ));
    let Event::ParticipantsChanged { participants } = &events[3] else {
        panic!()
    };
    assert_eq!(participants.as_ref().unwrap(), &HashSet::from([TARGET, 9]));
}

#[test]
fn own_and_foreign_streams_ignored() {
    let rig = Rig::new();
    rig.dispatch("STREAM_CREATE", Rig::stream_data(RELAY, CHAN));
    rig.dispatch("STREAM_CREATE", Rig::stream_data(77, CHAN));
    rig.dispatch(
        "STREAM_DELETE",
        json!({"stream_key": format!("guild:{GUILD}:{CHAN}:{RELAY}"), "reason": "x"}),
    );
    assert!(rig.events().is_empty());
}

#[test]
fn incarnation_increments_on_key_reuse() {
    let rig = Rig::new();
    let key = json!({"stream_key": real_key(), "reason": "ended"});
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    rig.dispatch("STREAM_DELETE", key);
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    let events = rig.model();
    let started = started(&events);
    assert_eq!(
        started.iter().map(|s| s.incarnation).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(started[0].key, started[1].key);
    let ended: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Event::StreamEnded { incarnation, .. } => Some(*incarnation),
            _ => None,
        })
        .collect();
    assert_eq!(ended, [1]);
}

#[test]
fn paused_hidden_until_capability_verified() {
    let rig = Rig::new();
    let mut data = Rig::stream_data(TARGET, CHAN);
    data["paused"] = json!(true);
    rig.dispatch("STREAM_CREATE", data.clone());
    assert_eq!(started(&rig.model())[0].paused, None);

    let verified = Rig::with(AdapterOptions {
        paused_verified: true,
        ..options(control(true, None, &[]))
    });
    verified.dispatch("STREAM_CREATE", data);
    assert_eq!(started(&verified.model())[0].paused, Some(true));
}

#[test]
fn participants_exclude_relay_and_leaver() {
    let rig = Rig::new();
    for user in [TARGET, RELAY, 9] {
        rig.seed(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": "1", "user_id": user.to_string(), "channel_id": "2"}),
        );
    }
    rig.vsu(9, None, false);
    let Some(Event::ParticipantsChanged { participants }) = rig.model().pop() else {
        panic!("no participants event")
    };
    assert_eq!(participants.unwrap(), HashSet::from([TARGET]));
}

#[test]
fn self_stream_flag_drives_incarnations_and_ignores_stale_flag() {
    let rig = Rig::new();
    rig.vsu(TARGET, Some(CHAN), false); // joins, not streaming
    rig.vsu(TARGET, Some(CHAN), true); // starts
    rig.vsu(TARGET, Some(CHAN), false); // stops
    rig.vsu(TARGET, Some(CHAN), true); // starts again
    rig.vsu(TARGET, None, true); // leaves with a stale self_stream=true
    let s: Vec<Event> = rig
        .model()
        .into_iter()
        .filter(|e| matches!(e, Event::StreamStarted { .. } | Event::StreamEnded { .. }))
        .collect();
    assert!(matches!(
        s.as_slice(),
        [
            Event::StreamStarted { .. },
            Event::StreamEnded { .. },
            Event::StreamStarted { .. },
            Event::StreamEnded { .. }
        ]
    ));
    let events = rig.model();
    let starts = started(&events);
    assert_eq!(
        starts.iter().map(|s| s.incarnation).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(starts[0].key, real_key());
    assert_eq!(starts[0].paused, None);
}

#[test]
fn real_stream_create_after_synth_is_update_and_no_double_bump() {
    let rig = Rig::new();
    rig.vsu(TARGET, Some(CHAN), true);
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    let rest: Vec<Event> = rig
        .model()
        .into_iter()
        .filter(|e| {
            !matches!(
                e,
                Event::TargetVoiceChanged { .. } | Event::ParticipantsChanged { .. }
            )
        })
        .collect();
    assert!(matches!(
        rest.as_slice(),
        [Event::StreamStarted { .. }, Event::StreamUpdated { stream }] if stream.incarnation == 1
    ));
}

#[test]
fn stream_delete_then_self_stream_false_emits_one_end() {
    let rig = Rig::new();
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    rig.vsu(TARGET, Some(CHAN), true); // unrelated update (e.g. mute) must not restart
    rig.dispatch(
        "STREAM_DELETE",
        json!({"stream_key": real_key(), "reason": "x"}),
    );
    rig.vsu(TARGET, Some(CHAN), false);
    let events = rig.model();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::StreamEnded { .. }))
            .count(),
        1
    );
    assert_eq!(started(&events).len(), 1);
}

/// The tracked stream is the one synthesized from voice state; the target streams or not.
fn stream_rig(target_streaming: bool) -> Rig {
    let rig = Rig::new();
    rig.seed("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "3", "channel_id": "2", "self_stream": target_streaming}),
    );
    let inner = &rig.adapter.inner;
    let source = inner.voice_source(CHAN, 1);
    {
        let mut st = inner.st.lock();
        st.incarnation = 1;
        st.stream = Some(source);
    }
    rig
}

#[test]
fn stream_delete_ignored_while_target_self_stream_true() {
    let rig = stream_rig(true);
    rig.dispatch(
        "STREAM_DELETE",
        json!({"stream_key": real_key(), "reason": "user_requested"}),
    );
    assert!(rig.events().is_empty());
    assert!(rig.adapter.inner.st.lock().stream.is_some());
    assert_eq!(
        rig.logs(),
        vec![(
            "stream_delete_ignored".to_owned(),
            json!({"reason": "user_requested"})
        )]
    );
}

#[test]
fn stream_delete_honored_when_target_not_streaming() {
    let rig = stream_rig(false);
    rig.dispatch(
        "STREAM_DELETE",
        json!({"stream_key": real_key(), "reason": "x"}),
    );
    assert_eq!(rig.model().len(), 1);
    assert!(rig.adapter.inner.st.lock().stream.is_none());
}

#[test]
fn voice_update_self_stream_false_still_ends_stream() {
    let rig = stream_rig(true);
    rig.vsu(TARGET, Some(CHAN), false);
    assert!(rig.adapter.inner.st.lock().stream.is_none());
}

#[test]
fn an_unavailable_stream_is_an_update_not_an_end() {
    let rig = Rig::new();
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    rig.dispatch(
        "STREAM_DELETE",
        json!({"stream_key": real_key(), "unavailable": true}),
    );
    let events = rig.model();
    let Some(Event::StreamUpdated { stream }) = events.last() else {
        panic!("{events:?}")
    };
    assert!(stream.unavailable);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::StreamEnded { .. }))
    );
}

#[test]
fn snapshot_synthesizes_stream_from_voice_state() {
    let rig = Rig::new();
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "3", "channel_id": "2", "self_stream": true}),
    );
    let Event::GatewaySnapshot { stream, target, .. } = rig.adapter.snapshot(false) else {
        panic!()
    };
    let stream = stream.unwrap();
    assert_eq!(
        (stream.key.as_str(), stream.incarnation),
        (real_key().as_str(), 1)
    );
    assert_eq!(target.unwrap().channel_id, Some(CHAN));
    let Event::GatewaySnapshot { stream, .. } = rig.adapter.snapshot(false) else {
        panic!()
    };
    assert_eq!(stream.unwrap().incarnation, 1);
}

#[test]
fn snapshot_of_a_new_session_lists_participants_without_the_relay() {
    let rig = Rig::new();
    for user in [TARGET, RELAY, 9] {
        rig.seed(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": "1", "user_id": user.to_string(), "channel_id": "2"}),
        );
    }
    let Event::GatewaySnapshot {
        participants,
        new_session,
        stream,
        target,
    } = rig.adapter.snapshot(true)
    else {
        panic!()
    };
    assert!(new_session && stream.is_none());
    assert_eq!(participants.unwrap(), HashSet::from([TARGET, 9]));
    assert!(!target.unwrap().self_stream);
}

#[test]
fn a_live_cached_stream_is_picked_up_by_the_snapshot() {
    let rig = Rig::new();
    rig.seed("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    let Event::GatewaySnapshot { stream, .. } = rig.adapter.snapshot(true) else {
        panic!()
    };
    let stream = stream.unwrap();
    assert_eq!(
        (stream.key.as_str(), stream.incarnation),
        (real_key().as_str(), 1)
    );
}

// ----------------------------------------------------------------- gateway state

#[test]
fn ready_of_another_account_is_config_invalid() {
    let rig = Rig::new();
    rig.seed("READY", json!({"user": {"id": "99"}, "guilds": []}));
    rig.dispatch("READY_SUPPLEMENTAL", json!({"guilds": []}));
    assert!(matches!(
        rig.model().as_slice(),
        [Event::ConfigInvalid { .. }]
    ));
}

#[test]
fn ready_with_a_channel_that_is_not_a_voice_channel_is_config_invalid() {
    let rig = Rig::new();
    rig.seed(
        "CHANNEL_UPDATE",
        json!({"guild_id": "1", "id": "2", "type": 0}),
    );
    rig.dispatch("READY_SUPPLEMENTAL", json!({"guilds": []}));
    assert!(matches!(
        rig.model().as_slice(),
        [Event::ConfigInvalid { detail }] if detail.contains("allowed_voice_channel_id")
    ));
}

#[test]
fn ready_with_the_guild_unavailable_waits_for_guild_create() {
    let rig = Rig::new();
    rig.seed("GUILD_DELETE", json!({"id": "1", "unavailable": true}));
    rig.dispatch("READY_SUPPLEMENTAL", json!({"guilds": []}));
    assert!(rig.model().is_empty());
    rig.dispatch(
        "GUILD_CREATE",
        json!({"id": "1", "channels": [{"id": "2", "type": 2}]}),
    );
    assert!(matches!(
        rig.model().as_slice(),
        [Event::GatewaySnapshot {
            new_session: true,
            ..
        }]
    ));
}

#[test]
fn voice_state_of_the_target_in_another_guild_is_ignored() {
    let rig = Rig::new();
    rig.seed("GUILD_CREATE", json!({"id": "9", "channels": []}));
    rig.dispatch(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "9", "user_id": TARGET.to_string(),
               "channel_id": "99", "self_stream": true}),
    );
    assert!(rig.model().is_empty());
}

#[test]
fn passive_update_v2_voice_states_reach_the_target_state() {
    let rig = Rig::new();
    rig.dispatch(
        "PASSIVE_UPDATE_V2",
        json!({"guild_id": "1", "updated_voice_states": [
            {"user_id": TARGET.to_string(), "channel_id": CHAN.to_string(), "self_stream": true}]}),
    );
    assert!(
        rig.model()
            .iter()
            .any(|e| matches!(e, Event::TargetVoiceChanged { target: Some(_) }))
    );
}

#[test]
fn ready_emits_a_new_session_snapshot() {
    let rig = Rig::new();
    rig.dispatch("READY_SUPPLEMENTAL", json!({"guilds": []}));
    assert!(matches!(
        rig.model().as_slice(),
        [Event::GatewaySnapshot {
            new_session: true,
            ..
        }]
    ));
}

#[test]
fn gateway_exit_emits_auth_rejected_or_invalid() {
    let rig = Rig::new();
    rig.adapter
        .handle_gateway_closed(&crate::Error::LoginFailed);
    assert!(matches!(rig.model().last(), Some(Event::AuthRejected)));
    let rig = Rig::new();
    rig.adapter
        .handle_gateway_closed(&crate::Error::GatewayClosed {
            code: 4011,
            reason: "x".into(),
        });
    assert!(matches!(
        rig.model().last(),
        Some(Event::GatewayChanged {
            state: GatewayState::Invalid
        })
    ));
    assert!(rig.logs().iter().any(|(n, _)| n == "gateway_ended"));
}

// ------------------------------------------------------------------- join / voice

#[derive(Default)]
struct FakeLink {
    states: StdMutex<Vec<VoiceStateRequest>>,
    asked: Notify,
}

impl GatewayLink for FakeLink {
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_> {
        self.states.lock().unwrap().push(request);
        self.asked.notify_one();
        Box::pin(async { Ok(()) })
    }

    fn stream_create(&self, _: StreamCreateRequest) -> LinkFuture<'_> {
        Box::pin(async { Err(LinkError("unused".into())) })
    }

    fn stream_watch(&self, _: &str) -> LinkFuture<'_> {
        Box::pin(async { Err(LinkError("unused".into())) })
    }

    fn stream_delete(&self, _: &str) -> LinkFuture<'_> {
        Box::pin(async { Err(LinkError("unused".into())) })
    }

    fn stream_set_paused(&self, _: &str, _: bool) -> LinkFuture<'_> {
        Box::pin(async { Err(LinkError("unused".into())) })
    }
}

fn own_voice_state(session: &str) -> Value {
    json!({"guild_id": GUILD.to_string(), "user_id": RELAY.to_string(),
           "channel_id": CHAN.to_string(), "session_id": session})
}

#[tokio::test]
async fn join_reports_voice_state_echo_and_a_failed_join_clears_the_voice_client() {
    for self_deaf in [true, false] {
        let rig = Rig::with(AdapterOptions {
            self_deaf,
            ..options(control(true, None, &[]))
        });
        let link = Arc::new(FakeLink::default());
        rig.adapter
            .attach_detached(Net::detached(link.clone(), Some("gw-1")));
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let marker = Arc::clone(&seen);
        let adapter: &dyn Adapter = &rig.adapter;
        let join = adapter.join(Some(Box::new(move || {
            marker.lock().unwrap().push("callback")
        })));
        let echo = async {
            link.asked.notified().await;
            // the gateway echoes our voice state; the media connection never comes (no
            // VOICE_SERVER_UPDATE), so the join times out
            rig.dispatch("VOICE_STATE_UPDATE", own_voice_state("sess-1"));
        };
        let (result, ()) = tokio::join!(join, echo);

        let err = result.expect_err("the join must fail without a voice server");
        assert!(matches!(err, AdapterError::Voice(_)), "{err:?}");
        assert_eq!(
            *seen.lock().unwrap(),
            ["callback"],
            "echo reported before the connect ended"
        );
        assert!(
            rig.adapter.inner.st.lock().vc.is_none(),
            "failed join clears the voice client"
        );
        assert!(!adapter.voice_alive());
        assert_eq!(adapter.last_vsu_session().as_deref(), Some("sess-1"));
        let first = link.states.lock().unwrap()[0];
        assert_eq!(first.channel_id, Some(CHAN));
        assert_eq!(first.guild_id, Some(GUILD));
        assert!(first.self_mute, "join is always muted");
        assert_eq!(first.self_deaf, self_deaf, "runtime.self_deaf_ingest");
    }
}

#[tokio::test]
async fn join_needs_the_allowed_voice_channel_in_the_cache() {
    let rig = Rig::new();
    rig.adapter
        .attach_detached(Net::detached(Arc::new(FakeLink::default()), None));
    rig.adapter.inner.channel.store(555, Ordering::Relaxed);
    let adapter: &dyn Adapter = &rig.adapter;
    let err = adapter.join(None).await.unwrap_err();
    assert!(
        err.to_string().contains("voice channel not available"),
        "{err}"
    );
}

fn register_voice_client(rig: &Rig, link: Arc<FakeLink>) -> VoiceClient {
    let (vc, _events) = VoiceClient::new(VoiceClientConfig {
        user_id: RELAY,
        guild_id: Some(GUILD),
        channel_id: CHAN,
        gateway: link,
        http: wreq::Client::builder().build().unwrap(),
        rtc: RtcSettings::new(
            decode_codecs(&[VoiceCodec::H264]),
            vec![StreamSpec::video(100)],
            true,
        ),
    });
    rig.adapter.inner.st.lock().vc = Some(vc.clone());
    vc
}

#[test]
fn parent_voice_state_logs_vsu_and_gateway_session() {
    let rig = Rig::new();
    let link = Arc::new(FakeLink::default());
    rig.adapter
        .attach_detached(Net::detached(link.clone(), Some("gw-1")));
    register_voice_client(&rig, link);
    // no guild in the payload: only the observation, no participants event
    rig.dispatch(
        "VOICE_STATE_UPDATE",
        json!({"user_id": RELAY.to_string(), "channel_id": CHAN.to_string(), "session_id": "vs-1"}),
    );
    assert_eq!(
        rig.logs(),
        vec![(
            "relay_voice_state".to_owned(),
            json!({"vsu_session": "vs-1", "gw_session": "gw-1", "channel_id": CHAN, "connected": false})
        )]
    );
    let adapter: &dyn Adapter = &rig.adapter;
    assert_eq!(adapter.last_vsu_session().as_deref(), Some("vs-1"));
    assert!(adapter.foreign_voice_session());
    assert!(matches!(
        rig.model().as_slice(),
        [Event::RelayVoiceObserved(RelayVoiceObserved { session_id: Some(s), channel_id: Some(CHAN) })] if s == "vs-1"
    ));
    // the same session as the gateway's is not foreign
    rig.dispatch(
        "VOICE_STATE_UPDATE",
        json!({"user_id": RELAY.to_string(), "channel_id": CHAN.to_string(), "session_id": "gw-1"}),
    );
    assert!(!adapter.foreign_voice_session());
}

#[test]
fn the_relays_voice_state_is_not_observed_without_a_voice_client() {
    let rig = Rig::new();
    rig.dispatch(
        "VOICE_STATE_UPDATE",
        json!({"user_id": RELAY.to_string(), "channel_id": CHAN.to_string(), "session_id": "vs-1"}),
    );
    assert!(rig.model().is_empty() && rig.logs().is_empty());
    assert_eq!(Adapter::last_vsu_session(&rig.adapter), None);
}

#[test]
fn voice_alive_is_false_without_a_connected_voice_client() {
    let rig = Rig::new();
    assert!(!Adapter::voice_alive(&rig.adapter));
    register_voice_client(&rig, Arc::new(FakeLink::default()));
    assert!(
        !Adapter::voice_alive(&rig.adapter),
        "registered but not connected"
    );
}

#[tokio::test]
async fn leave_without_a_voice_client_is_a_noop() {
    let rig = Rig::new();
    Adapter::leave(&rig.adapter).await.unwrap();
}

// ------------------------------------------------------------------ receive path

fn packet(
    user: u64,
    media: MediaType,
    codec: PacketCodec,
    ssrc: u32,
    seq: u16,
    payload: &[u8],
) -> Packet {
    Packet {
        user_id: Some(user),
        media_type: media,
        codec,
        ssrc,
        timestamp: 0,
        sequence: seq,
        payload: payload.to_vec(),
        payload_type: 105,
        marker: false,
        rtp_extensions: Vec::new(),
        rtp_packets: vec![rtp_info(seq, ssrc)],
        received_at: Instant::now(),
        rtcp_time: None,
        speaking_flags: None,
        audio_level: None,
        audio_voice_activity: None,
    }
}

fn rtp_info(sequence: u16, ssrc: u32) -> RtpPacketInfo {
    RtpPacketInfo {
        sequence,
        timestamp: 0,
        ssrc,
        payload_type: 105,
        marker: false,
        rtx: false,
        payload_len: 1,
    }
}

fn audio(user: u64, seq: u16, payload: &[u8]) -> Packet {
    packet(user, MediaType::Audio, PacketCodec::Opus, 8, seq, payload)
}

fn video(seq: u16, ssrc: u32, codec: VoiceCodec, count: u16) -> Packet {
    let mut p = packet(
        TARGET,
        MediaType::Video,
        PacketCodec::Video(codec),
        ssrc,
        seq,
        b"x",
    );
    p.rtp_packets = (0..count)
        .map(|i| rtp_info(seq.wrapping_sub(count - 1).wrapping_add(i), ssrc))
        .collect();
    p
}

#[test]
fn progress_rate_limit_is_per_media_type() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    inner.on_packet(&audio(TARGET, 1, &[0xFC]));
    inner.on_packet(&packet(
        TARGET,
        MediaType::Video,
        PacketCodec::Video(VoiceCodec::H264),
        7,
        1,
        b"x",
    ));
    // within 250 ms: another fresh packet of either type is not reported again
    inner.on_packet(&audio(TARGET, 2, &[0xFC]));
    let progress: Vec<bool> = rig
        .model()
        .iter()
        .filter_map(|e| match e {
            Event::SourceProgress { video, .. } => Some(*video),
            _ => None,
        })
        .collect();
    assert_eq!(progress, [false, true]);
}

#[test]
fn packets_of_other_users_are_ignored_and_ingress_carries_generation_and_epoch() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    {
        let mut hot = inner.hot.lock();
        hot.generation = 3;
        hot.epoch = 4;
    }
    inner.on_packet(&audio(99, 1, &[0xFC]));
    let mut anonymous = audio(TARGET, 1, &[0xFC]);
    anonymous.user_id = None;
    inner.on_packet(&anonymous);
    assert!(rig.ingress.lock().unwrap().is_empty() && rig.model().is_empty());

    inner.on_packet(&audio(TARGET, 5, &[0xFC]));
    let Ingress::Audio(a) = rig.ingress.lock().unwrap()[0].clone() else {
        panic!()
    };
    assert_eq!((a.generation, a.epoch, a.sequence), (3, 4, Some(5)));
    Adapter::set_epoch(&rig.adapter, 9);
    inner.on_packet(&video(1, 7, VoiceCodec::H264, 1));
    let Ingress::Video(v) = rig.ingress.lock().unwrap()[1].clone() else {
        panic!()
    };
    assert_eq!((v.generation, v.epoch, v.codec.as_str()), (3, 9, "H264"));
}

#[test]
fn ingress_audio_window_counts_packets_and_opus_silence() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    for p in [
        audio(TARGET, 1, &[0xF8, 0xFF, 0xFE]),
        audio(TARGET, 2, &[0xF8, 0xFF, 0xFE]),
        audio(TARGET, 3, &[0x78, 0x01]),
        // a video packet carrying the silence bytes is not audio
        video(1, 7, VoiceCodec::H264, 1),
        audio(99, 4, &[0x78, 0x01]),
    ] {
        inner.on_packet(&p);
    }
    let adapter: &dyn Adapter = &rig.adapter;
    let w = adapter.take_audio_window();
    assert_eq!(
        (w["packets"].as_u64(), w["silence"].as_u64()),
        (Some(3), Some(2))
    );
    let w = adapter.take_audio_window();
    assert_eq!(
        (w["packets"].as_u64(), w["silence"].as_u64()),
        (Some(0), Some(0))
    );
}

#[test]
fn ingress_audio_format_and_sequence_counters() {
    let rig = Rig::new();
    let celt20s = [&[0xFCu8][..], &[0; 9]].concat(); // config 31, stereo, 1 frame
    let silk60m2 = [0x19u8, 0x00]; // config 3 (SILK 60 ms), mono, code 1 -> 2 frames
    for (seq, payload) in [
        (65534u16, &celt20s[..]),
        (65535, &celt20s),
        (2, &silk60m2), // wrap 65535 -> 0, 1 missing twice
        (1, &celt20s),
        (2, &[0xF8, 0xFF, 0xFE]),
        (3, &[]),
        (4, &[0xFB]),
    ] {
        rig.adapter.inner.on_packet(&audio(TARGET, seq, payload));
    }
    let w = Adapter::take_audio_window(&rig.adapter);
    assert_eq!(
        (
            w["packets"].as_u64(),
            w["silence"].as_u64(),
            w["bad"].as_u64()
        ),
        (Some(7), Some(1), Some(2))
    );
    assert_eq!(
        (w["seq_gaps"].as_u64(), w["seq_back"].as_u64()),
        (Some(2), Some(2))
    );
    assert_eq!(w["durations"], json!({"20": 3, "120": 1}));
    assert_eq!(w["channels"], json!({"stereo": 3, "mono": 1}));
    assert_eq!(w["modes"], json!({"celt": 3, "silk": 1, "hybrid": 0}));
    assert_eq!(
        (w["multi_frame"].as_u64(), w["bytes"].as_u64()),
        (Some(1), Some(32))
    );
    assert!(w.get("kbps").is_some());
}

#[test]
fn video_gap_detected_from_sequence_holes() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    let seq = [
        (video(10, 7, VoiceCodec::H264, 3), false),
        (video(12, 7, VoiceCodec::H264, 1), true),
        (video(14, 7, VoiceCodec::H264, 2), false),
        (video(15, 7, VoiceCodec::H264, 1), false),
        (video(17, 7, VoiceCodec::H264, 1), true),
        (video(16, 7, VoiceCodec::H264, 1), false), // late/backward: no gap, state kept
        (video(18, 7, VoiceCodec::H264, 1), false),
        (video(3, 8, VoiceCodec::H264, 1), false), // fresh ssrc / codec state
        (video(5, 7, VoiceCodec::VP9, 1), false),
        (video(65535, 9, VoiceCodec::H264, 1), false),
        (video(1, 9, VoiceCodec::H264, 1), true), // wraps, one number missing
        (video(2, 9, VoiceCodec::H264, 1), false),
    ];
    for (p, _) in &seq {
        inner.on_packet(p);
    }
    let gaps: Vec<bool> = rig
        .ingress
        .lock()
        .unwrap()
        .iter()
        .map(|m| match m {
            Ingress::Video(v) => v.gap,
            _ => panic!(),
        })
        .collect();
    assert_eq!(gaps, seq.iter().map(|(_, g)| *g).collect::<Vec<_>>());
    inner.hot.lock().gaps.clear();
    inner.on_packet(&video(9, 7, VoiceCodec::H264, 1));
    let Some(Ingress::Video(last)) = rig.ingress.lock().unwrap().last().cloned() else {
        panic!()
    };
    assert!(!last.gap);
}

#[test]
fn codec_switch_forgets_the_old_codecs_sequence_memory() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    let frame = |seq, codec, pt| {
        let mut p = video(seq, 7, codec, 1);
        p.payload_type = pt;
        inner.on_packet(&p);
    };
    frame(10, VoiceCodec::AV1, 101);
    frame(11, VoiceCodec::AV1, 101);
    frame(12, VoiceCodec::H264, 105);
    // returning to AV1 starts fresh: no false gap
    frame(500, VoiceCodec::AV1, 101);
    let Some(Ingress::Video(last)) = rig.ingress.lock().unwrap().last().cloned() else {
        panic!()
    };
    assert!(!last.gap);
}

#[test]
fn the_first_video_packet_of_an_ssrc_is_remembered_for_the_video_request() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    inner.on_packet(&video(1, 7, VoiceCodec::H264, 1));
    assert_eq!(inner.hot.lock().video_ssrc, Some(7));
    inner.on_packet(&video(2, 8, VoiceCodec::H264, 1));
    assert_eq!(
        inner.hot.lock().video_ssrc,
        Some(8),
        "a new SSRC is requested again"
    );
}

// ------------------------------------------------------------- output and codecs

fn session_update(key: Option<&str>, codec: Option<&str>) -> ClientEvent {
    ClientEvent::SessionUpdate {
        stream_key: key.map(str::to_owned),
        info: SessionInfo {
            video_codec: codec.map(str::to_owned),
            ..SessionInfo::default()
        },
    }
}

#[test]
fn op14_of_the_adaptive_output_becomes_codec_changed() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    let out_key = format!("guild:{GUILD}:{CHAN}:{RELAY}");
    inner.on_client_event(session_update(Some(&out_key), Some("h265"))); // no output yet: ignored
    inner.st.lock().codec_hook = Some((out_key.clone(), 7));
    inner.on_client_event(session_update(Some(&out_key), Some("h265")));
    inner.on_client_event(session_update(Some(&out_key), None)); // no video codec in the update
    inner.on_client_event(session_update(Some(&real_key()), Some("av1"))); // the watch, not the output
    inner.on_client_event(session_update(None, Some("av1"))); // the voice connection itself
    assert_eq!(
        rig.model(),
        vec![Event::CodecChanged {
            generation: 7,
            negotiated_video_codec: Some("H265".into())
        }]
    );
}

#[test]
fn a_viewer_pli_the_library_cannot_answer_is_an_out_pli_of_the_ingest_output() {
    let rig = Rig::new();
    rig.adapter.inner.on_output_pli();
    assert_eq!(
        rig.events(),
        vec![AdapterEvent::Pool(PoolEvent::OutPli { account: None })]
    );
}

#[tokio::test]
async fn codec_updates_and_swaps_without_a_connection_are_noops() {
    let rig = Rig::new();
    let adapter: &dyn Adapter = &rig.adapter;
    assert!(
        !adapter
            .update_watch_codecs(&["H265", "H264"])
            .await
            .unwrap()
    );
    assert!(!adapter.update_output_codecs(&["H264"]).await.unwrap());
    let swapped = adapter
        .swap_output_source(Box::new(|_| panic!("no output to build a source for")))
        .await;
    assert_eq!(swapped, None);
    assert!(rig.logs().iter().any(|(n, _)| n == "codec_swap_skipped"));
    assert_eq!(adapter.output_codec(), None);
    adapter.end_output().await.unwrap();
    adapter.stop_watch().await.unwrap();
    assert!(adapter.request_pli().await.is_ok());
}

#[tokio::test]
async fn rewatch_needs_a_watch_to_renew_and_watch_needs_the_tracked_stream() {
    let rig = Rig::new();
    let adapter: &dyn Adapter = &rig.adapter;
    let err = adapter.rewatch().await.unwrap_err();
    assert!(err.to_string().contains("no watch to renew"));

    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    let tracked = rig.adapter.inner.st.lock().stream.clone().unwrap();
    // another incarnation than the tracked one
    let stale = SourceStream {
        incarnation: tracked.incarnation + 1,
        ..tracked.clone()
    };
    let err = adapter.watch(1, 1, stale).await.unwrap_err();
    assert!(err.to_string().contains("target stream changed"), "{err}");
    // the right stream, but the key does not belong to the configured target
    let wrong_key = SourceStream {
        key: "guild:1:2:77".into(),
        ..tracked.clone()
    };
    rig.adapter.inner.st.lock().stream = Some(wrong_key.clone());
    let err = adapter.watch(1, 1, wrong_key).await.unwrap_err();
    assert!(err.to_string().contains("does not match"), "{err}");
}

#[test]
fn network_stats_are_empty_without_connections_and_routing_is_changeable() {
    let rig = Rig::new();
    let adapter: &dyn Adapter = &rig.adapter;
    assert_eq!(adapter.network_stats(), json!({}));
    assert_eq!((adapter.target_id(), adapter.channel_id()), (TARGET, CHAN));
    adapter.set_routing(30, 20);
    assert_eq!((adapter.target_id(), adapter.channel_id()), (30, 20));
    // streams of the old target are no longer the target's
    rig.dispatch("STREAM_CREATE", Rig::stream_data(TARGET, CHAN));
    assert!(rig.events().is_empty());
}

fn example_config() -> relay_core::config::RelayConfig {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml");
    relay_core::config::load_config(&path).expect("the example config loads")
}

#[test]
fn adapter_options_follow_the_codec_policy_of_the_config() {
    use relay_core::config::OutputCodec;
    let mut cfg = example_config();
    cfg.discord.expected_relay_user_id = RELAY;
    cfg.logging.network_stats = true;
    cfg.runtime.self_deaf_ingest = false;

    let adaptive = AdapterOptions::from_config(&cfg);
    assert!(adaptive.adaptive);
    assert_eq!(adaptive.relay_id, RELAY);
    assert!(adaptive.network_stats && !adaptive.self_deaf);
    assert!(
        !adaptive.paused_verified,
        "the capability is the service's to verify"
    );
    // h264_only in the file is ignored by adaptive mode: every codec is offered
    assert_eq!(adaptive.parent_codecs.len(), 5);

    cfg.codecs.output_codec = OutputCodec::H264;
    let fixed = AdapterOptions::from_config(&cfg);
    assert!(!fixed.adaptive);
    assert_eq!(fixed.parent_codecs, [VoiceCodec::H264]);
    assert_eq!(fixed.watch_codecs, [VoiceCodec::H264]);
    let names: Vec<_> = decode_codecs(&fixed.watch_codecs)
        .iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(names, ["opus", "H264"]);
}

fn health(rtp: u64, packet: u64, decrypted: u64, stuck: bool) -> ReceiveHealth {
    ReceiveHealth {
        rtp_age: Duration::from_secs(rtp),
        packet_age: Duration::from_secs(packet),
        decrypted_age: Duration::from_secs(decrypted),
        undecryptable_age: Duration::from_secs(0),
        dave_stuck: stuck,
    }
}

#[test]
fn repair_reconnects_the_rtc_connection_when_no_rtp_arrives() {
    assert_eq!(
        decide_repair(&health(3, 3, 3, false), true, true),
        Repair::ReconnectRtc
    );
    // no RTP wins over a stuck DAVE session
    assert_eq!(
        decide_repair(&health(5, 5, 5, true), true, true),
        Repair::ReconnectRtc
    );
    assert_eq!(
        decide_repair(&health(5, 5, 5, false), false, true),
        Repair::Pli
    );
}

#[test]
fn repair_recovers_dave_when_packets_arrive_but_never_decrypt() {
    assert_eq!(
        decide_repair(&health(0, 0, 4, false), true, true),
        Repair::RecoverDave
    );
    assert_eq!(
        decide_repair(&health(0, 5, 5, true), true, true),
        Repair::RecoverDave
    );
    assert_eq!(
        decide_repair(&health(0, 0, 4, false), true, false),
        Repair::Pli
    );
}

#[test]
fn repair_sends_a_pli_when_the_receive_path_looks_healthy() {
    assert_eq!(
        decide_repair(&health(0, 0, 1, false), true, true),
        Repair::Pli
    );
    // the target went quiet but the connection and DAVE are fine
    assert_eq!(
        decide_repair(&health(0, 4, 4, false), true, true),
        Repair::Pli
    );
}
