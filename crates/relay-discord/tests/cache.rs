use relay_discord::{Cache, CacheChange, StreamKey, VoiceStateEntry};
use serde_json::json;

fn entry(channel: u64, self_stream: bool, self_video: bool) -> VoiceStateEntry {
    VoiceStateEntry {
        channel_id: Some(channel),
        self_stream,
        self_video,
    }
}

#[test]
fn stream_key_round_trips() {
    for (text, key) in [
        (
            "guild:1:2:3",
            StreamKey::Guild {
                guild_id: 1,
                channel_id: 2,
                owner_id: 3,
            },
        ),
        (
            "call:2:3",
            StreamKey::Call {
                channel_id: 2,
                owner_id: 3,
            },
        ),
        ("test:3", StreamKey::Test { owner_id: 3 }),
    ] {
        assert_eq!(text.parse::<StreamKey>().unwrap(), key);
        assert_eq!(key.to_string(), text);
    }
    assert_eq!(
        StreamKey::Guild {
            guild_id: 1,
            channel_id: 2,
            owner_id: 3
        }
        .guild_id(),
        Some(1)
    );
    assert_eq!(StreamKey::Test { owner_id: 3 }.channel_id(), None);
    for bad in [
        "",
        "guild:1:2",
        "guild:a:2:3",
        "call:1:2:3",
        "voice:1",
        "test:",
    ] {
        assert!(bad.parse::<StreamKey>().is_err(), "{bad:?}");
    }
}

fn ready() -> serde_json::Value {
    json!({
        "user": {"id": "99"},
        "geo_ordered_rtc_regions": ["rotterdam", "frankfurt"],
        "apex_experiments": {"installation": "inst.1"},
        "guilds": [
            {"id": "10", "voice_states": [{"user_id": "20", "channel_id": "30", "self_stream": true}]},
            {"properties": {"id": "11"}},
        ],
    })
}

#[test]
fn ready_and_supplemental_merge_voice_states_by_guild_index() {
    let mut c = Cache::new();
    c.apply("READY", &ready());
    assert_eq!(c.self_id(), Some(99));
    assert_eq!(c.installation_id(), Some("inst.1"));
    assert_eq!(c.preferred_rtc_regions(), ["rotterdam", "frankfurt"]);
    assert!(
        c.has_guild(10) && c.has_guild(11),
        "guild id also read from properties"
    );
    assert_eq!(c.voice_state(10, 20), Some(&entry(30, true, false)));

    c.apply(
        "READY_SUPPLEMENTAL",
        &json!({"guilds": [
            {"voice_states": [{"user_id": "21", "channel_id": "30", "self_video": true}]},
            {"voice_states": [{"user_id": "22", "channel_id": "31"}]},
        ]}),
    );
    assert_eq!(c.voice_state(10, 21), Some(&entry(30, false, true)));
    assert_eq!(c.voice_state(11, 22), Some(&entry(31, false, false)));
    assert_eq!(c.voice_states(10).count(), 2);

    // A new READY starts from scratch.
    c.apply("READY", &json!({"user": {"id": "99"}, "guilds": []}));
    assert!(!c.has_guild(10));
    assert_eq!(
        c.preferred_rtc_regions(),
        ["us-central"],
        "default when READY has none"
    );
}

#[test]
fn voice_state_update_tracks_join_move_leave_with_before_after() {
    let mut c = Cache::new();
    c.apply("READY", &ready());

    let join = c
        .apply(
            "VOICE_STATE_UPDATE",
            &json!({"guild_id": "10", "user_id": "55", "channel_id": "30", "self_stream": true, "self_video": true}),
        )
        .unwrap();
    assert_eq!(
        join,
        CacheChange::Voice {
            guild_id: Some(10),
            user_id: 55,
            before: None,
            after: entry(30, true, true)
        }
    );
    assert_eq!(c.voice_state(10, 55), Some(&entry(30, true, true)));

    let moved = c
        .apply(
            "VOICE_STATE_UPDATE",
            &json!({"guild_id": "10", "user_id": "55", "channel_id": "31"}),
        )
        .unwrap();
    assert_eq!(
        moved,
        CacheChange::Voice {
            guild_id: Some(10),
            user_id: 55,
            before: Some(entry(30, true, true)),
            after: entry(31, false, false),
        }
    );

    // Leaving: removed from the cache, `after` keeps the payload's (possibly stale) flags.
    let left = c
        .apply(
            "VOICE_STATE_UPDATE",
            &json!({"guild_id": "10", "user_id": "55", "channel_id": null, "self_stream": true}),
        )
        .unwrap();
    assert_eq!(
        left,
        CacheChange::Voice {
            guild_id: Some(10),
            user_id: 55,
            before: Some(entry(31, false, false)),
            after: VoiceStateEntry {
                channel_id: None,
                self_stream: true,
                self_video: false
            },
        }
    );
    assert_eq!(c.voice_state(10, 55), None);
}

#[test]
fn voice_state_update_for_unknown_guild_or_call_is_discarded() {
    let mut c = Cache::new();
    c.apply("READY", &ready());
    assert_eq!(
        c.apply(
            "VOICE_STATE_UPDATE",
            &json!({"guild_id": "404", "user_id": "1", "channel_id": "2"})
        ),
        None
    );
    assert_eq!(
        c.apply(
            "VOICE_STATE_UPDATE",
            &json!({"user_id": "1", "channel_id": "2"})
        ),
        None
    );
}

#[test]
fn guild_create_delete_and_passive_update() {
    let mut c = Cache::new();
    c.apply("READY", &ready());

    c.apply(
        "GUILD_CREATE",
        &json!({"id": "12", "voice_states": [{"user_id": "1", "channel_id": "2"}, {"user_id": "3", "channel_id": "2"}]}),
    );
    assert_eq!(c.voice_states(12).count(), 2);

    c.apply(
        "PASSIVE_UPDATE_V2",
        &json!({
            "guild_id": "12",
            "removed_voice_states": ["1"],
            "updated_voice_states": [{"user_id": "3", "channel_id": "5", "self_stream": true}, {"user_id": "7", "channel_id": "5"}],
        }),
    );
    assert_eq!(c.voice_state(12, 1), None);
    assert_eq!(c.voice_state(12, 3), Some(&entry(5, true, false)));
    assert_eq!(c.voice_state(12, 7), Some(&entry(5, false, false)));

    // Outage keeps the guild; a real removal drops it.
    c.apply("GUILD_DELETE", &json!({"id": "12", "unavailable": true}));
    assert!(c.has_guild(12));
    c.apply("GUILD_DELETE", &json!({"id": "12"}));
    assert!(!c.has_guild(12));
}

#[test]
fn stream_create_update_delete() {
    let mut c = Cache::new();
    let key: StreamKey = "guild:10:30:20".parse().unwrap();

    let created = c
        .apply(
            "STREAM_CREATE",
            &json!({"stream_key": "guild:10:30:20", "rtc_server_id": "77", "rtc_channel_id": "78", "region": "rotterdam", "viewer_ids": ["1", "2"], "paused": false}),
        )
        .unwrap();
    let CacheChange::Stream { before, after, .. } = created else {
        panic!()
    };
    assert_eq!(before, None);
    let s = after.unwrap();
    assert_eq!(
        (s.rtc_server_id, s.rtc_channel_id, s.region.as_deref()),
        (Some(77), Some(78), Some("rotterdam"))
    );
    assert_eq!(s.viewer_ids, [1, 2]);

    // Update only touches the fields present.
    let updated = c
        .apply(
            "STREAM_UPDATE",
            &json!({"stream_key": "guild:10:30:20", "paused": true}),
        )
        .unwrap();
    let CacheChange::Stream { before, after, .. } = updated else {
        panic!()
    };
    assert!(!before.unwrap().paused);
    let after = after.unwrap();
    assert!(after.paused);
    assert_eq!(after.viewer_ids, [1, 2]);
    assert_eq!(c.stream(&key).unwrap().region.as_deref(), Some("rotterdam"));

    // Unknown stream: discarded.
    assert_eq!(
        c.apply(
            "STREAM_UPDATE",
            &json!({"stream_key": "guild:1:2:3", "paused": true})
        ),
        None
    );

    // Outage: kept but flagged, and STREAM_CREATE clears the flag.
    c.apply(
        "STREAM_DELETE",
        &json!({"stream_key": "guild:10:30:20", "reason": "unknown", "unavailable": true}),
    );
    assert!(c.stream(&key).unwrap().unavailable);
    c.apply(
        "STREAM_CREATE",
        &json!({"stream_key": "guild:10:30:20", "rtc_server_id": "77", "rtc_channel_id": "78"}),
    );
    assert!(!c.stream(&key).unwrap().unavailable);

    let deleted = c
        .apply(
            "STREAM_DELETE",
            &json!({"stream_key": "guild:10:30:20", "reason": "user_requested"}),
        )
        .unwrap();
    let CacheChange::Stream { after, .. } = deleted else {
        panic!()
    };
    assert_eq!(after, None);
    assert!(c.stream(&key).is_none());
    assert_eq!(c.streams().count(), 0);
}

#[test]
fn unavailable_delete_of_unknown_stream_creates_a_flagged_stub() {
    let mut c = Cache::new();
    c.apply(
        "STREAM_DELETE",
        &json!({"stream_key": "call:5:6", "reason": "x", "unavailable": true}),
    );
    let s = c.stream(&"call:5:6".parse().unwrap()).unwrap();
    assert!(s.unavailable);
}

#[test]
fn channel_names_kept_updated_and_removed() {
    let mut cache = Cache::new();
    cache.apply(
        "GUILD_CREATE",
        &json!({"id": "1", "channels": [{"id": "70", "type": 2, "name": "Lounge"}, {"id": "71", "type": 2}]}),
    );
    assert_eq!(cache.channel_name(1, 70), Some("Lounge"));
    assert_eq!(cache.channel_name(1, 71), None);
    cache.apply(
        "CHANNEL_UPDATE",
        &json!({"guild_id": "1", "id": "70", "type": 2, "name": "Den"}),
    );
    assert_eq!(cache.channel_name(1, 70), Some("Den"));
    cache.apply("CHANNEL_DELETE", &json!({"guild_id": "1", "id": "70"}));
    assert_eq!(cache.channel_name(1, 70), None);
}

fn guild_with_directory() -> Cache {
    let mut cache = Cache::new();
    cache.apply(
        "GUILD_CREATE",
        &json!({
            "id": "1",
            "channels": [{"id": "70", "type": 2}, {"id": "71", "type": 0}, {"id": "72", "type": 13}],
            "roles": [{"id": "500", "name": "Mods"}],
            "members": [{"user": {"id": "8", "username": "eight", "global_name": null}, "nick": "Nick"}],
        }),
    );
    cache
}

#[test]
fn directory_lookups_for_routing_context() {
    let mut cache = guild_with_directory();
    // only plain voice channels count (not text, not stage)
    assert_eq!(cache.voice_channel_ids(1), vec![70]);
    assert!(cache.is_voice_channel(1, 70) && !cache.is_voice_channel(1, 71));
    assert!(cache.voice_channel_ids(2).is_empty());
    cache.apply(
        "CHANNEL_CREATE",
        &json!({"guild_id": "1", "id": "73", "type": 2}),
    );
    cache.apply("CHANNEL_DELETE", &json!({"guild_id": "1", "id": "70"}));
    let mut ids = cache.voice_channel_ids(1);
    ids.sort_unstable();
    assert_eq!(ids, vec![73]);

    // member nickname wins, then the global name, then the username
    assert_eq!(cache.display_name(1, 8).as_deref(), Some("Nick"));
    cache.apply(
        "GUILD_MEMBER_UPDATE",
        &json!({"guild_id": "1", "user": {"id": "8", "username": "eight", "global_name": "Eight"}, "nick": null}),
    );
    assert_eq!(cache.display_name(1, 8).as_deref(), Some("Eight"));
    // a user who is not a member of the guild but was seen elsewhere
    cache.apply(
        "MESSAGE_CREATE",
        &json!({"id": "900", "channel_id": "5", "author": {"id": "9", "username": "glob", "global_name": "Glob"}}),
    );
    assert_eq!(cache.display_name(1, 9).as_deref(), Some("Glob"));
    assert_eq!(cache.display_name(1, 10), None);
}

#[test]
fn directory_lookups_for_operator_list() {
    let mut cache = guild_with_directory();
    cache.apply(
        "GUILD_MEMBER_ADD",
        &json!({"guild_id": "1", "user": {"id": "11", "username": "human", "bot": false}}),
    );
    cache.apply(
        "GUILD_MEMBER_ADD",
        &json!({"guild_id": "1", "user": {"id": "12", "username": "robot", "bot": true}}),
    );
    assert!(cache.is_bot(12) && !cache.is_bot(11) && !cache.is_bot(10));
    assert_eq!(cache.role_name(1, 500), Some("Mods"));
    assert_eq!(cache.role_name(1, 501), None);
    cache.apply(
        "GUILD_ROLE_CREATE",
        &json!({"guild_id": "1", "role": {"id": "501", "name": "Admins"}}),
    );
    cache.apply(
        "GUILD_ROLE_DELETE",
        &json!({"guild_id": "1", "role_id": "500"}),
    );
    assert_eq!(cache.role_name(1, 501), Some("Admins"));
    assert_eq!(cache.role_name(1, 500), None);
    // a voice-state member object feeds the directory too
    let mut cache = guild_with_directory();
    cache.apply(
        "VOICE_STATE_UPDATE",
        &json!({"guild_id": "1", "user_id": "20", "channel_id": "70",
                "member": {"user": {"id": "20", "username": "twenty", "bot": true}, "nick": "T"}}),
    );
    assert!(cache.is_bot(20));
    assert_eq!(cache.display_name(1, 20).as_deref(), Some("T"));
}

#[test]
fn ready_user_name_is_known() {
    let mut c = Cache::new();
    c.apply(
        "READY",
        &json!({"user": {"id": "99", "username": "me", "global_name": "Me"}, "guilds": []}),
    );
    assert_eq!(c.display_name(10, 99).as_deref(), Some("Me"));
}
