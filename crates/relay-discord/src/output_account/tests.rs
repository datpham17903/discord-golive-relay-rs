//! Port of `tests/adapter/test_output_account.py` (and the output half of `test_adaptive_codec.py`)
//! for what runs without a voice server: dispatch handling, the pre-join checks and the bookkeeping
//! around `end`. Login and READY are covered by `tests/adapter_start.rs` against a fake gateway.

use std::sync::Mutex as StdMutex;

use relay_core::supervisor::outputs_model::PoolEvent;
use relay_media::{FrameSource, OutFrame};
use relay_voice::VoiceClientConfig;
use relay_voice::link::{
    GatewayLink, LinkError, LinkFuture, StreamCreateRequest, VoiceStateRequest,
};
use relay_voice::publish::VideoParams;
use relay_voice::voice_ws::SessionInfo;

use super::*;
use crate::api::OutputAdapter;

const USER: u64 = 7;
const GUILD: u64 = 1;
const CHAN: u64 = 2;

#[derive(Default)]
struct FakeLink {
    states: StdMutex<Vec<VoiceStateRequest>>,
}

impl GatewayLink for FakeLink {
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_> {
        self.states.lock().unwrap().push(request);
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

struct NoMedia;

impl FrameSource for NoMedia {
    fn next_video(&mut self) -> Option<OutFrame> {
        None
    }

    fn next_audio(&mut self) -> Vec<u8> {
        Vec::new()
    }
}

fn params() -> VideoParams {
    VideoParams {
        width: 1280,
        height: 720,
        fps: 30,
        bitrate: 0,
    }
}

struct Rig {
    adapter: OutputAccountAdapter,
    cache: SharedCache,
    events: Arc<StdMutex<Vec<PoolEvent>>>,
    logs: Arc<StdMutex<Vec<String>>>,
    link: Arc<FakeLink>,
}

fn options() -> OutputOptions {
    OutputOptions {
        user_id: USER,
        guild_id: GUILD,
        channel_id: CHAN,
        parent_codecs: vec![VoiceCodec::H264],
        adaptive: false,
        network_stats: false,
        excluded_viewer_ids: HashSet::new(),
        bot_user_ids: [40, 41].into(),
        self_deaf: true,
        urls: Urls::default(),
        connect_timeout: Duration::from_millis(200),
        health_poll: Duration::from_millis(10),
    }
}

impl Rig {
    fn new() -> Self {
        Self::with(options())
    }

    fn with(opts: OutputOptions) -> Self {
        let cache = Cache::shared();
        {
            let mut c = cache.write();
            c.apply(
                "READY",
                &json!({"user": {"id": USER.to_string()}, "guilds": []}),
            );
            c.apply(
                "GUILD_CREATE",
                &json!({"id": "1", "channels": [{"id": "2", "type": 2}, {"id": "77", "type": 2}]}),
            );
        }
        let events = Arc::new(StdMutex::new(Vec::new()));
        let logs = Arc::new(StdMutex::new(Vec::new()));
        let (e, l) = (Arc::clone(&events), Arc::clone(&logs));
        let adapter = OutputAccountAdapter::with_cache(
            opts,
            Arc::clone(&cache),
            Arc::new(move |ev| e.lock().unwrap().push(ev)),
            Arc::new(move |name, _| l.lock().unwrap().push(name.to_owned())),
        );
        let link = Arc::new(FakeLink::default());
        adapter.attach_detached(Net::detached(link.clone(), None));
        Self {
            adapter,
            cache,
            events,
            logs,
            link,
        }
    }

    fn dispatch(&self, name: &str, data: Value) {
        let change = self.cache.write().apply(name, &data);
        self.adapter.handle_dispatch(&Dispatch {
            name: name.to_owned(),
            seq: None,
            data,
            change,
        });
    }

    fn seed(&self, name: &str, data: Value) {
        self.cache.write().apply(name, &data);
    }

    fn events(&self) -> Vec<PoolEvent> {
        self.events.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// A voice client that is registered (as after a join), never connected.
    fn register_voice_client(&self) {
        let (vc, _events) = VoiceClient::new(VoiceClientConfig {
            user_id: USER,
            guild_id: Some(GUILD),
            channel_id: CHAN,
            gateway: self.link.clone(),
            http: wreq::Client::builder().build().unwrap(),
            rtc: RtcSettings::new(
                decode_codecs(&[VoiceCodec::H264]),
                vec![StreamSpec::video(100)],
                true,
            ),
        });
        self.adapter.inner.st.lock().vc = Some(vc);
    }

    fn vsu(&self, user: u64, channel: Option<u64>) {
        self.dispatch(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": "1", "user_id": user.to_string(),
                   "channel_id": channel.map(|c| c.to_string())}),
        );
    }
}

fn publish_args(rig: &Rig, channel: u64) -> ApiFuture<'_, Option<String>> {
    let adapter: &dyn OutputAdapter = &rig.adapter;
    adapter.publish(
        5,
        1,
        OutputSource::Ready(Box::new(NoMedia)),
        params(),
        channel,
        false,
    )
}

#[test]
fn voice_observed_before_the_client_applies_it() {
    let rig = Rig::new();
    let own = |session: Value, channel: Value| json!({"guild_id": "1", "user_id": USER.to_string(), "session_id": session, "channel_id": channel});
    // without a registered voice client nothing is observed
    rig.dispatch("VOICE_STATE_UPDATE", own(json!("s"), json!("2")));
    assert!(
        rig.events()
            .iter()
            .all(|e| !matches!(e, PoolEvent::OutVoiceObserved { .. }))
    );
    rig.clear();
    rig.register_voice_client();
    rig.dispatch("VOICE_STATE_UPDATE", own(json!("s"), json!("2")));
    rig.dispatch("VOICE_STATE_UPDATE", own(json!("s"), Value::Null));
    let observed: Vec<_> = rig
        .events()
        .into_iter()
        .filter(|e| matches!(e, PoolEvent::OutVoiceObserved { .. }))
        .collect();
    assert_eq!(
        observed,
        vec![
            PoolEvent::OutVoiceObserved {
                account: USER,
                session_id: Some("s".into()),
                channel_id: Some(2)
            },
            PoolEvent::OutVoiceObserved {
                account: USER,
                session_id: Some("s".into()),
                channel_id: None
            },
        ]
    );
}

#[test]
fn member_voice_updates_in_the_assigned_channel_report_participants() {
    let rig = Rig::new();
    rig.adapter.inner.channel.store(77, Ordering::Relaxed);
    for user in [5, 6] {
        rig.seed(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": "1", "user_id": user.to_string(), "channel_id": "77"}),
        );
    }
    rig.vsu(6, None); // leaves 77
    assert_eq!(
        rig.events(),
        vec![PoolEvent::MirrorParticipants {
            account: USER,
            participants: [5].into(),
            channel_id: 77
        }]
    );
    rig.clear();
    // moves between other channels: not ours
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "6", "channel_id": "3"}),
    );
    rig.vsu(6, Some(4));
    assert!(rig.events().is_empty());
}

#[test]
fn own_go_live_viewers_emitted_without_bots() {
    let rig = Rig::new();
    let stream = |name: &str, owner: u64, channel: u64, viewers: &[u64]| {
        rig.dispatch(
            name,
            json!({"stream_key": format!("guild:1:{channel}:{owner}"), "rtc_server_id": "9",
                   "rtc_channel_id": "9", "viewer_ids": viewers}),
        );
    };
    // not in voice yet: no data for a Go Live of this run
    stream("STREAM_CREATE", USER, CHAN, &[5]);
    assert!(rig.events().is_empty());
    rig.register_voice_client();
    // the account itself and the bots do not count
    stream("STREAM_UPDATE", USER, CHAN, &[USER, 40]);
    stream("STREAM_UPDATE", USER, CHAN, &[5, 41, 6]);
    stream("STREAM_UPDATE", USER, CHAN, &[6, 5]); // same viewers: no event
    stream("STREAM_CREATE", 8, CHAN, &[9]); // another user's Go Live
    stream("STREAM_CREATE", USER, 77, &[9]); // another channel
    assert_eq!(
        rig.events(),
        vec![
            PoolEvent::OutViewers {
                account: USER,
                viewers: HashSet::new()
            },
            PoolEvent::OutViewers {
                account: USER,
                viewers: [5, 6].into()
            },
        ]
    );
}

#[tokio::test]
async fn end_forgets_the_viewers_so_a_new_run_reports_again() {
    let rig = Rig::new();
    let stream = |viewers: &[u64]| {
        rig.dispatch(
            "STREAM_UPDATE",
            json!({"stream_key": "guild:1:2:7", "viewer_ids": viewers}),
        );
    };
    rig.seed(
        "STREAM_CREATE",
        json!({"stream_key": "guild:1:2:7", "rtc_server_id": "9", "rtc_channel_id": "9"}),
    );
    rig.register_voice_client();
    stream(&[5, 6]);
    assert_eq!(rig.events().len(), 1);
    let adapter: &dyn OutputAdapter = &rig.adapter;
    adapter.end().await.unwrap();
    rig.clear();
    stream(&[5]); // ended: the account is not live
    assert!(rig.events().is_empty());
    rig.register_voice_client();
    stream(&[5, 6]); // a new run reports again
    assert_eq!(
        rig.events(),
        vec![PoolEvent::OutViewers {
            account: USER,
            viewers: [5, 6].into()
        }]
    );
}

#[tokio::test]
async fn end_is_idempotent_and_close_ends_first() {
    let rig = Rig::new();
    let adapter: &dyn OutputAdapter = &rig.adapter;
    adapter.end().await.unwrap();
    adapter.end().await.unwrap();
    assert!(
        rig.link.states.lock().unwrap().is_empty(),
        "nothing to leave"
    );
    adapter.close().await;
    assert_eq!(adapter.user_id(), USER);
}

#[tokio::test]
async fn publish_does_not_join_while_excluded_viewer_present() {
    let rig = Rig::with(OutputOptions {
        excluded_viewer_ids: [555].into(),
        ..options()
    });
    for user in [5, 555] {
        rig.seed(
            "VOICE_STATE_UPDATE",
            json!({"guild_id": "1", "user_id": user.to_string(), "channel_id": "77"}),
        );
    }
    let err = publish_args(&rig, 77).await.unwrap_err();
    assert!(err.to_string().contains("excluded viewer"), "{err}");
    assert!(rig.link.states.lock().unwrap().is_empty(), "never joined");
    assert_eq!(
        rig.events(),
        vec![PoolEvent::MirrorParticipants {
            account: USER,
            participants: [5, 555].into(),
            channel_id: 77
        }]
    );
    // the viewer left: the next attempt joins (and, with no voice server in this test, times out)
    rig.seed(
        "VOICE_STATE_UPDATE",
        json!({"guild_id": "1", "user_id": "555", "channel_id": null}),
    );
    let err = publish_args(&rig, 77).await.unwrap_err();
    assert!(matches!(err, AdapterError::Voice(_)), "{err:?}");
    let first = rig.link.states.lock().unwrap()[0];
    assert_eq!(first.channel_id, Some(77), "joins the channel it is given");
    assert!(first.self_mute && first.self_deaf);
    assert!(
        rig.events()
            .iter()
            .all(|e| !matches!(e, PoolEvent::OutJoined { .. } | PoolEvent::OutReady { .. })),
        "a failed join reports neither"
    );
}

#[tokio::test]
async fn publish_self_deaf_follows_config() {
    for self_deaf in [true, false] {
        let rig = Rig::with(OutputOptions {
            self_deaf,
            ..options()
        });
        let _ = publish_args(&rig, CHAN).await;
        let first = rig.link.states.lock().unwrap()[0];
        assert!(first.self_mute);
        assert_eq!(first.self_deaf, self_deaf);
    }
}

#[tokio::test]
async fn publish_needs_a_known_voice_channel_and_a_started_adapter() {
    let rig = Rig::new();
    let err = publish_args(&rig, 555).await.unwrap_err();
    assert!(
        err.to_string().contains("voice channel not available"),
        "{err}"
    );

    let cache = Cache::shared();
    let bare =
        OutputAccountAdapter::with_cache(options(), cache, Arc::new(|_| {}), Arc::new(|_, _| {}));
    let adapter: &dyn OutputAdapter = &bare;
    let err = adapter
        .publish(
            1,
            1,
            OutputSource::Ready(Box::new(NoMedia)),
            params(),
            CHAN,
            false,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not started"), "{err}");
}

#[test]
fn op14_of_the_adaptive_output_becomes_out_codec_changed() {
    let rig = Rig::new();
    let inner = &rig.adapter.inner;
    let key = format!("guild:{GUILD}:{CHAN}:{USER}");
    let update = |key: Option<&str>, codec: Option<&str>| {
        inner.on_client_event(ClientEvent::SessionUpdate {
            stream_key: key.map(str::to_owned),
            info: SessionInfo {
                video_codec: codec.map(str::to_owned),
                ..SessionInfo::default()
            },
        });
    };
    update(Some(&key), Some("h265")); // no hook yet
    inner.st.lock().codec_hook = Some((key.clone(), 5, 2));
    update(Some(&key), Some("h265"));
    update(Some(&key), None);
    update(Some("guild:1:2:99"), Some("av1"));
    assert_eq!(
        rig.events(),
        vec![PoolEvent::OutCodecChanged {
            account: USER,
            generation: 5,
            attempt: 2,
            codec: "H265".into()
        }]
    );
}

#[tokio::test]
async fn codec_updates_and_swaps_without_an_output_are_noops() {
    let rig = Rig::new();
    let adapter: &dyn OutputAdapter = &rig.adapter;
    assert!(!adapter.update_codecs(&["H264"]).await.unwrap());
    let swapped = adapter
        .swap_source(Box::new(|_| panic!("no output to build a source for")))
        .await;
    assert_eq!(swapped, None);
    assert!(
        rig.logs
            .lock()
            .unwrap()
            .iter()
            .any(|n| n == "codec_swap_skipped")
    );
}

#[test]
fn network_stats_empty_without_client() {
    let rig = Rig::new();
    assert_eq!(OutputAdapter::network_stats(&rig.adapter), json!({}));
}

#[test]
fn output_options_follow_the_config() {
    use relay_core::config::{AccountRole, OutputAccount, load_config};
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml");
    let mut cfg = load_config(&path).expect("the example config loads");
    cfg.discord.expected_relay_user_id = 4;
    let account = OutputAccount {
        user_id: USER,
        credential: "relay-token-2".into(),
        role: AccountRole::Relay,
        channel_id: None,
    };
    cfg.discord.output_accounts = vec![account.clone()];
    cfg.authorization.excluded_viewer_ids = [555].into();
    cfg.runtime.self_deaf_outputs = false;

    let opts = OutputOptions::from_config(&cfg, &account);
    assert_eq!(opts.user_id, USER);
    assert_eq!(opts.guild_id, cfg.discord.guild_id);
    assert_eq!(opts.channel_id, cfg.discord.allowed_voice_channel_id);
    assert!(opts.adaptive && !opts.self_deaf);
    assert_eq!(opts.excluded_viewer_ids, HashSet::from([555]));
    assert_eq!(
        opts.bot_user_ids,
        HashSet::from([4, USER]),
        "the ingest and every output account"
    );
    assert_eq!(opts.parent_codecs.len(), 5, "adaptive offers every codec");
}

#[test]
fn health_tolerates_a_reconnect_until_it_gives_up_or_times_out() {
    let t0 = Instant::now();
    let mut health = HealthWatch::default();
    assert!(health.healthy(true, true, t0));
    // a server move or forced close: down but reconnecting
    assert!(health.healthy(false, true, t0 + Duration::from_secs(1)));
    assert!(health.healthy(false, true, t0 + Duration::from_secs(20)));
    // back up: the clock starts over for the next reconnect
    assert!(health.healthy(true, true, t0 + Duration::from_secs(21)));
    let next = t0 + Duration::from_secs(40);
    assert!(health.healthy(false, true, next));
    assert!(health.healthy(
        false,
        true,
        next + RECONNECT_TIMEOUT - Duration::from_millis(1)
    ));
    assert!(
        !health.healthy(false, true, next + RECONNECT_TIMEOUT),
        "a reconnect that takes too long"
    );

    // the client gave up (ClientEvent::Disconnected): no longer reconnecting
    let mut health = HealthWatch::default();
    assert!(health.healthy(false, true, t0));
    assert!(!health.healthy(false, false, t0 + Duration::from_millis(10)));
    // a plain drop is reported at once
    assert!(!HealthWatch::default().healthy(false, false, t0));
}
