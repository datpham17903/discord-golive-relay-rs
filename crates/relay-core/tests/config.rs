//! Port of `tests/test_config.py` (golive_relay.config). Python builds YAML with `yaml.safe_dump`; here the
//! fixtures are `serde_json::Value` documents written as JSON, which is valid YAML 1.2.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use relay_core::config::{
    AudioMode, CodecMode, CodecPolicy, ConfigInvalid, ControlConfig, OutputCodec, OutputProfile,
    QualityMode, RelayConfig, VideoMode, apply_account_ids, check_operable, ignored_setting_notes,
    load_config, load_config_with_warnings, unresolved_credentials,
};
use saphyr::{LoadableYamlNode, Yaml};
use serde_json::{Value, json};
use tempfile::tempdir;

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml")
}

fn yaml_to_json(y: &Yaml) -> Value {
    if let Some(m) = y.as_mapping() {
        let mut out = serde_json::Map::new();
        for (k, v) in m {
            out.insert(k.as_str().expect("string key").to_string(), yaml_to_json(v));
        }
        Value::Object(out)
    } else if let Some(s) = y.as_sequence() {
        Value::Array(s.iter().map(yaml_to_json).collect())
    } else if y.is_null() {
        Value::Null
    } else if let Some(b) = y.as_bool() {
        json!(b)
    } else if let Some(i) = y.as_integer() {
        json!(i)
    } else if let Some(f) = y.as_floating_point() {
        json!(f)
    } else {
        json!(y.as_str().expect("scalar"))
    }
}

fn raw() -> Value {
    let text = std::fs::read_to_string(example_path()).unwrap();
    let docs = Yaml::load_from_str(&text).unwrap();
    let mut raw = yaml_to_json(&docs[0]);
    // the example resolves it at startup
    let d = raw["discord"].as_object_mut().unwrap();
    d.entry("expected_relay_user_id")
        .or_insert(json!("100000000000000001"));
    raw
}

fn del(raw: &mut Value, section: &str, key: &str) {
    raw[section].as_object_mut().unwrap().remove(key);
}

fn write_text(dir: &Path, text: &str) -> PathBuf {
    let p = dir.join("config.yaml");
    std::fs::write(&p, text).unwrap();
    p
}

fn load(dir: &Path, raw: &Value) -> Result<RelayConfig, ConfigInvalid> {
    load_config(&write_text(dir, &serde_json::to_string(raw).unwrap()))
}

fn load_ok(dir: &Path, raw: &Value) -> RelayConfig {
    load(dir, raw).unwrap_or_else(|e| panic!("config rejected: {:?}", e.errors))
}

fn errors(dir: &Path, raw: &Value) -> Vec<String> {
    load(dir, raw).expect_err("config accepted").errors
}

fn any_err(errs: &[String], needle: &str) -> bool {
    errs.iter().any(|e| e.contains(needle))
}

fn example() -> RelayConfig {
    load_config(&example_path()).unwrap()
}

fn manifests() -> Value {
    json!({"live": {"source_dead_timeout_seconds": 5}})
}

fn pairs(accounts: &[relay_core::config::OutputAccount]) -> Vec<(u64, &str)> {
    accounts
        .iter()
        .map(|a| (a.user_id, a.credential.as_str()))
        .collect()
}

#[test]
fn test_example_loads_and_ids_are_int() {
    let cfg = example();
    assert_eq!(cfg.discord.expected_relay_user_id, 0);
    assert_eq!(cfg.discord.guild_id, 100000000000000002);
    assert_eq!(cfg.discord.target_user_id, 100000000000000003);
    assert_eq!(cfg.discord.allowed_voice_channel_id, 100000000000000004);
    assert_eq!(
        cfg.runtime.retry_delays_seconds,
        vec![1.0, 2.0, 4.0, 8.0, 15.0]
    );
    assert_eq!(cfg.liveness.source_dead_timeout_seconds, None);
    assert!(cfg.authorization.excluded_viewer_ids.is_empty());
}

#[test]
fn test_output_profile() {
    let cfg = example();
    let prof: OutputProfile = cfg.output_profile();
    assert_eq!(
        (prof.mode, prof.max_width, prof.max_height, prof.fps),
        (QualityMode::Auto, 2560, 1440, 60)
    );
    assert_eq!(prof.video_bitrate_kbps, 0); // auto
    assert_eq!(prof.keyframe_interval_s, 2.0);
    assert!(prof.audio_enabled);
    assert_eq!(
        (
            cfg.codecs.video_encoder.as_str(),
            cfg.codecs.hw_decode.as_str()
        ),
        ("auto", "none")
    );
}

#[test]
fn test_encoder_and_hw_decode_keys() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["codecs"]["video_encoder"] = json!("h264_amf");
    r["codecs"]["hw_decode"] = json!("none");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(
        (
            cfg.codecs.video_encoder.as_str(),
            cfg.codecs.hw_decode.as_str()
        ),
        ("h264_amf", "none")
    );
    r["codecs"]["video_encoder"] = json!("h265_foo");
    assert!(any_err(&errors(tmp.path(), &r), "codecs.video_encoder"));
    let mut r = raw();
    del(&mut r, "codecs", "video_encoder");
    del(&mut r, "codecs", "hw_decode"); // both optional
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(
        (
            cfg.codecs.video_encoder.as_str(),
            cfg.codecs.hw_decode.as_str()
        ),
        ("auto", "none")
    );
}

#[test]
fn test_hw_decode_typo_rejected_and_devices_match_hwaccel() {
    // The Python test also compares HW_DECODE_DEVICES with media.hwaccel.HW_DEVICE_ORDER (dropped: no hwaccel).
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["codecs"]["hw_decode"] = json!("d3d11");
    assert!(any_err(&errors(tmp.path(), &r), "codecs.hw_decode"));
    for device in ["none", "d3d11va", "cuda", "vaapi", "videotoolbox", "auto"] {
        r["codecs"]["hw_decode"] = json!(device);
        assert_eq!(load_ok(tmp.path(), &r).codecs.hw_decode, device);
    }
}

#[test]
fn test_target_equals_relay_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["discord"]["target_user_id"] = r["discord"]["expected_relay_user_id"].clone();
    assert!(any_err(&errors(tmp.path(), &r), "target_user_id"));
}

#[test]
fn test_odd_fixed_width_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["quality"]["fixed_width"] = json!(1281);
    assert!(any_err(&errors(tmp.path(), &r), "fixed_width"));
}

#[test]
fn test_recording_enabled_true_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["runtime"]["recording_enabled"] = json!(true);
    assert!(any_err(&errors(tmp.path(), &r), "recording_enabled"));
}

#[test]
fn test_include_media_true_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["logging"]["include_media"] = json!(true);
    assert!(any_err(&errors(tmp.path(), &r), "include_media"));
}

#[test]
fn test_audio_source_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["audio"]["source"] = json!("mix");
    assert!(any_err(&errors(tmp.path(), &r), "audio.source"));
}

#[test]
fn test_unknown_top_level_key() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["enabled"] = json!(true);
    assert!(any_err(&errors(tmp.path(), &r), "enabled"));
}

#[test]
fn test_unknown_section_key() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["quality"]["bogus"] = json!(1);
    let errs = errors(tmp.path(), &r);
    assert!(any_err(&errs, "bogus"));
    assert!(errs.contains(&"quality.bogus: unknown key".to_string()));
}

#[test]
fn test_bad_id() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["discord"]["guild_id"] = json!("abc");
    let errs = errors(tmp.path(), &r);
    assert!(any_err(&errs, "guild_id"));
    assert!(errs.contains(&"discord.guild_id: must be a string of decimal digits".to_string()));
}

#[test]
fn test_retry_delays_invalid() {
    let tmp = tempdir().unwrap();
    for delays in [json!([1, 0]), json!([-1]), json!([]), json!("x")] {
        let mut r = raw();
        r["runtime"]["retry_delays_seconds"] = delays;
        assert!(any_err(&errors(tmp.path(), &r), "retry_delays_seconds"));
    }
}

#[test]
fn test_missing_file_is_config_invalid() {
    let tmp = tempdir().unwrap();
    assert!(load_config(&tmp.path().join("nope.yaml")).is_err());
}

#[test]
fn test_errors_are_collected() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["quality"]["fixed_width"] = json!(1281);
    r["runtime"]["recording_enabled"] = json!(true);
    assert!(errors(tmp.path(), &r).len() >= 2);
}

#[test]
fn test_check_operable_null_timeout() {
    let cfg = example();
    let errs = check_operable(&cfg, &manifests());
    assert!(any_err(&errs, "source_dead_timeout_seconds"));
}

fn operable_cfg(tmp: &Path) -> RelayConfig {
    let mut r = raw();
    r["liveness"]["source_dead_timeout_seconds"] = json!(5);
    load_ok(tmp, &r)
}

#[test]
fn test_check_operable_ok() {
    let tmp = tempdir().unwrap();
    let cfg = operable_cfg(tmp.path());
    assert_eq!(check_operable(&cfg, &manifests()), Vec::<String>::new());
}

#[test]
fn test_check_operable_ignores_missing_profile_manifest() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["liveness"]["source_dead_timeout_seconds"] = json!(5);
    r["quality"]["approved_profile_manifest"] = json!("/nonexistent/verified-profiles.json");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(check_operable(&cfg, &manifests()), Vec::<String>::new());
    assert!(any_err(
        &ignored_setting_notes(&cfg),
        "approved_profile_manifest"
    ));
}

#[test]
fn test_check_operable_malformed_capability_live() {
    let tmp = tempdir().unwrap();
    let cfg = operable_cfg(tmp.path());
    for cap_live in [Value::Null, json!("x"), json!([])] {
        let mut cap = manifests();
        cap["live"] = cap_live;
        assert!(any_err(&check_operable(&cfg, &cap), "capability manifest"));
    }
}

#[test]
fn test_keyframe_interval_must_be_within_2s() {
    let tmp = tempdir().unwrap();
    for value in [json!(0), json!(-1), json!(2.5)] {
        let mut r = raw();
        r["runtime"]["keyframe_interval_seconds"] = value;
        assert!(any_err(
            &errors(tmp.path(), &r),
            "keyframe_interval_seconds"
        ));
    }
}

fn with_outputs(outputs: Value, extra: &[(&str, Value)]) -> Value {
    let mut r = raw();
    del(&mut r, "discord", "relay_accounts");
    del(&mut r, "discord", "mirror_accounts");
    r["discord"]["output_accounts"] = outputs;
    for (k, v) in extra {
        r["discord"][*k] = v.clone();
    }
    r
}

#[test]
fn test_output_accounts_default_empty() {
    let cfg = example();
    assert!(cfg.output_accounts().is_empty());
    assert!(cfg.discord.ingest_credential == "relay-token" && cfg.discord.ingest_publishes);
}

#[test]
fn test_output_accounts_parsed() {
    let tmp = tempdir().unwrap();
    let cfg = load_ok(
        tmp.path(),
        &with_outputs(
            json!([
                {"user_id": "5", "credential": "relay-token-2"},
                {"user_id": "6", "credential": "r.3"},
            ]),
            &[
                ("ingest_publishes", json!(false)),
                ("ingest_credential", json!("main")),
            ],
        ),
    );
    assert_eq!(
        pairs(&cfg.output_accounts()),
        vec![(5, "relay-token-2"), (6, "r.3")]
    );
    assert!(!cfg.discord.ingest_publishes && cfg.discord.ingest_credential == "main");
}

#[test]
fn test_output_accounts_rejected() {
    let tmp = tempdir().unwrap();
    let ten: Vec<Value> = (0..10)
        .map(|i| json!({"user_id": (i + 5).to_string(), "credential": format!("c{i}")}))
        .collect();
    let cases = [
        json!([{"user_id": "100000000000000003", "credential": "a"}]), // target
        json!([{"user_id": "100000000000000001", "credential": "a"}]), // ingest relay
        json!([{"user_id": "5", "credential": "a"}, {"user_id": "5", "credential": "b"}]), // dup id
        json!([{"user_id": "5", "credential": "a"}, {"user_id": "6", "credential": "a"}]), // dup credential
        json!([{"user_id": "5", "credential": "relay-token"}]), // clashes with ingest credential
        json!([{"user_id": "5", "credential": "../x"}]),
        json!([{"user_id": "5", "credential": ""}]),
        json!([{"user_id": 5, "credential": "a"}]),
        json!([{"user_id": "5"}]),
        Value::Array(ten), // > 9
        json!("x"),
    ];
    for outputs in cases {
        let errs = errors(tmp.path(), &with_outputs(outputs.clone(), &[]));
        assert!(any_err(&errs, "output_accounts"), "{outputs}: {errs:?}");
    }
}

#[test]
fn test_nine_output_accounts_allowed() {
    let tmp = tempdir().unwrap();
    let nine: Vec<Value> = (0..9)
        .map(|i| json!({"user_id": (i + 5).to_string(), "credential": format!("c{i}")}))
        .collect();
    let cfg = load_ok(tmp.path(), &with_outputs(Value::Array(nine), &[]));
    assert_eq!(cfg.output_accounts().len(), 9);
}

#[test]
fn test_bad_ingest_options_rejected() {
    let tmp = tempdir().unwrap();
    let errs = errors(
        tmp.path(),
        &with_outputs(
            json!([]),
            &[
                ("ingest_credential", json!("a/b")),
                ("ingest_publishes", json!("yes")),
            ],
        ),
    );
    assert!(any_err(&errs, "ingest_credential") && any_err(&errs, "ingest_publishes"));
}

#[test]
fn test_network_stats_default_false_and_true() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    assert!(!load_ok(tmp.path(), &r).logging.network_stats);
    r["logging"]["network_stats"] = json!(true);
    assert!(load_ok(tmp.path(), &r).logging.network_stats);
    r["logging"]["network_stats"] = json!("yes");
    assert!(any_err(&errors(tmp.path(), &r), "network_stats"));
}

#[test]
fn test_video_mode_default_and_parse() {
    // Differs from Python: this build only passes through, so a missing key means passthrough.
    let tmp = tempdir().unwrap();
    let mut r = raw();
    del(&mut r, "codecs", "video_mode");
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.video_mode,
        VideoMode::Passthrough
    );
    r["codecs"]["video_mode"] = json!("passthrough");
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.video_mode,
        VideoMode::Passthrough
    );
    r["codecs"]["video_mode"] = json!("transcode");
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.video_mode,
        VideoMode::Transcode
    );
    r["codecs"]["video_mode"] = json!("auto");
    assert!(any_err(&errors(tmp.path(), &r), "codecs.video_mode"));
}

#[test]
fn test_audio_mode_default_and_parse() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    del(&mut r, "codecs", "audio_mode");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(cfg.codecs.audio_mode, AudioMode::Passthrough);
    r["codecs"]["audio_mode"] = json!("transcode");
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.audio_mode,
        AudioMode::Transcode
    );
    r["codecs"]["audio_mode"] = json!("auto");
    assert!(any_err(&errors(tmp.path(), &r), "codecs.audio_mode"));
}

#[test]
fn test_codec_modes_accept_h264_only() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["codecs"]["parent_codecs"] = json!("h264_only");
    r["codecs"]["watch_codecs"] = json!("h264_only");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(
        (cfg.codecs.parent_codecs, cfg.codecs.watch_codecs),
        (CodecPolicy::H264Only, CodecPolicy::H264Only)
    );
    r["codecs"]["watch_codecs"] = json!("av1_only");
    let errs = errors(tmp.path(), &r);
    assert!(any_err(&errs, "codecs.watch_codecs"));
    assert!(
        errs.contains(
            &"codecs.watch_codecs: must be one of ['auto_decode', 'h264_only']".to_string()
        )
    );
}

#[test]
fn test_control_config_defaults_when_absent_and_example() {
    let cfg = example();
    assert!(cfg.control.chat_commands && cfg.control.command_prefix == "!relay");
    assert!(cfg.control.operator_user_ids.is_empty() && cfg.control.command_channel_id.is_none());
    assert_eq!(
        cfg.control.public_commands,
        ["status", "mirror", "unmirror"].map(String::from).into()
    );
    assert_eq!(
        (
            cfg.control.command_cooldown_seconds,
            cfg.control.change_cooldown_seconds
        ),
        (5, 3)
    );
    let d = ControlConfig::default();
    assert!(d.chat_commands);
    assert_eq!(
        d.public_commands,
        ["status", "mirror", "unmirror"].map(String::from).into()
    );
    assert_eq!(
        (d.command_cooldown_seconds, d.change_cooldown_seconds),
        (5, 3)
    );
}

#[test]
fn test_control_public_commands_and_cooldowns() {
    let tmp = tempdir().unwrap();
    let set = |names: &[&str]| names.iter().map(|s| s.to_string()).collect();
    let mut r = raw();
    r["control"] = json!({});
    let c = load_ok(tmp.path(), &r).control;
    assert!(c.chat_commands && c.public_commands == set(&["status", "mirror", "unmirror"]));
    r["control"] = json!({"public_commands": ["autoscale"]});
    assert_eq!(
        load_ok(tmp.path(), &r).control.public_commands,
        set(&["autoscale"])
    ); // a valid name, opt-in only
    r["control"] = json!({"public_commands": ["status", "unmirror"], "command_cooldown_seconds": 0,
                          "change_cooldown_seconds": 10});
    let c = load_ok(tmp.path(), &r).control;
    assert_eq!(c.public_commands, set(&["status", "unmirror"]));
    assert_eq!(
        (c.command_cooldown_seconds, c.change_cooldown_seconds),
        (0, 10)
    );
    r["control"] = json!({"public_commands": []});
    assert!(load_ok(tmp.path(), &r).control.public_commands.is_empty());
    for bad in [json!(["status", "bogus"]), json!("status"), json!([1])] {
        r["control"] = json!({"public_commands": bad});
        assert!(
            any_err(&errors(tmp.path(), &r), "control.public_commands"),
            "{bad}"
        );
    }
    for key in ["command_cooldown_seconds", "change_cooldown_seconds"] {
        for bad in [json!(-1), json!("5"), json!(true), json!(1.5)] {
            r["control"] = json!({ key: bad });
            assert!(
                any_err(&errors(tmp.path(), &r), &format!("control.{key}")),
                "{key} {bad}"
            );
        }
    }
}

#[test]
fn test_control_config_operators_optional_when_enabled() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["control"] = json!({"chat_commands": true, "operator_user_ids": []});
    let c = load_ok(tmp.path(), &r).control; // no operators: only public commands work
    assert!(c.chat_commands && c.operator_user_ids.is_empty() && c.operator_role_ids.is_empty());
    r["control"] = json!({"chat_commands": true, "operator_user_ids": ["42"],
                          "command_channel_id": "7", "command_prefix": "!r"});
    let c = load_ok(tmp.path(), &r).control;
    assert!(
        c.operator_user_ids == [42].into()
            && c.command_channel_id == Some(7)
            && c.command_prefix == "!r"
    );
    r["control"] = json!({"chat_commands": true, "operator_role_ids": ["77"]});
    let c = load_ok(tmp.path(), &r).control;
    assert!(c.operator_role_ids == [77].into() && c.operator_user_ids.is_empty());
    r["control"] = json!({"chat_commands": true, "operator_user_ids": [], "operator_role_ids": []});
    assert!(load_ok(tmp.path(), &r).control.chat_commands);
    r["control"] = json!({"operator_role_ids": ["x"]});
    assert!(any_err(
        &errors(tmp.path(), &r),
        "control.operator_role_ids"
    ));
    r["control"] = json!({"command_prefix": "! relay"});
    assert!(any_err(&errors(tmp.path(), &r), "control.command_prefix"));
    r["control"] = json!({"bogus": 1});
    assert!(any_err(&errors(tmp.path(), &r), "control.bogus"));
}

#[test]
fn test_self_deaf_defaults_on_and_validates() {
    let tmp = tempdir().unwrap();
    for key in ["self_deaf_ingest", "self_deaf_outputs"] {
        let get = |cfg: RelayConfig| match key {
            "self_deaf_ingest" => cfg.runtime.self_deaf_ingest,
            _ => cfg.runtime.self_deaf_outputs,
        };
        let mut r = raw();
        del(&mut r, "runtime", key);
        assert!(get(load_ok(tmp.path(), &r)));
        r["runtime"][key] = json!(false);
        assert!(!get(load_ok(tmp.path(), &r)));
        r["runtime"][key] = json!("yes");
        assert!(any_err(&errors(tmp.path(), &r), key));
    }
}

#[test]
fn test_prejoin_voice_defaults_on() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    del(&mut r, "runtime", "prejoin_voice");
    assert!(load_ok(tmp.path(), &r).runtime.prejoin_voice);
    r["runtime"]["prejoin_voice"] = json!(false);
    assert!(!load_ok(tmp.path(), &r).runtime.prejoin_voice);
    r["runtime"]["prejoin_voice"] = json!("yes");
    assert!(any_err(&errors(tmp.path(), &r), "prejoin_voice"));
}

#[test]
fn test_control_is_operator_by_user_role_or_neither() {
    let c = ControlConfig {
        operator_user_ids: [1].into(),
        operator_role_ids: [10].into(),
        ..ControlConfig::default()
    };
    let none = Default::default();
    assert!(c.is_operator(1, &none, &none));
    assert!(c.is_operator(2, &[10, 11].into(), &none));
    assert!(!c.is_operator(2, &[11].into(), &none));
    assert!(c.is_owner(1, &none) && c.is_owner(2, &[10].into()));
    assert!(!c.is_owner(3, &none) && !c.is_owner(3, &[11].into()));
    // chat-added operators are operators but never owners
    assert!(c.is_operator(3, &none, &[3].into()) && !c.is_operator(4, &none, &[3].into()));
}

#[test]
fn test_control_public_commands_never_include_op() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["control"] = json!({"public_commands": ["status", "op"]});
    assert!(any_err(&errors(tmp.path(), &r), "control.public_commands"));
}

#[test]
fn test_control_config_rejects_bad_command_channel() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    for bad in [
        json!(7),
        json!("abc"),
        json!("-7"),
        json!("0"),
        json!(["7"]),
    ] {
        r["control"] = json!({"command_channel_id": bad});
        assert!(
            any_err(&errors(tmp.path(), &r), "control.command_channel_id"),
            "{bad}"
        );
    }
    r["control"] = json!({"chat_commands": true});
    assert!(load_ok(tmp.path(), &r).control.chat_commands);
}

fn with_pools(relay: Value, mirror: Value) -> Value {
    let mut r = raw();
    del(&mut r, "discord", "output_accounts");
    r["discord"]["relay_accounts"] = relay;
    r["discord"]["mirror_accounts"] = mirror;
    r
}

#[test]
fn test_relay_and_mirror_pools_parse() {
    let tmp = tempdir().unwrap();
    let cfg = load_ok(
        tmp.path(),
        &with_pools(
            json!([{"user_id": "5", "credential": "r2"}, {"user_id": "6", "credential": "r3"}]),
            json!([{"user_id": "7", "credential": "m1", "channel_id": "900"},
                   {"user_id": "8", "credential": "m2"}]),
        ),
    );
    let d = &cfg.discord;
    assert_eq!(pairs(&d.relay_accounts()), vec![(5, "r2"), (6, "r3")]);
    assert!(
        d.relay_accounts()
            .iter()
            .all(|a| a.role.as_str() == "relay" && a.channel_id.is_none())
    );
    let mirrors = d.mirror_accounts();
    let m: Vec<_> = mirrors
        .iter()
        .map(|a| (a.user_id, a.credential.as_str(), a.channel_id))
        .collect();
    assert_eq!(m, vec![(7, "m1", Some(900)), (8, "m2", None)]);
    assert!(
        d.mirror_accounts()
            .iter()
            .all(|a| a.role.as_str() == "mirror")
    );
    assert_eq!(
        cfg.output_accounts()
            .iter()
            .map(|a| a.user_id)
            .collect::<Vec<_>>(),
        vec![5, 6, 7, 8]
    );
    let r = with_pools(
        json!([{"user_id": "5", "credential": "r2"}]),
        json!([{"user_id": "5", "credential": "m1"}]),
    );
    assert!(any_err(&errors(tmp.path(), &r), "mirror_accounts[0]")); // id shared across pools
    let main = raw()["discord"]["allowed_voice_channel_id"].clone();
    let r = with_pools(
        json!([]),
        json!([{"user_id": "7", "credential": "m1", "channel_id": main}]),
    );
    assert!(
        errors(tmp.path(), &r)
            .iter()
            .any(|e| e.contains("mirror_accounts[0]") && e.contains("channel_id"))
    );
    let r = with_pools(
        json!([]),
        json!([{"user_id": "7", "credential": "m1", "channel_id": 900}]),
    );
    assert!(any_err(&errors(tmp.path(), &r), "mirror_accounts[0]"));
    let r = with_pools(
        json!([]),
        json!([{"user_id": "7", "credential": "m1", "bogus": "1"}]),
    );
    assert!(any_err(&errors(tmp.path(), &r), "mirror_accounts[0]"));
    let r = with_pools(
        json!([{"user_id": "5", "credential": "r2", "channel_id": "900"}]),
        json!([]),
    );
    assert!(any_err(&errors(tmp.path(), &r), "relay_accounts[0]"));
    let relays: Vec<Value> = (0..5)
        .map(|i| json!({"user_id": (i + 5).to_string(), "credential": format!("r{i}")}))
        .collect();
    let mirrors: Vec<Value> = (0..5)
        .map(|i| json!({"user_id": (i + 20).to_string(), "credential": format!("m{i}")}))
        .collect();
    let r = with_pools(Value::Array(relays), Value::Array(mirrors));
    assert!(any_err(&errors(tmp.path(), &r), "at most 9"));
}

#[test]
fn test_output_accounts_alias_to_relay_with_warning() {
    let tmp = tempdir().unwrap();
    let r = with_outputs(json!([{"user_id": "5", "credential": "r2"}]), &[]);
    let p = write_text(tmp.path(), &serde_json::to_string(&r).unwrap());
    let (cfg, warnings) = load_config_with_warnings(&p).unwrap();
    assert!(
        warnings.iter().any(|w| w.contains("output_accounts")),
        "{warnings:?}"
    );
    let relay_accounts = cfg.discord.relay_accounts();
    let relays: Vec<_> = relay_accounts
        .iter()
        .map(|a| (a.user_id, a.role.as_str()))
        .collect();
    assert_eq!(relays, vec![(5, "relay")]);
    assert!(cfg.discord.mirror_accounts().is_empty());
}

#[test]
fn test_output_and_new_lists_together_rejected() {
    let tmp = tempdir().unwrap();
    let mut r = with_pools(json!([]), json!([]));
    r["discord"]["output_accounts"] = json!([]);
    assert!(any_err(&errors(tmp.path(), &r), "output_accounts"));
}

#[test]
fn test_two_mirrors_same_fixed_channel_rejected() {
    let tmp = tempdir().unwrap();
    let r = with_pools(
        json!([]),
        json!([{"user_id": "7", "credential": "m1", "channel_id": "900"},
               {"user_id": "8", "credential": "m2", "channel_id": "900"}]),
    );
    assert!(
        errors(tmp.path(), &r)
            .iter()
            .any(|e| e.contains("mirror_accounts[1]") && e.contains("channel_id"))
    );
}

#[test]
fn test_new_runtime_keys_defaults() {
    let tmp = tempdir().unwrap();
    let rt = example().runtime;
    assert_eq!(
        (
            rt.relay_autoscale,
            rt.relay_idle_stop_seconds,
            rt.stream_viewer_cap
        ),
        (true, 30.0, 50)
    );
    let mut r = raw();
    for k in [
        "relay_autoscale",
        "relay_idle_stop_seconds",
        "stream_viewer_cap",
    ] {
        del(&mut r, "runtime", k);
    }
    let rt = load_ok(tmp.path(), &r).runtime;
    assert_eq!(
        (
            rt.relay_autoscale,
            rt.relay_idle_stop_seconds,
            rt.stream_viewer_cap
        ),
        (true, 30.0, 50)
    );
    r["runtime"]["relay_autoscale"] = json!(false);
    r["runtime"]["relay_idle_stop_seconds"] = json!(5);
    r["runtime"]["stream_viewer_cap"] = json!(1);
    let rt = load_ok(tmp.path(), &r).runtime;
    assert_eq!(
        (
            rt.relay_autoscale,
            rt.relay_idle_stop_seconds,
            rt.stream_viewer_cap
        ),
        (false, 5.0, 1)
    );
    for (key, bad) in [
        ("relay_autoscale", json!("yes")),
        ("relay_idle_stop_seconds", json!(0)),
        ("stream_viewer_cap", json!(0)),
    ] {
        let mut r = raw();
        r["runtime"][key] = bad;
        assert!(
            any_err(&errors(tmp.path(), &r), &format!("runtime.{key}")),
            "{key}"
        );
    }
}

#[test]
fn test_relay_scale_down_seconds_is_a_deprecated_no_op() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["runtime"]["relay_scale_down_seconds"] = json!(120);
    let p = write_text(tmp.path(), &serde_json::to_string(&r).unwrap());
    let (cfg, warnings) = load_config_with_warnings(&p).unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("relay_scale_down_seconds")),
        "{warnings:?}"
    );
    assert_eq!(cfg.runtime.relay_idle_stop_seconds, 30.0);
}

#[test]
fn test_example_defaults_debounce_100_prejoin_true() {
    let cfg = example();
    assert!(cfg.runtime.start_debounce_ms == 100 && cfg.runtime.prejoin_voice);
    assert!(cfg.discord.relay_accounts().is_empty() && cfg.discord.mirror_accounts().is_empty());
    assert!(raw()["discord"].get("output_accounts").is_none());
}

#[test]
fn test_credential_only_accounts_parse_with_unresolved_ids() {
    let tmp = tempdir().unwrap();
    let mut r = with_pools(
        json!([{"credential": "r2"}, {"user_id": "6", "credential": "r3"}]),
        json!([{"credential": "m1", "channel_id": "900"}]),
    );
    del(&mut r, "discord", "expected_relay_user_id");
    let cfg = load_ok(tmp.path(), &r);
    let d = &cfg.discord;
    assert_eq!(d.expected_relay_user_id, 0);
    let got: Vec<_> = d
        .output_accounts
        .iter()
        .map(|a| {
            (
                a.user_id,
                a.credential.as_str(),
                a.role.as_str(),
                a.channel_id,
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (0, "r2", "relay", None),
            (6, "r3", "relay", None),
            (0, "m1", "mirror", Some(900))
        ]
    );
    assert_eq!(
        unresolved_credentials(&cfg),
        vec!["relay-token", "r2", "m1"]
    );
    assert!(unresolved_credentials(&load_ok(tmp.path(), &raw())).is_empty());
    let r = with_pools(
        json!([{"credential": "r2"}, {"credential": "r2"}]),
        json!([]),
    );
    assert!(
        errors(tmp.path(), &r)
            .iter()
            .any(|e| e.contains("relay_accounts[1]") && e.contains("credential"))
    );
}

fn ids(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

#[test]
fn test_apply_account_ids_fills_ids_and_keeps_explicit_ones() {
    let tmp = tempdir().unwrap();
    let mut r = with_pools(
        json!([{"credential": "r2"}, {"user_id": "6", "credential": "r3"}]),
        json!([{"credential": "m1"}]),
    );
    del(&mut r, "discord", "expected_relay_user_id");
    let cfg = apply_account_ids(
        &load_ok(tmp.path(), &r),
        &ids(&[("relay-token", 11), ("r2", 12), ("m1", 13)]),
    )
    .unwrap();
    let d = &cfg.discord;
    assert_eq!(d.expected_relay_user_id, 11);
    let got: Vec<_> = d
        .output_accounts
        .iter()
        .map(|a| (a.user_id, a.credential.as_str(), a.role.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            (12, "r2", "relay"),
            (6, "r3", "relay"),
            (13, "m1", "mirror")
        ]
    );
    assert!(unresolved_credentials(&cfg).is_empty());
}

#[test]
fn test_apply_account_ids_revalidates_resolved_ids() {
    let tmp = tempdir().unwrap();
    let mut r = with_pools(
        json!([{"credential": "r2"}]),
        json!([{"user_id": "6", "credential": "m1"}]),
    );
    del(&mut r, "discord", "expected_relay_user_id");
    let cfg = load_ok(tmp.path(), &r);
    let target = cfg.discord.target_user_id;
    let e = apply_account_ids(&cfg, &ids(&[("relay-token", 11), ("r2", 11)])).unwrap_err();
    assert!(
        e.errors
            .iter()
            .any(|e| e.contains("r2") && e.contains("relay-token"))
    );
    let e = apply_account_ids(&cfg, &ids(&[("relay-token", 11), ("r2", 6)])).unwrap_err();
    assert!(
        e.errors
            .iter()
            .any(|e| e.contains("r2") && e.contains("m1"))
    );
    let e = apply_account_ids(&cfg, &ids(&[("relay-token", target), ("r2", 12)])).unwrap_err();
    assert!(
        e.errors
            .iter()
            .any(|e| e.contains("relay-token") && e.contains("target_user_id"))
    );
}

#[test]
fn test_check_operable_ok_while_ingest_id_unresolved() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    del(&mut r, "discord", "expected_relay_user_id");
    r["liveness"]["source_dead_timeout_seconds"] = json!(5);
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(check_operable(&cfg, &manifests()), Vec::<String>::new());
}

#[test]
fn test_config_output_codec_adaptive_default() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.output_codec,
        OutputCodec::Adaptive
    );
    assert_eq!(
        load_ok(tmp.path(), &r).output_profile().codec_mode,
        CodecMode::Adaptive
    );
    del(&mut r, "codecs", "output_codec");
    assert_eq!(
        load_ok(tmp.path(), &r).codecs.output_codec,
        OutputCodec::Adaptive
    );
    r["codecs"]["output_codec"] = json!("H264");
    let cfg = load_ok(tmp.path(), &r);
    assert!(
        cfg.codecs.output_codec == OutputCodec::H264
            && cfg.output_profile().codec_mode == CodecMode::H264
    );
    assert_eq!(cfg.codecs.output_codec.as_str(), "H264");
    assert_eq!(cfg.output_profile().codec_mode.as_str(), "h264");
    r["codecs"]["output_codec"] = json!("H265");
    assert!(any_err(&errors(tmp.path(), &r), "codecs.output_codec"));
}

#[test]
fn test_video_bitrate_auto_or_number() {
    // The Python test also checks media.hwaccel.auto_bitrate_kbps (dropped: encoder-only).
    let tmp = tempdir().unwrap();
    let example_text = std::fs::read_to_string(example_path()).unwrap();
    for (value, want) in [("auto", 0), ("8000", 8000)] {
        let text = example_text.replace(
            "video_bitrate_kbps: auto",
            &format!("video_bitrate_kbps: {value}"),
        );
        let p = write_text(tmp.path(), &text);
        assert_eq!(load_config(&p).unwrap().quality.video_bitrate_kbps, want);
    }
}

// ---- additions for the Rust port (passthrough-only build, YAML 1.2, malformed input) ----

#[test]
fn test_check_operable_rejects_transcode_modes() {
    let tmp = tempdir().unwrap();
    for (key, name) in [
        ("video_mode", "codecs.video_mode"),
        ("audio_mode", "codecs.audio_mode"),
    ] {
        let mut r = raw();
        r["liveness"]["source_dead_timeout_seconds"] = json!(5);
        r["codecs"][key] = json!("transcode");
        let cfg = load_ok(tmp.path(), &r);
        let cap = manifests();
        let errs = check_operable(&cfg, &cap);
        assert!(
            errs.iter()
                .any(|e| e.contains(name) && e.contains("transcode not supported in this build")),
            "{errs:?}"
        );
        // reported even while the ingest ID is unresolved
        del(&mut r, "discord", "expected_relay_user_id");
        let cfg = load_ok(tmp.path(), &r);
        assert!(any_err(
            &check_operable(&cfg, &cap),
            "transcode not supported in this build"
        ));
    }
}

#[test]
fn test_ignored_settings_and_h264_policy_notes() {
    let tmp = tempdir().unwrap();
    assert!(ignored_setting_notes(&example()).is_empty());
    let mut r = raw();
    r["codecs"]["video_encoder"] = json!("h264_nvenc");
    r["codecs"]["hw_decode"] = json!("cuda");
    r["quality"]["audio_bitrate_kbps"] = json!(64);
    let notes = ignored_setting_notes(&load_ok(tmp.path(), &r));
    assert_eq!(notes.len(), 3, "{notes:?}");
    assert!(notes.iter().all(|n| n.starts_with("note: ")));
    assert!(
        any_err(&notes, "video_encoder")
            && any_err(&notes, "hw_decode")
            && any_err(&notes, "audio_bitrate_kbps")
    );

    let mut r = raw();
    r["codecs"]["output_codec"] = json!("H264");
    r["codecs"]["parent_codecs"] = json!("auto_decode");
    r["codecs"]["watch_codecs"] = json!("auto_decode");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(cfg.codecs.parent_codecs, CodecPolicy::AutoDecode); // the file value is kept
    assert_eq!(cfg.codecs.effective_parent_codecs(), CodecPolicy::H264Only);
    assert_eq!(cfg.codecs.effective_watch_codecs(), CodecPolicy::H264Only);
    let notes = ignored_setting_notes(&cfg);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert!(notes[0].starts_with("note: ") && notes[0].contains("h264_only"));
    // adaptive ignores both lists: the configured values stay effective, no note
    r["codecs"]["output_codec"] = json!("adaptive");
    let cfg = load_ok(tmp.path(), &r);
    assert_eq!(
        cfg.codecs.effective_parent_codecs(),
        CodecPolicy::AutoDecode
    );
    assert!(ignored_setting_notes(&cfg).is_empty());
}

#[test]
fn test_yaml12_yes_no_on_off_are_not_booleans() {
    let tmp = tempdir().unwrap();
    let text = std::fs::read_to_string(example_path()).unwrap();
    for (key, current, bad) in [
        ("ingest_publishes", "true", "yes"),
        ("network_stats", "false", "on"),
        ("prejoin_voice", "true", "no"),
        ("self_deaf_ingest", "true", "off"),
    ] {
        let edited = text.replace(&format!("{key}: {current}"), &format!("{key}: {bad}"));
        assert_ne!(edited, text, "{key} not found in the example");
        let errs = load_config(&write_text(tmp.path(), &edited))
            .unwrap_err()
            .errors;
        assert!(
            errs.iter()
                .any(|e| e.ends_with(&format!("{key}: must be a boolean"))),
            "{key}: {errs:?}"
        );
    }
}

#[test]
fn test_malformed_yaml_returns_errors_without_panicking() {
    let tmp = tempdir().unwrap();
    let bad_texts = [
        "",
        "42",
        "- a\n- b\n",
        "~",
        "{",
        "a: [1, 2",
        "a: 1\n---\nb: 2\n",
        "a: 1\na: 2\n",
        "\u{0}\u{1}",
    ];
    for text in bad_texts {
        let e = load_config(&write_text(tmp.path(), text)).expect_err(text);
        assert!(!e.errors.is_empty());
    }
    let e = load_config(&write_text(tmp.path(), "- a\n")).unwrap_err();
    assert_eq!(e.errors, vec!["config: top level must be a mapping"]);

    // wrong types everywhere: collected, never a panic
    let mut r = raw();
    r["discord"]["guild_id"] = json!(100000000000000002_u64); // number, not a string
    r["discord"]["target_user_id"] = Value::Null;
    r["discord"]["allowed_voice_channel_id"] = json!(["1"]);
    r["authorization"]["excluded_viewer_ids"] = json!("x");
    r["codecs"]["output_rids"] = json!("1");
    r["quality"]["max_width"] = json!(2560.5);
    r["quality"]["max_height"] = json!(true);
    r["quality"]["fixed_fps"] = json!(1.0);
    r["quality"]["video_bitrate_kbps"] = json!([1]);
    r["audio"]["enabled"] = Value::Null;
    r["runtime"]["idle_voice_grace_seconds"] = json!("5");
    r["runtime"]["connect_timeout_seconds"] = Value::Null;
    r["liveness"]["source_dead_timeout_seconds"] = json!("5");
    r["storage"] = json!(7);
    r["logging"]["level"] = json!("TRACE");
    r["control"] = json!([]);
    let errs = errors(tmp.path(), &r);
    for needle in [
        "discord.guild_id: must be a string of decimal digits",
        "discord.target_user_id: must be a string of decimal digits",
        "discord.allowed_voice_channel_id: must be a string of decimal digits",
        "authorization.excluded_viewer_ids: must be a list of ID strings",
        "codecs.output_rids: must be an integer",
        "quality.max_width: must be an integer",
        "quality.max_height: must be an integer",
        "quality.fixed_fps: must be an integer",
        "quality.video_bitrate_kbps: must be an integer",
        "audio.enabled: must be a boolean",
        "runtime.idle_voice_grace_seconds: must be a positive number",
        "runtime.connect_timeout_seconds: must be a number",
        "liveness.source_dead_timeout_seconds: must be a positive number",
        "storage: must be a mapping",
        "logging.level: must be one of ['CRITICAL', 'DEBUG', 'ERROR', 'INFO', 'WARNING']",
        "control: must be a mapping",
    ] {
        assert!(
            errs.iter().any(|e| e == needle),
            "missing {needle:?} in {errs:#?}"
        );
    }

    // missing keys and sections
    let e = errors(tmp.path(), &json!({"schema_version": 1}));
    assert!(e.contains(&"config.discord: missing".to_string()));
    assert!(e.contains(&"discord: must be a mapping".to_string()));
    let e = errors(tmp.path(), &json!({"schema_version": 2}));
    assert!(e.contains(&"config.schema_version: unsupported (expected 1)".to_string()));
}

#[test]
fn test_bad_id_item_message_uses_python_repr() {
    let tmp = tempdir().unwrap();
    let mut r = raw();
    r["authorization"]["excluded_viewer_ids"] = json!(["12", 5, "abc", "0", null, true]);
    let errs = errors(tmp.path(), &r);
    for item in ["5", "'abc'", "'0'", "None", "True"] {
        let want = format!("authorization.excluded_viewer_ids: bad id {item}");
        assert!(errs.contains(&want), "{want} not in {errs:?}");
    }
}

fn example_with_python_style_accounts(dir: &Path, accounts: &str) -> PathBuf {
    let text = std::fs::read_to_string(example_path())
        .unwrap()
        .replace("\r\n", "\n");
    let mut out = String::new();
    for line in text.lines() {
        if line.starts_with("  relay_accounts:") {
            out.push_str(accounts);
        } else if !line.starts_with("  mirror_accounts:") {
            out.push_str(line);
            out.push('\n');
        }
    }
    write_text(dir, &out)
}

#[test]
fn python_style_flow_closers_at_column_zero_load() {
    let tmp = tempdir().unwrap();
    let p = example_with_python_style_accounts(
        tmp.path(),
        "  relay_accounts: [\n    {credential: relay1},\n    {credential: relay2}\n]               # comment\n  mirror_accounts: [\n    {credential: mirror1}\n]\n",
    );
    let cfg = load_config(&p).unwrap_or_else(|e| panic!("{:?}", e.errors));
    assert_eq!(cfg.discord.relay_accounts().len(), 2);
    assert_eq!(cfg.discord.mirror_accounts().len(), 1);
}

#[test]
fn unfixable_indentation_error_gets_a_hint_with_the_line() {
    let tmp = tempdir().unwrap();
    // The bad line is not a lone closer, so the pre-pass leaves it and the parser rejects it.
    let p = example_with_python_style_accounts(
        tmp.path(),
        "  relay_accounts: [\n    {credential: relay1},\n]  x\n",
    );
    let e = load_config(&p).unwrap_err();
    let msg = e.errors.join("\n");
    assert!(msg.contains("hint: line"), "{msg}");
}
