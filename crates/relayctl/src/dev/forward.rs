//! Pure parts of `dev relay`: the report and the final summary. What the Python adapter and service do
//! between the watch connection and the worker (packet to ingress message, sequence gap detection, PLI
//! rate limit) lives in `relay_discord::ingress` and `relay_core::ingress`.

use serde_json::{Value, json};

/// The relay must reach this frame rate at the viewer for the probe to pass.
pub const MIN_VIEWER_FPS: f64 = 20.0;

/// The output can only pass the source through when both use the same codec.
pub fn passthrough_mismatch(negotiated: Option<&str>, source: Option<&str>) -> Option<String> {
    match (negotiated, source) {
        (Some(n), Some(s)) if n != s => Some(format!(
            "passthrough impossible: the output negotiated {n} but the source is {s}"
        )),
        _ => None,
    }
}

/// What the relay session reports.
#[derive(Debug, Default)]
pub struct RelayReport {
    pub watch_frames: u64,
    pub published_video_frames: u64,
    pub published_audio_frames: u64,
    pub negotiated_codec: Option<String>,
    pub source_codec: Option<String>,
    pub pli_from_viewers: u64,
    pub errors: Vec<String>,
}

impl RelayReport {
    pub fn to_json(&self) -> Value {
        json!({
            "watch_frames": self.watch_frames,
            "published_video_frames": self.published_video_frames,
            "published_audio_frames": self.published_audio_frames,
            "negotiated_codec": self.negotiated_codec,
            "source_codec": self.source_codec,
            "pli_from_viewers": self.pli_from_viewers,
            "errors": self.errors,
        })
    }
}

/// The viewer half of the summary, picked from a `stats::summary_json` object.
pub fn viewer_json(summary: &Value) -> Value {
    let field = |name: &str| summary.get(name).cloned().unwrap_or(Value::Null);
    json!({
        "stream_found": field("stream_key_found"),
        "watch_connected": field("watch_connected"),
        "dave_protocol_version": field("dave_protocol_version"),
        "frames_assembled": field("frames_assembled"),
        "keyframes": field("keyframes"),
        "first_frame": field("first_frame"),
        "fps_avg": field("fps_avg"),
        "audio_packets": field("audio_packets"),
        "decrypt_failures": field("decrypt_failures"),
        "errors": field("errors"),
    })
}

/// The single object printed on stdout.
pub fn combined_summary(relay: &RelayReport, viewer: Value) -> Value {
    json!({"relay": relay.to_json(), "viewer": viewer})
}

/// 0 only when the viewer assembled frames at a usable rate.
pub fn exit_code(viewer: &Value) -> u8 {
    let frames = viewer["frames_assembled"].as_u64().unwrap_or(0);
    let fps = viewer["fps_avg"].as_f64().unwrap_or(0.0);
    u8::from(!(frames > 0 && fps >= MIN_VIEWER_FPS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_codec_mismatch_is_reported_only_when_both_are_known_and_differ() {
        assert_eq!(passthrough_mismatch(Some("H264"), Some("H264")), None);
        assert_eq!(passthrough_mismatch(None, Some("H264")), None);
        assert_eq!(passthrough_mismatch(Some("H264"), None), None);
        let e = passthrough_mismatch(Some("H264"), Some("AV1")).unwrap();
        assert!(e.contains("H264") && e.contains("AV1"), "{e}");
    }

    fn viewer(frames: u64, fps: f64) -> Value {
        let full = json!({
            "login_user_id": 1, "ready": true, "voice_joined": true,
            "stream_key_found": true, "watch_connected": true, "dave_protocol_version": 1,
            "video_packets": 99, "frames_assembled": frames, "keyframes": 2,
            "first_frame": {"codec": "H264", "width": 1280, "height": 720},
            "fps_avg": fps, "audio_packets": 500, "decrypt_failures": 0, "errors": [],
        });
        viewer_json(&full)
    }

    #[test]
    fn viewer_summary_has_exactly_the_documented_fields() {
        let v = viewer(300, 29.97);
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "stream_found",
                "watch_connected",
                "dave_protocol_version",
                "frames_assembled",
                "keyframes",
                "first_frame",
                "fps_avg",
                "audio_packets",
                "decrypt_failures",
                "errors"
            ]
        );
        assert_eq!(v["stream_found"], true);
        assert_eq!(v["first_frame"]["width"], 1280);
    }

    #[test]
    fn combined_summary_has_the_relay_fields_in_order() {
        let report = RelayReport {
            watch_frames: 10,
            published_video_frames: 8,
            published_audio_frames: 400,
            negotiated_codec: Some("H264".into()),
            source_codec: Some("H264".into()),
            pli_from_viewers: 2,
            errors: vec!["x".into()],
        };
        let summary = combined_summary(&report, viewer(1, 30.0));
        let keys: Vec<&str> = summary["relay"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "watch_frames",
                "published_video_frames",
                "published_audio_frames",
                "negotiated_codec",
                "source_codec",
                "pli_from_viewers",
                "errors"
            ]
        );
        assert_eq!(summary["relay"]["errors"][0], "x");
        assert_eq!(summary["viewer"]["frames_assembled"], 1);
        assert_eq!(summary.as_object().unwrap().len(), 2);
    }

    #[test]
    fn an_empty_relay_report_uses_nulls() {
        let j = RelayReport::default().to_json();
        assert!(j["negotiated_codec"].is_null() && j["source_codec"].is_null());
        assert_eq!(j["errors"], json!([]));
    }

    #[test]
    fn exit_code_needs_frames_and_twenty_fps() {
        assert_eq!(exit_code(&viewer(300, 29.9)), 0);
        assert_eq!(exit_code(&viewer(300, 20.0)), 0);
        assert_eq!(exit_code(&viewer(300, 19.99)), 1);
        assert_eq!(exit_code(&viewer(0, 30.0)), 1);
        assert_eq!(exit_code(&viewer_json(&json!({}))), 1);
    }
}
