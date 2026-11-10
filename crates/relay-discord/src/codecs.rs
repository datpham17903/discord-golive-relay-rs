//! Voice codec tables for the adapters. Port of `adapter/codecs.py` (payload types live in
//! `relay_media::codec::VoiceCodec`; the lists below only choose which codecs are offered and how).

use relay_media::codec::{UnknownCodec, VoiceCodec};
use relay_voice::connection::RtcSettings;
use relay_voice::voice_ws::{CodecSpec, video_codec_list};

/// Every video codec in `CODEC_ORDER` order: what adaptive mode offers for decode and encode.
pub const ALL_VIDEO_CODECS: [VoiceCodec; 5] = [
    VoiceCodec::AV1,
    VoiceCodec::H265,
    VoiceCodec::H264,
    VoiceCodec::VP8,
    VoiceCodec::VP9,
];

/// Names from the codec controller (`CodecAction`) as codecs; a name outside the table is an error
/// (Python raised `KeyError`).
pub fn parse_names(names: &[&str]) -> Result<Vec<VoiceCodec>, UnknownCodec> {
    names.iter().map(|name| name.parse()).collect()
}

/// `adaptive_decode_codecs` / `video_codec_list(.., encode=False)` of every codec: Opus plus the
/// video codecs in order, decode only.
pub fn adaptive_decode_codecs() -> Vec<CodecSpec> {
    video_codec_list(&ALL_VIDEO_CODECS, false)
}

/// `parent_codecs` / `watch_codecs`: Opus plus `codecs` for decode; only H264 is also marked for
/// encode. Priorities follow the position (the library recomputes them from the order).
pub fn decode_codecs(codecs: &[VoiceCodec]) -> Vec<CodecSpec> {
    let mut out = vec![CodecSpec::opus()];
    out.extend(codecs.iter().enumerate().map(|(i, &codec)| {
        CodecSpec::video(
            codec,
            1000 * (i as u32 + 1),
            codec == VoiceCodec::H264,
            true,
        )
    }));
    out
}

/// `output_codecs`: Opus and H264 only.
pub fn h264_output_codecs() -> Vec<CodecSpec> {
    vec![
        CodecSpec::opus(),
        CodecSpec::video(VoiceCodec::H264, 1000, true, true),
    ]
}

/// Settings of the output stream connection: H264 + Opus (`output_stream_client`), or in adaptive
/// mode every codec for encode, narrowed later through SESSION_UPDATE
/// (`adaptive_output_stream_client`); one `screen` stream capped at `fps`.
pub fn output_settings(
    adaptive: bool,
    fps: u32,
    debug_stats: bool,
    user_agent: &str,
) -> RtcSettings {
    let codecs = if adaptive {
        video_codec_list(&ALL_VIDEO_CODECS, true)
    } else {
        h264_output_codecs()
    };
    let mut settings = RtcSettings::publish(codecs, true, fps);
    settings.user_agent = user_agent.to_owned();
    settings.receive.debug_stats = debug_stats;
    settings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[CodecSpec]) -> Vec<&str> {
        list.iter().map(|c| c.name).collect()
    }

    #[test]
    fn names_parse_in_order_and_unknown_names_are_errors() {
        assert_eq!(
            parse_names(&["H265", "H264"]).unwrap(),
            [VoiceCodec::H265, VoiceCodec::H264]
        );
        assert!(parse_names(&["H265", "nope"]).is_err());
        assert!(parse_names(&[]).unwrap().is_empty());
    }

    #[test]
    fn decode_lists_are_opus_plus_the_given_codecs_for_decode() {
        let list = decode_codecs(&[VoiceCodec::H265, VoiceCodec::H264]);
        assert_eq!(names(&list), ["opus", "H265", "H264"]);
        assert_eq!(
            list[1..].iter().map(|c| c.priority).collect::<Vec<_>>(),
            [1000, 2000]
        );
        assert!(list[1..].iter().all(|c| c.decode));
        assert!(
            !list[1].encode && list[2].encode,
            "only H264 may be encoded"
        );
    }

    #[test]
    fn adaptive_decode_offers_every_codec_decode_only() {
        let list = adaptive_decode_codecs();
        assert_eq!(names(&list), ["opus", "AV1", "H265", "H264", "VP8", "VP9"]);
        assert!(list[1..].iter().all(|c| c.decode && !c.encode));
    }

    #[test]
    fn the_h264_output_offers_only_h264_and_opus() {
        let settings = output_settings(false, 30, false, "UA");
        assert_eq!(names(&settings.receive.codecs), ["opus", "H264"]);
        assert!(settings.receive.codecs[1].encode);
        assert_eq!(settings.user_agent, "UA");
        assert!(!settings.receive.debug_stats);
    }

    #[test]
    fn the_adaptive_output_offers_every_codec_for_encode() {
        let settings = output_settings(true, 60, true, "");
        assert_eq!(
            names(&settings.receive.codecs),
            ["opus", "AV1", "H265", "H264", "VP8", "VP9"]
        );
        assert!(settings.receive.codecs[1..].iter().all(|c| c.encode));
        assert!(
            settings.receive.debug_stats,
            "enable_debug_stats follows network_stats"
        );
        assert_eq!(settings.streams.len(), 1);
        assert_eq!(settings.streams[0].max_framerate, Some(60));
    }
}
