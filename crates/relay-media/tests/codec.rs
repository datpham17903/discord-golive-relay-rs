//! `VoiceCodec` enum and the `codec` module's single public path for the ranking helpers.

use relay_media::codec::{
    CODEC_ORDER, OPUS_PAYLOAD_TYPE, UnknownCodec, VoiceCodec, better, down_from, lowest, rank,
};

const ALL: [VoiceCodec; 5] = [
    VoiceCodec::AV1,
    VoiceCodec::H265,
    VoiceCodec::H264,
    VoiceCodec::VP8,
    VoiceCodec::VP9,
];

#[test]
fn as_str_matches_codec_order() {
    let names: Vec<&str> = ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(names, CODEC_ORDER);
    assert_eq!(names, ["AV1", "H265", "H264", "VP8", "VP9"]);
}

#[test]
fn from_str_round_trips_and_rejects_unknown() {
    for c in ALL {
        assert_eq!(c.as_str().parse::<VoiceCodec>(), Ok(c));
    }
    assert_eq!(
        "h264".parse::<VoiceCodec>(),
        Err(UnknownCodec("h264".to_owned()))
    );
    assert_eq!("".parse::<VoiceCodec>(), Err(UnknownCodec(String::new())));
}

#[test]
fn payload_types_match_native_voice_codec_table() {
    let pts: Vec<(u8, u8)> = ALL
        .iter()
        .map(|c| (c.payload_type(), c.rtx_payload_type()))
        .collect();
    assert_eq!(
        pts,
        [(101, 102), (103, 104), (105, 106), (107, 108), (109, 110)]
    );
    assert_eq!(OPUS_PAYLOAD_TYPE, 120);
}

#[test]
fn str_helpers_remain_available_from_codec_module() {
    assert_eq!(rank("H264"), Ok(2));
    assert_eq!(better("VP9", "AV1"), Ok("AV1"));
    assert_eq!(lowest(&["AV1", "VP8"]), Ok(Some("VP8")));
    assert_eq!(down_from("VP8").unwrap(), ["VP8", "VP9"]);
}
