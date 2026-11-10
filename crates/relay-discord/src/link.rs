//! Glue between the main gateway and the voice clients (`relay-voice`): the `GatewayLink` the
//! clients send through, and the translation of gateway dispatches into the `LinkEvent`s they
//! consume (contract in `relay_voice::link`). `relay-voice` does not depend on this crate; this is
//! where the two meet.

use crate::{GatewayHandle, StreamKey, StreamType, VoiceStateEntry, VoiceStateUpdate};
use relay_voice::link::{
    GatewayLink, LinkError, LinkEvent, LinkFuture, Secret, StreamCreateRequest, StreamDelete,
    VoiceStateRequest,
};
use serde_json::Value;

pub struct GatewayLinkImpl {
    pub gateway: GatewayHandle,
}

fn link_err(e: impl std::fmt::Display) -> LinkError {
    LinkError(e.to_string())
}

fn parse_key(key: &str) -> Result<StreamKey, LinkError> {
    key.parse().map_err(link_err)
}

impl GatewayLink for GatewayLinkImpl {
    fn voice_state(&self, request: VoiceStateRequest) -> LinkFuture<'_> {
        Box::pin(async move {
            self.gateway
                .voice_state(VoiceStateUpdate {
                    guild_id: request.guild_id,
                    channel_id: request.channel_id,
                    self_mute: request.self_mute,
                    self_deaf: request.self_deaf,
                    self_video: request.self_video,
                    self_stream: None,
                })
                .await
                .map_err(link_err)
        })
    }

    fn stream_create(&self, request: StreamCreateRequest) -> LinkFuture<'_> {
        Box::pin(async move {
            let stream_type = if request.guild_id.is_some() {
                StreamType::Guild
            } else {
                StreamType::Call
            };
            self.gateway
                .stream_create(stream_type, request.guild_id, request.channel_id)
                .await
                .map_err(link_err)
        })
    }

    fn stream_watch(&self, stream_key: &str) -> LinkFuture<'_> {
        let key = parse_key(stream_key);
        Box::pin(async move { self.gateway.stream_watch(&key?).await.map_err(link_err) })
    }

    fn stream_delete(&self, stream_key: &str) -> LinkFuture<'_> {
        let key = parse_key(stream_key);
        Box::pin(async move { self.gateway.stream_delete(&key?).await.map_err(link_err) })
    }

    fn stream_set_paused(&self, stream_key: &str, paused: bool) -> LinkFuture<'_> {
        let key = parse_key(stream_key);
        Box::pin(async move {
            self.gateway
                .stream_set_paused(&key?, paused)
                .await
                .map_err(link_err)
        })
    }
}

/// A snowflake that arrives as a decimal string (or, rarely, a number).
pub fn snowflake(value: &Value) -> Option<u64> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn id(data: &Value, field: &str) -> Option<u64> {
    data.get(field).and_then(snowflake)
}

fn text(data: &Value, field: &str) -> Option<String> {
    data.get(field).and_then(Value::as_str).map(str::to_owned)
}

/// The dispatches the voice clients consume: our own VOICE_STATE_UPDATE, VOICE_SERVER_UPDATE and
/// every STREAM_* event. Anything else, or a malformed payload, is `None`.
pub fn link_event(name: &str, data: &Value, self_id: u64) -> Option<LinkEvent> {
    match name {
        "VOICE_STATE_UPDATE" if id(data, "user_id") == Some(self_id) => {
            Some(LinkEvent::VoiceStateUpdate {
                channel_id: id(data, "channel_id"),
                session_id: text(data, "session_id"),
            })
        }
        "VOICE_SERVER_UPDATE" => Some(LinkEvent::VoiceServerUpdate {
            token: text(data, "token").map(Secret::new),
            endpoint: text(data, "endpoint"),
            guild_id: id(data, "guild_id"),
            channel_id: id(data, "channel_id"),
        }),
        "STREAM_CREATE" => Some(LinkEvent::StreamCreate {
            stream_key: text(data, "stream_key")?,
            rtc_server_id: id(data, "rtc_server_id")?,
            rtc_channel_id: id(data, "rtc_channel_id")?,
        }),
        "STREAM_SERVER_UPDATE" => Some(LinkEvent::StreamServerUpdate {
            stream_key: text(data, "stream_key")?,
            token: text(data, "token").map(Secret::new),
            endpoint: text(data, "endpoint"),
        }),
        "STREAM_UPDATE" => Some(LinkEvent::StreamUpdate {
            stream_key: text(data, "stream_key")?,
            paused: data.get("paused").and_then(Value::as_bool),
        }),
        "STREAM_DELETE" => Some(LinkEvent::StreamDelete(StreamDelete {
            stream_key: text(data, "stream_key")?,
            reason: text(data, "reason").unwrap_or_default(),
            unavailable: data
                .get("unavailable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })),
        _ => None,
    }
}

/// The target is live in `channel_id` when its voice state there says `self_stream`, or the stream
/// itself is already known (STREAM_CREATE seen).
pub fn target_is_live(
    voice_state: Option<&VoiceStateEntry>,
    stream_known: bool,
    channel_id: u64,
) -> bool {
    stream_known
        || voice_state.is_some_and(|vs| vs.channel_id == Some(channel_id) && vs.self_stream)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const ME: u64 = 100;

    fn vs(channel: Option<u64>, self_stream: bool) -> VoiceStateEntry {
        VoiceStateEntry {
            channel_id: channel,
            self_stream,
            self_video: false,
        }
    }

    #[test]
    fn own_voice_state_is_forwarded_with_session_id() {
        let data =
            json!({"user_id": "100", "channel_id": "7", "session_id": "sess", "guild_id": "1"});
        assert_eq!(
            link_event("VOICE_STATE_UPDATE", &data, ME),
            Some(LinkEvent::VoiceStateUpdate {
                channel_id: Some(7),
                session_id: Some("sess".into())
            })
        );
        let left = json!({"user_id": "100", "channel_id": null, "session_id": "sess"});
        assert_eq!(
            link_event("VOICE_STATE_UPDATE", &left, ME),
            Some(LinkEvent::VoiceStateUpdate {
                channel_id: None,
                session_id: Some("sess".into())
            })
        );
    }

    #[test]
    fn other_users_voice_state_is_not_forwarded() {
        let data = json!({"user_id": "101", "channel_id": "7", "session_id": "x"});
        assert_eq!(link_event("VOICE_STATE_UPDATE", &data, ME), None);
    }

    #[test]
    fn voice_server_update_carries_endpoint_and_secret_token() {
        let data = json!({"token": "tok", "endpoint": "x.discord.media:443", "guild_id": "1"});
        let Some(LinkEvent::VoiceServerUpdate {
            token,
            endpoint,
            guild_id,
            channel_id,
        }) = link_event("VOICE_SERVER_UPDATE", &data, ME)
        else {
            panic!("no event");
        };
        assert_eq!(token.unwrap().expose(), "tok");
        assert_eq!(endpoint.as_deref(), Some("x.discord.media:443"));
        assert_eq!((guild_id, channel_id), (Some(1), None));
    }

    #[test]
    fn voice_server_update_without_endpoint_keeps_the_fields_optional() {
        let data = json!({"token": "tok", "endpoint": null, "guild_id": "1"});
        let Some(LinkEvent::VoiceServerUpdate { endpoint, .. }) =
            link_event("VOICE_SERVER_UPDATE", &data, ME)
        else {
            panic!("no event");
        };
        assert_eq!(endpoint, None);
    }

    #[test]
    fn stream_events_are_translated() {
        let key = "guild:1:7:55";
        assert_eq!(
            link_event(
                "STREAM_CREATE",
                &json!({"stream_key": key, "rtc_server_id": "900", "rtc_channel_id": "901"}),
                ME
            ),
            Some(LinkEvent::StreamCreate {
                stream_key: key.into(),
                rtc_server_id: 900,
                rtc_channel_id: 901
            })
        );
        let Some(LinkEvent::StreamServerUpdate {
            stream_key,
            token,
            endpoint,
        }) = link_event(
            "STREAM_SERVER_UPDATE",
            &json!({"stream_key": key, "token": "t", "endpoint": "e:1"}),
            ME,
        )
        else {
            panic!("no event");
        };
        assert_eq!(stream_key, key);
        assert_eq!(token.unwrap().expose(), "t");
        assert_eq!(endpoint.as_deref(), Some("e:1"));
        assert_eq!(
            link_event(
                "STREAM_UPDATE",
                &json!({"stream_key": key, "paused": true}),
                ME
            ),
            Some(LinkEvent::StreamUpdate {
                stream_key: key.into(),
                paused: Some(true)
            })
        );
        assert_eq!(
            link_event(
                "STREAM_DELETE",
                &json!({"stream_key": key, "reason": "user_requested", "unavailable": false}),
                ME
            ),
            Some(LinkEvent::StreamDelete(StreamDelete {
                stream_key: key.into(),
                reason: "user_requested".into(),
                unavailable: false
            }))
        );
    }

    #[test]
    fn malformed_and_unrelated_dispatches_are_ignored() {
        assert_eq!(
            link_event("STREAM_CREATE", &json!({"stream_key": "k"}), ME),
            None
        );
        assert_eq!(link_event("STREAM_DELETE", &json!({}), ME), None);
        assert_eq!(link_event("MESSAGE_CREATE", &json!({}), ME), None);
        assert_eq!(link_event("READY", &json!({}), ME), None);
    }

    #[test]
    fn link_events_never_print_the_server_token() {
        let data = json!({"token": "super-secret", "endpoint": "e", "guild_id": "1"});
        let event = link_event("VOICE_SERVER_UPDATE", &data, ME).unwrap();
        assert!(!format!("{event:?}").contains("super-secret"));
    }

    #[test]
    fn target_liveness() {
        assert!(target_is_live(Some(&vs(Some(7), true)), false, 7));
        assert!(!target_is_live(Some(&vs(Some(7), false)), false, 7));
        assert!(
            !target_is_live(Some(&vs(Some(8), true)), false, 7),
            "other channel"
        );
        assert!(
            !target_is_live(Some(&vs(None, true)), false, 7),
            "left, stale flag"
        );
        assert!(!target_is_live(None, false, 7));
        assert!(target_is_live(None, true, 7), "stream already known");
    }

    #[test]
    fn snowflakes_parse_from_strings_and_numbers() {
        assert_eq!(snowflake(&json!("123")), Some(123));
        assert_eq!(snowflake(&json!(123)), Some(123));
        assert_eq!(snowflake(&json!("x")), None);
        assert_eq!(snowflake(&json!(null)), None);
    }
}
