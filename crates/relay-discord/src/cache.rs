//! Minimal gateway-state cache: per-guild voice states and Go Live streams.
//!
//! Mirrors what `discord/state.py` keeps for the relay: `parse_ready` +
//! `parse_ready_supplemental` (guild voice states), `parse_guild_create/delete`,
//! `parse_voice_state_update`, `parse_passive_update_v2` and `parse_stream_create/update/delete`.
//! The gateway task applies every dispatch to the shared cache *before* delivering the event, so a
//! consumer reading the cache on an event sees the post-event state; [`CacheChange`] carries the
//! pre-event state.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;

use crate::error::Error;

pub type SharedCache = Arc<RwLock<Cache>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamType {
    Guild,
    Call,
    Test,
}

impl StreamType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guild => "guild",
            Self::Call => "call",
            Self::Test => "test",
        }
    }
}

/// `discord.stream.StreamKey`: `guild:<guild>:<channel>:<owner>`, `call:<channel>:<owner>`,
/// `test:<owner>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKey {
    Guild {
        guild_id: u64,
        channel_id: u64,
        owner_id: u64,
    },
    Call {
        channel_id: u64,
        owner_id: u64,
    },
    Test {
        owner_id: u64,
    },
}

impl StreamKey {
    pub fn stream_type(&self) -> StreamType {
        match self {
            Self::Guild { .. } => StreamType::Guild,
            Self::Call { .. } => StreamType::Call,
            Self::Test { .. } => StreamType::Test,
        }
    }

    pub fn guild_id(&self) -> Option<u64> {
        match self {
            Self::Guild { guild_id, .. } => Some(*guild_id),
            _ => None,
        }
    }

    pub fn channel_id(&self) -> Option<u64> {
        match self {
            Self::Guild { channel_id, .. } | Self::Call { channel_id, .. } => Some(*channel_id),
            Self::Test { .. } => None,
        }
    }

    pub fn owner_id(&self) -> u64 {
        match self {
            Self::Guild { owner_id, .. }
            | Self::Call { owner_id, .. }
            | Self::Test { owner_id } => *owner_id,
        }
    }
}

impl fmt::Display for StreamKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Guild {
                guild_id,
                channel_id,
                owner_id,
            } => write!(f, "guild:{guild_id}:{channel_id}:{owner_id}"),
            Self::Call {
                channel_id,
                owner_id,
            } => write!(f, "call:{channel_id}:{owner_id}"),
            Self::Test { owner_id } => write!(f, "test:{owner_id}"),
        }
    }
}

impl FromStr for StreamKey {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Error> {
        let bad = || Error::InvalidData(format!("invalid stream key: {value:?}"));
        let parts: Vec<&str> = value.split(':').collect();
        let num = |s: &str| s.parse::<u64>().map_err(|_| bad());
        match (parts.first().copied(), parts.len()) {
            (Some("guild"), 4) => Ok(Self::Guild {
                guild_id: num(parts[1])?,
                channel_id: num(parts[2])?,
                owner_id: num(parts[3])?,
            }),
            (Some("call"), 3) => Ok(Self::Call {
                channel_id: num(parts[1])?,
                owner_id: num(parts[2])?,
            }),
            (Some("test"), 2) => Ok(Self::Test {
                owner_id: num(parts[1])?,
            }),
            _ => Err(bad()),
        }
    }
}

/// A user currently in a voice channel. `channel_id` is `None` only in [`CacheChange::Voice::after`]
/// (the user left; `self_stream` may still be stale-true, as the Python adapter notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceStateEntry {
    pub channel_id: Option<u64>,
    pub self_stream: bool,
    pub self_video: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamState {
    pub key: StreamKey,
    pub rtc_server_id: Option<u64>,
    pub rtc_channel_id: Option<u64>,
    pub region: Option<String>,
    pub viewer_ids: Vec<u64>,
    pub paused: bool,
    pub unavailable: bool,
}

impl StreamState {
    fn new(key: StreamKey) -> Self {
        Self {
            key,
            rtc_server_id: None,
            rtc_channel_id: None,
            region: None,
            viewer_ids: Vec::new(),
            paused: false,
            unavailable: false,
        }
    }

    /// `Stream._update`: only the fields present in the payload change.
    fn update(&mut self, data: &Value) {
        if let Some(v) = data.get("rtc_server_id").and_then(snowflake) {
            self.rtc_server_id = Some(v);
        }
        if let Some(v) = data.get("rtc_channel_id").and_then(snowflake) {
            self.rtc_channel_id = Some(v);
        }
        if let Some(v) = data.get("region").and_then(Value::as_str) {
            self.region = Some(v.to_owned());
        }
        if let Some(v) = data.get("viewer_ids").and_then(Value::as_array) {
            self.viewer_ids = v.iter().filter_map(snowflake).collect();
        }
        if let Some(v) = data.get("paused").and_then(Value::as_bool) {
            self.paused = v;
        }
    }
}

/// What one dispatch changed, for consumers that need the before/after pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheChange {
    Voice {
        guild_id: Option<u64>,
        user_id: u64,
        before: Option<VoiceStateEntry>,
        after: VoiceStateEntry,
    },
    Stream {
        key: StreamKey,
        before: Option<StreamState>,
        after: Option<StreamState>,
    },
}

/// Channel type of a guild voice channel (`ChannelType.voice`; stage channels are not one).
const VOICE_CHANNEL: u64 = 2;

#[derive(Debug, Clone, Default)]
struct UserInfo {
    username: Option<String>,
    global_name: Option<String>,
    bot: bool,
}

#[derive(Debug, Default)]
struct GuildCache {
    voice_states: HashMap<u64, VoiceStateEntry>,
    unavailable: bool,
    /// Channel id -> channel type.
    channels: HashMap<u64, u64>,
    roles: HashMap<u64, String>,
    /// Members seen so far (guild payload, member events, messages, voice states) -> nickname.
    members: HashMap<u64, Option<String>>,
}

#[derive(Debug, Default)]
pub struct Cache {
    self_id: Option<u64>,
    installation_id: Option<String>,
    preferred_rtc_regions: Vec<String>,
    guilds: HashMap<u64, GuildCache>,
    /// Guild ids in READY order; READY_SUPPLEMENTAL is zipped against it by index.
    ready_guild_order: Vec<u64>,
    streams: HashMap<StreamKey, StreamState>,
    users: HashMap<u64, UserInfo>,
}

fn snowflake(v: &Value) -> Option<u64> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn guild_id_of(guild: &Value) -> Option<u64> {
    guild
        .get("id")
        .or_else(|| guild.get("properties").and_then(|p| p.get("id")))
        .and_then(snowflake)
}

impl Cache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> SharedCache {
        Arc::new(RwLock::new(Self::new()))
    }

    pub fn self_id(&self) -> Option<u64> {
        self.self_id
    }

    /// `apex_experiments.installation` from READY; sent back in later IDENTIFY properties.
    pub fn installation_id(&self) -> Option<&str> {
        self.installation_id.as_deref()
    }

    /// `geo_ordered_rtc_regions` from READY (`preferred_rtc_regions` in the Python state).
    pub fn preferred_rtc_regions(&self) -> &[String] {
        &self.preferred_rtc_regions
    }

    pub fn has_guild(&self, guild_id: u64) -> bool {
        self.guilds.contains_key(&guild_id)
    }

    pub fn guild_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.guilds.keys().copied()
    }

    pub fn voice_state(&self, guild_id: u64, user_id: u64) -> Option<&VoiceStateEntry> {
        self.guilds.get(&guild_id)?.voice_states.get(&user_id)
    }

    /// `(user_id, state)` of everyone currently in a voice channel of the guild.
    pub fn voice_states(&self, guild_id: u64) -> impl Iterator<Item = (u64, &VoiceStateEntry)> {
        self.guilds
            .get(&guild_id)
            .into_iter()
            .flat_map(|g| g.voice_states.iter().map(|(u, v)| (*u, v)))
    }

    pub fn stream(&self, key: &StreamKey) -> Option<&StreamState> {
        self.streams.get(key)
    }

    pub fn streams(&self) -> impl Iterator<Item = &StreamState> {
        self.streams.values()
    }

    /// Ids of the guild voice channels (stage channels excluded), as `discord.VoiceChannel`.
    pub fn voice_channel_ids(&self, guild_id: u64) -> Vec<u64> {
        self.guilds.get(&guild_id).map_or_else(Vec::new, |g| {
            g.channels
                .iter()
                .filter(|(_, kind)| **kind == VOICE_CHANNEL)
                .map(|(id, _)| *id)
                .collect()
        })
    }

    pub fn is_voice_channel(&self, guild_id: u64, channel_id: u64) -> bool {
        self.guilds
            .get(&guild_id)
            .and_then(|g| g.channels.get(&channel_id))
            .is_some_and(|kind| *kind == VOICE_CHANNEL)
    }

    pub fn role_name(&self, guild_id: u64, role_id: u64) -> Option<&str> {
        self.guilds
            .get(&guild_id)?
            .roles
            .get(&role_id)
            .map(String::as_str)
    }

    /// Guild display name (nickname, else global name, else username) when the member is known,
    /// else the global name or username of the user; `None` when the id was never seen.
    pub fn display_name(&self, guild_id: u64, user_id: u64) -> Option<String> {
        let user = self.users.get(&user_id);
        let name = |u: Option<&UserInfo>| {
            u.and_then(|u| u.global_name.clone().or_else(|| u.username.clone()))
        };
        match self
            .guilds
            .get(&guild_id)
            .and_then(|g| g.members.get(&user_id))
        {
            Some(nick) => nick.clone().or_else(|| name(user)),
            None => name(user),
        }
    }

    /// The cached user is a bot account; unknown ids are not.
    pub fn is_bot(&self, user_id: u64) -> bool {
        self.users.get(&user_id).is_some_and(|u| u.bot)
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Applies one dispatch (`t`, `d`). Unknown events and malformed fields are ignored, like the
    /// Python parsers' "unknown guild/stream, discarding" paths.
    pub fn apply(&mut self, event: &str, data: &Value) -> Option<CacheChange> {
        match event {
            "READY" => self.on_ready(data),
            "READY_SUPPLEMENTAL" => self.on_ready_supplemental(data),
            "GUILD_CREATE" => self.on_guild_create(data),
            "GUILD_DELETE" => self.on_guild_delete(data),
            "VOICE_STATE_UPDATE" => return self.on_voice_state_update(data),
            "PASSIVE_UPDATE_V2" => self.on_passive_update_v2(data),
            "CHANNEL_CREATE" | "CHANNEL_UPDATE" => self.on_channel_upsert(data),
            "CHANNEL_DELETE" => self.on_channel_delete(data),
            "GUILD_ROLE_CREATE" | "GUILD_ROLE_UPDATE" => self.on_role_upsert(data),
            "GUILD_ROLE_DELETE" => self.on_role_delete(data),
            "GUILD_MEMBER_ADD" | "GUILD_MEMBER_UPDATE" => self.on_member_upsert(data),
            "GUILD_MEMBER_REMOVE" => self.on_member_remove(data),
            "GUILD_MEMBERS_CHUNK" => self.on_members_chunk(data),
            "MESSAGE_CREATE" => self.on_message_create(data),
            "STREAM_CREATE" => return self.on_stream_create(data),
            "STREAM_UPDATE" => return self.on_stream_update(data),
            "STREAM_DELETE" => return self.on_stream_delete(data),
            _ => {}
        }
        None
    }

    fn on_ready(&mut self, data: &Value) {
        self.clear();
        self.self_id = data
            .get("user")
            .and_then(|u| u.get("id"))
            .and_then(snowflake);
        self.installation_id = data
            .get("apex_experiments")
            .and_then(|a| a.get("installation"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.preferred_rtc_regions = match data
            .get("geo_ordered_rtc_regions")
            .and_then(Value::as_array)
        {
            Some(r) => r
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
            None => vec!["us-central".to_owned()],
        };
        for guild in data
            .get("guilds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(id) = guild_id_of(guild) {
                self.ready_guild_order.push(id);
                self.add_guild(id, guild);
            }
        }
    }

    /// Guild `i` of READY pairs with guild `i` of the supplemental payload.
    fn on_ready_supplemental(&mut self, data: &Value) {
        let extras = data
            .get("guilds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for (id, extra) in self.ready_guild_order.clone().into_iter().zip(extras) {
            let Some(guild) = self.guilds.get_mut(&id) else {
                continue;
            };
            for vs in extra
                .get("voice_states")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some((user_id, entry)) = parse_joined_voice_state(vs) {
                    guild.voice_states.insert(user_id, entry);
                }
            }
        }
    }

    fn on_guild_create(&mut self, data: &Value) {
        if let Some(id) = guild_id_of(data) {
            self.add_guild(id, data);
        }
    }

    fn on_guild_delete(&mut self, data: &Value) {
        let Some(id) = data.get("id").and_then(snowflake) else {
            return;
        };
        if data
            .get("unavailable")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if let Some(g) = self.guilds.get_mut(&id) {
                g.unavailable = true;
            }
        } else {
            self.guilds.remove(&id);
        }
    }

    fn add_guild(&mut self, id: u64, data: &Value) {
        let mut guild = GuildCache {
            unavailable: data
                .get("unavailable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            ..GuildCache::default()
        };
        for vs in data
            .get("voice_states")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some((user_id, entry)) = parse_joined_voice_state(vs) {
                guild.voice_states.insert(user_id, entry);
            }
        }
        for channel in data
            .get("channels")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some((cid, kind)) = parse_channel(channel) {
                guild.channels.insert(cid, kind);
            }
        }
        for role in data
            .get("roles")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some((rid, name)) = parse_role(role) {
                guild.roles.insert(rid, name);
            }
        }
        self.guilds.insert(id, guild);
        for member in data
            .get("members")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            self.note_member(id, member);
        }
    }

    /// Remembers the user and, for a guild, the member (nickname) of a member object
    /// (`{user: {...}, nick}`). Unknown guilds are ignored.
    fn note_member(&mut self, guild_id: u64, member: &Value) {
        let Some(user_id) = member.get("user").and_then(|u| self.note_user(u)) else {
            return;
        };
        if let Some(guild) = self.guilds.get_mut(&guild_id) {
            let nick = member
                .get("nick")
                .and_then(Value::as_str)
                .map(str::to_owned);
            guild.members.insert(user_id, nick);
        }
    }

    fn note_user(&mut self, user: &Value) -> Option<u64> {
        let id = user.get("id").and_then(snowflake)?;
        let info = self.users.entry(id).or_default();
        if let Some(v) = user.get("username").and_then(Value::as_str) {
            info.username = Some(v.to_owned());
        }
        if let Some(v) = user.get("global_name") {
            info.global_name = v.as_str().map(str::to_owned);
        }
        if let Some(bot) = user.get("bot").and_then(Value::as_bool) {
            info.bot = bot;
        }
        Some(id)
    }

    fn guild_mut(&mut self, data: &Value) -> Option<&mut GuildCache> {
        let id = data.get("guild_id").and_then(snowflake)?;
        self.guilds.get_mut(&id)
    }

    fn on_channel_upsert(&mut self, data: &Value) {
        if let (Some(guild), Some((id, kind))) = (self.guild_mut(data), parse_channel(data)) {
            guild.channels.insert(id, kind);
        }
    }

    fn on_channel_delete(&mut self, data: &Value) {
        if let (Some(guild), Some(id)) = (self.guild_mut(data), data.get("id").and_then(snowflake))
        {
            guild.channels.remove(&id);
        }
    }

    fn on_role_upsert(&mut self, data: &Value) {
        if let (Some(guild), Some((id, name))) =
            (self.guild_mut(data), data.get("role").and_then(parse_role))
        {
            guild.roles.insert(id, name);
        }
    }

    fn on_role_delete(&mut self, data: &Value) {
        if let (Some(guild), Some(id)) = (
            self.guild_mut(data),
            data.get("role_id").and_then(snowflake),
        ) {
            guild.roles.remove(&id);
        }
    }

    fn on_member_upsert(&mut self, data: &Value) {
        if let Some(guild_id) = data.get("guild_id").and_then(snowflake) {
            self.note_member(guild_id, data);
        }
    }

    fn on_member_remove(&mut self, data: &Value) {
        let Some(user_id) = data
            .get("user")
            .and_then(|u| u.get("id"))
            .and_then(snowflake)
        else {
            return;
        };
        if let Some(guild) = self.guild_mut(data) {
            guild.members.remove(&user_id);
        }
    }

    fn on_members_chunk(&mut self, data: &Value) {
        let Some(guild_id) = data.get("guild_id").and_then(snowflake) else {
            return;
        };
        for member in data
            .get("members")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            self.note_member(guild_id, member);
        }
    }

    /// The author (and, in a guild, the member) of a message is a known user from then on.
    fn on_message_create(&mut self, data: &Value) {
        let Some(id) = data.get("author").and_then(|a| self.note_user(a)) else {
            return;
        };
        let nick = data
            .get("member")
            .map(|m| m.get("nick").and_then(Value::as_str).map(str::to_owned));
        if let (Some(nick), Some(guild)) = (nick, self.guild_mut(data)) {
            guild.members.insert(id, nick);
        }
    }

    fn on_voice_state_update(&mut self, data: &Value) -> Option<CacheChange> {
        let guild_id = data.get("guild_id").and_then(snowflake);
        let user_id = data.get("user_id").and_then(snowflake)?;
        if let (Some(guild_id), Some(member)) = (guild_id, data.get("member")) {
            self.note_member(guild_id, member);
        }
        let after = VoiceStateEntry {
            channel_id: data.get("channel_id").and_then(snowflake),
            self_stream: data
                .get("self_stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            self_video: data
                .get("self_video")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        // Unknown guild: discarded, as is a call (no guild) voice state.
        let guild = self.guilds.get_mut(&guild_id?)?;
        let before = if after.channel_id.is_some() {
            guild.voice_states.insert(user_id, after)
        } else {
            guild.voice_states.remove(&user_id)
        };
        Some(CacheChange::Voice {
            guild_id,
            user_id,
            before,
            after,
        })
    }

    fn on_passive_update_v2(&mut self, data: &Value) {
        let Some(guild) = data
            .get("guild_id")
            .and_then(snowflake)
            .and_then(|id| self.guilds.get_mut(&id))
        else {
            return;
        };
        for user_id in data
            .get("removed_voice_states")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(user_id) = snowflake(user_id) {
                guild.voice_states.remove(&user_id);
            }
        }
        for vs in data
            .get("updated_voice_states")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(user_id) = vs.get("user_id").and_then(snowflake) else {
                continue;
            };
            match parse_joined_voice_state(vs) {
                Some((_, entry)) => guild.voice_states.insert(user_id, entry),
                None => guild.voice_states.remove(&user_id),
            };
        }
    }

    fn on_stream_create(&mut self, data: &Value) -> Option<CacheChange> {
        let key = stream_key_of(data)?;
        let before = self.streams.get(&key).cloned();
        let stream = self
            .streams
            .entry(key)
            .or_insert_with(|| StreamState::new(key));
        stream.update(data);
        stream.unavailable = false;
        Some(CacheChange::Stream {
            key,
            before,
            after: Some(stream.clone()),
        })
    }

    /// Unknown stream keys are discarded.
    fn on_stream_update(&mut self, data: &Value) -> Option<CacheChange> {
        let key = stream_key_of(data)?;
        let stream = self.streams.get_mut(&key)?;
        let before = Some(stream.clone());
        stream.update(data);
        Some(CacheChange::Stream {
            key,
            before,
            after: Some(stream.clone()),
        })
    }

    /// `unavailable: true` keeps the stream (flagged); anything else removes it.
    fn on_stream_delete(&mut self, data: &Value) -> Option<CacheChange> {
        let key = stream_key_of(data)?;
        let before = self.streams.get(&key).cloned();
        if data
            .get("unavailable")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let stream = self.streams.entry(key).or_insert_with(|| {
                let mut s = StreamState::new(key);
                s.update(data);
                s
            });
            stream.unavailable = true;
            return Some(CacheChange::Stream {
                key,
                before,
                after: Some(stream.clone()),
            });
        }
        self.streams.remove(&key);
        Some(CacheChange::Stream {
            key,
            before,
            after: None,
        })
    }
}

fn parse_channel(channel: &Value) -> Option<(u64, u64)> {
    Some((
        channel.get("id").and_then(snowflake)?,
        channel.get("type").and_then(Value::as_u64)?,
    ))
}

fn parse_role(role: &Value) -> Option<(u64, String)> {
    Some((
        role.get("id").and_then(snowflake)?,
        role.get("name").and_then(Value::as_str)?.to_owned(),
    ))
}

fn stream_key_of(data: &Value) -> Option<StreamKey> {
    data.get("stream_key")?.as_str()?.parse().ok()
}

/// A voice-state object that has a channel (`Guild._from_data` does `int(vs['channel_id'])`).
fn parse_joined_voice_state(vs: &Value) -> Option<(u64, VoiceStateEntry)> {
    let user_id = vs.get("user_id").and_then(snowflake)?;
    let channel_id = vs.get("channel_id").and_then(snowflake)?;
    Some((
        user_id,
        VoiceStateEntry {
            channel_id: Some(channel_id),
            self_stream: vs
                .get("self_stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            self_video: vs
                .get("self_video")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
    ))
}
