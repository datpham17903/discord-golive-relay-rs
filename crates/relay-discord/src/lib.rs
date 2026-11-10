//! Discord user-account client: REST and gateway over `wreq` (browser-matching TLS/HTTP fingerprint).
//!
//! Port of the pinned `discord.py-self` pieces the relay relies on: `tracking.HeadersContext`
//! (client properties), `http.HTTPClient` (headers) and `gateway.DiscordWebSocket` (v9, zstd-stream);
//! and, on top of them, the adapters the service drives: [`DiscordAdapter`] (gateway events to model
//! events, voice join, watch and output of the ingest account), [`OutputAccountAdapter`] (extra
//! output accounts) and the TWCC feedback. The service sees them through the [`Adapter`] and
//! [`OutputAdapter`] traits of the [`api`] module, which also documents the crate dependency
//! direction.

pub mod adapter;
pub mod api;
pub mod cache;
pub mod codecs;
pub mod error;
pub mod gateway;
pub mod http;
pub mod ingress;
pub mod link;
mod net;
pub mod output_account;
pub mod props;
pub mod token;
pub mod twcc;

pub use adapter::{AdapterOptions, DiscordAdapter};
pub use api::{
    Adapter, AdapterError, AdapterEvent, ApiFuture, BoxFuture, EventSink, IngressSink, LogFn,
    OutputAdapter, OutputSource, PoolEventSink, SourceFactory, VoiceStateCallback,
};
pub use output_account::{OutputAccountAdapter, OutputOptions};

pub use cache::{
    Cache, CacheChange, SharedCache, StreamKey, StreamState, StreamType, VoiceStateEntry,
};
pub use error::{Error, Result};
pub use gateway::{
    Dispatch, Gateway, GatewayConfig, GatewayEvent, GatewayHandle, VoiceStateUpdate,
};
pub use http::{RestClient, User};
pub use props::{HeadersContext, Urls};
pub use token::Token;
