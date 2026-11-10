// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/transport/mod.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       added the `net` and `packets` modules (ported from src/python/).

mod crypto;
pub mod net;
pub mod packets;
pub mod udp;

pub use crypto::{CryptoError, TransportCrypto, TransportEncryptionMode};
