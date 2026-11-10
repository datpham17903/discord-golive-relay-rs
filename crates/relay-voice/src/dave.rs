//! DAVE (end-to-end encryption) session handling for a voice connection.
//!
//! Port of the DAVE parts of `discord/gateway.py::DiscordVoiceWebSocket` (ops 21-31),
//! `discord/voice_state.py::VoiceConnectionState` (`reinit_dave_session`, `_execute_transition`,
//! `_recover_from_invalid_commit`) and `discord/ext/native_voice` (`_apply_dave`, `_can_decrypt_dave`,
//! the retryable decrypt errors).
//!
//! This is the only file that names `davey`, so the crate version can be swapped here alone.
//!
//! `davey 0.2.0` differs from the 0.1.x the Python side is pinned to: `process_commit` /
//! `process_welcome` no longer install keys. The caller must follow them with `prepare_transition`
//! and the key used to encrypt only switches on `execute_transition` (immediately for transition 0).
//! The Python stack never calls those two, so with 0.2.x its decryptors stay empty and DAVE frames
//! never decrypt ("packets arrive, no frame assembles"). This port calls them, like libdave.

use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU64, Ordering};

use davey::errors::{DecryptError, DecryptorDecryptError};
use davey::{Codec, DaveSession, MediaType, ProposalsOperationType};
use parking_lot::Mutex;
use relay_media::codec::VoiceCodec;

use crate::voice_ws::op;

/// Seconds an unencrypted frame is still accepted from users when the group is being downgraded.
const DOWNGRADE_PASSTHROUGH_EXPIRY: u32 = 120;
/// Seconds passed with passthrough mode on a reset/upgrade (`set_passthrough_mode(True, 10)`).
const PASSTHROUGH_EXPIRY: u32 = 10;

#[derive(Debug, thiserror::Error)]
pub enum DaveError {
    #[error("DAVE: {0}")]
    Session(String),
    #[error("DAVE: malformed {0} message")]
    Malformed(&'static str),
    #[error("DAVE: unsupported protocol version {0}")]
    UnsupportedVersion(u64),
}

fn session_err(e: impl std::fmt::Display) -> DaveError {
    DaveError::Session(e.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum DaveDecryptError {
    #[error("no DAVE session")]
    NoSession,
    #[error("DAVE decrypt failed: {0}")]
    Failed(String),
    /// Same condition as Python `_is_retryable_dave_decrypt_error` (`NoValidCryptorFound` /
    /// `NoDecryptorForUser`): the key may arrive with a later transition, so the frame can be retried.
    #[error("DAVE decrypt failed (retryable): {0}")]
    Retryable(String),
}

impl DaveDecryptError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("DAVE encrypt failed: {0}")]
pub struct DaveEncryptError(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DaveMedia {
    Audio,
    Video,
}

impl DaveMedia {
    fn raw(self) -> MediaType {
        match self {
            Self::Audio => MediaType::AUDIO,
            Self::Video => MediaType::VIDEO,
        }
    }
}

fn dave_codec(codec: VoiceCodec) -> Codec {
    match codec {
        VoiceCodec::VP8 => Codec::VP8,
        VoiceCodec::VP9 => Codec::VP9,
        VoiceCodec::H264 => Codec::H264,
        VoiceCodec::H265 => Codec::H265,
        VoiceCodec::AV1 => Codec::AV1,
    }
}

/// A message the voice websocket must send as a result of a DAVE event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DaveOut {
    /// op 23 `{"transition_id"}`.
    TransitionReady(u16),
    /// op 26, serialized key package (binary).
    KeyPackage(Vec<u8>),
    /// op 28, `commit` followed by the optional `welcome` (binary).
    CommitWelcome(Vec<u8>),
    /// op 31 `{"transition_id"}`.
    InvalidCommitWelcome(u16),
}

struct Inner {
    user_id: u64,
    /// MLS group id: the voice channel id.
    group_id: u64,
    session: Option<DaveSession>,
    protocol_version: u16,
    /// `dave_pending_transitions`: transition id to protocol version.
    pending: HashMap<u16, u16>,
    downgraded: bool,
}

/// Read-only view of the session, for probes and status output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaveSnapshot {
    pub protocol_version: u16,
    pub has_session: bool,
    /// `davey` session status (`INACTIVE`, `PENDING`, `AWAITING_RESPONSE`, `ACTIVE`), if a session exists.
    pub status: Option<String>,
    pub ready: bool,
    pub epoch: Option<u64>,
    pub pending_transitions: usize,
    pub downgraded: bool,
}

/// DAVE state of one voice connection; shared (`Arc`) between the websocket task and the media paths.
/// It outlives a websocket resume, like the Python connection state that owns `dave_session`.
pub struct Dave {
    inner: Mutex<Inner>,
    decrypt_failures: AtomicU64,
}

impl Dave {
    pub fn new(user_id: u64, group_id: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                user_id,
                group_id,
                session: None,
                protocol_version: 0,
                pending: HashMap::new(),
                downgraded: false,
            }),
            decrypt_failures: AtomicU64::new(0),
        }
    }

    /// `max_dave_protocol_version` for IDENTIFY.
    pub fn max_protocol_version() -> u16 {
        davey::DAVE_PROTOCOL_VERSION
    }

    pub fn protocol_version(&self) -> u16 {
        self.inner.lock().protocol_version
    }

    pub fn snapshot(&self) -> DaveSnapshot {
        let inner = self.inner.lock();
        DaveSnapshot {
            protocol_version: inner.protocol_version,
            has_session: inner.session.is_some(),
            status: inner.session.as_ref().map(|s| format!("{:?}", s.status())),
            ready: inner.session.as_ref().is_some_and(DaveSession::is_ready),
            epoch: inner
                .session
                .as_ref()
                .and_then(DaveSession::epoch)
                .map(|e| e.as_u64()),
            pending_transitions: inner.pending.len(),
            downgraded: inner.downgraded,
        }
    }

    pub fn has_session(&self) -> bool {
        self.inner.lock().session.is_some()
    }

    /// Frames `decrypt` could not decrypt (including retryable ones).
    pub fn decrypt_failures(&self) -> u64 {
        self.decrypt_failures.load(Ordering::Relaxed)
    }

    /// `VoiceConnectionState.can_encrypt`.
    pub fn can_encrypt(&self) -> bool {
        self.inner.lock().can_encrypt()
    }

    /// `VoiceClient._can_decrypt_dave`.
    pub fn can_decrypt(&self, user_id: u64) -> bool {
        let inner = self.inner.lock();
        if inner.can_encrypt() {
            return true;
        }
        inner
            .session
            .as_ref()
            .is_some_and(|s| s.is_ready() && s.can_passthrough(user_id))
    }

    /// SESSION_DESCRIPTION `dave_protocol_version`: records it and, when above 0, (re)initializes the
    /// session; the returned key package (op 26) must be sent.
    pub fn on_session_description(&self, version: u64) -> Result<Vec<DaveOut>, DaveError> {
        let version = u16::try_from(version).map_err(|_| DaveError::UnsupportedVersion(version))?;
        let mut inner = self.inner.lock();
        inner.protocol_version = version;
        inner.reinit_session()
    }

    /// op 21 DAVE_PREPARE_TRANSITION. Ignored without a session, like every DAVE op.
    pub fn on_prepare_transition(
        &self,
        transition_id: u16,
        protocol_version: u16,
    ) -> Result<Vec<DaveOut>, DaveError> {
        let mut inner = self.inner.lock();
        let Some(session) = inner.session.as_mut() else {
            return Ok(Vec::new());
        };
        session
            .prepare_transition(transition_id, protocol_version)
            .map_err(session_err)?;
        inner.pending.insert(transition_id, protocol_version);
        if transition_id == 0 {
            inner.execute_transition(transition_id)?;
            return Ok(Vec::new());
        }
        if protocol_version == 0
            && let Some(session) = inner.session.as_mut()
        {
            session.set_passthrough_mode(true, Some(DOWNGRADE_PASSTHROUGH_EXPIRY));
        }
        Ok(vec![DaveOut::TransitionReady(transition_id)])
    }

    /// op 22 DAVE_EXECUTE_TRANSITION.
    pub fn on_execute_transition(&self, transition_id: u16) -> Result<(), DaveError> {
        let mut inner = self.inner.lock();
        if inner.session.is_none() {
            return Ok(());
        }
        inner.execute_transition(transition_id)
    }

    /// op 24 DAVE_PREPARE_EPOCH: epoch 1 announces a new MLS group, so the session is re-initialized.
    pub fn on_prepare_epoch(
        &self,
        epoch: u64,
        protocol_version: u16,
    ) -> Result<Vec<DaveOut>, DaveError> {
        let mut inner = self.inner.lock();
        if inner.session.is_none() || epoch != 1 {
            return Ok(Vec::new());
        }
        inner.protocol_version = protocol_version;
        inner.reinit_session()
    }

    /// Binary ops 25, 27, 29 and 30. `payload` is what follows the 3-byte `seq` + opcode header.
    pub fn on_binary(&self, opcode: u8, payload: &[u8]) -> Result<Vec<DaveOut>, DaveError> {
        let mut inner = self.inner.lock();
        if inner.session.is_none() {
            return Ok(Vec::new());
        }
        match opcode {
            op::MLS_EXTERNAL_SENDER => {
                inner
                    .session_mut()
                    .set_external_sender(payload)
                    .map_err(session_err)?;
                Ok(Vec::new())
            }
            op::MLS_PROPOSALS => {
                let [optype, proposals @ ..] = payload else {
                    return Err(DaveError::Malformed("MLS_PROPOSALS"));
                };
                let kind = if *optype == 0 {
                    ProposalsOperationType::APPEND
                } else {
                    ProposalsOperationType::REVOKE
                };
                let result = inner
                    .session_mut()
                    .process_proposals(kind, proposals, None)
                    .map_err(session_err)?;
                Ok(result
                    .map(|cw| {
                        let mut data = cw.commit;
                        if let Some(welcome) = cw.welcome {
                            data.extend_from_slice(&welcome);
                        }
                        DaveOut::CommitWelcome(data)
                    })
                    .into_iter()
                    .collect())
            }
            op::MLS_ANNOUNCE_COMMIT_TRANSITION | op::MLS_WELCOME => {
                let name = if opcode == op::MLS_WELCOME {
                    "MLS_WELCOME"
                } else {
                    "MLS_ANNOUNCE_COMMIT_TRANSITION"
                };
                let [hi, lo, data @ ..] = payload else {
                    return Err(DaveError::Malformed(name));
                };
                let transition_id = u16::from_be_bytes([*hi, *lo]);
                let welcome = opcode == op::MLS_WELCOME;
                match inner.process_commit_or_welcome(welcome, transition_id, data) {
                    Ok(out) => Ok(out),
                    Err(_) => inner.recover_from_invalid_commit(transition_id),
                }
            }
            _ => Ok(Vec::new()),
        }
    }

    /// `_decrypt_video_frame` / the audio decrypt path: decrypt `payload` sent by `user_id`.
    pub fn decrypt(
        &self,
        user_id: u64,
        media: DaveMedia,
        payload: &[u8],
    ) -> Result<Vec<u8>, DaveDecryptError> {
        let mut inner = self.inner.lock();
        let Some(session) = inner.session.as_mut() else {
            return Err(DaveDecryptError::NoSession);
        };
        session.decrypt(user_id, media.raw(), payload).map_err(|e| {
            self.decrypt_failures.fetch_add(1, Ordering::Relaxed);
            classify_decrypt_error(e)
        })
    }

    /// `_apply_dave` for Opus: the frame unchanged when this connection cannot encrypt.
    pub fn encrypt_opus<'a>(&self, packet: &'a [u8]) -> Result<Cow<'a, [u8]>, DaveEncryptError> {
        let mut inner = self.inner.lock();
        if !inner.can_encrypt() {
            return Ok(Cow::Borrowed(packet));
        }
        inner
            .session_mut()
            .encrypt_opus(packet)
            .map_err(|e| DaveEncryptError(e.to_string()))
    }

    /// `_apply_dave` for video: the frame unchanged when this connection cannot encrypt.
    pub fn encrypt_video<'a>(
        &self,
        codec: VoiceCodec,
        frame: &'a [u8],
    ) -> Result<Cow<'a, [u8]>, DaveEncryptError> {
        let mut inner = self.inner.lock();
        if !inner.can_encrypt() {
            return Ok(Cow::Borrowed(frame));
        }
        inner
            .session_mut()
            .encrypt(MediaType::VIDEO, dave_codec(codec), frame)
            .map_err(|e| DaveEncryptError(e.to_string()))
    }
}

fn classify_decrypt_error(e: DecryptError) -> DaveDecryptError {
    let message = e.to_string();
    match e {
        DecryptError::NoDecryptorForUser => DaveDecryptError::Retryable(message),
        DecryptError::DecryptionFailed(
            DecryptorDecryptError::UnencryptedWhenPassthroughDisabled,
        ) => DaveDecryptError::Failed(message),
        DecryptError::DecryptionFailed(_) => DaveDecryptError::Retryable(message),
    }
}

impl Inner {
    fn session_mut(&mut self) -> &mut DaveSession {
        self.session.as_mut().expect("checked by the caller")
    }

    fn can_encrypt(&self) -> bool {
        self.protocol_version != 0 && self.session.as_ref().is_some_and(DaveSession::can_encrypt)
    }

    /// `reinit_dave_session`.
    fn reinit_session(&mut self) -> Result<Vec<DaveOut>, DaveError> {
        if let Some(version) = NonZeroU16::new(self.protocol_version) {
            let (user_id, group_id) = (self.user_id, self.group_id);
            match self.session.as_mut() {
                Some(session) => session
                    .reinit(version, user_id, group_id, None)
                    .map_err(session_err)?,
                None => {
                    self.session = Some(
                        DaveSession::new(version, user_id, group_id, None).map_err(session_err)?,
                    );
                }
            }
            let key_package = self
                .session_mut()
                .create_key_package()
                .map_err(session_err)?;
            Ok(vec![DaveOut::KeyPackage(key_package)])
        } else {
            if let Some(session) = self.session.as_mut() {
                session.reset().map_err(session_err)?;
                session.set_passthrough_mode(true, Some(PASSTHROUGH_EXPIRY));
            }
            Ok(Vec::new())
        }
    }

    /// `_recover_from_invalid_commit`.
    fn recover_from_invalid_commit(
        &mut self,
        transition_id: u16,
    ) -> Result<Vec<DaveOut>, DaveError> {
        let mut out = vec![DaveOut::InvalidCommitWelcome(transition_id)];
        out.extend(self.reinit_session()?);
        Ok(out)
    }

    /// ops 29/30: process the message, then prepare its transition (davey 0.2 does not install
    /// keys in `process_*`).
    fn process_commit_or_welcome(
        &mut self,
        welcome: bool,
        transition_id: u16,
        data: &[u8],
    ) -> Result<Vec<DaveOut>, DaveError> {
        let version = self.protocol_version;
        let session = self.session_mut();
        if welcome {
            session.process_welcome(data, None).map_err(session_err)?;
        } else if !session.process_commit(data).map_err(session_err)? {
            return Ok(Vec::new());
        }
        session
            .prepare_transition(transition_id, version)
            .map_err(session_err)?;
        if transition_id == 0 {
            return Ok(Vec::new());
        }
        self.pending.insert(transition_id, version);
        Ok(vec![DaveOut::TransitionReady(transition_id)])
    }

    /// `_execute_transition`.
    fn execute_transition(&mut self, transition_id: u16) -> Result<(), DaveError> {
        let Some(new_version) = self.pending.remove(&transition_id) else {
            return Ok(());
        };
        self.session_mut()
            .execute_transition(transition_id)
            .map_err(session_err)?;
        let old_version = std::mem::replace(&mut self.protocol_version, new_version);
        if old_version != new_version && new_version == 0 {
            self.downgraded = true;
        } else if transition_id > 0 && self.downgraded {
            self.downgraded = false;
            self.session_mut()
                .set_passthrough_mode(true, Some(PASSTHROUGH_EXPIRY));
        }
        Ok(())
    }
}

impl std::fmt::Debug for Dave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("Dave")
            .field("protocol_version", &inner.protocol_version)
            .field("has_session", &inner.session.is_some())
            .finish()
    }
}
