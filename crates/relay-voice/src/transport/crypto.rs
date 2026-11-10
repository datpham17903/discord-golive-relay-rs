// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/transport/crypto.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       added mode names (`as_str`/`FromStr`, from src/python/transport.rs) and
//                `CryptoError::UnsupportedMode`.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use chacha20poly1305::{Key as ChaChaKey, XChaCha20Poly1305, XNonce};

use crate::rtp::RtpPacket;

const INITIAL_LITE_NONCE: u32 = 0x8000_0000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportEncryptionMode {
    AeadXChaCha20Poly1305RtpSize,
    AeadAes256GcmRtpSize,
}

impl TransportEncryptionMode {
    /// Name used in the voice gateway `select_protocol` / `session_description` payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AeadXChaCha20Poly1305RtpSize => "aead_xchacha20_poly1305_rtpsize",
            Self::AeadAes256GcmRtpSize => "aead_aes256_gcm_rtpsize",
        }
    }
}

impl std::str::FromStr for TransportEncryptionMode {
    type Err = CryptoError;

    fn from_str(mode: &str) -> Result<Self, CryptoError> {
        match mode {
            "aead_xchacha20_poly1305_rtpsize" => Ok(Self::AeadXChaCha20Poly1305RtpSize),
            "aead_aes256_gcm_rtpsize" => Ok(Self::AeadAes256GcmRtpSize),
            _ => Err(CryptoError::UnsupportedMode(mode.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CryptoError {
    InvalidKeyLength(usize),
    UnsupportedMode(String),
    EncryptFailed,
    DecryptFailed,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidKeyLength(length) => write!(f, "invalid key length {length}, expected 32"),
            Self::UnsupportedMode(mode) => {
                write!(f, "Unsupported transport encryption mode: {mode}")
            }
            Self::EncryptFailed => write!(f, "transport encryption failed"),
            Self::DecryptFailed => write!(f, "transport decryption failed"),
        }
    }
}

impl std::error::Error for CryptoError {}

enum Cipher {
    XChaCha20(XChaCha20Poly1305),
    Aes256(Box<Aes256Gcm>),
}

pub struct TransportCrypto {
    cipher: Cipher,
    nonce: u32,
}

impl TransportCrypto {
    pub fn new(mode: TransportEncryptionMode, key: &[u8]) -> Result<Self, CryptoError> {
        if key.len() != 32 {
            return Err(CryptoError::InvalidKeyLength(key.len()));
        }

        let cipher = match mode {
            TransportEncryptionMode::AeadXChaCha20Poly1305RtpSize => {
                Cipher::XChaCha20(XChaCha20Poly1305::new(ChaChaKey::from_slice(key)))
            }
            TransportEncryptionMode::AeadAes256GcmRtpSize => Cipher::Aes256(Box::new(
                Aes256Gcm::new_from_slice(key)
                    .map_err(|_| CryptoError::InvalidKeyLength(key.len()))?,
            )),
        };

        Ok(Self {
            cipher,
            nonce: INITIAL_LITE_NONCE,
        })
    }

    pub fn encrypt_rtp(&mut self, header: &[u8], payload: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let nonce_value = self.nonce;
        self.nonce = self.nonce.wrapping_add(1);

        let ciphertext = match &self.cipher {
            Cipher::XChaCha20(cipher) => {
                let nonce = make_xchacha_nonce(nonce_value);
                cipher
                    .encrypt(
                        XNonce::from_slice(&nonce),
                        Payload {
                            msg: payload,
                            aad: header,
                        },
                    )
                    .map_err(|_| CryptoError::EncryptFailed)?
            }
            Cipher::Aes256(cipher) => {
                let nonce = make_aes_nonce(nonce_value);
                cipher
                    .encrypt(
                        AesNonce::from_slice(&nonce),
                        Payload {
                            msg: payload,
                            aad: header,
                        },
                    )
                    .map_err(|_| CryptoError::EncryptFailed)?
            }
        };

        let mut packet = Vec::with_capacity(header.len() + ciphertext.len() + 4);
        packet.extend_from_slice(header);
        packet.extend_from_slice(&ciphertext);
        packet.extend_from_slice(&nonce_value.to_le_bytes());
        Ok(packet)
    }

    pub fn decrypt_packet(
        &mut self,
        header: &[u8],
        encrypted_payload: &[u8],
        nonce_suffix: &[u8; 4],
    ) -> Result<Vec<u8>, CryptoError> {
        match &self.cipher {
            Cipher::XChaCha20(cipher) => {
                let nonce = make_nonce_from_suffix::<24>(nonce_suffix);
                cipher
                    .decrypt(
                        XNonce::from_slice(&nonce),
                        Payload {
                            msg: encrypted_payload,
                            aad: header,
                        },
                    )
                    .map_err(|_| CryptoError::DecryptFailed)
            }
            Cipher::Aes256(cipher) => {
                let nonce = make_nonce_from_suffix::<12>(nonce_suffix);
                cipher
                    .decrypt(
                        AesNonce::from_slice(&nonce),
                        Payload {
                            msg: encrypted_payload,
                            aad: header,
                        },
                    )
                    .map_err(|_| CryptoError::DecryptFailed)
            }
        }
    }

    pub fn decrypt_rtp(&mut self, packet: &RtpPacket) -> Result<Vec<u8>, CryptoError> {
        self.decrypt_packet(
            packet.header.as_slice(),
            packet.encrypted_payload.as_slice(),
            &packet.nonce_suffix,
        )
    }
}

fn make_xchacha_nonce(value: u32) -> [u8; 24] {
    let mut nonce = [0u8; 24];
    nonce[..4].copy_from_slice(&value.to_le_bytes());
    nonce
}

fn make_aes_nonce(value: u32) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..4].copy_from_slice(&value.to_le_bytes());
    nonce
}

fn make_nonce_from_suffix<const N: usize>(suffix: &[u8; 4]) -> [u8; N] {
    let mut nonce = [0u8; N];
    nonce[..4].copy_from_slice(suffix);
    nonce
}
