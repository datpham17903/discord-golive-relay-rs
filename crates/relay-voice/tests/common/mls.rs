//! Plays the voice gateway's MLS external sender for the DAVE tests (the role of libdave's
//! `cpp/test/external_sender.cpp`, as in davey's own tests).

use openmls::prelude::ExternalSender as MlsExternalSender;
use openmls::prelude::tls_codec::{Deserialize, Serialize};
use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;

pub struct FakeExternalSender {
    provider: OpenMlsRustCrypto,
    group_id: GroupId,
    signer: SignatureKeyPair,
    external_sender: MlsExternalSender,
}

impl FakeExternalSender {
    pub fn new(group_id: u64) -> Self {
        let ciphersuite = Ciphersuite::MLS_128_DHKEMP256_AES128GCM_SHA256_P256;
        let signer = SignatureKeyPair::new(ciphersuite.signature_algorithm()).unwrap();
        let external_sender = MlsExternalSender::new(
            signer.public().into(),
            BasicCredential::new(vec![0x00, 0x01, 0x01, 0x00]).into(),
        );
        Self {
            provider: OpenMlsRustCrypto::default(),
            group_id: GroupId::from_slice(&group_id.to_be_bytes()),
            signer,
            external_sender,
        }
    }

    /// Payload of op 25.
    pub fn marshalled(&self) -> Vec<u8> {
        self.external_sender.tls_serialize_detached().unwrap()
    }

    /// Payload of an append op 27 (after the optype byte): proposes adding the owner of `key_package`.
    pub fn propose_add(&self, epoch: u64, key_package: &[u8]) -> Vec<u8> {
        let key_package = KeyPackageIn::tls_deserialize_exact_bytes(key_package)
            .unwrap()
            .validate(self.provider.crypto(), ProtocolVersion::Mls10)
            .unwrap();
        let message = ExternalProposal::new_add::<OpenMlsRustCrypto>(
            key_package,
            self.group_id.clone(),
            GroupEpoch::from(epoch),
            &self.signer,
            SenderExtensionIndex::new(0),
        )
        .unwrap();
        VLBytes::new(message.tls_serialize_detached().unwrap())
            .tls_serialize_detached()
            .unwrap()
    }
}

/// Splits an op 28 payload (`commit` then optional `welcome`) at the end of the commit message.
pub fn split_commit_welcome(payload: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut rest = payload;
    MlsMessageIn::tls_deserialize(&mut rest).expect("commit is an MLS message");
    let commit_len = payload.len() - rest.len();
    (payload[..commit_len].to_vec(), rest.to_vec())
}
