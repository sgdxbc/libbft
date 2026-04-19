use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Buf;

use crate::{
    crypto::{DigestScheme, SigBytes, SigScheme},
    pbft::{
        PbftRequest,
        core::{Checkpoint, PbftSyncMessage},
        events::HandleMessageValue,
    },
};

use super::core::{Commit, PbftMessage, PbftParams, PrePrepare, Prepare};

pub trait PbftCryptoContext: SigScheme + DigestScheme {}
impl<C: SigScheme + DigestScheme> PbftCryptoContext for C {}

// both ingress and egress (happen to) need these same states
pub struct PbftWorker<C> {
    context: C,
    params: PbftParams,
}

// verifiable data covered by signatures
#[derive(Debug, BorshSerialize, BorshDeserialize)]
enum PbftNetworkMessage {
    PrePrepare(PrePrepare), // requests piggybacked to avoid double serialization
    Prepare(Prepare),
    Commit(Commit),
    Checkpoint(Checkpoint),

    Request(PbftRequest),
}

impl<C> PbftWorker<C> {
    pub fn new(context: C, params: PbftParams) -> Self {
        Self { context, params }
    }
}

impl<C: PbftCryptoContext> PbftWorker<C> {
    pub fn egress_broadcast(&self, message: &mut PbftMessage) -> (Vec<u8>, SigBytes) {
        let mut piggyback_bytes = vec![];
        let network_message = match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                piggyback_bytes = borsh::to_vec(requests).unwrap();
                pre_prepare.digest = self.context.digest(&piggyback_bytes);
                PbftNetworkMessage::PrePrepare(pre_prepare.clone())
            }
            PbftMessage::Prepare(prepare) => PbftNetworkMessage::Prepare(prepare.clone()),
            PbftMessage::Commit(commit) => PbftNetworkMessage::Commit(commit.clone()),
            PbftMessage::Checkpoint(checkpoint) => {
                PbftNetworkMessage::Checkpoint(checkpoint.clone())
            }
        };
        let message_bytes = borsh::to_vec(&network_message).unwrap();
        let SigBytes(sig_bytes) = self.context.sign(&message_bytes);
        let bytes = [
            &(message_bytes.len() as u32).to_le_bytes()[..],
            &message_bytes,
            &sig_bytes,
            &piggyback_bytes,
        ]
        .concat();
        (bytes, SigBytes(sig_bytes))
    }

    pub fn egress_sync(&self, message: PbftSyncMessage) -> Vec<u8> {
        let PbftSyncMessage(request) = message;
        let message_bytes = borsh::to_vec(&PbftNetworkMessage::Request(request)).unwrap();
        [
            &(message_bytes.len() as u32).to_le_bytes()[..],
            &message_bytes,
        ]
        .concat()
    }

    pub fn ingress(&self, mut bytes: &[u8]) -> anyhow::Result<HandleMessageValue> {
        let message_len = bytes.try_get_u32_le()? as usize;
        let message_bytes = bytes
            .split_off(..message_len)
            .context("Invalid message: data length prefix exceeds actual length")?;
        let message =
            borsh::from_slice(message_bytes).context("Failed to deserialize verifiable data")?;
        let value = match message {
            PbftNetworkMessage::PrePrepare(pre_prepare) => {
                let sig = SigBytes(
                    bytes
                        .split_off(..C::SIG_BYTES_LEN)
                        .context("Message too short for signature")?
                        .into(),
                );
                let expected_digest = self.context.digest(bytes);
                anyhow::ensure!(
                    pre_prepare.digest == expected_digest,
                    "Invalid PrePrepare digest: expected {expected_digest:?}, got {:?}",
                    pre_prepare.digest
                );
                let requests =
                    borsh::from_slice(bytes).context("Failed to deserialize requests")?;
                self.context
                    .verify(
                        message_bytes,
                        &sig,
                        self.params.view_leader(pre_prepare.view_num),
                    )
                    .context("Failed to verify PrePrepare signature")?;
                HandleMessageValue::Signed(PbftMessage::PrePrepare(pre_prepare, requests), sig)
            }
            PbftNetworkMessage::Prepare(prepare) => {
                let sig = SigBytes(
                    bytes
                        .split_off(..C::SIG_BYTES_LEN)
                        .context("Message too short for signature")?
                        .into(),
                );
                self.context
                    .verify(message_bytes, &sig, prepare.replica_index)
                    .context("Failed to verify Prepare signature")?;
                HandleMessageValue::Signed(PbftMessage::Prepare(prepare), sig)
            }
            PbftNetworkMessage::Commit(commit) => {
                let sig = SigBytes(
                    bytes
                        .split_off(..C::SIG_BYTES_LEN)
                        .context("Message too short for signature")?
                        .into(),
                );
                self.context
                    .verify(message_bytes, &sig, commit.replica_index)
                    .context("Failed to verify Commit signature")?;
                HandleMessageValue::Signed(PbftMessage::Commit(commit), sig)
            }
            PbftNetworkMessage::Checkpoint(checkpoint) => {
                let sig = SigBytes(
                    bytes
                        .split_off(..C::SIG_BYTES_LEN)
                        .context("Message too short for signature")?
                        .into(),
                );
                self.context
                    .verify(message_bytes, &sig, checkpoint.replica_index)
                    .context("Failed to verify Checkpoint signature")?;
                HandleMessageValue::Signed(PbftMessage::Checkpoint(checkpoint), sig)
            }

            PbftNetworkMessage::Request(request) => {
                HandleMessageValue::Sync(PbftSyncMessage(request))
            }
        };
        Ok(value)
    }
}
