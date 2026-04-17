use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Buf;

use crate::{
    crypto::{DigestScheme, SigBytes, SigScheme},
    pbft::core::Checkpoint,
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
enum PbftVerifiableData {
    PrePrepare(PrePrepare),
    Prepare(Prepare),
    Commit(Commit),
    Checkpoint(Checkpoint),
}

impl<C> PbftWorker<C> {
    pub fn new(context: C, params: PbftParams) -> Self {
        Self { context, params }
    }
}

impl<C: PbftCryptoContext> PbftWorker<C> {
    pub fn egress(&self, message: &mut PbftMessage) -> (Vec<u8>, SigBytes) {
        let mut piggyback_bytes = vec![];
        let verifiable_data = match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                piggyback_bytes = borsh::to_vec(requests).unwrap();
                pre_prepare.digest = self.context.digest(&piggyback_bytes);
                PbftVerifiableData::PrePrepare(pre_prepare.clone())
            }
            PbftMessage::Prepare(prepare) => PbftVerifiableData::Prepare(prepare.clone()),
            PbftMessage::Commit(commit) => PbftVerifiableData::Commit(commit.clone()),
            PbftMessage::Checkpoint(checkpoint) => {
                PbftVerifiableData::Checkpoint(checkpoint.clone())
            }
        };
        let data_bytes = borsh::to_vec(&verifiable_data).unwrap();
        let sig = self.context.sign(&borsh::to_vec(&verifiable_data).unwrap());
        let bytes = [
            &(data_bytes.len() as u32).to_le_bytes()[..],
            &data_bytes,
            &piggyback_bytes,
        ]
        .concat();
        (bytes, sig)
    }

    pub fn ingress(&self, mut bytes: &[u8], sig: &SigBytes) -> anyhow::Result<PbftMessage> {
        let data_len = bytes.try_get_u32_le()? as usize;
        let (data_bytes, piggyback_bytes) = bytes
            .split_at_checked(data_len)
            .context("Invalid message: data length prefix exceeds actual length")?;
        let data =
            borsh::from_slice(data_bytes).context("Failed to deserialize verifiable data")?;
        if !matches!(data, PbftVerifiableData::PrePrepare(_)) {
            anyhow::ensure!(
                piggyback_bytes.is_empty(),
                "Unexpected piggyback data for non-PrePrepare message"
            );
        }
        let message = match data {
            PbftVerifiableData::PrePrepare(pre_prepare) => {
                let expected_digest = self.context.digest(piggyback_bytes);
                anyhow::ensure!(
                    pre_prepare.digest == expected_digest,
                    "Invalid PrePrepare digest: expected {:?}, got {:?}",
                    expected_digest,
                    pre_prepare.digest
                );
                self.context
                    .verify(bytes, sig, self.params.view_leader(pre_prepare.view_num))
                    .context("Failed to verify PrePrepare signature")?;
                let requests =
                    borsh::from_slice(piggyback_bytes).context("Failed to deserialize requests")?;
                PbftMessage::PrePrepare(pre_prepare, requests)
            }
            PbftVerifiableData::Prepare(prepare) => {
                self.context
                    .verify(bytes, sig, prepare.replica_index)
                    .context("Failed to verify Prepare signature")?;
                PbftMessage::Prepare(prepare)
            }
            PbftVerifiableData::Commit(commit) => {
                self.context
                    .verify(bytes, sig, commit.replica_index)
                    .context("Failed to verify Commit signature")?;
                PbftMessage::Commit(commit)
            }
            PbftVerifiableData::Checkpoint(checkpoint) => {
                self.context
                    .verify(bytes, sig, checkpoint.replica_index)
                    .context("Failed to verify Checkpoint signature")?;
                PbftMessage::Checkpoint(checkpoint)
            }
        };
        Ok(message)
    }
}
