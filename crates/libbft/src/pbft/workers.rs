use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};

use crate::crypto::CryptoKit;

use super::core::{Commit, PbftMessage, PbftParams, PrePrepare, Prepare};

pub trait PbftCryptoContext: CryptoKit + Send {}
impl<C: CryptoKit + Send> PbftCryptoContext for C {}

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
}

impl<C: PbftCryptoContext> PbftWorker<C> {
    pub fn new(context: C, params: PbftParams) -> Self {
        Self { context, params }
    }

    pub fn ingress(&self, message: &mut PbftMessage) -> (Vec<u8>, C::Sig, Vec<u8>) {
        let mut piggyback_data = vec![];
        let verifiable_data = match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                piggyback_data = borsh::to_vec(requests).unwrap();
                pre_prepare.digest = self.context.digest(&piggyback_data);
                PbftVerifiableData::PrePrepare(pre_prepare.clone())
            }
            PbftMessage::Prepare(prepare) => PbftVerifiableData::Prepare(prepare.clone()),
            PbftMessage::Commit(commit) => PbftVerifiableData::Commit(commit.clone()),
        };
        let bytes = borsh::to_vec(&verifiable_data).unwrap();
        let sig = self.context.sign(&borsh::to_vec(&verifiable_data).unwrap());
        (bytes, sig, piggyback_data)
    }

    pub fn egress(
        &self,
        bytes: &[u8],
        sig: &C::Sig,
        piggyback_data: &[u8],
    ) -> anyhow::Result<PbftMessage> {
        let data = borsh::from_slice(bytes).context("Failed to deserialize verifiable data")?;
        if !matches!(data, PbftVerifiableData::PrePrepare(_)) {
            anyhow::ensure!(
                piggyback_data.is_empty(),
                "Unexpected piggyback data for non-PrePrepare message"
            );
        }
        let message = match data {
            PbftVerifiableData::PrePrepare(pre_prepare) => {
                let expected_digest = self.context.digest(piggyback_data);
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
                    borsh::from_slice(piggyback_data).context("Failed to deserialize requests")?;
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
        };
        Ok(message)
    }
}
