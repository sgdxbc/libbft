use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Buf as _;

use crate::{
    crypto::{DigestScheme, SigScheme},
    narwhal::core::{
        BlockHash, NarwhalBlock, NarwhalCert, NarwhalMessage, NarwhalParams, ReplicaIndex,
        RoundNum, Sig,
    },
};

pub trait NarwhalCryptoContext: SigScheme + DigestScheme {}
impl<C: SigScheme + DigestScheme> NarwhalCryptoContext for C {}

pub struct NarwhalWorker<C> {
    context: C,
    params: NarwhalParams,
}

impl<C> NarwhalWorker<C> {
    pub fn new(context: C, params: NarwhalParams) -> Self {
        Self { context, params }
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
enum NarwhalNetworkMessage {
    Block(Sig), // block data piggybacked
    Ack(BlockHash, RoundNum, ReplicaIndex, Sig),
    Cert(NarwhalCert),
}

impl<C: NarwhalCryptoContext> NarwhalWorker<C> {
    pub fn egress_block(&self, block: NarwhalBlock) -> (Vec<u8>, BlockHash) {
        let block_bytes = borsh::to_vec(&block).unwrap();
        let block_hash = self.context.digest(&block_bytes);
        let sig = self.context.sign(&block_bytes);
        let message_bytes = borsh::to_vec(&NarwhalNetworkMessage::Block(sig)).unwrap();
        (
            [
                &(message_bytes.len() as u32).to_le_bytes()[..],
                &message_bytes,
                &block_bytes,
            ]
            .concat(),
            block_hash,
        )
    }

    pub fn sign_ack(
        &self,
        block_hash: &BlockHash,
        round: RoundNum,
        replica_index: ReplicaIndex,
    ) -> Sig {
        let ack_bytes = borsh::to_vec(&(block_hash, round, replica_index)).unwrap();
        self.context.sign(&ack_bytes)
    }

    pub fn egress_ack(
        &self,
        block_hash: BlockHash,
        round: RoundNum,
        replica_index: ReplicaIndex,
    ) -> Vec<u8> {
        let sig = self.sign_ack(&block_hash, round, replica_index);
        let message_bytes = borsh::to_vec(&NarwhalNetworkMessage::Ack(
            block_hash,
            round,
            replica_index,
            sig,
        ))
        .unwrap();
        [
            &(message_bytes.len() as u32).to_le_bytes()[..],
            &message_bytes,
        ]
        .concat()
    }

    pub fn egress_cert(&self, cert: NarwhalCert) -> Vec<u8> {
        let message_bytes = borsh::to_vec(&NarwhalNetworkMessage::Cert(cert)).unwrap();
        [
            &(message_bytes.len() as u32).to_le_bytes()[..],
            &message_bytes,
        ]
        .concat()
    }

    pub fn ingress(&self, mut bytes: &[u8]) -> anyhow::Result<NarwhalMessage> {
        let message_len = bytes
            .try_get_u32_le()
            .context("Ingress message too short")? as usize;
        let message_bytes = bytes
            .split_off(..message_len)
            .context("Ingress message too short")?;
        let message = match borsh::from_slice(message_bytes)? {
            NarwhalNetworkMessage::Block(sig) => {
                let block = borsh::from_slice::<NarwhalBlock>(bytes)?;
                self.context.verify(bytes, &sig, block.replica_index)?;
                let block_hash = self.context.digest(bytes);
                NarwhalMessage::Block(block_hash, block)
            }
            NarwhalNetworkMessage::Ack(block_hash, round, replica_index, sig) => {
                let ack_bytes = borsh::to_vec(&(&block_hash, round, replica_index)).unwrap();
                self.context.verify(&ack_bytes, &sig, replica_index)?;
                NarwhalMessage::Ack(block_hash, replica_index, sig)
            }
            NarwhalNetworkMessage::Cert(cert) => {
                if cert.round != Default::default() {
                    anyhow::ensure!(
                        cert.sigs.len() >= self.params.quorum_size(),
                        "Not enough signatures in cert"
                    );
                }
                let cert_bytes =
                    borsh::to_vec(&(&cert.block_hash, cert.round, cert.replica_index)).unwrap();
                for (&signer, sig) in &cert.sigs {
                    self.context.verify(&cert_bytes, sig, signer)?;
                }
                NarwhalMessage::Cert(cert)
            }
        };
        Ok(message)
    }
}
