use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Buf;
use tracing::instrument;

use crate::{
    crypto::{DigestScheme, PartialSigBytes, ThresholdSigScheme},
    hotstuff::core::{BlockDigest, HotStuffMessage, HotStuffNode, ReplicaIndex},
};

pub trait HotStuffCryptoContext: ThresholdSigScheme + DigestScheme {}
impl<C: ThresholdSigScheme + DigestScheme> HotStuffCryptoContext for C {}

pub struct HotStuffWorker<C> {
    context: C,
}

impl<C> HotStuffWorker<C> {
    pub fn new(context: C) -> Self {
        Self { context }
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
enum HotStuffNetworkMessage {
    Generic, // node data piggybacked
    Vote(
        BlockDigest,
        PartialSigBytes,
        ReplicaIndex, // not covered by sig
    ),
}

impl<C: HotStuffCryptoContext> HotStuffWorker<C> {
    #[instrument(skip(self))]
    pub fn egress_generic(&self, node: &HotStuffNode) -> (Vec<u8>, BlockDigest) {
        let message_bytes = borsh::to_vec(&HotStuffNetworkMessage::Generic).unwrap();
        let node_bytes = borsh::to_vec(&node).unwrap();
        (
            [
                &(message_bytes.len() as u32).to_le_bytes()[..],
                &message_bytes,
                &node_bytes,
            ]
            .concat(),
            self.context.digest(&node_bytes),
        )
    }

    // for loopback
    pub fn sign_vote(&self, block: &BlockDigest) -> PartialSigBytes {
        self.context.partial_sign(&block.0)
    }

    #[instrument(skip(self))]
    pub fn egress_vote(&self, block: BlockDigest, replica_index: ReplicaIndex) -> Vec<u8> {
        let partial_sig = self.sign_vote(&block);
        let bytes = borsh::to_vec(&HotStuffNetworkMessage::Vote(
            block,
            partial_sig,
            replica_index,
        ))
        .unwrap();
        [&(bytes.len() as u32).to_le_bytes()[..], &bytes].concat()
    }

    pub fn ingress(&self, mut bytes: &[u8]) -> anyhow::Result<HotStuffMessage> {
        let message_len = bytes.try_get_u32_le()? as usize;
        let message_bytes = bytes
            .split_off(..message_len)
            .context("Ingress message too short")?;
        let message = match borsh::from_slice(message_bytes)? {
            HotStuffNetworkMessage::Generic => {
                let node = borsh::from_slice::<HotStuffNode>(bytes)?;
                self.context
                    .verify(&node.justify.block.0, &node.justify.sig)?;
                let block = self.context.digest(&borsh::to_vec(&node).unwrap());
                HotStuffMessage::Generic(block, node)
            }
            HotStuffNetworkMessage::Vote(block, partial_sig, replica_index) => {
                self.context
                    .partial_verify(&block.0, &partial_sig, replica_index)?;
                HotStuffMessage::Vote(block, replica_index, partial_sig)
            }
        };
        Ok(message)
    }
}
