use std::{
    collections::{HashMap, hash_map::Entry},
    mem::take,
};

use borsh::{BorshDeserialize, BorshSerialize};
use tracing::warn;

pub type BlockHash = crate::crypto::Digest;
pub type RoundNum = u64;
pub type ReplicaIndex = crate::types::ReplicaIndex;
type Dag = HashMap<RoundNum, HashMap<ReplicaIndex, NarwhalCert>>;
type BlockStore = HashMap<BlockHash, NarwhalBlock>;
type Sig = crate::crypto::SigBytes;

pub struct NarwhalParams {
    pub num_replicas: usize,
    pub num_faulty_replicas: usize,
}

pub struct NarwhalCoreConfig<C> {
    pub consensus: C,
    pub params: NarwhalParams,
    pub replica_index: ReplicaIndex,

    pub max_block_size: usize,
}

pub trait NarwhalCoreContext {
    fn deliver(&mut self, block: NarwhalBlock) -> impl Future<Output = ()>;

    // need to sign. no loopback
    fn broadcast_block(&mut self, block: NarwhalBlock) -> impl Future<Output = BlockHash>;

    // no need to sign. no loopback
    fn broadcast_cert(&mut self, cert: NarwhalCert) -> impl Future<Output = ()>;

    // need to sign. may be loopback if signer == replica_index
    fn ack(
        &mut self,
        round_num: RoundNum,
        replica_index: ReplicaIndex,
        block_hash: BlockHash,
        signer: ReplicaIndex,
    ) -> impl Future<Output = ()>;
}

pub trait ConsensusProtocol {
    // called when a new block is certified and added to the DAG
    // the returned block hashes will be committed and removed from the DAG and the store
    // the most useful states are `dag` and `store`
    fn on_certified(&mut self, block_hash: &BlockHash) -> Vec<BlockHash>;
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct NarwhalTxn(pub Vec<u8>);

pub enum NarwhalMessage {
    Block(BlockHash, NarwhalBlock),
    Ack(BlockHash, ReplicaIndex, Sig), // digest, signer, sig
    Cert(NarwhalCert),
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct NarwhalBlock {
    round_num: RoundNum,
    replica_index: ReplicaIndex,
    certs: Vec<NarwhalCert>,
    txns: Vec<NarwhalTxn>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct NarwhalCert {
    round_num: RoundNum,
    replica_index: ReplicaIndex,
    block_hash: BlockHash,
    // this seems to be a perfect use case of threshold schemes, but the origin paper explicitly
    // opted for a simple signature vector
    sigs: HashMap<ReplicaIndex, Sig>,
}

pub struct NarwhalCore<Context, Consensus> {
    context: Context,
    config: NarwhalCoreConfig<Consensus>,

    store: BlockStore,
    dag: Dag,
    pending_txns: Vec<NarwhalTxn>,
    round_num: RoundNum,
    received_blocks: HashMap<ReplicaIndex, BlockHash>, // of current round
    ack_sigs: HashMap<ReplicaIndex, Sig>,              // of current round
}

impl<Context, Consensus> NarwhalCore<Context, Consensus> {
    pub fn new(
        context: Context,
        consensus: Consensus,
        config: NarwhalCoreConfig<Consensus>,
    ) -> Self {
        let (store, dag, pending_txns, round_num, received_blocks, ack_sigs) = Default::default();
        Self {
            context,
            config,
            store,
            dag,
            pending_txns,
            round_num,
            received_blocks,
            ack_sigs,
        }
    }
}

impl NarwhalParams {
    fn quorum_size(&self) -> usize {
        self.num_replicas - self.num_faulty_replicas
    }
}

impl<Context: NarwhalCoreContext, Consensus> NarwhalCore<Context, Consensus>
where
    Self: ConsensusProtocol,
{
    pub async fn on_init(&mut self) {
        self.create_block().await;
    }

    pub async fn on_message(&mut self, msg: NarwhalMessage) {
        //
    }

    async fn create_block(&mut self) {
        let block = NarwhalBlock {
            round_num: self.round_num,
            replica_index: self.config.replica_index,
            certs: if self.round_num == 0 {
                Default::default()
            } else {
                self.dag[&(self.round_num - 1)].values().cloned().collect()
            },
            txns: self
                .pending_txns
                .drain(..self.pending_txns.len().min(self.config.max_block_size))
                .collect(),
        };
        let block_hash = self.context.broadcast_block(block.clone()).await;
        self.store.insert(block_hash, block);
    }

    async fn handle_block(&mut self, block_hash: BlockHash, block: NarwhalBlock) {
        assert!(block.replica_index != self.config.replica_index);
        if block.round_num != self.round_num {
            return;
        }
        if let Some(existing_block_hash) = self.received_blocks.get(&block.replica_index)
            && existing_block_hash != &block_hash
        {
            warn!(
                self.config.replica_index,
                "Received duplicate block from replica {} for round {}: {existing_block_hash:?} vs {block_hash:?}",
                block.replica_index,
                block.round_num
            );
            return;
        }
        self.context
            .ack(
                block.round_num,
                block.replica_index,
                block_hash.clone(),
                self.config.replica_index,
            )
            .await;
        self.received_blocks
            .insert(block.replica_index, block_hash.clone());
        self.store.insert(block_hash, block);
    }

    async fn handle_ack(&mut self, block_hash: BlockHash, signer: ReplicaIndex, sig: Sig) {
        if self
            .dag
            .get(&self.round_num)
            .is_some_and(|round| round.contains_key(&self.config.replica_index))
        {
            return;
        }
        let Some(block) = self.store.get(&block_hash) else {
            warn!(
                self.config.replica_index,
                "Received ack for unknown block {block_hash:?} from replica {signer}"
            );
            return;
        };
        if block.replica_index != self.config.replica_index {
            warn!(
                self.config.replica_index,
                "Ack for block {block_hash:?} from replica {signer}, but the block was produced by replica {}",
                block.replica_index
            );
            return;
        }
        if block.round_num != self.round_num {
            return;
        }
        self.ack_sigs.insert(signer, sig);
        if self.ack_sigs.len() >= self.config.params.quorum_size() {
            let cert = NarwhalCert {
                round_num: self.round_num,
                replica_index: self.config.replica_index,
                block_hash,
                sigs: take(&mut self.ack_sigs),
            };
            self.context.broadcast_cert(cert.clone()).await;
            self.insert_cert(cert).await;
        }
    }

    async fn handle_cert(&mut self, cert: NarwhalCert) {
        if self
            .dag
            .get(&cert.round_num)
            .is_some_and(|round| round.contains_key(&cert.replica_index))
        {
            return;
        }
        self.insert_cert(cert).await;
    }

    async fn insert_cert(&mut self, cert: NarwhalCert) {
        let block_hash = cert.block_hash.clone();
        let round_certs = self.dag.entry(cert.round_num).or_default();
        round_certs.insert(cert.replica_index, cert);
        assert!(round_certs.len() <= self.config.params.quorum_size());
        if round_certs.len() == self.config.params.quorum_size() {
            self.round_num += 1;
            self.create_block().await;
        }
        for committed_block_hash in self.on_certified(&block_hash) {
            let block = self.store.remove(&committed_block_hash).unwrap();
            let Entry::Occupied(mut entry) = self.dag.entry(block.round_num) else {
                unreachable!()
            };
            entry.get_mut().remove(&block.replica_index).unwrap();
            if entry.get().is_empty() {
                entry.remove();
            }
            self.context.deliver(block).await;
        }
    }
}
