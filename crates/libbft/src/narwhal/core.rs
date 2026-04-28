use std::{collections::HashMap, mem::take};

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::time::Instant;
use tracing::warn;

pub type BlockHash = crate::crypto::Digest;
pub type RoundNum = u64;
pub type ReplicaIndex = crate::types::ReplicaIndex;
pub type Sig = crate::crypto::SigBytes;

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
    fn broadcast_block(&mut self, block: NarwhalBlock) -> impl Future<Output = Option<BlockHash>>;

    // no need to sign. no loopback
    fn broadcast_cert(&mut self, cert: NarwhalCert) -> impl Future<Output = ()>;

    // need to sign. may be loopback if signer == replica_index
    fn ack(
        &mut self,
        block_hash: BlockHash,
        round: RoundNum,
        replica_index: ReplicaIndex,
        signer: ReplicaIndex,
    ) -> impl Future<Output = ()>;
}

// common abstraction for "zero-cost" consensus like Tusk and Bullshark
// the "using narwhal for consensus" (section 3.2) mode is not explicitly
// supported but probably feasible with an EventSender as consensus state that
// emit certified blocks to an external consensus protocol
pub trait ConsensusProtocol {
    type State;

    // called when a new block is certified and added to the DAG
    // the most useful states are `dag` and `store`
    fn on_certified(&mut self, block_hash: &BlockHash) -> impl Future<Output = ()>;
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
    round: RoundNum,
    pub replica_index: ReplicaIndex,
    certs: Vec<NarwhalCert>,
    txns: Vec<NarwhalTxn>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct NarwhalCert {
    pub round: RoundNum,
    pub replica_index: ReplicaIndex,
    pub block_hash: BlockHash,
    // this seems to be a perfect use case of threshold schemes, but the origin paper explicitly
    // opted for a simple signature vector
    pub sigs: HashMap<ReplicaIndex, Sig>,
}

pub struct NarwhalCore<Context, Consensus>
where
    Self: ConsensusProtocol,
{
    pub context: Context,
    config: NarwhalCoreConfig<Consensus>,
    consensus_state: <Self as ConsensusProtocol>::State,

    store: HashMap<BlockHash, NarwhalBlock>,
    dag: HashMap<RoundNum, HashMap<ReplicaIndex, NarwhalCert>>,
    pending_txns: Vec<NarwhalTxn>,
    round: RoundNum,
    received_blocks: HashMap<ReplicaIndex, BlockHash>, // of current round
    ack_sigs: HashMap<ReplicaIndex, Sig>,              // of current round
}

impl<Context, Consensus> NarwhalCore<Context, Consensus>
where
    Self: ConsensusProtocol,
{
    pub fn new_with_consensus_state(
        context: Context,
        config: NarwhalCoreConfig<Consensus>,
        consensus_state: <Self as ConsensusProtocol>::State,
    ) -> Self {
        let (store, dag, pending_txns, round, received_blocks, ack_sigs) = Default::default();
        Self {
            context,
            config,
            consensus_state,
            store,
            dag,
            pending_txns,
            round,
            received_blocks,
            ack_sigs,
        }
    }
}

impl NarwhalParams {
    pub fn quorum_size(&self) -> usize {
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

    pub async fn on_txn(&mut self, txn: NarwhalTxn) {
        self.pending_txns.push(txn);
    }

    pub async fn on_message(&mut self, message: NarwhalMessage) {
        match message {
            NarwhalMessage::Block(block_hash, block) => self.handle_block(block_hash, block).await,
            NarwhalMessage::Ack(block_hash, signer, sig) => {
                self.handle_ack(block_hash, signer, sig).await
            }
            NarwhalMessage::Cert(cert) => self.handle_cert(cert).await,
        }
    }

    pub async fn on_tick(&mut self, now: Instant) {
        let _ = now;
    }

    async fn create_block(&mut self) {
        let block = NarwhalBlock {
            round: self.round,
            replica_index: self.config.replica_index,
            certs: if self.round == Default::default() {
                Default::default()
            } else {
                self.dag[&(self.round - 1)].values().cloned().collect()
            },
            txns: self
                .pending_txns
                .drain(..self.pending_txns.len().min(self.config.max_block_size))
                .collect(),
        };
        if let Some(block_hash) = self.context.broadcast_block(block.clone()).await {
            self.store.insert(block_hash, block);
        }
    }

    async fn handle_block(&mut self, block_hash: BlockHash, block: NarwhalBlock) {
        assert!(block.replica_index != self.config.replica_index);
        if block.round != self.round {
            return;
        }
        if let Some(existing_block_hash) = self.received_blocks.get(&block.replica_index)
            && existing_block_hash != &block_hash
        {
            warn!(
                self.config.replica_index,
                "Received duplicate block from replica {} for round {}: {existing_block_hash:?} vs {block_hash:?}",
                block.replica_index,
                block.round
            );
            return;
        }
        self.context
            .ack(
                block_hash.clone(),
                block.round,
                block.replica_index,
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
            .get(&self.round)
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
        if block.round != self.round {
            return;
        }
        self.ack_sigs.insert(signer, sig);
        if self.ack_sigs.len() >= self.config.params.quorum_size() {
            let cert = NarwhalCert {
                round: self.round,
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
            .get(&cert.round)
            .is_some_and(|round| round.contains_key(&cert.replica_index))
        {
            return;
        }
        self.insert_cert(cert).await;
    }

    async fn insert_cert(&mut self, cert: NarwhalCert) {
        let block_hash = cert.block_hash.clone();
        let round_certs = self.dag.entry(cert.round).or_default();
        round_certs.insert(cert.replica_index, cert);
        assert!(round_certs.len() <= self.config.params.quorum_size());
        if round_certs.len() == self.config.params.quorum_size() {
            self.round += 1;
            self.received_blocks.clear();
            self.ack_sigs.clear();
            self.create_block().await;
        }
        self.on_certified(&block_hash).await;
    }
}

pub struct Bullshark;

#[derive(Default)]
pub struct BullsharkState {
    last_ordered_round: RoundNum,
}

impl<C: NarwhalCoreContext> NarwhalCore<C, Bullshark> {
    pub fn new(context: C, config: NarwhalCoreConfig<Bullshark>) -> Self {
        Self::new_with_consensus_state(context, config, Default::default())
    }
}

impl<C: NarwhalCoreContext> ConsensusProtocol for NarwhalCore<C, Bullshark> {
    type State = BullsharkState;

    async fn on_certified(&mut self, block_hash: &BlockHash) {
        let block = &self.store[block_hash];
        if block.round == 0 || !block.round.is_multiple_of(2) {
            return;
        }
        let anchor_round = block.round - 2;
        let Some(anchor) = self.get_anchor(anchor_round) else {
            return;
        };
        if self.dag[&(anchor_round + 1)]
            .values()
            .filter(|cert| {
                self.store[&cert.block_hash]
                    .certs
                    .iter()
                    .any(|c| &c.block_hash == anchor)
            })
            .count()
            > self.config.params.num_faulty_replicas
        {
            self.order_anchors(anchor.clone()).await;
        }
    }
}

impl<C: NarwhalCoreContext> NarwhalCore<C, Bullshark> {
    fn get_anchor(&self, round: RoundNum) -> Option<&BlockHash> {
        assert!(round.is_multiple_of(2));
        let anchor = &self.dag[&round]
            .get(&((round / 2 % self.config.params.num_replicas as RoundNum) as _))?
            .block_hash;
        Some(anchor)
    }

    async fn order_anchors(&mut self, anchor: BlockHash) {
        let mut prev_round = self.store[&anchor].round - 2;
        let mut anchors = vec![anchor];
        while prev_round > self.consensus_state.last_ordered_round {
            let Some(prev_anchor) = self.get_anchor(prev_round) else {
                continue;
            };
            if self.store[anchors.last().unwrap()]
                .certs
                .iter()
                .any(|cert| {
                    self.store[&cert.block_hash]
                        .certs
                        .iter()
                        .any(|c| &c.block_hash == prev_anchor)
                })
            {
                anchors.push(prev_anchor.clone());
            }
            prev_round -= 2;
        }
        self.consensus_state.last_ordered_round = self.store[anchors.first().unwrap()].round;

        while let Some(anchor) = anchors.pop() {
            self.deliver_blocks(anchor).await;
        }
    }

    async fn deliver_blocks(&mut self, block_hash: BlockHash) {
        // it is safe to do inline garbage collection here because we will not track down the out
        // links of this block for a second time, and if we are led to this very block again via
        // some other path, the recursion should stop here anyway
        let Some(block) = self.store.remove(&block_hash) else {
            return;
        };
        for cert in &block.certs {
            Box::pin(self.deliver_blocks(cert.block_hash.clone())).await;
        }
        self.context.deliver(block).await;
    }
}
