use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::time::Instant;
use tracing::{info, instrument, warn};

pub type ReplicaIndex = crate::types::ReplicaIndex;
pub type BlockDigest = crate::crypto::Digest; // we will call this `block`
type Height = u64;
type PartialSigBytes = crate::crypto::PartialSigBytes;
type SigBytes = crate::crypto::SigBytes;

pub struct HotStuffParams {
    pub num_replicas: usize,
    pub num_faulty_replicas: usize,
}

pub struct HotStuffCoreConfig {
    pub params: HotStuffParams,
    pub replica_index: ReplicaIndex,

    pub max_block_size: usize,
}

pub trait HotStuffCoreContext {
    fn deliver(&mut self, node: HotStuffNode) -> impl Future<Output = ()>;

    // broadcast `Generic` for `block`. the `Generic` will also loop back after `block` digest is
    // computed and the message is signed
    fn broadcast_generic(&mut self, node: HotStuffNode) -> impl Future<Output = ()>;

    fn send_vote(
        &mut self,
        to: ReplicaIndex,
        block: BlockDigest,
        replica_index: ReplicaIndex, // could be `to` for loopback voting
    ) -> impl Future<Output = ()>;

    // TODO send (unauthenticated) NewView
    // there should also be other sync messages for transferring node data, omitted in the paper

    // the ready `QuorumCert` will be passed into `on_quorum_cert`
    fn make_quorum_cert(
        &mut self,
        block: BlockDigest,
        sigs: impl IntoIterator<Item = (ReplicaIndex, PartialSigBytes)>,
    ) -> impl Future<Output = ()>;
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct HotStuffCommand(pub Vec<u8>);

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum HotStuffMessage {
    Generic(BlockDigest, HotStuffNode),
    Vote(BlockDigest, ReplicaIndex, PartialSigBytes),
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct HotStuffNode {
    pub parent: BlockDigest,
    pub commands: Vec<HotStuffCommand>,
    pub height: Height,
    pub justify: QuorumCert,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct QuorumCert {
    pub block: BlockDigest,
    pub sig: SigBytes,
}

pub struct HotStuffCore<C> {
    pub context: C,
    config: HotStuffCoreConfig,

    // simulated block storage. consider generalize to context interfaces if necessary
    nodes: HashMap<BlockDigest, HotStuffNode>,
    votes: HashMap<BlockDigest, HashMap<ReplicaIndex, PartialSigBytes>>,
    voted_height: Height,
    locked_block: BlockDigest,
    executed_block: BlockDigest,
    highest_quorum_cert: QuorumCert,

    // pacemaker states
    leaf_block: BlockDigest,
    leader: ReplicaIndex,
    pending_commands: Vec<HotStuffCommand>,
    empty_round_budget: u32,

    reorder_nodes: HashMap<BlockDigest, Vec<(BlockDigest, HotStuffNode)>>,
    reorder_quorum_certs: HashMap<BlockDigest, QuorumCert>,
}

pub const GENESIS: BlockDigest = crate::crypto::Digest(Vec::new());

impl<C> HotStuffCore<C> {
    pub fn new(context: C, config: HotStuffCoreConfig) -> Self {
        let (
            votes,
            voted_height,
            leader,
            pending_commands,
            empty_round_budget,
            reorder_nodes,
            reorder_quorum_certs,
        ) = Default::default();
        Self {
            context,
            config,
            nodes: [(
                GENESIS,
                // self-referential genesis can simplify the corner cases during initial blocks
                HotStuffNode {
                    parent: GENESIS,
                    commands: vec![],
                    height: 0,
                    justify: QuorumCert {
                        block: GENESIS,
                        sig: crate::crypto::SigBytes(Default::default()),
                    },
                },
            )]
            .into(),
            votes,
            voted_height,
            locked_block: GENESIS,
            executed_block: GENESIS,
            highest_quorum_cert: QuorumCert {
                block: GENESIS,
                sig: crate::crypto::SigBytes(Default::default()),
            },
            leaf_block: GENESIS,
            leader,
            pending_commands,
            empty_round_budget,
            reorder_nodes,
            reorder_quorum_certs,
        }
    }

    fn is_leader(&self) -> bool {
        self.config.replica_index == self.leader
    }

    fn extends(&self, block: &BlockDigest, ancestor: &BlockDigest) -> bool {
        let mut node = &self.nodes[block];
        while node.height > self.nodes[ancestor].height {
            if &node.parent == ancestor {
                return true;
            }
            node = &self.nodes[&node.parent];
        }
        false
    }
}

impl HotStuffParams {
    fn quorum_size(&self) -> usize {
        self.num_replicas - self.num_faulty_replicas
    }
}

impl<C: HotStuffCoreContext> HotStuffCore<C> {
    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_command(&mut self, command: HotStuffCommand) {
        self.pending_commands.push(command);
        if self.leaf_block == self.highest_quorum_cert.block {
            self.beat().await;
        }
    }

    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_message(&mut self, message: HotStuffMessage) {
        match message {
            HotStuffMessage::Generic(block, node) => self.handle_generic(block, node).await,
            HotStuffMessage::Vote(block, replica_index, partial_sig) => {
                self.handle_vote(block, replica_index, partial_sig).await
            }
        }
    }

    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_quorum_cert(&mut self, quorum_cert: QuorumCert) {
        if !self.nodes.contains_key(&quorum_cert.block) {
            let replaced = self
                .reorder_quorum_certs
                .insert(quorum_cert.block.clone(), quorum_cert);
            if let Some(quorum_cert) = replaced {
                warn!(
                    self.config.replica_index,
                    "duplicate quorum cert for block {:?}", quorum_cert.block
                );
            }
            return;
        }
        self.update_highest_quorum_cert(quorum_cert).await
    }

    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_tick(&mut self, now: Instant) {
        let _ = now;
        //
    }

    #[instrument(parent = None, skip(self), fields(replica_index = self.config.replica_index))]
    async fn propose(&mut self, commands: Vec<HotStuffCommand>) {
        assert!(self.is_leader());
        if commands.is_empty() {
            self.empty_round_budget -= 1;
        } else {
            self.empty_round_budget = 3;
        }
        let node = HotStuffNode {
            parent: self.leaf_block.clone(),
            commands,
            height: self.nodes[&self.leaf_block].height + 1,
            justify: self.highest_quorum_cert.clone(),
        };
        tracing::Span::current().record("height", node.height);
        self.context.broadcast_generic(node).await;
    }

    async fn handle_generic(&mut self, block: BlockDigest, node: HotStuffNode) {
        // TODO carefully reason about corner cases of very slow loopback generics
        if self.is_leader() {
            self.leaf_block = block.clone();
        }

        if self.nodes.contains_key(&block) {
            assert!(!self.is_leader());
            warn!(self.config.replica_index, "duplicate block {block:?}");
            return;
        }
        if !self.nodes.contains_key(&node.parent) {
            assert!(!self.is_leader());
            self.reorder_nodes
                .entry(node.parent.clone())
                .or_default()
                .push((block, node));
            return;
        }
        if !self.nodes.contains_key(&node.justify.block) {
            assert!(!self.is_leader());
            self.reorder_nodes
                .entry(node.justify.block.clone())
                .or_default()
                .push((block, node));
            return;
        }

        let node_height = node.height;
        let justify_block = node.justify.block.clone();
        self.nodes.insert(block.clone(), node);
        if node_height > self.voted_height
            && (self.extends(&block, &self.locked_block)
                || self.nodes[&justify_block].height > self.nodes[&self.locked_block].height)
        {
            self.voted_height = node_height;
            self.context
                .send_vote(self.leader, block.clone(), self.config.replica_index)
                .await
        } else {
            assert!(!self.is_leader());
            warn!(
                self.config.replica_index,
                "not voting for unsafe block {block:?}"
            )
        }

        self.update(&block).await;

        if let Some(nodes) = self.reorder_nodes.remove(&block) {
            for (node, block) in nodes {
                Box::pin(self.handle_generic(node, block)).await;
            }
        }
        if let Some(quorum_cert) = self.reorder_quorum_certs.remove(&block) {
            self.on_quorum_cert(quorum_cert).await;
        }
    }

    async fn handle_vote(
        &mut self,
        block: BlockDigest,
        replica_index: ReplicaIndex,
        partial_sig: PartialSigBytes,
    ) {
        if !self.nodes.contains_key(&block) {
            warn!(
                self.config.replica_index,
                "vote for unknown node {block:?} from replica {replica_index}"
            );
            return;
        }
        let votes = self.votes.entry(block.clone()).or_default();
        if votes.len() >= self.config.params.quorum_size() {
            return;
        }
        votes.insert(replica_index, partial_sig);
        if votes.len() == self.config.params.quorum_size() {
            self.context.make_quorum_cert(block, votes.clone()).await;
        }
    }

    async fn update(&mut self, block: &BlockDigest) {
        self.update_highest_quorum_cert(self.nodes[block].justify.clone())
            .await;
        let block2 = &self.nodes[block].justify.block;
        let block1 = &self.nodes[block2].justify.block;
        let block0 = &self.nodes[block1].justify.block;
        if self.nodes[block1].height > self.nodes[&self.locked_block].height {
            self.locked_block = block1.clone();
        }
        if &self.nodes[block2].parent == block1 && &self.nodes[block1].parent == block0 {
            info!(self.config.replica_index, "committing block {block0:?}");
            let block0 = block0.clone();
            commit(
                &block0,
                &self.executed_block,
                &mut self.nodes,
                &mut self.context,
            )
            .await;
            self.executed_block = block0;
        }
    }

    async fn update_highest_quorum_cert(&mut self, quorum_cert: QuorumCert) {
        if self.nodes[&quorum_cert.block].height
            > self.nodes[&self.highest_quorum_cert.block].height
        {
            self.leaf_block = quorum_cert.block.clone();
            self.highest_quorum_cert = quorum_cert;
            if !self.pending_commands.is_empty() || self.empty_round_budget > 0 {
                self.beat().await;
            }
        }
    }

    async fn beat(&mut self) {
        if self.is_leader() {
            let commands = self
                .pending_commands
                .drain(..self.pending_commands.len().min(self.config.max_block_size))
                .collect();
            self.propose(commands).await;
        } else {
            self.pending_commands.clear();
        }
    }
}

async fn commit(
    block: &BlockDigest,
    executed_block: &BlockDigest,
    nodes: &mut HashMap<BlockDigest, HotStuffNode>,
    context: &mut impl HotStuffCoreContext,
) {
    if nodes[executed_block].height < nodes[block].height {
        Box::pin(commit(
            &nodes[block].parent.clone(),
            executed_block,
            nodes,
            context,
        ))
        .await;
        // garbage collection is omitted in the original paper, here is a minimal implementation
        // that ensures long-term evaluation is feasible with limited memory
        // if the f slowest replicas miss any committed block, they will never be able to catch up
        // context.deliver(nodes.remove(block).unwrap()).await;
        context.deliver(nodes[block].clone()).await;
    }
}

mod debug_impl {
    use std::fmt::Debug;

    impl Debug for super::HotStuffNode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("HotStuffNode")
                .field("parent", &self.parent)
                .field("commands", &format!("<{} commands>", self.commands.len()))
                .field("height", &self.height)
                .field("justify_block", &self.justify.block)
                .finish()
        }
    }
}
