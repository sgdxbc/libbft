use std::{
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
};

use borsh::{BorshDeserialize, BorshSerialize};
use tracing::{info, warn};

pub type ReplicaIndex = u8;
pub type ClientAddr = SocketAddr;
pub type ClientSeqNum = u64;
pub type ViewNum = u64;
pub type SeqNum = u64;
pub type Digest = libbft_crypto::Digest;

pub struct PbftParams {
    num_replicas: usize,
    num_faulty_replicas: usize,
}

pub struct PbftCoreConfig {
    params: PbftParams,
    replica_index: ReplicaIndex,

    window_size: SeqNum,
    max_block_size: usize,
}

pub trait PbftCoreContext {
    // "message" is short for "peer message" in this codebase

    // sign `message` and send message along with the signature to `to`
    fn send_message(&mut self, to: ReplicaIndex, message: PbftMessage);

    // sign `message` and broadcast message along with the signature, and call
    // `handle_loopback_broadcast_message` with the message and signature
    fn broadcast_message(&mut self, message: PbftMessage);

    fn deliver(&mut self, requests: Vec<PbftRequest>, view_num: ViewNum);

    type Sig;
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct PbftRequest(ClientAddr, ClientSeqNum, Vec<u8>);

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct PbftReply(ClientSeqNum, Vec<u8>, ViewNum);

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum PbftMessage {
    PrePrepare(PrePrepare, Vec<PbftRequest>),
    Prepare(Prepare),
    Commit(Commit),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct PrePrepare {
    view_num: ViewNum,
    seq_num: SeqNum,
    digest: Digest,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct Prepare {
    view_num: ViewNum,
    seq_num: SeqNum,
    digest: Digest,
    replica_index: ReplicaIndex,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct Commit {
    view_num: ViewNum,
    seq_num: SeqNum,
    digest: Digest,
    replica_index: ReplicaIndex,
}

impl PbftMessage {
    fn view_num(&self) -> ViewNum {
        match self {
            PbftMessage::PrePrepare(pre_prepare, _) => pre_prepare.view_num,
            PbftMessage::Prepare(prepare) => prepare.view_num,
            PbftMessage::Commit(commit) => commit.view_num,
        }
    }
}

pub struct PbftCore<C: PbftCoreContext> {
    context: C,
    config: PbftCoreConfig,
    view_num: ViewNum,
    seq_num: SeqNum,
    pending_requests: Vec<PbftRequest>,
    log: HashMap<SeqNum, LogSlot<C::Sig>>,
    executed_seq_num: SeqNum,

    reorder_prepares: HashMap<SeqNum, Vec<(Prepare, C::Sig)>>,
    reorder_commits: HashMap<SeqNum, Vec<(Commit, C::Sig)>>,
    // TODO reorder messages from later views
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct LogSlot<S> {
    requests: Vec<PbftRequest>,
    signed_pre_prepare: (PrePrepare, S),
    signed_prepares: HashMap<ReplicaIndex, (Prepare, S)>,
    signed_commits: HashMap<ReplicaIndex, (Commit, S)>,
}

impl PbftParams {
    fn view_leader(&self, view_num: ViewNum) -> ReplicaIndex {
        ((view_num as usize) % self.num_replicas) as _
    }

    fn slot_prepared<S>(&self, slot: &LogSlot<S>) -> bool {
        slot.signed_prepares.len() >= self.num_replicas - self.num_faulty_replicas
    }

    fn slot_committed<S>(&self, slot: &LogSlot<S>) -> bool {
        slot.signed_commits.len() >= self.num_replicas - self.num_faulty_replicas
    }
}

impl PbftCoreConfig {
    fn is_view_leader(&self, view_num: ViewNum) -> bool {
        self.replica_index == self.params.view_leader(view_num)
    }
}

impl<C: PbftCoreContext> PbftCore<C> {
    pub fn new(context: C, config: PbftCoreConfig) -> Self {
        let (
            view_num,
            seq_num,
            pending_requests,
            log,
            executed_seq_num,
            reorder_prepares,
            reorder_commits,
        ) = Default::default();
        Self {
            context,
            config,
            view_num,
            seq_num,
            pending_requests,
            log,
            executed_seq_num,
            reorder_prepares,
            reorder_commits,
        }
    }

    pub fn handle_request(&mut self, request: PbftRequest) {
        if !self.config.is_view_leader(self.view_num) {
            // TODO set timer
            return;
        }
        self.pending_requests.push(request);
        if self.seq_num < self.executed_seq_num + self.config.window_size {
            self.close_batch();
        }
    }

    fn close_batch(&mut self) {
        self.seq_num += 1;
        let digest = Default::default(); // will be filled by crypto worker
        let pre_prepare = PrePrepare {
            view_num: self.view_num,
            seq_num: self.seq_num,
            digest,
        };
        let requests = self
            .pending_requests
            .drain(..self.pending_requests.len().min(self.config.max_block_size))
            .collect();
        self.context
            .broadcast_message(PbftMessage::PrePrepare(pre_prepare, requests));
    }

    // should call with verified messages and their signatures
    pub fn handle_message(&mut self, message: PbftMessage, sig: C::Sig) {
        if message.view_num() != self.view_num {
            if message.view_num() > self.view_num {
                // TODO state transfer if necessary
            }
            return;
        }
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                self.handle_pre_prepare(pre_prepare, requests, sig)
            }
            PbftMessage::Prepare(prepare) => self.handle_prepare(prepare, sig),
            PbftMessage::Commit(commit) => self.handle_commit(commit, sig),
        }
    }

    // trusted loopback messages that may be processed differently or bypass some further validation
    // compared to verified remote messages
    pub fn handle_loopback_broadcast_message(&mut self, message: PbftMessage, sig: C::Sig) {
        if message.view_num() != self.view_num {
            return;
        }
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                let replaced = self.log.insert(
                    pre_prepare.seq_num,
                    LogSlot {
                        requests,
                        signed_pre_prepare: (pre_prepare, sig),
                        signed_prepares: Default::default(),
                        signed_commits: Default::default(),
                    },
                );
                assert!(replaced.is_none());
            }
            PbftMessage::Prepare(prepare) => {
                if let Some(slot) = self.log.get(&prepare.seq_num)
                    && self.config.params.slot_prepared(slot)
                {
                    return;
                }
                self.insert_prepare(prepare, sig)
            }
            PbftMessage::Commit(commit) => {
                if let Some(slot) = self.log.get(&commit.seq_num)
                    && self.config.params.slot_committed(slot)
                {
                    return;
                }
                self.insert_commit(commit, sig)
            }
        }
    }

    fn handle_pre_prepare(
        &mut self,
        pre_prepare: PrePrepare,
        requests: Vec<PbftRequest>,
        sig: C::Sig,
    ) {
        let seq_num = pre_prepare.seq_num;
        let slot = self.log.entry(seq_num);
        if let Entry::Occupied(slot) = slot {
            let (existing_pre_prepare, _) = &slot.get().signed_pre_prepare;
            assert_eq!(existing_pre_prepare.view_num, self.view_num);
            if existing_pre_prepare.digest != pre_prepare.digest {
                warn!(
                    self.config.replica_index,
                    "Conflicting PrePrepare for seq_num {}: existing digest {:?}, new digest {:?}",
                    seq_num,
                    existing_pre_prepare.digest,
                    pre_prepare.digest
                );
            }
            return;
        }
        let digest = pre_prepare.digest.clone();
        slot.insert_entry(LogSlot {
            requests,
            signed_pre_prepare: (pre_prepare, sig),
            signed_prepares: Default::default(),
            signed_commits: Default::default(),
        });
        let prepare = Prepare {
            view_num: self.view_num,
            seq_num,
            digest,
            replica_index: self.config.replica_index,
        };
        self.context
            .broadcast_message(PbftMessage::Prepare(prepare));
        if let Some(prepares) = self.reorder_prepares.remove(&seq_num) {
            for (prepare, sig) in prepares {
                self.handle_prepare(prepare, sig);
            }
        }
        if let Some(commits) = self.reorder_commits.remove(&seq_num) {
            for (commit, sig) in commits {
                self.handle_commit(commit, sig);
            }
        }
    }

    fn handle_prepare(&mut self, prepare: Prepare, sig: C::Sig) {
        let Some(slot) = &self.log.get(&prepare.seq_num) else {
            self.reorder_prepares
                .entry(prepare.seq_num)
                .or_default()
                .push((prepare, sig));
            return;
        };
        if slot.signed_pre_prepare.0.digest != prepare.digest {
            warn!(
                self.config.replica_index,
                "Conflicting Prepare for seq_num {} from replica {}: expected digest {:?}, got {:?}",
                prepare.seq_num,
                prepare.replica_index,
                slot.signed_pre_prepare.0.digest,
                prepare.digest
            );
            return;
        }
        if self.config.params.slot_prepared(slot) {
            info!(
                self.config.replica_index,
                "Sending Commit for already prepared slot seq_num {} to replica {}",
                prepare.seq_num,
                prepare.replica_index
            );
            // we don't need this Prepare, but the sender may need our Commit
            let commit = Commit {
                view_num: self.view_num,
                seq_num: prepare.seq_num,
                digest: prepare.digest.clone(),
                replica_index: self.config.replica_index,
            };
            self.context
                .send_message(prepare.replica_index, PbftMessage::Commit(commit));
            return;
        }
        self.insert_prepare(prepare, sig);
    }

    fn insert_prepare(&mut self, prepare: Prepare, sig: C::Sig) {
        let seq_num = prepare.seq_num;
        let digest = prepare.digest.clone();
        let slot = self.log.get_mut(&seq_num).unwrap();
        slot.signed_prepares
            .insert(prepare.replica_index, (prepare, sig));
        if self.config.params.slot_prepared(slot) {
            let commit = Commit {
                view_num: self.view_num,
                seq_num,
                digest,
                replica_index: self.config.replica_index,
            };
            self.context.broadcast_message(PbftMessage::Commit(commit));
        }
    }

    fn handle_commit(&mut self, commit: Commit, sig: C::Sig) {
        let Some(slot) = &self.log.get(&commit.seq_num) else {
            self.reorder_commits
                .entry(commit.seq_num)
                .or_default()
                .push((commit, sig));
            return;
        };
        if slot.signed_pre_prepare.0.digest != commit.digest {
            warn!(
                self.config.replica_index,
                "Conflicting Commit for seq_num {} from replica {}: expected digest {:?}, got {:?}",
                commit.seq_num,
                commit.replica_index,
                slot.signed_pre_prepare.0.digest,
                commit.digest
            );
            return;
        }
        if self.config.params.slot_committed(slot) {
            return;
        }
        self.insert_commit(commit, sig);
    }

    fn insert_commit(&mut self, commit: Commit, sig: C::Sig) {
        let slot = self.log.get_mut(&commit.seq_num).unwrap();
        slot.signed_commits
            .insert(commit.replica_index, (commit, sig));
        if self.config.params.slot_committed(slot) {
            self.execute_slots();
        }
    }

    fn execute_slots(&mut self) {
        while let Some(slot) = self.log.get(&(self.executed_seq_num + 1)) {
            if !self.config.params.slot_committed(slot) {
                break;
            }
            self.context.deliver(slot.requests.clone(), self.view_num);
            self.executed_seq_num += 1;
        }
        if self.seq_num < self.executed_seq_num + self.config.window_size
            && !self.pending_requests.is_empty()
        {
            self.close_batch();
        }
    }
}

pub trait PbftCryptoContext: libbft_crypto::CryptoKit {}
impl<C: libbft_crypto::CryptoKit> PbftCryptoContext for C {}

pub struct PbftCrypto<C> {
    context: C,
    params: PbftParams,
}

impl<C: PbftCryptoContext> PbftCrypto<C> {
    pub fn sign(&self, message: &mut PbftMessage) -> C::Sig {
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                let requests_bytes = borsh::to_vec(requests).unwrap();
                pre_prepare.digest = self.context.digest(&requests_bytes);
                let bytes = borsh::to_vec(pre_prepare).unwrap();
                self.context.sign(&bytes)
            }
            PbftMessage::Prepare(prepare) => self.context.sign(&borsh::to_vec(prepare).unwrap()),
            PbftMessage::Commit(commit) => self.context.sign(&borsh::to_vec(commit).unwrap()),
        }
    }

    pub fn verify(&self, message: &PbftMessage, sig: &C::Sig) -> anyhow::Result<()> {
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                let requests_bytes = borsh::to_vec(requests).unwrap();
                let expected_digest = self.context.digest(&requests_bytes);
                anyhow::ensure!(
                    pre_prepare.digest == expected_digest,
                    "Invalid PrePrepare digest: expected {:?}, got {:?}",
                    expected_digest,
                    pre_prepare.digest
                );
                self.context.verify(
                    &borsh::to_vec(pre_prepare).unwrap(),
                    sig,
                    self.params.view_leader(pre_prepare.view_num),
                )
            }
            PbftMessage::Prepare(prepare) => {
                self.context
                    .verify(&borsh::to_vec(prepare).unwrap(), sig, prepare.replica_index)
            }
            PbftMessage::Commit(commit) => {
                self.context
                    .verify(&borsh::to_vec(commit).unwrap(), sig, commit.replica_index)
            }
        }
    }
}
