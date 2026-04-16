use std::collections::{BTreeMap, HashMap, btree_map::Entry};

use borsh::{BorshDeserialize, BorshSerialize};
use metrics::{counter, gauge, histogram};
use tokio::time::Instant;
use tracing::{info, instrument, warn};

use crate::crypto::SigBytes;

pub type ReplicaIndex = crate::types::ReplicaIndex;
pub type ViewNum = u64;
pub type SeqNum = u64;
pub type Digest = crate::crypto::Digest;

pub struct PbftParams {
    pub num_replicas: usize,
    pub num_faulty_replicas: usize,
}

pub struct PbftCoreConfig {
    pub params: PbftParams,
    pub replica_index: ReplicaIndex,

    pub window_size: SeqNum,
    pub max_block_size: usize,
}

pub trait PbftCoreContext {
    fn deliver(
        &mut self,
        seq_num: SeqNum,
        requests: Vec<PbftRequest>,
        view_num: ViewNum,
    ) -> impl Future<Output = ()>;

    // "message" is short for "peer message" in this codebase

    // sign `message` and broadcast message along with the signature, and call `on_loopback_message`
    // with the message and signature
    fn broadcast_message(&mut self, message: PbftMessage) -> impl Future<Output = ()>;

    // send message in plain to `to`. sync messages are self-certified and do not require to
    // authenticate the sender
    fn send_sync_message(
        &mut self,
        to: ReplicaIndex,
        message: PbftSyncMessage,
    ) -> impl Future<Output = ()>;
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct PbftRequest(pub Vec<u8>);

#[derive(Debug)]
pub enum PbftMessage {
    PrePrepare(PrePrepare, Vec<PbftRequest>),
    Prepare(Prepare),
    Commit(Commit),
    Checkpoint(Checkpoint),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct PbftSyncMessage(pub PbftRequest);

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct PrePrepare {
    pub view_num: ViewNum,
    seq_num: SeqNum,
    pub digest: Digest,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct Prepare {
    view_num: ViewNum,
    seq_num: SeqNum,
    digest: Digest,
    pub replica_index: ReplicaIndex,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct Commit {
    view_num: ViewNum,
    seq_num: SeqNum,
    digest: Digest,
    pub replica_index: ReplicaIndex,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct Checkpoint {
    seq_num: SeqNum,
    state_digest: Digest,
    pub replica_index: ReplicaIndex,
}

impl PbftMessage {
    fn view_num(&self) -> Option<ViewNum> {
        match self {
            PbftMessage::PrePrepare(pre_prepare, _) => Some(pre_prepare.view_num),
            PbftMessage::Prepare(prepare) => Some(prepare.view_num),
            PbftMessage::Commit(commit) => Some(commit.view_num),
            _ => None,
        }
    }

    fn seq_num(&self) -> SeqNum {
        match self {
            PbftMessage::PrePrepare(pre_prepare, _) => pre_prepare.seq_num,
            PbftMessage::Prepare(prepare) => prepare.seq_num,
            PbftMessage::Commit(commit) => commit.seq_num,
            PbftMessage::Checkpoint(checkpoint) => checkpoint.seq_num,
        }
    }
}

pub struct PbftCore<C> {
    pub context: C,
    config: PbftCoreConfig,
    view_num: ViewNum,
    seq_num: SeqNum,
    pending_requests: Vec<PbftRequest>,
    log: BTreeMap<SeqNum, LogSlot>,
    executed_seq_num: SeqNum,
    // invariant: every proof except the first one (i.e. with smallest seq num) is not stable
    checkpoint_proofs: BTreeMap<SeqNum, CheckpointProof>,

    reorder_prepares: HashMap<SeqNum, Vec<(Prepare, SigBytes)>>,
    reorder_commits: HashMap<SeqNum, Vec<(Commit, SigBytes)>>,
    reorder_checkpoints: BTreeMap<SeqNum, Vec<(Checkpoint, SigBytes)>>,
    // TODO reorder messages from later views
    propose_starts: HashMap<SeqNum, Instant>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct LogSlot {
    requests: Vec<PbftRequest>,
    signed_pre_prepare: (PrePrepare, SigBytes),
    signed_prepares: HashMap<ReplicaIndex, (Prepare, SigBytes)>,
    signed_commits: HashMap<ReplicaIndex, (Commit, SigBytes)>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct CheckpointProof {
    state_digest: Digest,
    signed_checkpoints: HashMap<ReplicaIndex, (Checkpoint, SigBytes)>,
}

impl PbftParams {
    pub fn view_leader(&self, view_num: ViewNum) -> ReplicaIndex {
        ((view_num as usize) % self.num_replicas) as _
    }

    fn slot_prepared(&self, slot: &LogSlot) -> bool {
        slot.signed_prepares.len() + 1 >= self.num_replicas - self.num_faulty_replicas
    }

    fn slot_committed(&self, slot: &LogSlot) -> bool {
        slot.signed_commits.len() >= self.num_replicas - self.num_faulty_replicas
    }

    fn checkpoint_stable(&self, proof: &CheckpointProof) -> bool {
        proof.signed_checkpoints.len() >= self.num_replicas - self.num_faulty_replicas
    }
}

impl PbftCoreConfig {
    fn is_view_leader(&self, view_num: ViewNum) -> bool {
        self.replica_index == self.params.view_leader(view_num)
    }
}

impl<C> PbftCore<C> {
    pub fn new(context: C, config: PbftCoreConfig) -> Self {
        let (
            view_num,
            seq_num,
            pending_requests,
            log,
            executed_seq_num,
            checkpoint_proofs,
            reorder_prepares,
            reorder_commits,
            reorder_checkpoints,
            propose_starts,
        ) = Default::default();
        Self {
            context,
            config,
            view_num,
            seq_num,
            pending_requests,
            log,
            executed_seq_num,
            checkpoint_proofs,
            reorder_prepares,
            reorder_commits,
            reorder_checkpoints,
            propose_starts,
        }
    }

    fn last_stable(&self) -> SeqNum {
        self.checkpoint_proofs
            .first_key_value()
            .filter(|(_, proof)| self.config.params.checkpoint_stable(proof))
            .map(|(&seq_num, _)| seq_num)
            .unwrap_or(0)
    }
}

impl<C: PbftCoreContext> PbftCore<C> {
    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_request(&mut self, request: PbftRequest) {
        if !self.config.is_view_leader(self.view_num) {
            self.context
                .send_sync_message(
                    self.config.params.view_leader(self.view_num),
                    PbftSyncMessage(request),
                )
                .await;
            // TODO set timer
            return;
        }
        self.pending_requests.push(request);
        if self.seq_num < self.last_stable() + self.config.window_size {
            self.close_batch().await;
        }
    }

    // should call with verified messages and their signatures
    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_message(&mut self, message: PbftMessage, sig: SigBytes) {
        if let Some(view_num) = message.view_num()
            && view_num != self.view_num
        {
            if view_num > self.view_num {
                // TODO state transfer if necessary
            }
            return;
        }
        if !matches!(message, PbftMessage::Checkpoint(_))
            && message.seq_num() <= self.executed_seq_num
        {
            return;
        }
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                self.handle_pre_prepare(pre_prepare, requests, sig).await
            }
            PbftMessage::Prepare(prepare) => self.handle_prepare(prepare, sig).await,
            PbftMessage::Commit(commit) => self.handle_commit(commit, sig).await,
            PbftMessage::Checkpoint(checkpoint) => self.handle_checkpoint(checkpoint, sig).await,
        }
    }

    // trusted loopback messages that may be processed differently or bypass some further validation
    // compared to verified remote messages
    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_loopback_message(&mut self, message: PbftMessage, sig: SigBytes) {
        if let Some(view_num) = message.view_num()
            && view_num != self.view_num
        {
            return;
        }
        if !matches!(message, PbftMessage::Checkpoint(_))
            && message.seq_num() <= self.executed_seq_num
        {
            assert!(!matches!(message, PbftMessage::PrePrepare(_, _)));
            return;
        }
        match message {
            PbftMessage::PrePrepare(pre_prepare, requests) => {
                let seq_num = pre_prepare.seq_num;
                let replaced = self.log.insert(
                    seq_num,
                    LogSlot {
                        requests,
                        signed_pre_prepare: (pre_prepare, sig),
                        signed_prepares: Default::default(),
                        signed_commits: Default::default(),
                    },
                );
                assert!(replaced.is_none());
                gauge!("pbft.log_size", "replica_index" => self.config.replica_index.to_string())
                    .set(self.log.len() as f64);

                if let Some(prepares) = self.reorder_prepares.remove(&seq_num) {
                    for (prepare, sig) in prepares {
                        self.handle_prepare(prepare, sig).await;
                    }
                }
                if let Some(commits) = self.reorder_commits.remove(&seq_num) {
                    for (commit, sig) in commits {
                        self.handle_commit(commit, sig).await;
                    }
                }
            }
            PbftMessage::Prepare(prepare) => {
                if self
                    .config
                    .params
                    .slot_prepared(&self.log[&prepare.seq_num])
                {
                    return;
                }
                self.insert_prepare(prepare, sig).await
            }
            PbftMessage::Commit(commit) => {
                if self
                    .config
                    .params
                    .slot_committed(&self.log[&commit.seq_num])
                {
                    return;
                }
                self.insert_commit(commit, sig).await
            }
            PbftMessage::Checkpoint(checkpoint) => self.handle_checkpoint(checkpoint, sig).await,
        }
    }

    pub async fn on_sync_message(&mut self, PbftSyncMessage(request): PbftSyncMessage) {
        self.on_request(request).await;
    }

    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_snapshot(&mut self, seq_num: SeqNum, state_digest: Digest) {
        if seq_num <= self.last_stable() {
            return;
        }
        self.checkpoint_proofs.insert(
            seq_num,
            CheckpointProof {
                state_digest: state_digest.clone(),
                signed_checkpoints: Default::default(),
            },
        );
        let checkpoint = Checkpoint {
            seq_num,
            state_digest,
            replica_index: self.config.replica_index,
        };
        self.context
            .broadcast_message(PbftMessage::Checkpoint(checkpoint))
            .await;
        if let Some(checkpoints) = self.reorder_checkpoints.remove(&seq_num) {
            for (checkpoint, sig) in checkpoints {
                self.handle_checkpoint(checkpoint, sig).await;
            }
        }
    }

    #[instrument(skip(self), fields(replica_index = self.config.replica_index))]
    pub async fn on_tick(&mut self, now: Instant) {
        let _ = now;
        //
    }

    async fn close_batch(&mut self) {
        self.seq_num += 1;
        self.propose_starts.insert(self.seq_num, Instant::now());
        let pre_prepare = PrePrepare {
            view_num: self.view_num,
            seq_num: self.seq_num,
            digest: Default::default(), // will be filled by crypto worker
        };
        let requests = self
            .pending_requests
            .drain(..self.pending_requests.len().min(self.config.max_block_size))
            .collect();
        self.context
            .broadcast_message(PbftMessage::PrePrepare(pre_prepare, requests))
            .await;
    }

    async fn handle_pre_prepare(
        &mut self,
        pre_prepare: PrePrepare,
        requests: Vec<PbftRequest>,
        sig: SigBytes,
    ) {
        let seq_num = pre_prepare.seq_num;
        if seq_num > self.last_stable() + self.config.window_size {
            warn!(
                self.config.replica_index,
                "Received PrePrepare for seq_num {seq_num} which is outside the window (last stable: {}, window size: {})",
                self.last_stable(),
                self.config.window_size
            );
            return;
        }

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
        let prepare = Prepare {
            view_num: self.view_num,
            seq_num,
            digest,
            replica_index: self.config.replica_index,
        };
        self.context
            .broadcast_message(PbftMessage::Prepare(prepare))
            .await;

        slot.insert_entry(LogSlot {
            requests,
            signed_pre_prepare: (pre_prepare, sig),
            signed_prepares: Default::default(),
            signed_commits: Default::default(),
        });
        gauge!("pbft_log_size", "replica_index" => self.config.replica_index.to_string())
            .set(self.log.len() as f64);

        if let Some(prepares) = self.reorder_prepares.remove(&seq_num) {
            for (prepare, sig) in prepares {
                self.handle_prepare(prepare, sig).await;
            }
        }
        if let Some(commits) = self.reorder_commits.remove(&seq_num) {
            for (commit, sig) in commits {
                self.handle_commit(commit, sig).await;
            }
        }
    }

    async fn handle_prepare(&mut self, prepare: Prepare, sig: SigBytes) {
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
            // while we don't need this Prepare, the sender may need our Commit
            // however, if we reply a dedicated Commit in this case, we will always send f dedicated
            // Commit for each slot, since there will always be f slow Prepare received after we
            // have prepared. so we don't concern remote liveness here, but push that to dedicated
            // state transfer path
            return;
        }
        self.insert_prepare(prepare, sig).await;
    }

    async fn insert_prepare(&mut self, prepare: Prepare, sig: SigBytes) {
        let seq_num = prepare.seq_num;
        let digest = prepare.digest.clone();
        let slot = self.log.get_mut(&seq_num).unwrap();
        slot.signed_prepares
            .insert(prepare.replica_index, (prepare, sig));
        if self.config.params.slot_prepared(slot) {
            info!(self.config.replica_index, "Slot {seq_num} is prepared");
            let commit = Commit {
                view_num: self.view_num,
                seq_num,
                digest,
                replica_index: self.config.replica_index,
            };
            self.context
                .broadcast_message(PbftMessage::Commit(commit))
                .await;
        }
    }

    async fn handle_commit(&mut self, commit: Commit, sig: SigBytes) {
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
        self.insert_commit(commit, sig).await;
    }

    async fn insert_commit(&mut self, commit: Commit, sig: SigBytes) {
        let seq_num = commit.seq_num;
        let slot = self.log.get_mut(&seq_num).unwrap();
        slot.signed_commits
            .insert(commit.replica_index, (commit, sig));
        if self.config.params.slot_committed(slot) {
            info!(self.config.replica_index, "Slot {seq_num} is committed");
            if self.config.is_view_leader(self.view_num) {
                let elapsed = self.propose_starts.remove(&seq_num).unwrap().elapsed();
                histogram!("pbft_commit_latency", "replica_index" => self.config.replica_index.to_string()).record(elapsed.as_secs_f64());
            }
            self.execute_slots().await;
        }
    }

    async fn execute_slots(&mut self) {
        while let Some(slot) = self.log.get(&(self.executed_seq_num + 1)) {
            if !self.config.params.slot_committed(slot) {
                break;
            }

            counter!("pbft_committed_slots", "replica_index" => self.config.replica_index.to_string()).increment(1);
            counter!("pbft_committed_requests", "replica_index" => self.config.replica_index.to_string()).increment(slot.requests.len() as _);

            self.executed_seq_num += 1;
            self.context
                .deliver(self.executed_seq_num, slot.requests.clone(), self.view_num)
                .await;
        }
    }

    async fn handle_checkpoint(&mut self, checkpoint: Checkpoint, sig: SigBytes) {
        if checkpoint.seq_num <= self.last_stable() {
            return;
        }
        let seq_num = checkpoint.seq_num;
        let Some(proof) = self.checkpoint_proofs.get_mut(&seq_num) else {
            self.reorder_checkpoints
                .entry(checkpoint.seq_num)
                .or_default()
                .push((checkpoint, sig));
            return;
        };
        if checkpoint.state_digest != proof.state_digest {
            warn!(
                self.config.replica_index,
                "Conflicting Checkpoint for seq_num {seq_num} from replica {}: expected state digest {:?}, got {:?}",
                checkpoint.replica_index,
                proof.state_digest,
                checkpoint.state_digest
            );
            return;
        }
        proof
            .signed_checkpoints
            .insert(checkpoint.replica_index, (checkpoint, sig));
        if self.config.params.checkpoint_stable(proof) {
            info!(
                self.config.replica_index,
                "Checkpoint at seq_num {seq_num} is stable"
            );
            self.log = self.log.split_off(&(seq_num + 1));
            self.checkpoint_proofs = self.checkpoint_proofs.split_off(&seq_num);
            self.reorder_checkpoints = self.reorder_checkpoints.split_off(&(seq_num + 1));
            gauge!("pbft_log_size", "replica_index" => self.config.replica_index.to_string())
                .set(self.log.len() as f64);

            if self.config.is_view_leader(self.view_num)
                && self.seq_num < self.last_stable() + self.config.window_size
                && !self.pending_requests.is_empty()
            {
                self.close_batch().await;
            }
        }
    }
}
