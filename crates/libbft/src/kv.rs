use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    common::TxnCommit,
    event::{Emit, EventChannel},
};

pub mod events {
    use tokio::sync::oneshot;

    use crate::{
        common::{Txn, TxnCommit},
        event::Event,
    };

    pub struct Execute;
    impl Event for Execute {
        type Value = (Vec<Txn>, oneshot::Sender<Vec<TxnCommit>>);
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum Op {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
}

// avoid ambiguity with Rust's `Result`
#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum Res {
    Get(Option<Vec<u8>>),
    Put,
}

pub struct MemoryStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,

    op: EventChannel<events::Execute>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
            op: EventChannel::new(None),
        }
    }

    pub fn register(&self, mut emit_op: impl Emit<events::Execute>) {
        emit_op.install(self.op.sender());
    }

    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some(((txns, tx), span))) = token.run_until_cancelled(self.op.recv()).await {
            let mut commits = Vec::with_capacity(txns.len());
            span.in_scope(|| {
                for txn in txns {
                    let Ok(op) = borsh::from_slice(&txn.payload) else {
                        warn!("failed to deserialize transaction payload");
                        continue;
                    };
                    let res = match op {
                        Op::Get(key) => {
                            let value = self.data.get(&key).cloned();
                            Res::Get(value)
                        }
                        Op::Put(key, value) => {
                            self.data.insert(key, value);
                            Res::Put
                        }
                    };
                    let payload = borsh::to_vec(&res).expect("should serialize response");
                    commits.push(TxnCommit {
                        client_id: txn.client_id,
                        client_seq_num: txn.client_seq_num,
                        payload,
                    });
                }
            });
            if tx.send(commits).is_err() {
                warn!("client dropped before receiving response");
            }
        }
    }
}
