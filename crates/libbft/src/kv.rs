use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::{
    common::Payload,
    event::{Emit, EventChannel},
};

pub mod events {
    pub type Execute = crate::execute::events::Execute;
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
    #[allow(clippy::new_without_default)]
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
        'outer: while let Some(Some(((ops, tx), span))) =
            token.run_until_cancelled(self.op.recv()).await
        {
            let _enter = span.enter();
            let mut res_vec = Vec::with_capacity(ops.len());
            for Payload(op_bytes) in ops {
                let Ok(op) = borsh::from_slice(&op_bytes) else {
                    error!("failed to deserialize transaction payload");
                    continue 'outer;
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
                let res_bytes = borsh::to_vec(&res).expect("should serialize response");
                res_vec.push(Payload(res_bytes));
            }
            if tx.send(Some(res_vec)).is_err() {
                warn!("client dropped before receiving response");
            }
        }
    }
}
