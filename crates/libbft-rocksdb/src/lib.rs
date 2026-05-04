use std::collections::{BTreeSet, HashMap};

use libbft::{
    common::Payload,
    event::{Emit, EventChannel},
    kv::{Op, Res},
};
use rocksdb::{DEFAULT_COLUMN_FAMILY_NAME, WriteBatch};
use tokio_util::sync::CancellationToken;

pub mod events {
    pub type Execute = libbft::execute::events::Execute;
}

pub struct Store {
    db: rocksdb::DB,

    op: EventChannel<events::Execute>,
}

impl Store {
    pub fn new(db: rocksdb::DB) -> Self {
        Self {
            db,
            op: EventChannel::new(None),
        }
    }

    pub fn register(&self, mut emit_execute: impl Emit<events::Execute>) {
        emit_execute.install(self.op.sender());
    }

    pub async fn run(&mut self, token: &CancellationToken) -> anyhow::Result<()> {
        'outer: while let Some(Some(((ops, tx), span))) =
            token.run_until_cancelled(self.op.recv()).await
        {
            let _enter = span.enter();
            let mut read_keys = BTreeSet::new();
            let mut write_batch = WriteBatch::new();
            let mut puts = HashMap::<Vec<_>, Vec<_>>::new();

            enum OpState {
                Reading(Vec<u8>),
                Writing,
                ReadOwnWrite(Vec<u8>),
            }
            let mut op_states = Vec::new();

            for Payload(op_bytes) in ops {
                let state = match borsh::from_slice(&op_bytes) {
                    Ok(Op::Get(key)) => {
                        if let Some(value) = puts.get(&key) {
                            OpState::ReadOwnWrite(value.clone())
                        } else {
                            read_keys.insert(key.clone());
                            OpState::Reading(key)
                        }
                    }
                    Ok(Op::Put(key, value)) => {
                        puts.insert(key.clone(), value.clone());
                        write_batch.put(key, value);
                        OpState::Writing
                    }
                    Err(e) => {
                        tracing::error!("Failed to deserialize op: {e:#}");
                        continue 'outer;
                    }
                };
                op_states.push(state);
            }
            let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap();
            let values = self.db.batched_multi_get_cf(cf, &read_keys, true);
            self.db.write(write_batch)?;
            let mut data = HashMap::new();
            for (key, value) in read_keys.into_iter().zip(values) {
                data.insert(key, value?);
            }
            let mut res_vec = Vec::with_capacity(op_states.len());
            for op_state in op_states {
                let res = match op_state {
                    OpState::Reading(key) => Res::Get((data[&key]).as_ref().map(|v| v.to_vec())),
                    OpState::Writing => Res::Put,
                    OpState::ReadOwnWrite(value) => Res::Get(Some(value)),
                };
                let res_bytes = borsh::to_vec(&res).expect("should serialize response");
                res_vec.push(Payload(res_bytes));
            }
            if tx.send(Some(res_vec)).is_err() {
                tracing::warn!("Failed to send response");
            }
        }
        Ok(())
    }
}
