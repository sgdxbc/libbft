use borsh::{BorshDeserialize, BorshSerialize};

// replicas take consecutive 0-started "indices" while clients take arbitrary "IDs"
// socket addresses may be too rigid for client IDs e.g. considering overlay networks
// a dedicated `ClientOverlayId` may be good enough?
pub type ReplicaIndex = u8;
pub type ClientId = std::net::SocketAddr;
pub type ClientSeqNum = u64;

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct Transaction {
    pub client_id: ClientId,
    pub client_seq_num: ClientSeqNum,
    pub payload: Vec<u8>,
}

mod debug_impl {
    use std::fmt::Debug;

    use super::*;

    impl Debug for Transaction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Transaction")
                .field("client_id", &self.client_id)
                .field("client_seq_num", &self.client_seq_num)
                .field("payload", &fmt_payload(&self.payload))
                .finish()
        }
    }

    fn fmt_payload(payload: &[u8]) -> String {
        let s = if let Ok(s) = std::str::from_utf8(payload) {
            s.into()
        } else {
            hex::encode(payload)
        };
        if s.len() > 32 {
            format!("{}...", &s[..32])
        } else {
            s
        }
    }
}
