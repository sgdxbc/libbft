use borsh::{BorshDeserialize, BorshSerialize};

// replicas take consecutive 0-started "indices" while clients take arbitrary "IDs"
// socket addresses may be too rigid for client IDs e.g. considering overlay networks
// a dedicated `ClientOverlayId` may be good enough?
pub type ReplicaIndex = u8;
pub type ClientId = std::net::SocketAddr;
pub type ClientSeqNum = u64;

#[derive(Debug, BorshSerialize, BorshDeserialize, Clone)]
pub struct Txn {
    pub client_id: ClientId,
    pub client_seq_num: ClientSeqNum,
    pub payload: Payload,
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct Payload(pub Vec<u8>);

mod debug_impl {
    use std::fmt::Debug;

    use super::*;

    impl Debug for Payload {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let s = if let Ok(s) = std::str::from_utf8(&self.0) {
                s
            } else {
                &hex::encode(&self.0)
            };
            if s.len() > 32 {
                write!(f, "{}...", &s[..32])
            } else {
                write!(f, "{s}")
            }
        }
    }
}
