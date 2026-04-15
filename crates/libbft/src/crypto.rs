use borsh::{BorshDeserialize, BorshSerialize};

use crate::types::ReplicaIndex;

pub trait CryptoKit {
    const SIG_BYTES_LEN: usize;

    fn sign(&self, bytes: &[u8]) -> SigBytes;

    fn verify(
        &self,
        bytes: &[u8],
        sig: &SigBytes,
        replica_index: ReplicaIndex,
    ) -> anyhow::Result<()>;

    fn digest(&self, bytes: &[u8]) -> Digest;
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq)]
pub struct Digest(pub Vec<u8>);

mod digest_fmt {
    impl std::fmt::Debug for super::Digest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Digest({})", hex::encode(&self.0))
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct SigBytes(pub Vec<u8>);

mod sig_bytes_fmt {
    impl std::fmt::Debug for super::SigBytes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(head_bytes) = self.0.get(..4) {
                write!(f, "SigBytes({}...)", hex::encode(head_bytes))
            } else {
                write!(f, "SigBytes({})", hex::encode(&self.0))
            }
        }
    }
}

pub struct DummyCrypto;

impl CryptoKit for DummyCrypto {
    const SIG_BYTES_LEN: usize = 0;

    fn digest(&self, _bytes: &[u8]) -> Digest {
        Digest([0xde, 0xad, 0xbe, 0xef].into())
    }

    fn sign(&self, _bytes: &[u8]) -> SigBytes {
        SigBytes(Default::default())
    }

    fn verify(
        &self,
        _bytes: &[u8],
        _sig: &SigBytes,
        _replica_index: ReplicaIndex,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
