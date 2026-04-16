use borsh::{BorshDeserialize, BorshSerialize};

use crate::types::ReplicaIndex;

#[derive(BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq)]
pub struct Digest(pub Vec<u8>);

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct SigBytes(pub Vec<u8>);

pub trait CryptoScheme {
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

#[derive(BorshSerialize, BorshDeserialize, Clone)]
pub struct PartialSigBytes(pub Vec<u8>);

pub trait ThresholdCryptoScheme {
    const PARTIAL_SIG_BYTES_LEN: usize;

    const SIG_BYTES_LEN: usize;

    fn partial_sign(&self, bytes: &[u8]) -> PartialSigBytes;

    fn partial_verify(
        &self,
        bytes: &[u8],
        partial_sig: &PartialSigBytes,
        replica_index: ReplicaIndex,
    ) -> anyhow::Result<()> {
        // fallback implementation for schemes that don't support partial verification
        // for the schemes that support partial verification (and override this method), the
        // `combine` method must success if called with valid partial signatures, all of which
        // pass `partial_verify`
        let _ = (bytes, partial_sig, replica_index);
        Ok(())
    }

    fn combine(
        &self,
        partial_sigs: impl IntoIterator<Item = (ReplicaIndex, PartialSigBytes)>,
    ) -> anyhow::Result<SigBytes>;

    fn verify(&self, bytes: &[u8], sig: &SigBytes) -> anyhow::Result<()>;
}

pub struct DummyCrypto;

impl CryptoScheme for DummyCrypto {
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

mod debug_impl {
    impl std::fmt::Debug for super::Digest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Digest({})", hex::encode(&self.0))
        }
    }

    impl std::fmt::Debug for super::SigBytes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(head_bytes) = self.0.get(..4) {
                write!(f, "SigBytes({}...)", hex::encode(head_bytes))
            } else {
                write!(f, "SigBytes({})", hex::encode(&self.0))
            }
        }
    }

    impl std::fmt::Debug for super::PartialSigBytes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(head_bytes) = self.0.get(..4) {
                write!(f, "PartialSigBytes({}...)", hex::encode(head_bytes))
            } else {
                write!(f, "PartialSigBytes({})", hex::encode(&self.0))
            }
        }
    }
}
