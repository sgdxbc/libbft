use borsh::{BorshDeserialize, BorshSerialize};

pub trait CryptoKit {
    type Sig: Sig;

    fn sign(&self, bytes: &[u8]) -> Self::Sig;

    fn verify(
        &self,
        bytes: &[u8],
        sig: &Self::Sig,
        replica_index: ReplicaIndex,
    ) -> anyhow::Result<()>;

    fn digest(&self, bytes: &[u8]) -> Digest;
}

pub trait Sig {
    fn bytes_len() -> usize;

    fn from_bytes(bytes: &[u8]) -> Self;

    fn as_bytes(&self) -> &[u8];
}

pub type ReplicaIndex = u8;

#[derive(BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq)]
pub struct Digest(pub Vec<u8>);

mod digest_fmt {
    impl std::fmt::Debug for super::Digest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Digest({})", hex::encode(&self.0))
        }
    }
}

pub struct DummyCrypto;

impl CryptoKit for DummyCrypto {
    type Sig = ();

    fn digest(&self, _bytes: &[u8]) -> Digest {
        Digest([0xde, 0xad, 0xbe, 0xef].into())
    }

    fn sign(&self, _bytes: &[u8]) -> Self::Sig {}

    fn verify(
        &self,
        _bytes: &[u8],
        (): &Self::Sig,
        _replica_index: ReplicaIndex,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Sig for () {
    fn bytes_len() -> usize {
        0
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.is_empty(), "Expected empty bytes for unit sig");
    }

    fn as_bytes(&self) -> &[u8] {
        &[]
    }
}
