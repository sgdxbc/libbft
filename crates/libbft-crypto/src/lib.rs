use borsh::{BorshDeserialize, BorshSerialize};

pub trait CryptoKit {
    type Sig;

    fn sign(&self, bytes: &[u8]) -> Self::Sig;

    fn verify(
        &self,
        bytes: &[u8],
        sig: &Self::Sig,
        replica_index: ReplicaIndex,
    ) -> anyhow::Result<()>;

    fn digest(&self, bytes: &[u8]) -> Digest;
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
