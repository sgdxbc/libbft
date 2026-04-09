use libbft::{
    crypto::{CryptoKit, Digest, Sig},
    types::ReplicaIndex,
};

pub struct DummyCrypto;
pub struct DummySig;

impl CryptoKit for DummyCrypto {
    type Sig = DummySig;

    fn digest(&self, _bytes: &[u8]) -> Digest {
        Digest([0xde, 0xad, 0xbe, 0xef].into())
    }

    fn sign(&self, _bytes: &[u8]) -> Self::Sig {
        DummySig
    }

    fn verify(
        &self,
        _bytes: &[u8],
        DummySig: &Self::Sig,
        _replica_index: ReplicaIndex,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Sig for DummySig {
    fn bytes_len() -> usize {
        0
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.is_empty(), "Expected empty bytes for unit sig");
        Self
    }

    fn as_bytes(&self) -> &[u8] {
        &[]
    }
}
