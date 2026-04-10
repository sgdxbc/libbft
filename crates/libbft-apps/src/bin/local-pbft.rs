use tokio::sync::mpsc::Sender;

use libbft::{
    crypto::DummyCrypto,
    pbft::{PbftCoreConfig, PbftCryptoSignWorker, PbftCryptoVerifyWorker, PbftNode, PbftParams},
    types::ReplicaIndex,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Ok(())
}

fn params() -> PbftParams {
    PbftParams {
        num_replicas: 4,
        num_faulty_replicas: 1,
    }
}

async fn run_node(replica_index: ReplicaIndex) -> anyhow::Result<()> {
    let config = PbftCoreConfig {
        params: params(),
        replica_index,
        window_size: 1,
        max_block_size: 1,
    };
    let mut node = PbftNode::<DummyCrypto>::new(config);
    let mut verify_worker = PbftCryptoVerifyWorker::new(DummyCrypto, params());
    let mut sign_worker = PbftCryptoSignWorker::new(DummyCrypto, params());
    let mut emit_request = Option::<Sender<_>>::None;
    let mut emit_handle_bytes = Option::<Sender<_>>::None;
    node.register(&mut emit_request, &mut verify_worker, &mut sign_worker);
    verify_worker.register(&mut emit_handle_bytes);
    sign_worker.register(&mut node);
    tokio::join!(node.run(), verify_worker.run(), sign_worker.run());
    Ok(())
}
