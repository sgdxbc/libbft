use std::net::SocketAddr;

use anyhow::Context;
use libbft::{
    crypto::{Digest, DummyCrypto},
    event::{AsEmit, Emit},
    pbft::{
        PbftCoreConfig, PbftEgressWorker, PbftIngressWorker, PbftParams, PbftProtocol, PbftRequest,
        events::Deliver,
    },
    types::ReplicaIndex,
};
use libbft_apps::{init_metrics_exporter, init_telemetry};
use libbft_network::peer::PeerNetwork;
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc::channel, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span};

fn params() -> PbftParams {
    PbftParams {
        num_replicas: 4,
        num_faulty_replicas: 1,
    }
}

fn replica_addr(index: ReplicaIndex) -> SocketAddr {
    ([127, 0, 0, 1], 4000 + index as u16).into()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let index = std::env::args()
        .find_map(|arg| arg.strip_prefix("index=")?.parse().ok())
        .context("invalid replica index")?;

    let _telemetry = init_telemetry();
    init_metrics_exporter(9000 + index as u16)?;

    let mut request_tx = None;
    let mut snapshot_tx = None;
    let (deliver_tx, mut deliver_rx) = channel(1000);

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();
    let config = PbftCoreConfig {
        params: params(),
        replica_index: index,
        window_size: 200, // we ingest checkpoint per 100 delivery (see below)
        max_block_size: 1,
    };

    let mut protocol = PbftProtocol::new(config);
    let mut ingress = PbftIngressWorker::new(DummyCrypto, params());
    let mut egress = PbftEgressWorker::new(
        DummyCrypto,
        params(),
        (0..params().num_replicas)
            .filter(|&i| i != index as _)
            .map(|index| (index as _, replica_addr(index as _)))
            .collect(),
    );
    let listener = TcpListener::bind(replica_addr(index))
        .await
        .with_context(|| format!("Failed to bind to {}", replica_addr(index)))?;
    info!("Node {index} listening on {}", replica_addr(index));
    let mut network = PeerNetwork::new(listener);

    let emit_request = if index == 0 {
        &mut request_tx
    } else {
        &mut None
    };
    protocol.register(
        AsEmit(emit_request),
        AsEmit(&mut ingress).and(AsEmit(&mut egress)),
        AsEmit(&mut snapshot_tx),
    );
    Emit::<Deliver>::install(&mut AsEmit(&mut protocol), deliver_tx);
    ingress.register(AsEmit(&mut network));
    egress.register(AsEmit(&mut protocol));
    network.register(AsEmit(&mut egress));

    join_set.spawn({
        let token = token.clone();
        async move { protocol.run(&token).await }
    });
    join_set.spawn({
        let token = token.clone();
        async move { egress.run(&token).await }
    });
    join_set.spawn({
        let token = token.clone();
        async move { ingress.run(&token).await }
    });
    join_set.spawn({
        let token = token.clone();
        async move { network.run(&token).await.expect("network should not fail") }
    });

    let snapshot_tx = snapshot_tx.unwrap();
    let workload = async move {
        let mut count = 0;
        let rounds = async {
            loop {
                let span = info_span!("Workload", round = count);
                if let Some(request_tx) = &request_tx {
                    request_tx
                        .send((PbftRequest(b"hello".into()), span))
                        .await
                        .context("request")?;
                }
                deliver_rx
                    .recv()
                    .await
                    .with_context(|| format!("Node {index} deliver round {count}"))?;
                count += 1;
                if count % 100 == 0 {
                    snapshot_tx
                        .send((
                            (count, Digest([0u8; 32].into())),
                            info_span!("TriggerSnapshot", round = count),
                        ))
                        .await
                        .context("Failed to trigger snapshot")?;
                }
            }
            #[allow(unreachable_code)]
            anyhow::Ok(())
        };
        tokio::select! {
            res = rounds => res?,
            res = ctrl_c() => {
                res.context("Failed to listen for Ctrl+C")?;
                eprintln!();
            }
        }
        info!("Finished {count} rounds");
        anyhow::Ok(())
    };
    join_set.spawn(async move {
        if let Err(err) = workload.await {
            tracing::error!("Workload error: {err:#}");
        }
        token.cancel();
    });
    while let Some(res) = join_set.join_next().await {
        res.unwrap()
    }
    Ok(())
}
