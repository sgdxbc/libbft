use std::{net::SocketAddr, time::Instant};

use anyhow::Context;
use libbft::{
    common::ReplicaIndex,
    crypto::{Digest, DummyCrypto},
    event::{AsEmit, Emit, EventChannel},
    pbft::{
        PbftCoreConfig, PbftEgress, PbftIngress, PbftParams, PbftProtocol, PbftRequest,
        events::Deliver,
    },
};
use libbft_apps::{init_metrics_exporter, init_telemetry};
use libbft_tcp::peer::PeerNetwork;
use metrics::histogram;
use tokio::{net::TcpListener, signal::ctrl_c, task::JoinSet};
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

fn transaction(count: u64) -> PbftRequest {
    libbft::common::Txn {
        client_id: ([127, 0, 0, 1], 60000).into(),
        client_seq_num: count,
        payload: Default::default(),
    }
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
    let mut deliver = EventChannel::new(None);

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();
    let config = PbftCoreConfig {
        params: params(),
        replica_index: index,
        window_size: 200, // we ingest checkpoint per 100 delivery (see below)
        max_block_size: 1,
    };

    let mut protocol = PbftProtocol::new(config);
    let mut ingress = PbftIngress::new(DummyCrypto, params());
    let mut egress = PbftEgress::new(
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
    Emit::<Deliver>::install(&mut AsEmit(&mut protocol), deliver.sender());
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
        let mut count = 0u64;
        let rounds = async {
            loop {
                let start = Instant::now();
                let span = info_span!("Workload", round = count);
                if let Some(request_tx) = &request_tx {
                    let sent = request_tx.send(transaction(count), span).await;
                    anyhow::ensure!(sent, "Failed to send request");
                }
                deliver
                    .recv()
                    .await
                    .with_context(|| format!("Node {index} deliver round {count}"))?;
                count += 1;
                histogram!("deliver_latency").record(start.elapsed().as_secs_f64());
                if count.is_multiple_of(100) {
                    snapshot_tx
                        .send(
                            (count, Digest(count.to_be_bytes().into())),
                            info_span!("TriggerSnapshot", round = count),
                        )
                        .await;
                }
            }
            #[allow(unreachable_code)]
            anyhow::Ok(())
        };
        tokio::select! {
            res = rounds => res?,
            res = ctrl_c() => res.context("Failed to listen for Ctrl+C")?,
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
