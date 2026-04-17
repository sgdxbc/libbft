use std::{collections::HashMap, env::var, net::SocketAddr, time::Duration};

use anyhow::Context;
use libbft::{
    crypto::{Digest, DummyCrypto},
    event::{AsEmit, Emit},
    pbft::{
        PbftCoreConfig, PbftEgressWorker, PbftIngressWorker, PbftParams, PbftProtocol, PbftRequest,
        events::{Deliver, SendBytes},
    },
    types::ReplicaIndex,
};
use libbft_apps::{init_metrics_exporter, init_telemetry};
use tokio::{signal::ctrl_c, sync::mpsc::channel, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

fn params() -> PbftParams {
    PbftParams {
        num_replicas: 4,
        num_faulty_replicas: 1,
    }
}

fn replica_addr(index: ReplicaIndex) -> SocketAddr {
    ([10, 0, 0, index + 1], 3000).into()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = init_telemetry();
    init_metrics_exporter(None)?;

    let (fabric_bytes_tx, mut fabric_bytes_rx) = channel(1000);
    let mut request_tx = None;
    let mut deliver_rx_vec = Vec::new();
    let mut snapshot_tx_vec = Vec::new();
    let mut node_bytes_tx_map = HashMap::new();

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();
    for i in 0..params().num_replicas {
        let config = PbftCoreConfig {
            params: params(),
            replica_index: i as ReplicaIndex,
            window_size: 200, // we ingest checkpoint per 100 delivery (see below)
            max_block_size: 1,
        };

        let mut protocol = PbftProtocol::new(config);
        let mut ingress = PbftIngressWorker::new(DummyCrypto, params());
        let mut egress = PbftEgressWorker::new(
            DummyCrypto,
            params(),
            (0..params().num_replicas)
                .filter(|&index| index != i)
                .map(|index| (index as _, replica_addr(index as _)))
                .collect(),
        );

        let emit_request = if i == 0 { &mut request_tx } else { &mut None };
        let mut snapshot_tx = None;
        protocol.register(
            AsEmit(emit_request),
            AsEmit(&mut ingress).and(AsEmit(&mut egress)),
            AsEmit(&mut snapshot_tx),
        );
        snapshot_tx_vec.push(snapshot_tx.unwrap());
        let mut node_bytes_tx = None;
        ingress.register(AsEmit(&mut node_bytes_tx));
        let node_bytes_tx = node_bytes_tx.unwrap();
        node_bytes_tx_map.insert(replica_addr(i as _), node_bytes_tx);
        egress.register(AsEmit(&mut protocol));

        let (deliver_tx, deliver_rx) = channel(1000);
        Emit::<Deliver>::install(&mut AsEmit(&mut protocol), deliver_tx);
        deliver_rx_vec.push(deliver_rx);
        Emit::<SendBytes>::install(&mut AsEmit(&mut egress), fabric_bytes_tx.clone());

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
    }

    let fabric = async move {
        while let Some(((receiver_addr, bytes), span)) = fabric_bytes_rx.recv().await {
            node_bytes_tx_map[&receiver_addr]
                .send((bytes.into(), span))
                .await?;
        }
        anyhow::Ok(())
    };
    join_set.spawn({
        let token = token.clone();
        async move {
            if let Some(Err(err)) = token.run_until_cancelled(fabric).await {
                tracing::error!("Fabric error: {err:#}");
            }
        }
    });

    let request_tx = request_tx.unwrap();
    let workload = async move {
        let mut count = 0;
        let rounds = async {
            loop {
                let span = info_span!("Workload", round = count);
                request_tx
                    .send((PbftRequest(b"hello".into()), span))
                    .await
                    .context("request")?;
                for (i, deliver_rx) in deliver_rx_vec.iter_mut().enumerate() {
                    deliver_rx
                        .recv()
                        .await
                        .with_context(|| format!("Node {i} deliver round {count}"))?;
                    tracing::debug!("Node {i} delivered");
                }
                count += 1;
                if count % 100 == 0 {
                    for snapshot_tx in &snapshot_tx_vec {
                        snapshot_tx
                            .send((
                                (count, Digest([0u8; 32].into())),
                                info_span!("TriggerSnapshot", round = count),
                            ))
                            .await
                            .context("Failed to trigger snapshot")?;
                    }
                }
            }
            #[allow(unreachable_code)]
            anyhow::Ok(())
        };
        let interrupt = async {
            if var("CI") == Ok("true".into()) {
                sleep(Duration::from_secs(3)).await;
            } else {
                ctrl_c().await.context("Failed to listen for Ctrl+C")?;
                eprintln!();
            }
            anyhow::Ok(())
        };
        tokio::select! {
            res = rounds => res?,
            res = interrupt => res?,
        }
        tracing::info!("Finished {count} rounds");
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

    if let Some(telemetry) = telemetry {
        telemetry.shutdown()?;
    }
    Ok(())
}
