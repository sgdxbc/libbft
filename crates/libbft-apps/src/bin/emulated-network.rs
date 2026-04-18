use std::{collections::HashMap, env::var, net::SocketAddr, time::Duration};

use anyhow::Context;
use libbft::{
    crypto::{Digest, DummyCrypto},
    event::{AsEmit, Emit, EventReceiver, EventSender},
    network,
    pbft::{
        self, PbftCoreConfig, PbftEgressWorker, PbftIngressWorker, PbftParams, PbftProtocol,
        PbftRequest,
        events::{Deliver, SendBytes},
    },
    types::ReplicaIndex,
};
use libbft_apps::{init_metrics_exporter, init_telemetry};
use tokio::{signal::ctrl_c, sync::mpsc::channel, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

const NUM_REPLICAS: usize = 4;
const NUM_FAULTY_REPLICAS: usize = 1;

fn replica_addr(index: ReplicaIndex) -> SocketAddr {
    ([10, 0, 0, index + 1], 3000).into()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = init_telemetry();
    init_metrics_exporter(None)?;

    let (fabric_bytes_tx, mut fabric_bytes_rx) = channel(1000);
    let mut node_bytes_tx_map = HashMap::new();

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();

    let mut network = PbftNetwork::new();
    network.spawn_workers(
        &mut join_set,
        &mut node_bytes_tx_map,
        fabric_bytes_tx,
        &token,
    );

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

    let workload = async move {
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
            res = network.run_workload_loop() => res?,
            res = interrupt => res?,
        }
        tracing::info!("Finished {} rounds", network.count);
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

// "network" here means node network. network transportation is done by the fabric
// maybe need better naming

#[derive(Default)]
struct PbftNetwork {
    request_tx: Option<EventSender<pbft::events::HandleRequest>>,
    deliver_rx_vec: Vec<EventReceiver<pbft::events::Deliver>>,
    snapshot_tx_vec: Vec<EventSender<pbft::events::Snapshot>>,
    count: u64,
}

impl PbftNetwork {
    fn new() -> Self {
        Self::default()
    }

    fn params() -> PbftParams {
        PbftParams {
            num_replicas: NUM_REPLICAS,
            num_faulty_replicas: NUM_FAULTY_REPLICAS,
        }
    }

    fn spawn_workers(
        &mut self,
        join_set: &mut JoinSet<()>,
        node_bytes_tx_map: &mut HashMap<SocketAddr, EventSender<network::events::HandleBytes>>,
        fabric_bytes_tx: EventSender<network::events::SendBytes>,
        token: &CancellationToken,
    ) {
        for i in 0..NUM_REPLICAS {
            let config = PbftCoreConfig {
                params: Self::params(),
                replica_index: i as ReplicaIndex,
                window_size: 200, // we ingest checkpoint per 100 delivery (see below)
                max_block_size: 1,
            };

            let mut protocol = PbftProtocol::new(config);
            let mut ingress = PbftIngressWorker::new(DummyCrypto, Self::params());
            let mut egress = PbftEgressWorker::new(
                DummyCrypto,
                Self::params(),
                (0..NUM_REPLICAS)
                    .filter(|&index| index != i)
                    .map(|index| (index as _, replica_addr(index as _)))
                    .collect(),
            );

            let emit_request = if i == 0 {
                &mut self.request_tx
            } else {
                &mut None
            };
            let mut snapshot_tx = None;
            protocol.register(
                AsEmit(emit_request),
                AsEmit(&mut ingress).and(AsEmit(&mut egress)),
                AsEmit(&mut snapshot_tx),
            );
            self.snapshot_tx_vec.push(snapshot_tx.unwrap());
            let mut node_bytes_tx = None;
            ingress.register(AsEmit(&mut node_bytes_tx));
            let node_bytes_tx = node_bytes_tx.unwrap();
            node_bytes_tx_map.insert(replica_addr(i as _), node_bytes_tx);
            egress.register(AsEmit(&mut protocol));

            let (deliver_tx, deliver_rx) = channel(1000);
            Emit::<Deliver>::install(&mut AsEmit(&mut protocol), deliver_tx);
            self.deliver_rx_vec.push(deliver_rx);
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
    }

    async fn run_workload_loop(&mut self) -> anyhow::Result<()> {
        loop {
            let span = info_span!("Workload", round = self.count);
            self.request_tx
                .as_ref()
                .unwrap()
                .send((PbftRequest(b"hello".into()), span))
                .await
                .context("request")?;
            for (i, deliver_rx) in self.deliver_rx_vec.iter_mut().enumerate() {
                deliver_rx
                    .recv()
                    .await
                    .with_context(|| format!("Node {i} deliver round {}", self.count))?;
                tracing::debug!("Node {i} delivered");
            }
            self.count += 1;
            if self.count.is_multiple_of(100) {
                for snapshot_tx in &self.snapshot_tx_vec {
                    snapshot_tx
                        .send((
                            (self.count, Digest([0u8; 32].into())),
                            info_span!("TriggerSnapshot", round = self.count),
                        ))
                        .await
                        .context("Failed to trigger snapshot")?;
                }
            }
        }
    }
}
