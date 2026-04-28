use std::{
    collections::HashMap,
    env::{args, var},
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Context;
use libbft::{
    crypto::{Digest, DummyCrypto},
    event::{AsEmit, Emit, EventChannel, EventSender},
    hotstuff::{
        self, HotStuffCommand, HotStuffCoreConfig, HotStuffEgress, HotStuffIngress, HotStuffParams,
        HotStuffProtocol, HotStuffQuorumCertWorker,
    },
    narwhal::{
        self, Bullshark, NarwhalCoreConfig, NarwhalEgress, NarwhalIngress, NarwhalParams,
        NarwhalProtocol,
    },
    network,
    pbft::{
        self, PbftCoreConfig, PbftEgress, PbftIngress, PbftParams, PbftProtocol, PbftRequest,
        events::{Deliver, SendBytes},
    },
    types::ReplicaIndex,
};
use libbft_apps::{init_metrics_exporter, init_telemetry};
use metrics::histogram;
use tokio::{signal::ctrl_c, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let protocol = args()
        .find_map(|arg| match arg.strip_prefix("protocol=") {
            Some(protocol @ ("pbft" | "hotstuff" | "bullshark")) => Some(protocol.to_owned()),
            Some(protocol) => {
                eprintln!("Unsupported protocol: {protocol}");
                None
            }
            _ => None,
        })
        .context("Failed to parse protocol argument")?;

    let _telemetry = init_telemetry();
    init_metrics_exporter(None)?;

    let mut fabric_bytes = EventChannel::new(None);
    let mut node_bytes_tx_map = HashMap::new();

    let token = CancellationToken::new();
    let mut join_set = JoinSet::new();

    let mut network = match &*protocol {
        "pbft" => {
            let mut network = PbftNetwork::new();
            network.spawn_workers(
                &mut join_set,
                &mut node_bytes_tx_map,
                fabric_bytes.sender(),
                &token,
            );
            Network::Pbft(network)
        }
        "hotstuff" => {
            let mut network = HotStuffNetwork::new();
            network.spawn_workers(
                &mut join_set,
                &mut node_bytes_tx_map,
                fabric_bytes.sender(),
                &token,
            );
            Network::HotStuff(network)
        }
        "bullshark" => {
            let mut network = BullsharkNetwork::new();
            network.spawn_workers(
                &mut join_set,
                &mut node_bytes_tx_map,
                fabric_bytes.sender(),
                &token,
            );
            Network::Bullshark(network)
        }
        _ => unreachable!(),
    };

    let fabric = async move {
        while let Some(((receiver_addr, bytes), span)) = fabric_bytes.recv().await {
            node_bytes_tx_map[&receiver_addr]
                .send(bytes.into(), span)
                .await;
        }
    };
    join_set.spawn({
        let token = token.clone();
        async move {
            token.run_until_cancelled(fabric).await;
        }
    });

    let workload = async move {
        let workload = async {
            match &mut network {
                Network::Pbft(network) => network.run_workload_loop().await,
                Network::HotStuff(network) => network.run_workload_loop().await,
                Network::Bullshark(network) => network.run_workload_loop().await,
            }
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
            res = workload => res?,
            res = interrupt => res?,
        }
        let count = match &network {
            Network::Pbft(network) => network.count,
            Network::HotStuff(network) => network.count,
            Network::Bullshark(network) => network.count,
        };
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
    Ok(())
}

// "network" here means node network. network transportation is done by the fabric
// maybe need better naming
enum Network {
    Pbft(PbftNetwork),
    HotStuff(HotStuffNetwork),
    Bullshark(BullsharkNetwork),
}

const NUM_REPLICAS: usize = 4;
const NUM_FAULTY_REPLICAS: usize = 1;

fn replica_addr(index: ReplicaIndex) -> SocketAddr {
    ([10, 0, 0, index + 1], 3000).into()
}

#[derive(Default)]
struct PbftNetwork {
    request_tx: Option<EventSender<pbft::events::HandleRequest>>,
    deliver_vec: Vec<EventChannel<pbft::events::Deliver>>,
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
            let mut ingress = PbftIngress::new(DummyCrypto, Self::params());
            let mut egress = PbftEgress::new(
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

            let deliver = EventChannel::new(None);
            Emit::<Deliver>::install(&mut AsEmit(&mut protocol), deliver.sender());
            self.deliver_vec.push(deliver);
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
            let round_start = Instant::now();
            let span = info_span!("Workload", round = self.count);
            let sent = self
                .request_tx
                .as_ref()
                .unwrap()
                .send(PbftRequest(b"hello".into()), span)
                .await;
            anyhow::ensure!(sent, "Failed to send request");
            for (i, deliver_rx) in self.deliver_vec.iter_mut().enumerate() {
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
                        .send(
                            (self.count, Digest(self.count.to_be_bytes().into())),
                            info_span!("TriggerSnapshot", round = self.count),
                        )
                        .await;
                }
            }
            histogram!("deliver_latency").record(round_start.elapsed().as_secs_f64());
        }
    }
}

#[derive(Default)]
struct HotStuffNetwork {
    command_tx_vec: Vec<EventSender<hotstuff::events::HandleCommand>>,
    deliver_vec: Vec<EventChannel<hotstuff::events::Deliver>>,
    count: u64,
}

impl HotStuffNetwork {
    fn new() -> Self {
        Self::default()
    }

    fn params() -> HotStuffParams {
        HotStuffParams {
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
            let config = HotStuffCoreConfig {
                params: Self::params(),
                replica_index: i as _,
                max_block_size: 1,
            };

            let mut protocol = HotStuffProtocol::new(config);
            let mut ingress = HotStuffIngress::new(DummyCrypto);
            let mut egress = HotStuffEgress::new(
                DummyCrypto,
                (0..NUM_REPLICAS)
                    .filter(|&index| index != i)
                    .map(|index| (index as _, replica_addr(index as _)))
                    .collect(),
            );
            let mut quorum_cert = HotStuffQuorumCertWorker::new(DummyCrypto);

            let mut emit_command = None;
            protocol.register(
                AsEmit(&mut emit_command),
                AsEmit(&mut ingress).and(AsEmit(&mut egress)),
                AsEmit(&mut quorum_cert),
            );
            self.command_tx_vec.push(emit_command.unwrap());
            let mut node_bytes_tx = None;
            ingress.register(AsEmit(&mut node_bytes_tx));
            let node_bytes_tx = node_bytes_tx.unwrap();
            node_bytes_tx_map.insert(replica_addr(i as _), node_bytes_tx);
            egress.register(AsEmit(&mut protocol));
            quorum_cert.register(AsEmit(&mut protocol));

            let deliver = EventChannel::new(None);
            Emit::<hotstuff::events::Deliver>::install(
                &mut AsEmit(&mut protocol),
                deliver.sender(),
            );
            self.deliver_vec.push(deliver);
            Emit::<hotstuff::events::SendBytes>::install(
                &mut AsEmit(&mut egress),
                fabric_bytes_tx.clone(),
            );

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
                async move { quorum_cert.run(&token).await }
            });
        }
    }

    async fn run_workload_loop(&mut self) -> anyhow::Result<()> {
        loop {
            let round_start = Instant::now();
            let span = info_span!("Workload", round = self.count);
            for command_tx in &self.command_tx_vec {
                let sent = command_tx
                    .send(
                        HotStuffCommand(self.count.to_be_bytes().into()),
                        span.clone(),
                    )
                    .await;
                anyhow::ensure!(sent, "Failed to send command");
            }
            for (i, deliver) in self.deliver_vec.iter_mut().enumerate() {
                while {
                    let (block, _) = deliver
                        .rx
                        .recv()
                        .await
                        .with_context(|| format!("Node {i} deliver round {}", self.count))?;
                    block.commands.is_empty()
                } {}
                tracing::debug!("Node {i} delivered");
            }
            self.count += 1;
            histogram!("deliver_latency").record(round_start.elapsed().as_secs_f64());
        }
    }
}

#[derive(Default)]
struct BullsharkNetwork {
    deliver_vec: Vec<EventChannel<narwhal::events::Deliver>>,
    count: u64,
}

impl BullsharkNetwork {
    fn new() -> Self {
        Self::default()
    }

    fn params() -> NarwhalParams {
        NarwhalParams {
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
            let config = NarwhalCoreConfig {
                consensus: Bullshark,
                params: Self::params(),
                replica_index: i as _,
                max_block_size: 1,
            };

            let mut protocol = NarwhalProtocol::new(config);
            let mut ingress = NarwhalIngress::new(DummyCrypto, Self::params());
            let mut egress = NarwhalEgress::new(
                DummyCrypto,
                Self::params(),
                (0..NUM_REPLICAS)
                    .filter(|&index| index != i)
                    .map(|index| (index as _, replica_addr(index as _)))
                    .collect(),
            );

            protocol.register(
                AsEmit(&mut None),
                AsEmit(&mut ingress).and(AsEmit(&mut egress)),
            );
            let mut node_bytes_tx = None;
            ingress.register(AsEmit(&mut node_bytes_tx));
            let node_bytes_tx = node_bytes_tx.unwrap();
            node_bytes_tx_map.insert(replica_addr(i as _), node_bytes_tx);
            egress.register(AsEmit(&mut protocol));

            let deliver = EventChannel::new(None);
            Emit::<narwhal::events::Deliver>::install(&mut AsEmit(&mut protocol), deliver.sender());
            self.deliver_vec.push(deliver);
            Emit::<narwhal::events::SendBytes>::install(
                &mut AsEmit(&mut egress),
                fabric_bytes_tx.clone(),
            );

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
            let round_start = Instant::now();
            let mut expected = None;
            for (i, deliver) in self.deliver_vec.iter_mut().enumerate() {
                let (block, _) = deliver
                    .rx
                    .recv()
                    .await
                    .with_context(|| format!("Node {i} deliver round {}", self.count))?;
                tracing::debug!("Node {i} delivered");
                if i == 0 {
                    expected = Some((block.round, block.replica_index));
                } else {
                    anyhow::ensure!(
                        Some((block.round, block.replica_index)) == expected,
                        "Node {i} delivered unexpected block"
                    );
                }
            }
            self.count += 1;
            histogram!("deliver_latency").record(round_start.elapsed().as_secs_f64());
        }
    }
}
