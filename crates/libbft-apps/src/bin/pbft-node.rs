use std::net::SocketAddr;

use anyhow::Context;
use libbft::{
    crypto::{Digest, DummyCrypto},
    event::Emit,
    pbft::{
        PbftCoreConfig, PbftEgressWorker, PbftIngressWorker, PbftParams, PbftProtocol, PbftRequest,
        events::Deliver,
    },
    types::ReplicaIndex,
};
use libbft_network::peer::PeerNetwork;
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_VERSION},
};
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc::channel, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Level, info, info_span};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

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

    let tracer_provider = init_tracing_subscriber();
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(([0, 0, 0, 0], 9000 + index as u16))
        .install()?;

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
    protocol.register(emit_request, &mut ingress, &mut egress, &mut snapshot_tx);
    Emit::<Deliver>::set_tx(&mut protocol, deliver_tx);
    ingress.register(&mut network);
    egress.register(&mut protocol);
    network.register(&mut egress);

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

    if let Some(tracer_provider) = tracer_provider {
        tracer_provider.shutdown()?;
    }
    Ok(())
}

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_schema_url(
            [
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                // KeyValue::new(SERVICE_INSTANCE_ID, uuid::Uuid::new_v4().to_string()),
                KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
            ],
            SCHEMA_URL,
        )
        .build()
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider() -> SdkTracerProvider {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .unwrap();

    SdkTracerProvider::builder()
        // Customize sampling strategy
        // .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        //     0.01,
        // ))))
        // If export trace to AWS X-Ray, you can use XrayIdGenerator
        // .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter)
        .build()
}

fn init_tracing_subscriber() -> Option<SdkTracerProvider> {
    let targets = if let Ok(filter) = std::env::var("RUST_LOG") {
        filter.parse().unwrap()
    } else {
        Targets::new()
    };
    let subscriber = tracing_subscriber::registry()
        // The global level filter prevents the exporter network stack
        // from reentering the globally installed OpenTelemetryLayer with
        // its own spans while exporting, as the libraries should not use
        // tracing levels below DEBUG. If the OpenTelemetry layer needs to
        // trace spans and events with higher verbosity levels, consider using
        // per-layer filtering to target the telemetry layer specifically,
        // e.g. by target matching.
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::INFO,
        ))
        .with(tracing_subscriber::fmt::layer().with_filter(targets));
    if std::env::var("OTEL_SDK_DISABLED") != Ok("true".into()) {
        let tracer_provider = init_tracer_provider();
        let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));
        subscriber.with(OpenTelemetryLayer::new(tracer)).init();
        Some(tracer_provider)
    } else {
        subscriber.init();
        None
    }
}
