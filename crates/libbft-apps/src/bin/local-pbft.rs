use std::collections::HashMap;

use anyhow::Context;
use libbft::{
    crypto::DummyCrypto,
    event::{Emit, EmitMap},
    pbft::{
        PbftCoreConfig, PbftCryptoSignWorker, PbftCryptoVerifyWorker, PbftNode, PbftParams,
        PbftRequest,
        events::{Deliver, SendBytes},
    },
    types::ReplicaIndex,
};
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_VERSION},
};
use tokio::{sync::mpsc::channel, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Level, info_span};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

fn params() -> PbftParams {
    PbftParams {
        num_replicas: 4,
        num_faulty_replicas: 1,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer_provider = init_tracing_subscriber();

    let mut fabric_bytes_rx_vec = Vec::new();
    let mut fabric_bytes_tx_map = HashMap::new();
    for i in 0..params().num_replicas {
        let (bytes_tx, bytes_rx) = channel(1000);
        fabric_bytes_tx_map.insert(i as ReplicaIndex, bytes_tx);
        fabric_bytes_rx_vec.push(bytes_rx);
    }
    let mut join_set = JoinSet::new();
    let mut request_tx = None;
    let mut deliver_rx_vec = Vec::new();
    let token = CancellationToken::new();
    for (i, mut fabric_bytes_rx) in fabric_bytes_rx_vec.into_iter().enumerate() {
        let config = PbftCoreConfig {
            params: params(),
            replica_index: i as ReplicaIndex,
            window_size: 1,
            max_block_size: 1,
        };

        let mut node = PbftNode::<DummyCrypto>::new(config);
        let mut verify_worker = PbftCryptoVerifyWorker::new(DummyCrypto, params());
        let mut sign_worker = PbftCryptoSignWorker::new(DummyCrypto, params());

        let emit_request = if i == 0 { &mut request_tx } else { &mut None };
        node.register(emit_request, &mut verify_worker, &mut sign_worker);
        let mut node_bytes_tx = None;
        verify_worker.register(&mut node_bytes_tx);
        let node_bytes_tx = node_bytes_tx.unwrap();
        sign_worker.register(&mut node);

        let (deliver_tx, deliver_rx) = channel(1000);
        Emit::<Deliver>::set_tx(&mut node, deliver_tx);
        deliver_rx_vec.push(deliver_rx);
        let mut fabric_bytes_tx_map = fabric_bytes_tx_map.clone();
        fabric_bytes_tx_map.remove(&(i as ReplicaIndex));
        EmitMap::<_, SendBytes>::set_tx_map(&mut sign_worker, fabric_bytes_tx_map);

        join_set.spawn({
            let token = token.clone();
            async move { node.run(&token).await }
        });
        join_set.spawn({
            let token = token.clone();
            async move { sign_worker.run(&token).await }
        });
        join_set.spawn({
            let token = token.clone();
            async move { verify_worker.run(&token).await }
        });
        join_set.spawn({
            let token = token.clone();
            async move {
                while let Some(Some((bytes, span))) =
                    token.run_until_cancelled(fabric_bytes_rx.recv()).await
                {
                    let _enter = span.enter();
                    if let Err(err) = node_bytes_tx
                        .send((bytes.into(), info_span!("HandleBytes")))
                        .await
                    {
                        tracing::error!("Failed to send bytes to node {i}: {err:#}");
                    }
                }
            }
        });
    }
    let request_tx = request_tx.unwrap();
    let workload = async move {
        let span = info_span!("Workload");
        request_tx
            .send((PbftRequest(b"hello".into()), span))
            .await
            .context("request")?;
        for (i, mut deliver_rx) in deliver_rx_vec.into_iter().enumerate() {
            deliver_rx
                .recv()
                .await
                .context(format!("Node {i} deliver"))?;
            tracing::info!("Node {i} delivered");
        }
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
    tracer_provider.shutdown()?;
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
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .unwrap();

    SdkTracerProvider::builder()
        // Customize sampling strategy
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        // If export trace to AWS X-Ray, you can use XrayIdGenerator
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter)
        .build()
}

// Initialize tracing-subscriber and return OtelGuard for opentelemetry-related termination processing
fn init_tracing_subscriber() -> SdkTracerProvider {
    let tracer_provider = init_tracer_provider();

    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));

    let targets = if let Ok(filter) = std::env::var("RUST_LOG") {
        filter.parse().unwrap()
    } else {
        Targets::new()
    };
    tracing_subscriber::registry()
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
        .with(tracing_subscriber::fmt::layer().with_filter(targets))
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    tracer_provider
}
