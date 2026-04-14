use std::{collections::HashMap, env::var, time::Duration};

use anyhow::Context;
use libbft::{
    crypto::DummyCrypto,
    event::{Emit, EmitMap},
    pbft::{
        PbftCoreConfig, PbftEgressWorker, PbftIngressWorker, PbftParams, PbftProtocol, PbftRequest,
        events::{Deliver, SendBytes},
    },
    types::ReplicaIndex,
};
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_VERSION},
};
use tokio::{signal::ctrl_c, sync::mpsc::channel, task::JoinSet, time::sleep};
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
    let tracer_provider = init_tracing_subscriber();
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .install()?;

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

        let mut protocol = PbftProtocol::<DummyCrypto>::new(config);
        let mut ingress = PbftIngressWorker::new(DummyCrypto, params());
        let mut egress = PbftEgressWorker::new(DummyCrypto, params());

        let emit_request = if i == 0 { &mut request_tx } else { &mut None };
        protocol.register(emit_request, &mut ingress, &mut egress);
        let mut node_bytes_tx = None;
        ingress.register(&mut node_bytes_tx);
        let node_bytes_tx = node_bytes_tx.unwrap();
        egress.register(&mut protocol);

        let (deliver_tx, deliver_rx) = channel(1000);
        Emit::<Deliver>::set_tx(&mut protocol, deliver_tx);
        deliver_rx_vec.push(deliver_rx);
        let mut fabric_bytes_tx_map = fabric_bytes_tx_map.clone();
        fabric_bytes_tx_map.remove(&(i as ReplicaIndex));
        EmitMap::<_, SendBytes>::set_tx_map(&mut egress, fabric_bytes_tx_map);

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
            }
            #[allow(unreachable_code)]
            anyhow::Ok(())
        };
        let interrupt = async {
            if var("CI") == Ok("true".into()) {
                sleep(Duration::from_secs(3)).await;
            } else {
                ctrl_c().await.context("Failed to listen for Ctrl+C")?;
            }
            anyhow::Ok(())
        };
        tokio::select! {
            res = rounds => res?,
            res = interrupt => {
                eprintln!();
                res.context("Failed to listen for Ctrl+C")?;
            }
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
