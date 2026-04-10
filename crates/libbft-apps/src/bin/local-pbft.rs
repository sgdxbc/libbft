use libbft::{
    crypto::DummyCrypto,
    pbft::{PbftCoreConfig, PbftCryptoSignWorker, PbftCryptoVerifyWorker, PbftNode, PbftParams},
    types::ReplicaIndex,
};
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    // attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_VERSION},
    attribute::SERVICE_VERSION,
};
use tokio::sync::mpsc::Sender;
use tracing::Level;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let _guard = init_tracing_subscriber();

    foo().await;
}

#[tracing::instrument]
async fn foo() {
    tracing::info!(
        monotonic_counter.foo = 1_u64,
        key_1 = "bar",
        key_2 = 10,
        "handle foo",
    );

    tracing::info!(histogram.baz = 10, "histogram example",);
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

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_schema_url(
            [
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                // KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
            ],
            SCHEMA_URL,
        )
        .build()
}

// Construct MeterProvider for MetricsLayer
fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(30))
        .build();

    // For debugging in development
    // let stdout_reader =
    //     PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource())
        .with_reader(reader)
        // .with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider() -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
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
fn init_tracing_subscriber() -> OtelGuard {
    let tracer_provider = init_tracer_provider();
    let meter_provider = init_meter_provider();

    let tracer = tracer_provider.tracer("tracing-otel-subscriber");

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
        .with(tracing_subscriber::fmt::layer())
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    OtelGuard {
        tracer_provider,
        meter_provider,
    }
}

struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
}
