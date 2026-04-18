use opentelemetry::{
    KeyValue,
    trace::{TracerProvider as _, noop::NoopTracer},
};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_VERSION},
    resource::SERVICE_INSTANCE_ID,
};
use tracing::Level;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

pub struct TelemetryGuard(Option<SdkTracerProvider>);

pub fn init_telemetry() -> TelemetryGuard {
    TelemetryGuard(init_tracing_subscriber())
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.0.take() {
            if let Err(err) = tracer_provider.shutdown() {
                eprintln!("Error shutting down tracer provider: {err:#}");
            }
        }
    }
}

pub fn init_metrics_exporter(port: impl Into<Option<u16>>) -> anyhow::Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(([0, 0, 0, 0], port.into().unwrap_or(9000)))
        .install()?;
    Ok(())
}

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_schema_url(
            [
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                KeyValue::new(SERVICE_INSTANCE_ID, uuid::Uuid::new_v4().to_string()),
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
        subscriber
            .with(OpenTelemetryLayer::new(
                tracer_provider.tracer(env!("CARGO_PKG_NAME")),
            ))
            .init();
        Some(tracer_provider)
    } else {
        subscriber
            .with(OpenTelemetryLayer::new(NoopTracer::new()))
            .init();
        None
    }
}
