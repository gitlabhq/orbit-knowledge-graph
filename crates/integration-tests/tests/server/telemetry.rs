use std::time::Duration;

use opentelemetry::global;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use orbit_server::pipeline::OTelPipelineObserver;
use query_engine::pipeline::PipelineObserver;
use tokio::time::sleep;

fn setup_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone())
        .with_interval(Duration::from_millis(100))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    global::set_meter_provider(provider.clone());
    (provider, exporter)
}

fn find_metric<'a>(
    metrics: &'a [opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Option<&'a opentelemetry_sdk::metrics::data::Metric> {
    metrics.iter().find_map(|rm| {
        rm.scope_metrics()
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == name)
    })
}

#[tokio::test]
async fn pipeline_observer_records_query_metrics() {
    let (provider, exporter) = setup_meter_provider();

    let mut obs = OTelPipelineObserver::start();
    obs.set_query_type("traversal");
    obs.compiled(Duration::from_millis(5));
    obs.executed(Duration::from_millis(50), 3);
    obs.authorized(Duration::from_millis(10));
    obs.hydrated(Duration::from_millis(2));
    obs.finish(42, 3);

    sleep(Duration::from_millis(150)).await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();

    assert!(
        find_metric(&metrics, "gkg.query.pipeline.queries").is_some(),
        "gkg.query.pipeline.queries should be recorded"
    );
    assert!(
        find_metric(&metrics, "gkg.query.pipeline.duration").is_some(),
        "gkg.query.pipeline.duration should be recorded"
    );
    assert!(
        find_metric(&metrics, "gkg.query.pipeline.compile.duration").is_some(),
        "gkg.query.pipeline.compile.duration should be recorded"
    );
    assert!(
        find_metric(&metrics, "gkg.query.pipeline.execute.duration").is_some(),
        "gkg.query.pipeline.execute.duration should be recorded"
    );

    provider.shutdown().unwrap();
}

#[tokio::test]
async fn pipeline_observer_records_ch_resource_metrics() {
    let (provider, exporter) = setup_meter_provider();

    let mut obs = OTelPipelineObserver::start();
    obs.set_query_type("traverse");
    obs.compiled(Duration::from_millis(1));
    obs.executed(Duration::from_millis(20), 1);
    obs.query_executed("base", 5000, 128_000, 4_000_000);
    obs.query_executed("hydration:static", 200, 8_000, 500_000);
    obs.finish(10, 0);

    sleep(Duration::from_millis(150)).await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();

    assert!(
        find_metric(&metrics, "gkg.query.pipeline.ch.read_rows").is_some(),
        "gkg.query.pipeline.ch.read_rows should be recorded"
    );
    assert!(
        find_metric(&metrics, "gkg.query.pipeline.ch.read").is_some(),
        "gkg.query.pipeline.ch.read should be recorded"
    );
    assert!(
        find_metric(&metrics, "gkg.query.pipeline.ch.memory_usage").is_some(),
        "gkg.query.pipeline.ch.memory_usage should be recorded"
    );

    provider.shutdown().unwrap();
}
