use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use clickhouse_client::ArrowClickHouseClient;
use gkg_server::pipeline::OTelPipelineObserver;
use gkg_server::schema_watcher::{SchemaState, SchemaWatcher};
use gkg_server::webserver::create_router;
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, HistogramDataPoint, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use query_engine::pipeline::PipelineObserver;
use tokio::time::sleep;
use tower::ServiceExt;

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

fn extract_histogram_points(
    metric: &opentelemetry_sdk::metrics::data::Metric,
) -> Vec<&HistogramDataPoint<f64>> {
    match metric.data() {
        AggregatedMetrics::F64(MetricData::Histogram(h)) => h.data_points().collect(),
        _ => vec![],
    }
}

fn dummy_client() -> ArrowClickHouseClient {
    ArrowClickHouseClient::new(
        "http://127.0.0.1:1",
        "default",
        "x",
        None,
        &std::collections::HashMap::new(),
    )
}

fn ready_watcher() -> Arc<SchemaWatcher> {
    SchemaWatcher::for_state(SchemaState::Ready, 0)
}

#[tokio::test]
async fn http_request_records_duration_metric() {
    let (provider, exporter) = setup_meter_provider();
    let router = create_router(dummy_client(), None, ready_watcher());

    let request = Request::get("/live").body(Body::empty()).unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    sleep(Duration::from_millis(150)).await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();
    assert!(
        find_metric(&metrics, "http.server.request.duration").is_some(),
        "http.server.request.duration metric should be recorded"
    );

    provider.shutdown().unwrap();
}

#[tokio::test]
async fn http_metric_has_correct_attributes() {
    let (provider, exporter) = setup_meter_provider();
    let router = create_router(dummy_client(), None, ready_watcher());

    let request = Request::get("/live").body(Body::empty()).unwrap();
    router.oneshot(request).await.unwrap();

    sleep(Duration::from_millis(150)).await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();
    let metric =
        find_metric(&metrics, "http.server.request.duration").expect("metric should exist");
    let points = extract_histogram_points(metric);
    assert!(!points.is_empty());

    let dp = points[0];
    let has_method = dp
        .attributes()
        .any(|kv| kv.key.as_str() == "http.request.method" && kv.value.as_str() == "GET");
    let has_status = dp
        .attributes()
        .any(|kv| kv.key.as_str() == "http.response.status_code" && kv.value.as_str() == "200");
    let has_route = dp
        .attributes()
        .any(|kv| kv.key.as_str() == "http.route" && kv.value.as_str() == "/live");

    assert!(has_method, "should have http.request.method=GET");
    assert!(has_status, "should have http.response.status_code=200");
    assert!(has_route, "should have http.route=/live");

    provider.shutdown().unwrap();
}

#[tokio::test]
async fn correlation_id_echoed_in_response() {
    let _guard = labkit::Builder::new("test")
        .propagate_correlation(true)
        .echo_response_header(true)
        .init()
        .expect("labkit init");

    let router = create_router(dummy_client(), None, ready_watcher());

    let request = Request::get("/live")
        .header("x-request-id", "test-correlation-789")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();

    assert_eq!(
        response.headers().get("x-request-id").unwrap(),
        "test-correlation-789"
    );
}

#[tokio::test]
async fn correlation_id_generated_when_absent() {
    let _guard = labkit::Builder::new("test")
        .echo_response_header(true)
        .init()
        .expect("labkit init");

    let router = create_router(dummy_client(), None, ready_watcher());

    let request = Request::get("/live").body(Body::empty()).unwrap();
    let response = router.oneshot(request).await.unwrap();

    let header = response
        .headers()
        .get("x-request-id")
        .expect("response should have x-request-id");
    assert_eq!(header.len(), 26, "should be a 26-char ULID");
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
