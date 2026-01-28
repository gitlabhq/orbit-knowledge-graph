#![cfg(feature = "full")]

use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, HistogramDataPoint, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use tokio::time::sleep;
use tower::{Service, ServiceBuilder, ServiceExt};

use labkit_rs::metrics::http::HttpMetricsLayer;

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
    for rm in metrics {
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                if m.name() == name {
                    return Some(m);
                }
            }
        }
    }
    None
}

fn extract_f64_histogram_points(
    metric: &opentelemetry_sdk::metrics::data::Metric,
) -> Vec<&HistogramDataPoint<f64>> {
    match metric.data() {
        AggregatedMetrics::F64(MetricData::Histogram(h)) => h.data_points().collect(),
        _ => vec![],
    }
}

mod http_metrics {
    use super::*;

    async fn dummy_handler(
        _req: Request<Full<Bytes>>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        sleep(Duration::from_millis(10)).await;
        Ok(Response::builder()
            .status(200)
            .body(Full::new(Bytes::from("ok")))
            .unwrap())
    }

    #[tokio::test]
    async fn records_http_request_duration() {
        let (provider, exporter) = setup_meter_provider();

        let mut service = ServiceBuilder::new()
            .layer(HttpMetricsLayer::new())
            .service_fn(dummy_handler);

        let request: Request<Full<Bytes>> = Request::builder()
            .method("GET")
            .uri("/test")
            .body(Full::new(Bytes::new()))
            .unwrap();

        let _ = service.ready().await.unwrap().call(request).await;

        // Allow time for metrics to be collected
        sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();

        let metrics = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&metrics, "http.server.request.duration");

        assert!(
            duration_metric.is_some(),
            "http.server.request.duration metric not found"
        );

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty(), "histogram should have data points");

            let dp = data_points[0];
            assert!(dp.count() > 0, "histogram count should be > 0");
            assert!(dp.sum() > 0.0, "histogram sum should be > 0");
        }

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn records_multiple_requests_with_different_status_codes() {
        let (provider, exporter) = setup_meter_provider();

        async fn handler_with_status(
            req: Request<Full<Bytes>>,
        ) -> Result<Response<Full<Bytes>>, Infallible> {
            let status = if req.uri().path() == "/error" {
                500
            } else {
                200
            };
            Ok(Response::builder()
                .status(status)
                .body(Full::new(Bytes::from("ok")))
                .unwrap())
        }

        let mut service = ServiceBuilder::new()
            .layer(HttpMetricsLayer::new())
            .service_fn(handler_with_status);

        // Make successful request
        let req1: Request<Full<Bytes>> = Request::builder()
            .method("GET")
            .uri("/ok")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let _ = service.ready().await.unwrap().call(req1).await;

        // Make error request
        let req2: Request<Full<Bytes>> = Request::builder()
            .method("POST")
            .uri("/error")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let _ = service.ready().await.unwrap().call(req2).await;

        provider.force_flush().unwrap();

        let metrics = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&metrics, "http.server.request.duration");

        assert!(duration_metric.is_some());

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty(), "should have recorded data points");
        }

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn records_http_route_from_axum_router() {
        use axum::{Router, routing::get};
        use tower::ServiceExt;

        let (provider, exporter) = setup_meter_provider();

        async fn user_handler() -> &'static str {
            "ok"
        }

        let app = Router::new()
            .route("/users/{id}", get(user_handler))
            .layer(HttpMetricsLayer::new());

        let request = Request::builder()
            .method("GET")
            .uri("/users/123")
            .body(axum::body::Body::empty())
            .unwrap();

        let _ = app.oneshot(request).await;

        sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();

        let metrics = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&metrics, "http.server.request.duration");

        assert!(duration_metric.is_some());

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty());

            let dp = data_points[0];
            let has_route = dp
                .attributes()
                .any(|kv| kv.key.as_str() == "http.route" && kv.value.as_str() == "/users/{id}");
            assert!(
                has_route,
                "should have http.route attribute with value /users/{{id}}"
            );
        }

        provider.shutdown().unwrap();
    }
}

mod grpc_metrics {
    use super::*;
    use async_stream::stream;
    use futures_util::StreamExt;
    use labkit_rs::metrics::grpc::GrpcMetrics;
    use tonic::Status;

    #[tokio::test]
    async fn records_unary_call_duration() {
        let (provider, exporter) = setup_meter_provider();

        let metrics = GrpcMetrics::new();

        let result: Result<String, Status> = metrics
            .record("TestService", "UnaryMethod", || async {
                sleep(Duration::from_millis(10)).await;
                Ok("response".to_string())
            })
            .await;

        assert!(result.is_ok());

        // Allow time for metrics to be collected
        sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();

        let exported = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&exported, "rpc.server.duration");

        assert!(
            duration_metric.is_some(),
            "rpc.server.duration metric not found"
        );

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty(), "expected histogram data points");

            let dp = data_points[0];
            assert!(dp.count() > 0);
            assert!(dp.sum() >= 0.01, "duration should be at least 10ms");
        }

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn records_error_status_code() {
        let (provider, exporter) = setup_meter_provider();

        let metrics = GrpcMetrics::new();

        let result: Result<String, Status> = metrics
            .record("TestService", "FailingMethod", || async {
                Err(Status::internal("something went wrong"))
            })
            .await;

        assert!(result.is_err());

        provider.force_flush().unwrap();

        let exported = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&exported, "rpc.server.duration");

        assert!(duration_metric.is_some());

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty());

            let dp = data_points[0];
            let has_status_attr = dp
                .attributes()
                .any(|kv| kv.key.as_str() == "rpc.grpc.status_code" && kv.value.as_str() != "0");
            assert!(
                has_status_attr,
                "should have non-zero status code attribute"
            );
        }

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn records_streaming_duration() {
        let (provider, exporter) = setup_meter_provider();

        let metrics = GrpcMetrics::new();

        let inner_stream = stream! {
            for i in 0..3 {
                sleep(Duration::from_millis(10)).await;
                yield Ok::<_, Status>(format!("item {}", i));
            }
        };

        let metered = metrics.record_stream("TestService", "StreamMethod", inner_stream);

        // Consume the stream
        let items: Vec<_> = metered.collect().await;
        assert_eq!(items.len(), 3);
        assert!(items.iter().all(|r| r.is_ok()));

        // Give time for drop to be called and metrics recorded
        sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();

        let exported = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&exported, "rpc.server.duration");

        assert!(
            duration_metric.is_some(),
            "rpc.server.duration metric not found for streaming"
        );

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);
            assert!(!data_points.is_empty());

            let dp = data_points[0];
            // Stream should take at least 30ms (3 items * 10ms each)
            assert!(
                dp.sum() >= 0.03,
                "streaming duration should be >= 30ms, got {}",
                dp.sum()
            );
        }

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn records_streaming_error_status() {
        let (provider, exporter) = setup_meter_provider();

        let metrics = GrpcMetrics::new();

        let inner_stream = stream! {
            yield Ok::<_, Status>("item 1".to_string());
            yield Err(Status::resource_exhausted("too many"));
        };

        let metered = metrics.record_stream("TestService", "FailingStream", inner_stream);

        let items: Vec<_> = metered.collect().await;
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        assert!(items[1].is_err());

        sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();

        let exported = exporter.get_finished_metrics().unwrap();
        let duration_metric = find_metric(&exported, "rpc.server.duration");

        assert!(duration_metric.is_some());

        if let Some(metric) = duration_metric {
            let data_points = extract_f64_histogram_points(metric);

            // Find the data point for FailingStream
            let failing_dp = data_points.iter().find(|dp| {
                dp.attributes().any(|kv| {
                    kv.key.as_str() == "rpc.method" && kv.value.as_str() == "FailingStream"
                })
            });

            assert!(
                failing_dp.is_some(),
                "should have data point for FailingStream"
            );

            if let Some(dp) = failing_dp {
                let has_error_status = dp.attributes().any(|kv| {
                    kv.key.as_str() == "rpc.grpc.status_code" && kv.value.as_str() != "0"
                });
                assert!(has_error_status, "should record error status from stream");
            }
        }

        provider.shutdown().unwrap();
    }
}
