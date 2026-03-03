use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use crate::metrics::DURATION_BUCKETS;

#[derive(Clone)]
pub struct DispatchMetrics {
    runs: Counter<u64>,
    duration: Histogram<f64>,
    requests_published: Counter<u64>,
    requests_skipped: Counter<u64>,
    query_duration: Histogram<f64>,
    errors: Counter<u64>,
}

impl DispatchMetrics {
    pub fn new() -> Self {
        let meter = global::meter("indexer_dispatch");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let runs = meter
            .u64_counter("indexer.dispatch.runs")
            .with_description("Total dispatch runs by dispatcher")
            .build();

        let duration = meter
            .f64_histogram("indexer.dispatch.duration")
            .with_unit("s")
            .with_description("End-to-end duration of a dispatch cycle")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let requests_published = meter
            .u64_counter("indexer.dispatch.requests.published")
            .with_description("Namespace/global requests successfully published")
            .build();

        let requests_skipped = meter
            .u64_counter("indexer.dispatch.requests.skipped")
            .with_description("Requests skipped due to lock contention")
            .build();

        let query_duration = meter
            .f64_histogram("indexer.dispatch.query.duration")
            .with_unit("s")
            .with_description("Duration of the enabled-namespaces ClickHouse query")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let errors = meter
            .u64_counter("indexer.dispatch.errors")
            .with_description("Dispatch errors by stage")
            .build();

        Self {
            runs,
            duration,
            requests_published,
            requests_skipped,
            query_duration,
            errors,
        }
    }

    pub fn record_run(&self, dispatcher: &str, outcome: &str, duration: f64) {
        let labels = [
            KeyValue::new("dispatcher", dispatcher.to_owned()),
            KeyValue::new("outcome", outcome.to_owned()),
        ];
        self.runs.add(1, &labels);
        self.duration.record(
            duration,
            &[KeyValue::new("dispatcher", dispatcher.to_owned())],
        );
    }

    pub fn record_requests_published(&self, dispatcher: &str, count: u64) {
        self.requests_published
            .add(count, &[KeyValue::new("dispatcher", dispatcher.to_owned())]);
    }

    pub fn record_requests_skipped(&self, dispatcher: &str, count: u64) {
        self.requests_skipped
            .add(count, &[KeyValue::new("dispatcher", dispatcher.to_owned())]);
    }

    pub fn record_query_duration(&self, duration: f64) {
        self.query_duration.record(duration, &[]);
    }

    pub fn record_error(&self, dispatcher: &str, stage: &str) {
        self.errors.add(
            1,
            &[
                KeyValue::new("dispatcher", dispatcher.to_owned()),
                KeyValue::new("stage", stage.to_owned()),
            ],
        );
    }
}

impl Default for DispatchMetrics {
    fn default() -> Self {
        Self::new()
    }
}
