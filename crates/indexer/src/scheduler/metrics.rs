use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use crate::metrics::DURATION_BUCKETS;

#[derive(Clone)]
pub struct ScheduledTaskMetrics {
    runs: Counter<u64>,
    duration: Histogram<f64>,
    requests_published: Counter<u64>,
    requests_skipped: Counter<u64>,
    query_duration: Histogram<f64>,
    errors: Counter<u64>,
}

impl ScheduledTaskMetrics {
    pub fn new() -> Self {
        let meter = global::meter("gkg_scheduler");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let runs = meter
            .u64_counter("gkg.scheduler.task.runs")
            .with_description("Total scheduled task runs")
            .build();

        let duration = meter
            .f64_histogram("gkg.scheduler.task.duration")
            .with_unit("s")
            .with_description("End-to-end duration of a scheduled task run")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let requests_published = meter
            .u64_counter("gkg.scheduler.task.requests.published")
            .with_description("Requests successfully published")
            .build();

        let requests_skipped = meter
            .u64_counter("gkg.scheduler.task.requests.skipped")
            .with_description("Requests skipped because already in-flight")
            .build();

        let query_duration = meter
            .f64_histogram("gkg.scheduler.task.query.duration")
            .with_unit("s")
            .with_description("Duration of a scheduled task ClickHouse query")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let errors = meter
            .u64_counter("gkg.scheduler.task.errors")
            .with_description("Scheduled task errors by stage")
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

    pub fn record_run(&self, task: &str, outcome: &str, duration: f64) {
        let labels = [
            KeyValue::new("task", task.to_owned()),
            KeyValue::new("outcome", outcome.to_owned()),
        ];
        self.runs.add(1, &labels);
        self.duration
            .record(duration, &[KeyValue::new("task", task.to_owned())]);
    }

    pub fn record_requests_published(&self, task: &str, count: u64) {
        self.requests_published
            .add(count, &[KeyValue::new("task", task.to_owned())]);
    }

    pub fn record_requests_skipped(&self, task: &str, count: u64) {
        self.requests_skipped
            .add(count, &[KeyValue::new("task", task.to_owned())]);
    }

    pub fn record_query_duration(&self, query: &str, duration: f64) {
        self.query_duration
            .record(duration, &[KeyValue::new("query", query.to_owned())]);
    }

    pub fn record_error(&self, task: &str, stage: &str) {
        self.errors.add(
            1,
            &[
                KeyValue::new("task", task.to_owned()),
                KeyValue::new("stage", stage.to_owned()),
            ],
        );
    }
}

impl Default for ScheduledTaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}
