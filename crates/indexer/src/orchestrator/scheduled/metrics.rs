use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use gkg_observability::indexer::scheduler;

#[derive(Clone)]
pub struct ScheduledTaskMetrics {
    runs: Counter<u64>,
    duration: Histogram<f64>,
    requests_published: Counter<u64>,
    requests_skipped: Counter<u64>,
    query_duration: Histogram<f64>,
    errors: Counter<u64>,
    dirty_namespaces: Histogram<f64>,
    dirty_detection_query_duration: Histogram<f64>,
    dirty_detection_read_rows: Histogram<f64>,
    sweep_only_dispatched: Counter<u64>,
}

impl ScheduledTaskMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            runs: scheduler::RUNS.build_counter_u64(meter),
            duration: scheduler::DURATION.build_histogram_f64(meter),
            requests_published: scheduler::REQUESTS_PUBLISHED.build_counter_u64(meter),
            requests_skipped: scheduler::REQUESTS_SKIPPED.build_counter_u64(meter),
            query_duration: scheduler::QUERY_DURATION.build_histogram_f64(meter),
            errors: scheduler::ERRORS.build_counter_u64(meter),
            dirty_namespaces: scheduler::DIRTY_NAMESPACES.build_histogram_f64(meter),
            dirty_detection_query_duration: scheduler::DIRTY_DETECTION_QUERY_DURATION
                .build_histogram_f64(meter),
            dirty_detection_read_rows: scheduler::DIRTY_DETECTION_READ_ROWS
                .build_histogram_f64(meter),
            sweep_only_dispatched: scheduler::SWEEP_ONLY_DISPATCHED.build_counter_u64(meter),
        }
    }

    pub fn record_run(&self, task: &str, outcome: &str, duration: f64) {
        let labels = [
            KeyValue::new(scheduler::labels::TASK, task.to_owned()),
            KeyValue::new(scheduler::labels::OUTCOME, outcome.to_owned()),
        ];
        self.runs.add(1, &labels);
        self.duration.record(
            duration,
            &[KeyValue::new(scheduler::labels::TASK, task.to_owned())],
        );
    }

    pub fn record_requests_published(&self, task: &str, count: u64) {
        self.requests_published.add(
            count,
            &[KeyValue::new(scheduler::labels::TASK, task.to_owned())],
        );
    }

    pub fn record_requests_skipped(&self, task: &str, count: u64) {
        self.requests_skipped.add(
            count,
            &[KeyValue::new(scheduler::labels::TASK, task.to_owned())],
        );
    }

    pub fn record_query_duration(&self, query: &str, duration: f64) {
        self.query_duration.record(
            duration,
            &[KeyValue::new(scheduler::labels::QUERY, query.to_owned())],
        );
    }

    pub fn record_error(&self, task: &str, stage: &str) {
        self.errors.add(
            1,
            &[
                KeyValue::new(scheduler::labels::TASK, task.to_owned()),
                KeyValue::new(scheduler::labels::STAGE, stage.to_owned()),
            ],
        );
    }

    pub fn record_dirty_namespaces(&self, count: f64) {
        self.dirty_namespaces.record(count, &[]);
    }

    pub fn record_dirty_detection_query(&self, table: &str, duration: f64, read_rows: f64) {
        let label = [KeyValue::new(scheduler::labels::TABLE, table.to_owned())];
        self.dirty_detection_query_duration.record(duration, &label);
        self.dirty_detection_read_rows.record(read_rows, &label);
    }

    pub fn record_sweep_only_dispatched(&self, count: u64) {
        self.sweep_only_dispatched.add(count, &[]);
    }
}

impl Default for ScheduledTaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}
