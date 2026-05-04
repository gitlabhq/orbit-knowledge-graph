use std::sync::LazyLock;
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};

use gkg_observability::server::content;

static METRICS: LazyLock<ContentResolutionMetrics> = LazyLock::new(ContentResolutionMetrics::new);

struct ContentResolutionMetrics {
    resolve_duration: Histogram<f64>,
    resolve_total: Counter<u64>,
    batch_size: Histogram<u64>,
    blob_bytes: Histogram<u64>,
    gitaly_calls: Counter<u64>,
    mr_diff_calls: Counter<u64>,
}

impl ContentResolutionMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            resolve_duration: content::RESOLVE_DURATION.build_histogram_f64(&meter),
            resolve_total: content::RESOLVE_TOTAL.build_counter_u64(&meter),
            batch_size: content::BATCH_SIZE_METRIC.build_histogram_u64(&meter),
            blob_bytes: content::BLOB_BYTES_METRIC.build_histogram_u64(&meter),
            gitaly_calls: content::GITALY_CALLS.build_counter_u64(&meter),
            mr_diff_calls: content::MR_DIFF_CALLS.build_counter_u64(&meter),
        }
    }
}

pub(crate) fn start_resolve(batch_size: usize) -> ResolveTimer {
    METRICS.batch_size.record(batch_size as u64, &[]);
    ResolveTimer {
        start: Instant::now(),
        outcome: None,
    }
}

pub(crate) fn record_gitaly_call() {
    METRICS.gitaly_calls.add(1, &[]);
}

pub(crate) fn record_blob_bytes(bytes: u64) {
    METRICS.blob_bytes.record(bytes, &[]);
}

pub(crate) fn record_mr_diff_call(endpoint: &'static str) {
    METRICS
        .mr_diff_calls
        .add(1, &[KeyValue::new(content::labels::ENDPOINT, endpoint)]);
}

pub(crate) struct ResolveTimer {
    start: Instant,
    outcome: Option<&'static str>,
}

impl ResolveTimer {
    pub(crate) fn set_outcome(&mut self, outcome: &'static str) {
        self.outcome = Some(outcome);
    }
}

impl Drop for ResolveTimer {
    fn drop(&mut self) {
        let outcome = self.outcome.unwrap_or("error");
        let attrs = [KeyValue::new(content::labels::OUTCOME, outcome)];

        METRICS
            .resolve_duration
            .record(self.start.elapsed().as_secs_f64(), &attrs);
        METRICS.resolve_total.add(1, &attrs);
    }
}
