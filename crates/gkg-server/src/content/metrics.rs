use std::sync::LazyLock;
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

/// Gitaly latency buckets: 1ms to 5s, weighted toward sub-second calls.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

const BATCH_SIZE_BUCKETS: &[f64] = &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0];

const BLOB_BYTES_BUCKETS: &[f64] = &[256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0];

static METRICS: LazyLock<ContentResolutionMetrics> = LazyLock::new(ContentResolutionMetrics::new);

struct ContentResolutionMetrics {
    resolve_duration: Histogram<f64>,
    resolve_total: Counter<u64>,
    batch_size: Histogram<u64>,
    blob_bytes: Histogram<u64>,
    gitaly_calls: Counter<u64>,
}

impl ContentResolutionMetrics {
    fn new() -> Self {
        let meter = global::meter("gkg_content_resolution");

        Self {
            resolve_duration: meter
                .f64_histogram("gkg.content.resolve.duration")
                .with_unit("s")
                .with_description("Time spent resolving content from Gitaly")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            resolve_total: meter
                .u64_counter("gkg.content.resolve")
                .with_description("Total content resolution attempts")
                .build(),
            batch_size: meter
                .u64_histogram("gkg.content.resolve.batch_size")
                .with_description("Number of rows per content resolution batch")
                .with_boundaries(BATCH_SIZE_BUCKETS.to_vec())
                .build(),
            blob_bytes: meter
                .u64_histogram("gkg.content.blob.size")
                .with_unit("By")
                .with_description("Size of resolved blob content in bytes")
                .with_boundaries(BLOB_BYTES_BUCKETS.to_vec())
                .build(),
            gitaly_calls: meter
                .u64_counter("gkg.content.gitaly.calls")
                .with_description("Total list_blobs RPCs issued to Gitaly")
                .build(),
        }
    }
}

/// Records the start of a content resolution batch and returns a guard
/// that records duration and outcome on drop.
pub(crate) fn start_resolve(batch_size: usize) -> ResolveTimer {
    METRICS.batch_size.record(batch_size as u64, &[]);
    ResolveTimer {
        start: Instant::now(),
        outcome: None,
    }
}

/// Records a single list_blobs RPC call to Gitaly.
pub(crate) fn record_gitaly_call() {
    METRICS.gitaly_calls.add(1, &[]);
}

/// Records the byte size of a single resolved blob.
pub(crate) fn record_blob_bytes(bytes: u64) {
    METRICS.blob_bytes.record(bytes, &[]);
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
        let attrs = [KeyValue::new("outcome", outcome)];

        METRICS
            .resolve_duration
            .record(self.start.elapsed().as_secs_f64(), &attrs);
        METRICS.resolve_total.add(1, &attrs);
    }
}
