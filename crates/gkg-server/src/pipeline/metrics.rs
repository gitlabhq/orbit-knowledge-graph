use std::sync::LazyLock;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};

use gkg_observability::query::pipeline as spec;
use query_engine::pipeline::{PipelineError, PipelineObserver};

static METRICS: LazyLock<QueryPipelineMetrics> = LazyLock::new(QueryPipelineMetrics::new);

struct QueryPipelineMetrics {
    queries: Counter<u64>,
    compile_duration: Histogram<f64>,
    pipeline_duration: Histogram<f64>,
    execute_duration: Histogram<f64>,
    authorization_duration: Histogram<f64>,
    hydration_duration: Histogram<f64>,
    result_set_rows: Histogram<u64>,
    batches: Histogram<u64>,
    redactions: Histogram<u64>,
    ch_read_rows: Counter<u64>,
    ch_read_bytes: Counter<u64>,
    ch_memory_usage: Histogram<u64>,
    failed: Counter<u64>,
}

impl QueryPipelineMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            queries: spec::QUERIES.build_counter_u64(&meter),
            compile_duration: spec::COMPILE_DURATION.build_histogram_f64(&meter),
            pipeline_duration: spec::PIPELINE_DURATION.build_histogram_f64(&meter),
            execute_duration: spec::EXECUTE_DURATION.build_histogram_f64(&meter),
            authorization_duration: spec::AUTHORIZATION_DURATION.build_histogram_f64(&meter),
            hydration_duration: spec::HYDRATION_DURATION.build_histogram_f64(&meter),
            result_set_rows: spec::RESULT_SET_ROWS.build_histogram_u64(&meter),
            batches: spec::BATCHES.build_histogram_u64(&meter),
            redactions: spec::REDACTIONS.build_histogram_u64(&meter),
            ch_read_rows: spec::CH_READ_ROWS.build_counter_u64(&meter),
            ch_read_bytes: spec::CH_READ_BYTES.build_counter_u64(&meter),
            ch_memory_usage: spec::CH_MEMORY_USAGE.build_histogram_u64(&meter),
            failed: spec::FAILED.build_counter_u64(&meter),
        }
    }
}

/// Closed-enum mapping for `gkg.query.pipeline.failed{failure_reason}`.
/// Returns `None` for `Compile` because compile-time rejections are
/// counted on `gkg.query.engine.compiler.rejected` instead.
fn failure_reason(err: &PipelineError) -> Option<&'static str> {
    match err {
        PipelineError::Security(_) => Some("security"),
        PipelineError::Compile { .. } => None,
        PipelineError::Execution(_) => Some("execution"),
        PipelineError::Authorization(_) => Some("authorization"),
        PipelineError::ContentResolution(_) => Some("content_resolution"),
        PipelineError::Streaming(_) => Some("streaming"),
        PipelineError::Custom(_) => Some("custom"),
    }
}

/// OpenTelemetry-backed pipeline observer for the server.
pub struct OTelPipelineObserver {
    query_type: &'static str,
    start: Instant,
    compile_secs: f64,
    execute_secs: f64,
    authorization_secs: f64,
    hydration_secs: f64,
    batch_count: usize,
}

impl OTelPipelineObserver {
    pub fn start() -> Self {
        Self {
            query_type: "unknown",
            start: Instant::now(),
            compile_secs: 0.0,
            execute_secs: 0.0,
            authorization_secs: 0.0,
            hydration_secs: 0.0,
            batch_count: 0,
        }
    }
}

impl PipelineObserver for OTelPipelineObserver {
    fn set_query_type(&mut self, query_type: &'static str) {
        self.query_type = query_type;
    }

    fn compiled(&mut self, elapsed: Duration) {
        self.compile_secs = elapsed.as_secs_f64();
    }

    fn executed(&mut self, elapsed: Duration, batch_count: usize) {
        self.execute_secs = elapsed.as_secs_f64();
        self.batch_count = batch_count;
    }

    fn authorized(&mut self, elapsed: Duration) {
        self.authorization_secs = elapsed.as_secs_f64();
    }

    fn hydrated(&mut self, elapsed: Duration) {
        self.hydration_secs = elapsed.as_secs_f64();
    }

    fn query_executed(&mut self, label: &str, read_rows: u64, read_bytes: u64, memory: i64) {
        let static_label: &'static str = match label {
            "base" => "base",
            "hydration:static" => "hydration:static",
            "hydration:dynamic" => "hydration:dynamic",
            _ => "other",
        };
        let attrs = [
            KeyValue::new(spec::labels::QUERY_TYPE, self.query_type),
            KeyValue::new(spec::labels::LABEL, static_label),
        ];
        METRICS.ch_read_rows.add(read_rows, &attrs);
        METRICS.ch_read_bytes.add(read_bytes, &attrs);
        if memory > 0 {
            METRICS.ch_memory_usage.record(memory as u64, &attrs);
        }
    }

    fn record_error(&self, err: &PipelineError) {
        let attrs = [
            KeyValue::new(spec::labels::QUERY_TYPE, self.query_type),
            KeyValue::new(spec::labels::STATUS, err.code()),
        ];
        METRICS.queries.add(1, &attrs);
        METRICS
            .pipeline_duration
            .record(self.start.elapsed().as_secs_f64(), &attrs);

        if let Some(reason) = failure_reason(err) {
            METRICS
                .failed
                .add(1, &[KeyValue::new(spec::labels::FAILURE_REASON, reason)]);
        }
    }

    fn finish(&self, row_count: usize, redacted_count: usize) {
        let qt = [KeyValue::new(spec::labels::QUERY_TYPE, self.query_type)];
        let attrs = [
            KeyValue::new(spec::labels::QUERY_TYPE, self.query_type),
            KeyValue::new(spec::labels::STATUS, "ok"),
        ];
        METRICS.queries.add(1, &attrs);
        METRICS
            .pipeline_duration
            .record(self.start.elapsed().as_secs_f64(), &attrs);
        METRICS.compile_duration.record(self.compile_secs, &qt);
        METRICS.execute_duration.record(self.execute_secs, &qt);
        METRICS
            .authorization_duration
            .record(self.authorization_secs, &qt);
        METRICS.hydration_duration.record(self.hydration_secs, &qt);
        METRICS.batches.record(self.batch_count as u64, &qt);
        METRICS.result_set_rows.record(row_count as u64, &qt);
        METRICS.redactions.record(redacted_count as u64, &qt);
    }
}
