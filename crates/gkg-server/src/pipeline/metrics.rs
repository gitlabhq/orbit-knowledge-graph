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
    security_rejected: Counter<u64>,
    execution_failed: Counter<u64>,
    authorization_failed: Counter<u64>,
    content_resolution_failed: Counter<u64>,
    streaming_failed: Counter<u64>,
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
            security_rejected: spec::ERROR_SECURITY_REJECTED.build_counter_u64(&meter),
            execution_failed: spec::ERROR_EXECUTION_FAILED.build_counter_u64(&meter),
            authorization_failed: spec::ERROR_AUTHORIZATION_FAILED.build_counter_u64(&meter),
            content_resolution_failed: spec::ERROR_CONTENT_RESOLUTION_FAILED
                .build_counter_u64(&meter),
            streaming_failed: spec::ERROR_STREAMING_FAILED.build_counter_u64(&meter),
        }
    }
}

fn counter_info(err: &PipelineError) -> Option<(&Counter<u64>, &'static str)> {
    match err {
        PipelineError::Security(_) => Some((&METRICS.security_rejected, "security")),
        PipelineError::Compile { .. } => None,
        PipelineError::Execution(_) => Some((&METRICS.execution_failed, "execution")),
        PipelineError::Authorization(_) => Some((&METRICS.authorization_failed, "authorization")),
        PipelineError::ContentResolution(_) => {
            Some((&METRICS.content_resolution_failed, "content_resolution"))
        }
        PipelineError::Streaming(_) => Some((&METRICS.streaming_failed, "streaming")),
        PipelineError::Custom(_) => None,
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

        if let Some((counter, reason)) = counter_info(err) {
            counter.add(1, &[KeyValue::new(spec::labels::REASON, reason)]);
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
