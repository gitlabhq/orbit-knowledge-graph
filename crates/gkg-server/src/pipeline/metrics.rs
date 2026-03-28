use std::sync::LazyLock;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

use query_engine::pipeline::{PipelineError, PipelineObserver};

const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

static METRICS: LazyLock<QueryPipelineMetrics> = LazyLock::new(QueryPipelineMetrics::new);

struct QueryPipelineMetrics {
    queries: Counter<u64>,
    compile_duration: Histogram<f64>,
    pipeline_duration: Histogram<f64>,
    execute_duration: Histogram<f64>,
    authorization_duration: Histogram<f64>,
    hydration_duration: Histogram<f64>,
    result_set_size: Histogram<u64>,
    batch_count: Histogram<u64>,
    redacted_count: Histogram<u64>,
    security_rejected: Counter<u64>,
    execution_failed: Counter<u64>,
    authorization_failed: Counter<u64>,
    streaming_failed: Counter<u64>,
}

impl QueryPipelineMetrics {
    fn new() -> Self {
        let meter = global::meter("gkg_query_pipeline");

        Self {
            queries: meter
                .u64_counter("gkg.query.pipeline.queries")
                .with_description("Total queries processed through the pipeline")
                .build(),
            compile_duration: meter
                .f64_histogram("gkg.query.pipeline.compile.duration")
                .with_unit("s")
                .with_description("Time spent compiling a query from JSON to parameterized SQL")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            pipeline_duration: meter
                .f64_histogram("gkg.query.pipeline.duration")
                .with_unit("s")
                .with_description(
                    "End-to-end pipeline duration from security check to formatted output",
                )
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            execute_duration: meter
                .f64_histogram("gkg.query.pipeline.execute.duration")
                .with_unit("s")
                .with_description("Time spent executing the compiled query against ClickHouse")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            authorization_duration: meter
                .f64_histogram("gkg.query.pipeline.authorization.duration")
                .with_unit("s")
                .with_description("Time spent on authorization exchange with Rails")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            hydration_duration: meter
                .f64_histogram("gkg.query.pipeline.hydration.duration")
                .with_unit("s")
                .with_description("Time spent hydrating neighbor properties from ClickHouse")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            result_set_size: meter
                .u64_histogram("gkg.query.pipeline.result_set.size")
                .with_description("Number of rows returned after formatting")
                .build(),
            batch_count: meter
                .u64_histogram("gkg.query.pipeline.batch.count")
                .with_description("Number of Arrow record batches returned from ClickHouse")
                .build(),
            redacted_count: meter
                .u64_histogram("gkg.query.pipeline.redacted.count")
                .with_description("Number of rows redacted per query")
                .build(),
            security_rejected: meter
                .u64_counter("gkg.query.pipeline.error.security_rejected")
                .with_description("Pipeline rejected due to invalid or missing security context")
                .build(),
            execution_failed: meter
                .u64_counter("gkg.query.pipeline.error.execution_failed")
                .with_description("ClickHouse query execution failed")
                .build(),
            authorization_failed: meter
                .u64_counter("gkg.query.pipeline.error.authorization_failed")
                .with_description("Authorization exchange with Rails failed")
                .build(),
            streaming_failed: meter
                .u64_counter("gkg.query.pipeline.error.streaming_failed")
                .with_description("Streaming channel unavailable during authorization")
                .build(),
        }
    }
}

fn counter_info(err: &PipelineError) -> Option<(&Counter<u64>, &'static str)> {
    match err {
        PipelineError::Security(_) => Some((&METRICS.security_rejected, "security")),
        PipelineError::Compile(_) => None,
        PipelineError::Execution(_) => Some((&METRICS.execution_failed, "execution")),
        PipelineError::Authorization(_) => Some((&METRICS.authorization_failed, "authorization")),
        PipelineError::ContentResolution(_) => None,
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

    fn record_error(&self, err: &PipelineError) {
        let attrs = [
            KeyValue::new("query_type", self.query_type),
            KeyValue::new("status", err.code()),
        ];
        METRICS.queries.add(1, &attrs);
        METRICS
            .pipeline_duration
            .record(self.start.elapsed().as_secs_f64(), &attrs);

        if let Some((counter, reason)) = counter_info(err) {
            counter.add(1, &[KeyValue::new("reason", reason)]);
        }
    }

    fn finish(&self, row_count: usize, redacted_count: usize) {
        let qt = [KeyValue::new("query_type", self.query_type)];
        let attrs = [
            KeyValue::new("query_type", self.query_type),
            KeyValue::new("status", "ok"),
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
        METRICS.batch_count.record(self.batch_count as u64, &qt);
        METRICS.result_set_size.record(row_count as u64, &qt);
        METRICS.redacted_count.record(redacted_count as u64, &qt);
    }
}
