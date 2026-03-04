use std::sync::LazyLock;
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

use super::error::PipelineError;
use super::types::PipelineOutput;

static METRICS: LazyLock<QueryPipelineMetrics> = LazyLock::new(QueryPipelineMetrics::new);

struct QueryPipelineMetrics {
    queries_total: Counter<u64>,
    compile_duration_ms: Histogram<f64>,
    pipeline_duration_ms: Histogram<f64>,
    execute_duration_ms: Histogram<f64>,
    authorization_duration_ms: Histogram<f64>,
    hydration_duration_ms: Histogram<f64>,
    result_set_size: Histogram<u64>,
    node_count: Histogram<u64>,
    redacted_count: Histogram<u64>,
    security_rejected: Counter<u64>,
    execution_failed: Counter<u64>,
    authorization_failed: Counter<u64>,
}

impl QueryPipelineMetrics {
    fn new() -> Self {
        let meter = global::meter("query_pipeline");

        Self {
            queries_total: meter
                .u64_counter("qp.queries_total")
                .with_description("Total queries processed through the pipeline")
                .build(),
            compile_duration_ms: meter
                .f64_histogram("qp.compile_duration_ms")
                .with_description("Time spent compiling a query from JSON to parameterized SQL")
                .build(),
            pipeline_duration_ms: meter
                .f64_histogram("qp.pipeline_duration_ms")
                .with_description(
                    "End-to-end pipeline duration from security check to formatted output",
                )
                .build(),
            execute_duration_ms: meter
                .f64_histogram("qp.execute_duration_ms")
                .with_description("Time spent executing the compiled query against ClickHouse")
                .build(),
            authorization_duration_ms: meter
                .f64_histogram("qp.authorization_duration_ms")
                .with_description("Time spent on authorization exchange with Rails")
                .build(),
            hydration_duration_ms: meter
                .f64_histogram("qp.hydration_duration_ms")
                .with_description("Time spent hydrating neighbor properties from ClickHouse")
                .build(),
            result_set_size: meter
                .u64_histogram("qp.result_set_size")
                .with_description("Number of rows returned after formatting")
                .build(),
            node_count: meter
                .u64_histogram("qp.node_count")
                .with_description("Number of Arrow record batches returned from ClickHouse")
                .build(),
            redacted_count: meter
                .u64_histogram("qp.redacted_count")
                .with_description("Number of rows redacted per query")
                .build(),
            security_rejected: meter
                .u64_counter("qp.error.security_rejected")
                .with_description("Pipeline rejected due to invalid or missing security context")
                .build(),
            execution_failed: meter
                .u64_counter("qp.error.execution_failed")
                .with_description("ClickHouse query execution failed")
                .build(),
            authorization_failed: meter
                .u64_counter("qp.error.authorization_failed")
                .with_description("Authorization exchange with Rails failed")
                .build(),
        }
    }
}

/// Maps a [`PipelineError`] variant to its error counter and a reason label.
/// Returns `None` for `Compile` — those are already tracked by `qe.threat.*` counters.
fn counter_info(err: &PipelineError) -> Option<(&Counter<u64>, &'static str)> {
    match err {
        PipelineError::Security(_) => Some((&METRICS.security_rejected, "security")),
        PipelineError::Compile(_) => None,
        PipelineError::Execution(_) => Some((&METRICS.execution_failed, "execution")),
        PipelineError::Authorization(_) => Some((&METRICS.authorization_failed, "authorization")),
    }
}

/// Collects per-stage timings and records all pipeline metrics on completion.
pub struct PipelineObserver {
    query_type: &'static str,
    start: Instant,
    compile_ms: f64,
    execute_ms: f64,
    authorization_ms: f64,
    hydration_ms: f64,
    batch_count: usize,
}

impl PipelineObserver {
    pub fn start() -> Self {
        Self {
            query_type: "unknown",
            start: Instant::now(),
            compile_ms: 0.0,
            execute_ms: 0.0,
            authorization_ms: 0.0,
            hydration_ms: 0.0,
            batch_count: 0,
        }
    }

    pub fn set_query_type(&mut self, query_type: &'static str) {
        self.query_type = query_type;
    }

    pub fn compiled(&mut self, elapsed: Duration) {
        self.compile_ms = elapsed.as_secs_f64() * 1000.0;
    }

    pub fn executed(&mut self, elapsed: Duration, batch_count: usize) {
        self.execute_ms = elapsed.as_secs_f64() * 1000.0;
        self.batch_count = batch_count;
    }

    pub fn authorized(&mut self, elapsed: Duration) {
        self.authorization_ms = elapsed.as_secs_f64() * 1000.0;
    }

    pub fn hydrated(&mut self, elapsed: Duration) {
        self.hydration_ms = elapsed.as_secs_f64() * 1000.0;
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Pass a fallible stage result through, recording error metrics if it failed.
    pub fn check<T>(&self, result: Result<T, PipelineError>) -> Result<T, PipelineError> {
        if let Err(ref e) = result {
            record_error(self.query_type, self.start, e);
        }
        result
    }

    /// Record all metrics for a successful pipeline run.
    pub fn finish(self, output: &PipelineOutput) {
        let qt = [KeyValue::new("query_type", self.query_type)];
        let attrs = [
            KeyValue::new("query_type", self.query_type),
            KeyValue::new("status", "ok"),
        ];
        METRICS.queries_total.add(1, &attrs);
        METRICS
            .pipeline_duration_ms
            .record(self.start.elapsed().as_secs_f64() * 1000.0, &attrs);
        METRICS.compile_duration_ms.record(self.compile_ms, &qt);
        METRICS.execute_duration_ms.record(self.execute_ms, &qt);
        METRICS
            .authorization_duration_ms
            .record(self.authorization_ms, &qt);
        METRICS.hydration_duration_ms.record(self.hydration_ms, &qt);
        METRICS.node_count.record(self.batch_count as u64, &qt);
        METRICS.result_set_size.record(output.row_count as u64, &qt);
        METRICS
            .redacted_count
            .record(output.redacted_count as u64, &qt);
    }
}

fn record_error(query_type: &'static str, start: Instant, err: &PipelineError) {
    let attrs = [
        KeyValue::new("query_type", query_type),
        KeyValue::new("status", err.code()),
    ];
    METRICS.queries_total.add(1, &attrs);
    METRICS
        .pipeline_duration_ms
        .record(start.elapsed().as_secs_f64() * 1000.0, &attrs);

    if let Some((counter, reason)) = counter_info(err) {
        counter.add(1, &[KeyValue::new("reason", reason)]);
    }
}
