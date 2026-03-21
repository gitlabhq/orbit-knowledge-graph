use std::time::Duration;

use crate::error::PipelineError;

/// Trait for observing pipeline stage timings and outcomes.
pub trait PipelineObserver: Send {
    fn set_query_type(&mut self, query_type: &'static str);
    fn compiled(&mut self, elapsed: Duration);
    fn executed(&mut self, elapsed: Duration, batch_count: usize);
    fn authorized(&mut self, elapsed: Duration);
    fn hydrated(&mut self, elapsed: Duration);

    /// Called for each ClickHouse query execution (base and hydration queries).
    fn query_executed(&mut self, _label: &str, _read_rows: u64, _read_bytes: u64, _memory: i64) {}

    /// Record an error that occurred during pipeline execution.
    fn record_error(&self, error: &PipelineError);

    /// Record all metrics for a successful pipeline run.
    fn finish(&self, row_count: usize, redacted_count: usize);
}

/// No-op observer for local/CLI usage that doesn't need metrics.
pub struct NoOpObserver;

impl PipelineObserver for NoOpObserver {
    fn set_query_type(&mut self, _query_type: &'static str) {}
    fn compiled(&mut self, _elapsed: Duration) {}
    fn executed(&mut self, _elapsed: Duration, _batch_count: usize) {}
    fn authorized(&mut self, _elapsed: Duration) {}
    fn hydrated(&mut self, _elapsed: Duration) {}
    fn record_error(&self, _error: &PipelineError) {}
    fn finish(&self, _row_count: usize, _redacted_count: usize) {}
}
