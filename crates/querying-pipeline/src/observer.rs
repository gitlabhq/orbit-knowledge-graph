use std::time::Duration;

use crate::error::PipelineError;
use crate::types::PipelineOutput;

/// Trait for observing pipeline stage timings and outcomes.
pub trait PipelineObserver: Send {
    fn set_query_type(&mut self, query_type: &'static str);
    fn compiled(&mut self, elapsed: Duration);
    fn executed(&mut self, elapsed: Duration, batch_count: usize);
    fn authorized(&mut self, elapsed: Duration);
    fn hydrated(&mut self, elapsed: Duration);

    /// Record an error that occurred during pipeline execution.
    fn record_error(&self, error: &PipelineError);

    /// Record all metrics for a successful pipeline run.
    fn finish(self: Box<Self>, output: &PipelineOutput);
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
    fn finish(self: Box<Self>, _output: &PipelineOutput) {}
}
