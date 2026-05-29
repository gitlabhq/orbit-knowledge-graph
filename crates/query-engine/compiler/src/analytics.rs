//! Pipeline execution metrics for analytics and billing.

use std::time::Duration;

use serde::Serialize;

use crate::input::Input;
use crate::passes::hydrate::HydrationPlan;

/// Accumulated pipeline execution metrics. Embedded by observer impls.
///
/// Fields are `pub` -- observers write directly, no setters needed.
/// `query_executed` is the one exception: it accumulates across multiple
/// ClickHouse round-trips.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecMetrics {
    #[serde(skip)]
    pub input: Option<Input>,
    #[serde(skip)]
    pub hydration: Option<HydrationPlan>,
    pub compile_ms: Option<u64>,
    pub execute_ms: Option<u64>,
    pub authorization_ms: Option<u64>,
    pub hydration_ms: Option<u64>,
    pub ch_read_rows: u64,
    pub ch_read_bytes: u64,
    pub ch_memory_usage: u64,
}

impl ExecMetrics {
    pub fn ms(d: Duration) -> u64 {
        d.as_millis().min(u64::MAX as u128) as u64
    }

    pub fn query_executed(&mut self, read_rows: u64, read_bytes: u64, memory: i64) {
        self.ch_read_rows += read_rows;
        self.ch_read_bytes += read_bytes;
        if memory > 0 {
            self.ch_memory_usage = self.ch_memory_usage.max(memory as u64);
        }
    }
}
