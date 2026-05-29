//! Pipeline execution metrics for analytics and billing.

use std::time::Duration;

use serde::Serialize;

use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::hydrate::HydrationPlan;

/// Accumulated pipeline execution metrics. Embedded by observer impls.
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

    pub fn set_compiled(&mut self, ctx: &CompiledQueryContext) {
        self.input = Some(ctx.input.clone());
        self.hydration = Some(ctx.hydration.clone());
    }

    pub fn compiled(&mut self, elapsed: Duration) {
        self.compile_ms = Some(Self::ms(elapsed));
    }
    pub fn executed(&mut self, elapsed: Duration) {
        self.execute_ms = Some(Self::ms(elapsed));
    }
    pub fn authorized(&mut self, elapsed: Duration) {
        self.authorization_ms = Some(Self::ms(elapsed));
    }
    pub fn hydrated(&mut self, elapsed: Duration) {
        self.hydration_ms = Some(Self::ms(elapsed));
    }

    pub fn query_executed(&mut self, read_rows: u64, read_bytes: u64, memory: i64) {
        self.ch_read_rows += read_rows;
        self.ch_read_bytes += read_bytes;
        if memory > 0 {
            self.ch_memory_usage = self.ch_memory_usage.max(memory as u64);
        }
    }

    pub fn hydration_label(&self) -> &'static str {
        match self.hydration.as_ref() {
            Some(HydrationPlan::Static(_)) => "static",
            Some(HydrationPlan::Dynamic(_)) => "dynamic",
            _ => "none",
        }
    }
}
