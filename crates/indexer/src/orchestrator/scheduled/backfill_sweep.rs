//! Coverage-driven code-backfill sweep.
//!
//! On each tick, dispatches code indexing for every enabled namespace whose
//! projects aren't yet checkpointed for the current schema version. This is
//! deliberately coverage-driven rather than gated on a `migrating` version
//! (commit 0d0a33c9a): a migration-gated sweep stranded projects that weren't
//! indexed during the brief migration window. The scheduler's cadence lock
//! keeps a single replica running the enumeration per interval.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::warn;
use uuid::Uuid;

use crate::orchestrator::dispatch::CodeBackfill;
use crate::orchestrator::dispatch::code_backfill::METRIC_NAME;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use gkg_server_config::{CodeBackfillSweepConfig, ScheduleConfiguration};

pub struct CodeBackfillSweep {
    code_backfill: Arc<CodeBackfill>,
    config: CodeBackfillSweepConfig,
}

impl CodeBackfillSweep {
    pub fn new(code_backfill: Arc<CodeBackfill>, config: CodeBackfillSweepConfig) -> Self {
        Self {
            code_backfill,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for CodeBackfillSweep {
    fn name(&self) -> &str {
        METRIC_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        // Datalake unreachable is transient: record the failure and retry next
        // tick rather than treating it as a task error.
        let result = self.code_backfill.dispatch_enabled(Uuid::new_v4()).await;
        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.code_backfill
            .metrics()
            .record_run(METRIC_NAME, outcome, duration);

        if let Err(error) = result {
            warn!(%error, "active code backfill sweep failed, retrying next tick");
        }
        Ok(())
    }
}
