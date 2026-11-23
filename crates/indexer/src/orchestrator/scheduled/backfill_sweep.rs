use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use orbit_log_fields::warn;
use uuid::Uuid;

use crate::orchestrator::dispatch::CodeBackfill;
use crate::orchestrator::dispatch::code_backfill::METRIC_NAME;
use crate::orchestrator::scheduled::{CodeStaleSweep, ScheduledTask, TaskError};
use orbit_server_config::{CodeBackfillSweepConfig, ScheduleConfiguration};

pub struct CodeBackfillSweep {
    code_backfill: Arc<CodeBackfill>,
    stale_sweep: CodeStaleSweep,
    config: CodeBackfillSweepConfig,
}

impl CodeBackfillSweep {
    pub fn new(
        code_backfill: Arc<CodeBackfill>,
        stale_sweep: CodeStaleSweep,
        config: CodeBackfillSweepConfig,
    ) -> Self {
        Self {
            code_backfill,
            stale_sweep,
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
        let result = self.code_backfill.dispatch_enabled(Uuid::new_v4()).await;
        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.code_backfill
            .metrics()
            .record_run(METRIC_NAME, outcome, duration);

        match result {
            Ok(outcome) => {
                if let Err(error) = self
                    .stale_sweep
                    .run_for_drained(&outcome.drained_paths)
                    .await
                {
                    warn!(
                        safe(ERROR_TYPE) = "stale_sweep",
                        expanded(ERROR_MESSAGE) = %error,
                        "post-backfill stale sweep failed, retrying next tick"
                    );
                }
            }
            Err(error) => {
                warn!(
                    safe(ERROR_TYPE) = "backfill_sweep",
                    expanded(ERROR_MESSAGE) = %error,
                    "active code backfill sweep failed, retrying next tick"
                );
            }
        }
        Ok(())
    }
}
