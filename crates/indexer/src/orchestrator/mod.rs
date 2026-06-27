//! The orchestrator drives indexing: it owns the clock, decides what to index,
//! reacts to Siphon CDC, and dispatches work requests for the indexer to execute.
//!
//! Two trigger models share one trigger-agnostic [`dispatch`] layer:
//!
//! - [`scheduled`] — cron-driven periodic tasks ([`scheduled::Scheduled`]),
//!   including the coverage-driven code-backfill sweep.
//! - [`siphon`] — a continuous, reactive CDC consumer ([`siphon::Siphon`]).
//!
//! Each implements [`Trigger`]; [`launch`] runs them concurrently until one
//! errors or all complete.

pub mod dispatch;
pub mod scheduled;
pub mod siphon;

use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error(transparent)]
    Scheduler(#[from] scheduled::SchedulerError),
    #[error("{count} trigger(s) panicked")]
    Panicked { count: usize },
}

/// A long-running orchestration driver. Each trigger owns its own run loop and
/// exits cleanly when `cancel` is fired.
#[async_trait]
pub trait Trigger: Send + Sync {
    fn name(&self) -> &str;

    async fn run(self: Box<Self>, cancel: CancellationToken) -> Result<(), TriggerError>;
}

/// On the first error, the shared `cancel` token is fired so the remaining
/// triggers shut down, and the originating error is returned.
pub async fn launch(
    triggers: Vec<Box<dyn Trigger>>,
    cancel: CancellationToken,
) -> Result<(), TriggerError> {
    let mut handles = JoinSet::new();

    for trigger in triggers {
        let token = cancel.clone();
        let name = trigger.name().to_owned();
        handles.spawn(async move {
            info!(trigger = name, "trigger started");
            let result = trigger.run(token).await;
            (name, result)
        });
    }

    let mut first_error: Option<TriggerError> = None;
    let mut panicked = 0usize;

    while let Some(joined) = handles.join_next().await {
        match joined {
            Ok((name, Ok(()))) => {
                info!(trigger = name, "trigger stopped");
            }
            Ok((name, Err(error))) => {
                warn!(trigger = name, %error, "trigger failed");
                cancel.cancel();
                first_error.get_or_insert(error);
            }
            Err(error) => {
                warn!(%error, "trigger task panicked");
                cancel.cancel();
                panicked += 1;
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }
    if panicked > 0 {
        return Err(TriggerError::Panicked { count: panicked });
    }
    Ok(())
}
