pub mod dispatch;
pub mod max_deliveries;
pub mod scheduled;
pub mod siphon;

use async_trait::async_trait;
use orbit_log_fields::{info, warn};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum TriggerError {
    #[error(transparent)]
    Scheduler(#[from] scheduled::SchedulerError),
    #[error("{count} trigger(s) panicked")]
    Panicked { count: usize },
}

#[async_trait]
pub trait Trigger: Send + Sync {
    fn name(&self) -> &str;

    async fn run(self: Box<Self>, cancel: CancellationToken) -> Result<(), TriggerError>;
}

pub async fn launch(
    triggers: Vec<Box<dyn Trigger>>,
    cancel: CancellationToken,
) -> Result<(), TriggerError> {
    let mut handles = JoinSet::new();

    for trigger in triggers {
        let token = cancel.clone();
        let name = trigger.name().to_owned();
        handles.spawn(async move {
            info!(safe(TRIGGER) = name.as_str(), "trigger started");
            let result = trigger.run(token).await;
            (name, result)
        });
    }

    let mut first_error: Option<TriggerError> = None;
    let mut panicked = 0usize;

    while let Some(joined) = handles.join_next().await {
        match joined {
            Ok((name, Ok(()))) => {
                info!(safe(TRIGGER) = name.as_str(), "trigger stopped");
            }
            Ok((name, Err(error))) => {
                warn!(
                    safe(TRIGGER) = name.as_str(),
                    safe(ERROR_TYPE) = <&str>::from(&error),
                    expanded(ERROR_MESSAGE) = %error,
                    "trigger failed"
                );
                cancel.cancel();
                first_error.get_or_insert(error);
            }
            Err(error) => {
                warn!(
                    safe(ERROR_TYPE) = crate::engine::join_error_type(&error),
                    expanded(ERROR_MESSAGE) = %error,
                    "trigger task panicked"
                );
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
