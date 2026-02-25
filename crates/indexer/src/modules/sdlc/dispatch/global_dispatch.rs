use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::dispatcher::{DispatchError, Dispatcher};
use crate::locking::LockService;
use crate::modules::sdlc::locking::{LOCK_TTL, global_lock_key};
use crate::nats::NatsServices;
use crate::topic::GlobalIndexingRequest;
use crate::types::{Envelope, Event};

pub struct GlobalDispatcher {
    nats: Arc<dyn NatsServices>,
    lock_service: Arc<dyn LockService>,
}

impl GlobalDispatcher {
    pub fn new(nats: Arc<dyn NatsServices>, lock_service: Arc<dyn LockService>) -> Self {
        Self { nats, lock_service }
    }
}

#[async_trait]
impl Dispatcher for GlobalDispatcher {
    fn name(&self) -> &str {
        "sdlc.global"
    }

    async fn dispatch(&self) -> Result<(), DispatchError> {
        let acquired = self
            .lock_service
            .try_acquire(global_lock_key(), LOCK_TTL)
            .await
            .map_err(DispatchError::new)?;

        if !acquired {
            info!("skipping global indexing request, lock already held");
            return Ok(());
        }

        let envelope = Envelope::new(&GlobalIndexingRequest {
            watermark: Utc::now(),
        })
        .map_err(DispatchError::new)?;

        self.nats
            .publish(&GlobalIndexingRequest::topic(), &envelope)
            .await
            .map_err(DispatchError::new)?;

        info!("dispatched global indexing request");
        Ok(())
    }
}
