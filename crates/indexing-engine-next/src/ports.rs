use std::sync::Arc;

use crate::clock::Clock;
use crate::event::ProtocolLog;
use crate::lease::LeaseStore;
use crate::pending::PendingStore;
use crate::task::TaskQueue;

#[derive(Clone)]
pub(crate) struct Ports {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) queue: Arc<dyn TaskQueue>,
    pub(crate) leases: Arc<dyn LeaseStore>,
    pub(crate) pending: Arc<dyn PendingStore>,
    pub(crate) log: Arc<dyn ProtocolLog>,
}

pub(crate) fn warn_port_failure(failure: &'static str, error: &anyhow::Error) {
    orbit_log_fields::warn!(
        safe(ERROR_TYPE) = failure,
        expanded(ERROR_MESSAGE) = %error,
        "a port call of the runner failed"
    );
}
