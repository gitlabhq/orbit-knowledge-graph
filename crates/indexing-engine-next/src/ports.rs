use std::sync::Arc;

use crate::clock::Clock;
use crate::lease::Lease;
use crate::task::{TaskMessage, TaskQueue};
use crate::versioned::VersionedStore;

#[derive(Clone)]
pub struct Ports {
    pub clock: Arc<dyn Clock>,
    pub queue: Arc<dyn TaskQueue>,
    pub leases: Arc<dyn VersionedStore<Lease>>,
    pub pending: Arc<dyn VersionedStore<TaskMessage>>,
}
