use std::sync::Arc;

use crate::clock::Clock;
use crate::event::{LogObserver, Observer, Observers};
use crate::lease::LeaseStore;
use crate::pending::PendingStore;
use crate::task::TaskQueue;

#[derive(Clone)]
pub(crate) struct Ports {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) queue: Arc<dyn TaskQueue>,
    pub(crate) leases: Arc<dyn LeaseStore>,
    pub(crate) pending: Arc<dyn PendingStore>,
    pub(crate) observer: Arc<dyn Observer>,
}

impl Ports {
    pub(crate) fn observed_by(self, observers: &[Arc<dyn Observer>]) -> Self {
        let mut all: Vec<Arc<dyn Observer>> = vec![Arc::new(LogObserver), self.observer];
        all.extend(observers.iter().cloned());
        Self {
            observer: Arc::new(Observers::new(all)),
            ..self
        }
    }
}
