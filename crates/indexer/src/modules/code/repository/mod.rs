// pub: consumed by orbit-server's content resolver for Workhorse wire format parsing.
pub mod blob_stream;
pub mod cache;
pub mod resolver;
pub(crate) mod service;
pub mod storage_lease;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Sets a shared flag when dropped, so a detached blocking reader stops once its owning future is dropped.
#[derive(Default)]
pub(in crate::modules::code) struct CancelOnDrop(Arc<AtomicBool>);

impl CancelOnDrop {
    pub(in crate::modules::code) fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub use cache::{LocalRepositoryCache, RepositoryCache};
pub use resolver::{EmptyRepositoryReason, RepositoryResolver, ResolveError};
pub use service::{
    ByteStream, CachingRepositoryService, RailsRepositoryService, RepositoryService,
    RepositoryServiceError,
};
pub use storage_lease::{ArchiveLeases, LeaseError, LeaseSettings, slot_key};
