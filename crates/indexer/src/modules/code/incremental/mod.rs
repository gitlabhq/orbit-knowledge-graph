use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use orbit_server_config::CodeIndexingPipelineConfig;
use orbit_utils::vfs::{LimitKind, Limits, Options, SourceError, Vfs, sources::Archive};
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::metrics::CodeMetrics;
use super::repository::{ByteStream, cache::RepositoryCacheError};
use crate::handler::HandlerError;

mod policy;
#[cfg(test)]
mod tests;
use policy::CodePolicy;

pub type Repository = Arc<Vfs<()>>;

struct CancelLoad(Arc<AtomicBool>);

impl Drop for CancelLoad {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(super) async fn load(
    stream: ByteStream,
    config: &CodeIndexingPipelineConfig,
) -> Result<Repository, RepositoryCacheError> {
    let to_cap = |value| (value > 0).then_some(value);
    let limits = Limits {
        file_bytes: to_cap(config.max_file_size_bytes),
        total_bytes: to_cap(config.max_total_bytes),
        resident_bytes: Some(config.source_memory_budget_bytes),
        ..Limits::default()
    };
    let cancel = CancelLoad(Arc::new(AtomicBool::new(false)));
    let cancelled = cancel.0.clone();
    let options = Options {
        compress_spill: config.source_compress_spill,
        cancelled: Some(Box::new(move || cancelled.load(Ordering::Relaxed))),
        ..Options::default()
    };
    let reader = StreamReader::new(stream.map(|result| result.map_err(std::io::Error::other)));
    let handle = tokio::runtime::Handle::current();
    let result = tokio::task::spawn_blocking(move || {
        Vfs::load(
            Archive(SyncIoBridge::new_with_handle(reader, handle)),
            CodePolicy,
            limits,
            options,
        )
    })
    .await
    .map_err(|error| RepositoryCacheError::Archive(error.to_string()))?;
    result.map(Arc::new).map_err(|error| match error {
        SourceError::Empty => RepositoryCacheError::EmptyArchive,
        SourceError::Cap(cap) if cap.metric == LimitKind::TotalBytes => {
            RepositoryCacheError::RepositoryTooLarge
        }
        other => RepositoryCacheError::Archive(other.to_string()),
    })
}

pub(super) fn pending_handoff(repository: Repository, metrics: &CodeMetrics) -> HandlerError {
    for file in repository.files() {
        if let orbit_utils::vfs::Decision::List(reason) = file.decision() {
            metrics.record_archive_entry_skipped(reason, 1, file.size);
        }
    }
    // TODO: pass the filtered VFS to the incremental indexing pipeline.
    HandlerError::Processing("incremental VFS indexing is not implemented yet".into())
}
