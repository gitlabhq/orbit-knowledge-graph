use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use orbit_server_config::CodeIndexingPipelineConfig;
use orbit_utils::vfs::{
    LimitKind, Limits, Options as StorageOptions, SourceError, Vfs, sources::Archive,
};
use orbit_utils::vfs_policies::CodePolicy;
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::repository::{ByteStream, cache::RepositoryCacheError};

mod output;
#[cfg(test)]
mod policy;
mod run;
pub(super) use code_graph_incremental::templates::Options;
pub(super) use run::run;
#[cfg(test)]
mod tests;

pub type Repository = Arc<Vfs<()>>;

struct CancelLoad(Arc<AtomicBool>);

impl Drop for CancelLoad {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(super) fn limits(config: &CodeIndexingPipelineConfig) -> code_graph_incremental::Limits {
    let budget = |ms: u64| if ms == 0 { u64::MAX } else { ms };
    code_graph_incremental::Limits {
        max_files: config.max_files,
        file_parse_ms: budget(config.per_file_parse_timeout_ms),
        file_rewrite_ms: budget(config.per_file_walk_timeout_ms),
        file_link_ms: budget(config.per_file_ssa_timeout_ms),
        file_resolve_ms: budget(config.per_file_timeout_ms),
        cross_file_resolve_ms: budget(config.cross_file_resolve_timeout_ms),
        ..code_graph_incremental::Limits::UNLIMITED
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
    let options = StorageOptions {
        compress_spill: config.source_compress_spill,
        cancelled: Some(Box::new(move || cancelled.load(Ordering::Relaxed))),
        ..StorageOptions::default()
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
