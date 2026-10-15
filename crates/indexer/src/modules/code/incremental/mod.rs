use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::StreamExt;
use orbit_server_config::CodeIndexingPipelineConfig;
use orbit_utils::vfs::{
    LimitKind, Limits, Options as StorageOptions, SourceError, Vfs, sources::Archive,
};
use orbit_utils::vfs_policies::CodePolicy;
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::repository::{ByteStream, CancelOnDrop, cache::RepositoryCacheError};

mod output;
#[cfg(test)]
mod policy;
mod run;
pub(super) use code_graph_incremental::templates::Options;
pub(super) use run::run;
#[cfg(test)]
mod tests;

pub type Repository = Arc<Vfs<()>>;

pub(super) fn limits(config: &CodeIndexingPipelineConfig) -> code_graph_incremental::Limits {
    let budget = |ms: u64| if ms == 0 { u64::MAX } else { ms };
    code_graph_incremental::Limits {
        max_files: config.max_files,
        file_parse_ms: budget(config.per_file_parse_timeout_ms),
        file_rewrite_ms: budget(config.per_file_walk_timeout_ms),
        file_link_ms: budget(config.per_file_ssa_timeout_ms),
        file_resolve_ms: budget(config.per_file_timeout_ms),
        cross_file_resolve_ms: budget(config.cross_file_resolve_timeout_ms),
        max_tree_depth: config.max_tree_depth,
        max_cross_file_names: config.max_cross_file_names,
        max_manifest_bytes: config.max_manifest_bytes,
        ..code_graph_incremental::Limits::UNLIMITED
    }
}

pub(super) async fn load(
    stream: ByteStream,
    config: &CodeIndexingPipelineConfig,
) -> Result<Repository, RepositoryCacheError> {
    fn to_cap<T: Default + PartialOrd>(value: T) -> Option<T> {
        (value > T::default()).then_some(value)
    }
    let limits = Limits {
        file_bytes: to_cap(config.max_file_size_bytes),
        total_bytes: to_cap(config.max_total_bytes),
        files: to_cap(config.max_repository_files),
        resident_bytes: Some(config.source_memory_budget_bytes),
        ..Limits::default()
    };
    let cancel = CancelOnDrop::default();
    let cancelled = cancel.flag();
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
        SourceError::Cap(cap) if matches!(cap.metric, LimitKind::TotalBytes | LimitKind::Files) => {
            RepositoryCacheError::RepositoryTooLarge
        }
        other => RepositoryCacheError::Archive(other.to_string()),
    })
}
