use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use super::changed_path_stream::ChangeStatus;
use tracing::{debug, info, warn};

use super::blob_stream::{BlobStream, ResolvedBlob};
use super::cache::RepositoryCache;
use super::changed_path_stream::ChangedPathStream;
use super::service::RepositoryService;
use crate::handler::HandlerError;
use crate::modules::code::archive;

const SUBMODULE_MODE: u32 = 0o160000;
const MAX_CHANGED_PATHS: usize = 100_000;

#[derive(Debug)]
enum IncrementalUpdateError {
    ForcePushDetected,
    TooManyChangedPaths,
    Other(String),
}

impl fmt::Display for IncrementalUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForcePushDetected => write!(f, "force push detected"),
            Self::TooManyChangedPaths => {
                write!(f, "too many changed paths (exceeded {MAX_CHANGED_PATHS})")
            }
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl IncrementalUpdateError {
    fn should_fallback_to_full_download(&self) -> bool {
        matches!(self, Self::ForcePushDetected | Self::TooManyChangedPaths)
    }
}

impl From<IncrementalUpdateError> for HandlerError {
    fn from(error: IncrementalUpdateError) -> Self {
        HandlerError::Processing(error.to_string())
    }
}

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        cache: Arc<dyn RepositoryCache>,
    ) -> Self {
        Self {
            repository_service,
            cache,
        }
    }

    pub fn repository_service(&self) -> &Arc<dyn RepositoryService> {
        &self.repository_service
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
    ) -> Result<PathBuf, HandlerError> {
        let ref_name = commit_sha.unwrap_or(branch);

        let cached = self
            .cache
            .get(project_id, branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("cache lookup failed: {e}")))?;

        let Some(cached) = cached else {
            return self.full_download(project_id, branch, ref_name).await;
        };

        if cached.commit == ref_name {
            debug!(
                project_id,
                branch,
                commit = %ref_name,
                "using cached repository"
            );
            return Ok(cached.path);
        }

        match self
            .incremental_update(project_id, branch, &cached.commit, ref_name)
            .await
        {
            Ok(path) => Ok(path),
            Err(error) if error.should_fallback_to_full_download() => {
                warn!(
                    project_id,
                    branch,
                    reason = %error,
                    "falling back to full download"
                );
                self.cache
                    .invalidate(project_id, branch)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("cache invalidation failed: {e}"))
                    })?;
                self.full_download(project_id, branch, ref_name).await
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<PathBuf, HandlerError> {
        let repo_path = self.cache.code_repository_path(project_id, branch);

        let archive_bytes = self
            .repository_service
            .download_archive(project_id, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))?;

        match tokio::fs::remove_dir_all(&repo_path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(HandlerError::Processing(format!(
                    "failed to clean cache directory: {e}"
                )));
            }
        }
        tokio::fs::create_dir_all(&repo_path).await.map_err(|e| {
            HandlerError::Processing(format!("failed to create cache directory: {e}"))
        })?;

        archive::extract_tar_gz(&archive_bytes, &repo_path)
            .map_err(|e| HandlerError::Processing(format!("failed to extract archive: {e}")))?;

        self.cache
            .update_commit(project_id, branch, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to save cache: {e}")))?;

        Ok(repo_path)
    }

    async fn incremental_update(
        &self,
        project_id: i64,
        branch: &str,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<PathBuf, IncrementalUpdateError> {
        debug!(
            project_id,
            branch, from_sha, to_sha, "attempting incremental update"
        );

        let changed_path_stream = self
            .repository_service
            .changed_paths(project_id, from_sha, to_sha)
            .await
            .map_err(|e| {
                if e.is_force_push() {
                    IncrementalUpdateError::ForcePushDetected
                } else {
                    IncrementalUpdateError::Other(format!("failed to fetch changed paths: {e}"))
                }
            })?;

        let changeset = compute_changeset(changed_path_stream).await?;

        for path in &changeset.deletions {
            self.cache
                .delete_file(project_id, branch, path)
                .await
                .map_err(|e| {
                    IncrementalUpdateError::Other(format!("failed to delete cached file: {e}"))
                })?;
        }

        let blob_stream = self
            .repository_service
            .download_blobs(project_id, from_sha, to_sha)
            .await
            .map_err(|e| IncrementalUpdateError::Other(format!("failed to download blobs: {e}")))?;

        let mut blobs = BlobStream::new(blob_stream);
        let mut write_count = 0;
        while let Some(blob) = blobs
            .next_blob()
            .await
            .map_err(|e| IncrementalUpdateError::Other(format!("failed to decode blob: {e}")))?
        {
            let paths = paths_for_blob(&blob, &changeset.paths_by_blob_id);
            for path in paths {
                self.cache
                    .write_file(project_id, branch, path, &blob.data)
                    .await
                    .map_err(|e| {
                        IncrementalUpdateError::Other(format!("failed to write cached file: {e}"))
                    })?;
                write_count += 1;
            }
        }

        self.cache
            .update_commit(project_id, branch, to_sha)
            .await
            .map_err(|e| {
                IncrementalUpdateError::Other(format!("failed to update cache commit: {e}"))
            })?;

        info!(
            project_id,
            branch,
            from_sha,
            to_sha,
            deletions = changeset.deletions.len(),
            writes = write_count,
            "incremental update complete"
        );

        Ok(self.cache.code_repository_path(project_id, branch))
    }
}

#[derive(Debug)]
struct IncrementalChangeset {
    deletions: Vec<String>,
    paths_by_blob_id: HashMap<String, Vec<String>>,
}

async fn compute_changeset(
    stream: super::service::ByteStream,
) -> Result<IncrementalChangeset, IncrementalUpdateError> {
    let mut changed_paths = ChangedPathStream::new(stream);
    let mut deletions = Vec::new();
    let mut paths_by_blob_id: HashMap<String, Vec<String>> = HashMap::new();
    let mut count = 0usize;

    while let Some(change) = changed_paths
        .next_path()
        .await
        .map_err(|e| IncrementalUpdateError::Other(format!("failed to decode changed path: {e}")))?
    {
        count += 1;
        if count > MAX_CHANGED_PATHS {
            return Err(IncrementalUpdateError::TooManyChangedPaths);
        }

        if change.old_mode == SUBMODULE_MODE || change.new_mode == SUBMODULE_MODE {
            continue;
        }

        match change.status {
            ChangeStatus::Deleted => {
                deletions.push(change.path);
            }
            ChangeStatus::Renamed => {
                deletions.push(change.old_path);
                paths_by_blob_id
                    .entry(change.new_blob_id)
                    .or_default()
                    .push(change.path);
            }
            ChangeStatus::Added | ChangeStatus::Modified | ChangeStatus::Copied => {
                paths_by_blob_id
                    .entry(change.new_blob_id)
                    .or_default()
                    .push(change.path);
            }
            ChangeStatus::TypeChange => {
                warn!(path = %change.path, "skipping TYPE_CHANGE entry");
            }
            ChangeStatus::Unknown => {
                warn!(path = %change.path, "skipping unknown change status");
            }
        }
    }

    Ok(IncrementalChangeset {
        deletions,
        paths_by_blob_id,
    })
}

fn paths_for_blob<'a>(
    blob: &ResolvedBlob,
    paths_by_blob_id: &'a HashMap<String, Vec<String>>,
) -> &'a [String] {
    paths_by_blob_id
        .get(&blob.oid)
        .map(|v| v.as_slice())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::ResolvedBlob;
    use super::*;

    fn ndjson_line(
        path: &str,
        status: &str,
        old_path: &str,
        old_blob_id: &str,
        new_blob_id: &str,
    ) -> String {
        format!(
            r#"{{"path":"{path}","status":"{status}","old_path":"{old_path}","new_mode":33188,"old_mode":33188,"old_blob_id":"{old_blob_id}","new_blob_id":"{new_blob_id}"}}"#
        )
    }

    fn ndjson_line_with_modes(path: &str, status: &str, old_mode: u32, new_mode: u32) -> String {
        format!(
            r#"{{"path":"{path}","status":"{status}","old_path":"","new_mode":{new_mode},"old_mode":{old_mode},"old_blob_id":"","new_blob_id":"blob1"}}"#
        )
    }

    fn byte_stream_from_ndjson(
        lines: Vec<String>,
    ) -> crate::modules::code::repository::service::ByteStream {
        let body = lines.join("\n");
        let stream: Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<
                            bytes::Bytes,
                            crate::modules::code::repository::service::RepositoryServiceError,
                        >,
                    > + Send,
            >,
        > = Box::pin(futures::stream::once(async move {
            Ok(bytes::Bytes::from(body))
        }));
        stream
    }

    #[tokio::test]
    async fn deleted_goes_to_deletions() {
        let stream =
            byte_stream_from_ndjson(vec![ndjson_line("removed.rs", "DELETED", "", "old", "")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert_eq!(changeset.deletions, vec!["removed.rs"]);
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[tokio::test]
    async fn added_goes_to_blob_map() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line("new.rs", "ADDED", "", "", "blob1")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["new.rs"]);
    }

    #[tokio::test]
    async fn modified_goes_to_blob_map() {
        let stream =
            byte_stream_from_ndjson(vec![ndjson_line("file.rs", "MODIFIED", "", "old", "new")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["new"], vec!["file.rs"]);
    }

    #[tokio::test]
    async fn copied_goes_to_blob_map() {
        let stream =
            byte_stream_from_ndjson(vec![ndjson_line("copy.rs", "COPIED", "", "blob1", "blob1")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["copy.rs"]);
    }

    #[tokio::test]
    async fn renamed_creates_deletion_and_blob_entry() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line(
            "new_name.rs",
            "RENAMED",
            "old_name.rs",
            "blob1",
            "blob1",
        )]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert_eq!(changeset.deletions, vec!["old_name.rs"]);
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["new_name.rs"]);
    }

    #[tokio::test]
    async fn renamed_with_edit_creates_deletion_and_new_blob_entry() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line(
            "new_name.rs",
            "RENAMED",
            "old_name.rs",
            "blob_old",
            "blob_new",
        )]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert_eq!(changeset.deletions, vec!["old_name.rs"]);
        assert_eq!(changeset.paths_by_blob_id["blob_new"], vec!["new_name.rs"]);
    }

    #[tokio::test]
    async fn filters_submodule_by_new_mode() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line_with_modes(
            "submod",
            "ADDED",
            0,
            SUBMODULE_MODE,
        )]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[tokio::test]
    async fn filters_submodule_by_old_mode() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line_with_modes(
            "submod",
            "DELETED",
            SUBMODULE_MODE,
            0,
        )]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
    }

    #[tokio::test]
    async fn type_change_is_skipped() {
        let stream =
            byte_stream_from_ndjson(vec![ndjson_line("file", "TYPE_CHANGE", "", "old", "new")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[tokio::test]
    async fn unknown_status_is_skipped() {
        let stream =
            byte_stream_from_ndjson(vec![ndjson_line("file", "SOMETHING_NEW", "", "old", "new")]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[tokio::test]
    async fn same_blob_id_maps_to_multiple_paths() {
        let stream = byte_stream_from_ndjson(vec![
            ndjson_line("a.rs", "ADDED", "", "", "shared_blob"),
            ndjson_line("b.rs", "COPIED", "", "", "shared_blob"),
        ]);

        let changeset = compute_changeset(stream).await.unwrap();

        let blob_paths = &changeset.paths_by_blob_id["shared_blob"];
        assert_eq!(blob_paths.len(), 2);
        assert!(blob_paths.contains(&"a.rs".to_string()));
        assert!(blob_paths.contains(&"b.rs".to_string()));
    }

    #[tokio::test]
    async fn exceeding_max_changed_paths_returns_error() {
        let lines: Vec<String> = (0..MAX_CHANGED_PATHS + 1)
            .map(|i| {
                ndjson_line(
                    &format!("file_{i}.rs"),
                    "ADDED",
                    "",
                    "",
                    &format!("blob_{i}"),
                )
            })
            .collect();
        let stream = byte_stream_from_ndjson(lines);

        let err = compute_changeset(stream).await.unwrap_err();
        assert!(matches!(err, IncrementalUpdateError::TooManyChangedPaths));
    }

    #[test]
    fn paths_for_blob_returns_all_matching_paths() {
        let mut paths_by_blob_id = HashMap::new();
        paths_by_blob_id.insert(
            "blob1".to_string(),
            vec!["a.rs".to_string(), "b.rs".to_string()],
        );

        let blob = ResolvedBlob {
            oid: "blob1".to_string(),
            data: b"content".to_vec(),
        };

        let paths = paths_for_blob(&blob, &paths_by_blob_id);
        assert_eq!(paths, &["a.rs", "b.rs"]);
    }

    #[test]
    fn paths_for_blob_returns_empty_for_unmatched() {
        let paths_by_blob_id = HashMap::new();
        let blob = ResolvedBlob {
            oid: "orphan".to_string(),
            data: b"data".to_vec(),
        };

        let paths = paths_for_blob(&blob, &paths_by_blob_id);
        assert!(paths.is_empty());
    }
}
