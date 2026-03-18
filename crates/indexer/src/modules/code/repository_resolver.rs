use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use gitlab_client::{ChangeStatus, ChangedPath};
use tracing::{debug, info, warn};

use super::archive;
use super::blob_decoder::{BlobIterator, ResolvedBlob};
use super::repository_cache::RepositoryCache;
use super::repository_service::RepositoryService;
use crate::handler::HandlerError;

const SUBMODULE_MODE: u32 = 0o160000;

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
            Err(error) if is_force_push(&error) => {
                warn!(
                    project_id,
                    branch, "force push detected, falling back to full download"
                );
                self.cache
                    .invalidate(project_id, branch)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("cache invalidation failed: {e}"))
                    })?;
                self.full_download(project_id, branch, ref_name).await
            }
            Err(error) => Err(error),
        }
    }

    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<PathBuf, HandlerError> {
        let repo_path = self.cache.repository_path(project_id, branch);

        let archive_bytes = self
            .repository_service
            .download_archive(project_id, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))?;

        if repo_path.exists() {
            tokio::fs::remove_dir_all(&repo_path).await.map_err(|e| {
                HandlerError::Processing(format!("failed to clean cache directory: {e}"))
            })?;
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
    ) -> Result<PathBuf, HandlerError> {
        debug!(
            project_id,
            branch, from_sha, to_sha, "attempting incremental update"
        );

        let changed_paths = self
            .repository_service
            .changed_paths(project_id, from_sha, to_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to fetch changed paths: {e}")))?;

        let changeset = compute_changeset(changed_paths);

        for path in &changeset.deletions {
            self.cache
                .delete_file(project_id, branch, path)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to delete cached file: {e}"))
                })?;
        }

        let blob_bytes = self
            .repository_service
            .download_blobs(project_id, from_sha, to_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download blobs: {e}")))?;

        let mut blob_iter = BlobIterator::new(&blob_bytes);
        let mut write_count = 0;
        while let Some(blob) = blob_iter
            .next_blob()
            .map_err(|e| HandlerError::Processing(format!("failed to decode blob: {e}")))?
        {
            let paths = paths_for_blob(&blob, &changeset.paths_by_blob_id);
            for path in paths {
                self.cache
                    .write_file(project_id, branch, path, &blob.data)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("failed to write cached file: {e}"))
                    })?;
                write_count += 1;
            }
        }

        self.cache
            .update_commit(project_id, branch, to_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to update cache commit: {e}")))?;

        info!(
            project_id,
            branch,
            from_sha,
            to_sha,
            deletions = changeset.deletions.len(),
            writes = write_count,
            "incremental update complete"
        );

        Ok(self.cache.repository_path(project_id, branch))
    }
}

fn is_force_push(error: &HandlerError) -> bool {
    match error {
        HandlerError::Processing(msg) => msg.contains("force push"),
        _ => false,
    }
}

struct IncrementalChangeset {
    deletions: Vec<String>,
    paths_by_blob_id: HashMap<String, Vec<String>>,
}

fn compute_changeset(changed_paths: Vec<ChangedPath>) -> IncrementalChangeset {
    let mut deletions = Vec::new();
    let mut paths_by_blob_id: HashMap<String, Vec<String>> = HashMap::new();

    for change in changed_paths {
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

    IncrementalChangeset {
        deletions,
        paths_by_blob_id,
    }
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
    use super::ResolvedBlob;
    use super::*;

    fn changed_path(
        path: &str,
        status: ChangeStatus,
        old_path: &str,
        old_blob_id: &str,
        new_blob_id: &str,
    ) -> ChangedPath {
        ChangedPath {
            path: path.to_string(),
            status,
            old_path: old_path.to_string(),
            new_mode: 0o100644,
            old_mode: 0o100644,
            old_blob_id: old_blob_id.to_string(),
            new_blob_id: new_blob_id.to_string(),
        }
    }

    fn changed_path_with_modes(
        path: &str,
        status: ChangeStatus,
        old_mode: u32,
        new_mode: u32,
    ) -> ChangedPath {
        ChangedPath {
            path: path.to_string(),
            status,
            old_path: String::new(),
            new_mode,
            old_mode,
            old_blob_id: String::new(),
            new_blob_id: "blob1".to_string(),
        }
    }

    #[test]
    fn deleted_goes_to_deletions() {
        let paths = vec![changed_path(
            "removed.rs",
            ChangeStatus::Deleted,
            "",
            "old",
            "",
        )];

        let changeset = compute_changeset(paths);

        assert_eq!(changeset.deletions, vec!["removed.rs"]);
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[test]
    fn added_goes_to_blob_map() {
        let paths = vec![changed_path("new.rs", ChangeStatus::Added, "", "", "blob1")];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["new.rs"]);
    }

    #[test]
    fn modified_goes_to_blob_map() {
        let paths = vec![changed_path(
            "file.rs",
            ChangeStatus::Modified,
            "",
            "old",
            "new",
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["new"], vec!["file.rs"]);
    }

    #[test]
    fn copied_goes_to_blob_map() {
        let paths = vec![changed_path(
            "copy.rs",
            ChangeStatus::Copied,
            "",
            "blob1",
            "blob1",
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["copy.rs"]);
    }

    #[test]
    fn renamed_creates_deletion_and_blob_entry() {
        let paths = vec![changed_path(
            "new_name.rs",
            ChangeStatus::Renamed,
            "old_name.rs",
            "blob1",
            "blob1",
        )];

        let changeset = compute_changeset(paths);

        assert_eq!(changeset.deletions, vec!["old_name.rs"]);
        assert_eq!(changeset.paths_by_blob_id["blob1"], vec!["new_name.rs"]);
    }

    #[test]
    fn renamed_with_edit_creates_deletion_and_new_blob_entry() {
        let paths = vec![changed_path(
            "new_name.rs",
            ChangeStatus::Renamed,
            "old_name.rs",
            "blob_old",
            "blob_new",
        )];

        let changeset = compute_changeset(paths);

        assert_eq!(changeset.deletions, vec!["old_name.rs"]);
        assert_eq!(changeset.paths_by_blob_id["blob_new"], vec!["new_name.rs"]);
    }

    #[test]
    fn filters_submodule_by_new_mode() {
        let paths = vec![changed_path_with_modes(
            "submod",
            ChangeStatus::Added,
            0,
            SUBMODULE_MODE,
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[test]
    fn filters_submodule_by_old_mode() {
        let paths = vec![changed_path_with_modes(
            "submod",
            ChangeStatus::Deleted,
            SUBMODULE_MODE,
            0,
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
    }

    #[test]
    fn type_change_is_skipped() {
        let paths = vec![changed_path(
            "file",
            ChangeStatus::TypeChange,
            "",
            "old",
            "new",
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[test]
    fn unknown_status_is_skipped() {
        let paths = vec![changed_path(
            "file",
            ChangeStatus::Unknown,
            "",
            "old",
            "new",
        )];

        let changeset = compute_changeset(paths);

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
    }

    #[test]
    fn same_blob_id_maps_to_multiple_paths() {
        let paths = vec![
            changed_path("a.rs", ChangeStatus::Added, "", "", "shared_blob"),
            changed_path("b.rs", ChangeStatus::Copied, "", "", "shared_blob"),
        ];

        let changeset = compute_changeset(paths);

        let blob_paths = &changeset.paths_by_blob_id["shared_blob"];
        assert_eq!(blob_paths.len(), 2);
        assert!(blob_paths.contains(&"a.rs".to_string()));
        assert!(blob_paths.contains(&"b.rs".to_string()));
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
            size: 7,
            path: "a.rs".to_string(),
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
            size: 4,
            path: "file.rs".to_string(),
        };

        let paths = paths_for_blob(&blob, &paths_by_blob_id);
        assert!(paths.is_empty());
    }
}
