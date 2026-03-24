use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, warn};

use super::blob_stream::BlobStream;
use super::cache::RepositoryCache;
use super::cache_budget::RepositoryLease;
use super::changed_path_stream::{ChangeStatus, ChangedPath, ChangedPathStream};
use super::service::RepositoryService;
use crate::handler::HandlerError;
use crate::modules::code::metrics::CodeMetrics;

const SUBMODULE_MODE: u32 = 0o160000;
const MAX_CHANGED_PATHS: usize = 100_000;
const MAX_BLOB_OIDS_PER_REQUEST: usize = 5000;

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
    metrics: CodeMetrics,
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        cache: Arc<dyn RepositoryCache>,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            repository_service,
            cache,
            metrics,
        }
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
    ) -> Result<RepositoryLease, HandlerError> {
        let ref_name = commit_sha.unwrap_or(branch);

        let cached = self
            .cache
            .acquire(project_id, branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("cache lookup failed: {e}")))?;

        let Some((cached, lease)) = cached else {
            self.metrics.record_resolution_strategy("full_download");
            return self.full_download(project_id, branch, ref_name).await;
        };

        if cached.commit == ref_name {
            self.metrics.record_resolution_strategy("cache_hit");
            info!(
                project_id,
                branch,
                commit = %ref_name,
                "using cached repository"
            );
            return Ok(lease);
        }

        match self
            .incremental_update(project_id, branch, &cached.commit, ref_name)
            .await
        {
            Ok(()) => {
                self.metrics.record_resolution_strategy("incremental");
                Ok(lease)
            }
            Err(reason) => {
                self.metrics
                    .record_resolution_strategy("full_download_fallback");
                warn!(project_id, branch, reason, "falling back to full download");
                drop(lease);
                self.full_download(project_id, branch, ref_name).await
            }
        }
    }

    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<RepositoryLease, HandlerError> {
        info!(project_id, branch, commit = %commit_sha, "starting full repository download");

        let archive_stream = self
            .repository_service
            .download_archive(project_id, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))?;

        self.cache
            .extract_archive(project_id, branch, commit_sha, archive_stream)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to extract archive: {e}")))
    }

    async fn incremental_update(
        &self,
        project_id: i64,
        branch: &str,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<(), String> {
        info!(
            project_id,
            branch, from_sha, to_sha, "attempting incremental update"
        );

        let changed_path_stream = self
            .repository_service
            .changed_paths(project_id, from_sha, to_sha)
            .await
            .map_err(|e| format!("failed to fetch changed paths: {e}"))?;

        let changeset = compute_changeset(changed_path_stream).await?;

        for (old_path, new_path) in &changeset.renames {
            self.cache
                .rename_file(project_id, branch, old_path, new_path)
                .await
                .map_err(|e| format!("failed to rename cached file: {e}"))?;
        }

        for path in &changeset.deletions {
            self.cache
                .delete_file(project_id, branch, path)
                .await
                .map_err(|e| format!("failed to delete cached file: {e}"))?;
        }

        let expected_writes: usize = changeset.paths_by_blob_id.values().map(|v| v.len()).sum();
        let blob_oids: Vec<String> = changeset.paths_by_blob_id.keys().cloned().collect();
        let mut write_count = 0;

        for batch in blob_oids.chunks(MAX_BLOB_OIDS_PER_REQUEST) {
            let blob_stream = self
                .repository_service
                .list_blobs(project_id, batch)
                .await
                .map_err(|e| format!("failed to list blobs: {e}"))?;

            let mut blobs = BlobStream::new(blob_stream);
            while let Some(blob) = blobs
                .next_blob()
                .await
                .map_err(|e| format!("failed to decode blob: {e}"))?
            {
                let paths = changeset
                    .paths_by_blob_id
                    .get(&blob.oid)
                    .map(|v| v.as_slice())
                    .unwrap_or_default();
                for path in paths {
                    self.cache
                        .write_file(project_id, branch, path, &blob.data)
                        .await
                        .map_err(|e| format!("failed to write cached file: {e}"))?;
                    write_count += 1;
                }
            }
        }

        if write_count < expected_writes {
            return Err(format!(
                "blob download incomplete: expected {expected_writes} writes but got {write_count}"
            ));
        }

        self.cache
            .update_commit(project_id, branch, to_sha)
            .await
            .map_err(|e| format!("failed to update cache commit: {e}"))?;

        info!(
            project_id,
            branch,
            from_sha,
            to_sha,
            renames = changeset.renames.len(),
            deletions = changeset.deletions.len(),
            writes = write_count,
            "incremental update complete"
        );

        Ok(())
    }
}

#[derive(Debug)]
struct IncrementalChangeset {
    deletions: Vec<String>,
    renames: Vec<(String, String)>,
    paths_by_blob_id: HashMap<String, Vec<String>>,
}

#[derive(Default)]
struct ChangesetBuilder {
    deletions: Vec<String>,
    renames: Vec<(String, String)>,
    paths_by_blob_id: HashMap<String, Vec<String>>,
    deleted_by_blob_id: HashMap<String, Vec<String>>,
}

impl ChangesetBuilder {
    fn record(&mut self, change: ChangedPath) {
        match change.status {
            ChangeStatus::Deleted => {
                self.deleted_by_blob_id
                    .entry(change.old_blob_id)
                    .or_default()
                    .push(change.path);
            }
            ChangeStatus::Renamed if change.old_blob_id == change.new_blob_id => {
                self.renames.push((change.old_path, change.path));
            }
            ChangeStatus::Renamed => {
                self.deletions.push(change.old_path);
                self.paths_by_blob_id
                    .entry(change.new_blob_id)
                    .or_default()
                    .push(change.path);
            }
            ChangeStatus::Added | ChangeStatus::Modified | ChangeStatus::Copied => {
                self.paths_by_blob_id
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

    /// Gitaly sometimes reports renames as separate DELETED + ADDED entries.
    /// Match them up by blob ID; unpaired leftovers stay as plain deletions/additions.
    fn reconcile_delete_add_renames(&mut self) {
        for (blob_id, deleted_paths) in std::mem::take(&mut self.deleted_by_blob_id) {
            let Some(added_paths) = self.paths_by_blob_id.remove(&blob_id) else {
                self.deletions.extend(deleted_paths);
                continue;
            };

            let paired = deleted_paths.len().min(added_paths.len());

            self.renames.extend(
                deleted_paths
                    .iter()
                    .zip(added_paths.iter())
                    .map(|(d, a)| (d.clone(), a.clone())),
            );

            self.deletions
                .extend(deleted_paths.into_iter().skip(paired));

            let remaining: Vec<String> = added_paths.into_iter().skip(paired).collect();
            if !remaining.is_empty() {
                self.paths_by_blob_id.insert(blob_id, remaining);
            }
        }
    }

    fn build(mut self) -> IncrementalChangeset {
        self.reconcile_delete_add_renames();
        IncrementalChangeset {
            deletions: self.deletions,
            renames: self.renames,
            paths_by_blob_id: self.paths_by_blob_id,
        }
    }
}

async fn compute_changeset(
    stream: super::service::ByteStream,
) -> Result<IncrementalChangeset, String> {
    let mut changed_paths = ChangedPathStream::new(stream);
    let mut builder = ChangesetBuilder::default();
    let mut count = 0usize;

    while let Some(change) = changed_paths
        .next_path()
        .await
        .map_err(|e| format!("failed to decode changed path: {e}"))?
    {
        if change.old_mode == SUBMODULE_MODE || change.new_mode == SUBMODULE_MODE {
            continue;
        }

        count += 1;
        if count > MAX_CHANGED_PATHS {
            return Err(format!(
                "too many changed paths (exceeded {MAX_CHANGED_PATHS})"
            ));
        }

        builder.record(change);
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::pin::Pin;

    use super::*;
    use crate::modules::code::repository::cache::{LocalRepositoryCache, RepositoryCache};
    use crate::modules::code::repository::service::RepositoryServiceError;
    use async_trait::async_trait;
    use parking_lot::Mutex;

    struct FileSnapshot {
        path: String,
        content: Vec<u8>,
        blob_id: String,
    }

    struct ScriptedRepositoryService {
        archive: Mutex<Vec<u8>>,
        changed_paths_response: Mutex<Option<Result<String, RepositoryServiceError>>>,
        blobs: Mutex<Vec<FileSnapshot>>,
    }

    impl ScriptedRepositoryService {
        fn with_archive(files: &[(&str, &str)]) -> Arc<Self> {
            let snapshots: Vec<FileSnapshot> = files
                .iter()
                .map(|(path, content)| FileSnapshot {
                    path: path.to_string(),
                    content: content.as_bytes().to_vec(),
                    blob_id: format!("blob_{path}"),
                })
                .collect();
            Arc::new(Self {
                archive: Mutex::new(build_test_tar_gz(files)),
                changed_paths_response: Mutex::new(None),
                blobs: Mutex::new(snapshots),
            })
        }

        fn set_changed_paths_response(&self, response: Result<String, RepositoryServiceError>) {
            *self.changed_paths_response.lock() = Some(response);
        }

        fn set_archive(&self, files: &[(&str, &str)]) {
            *self.archive.lock() = build_test_tar_gz(files);
            *self.blobs.lock() = files
                .iter()
                .map(|(path, content)| FileSnapshot {
                    path: path.to_string(),
                    content: content.as_bytes().to_vec(),
                    blob_id: format!("blob_{path}"),
                })
                .collect();
        }
    }

    #[async_trait]
    impl RepositoryService for ScriptedRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<gitlab_client::ProjectInfo, RepositoryServiceError> {
            Ok(gitlab_client::ProjectInfo {
                project_id,
                default_branch: "main".to_string(),
            })
        }

        async fn download_archive(
            &self,
            _project_id: i64,
            _ref_name: &str,
        ) -> Result<super::super::service::ByteStream, RepositoryServiceError> {
            let data = self.archive.lock().clone();
            Ok(Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from(data))
            })))
        }

        async fn changed_paths(
            &self,
            _project_id: i64,
            _from_sha: &str,
            _to_sha: &str,
        ) -> Result<super::super::service::ByteStream, RepositoryServiceError> {
            let response = self
                .changed_paths_response
                .lock()
                .take()
                .unwrap_or(Ok(String::new()));
            match response {
                Ok(body) => {
                    let stream: Pin<
                        Box<
                            dyn futures::Stream<Item = Result<bytes::Bytes, RepositoryServiceError>>
                                + Send,
                        >,
                    > = Box::pin(futures::stream::once(async move {
                        Ok(bytes::Bytes::from(body))
                    }));
                    Ok(stream)
                }
                Err(e) => Err(e),
            }
        }

        async fn list_blobs(
            &self,
            _project_id: i64,
            oids: &[String],
        ) -> Result<super::super::service::ByteStream, RepositoryServiceError> {
            let data = encode_blobs(&self.blobs.lock(), oids);
            let stream: Pin<
                Box<
                    dyn futures::Stream<Item = Result<bytes::Bytes, RepositoryServiceError>> + Send,
                >,
            > = Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(data))
            }));
            Ok(stream)
        }
    }

    fn build_test_tar_gz(files: &[(&str, &str)]) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let content_bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(content_bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append(&header, content_bytes).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestListBlobsResponse {
        #[prost(message, repeated, tag = "1")]
        blobs: Vec<TestBlobChunk>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestBlobChunk {
        #[prost(string, tag = "1")]
        oid: String,
        #[prost(int64, tag = "2")]
        size: i64,
        #[prost(bytes = "vec", tag = "3")]
        data: Vec<u8>,
        #[prost(bytes = "vec", tag = "4")]
        path: Vec<u8>,
    }

    fn encode_blobs(snapshots: &[FileSnapshot], requested_oids: &[String]) -> Vec<u8> {
        use prost::Message;
        let chunks: Vec<TestBlobChunk> = snapshots
            .iter()
            .filter(|s| requested_oids.contains(&s.blob_id))
            .map(|s| TestBlobChunk {
                oid: s.blob_id.clone(),
                size: s.content.len() as i64,
                data: s.content.clone(),
                path: s.path.as_bytes().to_vec(),
            })
            .collect();
        let resp = TestListBlobsResponse { blobs: chunks };
        let frame = resp.encode_to_vec();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        buf.extend_from_slice(&frame);
        buf
    }

    fn create_resolver(
        service: Arc<ScriptedRepositoryService>,
    ) -> (tempfile::TempDir, RepositoryResolver) {
        create_resolver_with_config(
            service,
            crate::configuration::RepositoryCacheConfiguration::default(),
        )
    }

    fn create_resolver_with_config(
        service: Arc<ScriptedRepositoryService>,
        config: crate::configuration::RepositoryCacheConfiguration,
    ) -> (tempfile::TempDir, RepositoryResolver) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let metrics = CodeMetrics::default();
        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            &config,
            4,
            metrics.clone(),
        ));
        let resolver =
            RepositoryResolver::new(service as Arc<dyn RepositoryService>, cache, metrics);
        (temp_dir, resolver)
    }

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
    async fn resolve_cache_miss_does_full_download() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let lease = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert!(lease.join("src/main.rs").exists());
        let content = std::fs::read_to_string(lease.join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn resolve_cache_hit_returns_cached_path() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let first = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let first_path = first.path().to_path_buf();
        drop(first);
        let second = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let second_path = second.path().to_path_buf();

        assert_eq!(first_path, second_path);
    }

    #[tokio::test]
    async fn resolve_stale_cache_triggers_incremental_update() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("commit1")).await.unwrap();

        service.set_archive(&[
            ("src/main.rs", "fn main() {}"),
            ("src/lib.rs", "pub mod lib;"),
        ]);
        service.set_changed_paths_response(Ok(
            r#"{"path":"src/lib.rs","status":"ADDED","old_path":"","new_mode":33188,"old_mode":0,"old_blob_id":"","new_blob_id":"blob_src/lib.rs"}"#.to_string()
        ));

        let lease = resolver.resolve(1, "main", Some("commit2")).await.unwrap();

        assert!(lease.join("src/main.rs").exists());
        assert!(lease.join("src/lib.rs").exists());
    }

    #[tokio::test]
    async fn resolve_force_push_falls_back_to_full_download() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("commit1")).await.unwrap();

        service.set_changed_paths_response(Err(RepositoryServiceError::ForcePush(1)));
        service.set_archive(&[("src/new.rs", "fn new() {}")]);

        let lease = resolver.resolve(1, "main", Some("commit2")).await.unwrap();

        assert!(lease.join("src/new.rs").exists());
        assert!(!lease.join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn resolve_uses_branch_when_no_commit_sha() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let lease = resolver.resolve(1, "main", None).await.unwrap();

        assert!(lease.join("src/main.rs").exists());
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
    async fn renamed_same_content_creates_rename_entry() {
        let stream = byte_stream_from_ndjson(vec![ndjson_line(
            "new_name.rs",
            "RENAMED",
            "old_name.rs",
            "blob1",
            "blob1",
        )]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
        assert_eq!(
            changeset.renames,
            vec![("old_name.rs".to_string(), "new_name.rs".to_string())]
        );
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
    async fn deleted_plus_added_same_blob_detected_as_rename() {
        let stream = byte_stream_from_ndjson(vec![
            ndjson_line("old_name.rs", "DELETED", "", "blob1", ""),
            ndjson_line("new_name.rs", "ADDED", "", "", "blob1"),
        ]);

        let changeset = compute_changeset(stream).await.unwrap();

        assert!(changeset.deletions.is_empty());
        assert!(changeset.paths_by_blob_id.is_empty());
        assert_eq!(
            changeset.renames,
            vec![("old_name.rs".to_string(), "new_name.rs".to_string())]
        );
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
        assert!(err.contains("too many changed paths"));
    }

    #[tokio::test]
    async fn resolve_returns_processing_error_when_budget_exhausted() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let config = crate::configuration::RepositoryCacheConfiguration {
            disk_budget_bytes: 1,
            headroom_per_worker_bytes: 0,
            large_repo_threshold_bytes: 0,
            ..Default::default()
        };
        let (_dir, resolver) = create_resolver_with_config(service, config);

        let error = resolver
            .resolve(1, "main", Some("abc123"))
            .await
            .unwrap_err();

        assert!(
            matches!(error, HandlerError::Processing(_)),
            "expected HandlerError::Processing, got: {error:?}"
        );
    }
}
