use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gitlab_client::GitlabClient;
use gkg_utils::arrow::ColumnValue;
use indexer::modules::code::repository::blob_stream::BlobStream;
use query_engine::pipeline::PipelineError;
use tracing::{debug, warn};

use super::{ColumnResolver, PropertyRow, ResolverContext};

/// Gitaly-specific parameters extracted from a hydrated entity row.
///
/// `branch` is used as the Gitaly revision ref. A commit SHA would be
/// more precise but isn't available in the current schema — the indexer
/// stores the branch name at index time.
#[derive(Debug, Clone)]
pub struct GitalyBlobRequest {
    pub project_id: i64,
    pub branch: String,
    pub file_path: String,
    pub start_byte: Option<i64>,
    pub end_byte: Option<i64>,
}

/// File identity key for deduplicating Gitaly fetches.
type FileKey = (i64, String, String); // (project_id, branch, file_path)

/// Resolves file content by calling the GitLab internal API's `list_blobs`
/// endpoint, which streams blobs from Gitaly via Workhorse.
///
/// Requests are grouped by `project_id` and deduplicated by file identity.
/// Multiple definitions in the same file share the fetched content and
/// only receive their byte-range slice.
pub struct GitalyContentService {
    client: Arc<GitlabClient>,
}

impl GitalyContentService {
    pub fn new(client: Arc<GitlabClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ColumnResolver for GitalyContentService {
    async fn resolve_batch(
        &self,
        _lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let requests: Vec<Option<GitalyBlobRequest>> = rows
            .iter()
            .map(|props| Self::build_request(props))
            .collect();

        // Deduplicate: each unique (project_id, branch, file_path) is
        // fetched once via list_blobs.
        let mut file_cache: HashMap<FileKey, Option<String>> = HashMap::new();

        // Group unique file keys by project_id for batched Gitaly calls.
        let mut by_project: HashMap<i64, Vec<FileKey>> = HashMap::new();
        for req in requests.iter().flatten() {
            let key = (req.project_id, req.branch.clone(), req.file_path.clone());
            if !file_cache.contains_key(&key) {
                file_cache.insert(key.clone(), None);
                by_project
                    .entry(req.project_id)
                    .or_default()
                    .push(key);
            }
        }

        // Fetch blobs concurrently per project.
        let futures = by_project.iter().map(|(&project_id, keys)| {
            let client = Arc::clone(&self.client);
            let revisions: Vec<String> = keys
                .iter()
                .map(|(_, branch, path)| format!("{branch}:{path}"))
                .collect();
            let keys = keys.clone();
            async move {
                let stream = client.list_blobs(project_id, &revisions).await;
                (project_id, keys, stream)
            }
        });

        let responses: Vec<_> = futures::future::join_all(futures).await;

        for (project_id, keys, stream_result) in responses {
            let stream = match stream_result {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        project_id,
                        error = %e,
                        "list_blobs failed, content will be missing for this project"
                    );
                    continue;
                }
            };

            let mut blob_stream = BlobStream::new(stream);
            let mut blob_index = 0;

            loop {
                match blob_stream.next_blob().await {
                    Ok(Some(blob)) => {
                        if blob_index < keys.len() {
                            match String::from_utf8(blob.data) {
                                Ok(text) => {
                                    file_cache.insert(keys[blob_index].clone(), Some(text));
                                }
                                Err(_) => {
                                    debug!(
                                        project_id,
                                        path = %keys[blob_index].2,
                                        "skipping binary blob"
                                    );
                                }
                            }
                        }
                        blob_index += 1;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(
                            project_id,
                            error = %e,
                            "blob stream decode error"
                        );
                        break;
                    }
                }
            }
        }

        // For each row, look up cached content and return only the
        // relevant byte-range slice.
        Ok(requests
            .iter()
            .map(|req| {
                let req = req.as_ref()?;
                let key = (req.project_id, req.branch.clone(), req.file_path.clone());
                let content = file_cache.get(&key)?.as_deref()?;
                Some(ColumnValue::String(
                    slice_content(content, req.start_byte, req.end_byte).to_string(),
                ))
            })
            .collect())
    }
}

impl GitalyContentService {
    /// Extract a [`GitalyBlobRequest`] from a hydrated property map.
    ///
    ///
    /// Expects `project_id`, `branch`, and either `path` (File) or
    /// `file_path` (Definition). Returns `None` if any required field
    /// is missing or byte ranges are invalid.
    pub fn build_request(props: &HashMap<String, ColumnValue>) -> Option<GitalyBlobRequest> {
        let project_id = props
            .get("project_id")
            .and_then(|v| v.as_int64().copied())?;
        let branch = props.get("branch").and_then(|v| v.as_string().cloned())?;

        let file_path = props
            .get("file_path")
            .or_else(|| props.get("path"))
            .and_then(|v| v.as_string().cloned())?;

        let start_byte = props.get("start_byte").and_then(|v| v.as_int64().copied());
        let end_byte = props.get("end_byte").and_then(|v| v.as_int64().copied());

        match (start_byte, end_byte) {
            (Some(s), Some(e)) if s < 0 || e < 0 || s > e => return None,
            _ => {}
        }

        Some(GitalyBlobRequest {
            project_id,
            branch,
            file_path,
            start_byte,
            end_byte,
        })
    }
}

/// Return the byte-range slice of `content`, or the full string when no
/// range is specified. Falls back to the full content if the range is
/// out of bounds.
fn slice_content(content: &str, start_byte: Option<i64>, end_byte: Option<i64>) -> &str {
    match (start_byte, end_byte) {
        (Some(s), Some(e)) => content.get(s as usize..e as usize).unwrap_or(content),
        _ => content,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_request_from_file_props() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(42));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.project_id, 42);
        assert_eq!(req.branch, "main");
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, None);
        assert_eq!(req.end_byte, None);
    }

    #[test]
    fn build_request_from_definition_props() {
        let props = definition_props(100, 200);

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, Some(100));
        assert_eq!(req.end_byte, Some(200));
    }

    #[test]
    fn build_request_none_without_project_id() {
        let mut props = HashMap::new();
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));

        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_prefers_file_path_over_path() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(1));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("old.rs".into()));
        props.insert("file_path".into(), ColumnValue::String("new.rs".into()));

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.file_path, "new.rs");
    }

    #[test]
    fn build_request_rejects_negative_start_byte() {
        let props = definition_props(-1, 200);
        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_rejects_start_after_end() {
        let props = definition_props(200, 100);
        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_accepts_equal_start_end() {
        let props = definition_props(100, 100);
        assert!(GitalyContentService::build_request(&props).is_some());
    }

    // ── slice_content ───────────────────────────────────────────────────

    #[test]
    fn slice_full_when_no_range() {
        assert_eq!(slice_content("hello world", None, None), "hello world");
    }

    #[test]
    fn slice_byte_range() {
        assert_eq!(slice_content("hello world", Some(6), Some(11)), "world");
    }

    #[test]
    fn slice_falls_back_on_out_of_bounds() {
        assert_eq!(slice_content("hi", Some(0), Some(999)), "hi");
    }

    // ── resolve_batch ───────────────────────────────────────────────────
    // Integration tests for resolve_batch require a running GitlabClient
    // and are covered in the integration-tests crate.

    // ── helpers ─────────────────────────────────────────────────────────

    fn definition_props(start: i64, end: i64) -> HashMap<String, ColumnValue> {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(42));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("file_path".into(), ColumnValue::String("src/lib.rs".into()));
        props.insert("start_byte".into(), ColumnValue::Int64(start));
        props.insert("end_byte".into(), ColumnValue::Int64(end));
        props
    }
}
