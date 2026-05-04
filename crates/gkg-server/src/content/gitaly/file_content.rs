use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gitlab_client::GitlabClient;
use gkg_utils::arrow::ColumnValue;
use indexer::modules::code::repository::blob_stream::BlobStream;
use query_engine::pipeline::PipelineError;
use tracing::{debug, warn};

use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

use crate::content::metrics;

/// Gitaly-specific parameters extracted from a hydrated entity row.
///
/// `revision` is the git ref used in `<revision>:<path>` for `list_blobs`.
/// Prefers `commit_sha` (immutable) over `branch` (can advance).
#[derive(Debug, Clone)]
pub struct GitalyBlobRequest {
    pub project_id: i64,
    pub revision: String,
    pub file_path: String,
    pub start_byte: Option<i64>,
    pub end_byte: Option<i64>,
}

/// File identity key for deduplicating Gitaly fetches.
type FileKey = (i64, String, String); // (project_id, revision, file_path)

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
        let mut timer = metrics::start_resolve(rows.len());

        // Pre-compute keys alongside requests so they can be reused for
        // deduplication and final lookup without cloning strings twice.
        let requests: Vec<Option<(GitalyBlobRequest, FileKey)>> = rows
            .iter()
            .map(|props| {
                let req = Self::build_request(props)?;
                let key = (req.project_id, req.revision.clone(), req.file_path.clone());
                Some((req, key))
            })
            .collect();

        // Deduplicate: each unique (project_id, revision, file_path) is
        // fetched once via list_blobs.
        let mut file_cache: HashMap<FileKey, Option<String>> = HashMap::new();

        // Group unique file keys by project_id for batched Gitaly calls.
        let mut by_project: HashMap<i64, Vec<FileKey>> = HashMap::new();
        for (req, key) in requests.iter().flatten() {
            if !file_cache.contains_key(key) {
                file_cache.insert(key.clone(), None);
                by_project
                    .entry(req.project_id)
                    .or_default()
                    .push(key.clone());
            }
        }

        // Fetch and drain all project blob streams concurrently.
        // Each future returns (blobs, had_error) so we can track partial failures.
        let futures = by_project.iter().map(|(&project_id, keys)| {
            let client = Arc::clone(&self.client);
            let revisions: Vec<String> = keys
                .iter()
                .map(|(_, revision, path)| format!("{revision}:{path}"))
                .collect();
            let keys = keys.clone();
            async move {
                metrics::record_gitaly_call();
                let stream = match client.list_blobs(project_id, &revisions).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            project_id,
                            error = %e,
                            "list_blobs failed, content will be missing for this project"
                        );
                        return (vec![], true);
                    }
                };

                let (blobs, err) = BlobStream::new(stream).drain().await;

                let had_error = err.is_some();
                if let Some(e) = err {
                    warn!(project_id, error = %e, "blob stream decode error");
                }

                let results = blobs
                    .into_iter()
                    .zip(keys.iter())
                    .filter_map(|(blob, key)| match String::from_utf8(blob.data) {
                        Ok(text) => {
                            metrics::record_blob_bytes(text.len() as u64);
                            Some((key.clone(), text))
                        }
                        Err(_) => {
                            debug!(project_id, path = %key.2, "skipping binary blob");
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                (results, had_error)
            }
        });

        let mut had_errors = false;
        for (blobs, errored) in futures::future::join_all(futures).await {
            had_errors |= errored;
            for (key, text) in blobs {
                file_cache.insert(key, Some(text));
            }
        }

        timer.set_outcome(if had_errors { "error" } else { "gitaly_direct" });

        // For each row, look up cached content and extract the byte-range slice.
        // Non-UTF-8 blobs were already filtered during fetch, so their cache
        // entries remain None and resolve to None here.
        // `slice_content` returns a &str into the cached String, so only the
        // extracted range is copied into the ColumnValue.
        Ok(requests
            .iter()
            .map(|entry| {
                let (req, key) = entry.as_ref()?;
                let content = file_cache.get(key)?.as_deref()?;
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
    /// Builds the `revision` from `commit_sha` (preferred) or `branch`
    /// (fallback). Expects `project_id` and either `path` (File) or
    /// `file_path` (Definition). Returns `None` if any required field
    /// is missing or byte ranges are invalid.
    pub fn build_request(props: &HashMap<String, ColumnValue>) -> Option<GitalyBlobRequest> {
        let project_id: i64 = props.get("project_id").and_then(|v| v.coerce())?;

        // Prefer commit_sha (immutable) over branch (can advance).
        let revision: String = props
            .get("commit_sha")
            .and_then(|v| v.coerce::<String>())
            .filter(|s| !s.is_empty())
            .or_else(|| props.get("branch").and_then(|v| v.coerce()))?;

        let file_path: String = props
            .get("file_path")
            .or_else(|| props.get("path"))
            .and_then(|v| v.coerce())?;

        let start_byte: Option<i64> = props.get("start_byte").and_then(|v| v.coerce());
        let end_byte: Option<i64> = props.get("end_byte").and_then(|v| v.coerce());

        match (start_byte, end_byte) {
            (Some(s), Some(e)) if s < 0 || e < 0 || s > e => return None,
            _ => {}
        }

        Some(GitalyBlobRequest {
            project_id,
            revision,
            file_path,
            start_byte,
            end_byte,
        })
    }
}

/// Return the byte-range slice of `content`, or the full string when no
/// range is specified. Returns an empty string if the range is out of
/// bounds or lands on a UTF-8 boundary.
fn slice_content(content: &str, start_byte: Option<i64>, end_byte: Option<i64>) -> &str {
    match (start_byte, end_byte) {
        (Some(s), Some(e)) if s >= 0 && e >= s => {
            let s = s as usize;
            let e = (e as usize).min(content.len());
            if s >= content.len() {
                return "";
            }
            // str::get checks UTF-8 char boundaries and returns None
            // if either index falls inside a multi-byte character.
            content.get(s..e).unwrap_or("")
        }
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
        assert_eq!(req.revision, "main");
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
    fn slice_clamps_end_to_content_len() {
        assert_eq!(slice_content("hi", Some(0), Some(999)), "hi");
    }

    #[test]
    fn slice_empty_when_start_past_end_of_content() {
        assert_eq!(slice_content("hi", Some(100), Some(200)), "");
    }

    #[test]
    fn slice_empty_on_utf8_boundary() {
        // 'é' is 2 bytes (0xC3 0xA9). Slicing at byte 1 lands mid-character.
        assert_eq!(slice_content("é", Some(0), Some(1)), "");
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
