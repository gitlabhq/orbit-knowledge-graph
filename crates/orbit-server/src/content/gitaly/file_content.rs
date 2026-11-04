use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use gitlab_client::GitlabClient;
use indexer::modules::code::repository::blob_stream::BlobStream;
use orbit_arrow::ColumnValue;
use orbit_log_fields::{debug, warn};
use query_engine::pipeline::PipelineError;

use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

use crate::content::metrics;

/// `revision` is the git ref used in `<revision>:<path>` for `list_blobs`;
/// prefers `commit_sha` (immutable) over `branch` (can advance).
#[derive(Debug, Clone)]
pub struct GitalyBlobRequest {
    pub project_id: i64,
    pub revision: String,
    pub file_path: String,
    pub start_byte: Option<i64>,
    pub end_byte: Option<i64>,
}

type FileKey = (i64, String, String); // (project_id, revision, file_path)

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

        let requests: Vec<Option<(GitalyBlobRequest, FileKey)>> = rows
            .iter()
            .map(|props| {
                let req = Self::build_request(props)?;
                let key = (req.project_id, req.revision.clone(), req.file_path.clone());
                Some((req, key))
            })
            .collect();

        let mut file_cache: HashMap<FileKey, Option<String>> = HashMap::new();

        let mut batches: HashMap<(i64, String), Vec<FileKey>> = HashMap::new();
        for (_, key) in requests.iter().flatten() {
            if !file_cache.contains_key(key) {
                file_cache.insert(key.clone(), None);
                batches
                    .entry((key.0, key.1.clone()))
                    .or_default()
                    .push(key.clone());
            }
        }

        let futures = batches.into_values().map(|keys| {
            let client = Arc::clone(&self.client);
            async move { fetch_contents(&client, keys).await }
        });

        let mut had_errors = false;
        for fetched in futures::future::join_all(futures).await {
            had_errors |= fetched.had_error;
            for (key, data) in fetched.contents {
                match String::from_utf8(data) {
                    Ok(text) => {
                        metrics::record_blob_bytes(text.len() as u64);
                        file_cache.insert(key, Some(text));
                    }
                    Err(_) => debug!(
                        safe(GL_PROJECT_ID) = key.0,
                        expanded(FILE_PATH) = key.2.as_str(),
                        "skipping binary blob"
                    ),
                }
            }
        }

        timer.set_outcome(if had_errors { "error" } else { "gitaly_direct" });

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

const MAX_CONCURRENT_SINGLE_FETCHES: usize = 8;

struct FetchedContents {
    contents: HashMap<FileKey, Vec<u8>>,
    had_error: bool,
}

/// Gitaly lists a shared blob once, so each file the batch misses is fetched alone.
async fn fetch_contents(client: &GitlabClient, keys: Vec<FileKey>) -> FetchedContents {
    let Some(mut fetched) = list_contents(client, &keys).await else {
        return FetchedContents {
            contents: HashMap::new(),
            had_error: true,
        };
    };

    let missed: Vec<FileKey> = keys
        .into_iter()
        .filter(|key| !fetched.contents.contains_key(key))
        .collect();
    let singles: Vec<_> = futures::stream::iter(missed)
        .map(|key| async move { list_contents(client, &[key]).await })
        .buffer_unordered(MAX_CONCURRENT_SINGLE_FETCHES)
        .collect()
        .await;

    for single in singles {
        match single {
            Some(single) => {
                fetched.contents.extend(single.contents);
                fetched.had_error |= single.had_error;
            }
            None => fetched.had_error = true,
        }
    }
    fetched
}

/// Keys share one revision, so a path names one key. `None` when the request fails.
async fn list_contents(client: &GitlabClient, keys: &[FileKey]) -> Option<FetchedContents> {
    let project_id = keys[0].0;
    let revisions: Vec<String> = keys
        .iter()
        .map(|(_, revision, path)| format!("{revision}:{path}"))
        .collect();

    metrics::record_gitaly_call();
    let stream = match client.list_blobs(project_id, &revisions).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                safe(GL_PROJECT_ID) = project_id,
                expanded(ERROR_MESSAGE) = %e,
                safe(ERROR_TYPE) = <&str>::from(&e),
                "list_blobs failed, content will be missing for this project"
            );
            return None;
        }
    };

    let (blobs, err) = BlobStream::new(stream).drain().await;
    if let Some(e) = &err {
        warn!(
            safe(GL_PROJECT_ID) = project_id,
            safe(ERROR_TYPE) = "blob_decode",
            expanded(ERROR_MESSAGE) = %e,
            "blob stream decode error"
        );
    }

    let contents = blobs
        .into_iter()
        .filter_map(|blob| {
            let key = match keys {
                [only] => only,
                _ => keys.iter().find(|key| key.2.as_bytes() == blob.path)?,
            };
            Some((key.clone(), blob.data))
        })
        .collect();
    Some(FetchedContents {
        contents,
        had_error: err.is_some(),
    })
}

impl GitalyContentService {
    /// Expects `project_id` and either `path` (File) or `file_path`
    /// (Definition). Returns `None` if any required field is missing or byte
    /// ranges are invalid.
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

    // resolve_batch is covered in the integration-tests crate (needs a live GitlabClient).

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
