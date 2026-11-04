use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use gitlab_client::GitlabClient;
use indexer::modules::code::repository::blob_stream::{BlobStream, ResolvedBlob};
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

        let futures = by_project.into_iter().map(|(project_id, keys)| {
            let client = Arc::clone(&self.client);
            async move { fetch_project_blobs(&client, project_id, keys).await }
        });

        let mut had_errors = false;
        for fetch in futures::future::join_all(futures).await {
            had_errors |= fetch.had_error;
            for FileBlob { key, data } in fetch.files {
                match String::from_utf8(data) {
                    Ok(text) => {
                        metrics::record_blob_bytes(text.len() as u64);
                        file_cache.insert(key, Some(text));
                    }
                    Err(_) => debug!(expanded(FILE_PATH) = key.2.as_str(), "skipping binary blob"),
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

#[derive(Debug)]
struct FileBlob {
    key: FileKey,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct BlobFetch {
    files: Vec<FileBlob>,
    unmatched: Vec<FileKey>,
    had_error: bool,
}

/// `ListBlobs` lists a blob shared by several paths once, so the response can
/// hold fewer blobs than keys. A key left without a blob is fetched alone.
async fn fetch_project_blobs(
    client: &GitlabClient,
    project_id: i64,
    keys: Vec<FileKey>,
) -> BlobFetch {
    let mut fetch = fetch_blobs(client, project_id, keys).await;
    if fetch.unmatched.is_empty() {
        return fetch;
    }

    debug!(
        safe(GL_PROJECT_ID) = project_id,
        safe(COUNT) = fetch.unmatched.len(),
        "fetching blobs one by one for keys the batch did not match"
    );
    let singles: Vec<BlobFetch> = futures::stream::iter(std::mem::take(&mut fetch.unmatched))
        .map(|key| fetch_blobs(client, project_id, vec![key]))
        .buffer_unordered(MAX_CONCURRENT_SINGLE_FETCHES)
        .collect()
        .await;

    for single in singles {
        fetch.files.extend(single.files);
        fetch.unmatched.extend(single.unmatched);
        fetch.had_error |= single.had_error;
    }
    fetch
}

async fn fetch_blobs(client: &GitlabClient, project_id: i64, keys: Vec<FileKey>) -> BlobFetch {
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
            return BlobFetch {
                had_error: true,
                ..BlobFetch::default()
            };
        }
    };

    let (blobs, err) = BlobStream::new(stream).drain().await;
    let had_error = err.is_some();
    if let Some(e) = err {
        warn!(
            safe(GL_PROJECT_ID) = project_id,
            safe(ERROR_TYPE) = "blob_decode",
            expanded(ERROR_MESSAGE) = %e,
            "blob stream decode error"
        );
    }
    BlobFetch {
        had_error,
        ..match_blobs(keys, blobs)
    }
}

/// A path requested under two revisions is ambiguous, so its keys stay
/// unmatched. Pairing by position is safe only when no blob was dropped.
fn match_blobs(keys: Vec<FileKey>, blobs: Vec<ResolvedBlob>) -> BlobFetch {
    let all_have_paths = !blobs.is_empty() && blobs.iter().all(|b| !b.path.is_empty());

    if !all_have_paths {
        if blobs.len() == keys.len() {
            let files = keys
                .into_iter()
                .zip(blobs)
                .map(|(key, blob)| FileBlob {
                    key,
                    data: blob.data,
                })
                .collect();
            return BlobFetch {
                files,
                ..BlobFetch::default()
            };
        }
        return BlobFetch {
            unmatched: keys,
            ..BlobFetch::default()
        };
    }

    let mut key_count_by_path: HashMap<&[u8], usize> = HashMap::new();
    for (_, _, path) in &keys {
        *key_count_by_path.entry(path.as_bytes()).or_default() += 1;
    }
    let mut data_by_path: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for blob in blobs {
        if key_count_by_path.get(blob.path.as_slice()) == Some(&1) {
            data_by_path.entry(blob.path).or_insert(blob.data);
        }
    }

    let mut fetch = BlobFetch::default();
    for key in keys {
        match data_by_path.remove(key.2.as_bytes()) {
            Some(data) => fetch.files.push(FileBlob { key, data }),
            None => fetch.unmatched.push(key),
        }
    }
    fetch
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

    fn key(revision: &str, path: &str) -> FileKey {
        (1, revision.into(), path.into())
    }

    fn blob(oid: &str, path: &str, data: &str) -> ResolvedBlob {
        ResolvedBlob {
            oid: oid.into(),
            path: path.as_bytes().to_vec(),
            data: data.as_bytes().to_vec(),
        }
    }

    fn data_of(fetch: &BlobFetch, path: &str) -> Option<String> {
        fetch
            .files
            .iter()
            .find(|file| file.key.2 == path)
            .map(|file| String::from_utf8(file.data.clone()).unwrap())
    }

    #[test]
    fn match_blobs_pairs_by_path_when_a_duplicate_blob_is_dropped() {
        let keys = vec![
            key("main", "a/same.txt"),
            key("main", "b/same.txt"),
            key("main", "src/One.java"),
            key("main", "src/Two.java"),
        ];
        let blobs = vec![
            blob("s", "a/same.txt", "same"),
            blob("1", "src/One.java", "one"),
            blob("2", "src/Two.java", "two"),
        ];

        let fetch = match_blobs(keys, blobs);

        assert_eq!(data_of(&fetch, "a/same.txt").as_deref(), Some("same"));
        assert_eq!(data_of(&fetch, "src/One.java").as_deref(), Some("one"));
        assert_eq!(data_of(&fetch, "src/Two.java").as_deref(), Some("two"));
        assert_eq!(fetch.unmatched, vec![key("main", "b/same.txt")]);
    }

    #[test]
    fn match_blobs_pairs_by_path_in_any_order() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("b", "b.rs", "bee"), blob("a", "a.rs", "ay")];

        let fetch = match_blobs(keys, blobs);

        assert_eq!(data_of(&fetch, "a.rs").as_deref(), Some("ay"));
        assert_eq!(data_of(&fetch, "b.rs").as_deref(), Some("bee"));
        assert!(fetch.unmatched.is_empty());
    }

    #[test]
    fn match_blobs_leaves_a_path_shared_by_two_revisions_unmatched() {
        let keys = vec![key("sha1", "lib.rs"), key("sha2", "lib.rs")];
        let blobs = vec![blob("x", "lib.rs", "v1"), blob("y", "lib.rs", "v2")];

        let fetch = match_blobs(keys.clone(), blobs);

        assert!(fetch.files.is_empty());
        assert_eq!(fetch.unmatched, keys);
    }

    #[test]
    fn match_blobs_pairs_by_position_without_paths_when_counts_match() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("a", "", "ay"), blob("b", "", "bee")];

        let fetch = match_blobs(keys, blobs);

        assert_eq!(data_of(&fetch, "a.rs").as_deref(), Some("ay"));
        assert_eq!(data_of(&fetch, "b.rs").as_deref(), Some("bee"));
        assert!(fetch.unmatched.is_empty());
    }

    #[test]
    fn match_blobs_refuses_position_without_paths_when_counts_differ() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("a", "", "ay")];

        let fetch = match_blobs(keys.clone(), blobs);

        assert!(fetch.files.is_empty());
        assert_eq!(fetch.unmatched, keys);
    }

    #[test]
    fn match_blobs_ignores_blobs_for_other_paths() {
        // A key that names a directory lists the blobs inside it.
        let keys = vec![key("main", "src")];
        let blobs = vec![blob("a", "src/a.rs", "ay"), blob("b", "src/b.rs", "bee")];

        let fetch = match_blobs(keys.clone(), blobs);

        assert!(fetch.files.is_empty());
        assert_eq!(fetch.unmatched, keys);
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
