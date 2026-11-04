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
        for (blobs, errored) in futures::future::join_all(futures).await {
            had_errors |= errored;
            for (key, data) in blobs {
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

        // Non-UTF-8 blobs and keys without a blob keep a None cache entry.
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

/// Most single-file fetches one project may run at once.
const MAX_CONCURRENT_SINGLE_FETCHES: usize = 8;

/// Fetches the blobs of one project and pairs each blob with its key.
///
/// `ListBlobs` reads `git rev-list --objects`, which lists each object once.
/// Two paths with identical content share one blob, so the response can hold
/// fewer blobs than keys. Pairing by position then shifts every later key onto
/// another file's content. Blobs are paired by path instead, and each key left
/// without a blob is fetched alone, where no other path can share its blob.
async fn fetch_project_blobs(
    client: &GitlabClient,
    project_id: i64,
    keys: Vec<FileKey>,
) -> (Vec<(FileKey, Vec<u8>)>, bool) {
    let Some((blobs, mut had_error)) = list_blobs(client, project_id, &keys).await else {
        return (vec![], true);
    };
    let (mut matched, unmatched) = match_blobs(keys, blobs);

    if !unmatched.is_empty() {
        debug!(
            safe(GL_PROJECT_ID) = project_id,
            safe(COUNT) = unmatched.len(),
            "fetching blobs one by one for keys the batch did not match"
        );
        let singles: Vec<_> = futures::stream::iter(unmatched)
            .map(|key| async move {
                let keys = vec![key];
                let fetched = list_blobs(client, project_id, &keys).await;
                (keys, fetched)
            })
            .buffer_unordered(MAX_CONCURRENT_SINGLE_FETCHES)
            .collect()
            .await;

        for (keys, fetched) in singles {
            let Some((blobs, errored)) = fetched else {
                had_error = true;
                continue;
            };
            had_error |= errored;
            matched.extend(match_blobs(keys, blobs).0);
        }
    }

    (matched, had_error)
}

/// Returns `None` when the request fails, else the decoded blobs and whether
/// the stream ended with a decode error.
async fn list_blobs(
    client: &GitlabClient,
    project_id: i64,
    keys: &[FileKey],
) -> Option<(Vec<ResolvedBlob>, bool)> {
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
    let had_error = err.is_some();
    if let Some(e) = err {
        warn!(
            safe(GL_PROJECT_ID) = project_id,
            safe(ERROR_TYPE) = "blob_decode",
            expanded(ERROR_MESSAGE) = %e,
            "blob stream decode error"
        );
    }
    Some((blobs, had_error))
}

/// Pairs blobs with the keys they were fetched for. Returns the pairs and the
/// keys left without a blob.
///
/// A blob with a path goes to the one key with that path. A path shared by
/// keys of different revisions is ambiguous, so those keys stay unmatched.
/// Without paths, pairing by position is safe only when nothing was dropped,
/// which holds when there are as many blobs as keys.
fn match_blobs(
    keys: Vec<FileKey>,
    blobs: Vec<ResolvedBlob>,
) -> (Vec<(FileKey, Vec<u8>)>, Vec<FileKey>) {
    let all_have_paths = !blobs.is_empty() && blobs.iter().all(|b| !b.path.is_empty());

    if !all_have_paths {
        if blobs.len() == keys.len() {
            let matched = keys
                .into_iter()
                .zip(blobs)
                .map(|(k, b)| (k, b.data))
                .collect();
            return (matched, vec![]);
        }
        return (vec![], keys);
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

    let mut matched = Vec::with_capacity(keys.len());
    let mut unmatched = Vec::new();
    for key in keys {
        match data_by_path.remove(key.2.as_bytes()) {
            Some(data) => matched.push((key, data)),
            None => unmatched.push(key),
        }
    }
    (matched, unmatched)
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

    fn data_of(matched: &[(FileKey, Vec<u8>)], path: &str) -> Option<String> {
        matched
            .iter()
            .find(|(k, _)| k.2 == path)
            .map(|(_, d)| String::from_utf8(d.clone()).unwrap())
    }

    #[test]
    fn match_blobs_pairs_by_path_when_a_duplicate_blob_is_dropped() {
        // Gitaly lists the blob shared by a/same.txt and b/same.txt once.
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

        let (matched, unmatched) = match_blobs(keys, blobs);

        assert_eq!(data_of(&matched, "a/same.txt").as_deref(), Some("same"));
        assert_eq!(data_of(&matched, "src/One.java").as_deref(), Some("one"));
        assert_eq!(data_of(&matched, "src/Two.java").as_deref(), Some("two"));
        assert_eq!(unmatched, vec![key("main", "b/same.txt")]);
    }

    #[test]
    fn match_blobs_pairs_by_path_in_any_order() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("b", "b.rs", "bee"), blob("a", "a.rs", "ay")];

        let (matched, unmatched) = match_blobs(keys, blobs);

        assert_eq!(data_of(&matched, "a.rs").as_deref(), Some("ay"));
        assert_eq!(data_of(&matched, "b.rs").as_deref(), Some("bee"));
        assert!(unmatched.is_empty());
    }

    #[test]
    fn match_blobs_leaves_a_path_shared_by_two_revisions_unmatched() {
        let keys = vec![key("sha1", "lib.rs"), key("sha2", "lib.rs")];
        let blobs = vec![blob("x", "lib.rs", "v1"), blob("y", "lib.rs", "v2")];

        let (matched, unmatched) = match_blobs(keys.clone(), blobs);

        assert!(matched.is_empty());
        assert_eq!(unmatched, keys);
    }

    #[test]
    fn match_blobs_pairs_by_position_without_paths_when_counts_match() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("a", "", "ay"), blob("b", "", "bee")];

        let (matched, unmatched) = match_blobs(keys, blobs);

        assert_eq!(data_of(&matched, "a.rs").as_deref(), Some("ay"));
        assert_eq!(data_of(&matched, "b.rs").as_deref(), Some("bee"));
        assert!(unmatched.is_empty());
    }

    #[test]
    fn match_blobs_refuses_position_without_paths_when_counts_differ() {
        let keys = vec![key("main", "a.rs"), key("main", "b.rs")];
        let blobs = vec![blob("a", "", "ay")];

        let (matched, unmatched) = match_blobs(keys.clone(), blobs);

        assert!(matched.is_empty());
        assert_eq!(unmatched, keys);
    }

    #[test]
    fn match_blobs_ignores_blobs_for_other_paths() {
        // A key that names a directory lists the blobs inside it.
        let keys = vec![key("main", "src")];
        let blobs = vec![blob("a", "src/a.rs", "ay"), blob("b", "src/b.rs", "bee")];

        let (matched, unmatched) = match_blobs(keys.clone(), blobs);

        assert!(matched.is_empty());
        assert_eq!(unmatched, keys);
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
