use std::collections::HashMap;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

use super::VirtualService;

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

/// Stub Gitaly virtual service. Parses rows into typed
/// [`GitalyBlobRequest`]s, deduplicates by file identity, and slices
/// content by byte range per row. The actual Gitaly fetch is not yet
/// implemented — returns `None` for every row.
pub struct GitalyContentService;

#[async_trait]
impl VirtualService for GitalyContentService {
    async fn resolve_batch(
        &self,
        _lookup: &str,
        rows: &[&HashMap<String, ColumnValue>],
        _org_id: i64,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let requests: Vec<Option<GitalyBlobRequest>> = rows
            .iter()
            .map(|props| Self::build_request(props))
            .collect();

        // Deduplicate: each unique (project_id, branch, file_path) is
        // fetched once. Multiple definitions in the same file share the
        // cached content and only receive their byte-range slice.
        let mut file_cache: HashMap<FileKey, Option<String>> = HashMap::new();
        for req in requests.iter().flatten() {
            let key = (req.project_id, req.branch.clone(), req.file_path.clone());
            file_cache.entry(key).or_insert_with(|| {
                // TODO(#379): fetch blob from Gitaly
                None
            });
        }

        // For each row, look up cached content and return only the
        // relevant byte-range slice — never the full file.
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
        props.insert(
            "traversal_path".into(),
            ColumnValue::String("9970/123/".into()),
        );

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.project_id, 42);
        assert_eq!(req.branch, "main");
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, None);
        assert_eq!(req.end_byte, None);
    }

    #[test]
    fn build_request_from_definition_props() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(42));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("file_path".into(), ColumnValue::String("src/lib.rs".into()));
        props.insert(
            "traversal_path".into(),
            ColumnValue::String("9970/123/".into()),
        );
        props.insert("start_byte".into(), ColumnValue::Int64(100));
        props.insert("end_byte".into(), ColumnValue::Int64(200));

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

    #[tokio::test]
    async fn stub_returns_none() {
        let svc = GitalyContentService;
        let props = HashMap::new();
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&props, &props];

        let results = svc.resolve_batch("blob_content", &rows, 1).await.unwrap();
        assert_eq!(results, vec![None, None]);
    }

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
