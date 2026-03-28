use std::collections::HashMap;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

use super::{VirtualService, extract_root_namespace_id};

/// Gitaly-specific parameters extracted from a hydrated entity row.
#[derive(Debug, Clone)]
pub struct GitalyBlobRequest {
    pub project_id: i64,
    pub branch: String,
    pub file_path: String,
    pub start_byte: Option<i64>,
    pub end_byte: Option<i64>,
    pub organization_id: i64,
    pub root_namespace_id: i64,
}

/// Stub Gitaly virtual service. Parses rows into [`GitalyBlobRequest`]s but
/// does not call Gitaly yet — returns `None` for every row.
pub struct GitalyContentService;

#[async_trait]
impl VirtualService for GitalyContentService {
    async fn resolve_batch(
        &self,
        _lookup: &str,
        rows: &[&HashMap<String, ColumnValue>],
        _org_id: i64,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        Ok(vec![None; rows.len()])
    }
}

impl GitalyContentService {
    /// Extract a [`GitalyBlobRequest`] from a hydrated property map.
    ///
    /// Expects `project_id`, `branch`, `traversal_path`, and either `path`
    /// (File) or `file_path` (Definition). Returns `None` if any required
    /// field is missing.
    pub fn build_request(
        props: &HashMap<String, ColumnValue>,
        org_id: i64,
    ) -> Option<GitalyBlobRequest> {
        let project_id = props
            .get("project_id")
            .and_then(|v| v.as_int64().copied())?;
        let branch = props.get("branch").and_then(|v| v.as_string().cloned())?;

        let file_path = props
            .get("file_path")
            .or_else(|| props.get("path"))
            .and_then(|v| v.as_string().cloned())?;

        let traversal_path = props.get("traversal_path").and_then(|v| v.as_string())?;
        let root_namespace_id = extract_root_namespace_id(traversal_path)?;

        let start_byte = props.get("start_byte").and_then(|v| v.as_int64().copied());
        let end_byte = props.get("end_byte").and_then(|v| v.as_int64().copied());

        Some(GitalyBlobRequest {
            project_id,
            branch,
            file_path,
            start_byte,
            end_byte,
            organization_id: org_id,
            root_namespace_id,
        })
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

        let req = GitalyContentService::build_request(&props, 9970).unwrap();
        assert_eq!(req.project_id, 42);
        assert_eq!(req.branch, "main");
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, None);
        assert_eq!(req.end_byte, None);
        assert_eq!(req.organization_id, 9970);
        assert_eq!(req.root_namespace_id, 123);
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

        let req = GitalyContentService::build_request(&props, 9970).unwrap();
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, Some(100));
        assert_eq!(req.end_byte, Some(200));
    }

    #[test]
    fn build_request_none_without_project_id() {
        let mut props = HashMap::new();
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
        props.insert(
            "traversal_path".into(),
            ColumnValue::String("9970/123/".into()),
        );

        assert!(GitalyContentService::build_request(&props, 9970).is_none());
    }

    #[test]
    fn build_request_prefers_file_path_over_path() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(1));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("old.rs".into()));
        props.insert("file_path".into(), ColumnValue::String("new.rs".into()));
        props.insert("traversal_path".into(), ColumnValue::String("1/2/".into()));

        let req = GitalyContentService::build_request(&props, 1).unwrap();
        assert_eq!(req.file_path, "new.rs");
    }

    #[tokio::test]
    async fn stub_returns_none() {
        let svc = GitalyContentService;
        let props = HashMap::new();
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&props, &props];

        let results = svc.resolve_batch("blob_content", &rows, 1).await.unwrap();
        assert_eq!(results, vec![None, None]);
    }
}
