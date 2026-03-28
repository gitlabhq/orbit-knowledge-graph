use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

/// Maximum rows per virtual service batch call.
pub const MAX_VIRTUAL_BATCH_SIZE: usize = 100;

// ─────────────────────────────────────────────────────────────────────────────
// Trait + registry
// ─────────────────────────────────────────────────────────────────────────────

/// A service that resolves virtual column values from an external source.
///
/// Implementations receive the hydrated property map for each entity row
/// and extract whatever parameters they need internally. This keeps the
/// trait generic — a Gitaly implementation reads `project_id`/`branch`/`path`,
/// while a hypothetical CI service would read `pipeline_id`/`job_id`.
#[async_trait]
pub trait VirtualService: Send + Sync {
    /// Resolve a batch of rows for the given `lookup` operation.
    ///
    /// `rows` contains one property map per entity. Returns a
    /// `Vec<Option<ColumnValue>>` aligned with `rows` — `None` means
    /// the value could not be resolved for that row.
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&HashMap<String, ColumnValue>],
        org_id: i64,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError>;
}

/// Maps service names (e.g. `"gitaly"`) to their implementations.
#[derive(Default)]
pub struct VirtualServiceRegistry {
    services: HashMap<String, Arc<dyn VirtualService>>,
}

impl VirtualServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: impl Into<String>, service: Arc<dyn VirtualService>) {
        self.services.insert(name.into(), service);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn VirtualService>> {
        self.services.get(name)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Gitaly stub
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Mock
// ─────────────────────────────────────────────────────────────────────────────

/// Mock service that echoes the lookup name back as the resolved value.
pub struct MockVirtualService;

#[async_trait]
impl VirtualService for MockVirtualService {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&HashMap<String, ColumnValue>],
        _org_id: i64,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        Ok(rows
            .iter()
            .map(|_| Some(ColumnValue::String(format!("mock:{lookup}"))))
            .collect())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Extract `root_namespace_id` from a traversal path.
///
/// Traversal paths follow the format `"org_id/root_namespace_id/..."`.
/// Returns the second segment parsed as i64.
pub fn extract_root_namespace_id(traversal_path: &str) -> Option<i64> {
    traversal_path
        .trim_end_matches('/')
        .split('/')
        .nth(1)?
        .parse::<i64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── extract_root_namespace_id ────────────────────────────────────────

    #[test]
    fn root_namespace_from_two_segment_path() {
        assert_eq!(extract_root_namespace_id("9970/123/"), Some(123));
    }

    #[test]
    fn root_namespace_from_three_segment_path() {
        assert_eq!(extract_root_namespace_id("9970/456/789/"), Some(456));
    }

    #[test]
    fn root_namespace_none_for_single_segment() {
        assert_eq!(extract_root_namespace_id("9970/"), None);
        assert_eq!(extract_root_namespace_id("9970"), None);
    }

    #[test]
    fn root_namespace_none_for_empty() {
        assert_eq!(extract_root_namespace_id(""), None);
    }

    // ── VirtualServiceRegistry ──────────────────────────────────────────

    #[test]
    fn registry_lookup() {
        let mut reg = VirtualServiceRegistry::new();
        assert!(reg.get("gitaly").is_none());

        reg.register("gitaly", Arc::new(MockVirtualService));
        assert!(reg.get("gitaly").is_some());
        assert!(reg.get("other").is_none());
    }

    // ── MockVirtualService ──────────────────────────────────────────────

    #[tokio::test]
    async fn mock_service_echoes_lookup() {
        let svc = MockVirtualService;
        let props = HashMap::new();
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&props, &props];

        let results = svc.resolve_batch("blob_content", &rows, 1).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            Some(ColumnValue::String("mock:blob_content".into()))
        );
    }

    // ── GitalyContentService ────────────────────────────────────────────

    #[test]
    fn gitaly_build_request_from_file_props() {
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
    fn gitaly_build_request_from_definition_props() {
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
    fn gitaly_build_request_none_without_project_id() {
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
    fn gitaly_build_request_prefers_file_path_over_path() {
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
    async fn gitaly_stub_returns_none() {
        let svc = GitalyContentService;
        let props = HashMap::new();
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&props, &props];

        let results = svc.resolve_batch("blob_content", &rows, 1).await.unwrap();
        assert_eq!(results, vec![None, None]);
    }
}
