pub mod gitaly;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

/// Maximum rows per virtual service batch call.
pub const MAX_VIRTUAL_BATCH_SIZE: usize = 100;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lookup() {
        let mut reg = VirtualServiceRegistry::new();
        assert!(reg.get("gitaly").is_none());

        reg.register("gitaly", Arc::new(MockVirtualService));
        assert!(reg.get("gitaly").is_some());
        assert!(reg.get("other").is_none());
    }

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
}
