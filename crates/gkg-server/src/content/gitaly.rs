use std::collections::HashMap;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

use super::VirtualService;

/// Stub Gitaly virtual service.
///
/// Returns `None` for every row — the actual Gitaly client, request
/// building, file deduplication, and byte-range slicing are implemented
/// in a follow-up MR.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stub_returns_none() {
        let svc = GitalyContentService;
        let props = HashMap::new();
        let rows: Vec<&HashMap<String, ColumnValue>> = vec![&props, &props];

        let results = svc.resolve_batch("blob_content", &rows, 1).await.unwrap();
        assert_eq!(results, vec![None, None]);
    }
}
