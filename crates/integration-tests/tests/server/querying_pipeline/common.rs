use std::collections::HashMap;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;

use gkg_server::content::{ColumnResolver, PropertyRow, ResolverContext};

/// Mock resolver that echoes the lookup name back as the resolved value.
pub struct MockColumnResolver;

#[async_trait]
impl ColumnResolver for MockColumnResolver {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
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
    use query_engine::compiler::SecurityContext;

    #[tokio::test]
    async fn mock_resolver_echoes_lookup() {
        let svc = MockColumnResolver;
        let props = HashMap::new();
        let rows: Vec<&PropertyRow> = vec![&props, &props];

        let rctx = ResolverContext {
            security_context: SecurityContext::new(1, vec!["1/2/".into()]).unwrap(),
        };
        let results = svc
            .resolve_batch("blob_content", &rows, &rctx)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            Some(ColumnValue::String("mock:blob_content".into()))
        );
    }
}
