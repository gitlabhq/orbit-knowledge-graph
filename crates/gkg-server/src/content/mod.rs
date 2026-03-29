use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::compiler::SecurityContext;
use query_engine::pipeline::PipelineError;

/// A single entity row's hydrated properties, keyed by column name.
pub type PropertyRow = HashMap<String, ColumnValue>;

const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Context passed to every [`ColumnResolver::resolve_batch`] call.
/// Wraps [`SecurityContext`] and can be extended with additional
/// cross-cutting concerns (e.g. request ID for tracing) without
/// changing the trait signature.
#[derive(Debug, Clone)]
pub struct ResolverContext {
    pub security_context: SecurityContext,
}

/// A service that resolves virtual column values from an external source.
///
/// Implementations receive the hydrated property map for each entity row
/// and extract whatever parameters they need internally. This keeps the
/// trait generic — a Gitaly implementation reads `project_id`/`branch`/`path`,
/// while a hypothetical CI service would read `pipeline_id`/`job_id`.
#[async_trait]
pub trait ColumnResolver: Send + Sync {
    /// Resolve a batch of rows for the given `lookup` operation.
    ///
    /// `rows` contains one property map per entity. Returns a
    /// `Vec<Option<ColumnValue>>` aligned with `rows` — `None` means
    /// the value could not be resolved for that row.
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError>;
}

/// Maps service names (e.g. `"gitaly"`) to their [`ColumnResolver`]
/// implementations, with a configurable batch size limit.
pub struct ColumnResolverRegistry {
    services: HashMap<String, Arc<dyn ColumnResolver>>,
    max_batch_size: usize,
}

impl Default for ColumnResolverRegistry {
    fn default() -> Self {
        Self {
            services: HashMap::new(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }
}

impl ColumnResolverRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn register(&mut self, name: impl Into<String>, service: Arc<dyn ColumnResolver>) {
        self.services.insert(name.into(), service);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn ColumnResolver>> {
        self.services.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lookup() {
        let reg = ColumnResolverRegistry::new();
        assert!(reg.get("gitaly").is_none());
    }

    #[test]
    fn registry_default_batch_size() {
        let reg = ColumnResolverRegistry::new();
        assert_eq!(reg.max_batch_size(), DEFAULT_MAX_BATCH_SIZE);
    }

    #[test]
    fn registry_custom_batch_size() {
        let reg = ColumnResolverRegistry::new().with_max_batch_size(50);
        assert_eq!(reg.max_batch_size(), 50);
    }
}
