//! Virtual column resolution infrastructure.
//!
//! The [`ColumnResolver`] trait and [`ColumnResolverRegistry`] are shared
//! between the server (Gitaly) and local (filesystem) pipelines.
//! [`resolve_virtual_columns`] contains the dispatch loop that both
//! pipelines call after hydration.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use pipeline::PipelineError;

use compiler::VirtualColumnRequest;

/// A single entity row's hydrated properties, keyed by column name.
pub type PropertyRow = HashMap<String, ColumnValue>;

/// Hydrated properties keyed by `(entity_type, id)`.
pub type PropertyMap = HashMap<(String, i64), PropertyRow>;

/// Entity type paired with the virtual columns that need resolution.
pub type EntityVirtualColumns<'a> = (&'a str, &'a [VirtualColumnRequest]);

const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Context passed to every [`ColumnResolver::resolve_batch`] call.
///
/// Wraps an optional security context and can be extended with
/// additional cross-cutting concerns without changing the trait
/// signature.
#[derive(Debug, Clone, Default)]
pub struct ResolverContext {
    pub security_context: Option<compiler::SecurityContext>,
}

/// A service that resolves virtual column values from an external source.
///
/// Implementations receive the hydrated property map for each entity row
/// and extract whatever parameters they need internally.
#[async_trait]
pub trait ColumnResolver: Send + Sync {
    /// Resolve a batch of rows for the given `lookup` operation.
    ///
    /// Returns a `Vec<Option<ColumnValue>>` aligned with `rows` -- `None`
    /// means the value could not be resolved for that row.
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError>;
}

/// Maps service names (e.g. `"gitaly"`) to their [`ColumnResolver`]
/// implementations, with a configurable batch size limit.
#[derive(Clone)]
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

/// Resolve virtual columns for all entity types in `entity_virtual_columns`.
///
/// For each entity type, looks up the matching rows in `property_map`,
/// dispatches to the registered [`ColumnResolver`] service, and merges
/// resolved values back into the property map.
pub async fn resolve_virtual_columns(
    registry: &ColumnResolverRegistry,
    resolver_ctx: &ResolverContext,
    entity_virtual_columns: &[EntityVirtualColumns<'_>],
    property_map: &mut PropertyMap,
) -> Result<(), PipelineError> {
    let has_work = entity_virtual_columns.iter().any(|(_, vc)| !vc.is_empty());
    if !has_work {
        return Ok(());
    }

    let max_batch = registry.max_batch_size();

    for &(entity_type, virtual_columns) in entity_virtual_columns {
        let valid_keys: Vec<(String, i64)> = property_map
            .keys()
            .filter(|(etype, _)| etype == entity_type)
            .cloned()
            .collect();

        if valid_keys.is_empty() {
            continue;
        }

        if valid_keys.len() > max_batch {
            return Err(PipelineError::ContentResolution(format!(
                "column resolver batch size {} exceeds limit {max_batch}",
                valid_keys.len(),
            )));
        }

        let service_lookups: Vec<_> = virtual_columns
            .iter()
            .map(|vcr| {
                let service = registry.get(&vcr.service).ok_or_else(|| {
                    PipelineError::ContentResolution(format!(
                        "no virtual service registered for '{}'",
                        vcr.service,
                    ))
                })?;
                Ok((vcr, Arc::clone(service)))
            })
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let prop_refs: Vec<&PropertyRow> = valid_keys
            .iter()
            .map(|k| property_map.get(k).expect("key validated above"))
            .collect();

        let futures = service_lookups.iter().map(|(vcr, service)| {
            let prop_refs = &prop_refs;
            async move {
                let results = service
                    .resolve_batch(&vcr.lookup, prop_refs, resolver_ctx)
                    .await?;
                if results.len() != prop_refs.len() {
                    return Err(PipelineError::ContentResolution(format!(
                        "service '{}' returned {} results for {} rows",
                        vcr.service,
                        results.len(),
                        prop_refs.len(),
                    )));
                }
                Ok(results)
            }
        });
        let all_results = futures::future::try_join_all(futures).await?;

        for ((vcr, _), results) in service_lookups.iter().zip(all_results) {
            for (i, value) in results.into_iter().enumerate() {
                if let Some(value) = value
                    && let Some(props) = property_map.get_mut(&valid_keys[i])
                {
                    props.insert(vcr.column_name.clone(), value);
                }
            }
        }
    }

    Ok(())
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
