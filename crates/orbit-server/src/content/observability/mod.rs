pub mod client_pool;
pub mod host_map;
pub mod rows;
pub mod traces;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orbit_arrow::ColumnValue;
use orbit_log_fields::warn;
use orbit_server_config::ObservabilityResolverSettings;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

use client_pool::TenantClientPool;
use host_map::{TenantHostMap, TenantRoute};
use traces::{TraceKey, TraceLookup};

pub const SERVICE_NAME: &str = "observability";

/// Checked per row rather than once at startup, so the flag's namespace
/// allowlist can gate a rollout to one root namespace at a time.
fn enabled_for_path(traversal_path: &str) -> bool {
    let root = orbit_utils::traversal_path::TraversalPath::new_unchecked(traversal_path)
        .top_level_namespace_id();

    orbit_server_config::features::enabled_for(
        orbit_server_config::Feature::ObservabilityColumns,
        root,
    )
}

pub struct ObservabilityService {
    host_map: TenantHostMap,
    pool: TenantClientPool,
}

impl ObservabilityService {
    pub fn new(settings: &ObservabilityResolverSettings) -> Result<Self, reqwest::Error> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(settings.request_timeout_seconds))
            .build()?;

        Ok(Self {
            host_map: TenantHostMap::new(
                client,
                settings.provisioner_url.clone(),
                settings.provisioner_api_key.clone(),
                Duration::from_secs(settings.host_map_refresh_seconds),
                Duration::from_secs(settings.host_map_retry_after_seconds),
            ),
            pool: TenantClientPool::new(),
        })
    }

    async fn route_for(&self, props: &PropertyRow) -> Option<TenantRoute> {
        let traversal_path = props
            .get("traversal_path")
            .and_then(|value| value.coerce::<String>())?;

        if !enabled_for_path(&traversal_path) {
            return None;
        }

        self.host_map.route_for(&traversal_path).await
    }

    async fn evict_departed_tenants(&self) {
        self.pool
            .retain_databases(&self.host_map.live_databases().await);
    }
}

/// Rows are grouped by the tenant database they resolve against, not by
/// traversal path: many projects under one provisioned group share a tenant,
/// and grouping by path would query that tenant once per project.
type TenantBatch = (TenantRoute, Vec<TraceKey>);

#[async_trait]
impl ColumnResolver for ObservabilityService {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let Some(trace_lookup) = TraceLookup::from_name(lookup) else {
            warn!(
                safe(QUERY_NAME) = lookup,
                "unknown observability lookup, resolving to null"
            );
            return Ok(vec![None; rows.len()]);
        };

        let mut row_keys: Vec<Option<(String, TraceKey)>> = Vec::with_capacity(rows.len());
        let mut batches: HashMap<String, TenantBatch> = HashMap::new();

        for props in rows {
            let Some(route) = self.route_for(props).await else {
                row_keys.push(None);
                continue;
            };
            let Some(key) = traces::trace_key(props) else {
                row_keys.push(None);
                continue;
            };

            let batch = batches
                .entry(route.traces_db.clone())
                .or_insert_with(|| (route.clone(), Vec::new()));
            if !batch
                .1
                .iter()
                .any(|existing| existing.entity_id == key.entity_id)
            {
                batch.1.push(key.clone());
            }

            row_keys.push(Some((route.traces_db.clone(), key)));
        }

        let futures = batches.iter().map(|(database, (route, keys))| async move {
            let values = match traces::fetch(&self.pool, route, trace_lookup, keys).await {
                Ok(values) => values,
                Err(e) => {
                    warn!(
                        safe(ERROR_TYPE) = "observability_lookup",
                        safe(QUERY_NAME) = lookup,
                        expanded(ERROR_MESSAGE) = %e,
                        "observability lookup failed, resolving to null for this tenant"
                    );
                    HashMap::new()
                }
            };
            (database.clone(), values)
        });

        let resolved: HashMap<String, HashMap<String, String>> = futures::future::join_all(futures)
            .await
            .into_iter()
            .collect();

        self.evict_departed_tenants().await;

        Ok(row_keys
            .iter()
            .map(|row_key| {
                let (database, key) = row_key.as_ref()?;
                let raw = resolved.get(database)?.get(&key.entity_id)?;
                trace_lookup.column_value(raw.clone())
            })
            .collect())
    }
}

pub fn build(settings: &ObservabilityResolverSettings) -> Option<Arc<ObservabilityService>> {
    match ObservabilityService::new(settings) {
        Ok(service) => Some(Arc::new(service)),
        Err(e) => {
            warn!(
                safe(ERROR_TYPE) = "observability_client_build",
                expanded(ERROR_MESSAGE) = %e,
                "observability resolver disabled, HTTP client construction failed"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    fn root_namespace_of(traversal_path: &str) -> Option<i64> {
        orbit_utils::traversal_path::TraversalPath::new_unchecked(traversal_path)
            .top_level_namespace_id()
    }

    /// The first segment is the organization, so reading it would both miss
    /// every allowlisted namespace and match other tenants in that org.
    #[test]
    fn the_gate_reads_the_root_namespace_not_the_organization() {
        assert_eq!(root_namespace_of("9970/12345/67890/"), Some(12345));
        assert_eq!(root_namespace_of("9970/12345/"), Some(12345));
    }

    #[test]
    fn a_path_without_a_root_namespace_has_none() {
        assert_eq!(root_namespace_of("9970/"), None);
        assert_eq!(root_namespace_of(""), None);
    }

    /// Off until features::init runs, so an unconfigured server resolves
    /// nothing rather than resolving for everyone.
    #[test]
    fn the_gate_is_closed_before_the_flag_is_installed() {
        assert!(!super::enabled_for_path("9970/12345/"));
    }
}
