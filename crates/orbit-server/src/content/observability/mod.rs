pub mod client_pool;
pub mod host_map;
pub mod rows;
pub mod traces;

use std::collections::HashMap;
use std::future::Future;
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

/// A tenant whose ClickHouse accepts the connection and never answers would
/// otherwise hold the whole batch open.
async fn bounded<T, E>(
    timeout: Duration,
    fetch: impl Future<Output = Result<T, E>>,
) -> Result<Option<T>, E> {
    match tokio::time::timeout(timeout, fetch).await {
        Ok(Ok(value)) => Ok(Some(value)),
        Ok(Err(e)) => Err(e),
        Err(_) => Ok(None),
    }
}

pub struct ObservabilityService {
    host_map: TenantHostMap,
    pool: TenantClientPool,
    query_timeout: Duration,
}

impl ObservabilityService {
    pub fn new(settings: &ObservabilityResolverSettings) -> Result<Self, reqwest::Error> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // The https check only sees the configured URL, so a redirect would
        // carry the provisioner key somewhere unchecked.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(settings.request_timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
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
            query_timeout: Duration::from_secs(settings.query_timeout_seconds),
        })
    }

    async fn route_for(&self, props: &PropertyRow) -> Option<TenantRoute> {
        let traversal_path = props
            .get("traversal_path")
            .and_then(|value| value.coerce::<String>())?;

        self.host_map
            .route_for(&orbit_utils::traversal_path::TraversalPath::new_unchecked(
                traversal_path,
            ))
            .await
    }

    async fn evict_departed_tenants(&self) {
        self.pool.retain_routes(&self.host_map.live_routes().await);
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
            let fetch = traces::fetch(&self.pool, route, trace_lookup, keys);

            let values = match bounded(self.query_timeout, fetch).await {
                Ok(Some(values)) => values,
                Err(e) => {
                    warn!(
                        safe(ERROR_TYPE) = "observability_lookup",
                        safe(QUERY_NAME) = lookup,
                        expanded(ERROR_MESSAGE) = %e,
                        "observability lookup failed, resolving to null for this tenant"
                    );
                    HashMap::new()
                }
                Ok(None) => {
                    warn!(
                        safe(ERROR_TYPE) = "observability_timeout",
                        safe(QUERY_NAME) = lookup,
                        "observability lookup timed out, resolving to null for this tenant"
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
    use super::bounded;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn a_stalled_tenant_resolves_to_nothing_rather_than_holding_the_batch() {
        let never_answers = async {
            tokio::time::sleep(Duration::from_secs(600)).await;
            Ok::<&str, ()>("a trace id")
        };

        let resolved = bounded(Duration::from_secs(15), never_answers).await;

        assert_eq!(resolved, Ok(None));
    }

    #[tokio::test(start_paused = true)]
    async fn a_tenant_that_answers_in_time_resolves_normally() {
        let answers = async { Ok::<&str, ()>("a trace id") };

        assert_eq!(
            bounded(Duration::from_secs(15), answers).await,
            Ok(Some("a trace id"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_tenant_error_is_not_swallowed_by_the_bound() {
        let fails = async { Err::<&str, &str>("connection refused") };

        assert_eq!(
            bounded(Duration::from_secs(15), fails).await,
            Err("connection refused")
        );
    }
}
