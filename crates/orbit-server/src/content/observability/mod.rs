//! Resolves GitLab Observability virtual columns at query time.
//!
//! Observability data is not indexed into the graph: it is high cardinality,
//! lives in per-tenant ClickHouse databases, and expires on its own retention.
//! Rows reaching this resolver have already passed redaction, so a tenant is
//! only ever queried for rows the caller can see.

pub mod client_pool;
pub mod host_map;
pub mod logs;
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
use traces::TraceLookup;

#[derive(Clone, Copy)]
enum Lookup {
    Trace(TraceLookup),
    JobLogTail,
}

impl Lookup {
    fn from_name(lookup: &str) -> Option<Self> {
        if lookup == logs::JOB_LOG_TAIL {
            return Some(Self::JobLogTail);
        }
        TraceLookup::from_name(lookup).map(Self::Trace)
    }

    fn column_value(self, raw: String) -> Option<ColumnValue> {
        match self {
            Self::Trace(lookup) => lookup.column_value(raw),
            Self::JobLogTail => logs::column_value(raw),
        }
    }
}

pub const SERVICE_NAME: &str = "observability";

pub struct ObservabilityService {
    host_map: TenantHostMap,
    pool: TenantClientPool,
    max_log_lines: u32,
}

impl ObservabilityService {
    pub fn new(settings: &ObservabilityResolverSettings) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(settings.request_timeout_seconds))
            .build()?;

        Ok(Self {
            host_map: TenantHostMap::new(
                client,
                settings.provisioner_url.clone(),
                settings.provisioner_api_key.clone(),
                Duration::from_secs(settings.host_map_refresh_seconds),
            ),
            pool: TenantClientPool::new(
                settings.clickhouse_username.clone(),
                settings.clickhouse_password.clone(),
            ),
            max_log_lines: settings.max_log_lines,
        })
    }

    /// Groups rows by the tenant that owns them so each tenant is queried once.
    /// Rows whose namespace has no Observability instance are left unresolved.
    async fn routes_by_tenant(&self, rows: &[&PropertyRow]) -> HashMap<String, TenantRoute> {
        let mut routes: HashMap<String, TenantRoute> = HashMap::new();

        for props in rows {
            let Some(traversal_path) = props
                .get("traversal_path")
                .and_then(|value| value.coerce::<String>())
            else {
                continue;
            };

            if routes.contains_key(&traversal_path) {
                continue;
            }

            if let Some(route) = self.host_map.route_for(&traversal_path).await {
                routes.insert(traversal_path, route);
            }
        }

        routes
    }
}

#[async_trait]
impl ColumnResolver for ObservabilityService {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let Some(resolved_lookup) = Lookup::from_name(lookup) else {
            warn!(
                safe(QUERY_NAME) = lookup,
                "unknown observability lookup, resolving to null"
            );
            return Ok(vec![None; rows.len()]);
        };

        let routes = self.routes_by_tenant(rows).await;

        // Each row's identity is (tenant, entity id). Results are keyed by that
        // pair rather than by position so rows sharing a value cannot be
        // confused with one another.
        let keys: Vec<Option<(String, String)>> = rows
            .iter()
            .map(|props| {
                let traversal_path = props
                    .get("traversal_path")
                    .and_then(|value| value.coerce::<String>())?;
                routes.get(&traversal_path)?;
                Some((traversal_path, traces::entity_id(props)?))
            })
            .collect();

        let mut ids_by_tenant: HashMap<&String, Vec<String>> = HashMap::new();
        for (traversal_path, entity_id) in keys.iter().flatten() {
            let ids = ids_by_tenant.entry(traversal_path).or_default();
            if !ids.contains(entity_id) {
                ids.push(entity_id.clone());
            }
        }

        let futures = ids_by_tenant.iter().map(|(traversal_path, ids)| {
            let route = routes.get(*traversal_path).expect("route present");
            async move {
                let fetched = match resolved_lookup {
                    Lookup::Trace(trace_lookup) => {
                        traces::fetch(&self.pool, route, trace_lookup, ids).await
                    }
                    Lookup::JobLogTail => {
                        logs::fetch(&self.pool, route, ids, self.max_log_lines).await
                    }
                };

                let values = match fetched {
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
                ((*traversal_path).clone(), values)
            }
        });

        let resolved: HashMap<String, HashMap<String, String>> = futures::future::join_all(futures)
            .await
            .into_iter()
            .collect();

        Ok(keys
            .iter()
            .map(|key| {
                let (traversal_path, entity_id) = key.as_ref()?;
                let raw = resolved.get(traversal_path)?.get(entity_id)?;
                resolved_lookup.column_value(raw.clone())
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
