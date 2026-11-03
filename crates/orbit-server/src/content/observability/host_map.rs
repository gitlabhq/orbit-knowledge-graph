//! Routing from a graph row's namespace to a tenant's Observability databases.
//!
//! Observability is provisioned per group, and a group may be any namespace in
//! the hierarchy. A row's `traversal_path` is the full ancestor chain, so the
//! nearest provisioned ancestor owns the row's data. The provisioner owns the
//! group to host mapping because tenants move between central ClickHouse
//! servers, so a static map would go stale.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use orbit_log_fields::warn;
use serde::Deserialize;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TenantRoute {
    pub group_id: String,
    pub host: String,
    pub port: u16,
    pub traces_db: String,
    pub logs_db: String,
}

impl TenantRoute {
    pub fn url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Deserialize)]
struct HostMapResponse {
    clickhouse_hosts: Vec<TenantRoute>,
}

/// Splits a `traversal_path` ("9970/12345/") into its namespace IDs, nearest
/// first, so a subgroup's own Observability instance wins over an ancestor's.
pub fn namespace_ids_nearest_first(traversal_path: &str) -> Vec<&str> {
    let mut ids: Vec<&str> = traversal_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    ids.reverse();
    ids
}

pub struct TenantHostMap {
    client: reqwest::Client,
    provisioner_url: String,
    api_key: String,
    refresh_after: Duration,
    cache: RwLock<Option<CachedRoutes>>,
}

struct CachedRoutes {
    routes: Arc<HashMap<String, TenantRoute>>,
    fetched_at: Instant,
}

impl TenantHostMap {
    pub fn new(
        client: reqwest::Client,
        provisioner_url: String,
        api_key: String,
        refresh_after: Duration,
    ) -> Self {
        Self {
            client,
            provisioner_url: provisioner_url.trim_end_matches('/').to_string(),
            api_key,
            refresh_after,
            cache: RwLock::new(None),
        }
    }

    /// Returns the route for the nearest provisioned ancestor of this path.
    pub async fn route_for(&self, traversal_path: &str) -> Option<TenantRoute> {
        let routes = self.routes().await?;
        namespace_ids_nearest_first(traversal_path)
            .into_iter()
            .find_map(|id| routes.get(id).cloned())
    }

    async fn routes(&self) -> Option<Arc<HashMap<String, TenantRoute>>> {
        if let Some(cached) = self.fresh_cached().await {
            return Some(cached);
        }

        match self.fetch().await {
            Ok(routes) => {
                let routes = Arc::new(routes);
                *self.cache.write().await = Some(CachedRoutes {
                    routes: Arc::clone(&routes),
                    fetched_at: Instant::now(),
                });
                Some(routes)
            }
            Err(e) => {
                warn!(
                    safe(ERROR_TYPE) = "host_map_refresh",
                    expanded(ERROR_MESSAGE) = %e,
                    "observability host map refresh failed"
                );
                // A stale map routes better than no map: tenants rarely move,
                // and the alternative is dropping every lookup until recovery.
                self.stale_cached().await
            }
        }
    }

    async fn fresh_cached(&self) -> Option<Arc<HashMap<String, TenantRoute>>> {
        let cache = self.cache.read().await;
        let cached = cache.as_ref()?;
        (cached.fetched_at.elapsed() < self.refresh_after).then(|| Arc::clone(&cached.routes))
    }

    async fn stale_cached(&self) -> Option<Arc<HashMap<String, TenantRoute>>> {
        let cache = self.cache.read().await;
        cache.as_ref().map(|cached| Arc::clone(&cached.routes))
    }

    async fn fetch(&self) -> Result<HashMap<String, TenantRoute>, reqwest::Error> {
        let response: HostMapResponse = self
            .client
            .get(format!(
                "{}/api/v1/orbit/clickhouse_hosts",
                self.provisioner_url
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(response
            .clickhouse_hosts
            .into_iter()
            .map(|route| (route.group_id.clone(), route))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_ids_are_returned_nearest_first() {
        assert_eq!(
            namespace_ids_nearest_first("9970/12345/67890/"),
            vec!["67890", "12345", "9970"]
        );
    }

    #[test]
    fn a_root_only_path_yields_one_id() {
        assert_eq!(namespace_ids_nearest_first("9970/"), vec!["9970"]);
    }

    #[test]
    fn an_empty_path_yields_no_ids() {
        assert!(namespace_ids_nearest_first("").is_empty());
        assert!(namespace_ids_nearest_first("/").is_empty());
    }

    #[test]
    fn a_path_without_a_trailing_slash_still_parses() {
        assert_eq!(
            namespace_ids_nearest_first("9970/12345"),
            vec!["12345", "9970"]
        );
    }

    #[test]
    fn route_url_uses_the_http_interface() {
        let route = TenantRoute {
            group_id: "12345".to_string(),
            host: "10.0.1.5".to_string(),
            port: 8123,
            traces_db: "12345_signoz_traces".to_string(),
            logs_db: "12345_signoz_logs".to_string(),
        };

        assert_eq!(route.url(), "http://10.0.1.5:8123");
    }
}
