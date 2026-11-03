use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use orbit_log_fields::warn;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TenantRoute {
    pub group_id: String,
    pub scheme: String,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub traces_db: String,
    pub logs_db: String,
}

impl TenantRoute {
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

#[derive(Debug, Deserialize)]
struct HostMapResponse {
    clickhouse_hosts: Vec<TenantRoute>,
}

pub type TenantRoutes = Arc<HashMap<String, TenantRoute>>;

/// Nearest first, so a subgroup's own instance wins over an ancestor's.
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
    retry_after: Duration,
    cache: RwLock<Option<CachedRoutes>>,
    single_flight: Mutex<()>,
}

struct CachedRoutes {
    routes: TenantRoutes,
    usable_until: Instant,
    retry_fetch_at: Instant,
}

impl CachedRoutes {
    fn is_fresh(&self, now: Instant) -> bool {
        now < self.usable_until
    }

    fn may_retry(&self, now: Instant) -> bool {
        now >= self.retry_fetch_at
    }
}

impl TenantHostMap {
    pub fn new(
        client: reqwest::Client,
        provisioner_url: String,
        api_key: String,
        refresh_after: Duration,
        retry_after: Duration,
    ) -> Self {
        Self {
            client,
            provisioner_url: provisioner_url.trim_end_matches('/').to_string(),
            api_key,
            refresh_after,
            retry_after,
            cache: RwLock::new(None),
            single_flight: Mutex::new(()),
        }
    }

    pub async fn route_for(&self, traversal_path: &str) -> Option<TenantRoute> {
        let routes = self.routes().await?;
        namespace_ids_nearest_first(traversal_path)
            .into_iter()
            .find_map(|id| routes.get(id).cloned())
    }

    async fn routes(&self) -> Option<TenantRoutes> {
        if let Some(cached) = self.usable_cached().await {
            return Some(cached);
        }

        let _single_refresh_in_flight = self.single_flight.lock().await;

        if let Some(cached) = self.usable_cached().await {
            return Some(cached);
        }

        match self.fetch().await {
            Ok(routes) => {
                let routes: TenantRoutes = Arc::new(routes);
                self.store(Arc::clone(&routes), self.refresh_after).await;
                Some(routes)
            }
            Err(e) => {
                warn!(
                    safe(ERROR_TYPE) = "host_map_refresh",
                    expanded(ERROR_MESSAGE) = %e,
                    "observability host map refresh failed"
                );
                self.defer_retry().await
            }
        }
    }

    /// Fresh, or stale but within the retry-after window. Serving a stale map
    /// routes better than serving none, and holding off the retry keeps a
    /// provisioner outage from adding the request timeout to every query.
    async fn usable_cached(&self) -> Option<TenantRoutes> {
        let now = Instant::now();
        let cache = self.cache.read().await;
        let cached = cache.as_ref()?;

        (cached.is_fresh(now) || !cached.may_retry(now)).then(|| Arc::clone(&cached.routes))
    }

    async fn defer_retry(&self) -> Option<TenantRoutes> {
        let mut cache = self.cache.write().await;
        let cached = cache.as_mut()?;
        cached.retry_fetch_at = Instant::now() + self.retry_after;
        Some(Arc::clone(&cached.routes))
    }

    async fn store(&self, routes: TenantRoutes, refresh_after: Duration) {
        let now = Instant::now();
        *self.cache.write().await = Some(CachedRoutes {
            routes,
            usable_until: now + refresh_after,
            retry_fetch_at: now,
        });
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

    fn route(group_id: &str) -> TenantRoute {
        TenantRoute {
            group_id: group_id.to_string(),
            scheme: "http".to_string(),
            host: "10.0.1.5".to_string(),
            port: "8123".to_string(),
            username: "orbit_reader".to_string(),
            password: "secret".to_string(),
            traces_db: format!("{group_id}_signoz_traces"),
            logs_db: format!("{group_id}_signoz_logs"),
        }
    }

    fn host_map() -> TenantHostMap {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        TenantHostMap::new(
            reqwest::Client::new(),
            "https://provisioner.internal".to_string(),
            "key".to_string(),
            Duration::from_secs(300),
            Duration::from_secs(30),
        )
    }

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
    fn route_url_is_built_from_the_advertised_scheme_and_port() {
        assert_eq!(route("12345").url(), "http://10.0.1.5:8123");

        let mut tls = route("12345");
        tls.scheme = "https".to_string();
        tls.port = "8443".to_string();
        assert_eq!(tls.url(), "https://10.0.1.5:8443");
    }

    #[tokio::test]
    async fn a_fresh_map_is_served_without_refetching() {
        let map = host_map();
        map.store(
            Arc::new(HashMap::from([("12345".to_string(), route("12345"))])),
            Duration::from_secs(300),
        )
        .await;

        assert!(map.usable_cached().await.is_some());
    }

    #[tokio::test]
    async fn an_expired_map_is_not_served_as_fresh() {
        let map = host_map();
        map.store(
            Arc::new(HashMap::from([("12345".to_string(), route("12345"))])),
            Duration::ZERO,
        )
        .await;

        assert!(map.usable_cached().await.is_none());
    }

    #[tokio::test]
    async fn a_failed_refresh_serves_the_stale_map_and_defers_the_next_attempt() {
        let map = host_map();
        map.store(
            Arc::new(HashMap::from([("12345".to_string(), route("12345"))])),
            Duration::ZERO,
        )
        .await;

        assert!(map.defer_retry().await.is_some());
        assert!(
            map.usable_cached().await.is_some(),
            "a deferred retry should keep serving the stale map rather than refetching"
        );
    }

    #[tokio::test]
    async fn the_nearest_provisioned_ancestor_wins() {
        let map = host_map();
        map.store(
            Arc::new(HashMap::from([
                ("9970".to_string(), route("9970")),
                ("12345".to_string(), route("12345")),
            ])),
            Duration::from_secs(300),
        )
        .await;

        let resolved = map.route_for("9970/12345/67890/").await.expect("route");

        assert_eq!(resolved.group_id, "12345");
    }

    #[tokio::test]
    async fn a_path_with_no_provisioned_ancestor_resolves_to_nothing() {
        let map = host_map();
        map.store(
            Arc::new(HashMap::from([("9970".to_string(), route("9970"))])),
            Duration::from_secs(300),
        )
        .await;

        assert!(map.route_for("8888/7777/").await.is_none());
    }
}
