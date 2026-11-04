use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::{
    ClickHouseConfiguration, ObservabilityResolverSettings, ProfilingConfig,
};

use super::host_map::TenantRoute;

type ClientKey = (String, String);

pub struct TenantClientPool {
    query_timeout_seconds: u64,
    max_result_rows: u64,
    clients: RwLock<HashMap<ClientKey, Arc<ArrowClickHouseClient>>>,
}

impl TenantClientPool {
    pub fn new(settings: &ObservabilityResolverSettings) -> Self {
        Self {
            query_timeout_seconds: settings.query_timeout_seconds,
            max_result_rows: settings.max_result_rows,
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn traces_client(&self, route: &TenantRoute) -> ArrowClickHouseClient {
        self.client_for(route, &route.traces_db)
    }

    pub fn logs_client(&self, route: &TenantRoute) -> ArrowClickHouseClient {
        self.client_for(route, &route.logs_db)
    }

    /// Drops clients whose tenant no longer appears in the host map, so a
    /// moved or deprovisioned tenant does not keep a connection pool alive.
    pub fn retain_databases(&self, live: &[String]) {
        self.clients
            .write()
            .expect("tenant client pool poisoned")
            .retain(|(_, database), _| live.iter().any(|name| name == database));
    }

    fn client_for(&self, route: &TenantRoute, database: &str) -> ArrowClickHouseClient {
        let key = (route.url(), database.to_string());

        if let Some(client) = self
            .clients
            .read()
            .expect("tenant client pool poisoned")
            .get(&key)
        {
            return client.as_ref().clone();
        }

        let client = Arc::new(self.build_client(route, database));
        self.clients
            .write()
            .expect("tenant client pool poisoned")
            .insert(key, Arc::clone(&client));

        client.as_ref().clone()
    }

    fn build_client(&self, route: &TenantRoute, database: &str) -> ArrowClickHouseClient {
        ClickHouseConfiguration {
            database: database.to_string(),
            url: route.url(),
            username: route.username.clone(),
            password: Some(route.password.clone()),
            session_settings: self.session_settings(),
            replicated: false,
            insert_settings: HashMap::new(),
            profiling: ProfilingConfig {
                enabled: false,
                explain: false,
                query_log: false,
                processors: false,
                instance_health: false,
            },
        }
        .build_client("observability")
    }

    /// Tenant servers are shared between tenants, so every query is bounded
    /// server-side as well as by the client timeout.
    fn session_settings(&self) -> HashMap<String, String> {
        HashMap::from([
            ("readonly".to_string(), "1".to_string()),
            (
                "max_execution_time".to_string(),
                self.query_timeout_seconds.to_string(),
            ),
            (
                "max_result_rows".to_string(),
                self.max_result_rows.to_string(),
            ),
            ("result_overflow_mode".to_string(), "break".to_string()),
            ("timeout_overflow_mode".to_string(), "throw".to_string()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> ObservabilityResolverSettings {
        ObservabilityResolverSettings {
            provisioner_url: "https://provisioner.internal".to_string(),
            provisioner_api_key: "key".to_string(),
            host_map_refresh_seconds: 300,
            host_map_retry_after_seconds: 30,
            request_timeout_seconds: 10,
            query_timeout_seconds: 15,
            max_result_rows: 10_000,
        }
    }

    fn route(group_id: &str, host: &str) -> TenantRoute {
        TenantRoute {
            group_id: group_id.to_string(),
            scheme: "http".to_string(),
            host: host.to_string(),
            port: "8123".to_string(),
            username: "orbit_reader".to_string(),
            password: "secret".to_string(),
            traces_db: format!("{group_id}_signoz_traces"),
            logs_db: format!("{group_id}_signoz_logs"),
        }
    }

    #[test]
    fn a_tenant_client_is_cached_and_reused() {
        let pool = TenantClientPool::new(&settings());
        let route = route("12345", "10.0.1.5");

        pool.traces_client(&route);
        pool.traces_client(&route);

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn traces_and_logs_are_separate_clients_on_one_host() {
        let pool = TenantClientPool::new(&settings());
        let route = route("12345", "10.0.1.5");

        pool.traces_client(&route);
        pool.logs_client(&route);

        assert_eq!(pool.clients.read().unwrap().len(), 2);
    }

    #[test]
    fn tenants_sharing_a_host_do_not_share_a_client() {
        let pool = TenantClientPool::new(&settings());

        pool.traces_client(&route("12345", "10.0.1.5"));
        pool.traces_client(&route("67890", "10.0.1.5"));

        assert_eq!(pool.clients.read().unwrap().len(), 2);
    }

    #[test]
    fn a_tenant_that_leaves_the_host_map_is_evicted() {
        let pool = TenantClientPool::new(&settings());
        let staying = route("12345", "10.0.1.5");
        let leaving = route("67890", "10.0.1.5");
        pool.traces_client(&staying);
        pool.traces_client(&leaving);

        pool.retain_databases(std::slice::from_ref(&staying.traces_db));

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn every_tenant_query_is_bounded_server_side() {
        let pool = TenantClientPool::new(&settings());

        let applied = pool.session_settings();

        assert_eq!(applied.get("readonly"), Some(&"1".to_string()));
        assert_eq!(applied.get("max_execution_time"), Some(&"15".to_string()));
        assert_eq!(applied.get("max_result_rows"), Some(&"10000".to_string()));
    }
}
