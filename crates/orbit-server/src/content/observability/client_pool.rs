use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::{ClickHouseConfiguration, ProfilingConfig};

use super::host_map::TenantRoute;

/// Credentials are part of the identity: a rotated password must build a new
/// client rather than reuse one authenticating with the old one.
type ClientKey = (String, String, String, String);

pub struct TenantClientPool {
    clients: RwLock<HashMap<ClientKey, Arc<ArrowClickHouseClient>>>,
}

impl Default for TenantClientPool {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantClientPool {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn traces_client(&self, route: &TenantRoute) -> ArrowClickHouseClient {
        self.client_for(route, &route.traces_db)
    }

    /// Drops clients whose tenant no longer appears in the host map, so a
    /// moved or deprovisioned tenant does not keep a connection pool alive.
    pub fn retain_routes(&self, live: &[TenantRoute]) {
        let keys: Vec<ClientKey> = live
            .iter()
            .map(|route| Self::key_for(route, &route.traces_db))
            .collect();

        self.clients
            .write()
            .expect("tenant client pool poisoned")
            .retain(|key, _| keys.contains(key));
    }

    fn key_for(route: &TenantRoute, database: &str) -> ClientKey {
        (
            route.url(),
            database.to_string(),
            route.username.clone(),
            route.password.clone(),
        )
    }

    fn client_for(&self, route: &TenantRoute, database: &str) -> ArrowClickHouseClient {
        let key = Self::key_for(route, database);

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
        self.configuration_for(route, database)
            .build_client("observability")
    }

    /// No session settings: the tenant user carries a READONLY settings
    /// profile, and ClickHouse rejects any setting it does not already fix.
    fn configuration_for(&self, route: &TenantRoute, database: &str) -> ClickHouseConfiguration {
        ClickHouseConfiguration {
            database: database.to_string(),
            url: route.url(),
            username: route.username.clone(),
            password: Some(route.password.clone()),
            session_settings: HashMap::new(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let pool = TenantClientPool::new();
        let route = route("12345", "10.0.1.5");

        pool.traces_client(&route);
        pool.traces_client(&route);

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn tenants_sharing_a_host_do_not_share_a_client() {
        let pool = TenantClientPool::new();

        pool.traces_client(&route("12345", "10.0.1.5"));
        pool.traces_client(&route("67890", "10.0.1.5"));

        assert_eq!(pool.clients.read().unwrap().len(), 2);
    }

    #[test]
    fn a_tenant_that_leaves_the_host_map_is_evicted() {
        let pool = TenantClientPool::new();
        let staying = route("12345", "10.0.1.5");
        let leaving = route("67890", "10.0.1.5");
        pool.traces_client(&staying);
        pool.traces_client(&leaving);

        pool.retain_routes(std::slice::from_ref(&staying));

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn a_tenant_that_moves_host_does_not_leak_its_old_client() {
        let pool = TenantClientPool::new();
        let before = route("12345", "10.0.1.5");
        let after = route("12345", "10.0.2.9");
        pool.traces_client(&before);
        pool.traces_client(&after);
        assert_eq!(pool.clients.read().unwrap().len(), 2);

        pool.retain_routes(std::slice::from_ref(&after));

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn a_rotated_password_builds_a_new_client() {
        let pool = TenantClientPool::new();
        let before = route("12345", "10.0.1.5");
        let mut after = before.clone();
        after.password = "rotated".to_string();

        pool.traces_client(&before);
        pool.traces_client(&after);

        assert_eq!(
            pool.clients.read().unwrap().len(),
            2,
            "the old client still authenticates with the old password"
        );
    }

    /// The tenant user carries a READONLY settings profile, so any session
    /// setting outside it is rejected with code 164 and the query never runs.
    #[test]
    fn no_session_settings_are_sent_to_a_tenant() {
        let pool = TenantClientPool::new();

        let config = pool.configuration_for(&route("12345", "10.0.1.5"), "12345_signoz_traces");

        assert!(
            config.session_settings.is_empty(),
            "the server profile owns the limits: {:?}",
            config.session_settings
        );
    }
}
