//! Per-tenant ClickHouse clients.
//!
//! Tenants share central ClickHouse servers and are separated by database
//! name, so a client is identified by host and database rather than host
//! alone. Clients are cheap to build but hold a connection pool, so they are
//! cached per tenant database.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::{ClickHouseConfiguration, ProfilingConfig};

use super::host_map::TenantRoute;

type ClientKey = (String, String);

pub struct TenantClientPool {
    username: String,
    password: String,
    clients: RwLock<HashMap<ClientKey, Arc<ArrowClickHouseClient>>>,
}

impl TenantClientPool {
    pub fn new(username: String, password: String) -> Self {
        Self {
            username,
            password,
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn traces_client(&self, route: &TenantRoute) -> ArrowClickHouseClient {
        self.client_for(&route.url(), &route.traces_db)
    }

    pub fn logs_client(&self, route: &TenantRoute) -> ArrowClickHouseClient {
        self.client_for(&route.url(), &route.logs_db)
    }

    fn client_for(&self, url: &str, database: &str) -> ArrowClickHouseClient {
        let key = (url.to_string(), database.to_string());

        if let Some(client) = self
            .clients
            .read()
            .expect("tenant client pool poisoned")
            .get(&key)
        {
            return client.as_ref().clone();
        }

        let client = Arc::new(self.build_client(url, database));
        self.clients
            .write()
            .expect("tenant client pool poisoned")
            .insert(key, Arc::clone(&client));

        client.as_ref().clone()
    }

    fn build_client(&self, url: &str, database: &str) -> ArrowClickHouseClient {
        ClickHouseConfiguration {
            database: database.to_string(),
            url: url.to_string(),
            username: self.username.clone(),
            password: Some(self.password.clone()),
            session_settings: HashMap::new(),
            // Tenant observability data is a single-replica SigNoz deployment,
            // not the replicated graph store.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(group_id: &str, host: &str) -> TenantRoute {
        TenantRoute {
            group_id: group_id.to_string(),
            host: host.to_string(),
            port: 8123,
            traces_db: format!("{group_id}_signoz_traces"),
            logs_db: format!("{group_id}_signoz_logs"),
        }
    }

    #[test]
    fn a_tenant_client_is_cached_and_reused() {
        let pool = TenantClientPool::new("orbit_reader".to_string(), "secret".to_string());
        let route = route("12345", "10.0.1.5");

        pool.traces_client(&route);
        pool.traces_client(&route);

        assert_eq!(pool.clients.read().unwrap().len(), 1);
    }

    #[test]
    fn traces_and_logs_are_separate_clients_on_one_host() {
        let pool = TenantClientPool::new("orbit_reader".to_string(), "secret".to_string());
        let route = route("12345", "10.0.1.5");

        pool.traces_client(&route);
        pool.logs_client(&route);

        assert_eq!(pool.clients.read().unwrap().len(), 2);
    }

    #[test]
    fn tenants_sharing_a_host_do_not_share_a_client() {
        let pool = TenantClientPool::new("orbit_reader".to_string(), "secret".to_string());

        pool.traces_client(&route("12345", "10.0.1.5"));
        pool.traces_client(&route("67890", "10.0.1.5"));

        assert_eq!(pool.clients.read().unwrap().len(), 2);
    }
}
