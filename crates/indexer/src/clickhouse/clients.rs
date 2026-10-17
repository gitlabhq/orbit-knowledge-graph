use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::ClickHouseConfiguration;

#[derive(Clone)]
pub struct ClickHouseClients {
    pub graph: ArrowClickHouseClient,
    pub datalake: ArrowClickHouseClient,
}

impl ClickHouseClients {
    pub fn new(graph: &ClickHouseConfiguration, datalake: &ClickHouseConfiguration) -> Self {
        Self {
            graph: graph.build_client("graph").without_query_cache(),
            datalake: datalake.build_client("datalake").without_query_cache(),
        }
    }
}
