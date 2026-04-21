use clickhouse_client::ClickHouseConfigurationExt;
use health_check::{ClickHouseInstance, HealthChecker, run_server};

use gkg_server_config::AppConfig;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Health check error: {0}")]
    HealthCheck(#[from] health_check::Error),
}

pub async fn run(config: &AppConfig) -> Result<(), Error> {
    let instances = build_clickhouse_instances(config);
    let checker = HealthChecker::new(&config.health_check, instances).await?;

    run_server(config.health_check.bind_address, checker).await?;

    Ok(())
}

fn build_clickhouse_instances(config: &AppConfig) -> Vec<ClickHouseInstance> {
    let same_host = config.graph.url == config.datalake.url;

    if same_host {
        vec![ClickHouseInstance {
            name: "clickhouse".to_string(),
            client: config.graph.build_client(),
        }]
    } else {
        vec![
            ClickHouseInstance {
                name: "clickhouse-graph".to_string(),
                client: config.graph.build_client(),
            },
            ClickHouseInstance {
                name: "clickhouse-datalake".to_string(),
                client: config.datalake.build_client(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_urls(graph_url: &str, datalake_url: &str) -> AppConfig {
        let json = format!(
            r#"{{
                "graph": {{ "url": "{graph_url}", "database": "default", "username": "default" }},
                "datalake": {{ "url": "{datalake_url}", "database": "default", "username": "default" }}
            }}"#,
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn same_url_deduplicates_to_single_instance() {
        let config = config_with_urls("http://ch:8123", "http://ch:8123");
        let instances = build_clickhouse_instances(&config);
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].name, "clickhouse");
    }

    #[test]
    fn different_urls_produce_two_instances() {
        let config = config_with_urls("http://ch-graph:8123", "http://ch-datalake:8123");
        let instances = build_clickhouse_instances(&config);
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].name, "clickhouse-graph");
        assert_eq!(instances[1].name, "clickhouse-datalake");
    }
}
