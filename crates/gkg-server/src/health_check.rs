use clickhouse_client::ClickHouseConfigurationExt;
use health_check::{HealthChecker, run_server};

use gkg_server_config::AppConfig;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Health check error: {0}")]
    HealthCheck(#[from] health_check::Error),
}

pub async fn run(config: &AppConfig) -> Result<(), Error> {
    let clickhouse_client = config.graph.build_client();
    let checker = HealthChecker::new(&config.health_check, clickhouse_client).await?;

    run_server(config.health_check.bind_address, checker).await?;

    Ok(())
}
