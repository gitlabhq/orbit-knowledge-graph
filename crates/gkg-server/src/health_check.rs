use health_check::{HealthCheckConfig, HealthChecker, run_server};

use crate::config::AppConfig;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Health check error: {0}")]
    HealthCheck(#[from] health_check::Error),
}

pub async fn run(config: &AppConfig) -> Result<(), Error> {
    let health_config = HealthCheckConfig::from_env();
    let clickhouse_client = config.graph.build_client();

    let checker = HealthChecker::new(&health_config, clickhouse_client).await?;

    run_server(health_config.bind_address, checker).await?;

    Ok(())
}
