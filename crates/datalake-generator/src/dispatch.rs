use anyhow::{Context, Result};
use tracing::info;

use crate::config::SimulatorConfig;

pub async fn run_dispatch_indexing(config: &SimulatorConfig) -> Result<()> {
    info!("dispatching indexing");

    let nats_config = indexer::nats::NatsConfiguration {
        url: config.nats.url.clone(),
        ..Default::default()
    };

    let datalake_config = indexer::clickhouse::ClickHouseConfiguration {
        url: config.datalake.url.clone(),
        database: config.datalake.database.clone(),
        username: config.datalake.username.clone(),
        password: config.datalake.password.clone(),
    };

    indexer::dispatcher::run(&nats_config, &datalake_config)
        .await
        .context("dispatch indexing failed")?;

    info!("dispatch indexing completed");
    Ok(())
}
