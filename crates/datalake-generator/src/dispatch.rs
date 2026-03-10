use anyhow::{Context, Result};
use indexer::modules::sdlc::dispatch::{
    GlobalDispatcher, GlobalDispatcherConfig, NamespaceDispatcher, NamespaceDispatcherConfig,
};
use indexer::scheduler::ScheduledTask;
use indexer::scheduler::ScheduledTaskMetrics;
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

    let services = indexer::scheduler::connect(&nats_config)
        .await
        .context("dispatcher connect failed")?;

    let datalake = datalake_config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let lock_service = services.lock_service.clone();
    let tasks: Vec<Box<dyn ScheduledTask>> = vec![
        Box::new(GlobalDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            GlobalDispatcherConfig::default(),
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats,
            datalake,
            metrics,
            NamespaceDispatcherConfig::default(),
        )),
    ];

    indexer::scheduler::run(&tasks, &*lock_service)
        .await
        .context("dispatch indexing failed")?;

    info!("dispatch indexing completed");
    Ok(())
}
