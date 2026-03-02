use anyhow::{Context, Result};
use indexer::dispatcher::Dispatcher;
use indexer::modules::sdlc::dispatch::{
    DispatchMetrics, GlobalDispatcher, GlobalDispatcherConfig, NamespaceDispatcher,
    NamespaceDispatcherConfig,
};
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

    let services = indexer::dispatcher::connect(&nats_config)
        .await
        .context("dispatcher connect failed")?;

    let datalake = datalake_config.build_client();
    let metrics = DispatchMetrics::new();
    let lock_service = services.lock_service.clone();
    let dispatchers: Vec<Box<dyn Dispatcher>> = vec![
        Box::new(GlobalDispatcher::new(
            services.nats.clone(),
            services.lock_service.clone(),
            metrics.clone(),
            GlobalDispatcherConfig::default(),
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats,
            services.lock_service,
            datalake,
            metrics,
            NamespaceDispatcherConfig::default(),
        )),
    ];

    indexer::dispatcher::run(&dispatchers, &*lock_service)
        .await
        .context("dispatch indexing failed")?;

    info!("dispatch indexing completed");
    Ok(())
}
