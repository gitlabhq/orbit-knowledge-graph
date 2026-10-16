//! `--mode reindex`: empties every graph table of the embedded schema version, so
//! the dispatcher rebuilds the graph from the datalake and promotes it once complete.

use indexer::DispatcherConfig;
use orbit_server_config::AppConfig;

pub async fn run(config: &AppConfig, ontology: &ontology::Ontology) -> anyhow::Result<()> {
    config.schema.validate()?;
    indexer::run_reindex_reset(&DispatcherConfig::from(config), ontology).await?;
    Ok(())
}
