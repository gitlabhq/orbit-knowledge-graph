use std::sync::Arc;

use crate::IndexerConfig;
use crate::analytics::IndexingAnalytics;
use crate::clickhouse::{ClickHouseClients, ClickHouseWriter};
use crate::handler::HandlerRegistry;

pub mod code;
pub mod namespace_deletion;
pub mod sdlc;

pub struct ModuleDeps<'a> {
    pub registry: &'a HandlerRegistry,
    pub config: &'a IndexerConfig,
    pub clients: &'a ClickHouseClients,
    pub ontology: &'a ontology::Ontology,
    pub writer: Arc<ClickHouseWriter>,
    pub analytics: IndexingAnalytics,
}
