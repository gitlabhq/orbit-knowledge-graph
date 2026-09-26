mod counts;
mod visibility;

use std::collections::HashMap;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::SecurityContext;
use tracing::warn;

pub struct ItemCountService {
    client: Arc<ArrowClickHouseClient>,
}

impl ItemCountService {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn count(
        &self,
        ontology: &Ontology,
        security_context: &SecurityContext,
        scopes: &[TraversalPath],
    ) -> HashMap<String, i64> {
        let entities = visibility::visible_entities(ontology, security_context, scopes);
        if entities.is_empty() {
            return HashMap::new();
        }

        let counts = counts::count_entities(&self.client, &entities)
            .await
            .inspect_err(|error| warn!(%error, "Item counts could not be read"))
            .unwrap_or_default();
        entities
            .into_iter()
            .map(|entity| {
                let count = counts.get(&entity.name).copied().unwrap_or(0);
                (entity.name, count)
            })
            .collect()
    }
}
