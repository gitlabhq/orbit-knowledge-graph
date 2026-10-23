mod counts;
mod response;
mod visibility;

use std::collections::HashMap;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_log_fields::warn;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::SecurityContext;

use self::counts::ScopeCounts;
pub use self::response::build_item_counts_response;
use self::visibility::VisibleEntity;

pub struct ItemCountService {
    client: Arc<ArrowClickHouseClient>,
}

impl ItemCountService {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn count_items(
        &self,
        ontology: &Ontology,
        security_context: &SecurityContext,
        scopes: &[TraversalPath],
    ) -> Vec<HashMap<String, i64>> {
        let entities = visibility::get_visible_entities(ontology, security_context, scopes);
        let counts = if entities.is_empty() {
            ScopeCounts::new()
        } else {
            counts::count_visible_entities(&self.client, &entities)
                .await
                .inspect_err(|error| {
                    warn!(
                        safe(ERROR_TYPE) = "clickhouse_query",
                        expanded(ERROR_MESSAGE) = %error,
                        "Item counts could not be read"
                    )
                })
                .unwrap_or_default()
        };
        scopes
            .iter()
            .map(|scope| build_scope_counts(&entities, &counts, scope.as_str()))
            .collect()
    }
}

fn build_scope_counts(
    entities: &[VisibleEntity],
    counts: &ScopeCounts,
    scope: &str,
) -> HashMap<String, i64> {
    let scope_counts = counts.get(scope);
    entities
        .iter()
        .filter(|entity| entity.scopes.iter().any(|visible| visible == scope))
        .map(|entity| {
            let count = scope_counts
                .and_then(|by_entity| by_entity.get(&entity.name))
                .copied()
                .unwrap_or(0);
            (entity.name.clone(), count)
        })
        .collect()
}
