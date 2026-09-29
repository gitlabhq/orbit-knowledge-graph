mod counts;
mod response;
mod visibility;

use std::collections::HashMap;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::SecurityContext;
use tracing::warn;

pub use self::response::build_item_counts_response;
use self::visibility::VisibleEntity;

pub struct ItemCountService {
    client: Arc<ArrowClickHouseClient>,
}

pub struct ScopeItemCounts {
    pub scope: TraversalPath,
    pub counts: HashMap<String, i64>,
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
    ) -> Vec<ScopeItemCounts> {
        let entities = visibility::get_visible_entities(ontology, security_context, scopes);
        let counts_by_scope = if entities.is_empty() {
            HashMap::new()
        } else {
            counts::count_visible_entities(&self.client, &entities)
                .await
                .inspect_err(|error| warn!(%error, "Item counts could not be read"))
                .unwrap_or_default()
        };

        scopes
            .iter()
            .map(|scope| collect_scope_counts(scope, &entities, &counts_by_scope))
            .collect()
    }
}

fn collect_scope_counts(
    scope: &TraversalPath,
    entities: &[VisibleEntity],
    counts_by_scope: &HashMap<String, HashMap<String, i64>>,
) -> ScopeItemCounts {
    let found = counts_by_scope.get(scope.as_str());
    let counts = entities
        .iter()
        .filter(|entity| {
            entity
                .scopes
                .iter()
                .any(|visible| visible == scope.as_str())
        })
        .map(|entity| {
            let count = found
                .and_then(|counts| counts.get(&entity.name))
                .copied()
                .unwrap_or(0);
            (entity.name.clone(), count)
        })
        .collect();
    ScopeItemCounts {
        scope: scope.clone(),
        counts,
    }
}
