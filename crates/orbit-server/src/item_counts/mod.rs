mod counts;
mod response;
mod visibility;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_log_fields::warn;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::SecurityContext;

use self::counts::ScopeCounts;
pub use self::response::build_item_counts_response;
use self::visibility::Visibility;

pub struct ScopeItemCounts {
    pub scope: TraversalPath,
    pub counts: HashMap<String, i64>,
}

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
    ) -> Vec<ScopeItemCounts> {
        let distinct_scopes: Vec<&TraversalPath> =
            scopes.iter().collect::<BTreeSet<_>>().into_iter().collect();
        let visibility = visibility::get_visibility(ontology, security_context, &distinct_scopes);
        let counts = self.read_counts(&distinct_scopes, &visibility).await;
        let counts_by_scope: HashMap<&TraversalPath, HashMap<String, i64>> = distinct_scopes
            .iter()
            .enumerate()
            .map(|(index, scope)| {
                (
                    *scope,
                    build_scope_counts(&visibility, &counts, index, scope),
                )
            })
            .collect();
        scopes
            .iter()
            .map(|scope| ScopeItemCounts {
                scope: scope.clone(),
                counts: counts_by_scope[scope].clone(),
            })
            .collect()
    }

    async fn read_counts(&self, scopes: &[&TraversalPath], visibility: &Visibility) -> ScopeCounts {
        if visibility.entities.is_empty() {
            return ScopeCounts::new();
        }
        counts::count_visible_entities(&self.client, scopes, visibility)
            .await
            .inspect_err(|error| {
                warn!(
                    expanded(ERROR_MESSAGE) = %error,
                    "Item counts could not be read"
                )
            })
            .unwrap_or_default()
    }
}

fn build_scope_counts(
    visibility: &Visibility,
    counts: &ScopeCounts,
    index: usize,
    scope: &TraversalPath,
) -> HashMap<String, i64> {
    let scope_counts = counts.get(scope.as_str());
    visibility
        .entities
        .iter()
        .filter(|entity| visibility.get_scopes(entity).contains(&index))
        .map(|entity| {
            let count = scope_counts
                .and_then(|by_entity| by_entity.get(&entity.name))
                .copied()
                .unwrap_or(0);
            (entity.name.clone(), count)
        })
        .collect()
}
