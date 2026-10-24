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
use tonic::Status;

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
    ) -> Result<Vec<ScopeItemCounts>, Status> {
        let plan = CountPlan::new(ontology, security_context, scopes);
        let counts = self.read_counts(&plan).await?;
        Ok(plan.answer(scopes, &counts))
    }

    pub async fn count_items_or_zeros(
        &self,
        ontology: &Ontology,
        security_context: &SecurityContext,
        scopes: &[TraversalPath],
    ) -> Vec<ScopeItemCounts> {
        let plan = CountPlan::new(ontology, security_context, scopes);
        plan.answer(scopes, &self.read_counts(&plan).await.unwrap_or_default())
    }

    async fn read_counts(&self, plan: &CountPlan<'_>) -> Result<ScopeCounts, Status> {
        if plan.visibility.entities.is_empty() {
            return Ok(ScopeCounts::new());
        }
        counts::count_visible_entities(&self.client, &plan.scopes, &plan.visibility)
            .await
            .map_err(|error| {
                warn!(
                    safe(ERROR_TYPE) = "clickhouse_query",
                    expanded(ERROR_MESSAGE) = %error,
                    "Item counts could not be read"
                );
                Status::internal("item counts could not be read")
            })
    }
}

struct CountPlan<'a> {
    scopes: Vec<&'a TraversalPath>,
    visibility: Visibility,
}

impl<'a> CountPlan<'a> {
    fn new(
        ontology: &Ontology,
        security_context: &SecurityContext,
        scopes: &'a [TraversalPath],
    ) -> Self {
        let scopes: Vec<&TraversalPath> =
            scopes.iter().collect::<BTreeSet<_>>().into_iter().collect();
        let visibility = visibility::get_visibility(ontology, security_context, &scopes);
        Self { scopes, visibility }
    }

    fn answer(&self, requested: &[TraversalPath], counts: &ScopeCounts) -> Vec<ScopeItemCounts> {
        requested
            .iter()
            .map(|scope| ScopeItemCounts {
                scope: scope.clone(),
                counts: self.build_scope_counts(counts, scope),
            })
            .collect()
    }

    fn build_scope_counts(
        &self,
        counts: &ScopeCounts,
        scope: &TraversalPath,
    ) -> HashMap<String, i64> {
        let index = self.scopes.partition_point(|distinct| *distinct < scope);
        let scope_counts = counts.get(scope.as_str());
        self.visibility
            .entities_in_scope(index)
            .into_iter()
            .map(|entity| {
                let count = scope_counts
                    .and_then(|by_entity| by_entity.get(&entity.name))
                    .copied()
                    .unwrap_or(0);
                (entity.name.clone(), count)
            })
            .collect()
    }
}
