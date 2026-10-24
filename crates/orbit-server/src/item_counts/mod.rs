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
        let counts = self.read_counts(&plan).await;
        if !counts.is_empty() && counts.values().all(Result::is_err) {
            return Err(Status::internal("item counts could not be read"));
        }
        Ok(plan.answer(scopes, &counts))
    }

    pub async fn count_items_or_zeros(
        &self,
        ontology: &Ontology,
        security_context: &SecurityContext,
        scopes: &[TraversalPath],
    ) -> Vec<ScopeItemCounts> {
        let plan = CountPlan::new(ontology, security_context, scopes);
        let counts = self
            .read_counts(&plan)
            .await
            .into_iter()
            .map(|(scope, counts)| (scope, Ok(counts.unwrap_or_default())))
            .collect();
        plan.answer(scopes, &counts)
    }

    async fn read_counts(&self, plan: &CountPlan<'_>) -> ScopeCounts {
        if plan.visibility.entities.is_empty() {
            return ScopeCounts::new();
        }
        let counts =
            counts::count_visible_entities(&self.client, &plan.scopes, &plan.visibility).await;
        for error in counts.values().filter_map(|counts| counts.as_ref().err()) {
            warn!(
                safe(ERROR_TYPE) = "clickhouse_query",
                expanded(ERROR_MESSAGE) = %error,
                "Item counts could not be read"
            );
        }
        counts
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
            .filter_map(|scope| {
                let scope_counts = counts
                    .get(scope.as_str())
                    .map(Result::as_ref)
                    .transpose()
                    .ok()?;
                Some(ScopeItemCounts {
                    scope: scope.clone(),
                    counts: self.build_scope_counts(scope_counts, scope),
                })
            })
            .collect()
    }

    fn build_scope_counts(
        &self,
        scope_counts: Option<&HashMap<String, i64>>,
        scope: &TraversalPath,
    ) -> HashMap<String, i64> {
        let index = self.scopes.partition_point(|distinct| *distinct < scope);
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
