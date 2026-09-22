use std::collections::HashMap;
use std::sync::LazyLock;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use indexer::checkpoint::namespace_position_key;
use ontology::pipelines::PipelineDescriptor;
use ontology::{DomainInfo, NodeEntity, Ontology};
use orbit_migrations::completion::FINISHED_FIRST_PASS;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_migrations::version::prefixed_table_name;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;
use tracing::warn;

use super::execute_query;
use crate::active_schema::SchemaSnapshot;
use crate::proto::{IndexingPhase, IndexingState};

static FIRST_PASS_FLAGS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT splitByChar('.', key)[3] AS plan, toBool({FINISHED_FIRST_PASS}) AS finished \
         FROM {{table:Identifier}} FINAL \
         WHERE startsWith(key, {{prefix:String}}) \
           AND length(splitByChar('.', key)) = 3 \
           AND _deleted = false"
    )
});

pub struct FirstSync {
    pub phase: IndexingPhase,
    plans: Vec<PipelineDescriptor>,
    first_pass_finished: HashMap<String, bool>,
}

pub async fn read_first_sync(
    client: &ArrowClickHouseClient,
    schema: &SchemaSnapshot,
    traversal_path: &TraversalPath,
) -> FirstSync {
    let plans = schema.ontology.namespaced_pipeline_descriptors();
    let Some(root) = traversal_path.top_level_namespace_id() else {
        return FirstSync::unknown(plans);
    };

    let table = prefixed_table_name(CHECKPOINT_TABLE, schema.migration_version);
    let first_pass_finished = match read_first_pass_flags(client, &table, root).await {
        Ok(flags) => flags,
        Err(error) => {
            warn!(%traversal_path, %error, "Graph status branch failed");
            return FirstSync::unknown(plans);
        }
    };

    let phase = phase_from_first_pass_flags(plans.iter(), &first_pass_finished)
        .unwrap_or(IndexingPhase::Ready);
    FirstSync {
        phase,
        plans,
        first_pass_finished,
    }
}

impl FirstSync {
    pub fn phase_of_plans_feeding(
        &self,
        ontology: &Ontology,
        domain: &DomainInfo,
    ) -> Option<IndexingPhase> {
        let plans = self
            .plans
            .iter()
            .filter(|plan| plan_feeds_domain(ontology, plan, domain));
        self.phase_of_plans(plans)
    }

    pub fn node_state(&self, node: &NodeEntity) -> Option<IndexingState> {
        let plans = self.plans.iter().filter(|plan| plan.entity == node.name);
        self.phase_of_plans(plans).map(indexing_state_for)
    }

    fn phase_of_plans<'a>(
        &self,
        plans: impl Iterator<Item = &'a PipelineDescriptor>,
    ) -> Option<IndexingPhase> {
        phase_from_first_pass_flags(plans, &self.first_pass_finished).map(|phase| {
            if self.phase == IndexingPhase::Unknown {
                IndexingPhase::Unknown
            } else {
                phase
            }
        })
    }

    fn unknown(plans: Vec<PipelineDescriptor>) -> Self {
        Self {
            phase: IndexingPhase::Unknown,
            plans,
            first_pass_finished: HashMap::new(),
        }
    }
}

pub fn indexing_state_for(phase: IndexingPhase) -> IndexingState {
    match phase {
        IndexingPhase::Ready => IndexingState::Indexed,
        IndexingPhase::Syncing => IndexingState::Backfilling,
        IndexingPhase::NotStarted => IndexingState::NotIndexed,
        IndexingPhase::Unknown => IndexingState::Unknown,
    }
}

async fn read_first_pass_flags(
    client: &ArrowClickHouseClient,
    table: &str,
    root: i64,
) -> Result<HashMap<String, bool>, Status> {
    let prefix = format!("{}.", namespace_position_key(root));
    let params = [("table", table), ("prefix", prefix.as_str())];
    let batches = execute_query(client, &FIRST_PASS_FLAGS_SQL, &params, "first pass flags").await?;

    let plans = String::extract_column(&batches, 0).map_err(|e| Status::internal(e.to_string()))?;
    let finished =
        bool::extract_column(&batches, 1).map_err(|e| Status::internal(e.to_string()))?;
    Ok(plans.into_iter().zip(finished).collect())
}

fn phase_from_first_pass_flags<'a>(
    plans: impl Iterator<Item = &'a PipelineDescriptor>,
    first_pass_finished: &HashMap<String, bool>,
) -> Option<IndexingPhase> {
    let flags: Vec<Option<bool>> = plans
        .map(|plan| first_pass_finished.get(&plan.name).copied())
        .collect();
    if flags.is_empty() {
        return None;
    }

    let phase = if flags.iter().all(|flag| *flag == Some(true)) {
        IndexingPhase::Ready
    } else if flags.iter().all(Option::is_none) {
        IndexingPhase::NotStarted
    } else {
        IndexingPhase::Syncing
    };
    Some(phase)
}

fn plan_feeds_domain(ontology: &Ontology, plan: &PipelineDescriptor, domain: &DomainInfo) -> bool {
    let in_domain = |kind: &str| domain.node_names.iter().any(|name| name == kind);
    if in_domain(&plan.entity) {
        return true;
    }
    if ontology.get_node(&plan.entity).is_some() {
        return false;
    }
    plan.reindex_targets
        .iter()
        .filter_map(|kind| ontology.get_edge(kind))
        .flatten()
        .any(|edge| in_domain(&edge.source_kind) || in_domain(&edge.target_kind))
}
