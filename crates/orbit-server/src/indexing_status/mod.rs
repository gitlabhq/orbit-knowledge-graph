mod checkpoints;
mod domains;
mod phase;
mod projects;
mod response;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::ArrowClickHouseClient;
use orbit_log_fields::warn;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::sources::{EtlScope, PipelineDescriptor};

pub use self::domains::{DomainStatus, EntityStatus};
pub use self::phase::Phase;
pub use self::projects::ProjectCoverage;
pub use self::response::{build_indexing_status_response, format_timestamp};

use self::checkpoints::PlanCheckpoints;
use self::phase::combine_phases;
use crate::active_schema::SchemaSnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeStatus {
    pub scope: TraversalPath,
    pub phase: Phase,
    pub sdlc_phase: Phase,
    pub code_phase: Option<Phase>,
    pub last_completed_at: Option<DateTime<Utc>>,
    pub projects: ProjectCoverage,
    pub domains: Vec<DomainStatus>,
}

pub struct IndexingStatusService {
    client: Arc<ArrowClickHouseClient>,
}

impl IndexingStatusService {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn read_scope_statuses(
        &self,
        schema: &SchemaSnapshot,
        scopes: &[TraversalPath],
    ) -> Vec<ScopeStatus> {
        if scopes.is_empty() {
            return Vec::new();
        }

        let mut roots: Vec<i64> = scopes
            .iter()
            .filter_map(TraversalPath::top_level_namespace_id)
            .collect();
        roots.sort_unstable();
        roots.dedup();

        let (checkpoints, coverage) = tokio::join!(
            checkpoints::read_plan_checkpoints(&self.client, schema, &roots),
            projects::read_project_coverage(&self.client, &schema.data_model, scopes),
        );
        let checkpoints = checkpoints
            .inspect_err(|error| {
                warn!(
                    safe(ERROR_TYPE) = "clickhouse_query",
                    expanded(ERROR_MESSAGE) = %error,
                    "Indexing status could not read plan checkpoints"
                )
            })
            .unwrap_or_default();
        let coverage = coverage
            .inspect_err(|error| {
                warn!(
                    safe(ERROR_TYPE) = "clickhouse_query",
                    expanded(ERROR_MESSAGE) = %error,
                    "Indexing status could not read project coverage"
                )
            })
            .ok();

        let now = Utc::now();
        let model = schema.data_model.model();
        let repository = projects::repository_entity(model)
            .map(|entity| model.graph().entity(entity).name.clone())
            .inspect_err(|error| {
                warn!(
                    safe(ERROR_TYPE) = "model",
                    expanded(ERROR_MESSAGE) = %error,
                    "Indexing status could not find the repository entity"
                )
            })
            .ok();
        let plans: Vec<PipelineDescriptor> = model
            .sources()
            .descriptors(model.graph())
            .into_iter()
            .filter(|plan| plan.scope == EtlScope::Namespaced)
            .collect();

        scopes
            .iter()
            .map(|scope| {
                build_scope_status(
                    schema,
                    &plans,
                    repository.as_deref(),
                    scope,
                    &checkpoints,
                    coverage.as_ref(),
                    now,
                )
            })
            .collect()
    }
}

fn build_scope_status(
    schema: &SchemaSnapshot,
    plans: &[PipelineDescriptor],
    repository: Option<&str>,
    scope: &TraversalPath,
    checkpoints: &HashMap<i64, PlanCheckpoints>,
    coverage: Option<&HashMap<String, ProjectCoverage>>,
    now: DateTime<Utc>,
) -> ScopeStatus {
    let root_checkpoints = scope
        .top_level_namespace_id()
        .and_then(|root| checkpoints.get(&root));

    let (source_unavailable, plans): (Vec<&PipelineDescriptor>, Vec<&PipelineDescriptor>) =
        plans.iter().partition(|plan| {
            root_checkpoints.is_some_and(|by_plan| by_plan.is_source_unavailable(&plan.name))
        });
    let plan_phases: Vec<(&PipelineDescriptor, Phase)> = plans
        .iter()
        .map(|plan| {
            let phase = root_checkpoints
                .map(|by_plan| by_plan.get_plan_phase(&plan.name, now))
                .unwrap_or(Phase::Unknown);
            (*plan, phase)
        })
        .collect();
    let last_completed_at =
        root_checkpoints.and_then(|by_plan| by_plan.get_last_completed_at(&plans));

    // The repository entity's plan writes the repository list, so the code total settles with it.
    let project_list_phase = combine_phases(
        plan_phases
            .iter()
            .filter(|(plan, _)| Some(plan.entity.as_str()) == repository)
            .map(|(_, phase)| *phase),
    )
    .unwrap_or(Phase::Unknown);
    let projects =
        coverage.map(|by_scope| by_scope.get(scope.as_str()).copied().unwrap_or_default());
    let code_phase = match projects {
        Some(projects) => projects.get_code_phase(project_list_phase),
        None => Some(Phase::Unknown),
    };

    let domains = domains::get_domain_statuses(
        &schema.data_model,
        &plan_phases,
        &source_unavailable,
        code_phase,
    );
    let phase = combine_phases(domains.iter().map(|domain| domain.phase)).unwrap_or(Phase::Unknown);
    let sdlc_phase =
        combine_phases(plan_phases.iter().map(|(_, phase)| *phase)).unwrap_or(Phase::Unknown);

    ScopeStatus {
        scope: scope.clone(),
        phase,
        sdlc_phase,
        code_phase,
        last_completed_at,
        projects: projects.unwrap_or_default(),
        domains,
    }
}
