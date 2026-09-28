mod checkpoints;
mod domains;
mod phase;
mod projects;
mod response;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::ArrowClickHouseClient;
use ontology::EtlScope;
use ontology::pipelines::PipelineDescriptor;
use orbit_utils::traversal_path::TraversalPath;
use tracing::warn;

pub use self::domains::{DomainStatus, EntityStatus};
pub use self::phase::Phase;
pub use self::projects::ProjectCoverage;
pub use self::response::build_indexing_status_response;

use self::checkpoints::PlanCheckpoints;
use self::phase::combine_phases;
use self::projects::PROJECT_NODE;
use crate::active_schema::SchemaSnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeStatus {
    pub scope: TraversalPath,
    pub phase: Phase,
    pub sdlc_phase: Phase,
    pub code_phase: Option<Phase>,
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
            projects::read_project_coverage(&self.client, &schema.ontology, scopes),
        );
        let checkpoints = checkpoints
            .inspect_err(|error| warn!(%error, "Indexing status could not read plan checkpoints"))
            .ok();
        let coverage = coverage
            .inspect_err(|error| warn!(%error, "Indexing status could not read project coverage"))
            .ok();

        let now = Utc::now();
        let plans: Vec<PipelineDescriptor> = schema
            .ontology
            .pipeline_descriptors()
            .into_iter()
            .filter(|plan| plan.scope == EtlScope::Namespaced)
            .collect();

        scopes
            .iter()
            .map(|scope| {
                build_scope_status(
                    schema,
                    &plans,
                    scope,
                    checkpoints.as_ref(),
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
    scope: &TraversalPath,
    checkpoints: Option<&HashMap<i64, PlanCheckpoints>>,
    coverage: Option<&HashMap<String, ProjectCoverage>>,
    now: DateTime<Utc>,
) -> ScopeStatus {
    let root = scope.top_level_namespace_id();
    let plan_phases: Vec<(&PipelineDescriptor, Phase)> = plans
        .iter()
        .map(|plan| {
            (
                plan,
                get_root_plan_phase(checkpoints, root, &plan.name, now),
            )
        })
        .collect();

    // The Project plan writes the project list, so the code total is final only once it settles.
    let project_list_settled = plan_phases
        .iter()
        .filter(|(plan, _)| plan.entity == PROJECT_NODE)
        .all(|(_, phase)| phase.is_settled());
    let projects =
        coverage.map(|by_scope| by_scope.get(scope.as_str()).copied().unwrap_or_default());
    let code_phase = match projects {
        Some(projects) => projects.get_code_phase(project_list_settled),
        None => Some(Phase::Unknown),
    };

    let domains = domains::get_domain_statuses(&schema.ontology, &plan_phases, code_phase);
    let phase = combine_phases(domains.iter().map(|domain| domain.phase)).unwrap_or(Phase::Unknown);
    let sdlc_phase =
        combine_phases(plan_phases.iter().map(|(_, phase)| *phase)).unwrap_or(Phase::Unknown);

    ScopeStatus {
        scope: scope.clone(),
        phase,
        sdlc_phase,
        code_phase,
        projects: projects.unwrap_or_default(),
        domains,
    }
}

fn get_root_plan_phase(
    checkpoints: Option<&HashMap<i64, PlanCheckpoints>>,
    root: Option<i64>,
    plan: &str,
    now: DateTime<Utc>,
) -> Phase {
    let (Some(by_root), Some(root)) = (checkpoints, root) else {
        return Phase::Unknown;
    };
    let no_checkpoints = PlanCheckpoints::default();
    by_root
        .get(&root)
        .unwrap_or(&no_checkpoints)
        .get_plan_phase(plan, now)
}
