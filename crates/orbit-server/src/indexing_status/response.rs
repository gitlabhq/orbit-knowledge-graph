use super::{DomainStatus, Phase, ProjectCoverage, ScopeStatus};
use crate::proto::{
    DomainIndexingStatus, GetIndexingStatusResponse, IndexingPhase, NamespaceIndexingStatus,
    ProjectsStatus,
};

pub fn build_indexing_status_response(statuses: &[ScopeStatus]) -> GetIndexingStatusResponse {
    GetIndexingStatusResponse {
        statuses: statuses.iter().map(build_namespace_status).collect(),
    }
}

fn build_namespace_status(status: &ScopeStatus) -> NamespaceIndexingStatus {
    NamespaceIndexingStatus {
        traversal_path: status.scope.to_string(),
        phase: map_phase(status.phase).into(),
        domains: status
            .domains
            .iter()
            .map(|domain| build_domain_status(domain, status))
            .collect(),
    }
}

fn build_domain_status(domain: &DomainStatus, status: &ScopeStatus) -> DomainIndexingStatus {
    DomainIndexingStatus {
        name: domain.name.clone(),
        phase: map_phase(domain.phase).into(),
        projects: domain
            .has_code_nodes
            .then_some(ProjectsStatus::from(status.projects)),
    }
}

fn map_phase(phase: Phase) -> IndexingPhase {
    match phase {
        Phase::Unknown => IndexingPhase::Unknown,
        Phase::NotStarted => IndexingPhase::NotStarted,
        Phase::Syncing => IndexingPhase::Syncing,
        Phase::Ready => IndexingPhase::Ready,
        Phase::Error => IndexingPhase::Error,
    }
}

impl From<ProjectCoverage> for ProjectsStatus {
    fn from(coverage: ProjectCoverage) -> Self {
        Self {
            indexed: coverage.indexed,
            total_known: coverage.total_known,
            gaps: coverage.gaps,
        }
    }
}
