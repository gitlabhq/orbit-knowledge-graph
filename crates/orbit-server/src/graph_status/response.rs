use std::collections::HashMap;

use crate::indexing_status::{DomainStatus, Phase, ScopeStatus};
use crate::proto::{
    GraphStatusDomain, GraphStatusItem, IndexingState, IndexingStatus, ProjectsStatus,
    StructuredGraphStatus,
};

pub fn build_structured_status(
    status: &ScopeStatus,
    counts: &HashMap<String, i64>,
) -> StructuredGraphStatus {
    StructuredGraphStatus {
        projects: Some(ProjectsStatus::from(status.projects)),
        domains: status
            .domains
            .iter()
            .filter_map(|domain| build_visible_domain(domain, counts))
            .collect(),
        indexing: Some(build_indexing_status(status.phase)),
        sdlc_indexing: Some(build_indexing_status(status.sdlc_phase)),
        code_indexing: status.code_phase.map(build_indexing_status),
    }
}

fn build_visible_domain(
    domain: &DomainStatus,
    counts: &HashMap<String, i64>,
) -> Option<GraphStatusDomain> {
    let items: Vec<GraphStatusItem> = domain
        .entities
        .iter()
        .filter_map(|entity| {
            let count = *counts.get(&entity.name)?;
            Some(GraphStatusItem {
                name: entity.name.clone(),
                count,
                state: entity
                    .phase
                    .map(|phase| map_phase_to_indexing_state(phase) as i32),
            })
        })
        .collect();
    if items.is_empty() {
        return None;
    }

    Some(GraphStatusDomain {
        name: domain.name.clone(),
        items,
    })
}

fn build_indexing_status(phase: Phase) -> IndexingStatus {
    IndexingStatus {
        state: map_phase_to_indexing_state(phase).into(),
        ..Default::default()
    }
}

fn map_phase_to_indexing_state(phase: Phase) -> IndexingState {
    match phase {
        Phase::Ready => IndexingState::Indexed,
        Phase::Syncing => IndexingState::Backfilling,
        Phase::NotStarted => IndexingState::NotIndexed,
        Phase::Unknown => IndexingState::Unknown,
    }
}
