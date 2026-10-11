use std::collections::HashMap;

use crate::indexing_status::{DomainStatus, Phase, ScopeStatus, format_timestamp};
use crate::proto::{
    GraphStatusDomain, GraphStatusItem, IndexingState, IndexingStatus, ProjectsStatus,
    StructuredGraphStatus,
};

pub fn build_structured_status(
    status: &ScopeStatus,
    counts: &HashMap<String, i64>,
) -> StructuredGraphStatus {
    let last_completed_at = status.last_completed_at.map(format_timestamp);
    StructuredGraphStatus {
        projects: Some(ProjectsStatus::from(status.projects)),
        domains: status
            .domains
            .iter()
            .filter_map(|domain| build_visible_domain(domain, counts))
            .collect(),
        indexing: Some(build_indexing_status(
            status.phase,
            last_completed_at.clone(),
        )),
        sdlc_indexing: Some(build_indexing_status(status.sdlc_phase, last_completed_at)),
        code_indexing: status
            .code_phase
            .map(|phase| build_indexing_status(phase, None)),
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

fn build_indexing_status(phase: Phase, last_completed_at: Option<String>) -> IndexingStatus {
    IndexingStatus {
        state: map_phase_to_indexing_state(phase).into(),
        last_completed_at,
        ..Default::default()
    }
}

fn map_phase_to_indexing_state(phase: Phase) -> IndexingState {
    match phase {
        Phase::Ready => IndexingState::Indexed,
        Phase::Syncing => IndexingState::Backfilling,
        Phase::NotStarted => IndexingState::NotIndexed,
        Phase::Unknown => IndexingState::Unknown,
        Phase::Error => IndexingState::Error,
    }
}
