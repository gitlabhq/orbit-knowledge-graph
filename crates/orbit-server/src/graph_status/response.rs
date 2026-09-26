use std::collections::HashMap;

use crate::indexing_status::{DomainStatus, Phase, ScopeStatus};
use crate::proto::{
    GraphStatusDomain, GraphStatusItem, IndexingState, IndexingStatus, ProjectsStatus,
    StructuredGraphStatus,
};

pub fn structured_status(
    status: &ScopeStatus,
    counts: &HashMap<String, i64>,
) -> StructuredGraphStatus {
    StructuredGraphStatus {
        projects: Some(ProjectsStatus {
            indexed: status.projects.indexed,
            total_known: status.projects.total_known,
        }),
        domains: status
            .domains
            .iter()
            .filter_map(|domain| visible_domain(domain, counts))
            .collect(),
        indexing: Some(status_in_phase(status.phase)),
        sdlc_indexing: Some(status_in_phase(status.sdlc_phase)),
        code_indexing: status.code_phase.map(status_in_phase),
    }
}

fn visible_domain(
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
                state: entity.phase.map(|phase| indexing_state(phase) as i32),
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

fn status_in_phase(phase: Phase) -> IndexingStatus {
    IndexingStatus {
        state: indexing_state(phase).into(),
        ..Default::default()
    }
}

fn indexing_state(phase: Phase) -> IndexingState {
    match phase {
        Phase::Ready => IndexingState::Indexed,
        Phase::Syncing => IndexingState::Backfilling,
        Phase::NotStarted => IndexingState::NotIndexed,
        Phase::Unknown => IndexingState::Unknown,
    }
}
