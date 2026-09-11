use serde::Serialize;
use toon_format::{EncodeOptions, encode};

use crate::proto::{IndexingState, StructuredGraphStatus};

#[derive(Serialize)]
struct StatusToon<'a> {
    indexing: Option<IndexingToon<'a>>,
    projects: Option<ProjectsToon>,
    domains: Vec<DomainToon<'a>>,
}

#[derive(Serialize)]
struct IndexingToon<'a> {
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_completed_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_progress_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_pipelines: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_pipelines: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_projects: Option<u64>,
}

#[derive(Serialize)]
struct ProjectsToon {
    indexed: i64,
    total_known: i64,
}

#[derive(Serialize)]
struct DomainToon<'a> {
    name: &'a str,
    items: Vec<ItemToon<'a>>,
}

#[derive(Serialize)]
struct ItemToon<'a> {
    name: &'a str,
    count: i64,
}

pub fn format_status_as_toon(status: &StructuredGraphStatus) -> String {
    let output = StatusToon {
        indexing: status.indexing.as_ref().map(|indexing| IndexingToon {
            state: indexing_state_name(indexing.state),
            last_completed_at: indexing.last_completed_at.as_deref(),
            last_progress_at: indexing.last_progress_at.as_deref(),
            completed_pipelines: indexing.completed_pipelines,
            total_pipelines: indexing.total_pipelines,
            completed_projects: indexing.completed_projects,
        }),
        projects: status.projects.as_ref().map(|projects| ProjectsToon {
            indexed: projects.indexed,
            total_known: projects.total_known,
        }),
        domains: status
            .domains
            .iter()
            .map(|domain| DomainToon {
                name: &domain.name,
                items: domain
                    .items
                    .iter()
                    .map(|item| ItemToon {
                        name: &item.name,
                        count: item.count,
                    })
                    .collect(),
            })
            .collect(),
    };
    encode(&output, &EncodeOptions::default()).unwrap_or_else(|error| {
        tracing::warn!(%error, "failed to encode graph status");
        "indexing:\n  state: unknown".into()
    })
}

fn indexing_state_name(state: i32) -> String {
    IndexingState::try_from(state)
        .unwrap_or(IndexingState::Unknown)
        .as_str_name()
        .trim_start_matches("INDEXING_STATE_")
        .to_ascii_lowercase()
}
