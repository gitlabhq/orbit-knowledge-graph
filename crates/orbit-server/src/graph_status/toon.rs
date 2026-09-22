use serde::Serialize;
use toon_format::{EncodeOptions, encode};
use tracing::warn;

use crate::proto::{IndexingPhase, IndexingState, IndexingStatus, StructuredGraphStatus};

#[derive(Serialize)]
struct StatusToon {
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<ProgressToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    projects: Option<ProjectsToon>,
    domains: Vec<DomainToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexing: Option<IndexingToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code_indexing: Option<IndexingToon>,
}

#[derive(Serialize)]
struct ProgressToon {
    phase: String,
}

#[derive(Serialize)]
struct ProjectsToon {
    indexed: i64,
    total_known: i64,
}

#[derive(Serialize)]
struct DomainToon {
    name: String,
    phase: String,
    items: Vec<ItemToon>,
}

#[derive(Serialize)]
struct ItemToon {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}

#[derive(Serialize)]
struct IndexingToon {
    state: String,
}

pub fn format_status_as_toon(status: &StructuredGraphStatus) -> String {
    let toon = StatusToon {
        progress: status.progress.as_ref().map(|p| ProgressToon {
            phase: indexing_phase_name(p.phase),
        }),
        projects: status.projects.as_ref().map(|p| ProjectsToon {
            indexed: p.indexed,
            total_known: p.total_known,
        }),
        domains: status
            .domains
            .iter()
            .map(|d| DomainToon {
                name: d.name.clone(),
                phase: indexing_phase_name(d.phase),
                items: d
                    .items
                    .iter()
                    .map(|i| ItemToon {
                        name: i.name.clone(),
                        state: i.state.map(indexing_state_name),
                    })
                    .collect(),
            })
            .collect(),
        indexing: status.indexing.as_ref().map(indexing_toon),
        code_indexing: status.code_indexing.as_ref().map(indexing_toon),
    };

    encode(&toon, &EncodeOptions::default()).unwrap_or_else(|e| {
        warn!(error = %e, "Failed to encode graph status as TOON, falling back");
        format!(
            "projects:{}/{}",
            status.projects.as_ref().map_or(0, |p| p.indexed),
            status.projects.as_ref().map_or(0, |p| p.total_known)
        )
    })
}

fn indexing_toon(status: &IndexingStatus) -> IndexingToon {
    IndexingToon {
        state: indexing_state_name(status.state),
    }
}

fn indexing_phase_name(val: i32) -> String {
    let phase = IndexingPhase::try_from(val).unwrap_or(IndexingPhase::Unknown);
    lowercase_variant(phase.as_str_name(), "INDEXING_PHASE_")
}

fn indexing_state_name(val: i32) -> String {
    let state = IndexingState::try_from(val).unwrap_or(IndexingState::Unknown);
    lowercase_variant(state.as_str_name(), "INDEXING_STATE_")
}

fn lowercase_variant(proto_name: &str, prefix: &str) -> String {
    proto_name.trim_start_matches(prefix).to_lowercase()
}
