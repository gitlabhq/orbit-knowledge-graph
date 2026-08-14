use serde::Serialize;
use toon_format::{EncodeOptions, encode};
use tracing::warn;

use crate::proto::{IndexingState, IndexingStatus, StructuredGraphStatus};

#[derive(Serialize)]
struct StatusToon {
    #[serde(skip_serializing_if = "Option::is_none")]
    projects: Option<ProjectsToon>,
    domains: Vec<DomainToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexing: Option<IndexingToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdlc_indexing: Option<IndexingToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code_indexing: Option<IndexingToon>,
}

#[derive(Serialize)]
struct ProjectsToon {
    indexed: i64,
    total_known: i64,
}

#[derive(Serialize)]
struct DomainToon {
    name: String,
    items: Vec<ItemToon>,
}

#[derive(Serialize)]
struct ItemToon {
    name: String,
    count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
}

#[derive(Serialize)]
struct IndexingToon {
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_rows_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_rows_written: Option<u64>,
}

fn indexing_toon(status: &IndexingStatus) -> IndexingToon {
    IndexingToon {
        state: indexing_state_name(status.state),
        last_started_at: status.last_started_at.clone(),
        last_completed_at: status.last_completed_at.clone(),
        last_duration_ms: status.last_duration_ms,
        last_error: status.last_error.clone(),
        last_rows_read: status.last_rows_read,
        last_rows_written: status.last_rows_written,
    }
}

fn indexing_state_name(val: i32) -> String {
    match IndexingState::try_from(val) {
        Ok(IndexingState::NotIndexed) => "not_indexed".to_string(),
        Ok(IndexingState::Backfilling) => "backfilling".to_string(),
        Ok(IndexingState::Indexed) => "indexed".to_string(),
        Ok(IndexingState::Error) => "error".to_string(),
        Ok(IndexingState::Indexing) => "indexing".to_string(),
        _ => "unknown".to_string(),
    }
}

pub fn format_status_as_toon(status: &StructuredGraphStatus) -> String {
    let toon = StatusToon {
        projects: status.projects.as_ref().map(|p| ProjectsToon {
            indexed: p.indexed,
            total_known: p.total_known,
        }),
        domains: status
            .domains
            .iter()
            .map(|d| DomainToon {
                name: d.name.clone(),
                items: d
                    .items
                    .iter()
                    .map(|i| ItemToon {
                        name: i.name.clone(),
                        count: i.count,
                        state: i.state.map(indexing_state_name),
                    })
                    .collect(),
            })
            .collect(),
        indexing: status.indexing.as_ref().map(indexing_toon),
        sdlc_indexing: status.sdlc_indexing.as_ref().map(indexing_toon),
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
