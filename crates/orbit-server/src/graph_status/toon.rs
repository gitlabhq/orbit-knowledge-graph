use serde::Serialize;
use toon_format::{EncodeOptions, encode};

use crate::proto::{BackfillState, StructuredGraphStatus};

#[derive(Serialize)]
struct StatusToon<'a> {
    backfill: Option<BackfillToon<'a>>,
    projects: Option<ProjectsToon>,
    domains: Vec<DomainToon<'a>>,
}

#[derive(Serialize)]
struct BackfillToon<'a> {
    state: String,
    last_progress_at: Option<&'a str>,
    sdlc: Option<CountsToon>,
    code: Option<CountsToon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

#[derive(Serialize)]
struct CountsToon {
    completed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
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
    let counts = |counts: &crate::proto::BackfillCounts| CountsToon {
        completed: counts.completed,
        total: counts.total,
    };
    let output = StatusToon {
        backfill: status.backfill.as_ref().map(|backfill| BackfillToon {
            state: BackfillState::try_from(backfill.state)
                .unwrap_or(BackfillState::Unknown)
                .as_str_name()
                .trim_start_matches("BACKFILL_STATE_")
                .to_ascii_lowercase(),
            last_progress_at: backfill.last_progress_at.as_deref(),
            sdlc: backfill.sdlc.as_ref().map(counts),
            code: backfill.code.as_ref().map(counts),
            error: backfill.error.as_deref(),
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
        "backfill:\n  state: unknown".into()
    })
}
