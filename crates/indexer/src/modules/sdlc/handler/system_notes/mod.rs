//! System-notes edge materialization.
//!
//! Custom Rust handler implementing ADR 013. The current ontology filters
//! all system notes out at ingestion (`note.yaml: where: "system = false"`),
//! losing every cross-reference, lifecycle, and commit relationship Rails
//! encodes there. This module materialises those relationships as
//! first-class `gl_edge` rows.
//!
//! ## Why a custom Rust handler instead of ontology-driven ETL
//!
//! Per ADR 013, the standalone edge ETL YAML (`config/ontology/edges/*.yaml`)
//! has no `WHERE` clause in its schema (`edgeEtlConfig` in
//! `config/schemas/ontology.schema.json`), and the natural shape here —
//! one source table (`siphon_notes ⋈ siphon_system_note_metadata`)
//! dispatching into many edge variants whose target type depends on body
//! parsing — does not fit the ontology-first pattern. The edge **kinds**
//! still come from YAML (`config/ontology/edges/{mentions,reopened}.yaml`);
//! only the ETL logic is Rust.
//!
//! The handler is **not yet wired into the NATS engine** in this draft:
//! the production registration plugs into the `EntityPipeline` extension
//! point introduced by ADR 014 (`!1341`), which is scaffolded but not yet
//! on `main`. The pipeline's *internal* code (parser, resolver, edge
//! writer) is the production target shape; registration glue is the only
//! piece deferred.
//!
//! Custom-handler precedent in the existing codebase:
//! `crates/indexer/src/modules/namespace_deletion/`.
//!
//! The module's public items are intentionally `dead_code`-allowed at the
//! root: until the `EntityPipeline` registration glue lands on `main`, the
//! handler is exercised only by its own unit tests, and clippy `-D warnings`
//! would otherwise reject the unused-API surface.

#![allow(dead_code)]

pub(crate) mod emit;
pub(crate) mod extract;
pub(crate) mod parse;
pub(crate) mod resolve;
pub(crate) mod vendored;

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use emit::{EmittedEdge, NoteRow, NoteableKind, build_edges};
use parse::{Action, Reference, extract as parse_body};
use resolve::{ResolutionPlan, ResolvedTarget};

/// Raw row pulled from the `extract.rs` SQL. The handler parses, resolves,
/// and emits edges from these rows in batches.
#[derive(Debug, Clone)]
pub(crate) struct ExtractedNote {
    pub id: i64,
    pub note: String,
    pub noteable_id: i64,
    pub noteable_type: String,
    pub author_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub traversal_path: String,
    pub action: String,
    /// `siphon_system_note_metadata.commit_count`. Populated only for the
    /// `commit` action (Rails leaves it `NULL` for all others). The
    /// production handler compares this against the parser's SHA count to
    /// catch Rails-template-regression drift on the `<li>SHA - title</li>`
    /// commit-list body shape; carrying the column on the row keeps the
    /// SELECT in `extract.rs` stable when that assertion lands.
    pub commit_count: Option<i32>,
}

/// Best-effort default-project lookup keyed on `(noteable_type, noteable_id)`.
/// In production this comes from a same-batch ClickHouse lookup against the
/// noteable tables (`merge_requests`, `issues`, `work_items`) — the
/// handler resolves the source note's noteable to its owning project's
/// path, which becomes the default for unqualified GFM references on that
/// row (`!N`, `#N`, short SHAs without a project prefix).
pub(crate) type DefaultProjectLookup = HashMap<(String, i64), String>;

/// End-to-end pass over a batch of extracted notes: parse bodies, take a
/// resolver closure to look up targets, and return the resulting edge
/// batch.
///
/// Splitting the ClickHouse round-trip out as a closure keeps this function
/// pure: the unit tests exercise the full pipeline with a stub resolver,
/// and the production handler passes a closure that fans the
/// [`ResolutionPlan`] returned by [`plan_for_batch`] out to the actual
/// queries (see `resolve::ROUTES_SQL`, `resolve::MERGE_REQUESTS_SQL`,
/// `resolve::WORK_ITEMS_SQL`). The closure captures the
/// `DefaultProjectLookup` (or whatever shape the resolver needs) directly;
/// only [`plan_for_batch`] takes the lookup as a parameter because the plan
/// shape needs the default project at *planning* time (before the closure
/// runs).
pub(crate) fn process_batch<R>(notes: &[ExtractedNote], mut resolve: R) -> Vec<EmittedEdge>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut rows = Vec::with_capacity(notes.len());
    for n in notes {
        let Some(action) = Action::parse(&n.action) else {
            // Unknown action: log + drop. WARN-level (not debug) so a
            // staging deployment surfaces drift against Rails immediately
            // without a log-level config push. The production handler emits
            // `gkg.indexer.sdlc.system_notes.unknown_action_total{action}`
            // here (ADR 013 step 8); the bounded cardinality of
            // `ICON_TYPES` (~60–100) keeps the label dimension safe.
            tracing::warn!(action = %n.action, "system_notes: unknown action, dropping");
            continue;
        };
        let Some(noteable_kind) = NoteableKind::from_siphon(&n.noteable_type) else {
            tracing::debug!(
                noteable_type = %n.noteable_type,
                "system_notes: unsupported noteable_type, dropping"
            );
            continue;
        };
        let references = parse_body(action, &n.note);
        rows.push(NoteRow {
            traversal_path: n.traversal_path.clone(),
            author_id: n.author_id,
            noteable_id: n.noteable_id,
            noteable_kind,
            action,
            created_at: n.created_at,
            references,
        });
    }

    build_edges(&rows, |r, default_traversal| resolve(r, default_traversal))
}

/// Materialise the [`ResolutionPlan`] for the batch — the list of distinct
/// `(project_path, iid)` and path lookups the production resolver should
/// fan out to ClickHouse. Exposed as a top-level helper because the
/// production handler may pre-collect plans across multiple batches before
/// issuing the IN-list queries.
pub(crate) fn plan_for_batch(
    notes: &[ExtractedNote],
    default_projects: &DefaultProjectLookup,
) -> ResolutionPlan {
    let mut plan = ResolutionPlan::default();
    for n in notes {
        let Some(action) = Action::parse(&n.action) else {
            continue;
        };
        if NoteableKind::from_siphon(&n.noteable_type).is_none() {
            continue;
        }
        let default_project = default_projects
            .get(&(n.noteable_type.clone(), n.noteable_id))
            .map(String::as_str)
            .unwrap_or("");
        for r in parse_body(action, &n.note) {
            plan.add_ref(&r, default_project);
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_note(
        id: i64,
        action: &str,
        body: &str,
        noteable_type: &str,
        noteable_id: i64,
    ) -> ExtractedNote {
        ExtractedNote {
            id,
            note: body.to_string(),
            noteable_id,
            noteable_type: noteable_type.to_string(),
            author_id: Some(7),
            created_at: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            traversal_path: "1/100/".to_string(),
            action: action.to_string(),
            commit_count: None,
        }
    }

    #[test]
    fn process_batch_emits_mentions_edge_for_cross_reference() {
        let notes = vec![make_note(
            1,
            "cross_reference",
            "mentioned in !456",
            "MergeRequest",
            100,
        )];
        let edges = process_batch(&notes, |_r, tp| {
            Some(ResolvedTarget {
                id: 456,
                traversal_path: tp.to_string(),
            })
        });
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "MENTIONS");
    }

    #[test]
    fn process_batch_emits_user_closed_edge_for_lifecycle_action() {
        let notes = vec![make_note(2, "closed", "closed", "Issue", 999)];
        let edges = process_batch(&notes, |_, _| None);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "CLOSED");
        assert_eq!(edges[0].target_kind, "WorkItem");
        assert_eq!(edges[0].source_id, 7);
    }

    #[test]
    fn process_batch_drops_unknown_action_silently() {
        let notes = vec![make_note(3, "designs_added", "", "MergeRequest", 1)];
        let edges = process_batch(&notes, |_, _| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn process_batch_drops_unsupported_noteable_type_silently() {
        let notes = vec![make_note(4, "closed", "closed", "Snippet", 1)];
        let edges = process_batch(&notes, |_, _| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn plan_for_batch_collects_distinct_iid_pairs() {
        let notes = vec![
            make_note(
                1,
                "cross_reference",
                "mentioned in gitlab-org/gitlab!42",
                "MergeRequest",
                100,
            ),
            make_note(
                2,
                "cross_reference",
                "mentioned in gitlab-org/gitlab!42",
                "MergeRequest",
                101,
            ),
            make_note(
                3,
                "cross_reference",
                "mentioned in gitlab-org/gitlab#9",
                "MergeRequest",
                102,
            ),
        ];
        let defaults = DefaultProjectLookup::new();
        let plan = plan_for_batch(&notes, &defaults);
        assert_eq!(plan.mr_pairs.len(), 1, "MR pair !42 deduped");
        assert_eq!(plan.issue_pairs.len(), 1, "issue pair #9");
        assert_eq!(plan.paths.len(), 1, "project path gitlab-org/gitlab");
    }
}
