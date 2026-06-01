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
//! ## How it is wired into the engine
//!
//! The handler ([`handler::SystemNotesHandler`]) is a standalone
//! [`crate::handler::Handler`] registered through the engine's
//! [`crate::handler::HandlerRegistry`], exactly like
//! `crates/indexer/src/modules/namespace_deletion/`. It rides the existing
//! [`crate::topic::NamespaceIndexingRequest`] subscription: the dispatcher
//! already publishes one namespace indexing message per namespace, NATS
//! fans it out to every subscriber, and this handler is one more
//! subscriber alongside the per-entity handlers. It keeps its own
//! checkpoint key (`{scope}.SystemNote`) so its watermark advances
//! independently of the ontology entity handlers.
//!
//! ADR 014 sketches a future `EntityPipeline` custom-pipeline slot as the
//! eventual home for this logic. That slot is not on `main` (it lives in
//! the still-draft entity-handler stack), and it is not a prerequisite:
//! the parse/resolve/emit/extract core here is pure and reusable, so
//! migrating to the `EntityPipeline` slot later is a thin I/O-shell swap,
//! not a rewrite. Until then this standalone handler is the as-built path.
//!
//! Custom-handler precedent in the existing codebase:
//! `crates/indexer/src/modules/namespace_deletion/`.

pub(crate) mod emit;
pub(crate) mod extract;
pub(crate) mod handler;
pub(crate) mod parse;
pub(crate) mod resolve;
pub(crate) mod vendored;

pub use handler::register_handlers;
#[doc(hidden)]
pub use handler::{SystemNotesHandler, build_handler};

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
    /// `siphon_notes.project_id` — the note's owning project. Resolved to a
    /// path via `siphon_routes` to become the default project for
    /// unqualified GFM references on this row.
    pub project_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub traversal_path: String,
    pub action: String,
    /// `siphon_system_note_metadata.commit_count`. Populated only for the
    /// `commit` action (Rails leaves it `NULL` for all others). Carried on
    /// the row so the SELECT in `extract.rs` stays stable for the planned
    /// parsed-vs-replicated SHA-count drift assertion (commit edges are
    /// out of scope until `Commit` nodes exist), hence not yet read.
    #[allow(dead_code, reason = "reserved for the commit-count drift assertion")]
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
/// `default_projects` supplies each row's owning project path (keyed on
/// `(noteable_type, noteable_id)`) so same-project GFM shorthand resolves;
/// it is stamped onto each [`NoteRow::default_project`] and handed to the
/// resolver closure per reference.
///
/// Splitting the ClickHouse round-trip out as a closure keeps this function
/// pure: the unit tests exercise the full pipeline with a stub resolver,
/// and the production handler ([`handler::SystemNotesHandler`]) passes a
/// closure backed by the [`resolve::ResolvedIndex`] built from the
/// [`ResolutionPlan`] that [`plan_for_batch`] fans out to ClickHouse (see
/// `resolve::ROUTES_SQL`, `resolve::MERGE_REQUESTS_SQL`,
/// `resolve::WORK_ITEMS_SQL`).
pub(crate) fn process_batch<R>(
    notes: &[ExtractedNote],
    default_projects: &DefaultProjectLookup,
    mut resolve: R,
) -> Vec<EmittedEdge>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut rows = Vec::with_capacity(notes.len());
    for n in notes {
        let Some(action) = Action::parse(&n.action) else {
            // Unknown action: log + drop. WARN-level (not debug) so a
            // staging deployment surfaces drift against Rails immediately
            // without a log-level config push. The bounded cardinality of
            // `ICON_TYPES` (~60–100) keeps the metric label dimension safe;
            // the handler increments
            // `gkg.indexer.sdlc.system_notes.unknown_action_total{action}`
            // around this drop (ADR 013 step 8).
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
        let default_project = default_projects
            .get(&(n.noteable_type.clone(), n.noteable_id))
            .cloned()
            .unwrap_or_default();
        rows.push(NoteRow {
            traversal_path: n.traversal_path.clone(),
            default_project,
            author_id: n.author_id,
            noteable_id: n.noteable_id,
            noteable_kind,
            action,
            created_at: n.created_at,
            references,
        });
    }

    build_edges(&rows, |r, default_project| resolve(r, default_project))
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
            project_id: Some(100),
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
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_r, _default| {
            Some(ResolvedTarget {
                id: 456,
                traversal_path: "1/100/".to_string(),
            })
        });
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "MENTIONS");
    }

    #[test]
    fn process_batch_passes_default_project_to_resolver() {
        // `!456` is same-project shorthand; the handler must hand the
        // resolver the source note's owning project from the lookup so the
        // edge resolves against the right project.
        let notes = vec![make_note(
            1,
            "cross_reference",
            "mentioned in !456",
            "MergeRequest",
            100,
        )];
        let mut defaults = DefaultProjectLookup::new();
        defaults.insert(("MergeRequest".to_string(), 100), "my/proj".to_string());

        let mut seen_default = None;
        let edges = process_batch(&notes, &defaults, |_r, default| {
            seen_default = Some(default.to_string());
            Some(ResolvedTarget {
                id: 456,
                traversal_path: "1/100/".to_string(),
            })
        });
        assert_eq!(edges.len(), 1);
        assert_eq!(seen_default.as_deref(), Some("my/proj"));
    }

    #[test]
    fn process_batch_emits_user_closed_edge_for_lifecycle_action() {
        let notes = vec![make_note(2, "closed", "closed", "Issue", 999)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "CLOSED");
        assert_eq!(edges[0].target_kind, "WorkItem");
        assert_eq!(edges[0].source_id, 7);
    }

    #[test]
    fn process_batch_drops_unknown_action_silently() {
        let notes = vec![make_note(3, "designs_added", "", "MergeRequest", 1)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn process_batch_drops_unsupported_noteable_type_silently() {
        let notes = vec![make_note(4, "closed", "closed", "Snippet", 1)];
        let edges = process_batch(&notes, &DefaultProjectLookup::new(), |_, _| None);
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
