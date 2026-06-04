//! Edge row construction for the system-notes handler.
//!
//! Given a parsed `(source_note, action, references)` triple plus the
//! resolved entity ids from `resolve.rs`, emit one `gl_edge` row per
//! materialised edge. Edges that fail resolution (unknown path, unknown
//! `(project_id, iid)` pair) are silently dropped — the `_total{kind}`
//! observer in `metrics` counts them.
//!
//! Encoding shape matches the existing edge writers in
//! `crates/indexer/src/modules/code/arrow_converter.rs` so the same
//! `BatchWriter` plumbing carries these rows through to ClickHouse without
//! a new code path. The edge `relationship_kind` values are constants here
//! to keep the call sites grep-friendly and to make the unit tests below
//! cheap.

use super::parse::{Action, RefKind, Reference};
use super::resolve::ResolvedTarget;

/// Source-entity kind of a system note. Derived from `siphon_notes.noteable_type`.
///
/// Rails uses an STI discriminator with these string values. We collapse
/// the `Issue` family into `WorkItem` to match the upstream graph schema
/// (see `lower_edge_kind` precedent in
/// `crates/indexer/src/modules/sdlc/plan/lower.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteableKind {
    MergeRequest,
    WorkItem,
    Commit,
}

impl NoteableKind {
    pub fn from_siphon(noteable_type: &str) -> Option<Self> {
        Some(match noteable_type {
            "MergeRequest" => Self::MergeRequest,
            // Rails ships Issue, Epic, WorkItem, Task all as work-item-like
            // noteable_type strings. The graph collapses them to WorkItem,
            // mirroring the `lower_edge_kind` mapping for HAS_NOTE in
            // `modules/sdlc/plan/lower.rs`.
            "Issue" | "WorkItem" | "Epic" | "Task" => Self::WorkItem,
            "Commit" => Self::Commit,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::MergeRequest => "MergeRequest",
            Self::WorkItem => "WorkItem",
            Self::Commit => "Commit",
        }
    }
}

impl From<RefKind> for NoteableKind {
    fn from(kind: RefKind) -> Self {
        match kind {
            RefKind::Issue => Self::WorkItem,
            RefKind::MergeRequest => Self::MergeRequest,
            RefKind::Commit => Self::Commit,
        }
    }
}

/// Relationship kind constants for the system-notes handler. Mirror the
/// edge YAML filenames under `config/ontology/edges/`.
pub mod edge_kinds {
    pub const MENTIONS: &str = "MENTIONS";
    pub const REOPENED: &str = "REOPENED";
    pub const CLOSED: &str = "CLOSED";
    pub const MERGED: &str = "MERGED";
}

/// A single resolved edge ready to land in `gl_edge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedEdge {
    pub traversal_path: String,
    pub relationship_kind: &'static str,
    pub source_id: i64,
    pub source_kind: &'static str,
    pub target_id: i64,
    pub target_kind: &'static str,
}

/// A parsed system-note row that needs edge emission. The handler
/// constructs one of these per row pulled from `extract.rs` after
/// `parse.rs` runs. The references are resolved by `resolve.rs` into
/// `ResolvedTarget` entries before `build_edges` is called.
#[derive(Debug, Clone)]
pub struct NoteRow {
    pub traversal_path: String,
    /// Full path of the source note's owning project (e.g. `gitlab-org/gitlab`).
    /// Substituted for same-project GFM shorthand (`#123`, `!456`) when the
    /// reference carries no explicit project prefix. Empty string when the
    /// owning project is unknown — the resolver then declines to resolve
    /// unqualified references on this row.
    pub default_project: String,
    pub author_id: Option<i64>,
    pub noteable_id: i64,
    pub noteable_kind: NoteableKind,
    pub action: Action,
    pub references: Vec<Reference>,
}

/// Emit edges for a batch of parsed notes given a target resolver. The
/// resolver receives each [`Reference`] plus the source row's
/// `default_project` (for same-project shorthand) and returns `None` for any
/// unresolvable `(project_path, iid)` or commit SHA — those references are
/// silently dropped.
pub fn build_edges<R>(rows: &[NoteRow], mut resolve: R) -> Vec<EmittedEdge>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut edges = Vec::new();
    for row in rows {
        match row.action {
            // Lifecycle: User → Noteable.
            Action::Closed | Action::Reopened | Action::Merged => {
                let Some(author_id) = row.author_id else {
                    continue;
                };
                // Drop targets the edge YAML doesn't declare: `merged` only on
                // MergeRequest, `closed`/`reopened` on MR or WorkItem. Keeps the
                // emitter honest if Rails or a manual edit sends an odd noteable.
                let declared_target = match row.action {
                    Action::Merged => row.noteable_kind == NoteableKind::MergeRequest,
                    _ => matches!(
                        row.noteable_kind,
                        NoteableKind::MergeRequest | NoteableKind::WorkItem
                    ),
                };
                if !declared_target {
                    continue;
                }
                let kind = match row.action {
                    Action::Closed => edge_kinds::CLOSED,
                    Action::Reopened => edge_kinds::REOPENED,
                    Action::Merged => edge_kinds::MERGED,
                    _ => unreachable!(),
                };
                edges.push(EmittedEdge {
                    traversal_path: row.traversal_path.clone(),
                    relationship_kind: kind,
                    source_id: author_id,
                    source_kind: "User",
                    target_id: row.noteable_id,
                    target_kind: row.noteable_kind.as_str(),
                });
            }

            // `opened` produces no edge: there is no source-of-truth FK for
            // who opened, and the entity's `created_at` already covers the
            // lifecycle point. ADR 013: out of scope.
            Action::Opened => {}

            // Cross-reference / relate / hierarchy: Noteable → Target.
            // All collapse to MENTIONS edges in v1; link-type taxonomy is
            // an open question in the ADR.
            Action::CrossReference
            | Action::Relate
            | Action::Unrelate
            | Action::RelateToParent
            | Action::RelateToChild
            | Action::UnrelateFromParent
            | Action::UnrelateFromChild
            | Action::Moved
            | Action::Cloned
            | Action::Duplicate
            | Action::Commit
            | Action::Merge => {
                for r in &row.references {
                    let Some(resolved) = resolve(r, row.default_project.as_str()) else {
                        continue;
                    };
                    // Skip self-loops (MR !100 "mentioned in !100"): pollutes
                    // degree counts and the reader doesn't need them.
                    let target_kind = NoteableKind::from(r.kind);
                    if target_kind == row.noteable_kind && resolved.id == row.noteable_id {
                        continue;
                    }
                    edges.push(EmittedEdge {
                        traversal_path: resolved.traversal_path.clone(),
                        relationship_kind: edge_kinds::MENTIONS,
                        source_id: row.noteable_id,
                        source_kind: row.noteable_kind.as_str(),
                        target_id: resolved.id,
                        target_kind: target_kind.as_str(),
                    });
                }
            }
        }
    }
    edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::transform::system_notes::parse;
    use crate::modules::sdlc::transform::system_notes::resolve::ResolvedTarget;

    fn row_for(
        action: Action,
        body: &str,
        noteable_kind: NoteableKind,
        noteable_id: i64,
    ) -> NoteRow {
        NoteRow {
            traversal_path: "1/2/".to_string(),
            default_project: "src/proj".to_string(),
            author_id: Some(7),
            noteable_id,
            noteable_kind,
            action,
            references: parse::extract(action, body),
        }
    }

    fn always_resolve(
        id: i64,
        traversal_path: &str,
    ) -> impl FnMut(&Reference, &str) -> Option<ResolvedTarget> + use<> {
        let tp = traversal_path.to_string();
        move |_r, _default| {
            Some(ResolvedTarget {
                id,
                traversal_path: tp.clone(),
            })
        }
    }

    #[test]
    fn cross_reference_emits_mr_to_mr_mentions_edge() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !456",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(456, "1/2/"));
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!(e.relationship_kind, "MENTIONS");
        assert_eq!(e.source_id, 100);
        assert_eq!(e.source_kind, "MergeRequest");
        assert_eq!(e.target_id, 456);
        assert_eq!(e.target_kind, "MergeRequest");
    }

    #[test]
    fn cross_reference_collapses_issue_target_to_work_item() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in #99",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(99, "1/2/"));
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].target_kind, "WorkItem");
    }

    #[test]
    fn closed_lifecycle_emits_user_to_noteable_edge() {
        let row = row_for(Action::Closed, "closed", NoteableKind::WorkItem, 555);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!(e.relationship_kind, "CLOSED");
        assert_eq!(e.source_id, 7);
        assert_eq!(e.source_kind, "User");
        assert_eq!(e.target_id, 555);
        assert_eq!(e.target_kind, "WorkItem");
    }

    #[test]
    fn reopened_lifecycle_emits_user_reopened_target_edge() {
        let row = row_for(Action::Reopened, "reopened", NoteableKind::MergeRequest, 42);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "REOPENED");
        assert_eq!(edges[0].target_kind, "MergeRequest");
    }

    #[test]
    fn merged_on_non_mr_noteable_is_dropped() {
        // `merged.yaml` only declares User → MergeRequest. If Rails (or a
        // manual DB edit) ever lands `action='merged'` on a WorkItem
        // noteable, the emitter should drop it instead of producing an
        // undeclared edge variant.
        let row = row_for(Action::Merged, "merged", NoteableKind::WorkItem, 42);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn closed_on_commit_noteable_is_dropped() {
        // `closed.yaml` / `reopened.yaml` declare only MergeRequest and
        // WorkItem targets; there is no `Commit` node yet. A `closed`
        // action landing on a `Commit` noteable must drop rather than emit
        // an undeclared `User → Commit CLOSED` edge.
        let row = row_for(Action::Closed, "closed", NoteableKind::Commit, 5);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn reopened_on_commit_noteable_is_dropped() {
        let row = row_for(Action::Reopened, "reopened", NoteableKind::Commit, 5);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn opened_lifecycle_emits_nothing() {
        let row = row_for(Action::Opened, "opened", NoteableKind::WorkItem, 1);
        let edges = build_edges(&[row], always_resolve(0, "1/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn lifecycle_without_author_id_drops_edge() {
        let mut row = row_for(Action::Closed, "closed", NoteableKind::WorkItem, 555);
        row.author_id = None;
        let edges = build_edges(&[row], always_resolve(0, "1/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn unresolvable_reference_drops_edge_silently() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !456",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], |_r: &Reference, _d: &str| None);
        assert!(edges.is_empty());
    }

    #[test]
    fn self_loop_is_filtered() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !100",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(100, "1/2/"));
        assert!(
            edges.is_empty(),
            "self-loop MR !100 → MR !100 should be filtered"
        );
    }
}
