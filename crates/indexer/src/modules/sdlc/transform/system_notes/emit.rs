use orbit_log_fields::warn;

use super::parse::{Action, RefKind, Reference};
use super::resolve::ResolvedTarget;

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

pub mod edge_kinds {
    pub const MENTIONS: &str = "MENTIONS";
    pub const CLOSED: &str = "CLOSED";
    pub const MERGED: &str = "MERGED";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedEdge<'a> {
    pub traversal_path: &'a str,
    pub relationship_kind: &'static str,
    pub source_id: i64,
    pub source_kind: &'static str,
    pub target_id: i64,
    pub target_kind: &'static str,
}

#[derive(Debug, Clone)]
pub struct NoteRow<'a, 'p> {
    pub traversal_path: &'a str,
    pub default_project: &'p str,
    pub author_id: Option<i64>,
    pub noteable_id: i64,
    pub noteable_kind: NoteableKind,
    pub action: Action,
    pub references: Vec<Reference>,
}

fn lifecycle_edge_kind(action: Action) -> Option<&'static str> {
    match action {
        Action::Closed => Some(edge_kinds::CLOSED),
        Action::Merged => Some(edge_kinds::MERGED),
        _ => None,
    }
}

pub fn build_edges<'a, R>(rows: &[NoteRow<'a, '_>], mut resolve: R) -> Vec<EmittedEdge<'a>>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut edges = Vec::new();
    for row in rows {
        match row.action {
            Action::Closed | Action::Merged => {
                let Some(author_id) = row.author_id else {
                    continue;
                };
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
                let Some(kind) = lifecycle_edge_kind(row.action) else {
                    warn!(
                        safe(KIND) = row.action.as_str(),
                        safe(NOTEABLE_ID) = row.noteable_id,
                        "system_notes: unexpected action in lifecycle arm, skipping row"
                    );
                    continue;
                };
                edges.push(EmittedEdge {
                    traversal_path: row.traversal_path,
                    relationship_kind: kind,
                    source_id: author_id,
                    source_kind: "User",
                    target_id: row.noteable_id,
                    target_kind: row.noteable_kind.as_str(),
                });
            }

            Action::Opened => {}

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
                    let Some(resolved) = resolve(r, row.default_project) else {
                        continue;
                    };
                    let ref_kind = NoteableKind::from(r.kind);
                    if ref_kind == row.noteable_kind && resolved.id == row.noteable_id {
                        continue;
                    }
                    edges.push(EmittedEdge {
                        traversal_path: row.traversal_path,
                        relationship_kind: edge_kinds::MENTIONS,
                        source_id: resolved.id,
                        source_kind: ref_kind.as_str(),
                        target_id: row.noteable_id,
                        target_kind: row.noteable_kind.as_str(),
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
    ) -> NoteRow<'static, 'static> {
        NoteRow {
            traversal_path: "1/2/",
            default_project: "src/proj",
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
        let tp = orbit_utils::traversal_path::TraversalPath::new_unchecked(traversal_path);
        move |_r, _default| {
            Some(ResolvedTarget {
                id,
                traversal_path: tp.clone(),
            })
        }
    }

    #[test]
    fn cross_reference_emits_mentioner_to_mentioned_mentions_edge() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !456",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(456, "3/4/"));
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!(e.relationship_kind, "MENTIONS");
        assert_eq!(e.source_id, 456);
        assert_eq!(e.source_kind, "MergeRequest");
        assert_eq!(e.target_id, 100);
        assert_eq!(e.target_kind, "MergeRequest");
        assert_eq!(
            e.traversal_path, "1/2/",
            "edge lands in the noteable's (target's) namespace partition"
        );
    }

    #[test]
    fn cross_reference_collapses_issue_ref_to_work_item_source() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in #99",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(99, "3/4/"));
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_id, 99);
        assert_eq!(edges[0].source_kind, "WorkItem");
        assert_eq!(edges[0].target_id, 100);
        assert_eq!(edges[0].target_kind, "MergeRequest");
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
    fn lifecycle_edge_kind_maps_actions_and_skips_non_lifecycle() {
        assert_eq!(
            lifecycle_edge_kind(Action::Closed),
            Some(edge_kinds::CLOSED)
        );
        assert_eq!(
            lifecycle_edge_kind(Action::Merged),
            Some(edge_kinds::MERGED)
        );
        assert_eq!(lifecycle_edge_kind(Action::CrossReference), None);
    }

    #[test]
    fn merged_on_non_mr_noteable_is_dropped() {
        let row = row_for(Action::Merged, "merged", NoteableKind::WorkItem, 42);
        let edges = build_edges(&[row], always_resolve(0, "1/2/"));
        assert!(edges.is_empty());
    }

    #[test]
    fn closed_on_commit_noteable_is_dropped() {
        let row = row_for(Action::Closed, "closed", NoteableKind::Commit, 5);
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
