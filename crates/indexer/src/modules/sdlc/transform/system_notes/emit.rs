//! Encoding shape matches the existing edge writers in
//! `crates/indexer/src/modules/code/arrow_converter.rs` so the same
//! `BatchWriter` plumbing carries these rows through to ClickHouse without
//! a new code path.

use std::collections::BTreeMap;

use super::parse::{Action, RefKind, Reference};
use super::resolve::ResolvedTarget;

/// Collapses the `Issue` family into `WorkItem` to match the upstream graph
/// schema (see `lower_edge_kind` precedent in
/// `crates/indexer/src/modules/sdlc/plan/lower.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteableKind {
    MergeRequest,
    WorkItem,
    Commit,
}

/// The names a system note's edges carry, read from the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteKinds {
    pub mentions: String,
    pub merge_request: String,
    pub work_item: String,
    pub commit: String,
    /// The Rails noteable types that name another entity, from the note's own mapping.
    pub aliases: BTreeMap<String, String>,
}

impl NoteableKind {
    pub fn from_siphon(noteable_type: &str, kinds: &NoteKinds) -> Option<Self> {
        let name = kinds
            .aliases
            .get(noteable_type)
            .map_or(noteable_type, String::as_str);
        [Self::MergeRequest, Self::WorkItem, Self::Commit]
            .into_iter()
            .find(|kind| kind.name(kinds) == name)
    }

    pub fn name(self, kinds: &NoteKinds) -> &str {
        match self {
            Self::MergeRequest => &kinds.merge_request,
            Self::WorkItem => &kinds.work_item,
            Self::Commit => &kinds.commit,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedEdge<'a> {
    pub traversal_path: &'a str,
    pub relationship_kind: &'a str,
    pub source_id: i64,
    pub source_kind: &'a str,
    pub target_id: i64,
    pub target_kind: &'a str,
}

/// Two lifetimes because the default-project lookup is dropped before the emitted
/// edges are returned, while the block they borrow from outlives them.
#[derive(Debug, Clone)]
pub struct NoteRow<'a, 'p> {
    pub traversal_path: &'a str,
    /// Full path of the source note's owning project (e.g. `gitlab-org/gitlab`).
    /// Substituted for same-project GFM shorthand (`#123`, `!456`) when the
    /// reference carries no explicit project prefix. Empty string when the
    /// owning project is unknown — the resolver then declines to resolve
    /// unqualified references on this row.
    pub default_project: &'p str,
    pub noteable_id: i64,
    pub noteable_kind: NoteableKind,
    pub action: Action,
    pub references: Vec<Reference>,
}

/// The resolver returns `None` for any unresolvable `(project_path, iid)` or
/// commit SHA — those references are silently dropped.
pub fn build_edges<'a, R>(
    rows: &[NoteRow<'a, '_>],
    kinds: &'a NoteKinds,
    mut resolve: R,
) -> Vec<EmittedEdge<'a>>
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut edges = Vec::new();
    for row in rows {
        match row.action {
            // Edge points from the parsed ref (source) to the noteable
            // (target), matching the ontology's directional mentioner →
            // mentioned. It lands in the noteable's namespace partition
            // (`row.traversal_path`) so inbound-degree queries on the target
            // hit the right partition. All collapse to MENTIONS in v1;
            // link-type taxonomy is an open question in the ADR.
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
                    // Skip self-loops (MR !100 "mentioned in !100"): they
                    // pollute degree counts.
                    let ref_kind = NoteableKind::from(r.kind);
                    if ref_kind == row.noteable_kind && resolved.id == row.noteable_id {
                        continue;
                    }
                    edges.push(EmittedEdge {
                        traversal_path: row.traversal_path,
                        relationship_kind: &kinds.mentions,
                        source_id: resolved.id,
                        source_kind: ref_kind.name(kinds),
                        target_id: row.noteable_id,
                        target_kind: row.noteable_kind.name(kinds),
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
    use crate::modules::sdlc::transform::system_notes::test_resolve_sql;

    static KINDS: std::sync::LazyLock<NoteKinds> =
        std::sync::LazyLock::new(|| test_resolve_sql().kinds);
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
        let edges = build_edges(&[row], &KINDS, always_resolve(456, "3/4/"));
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
        let edges = build_edges(&[row], &KINDS, always_resolve(99, "3/4/"));
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_id, 99);
        assert_eq!(edges[0].source_kind, "WorkItem");
        assert_eq!(edges[0].target_id, 100);
        assert_eq!(edges[0].target_kind, "MergeRequest");
    }

    #[test]
    fn unresolvable_reference_drops_edge_silently() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !456",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], &KINDS, |_r: &Reference, _d: &str| None);
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
        let edges = build_edges(&[row], &KINDS, always_resolve(100, "1/2/"));
        assert!(
            edges.is_empty(),
            "self-loop MR !100 → MR !100 should be filtered"
        );
    }
}
