//! Encoding shape matches the existing edge writers in
//! `crates/indexer/src/modules/code/arrow_converter.rs` so the same
//! `BatchWriter` plumbing carries these rows through to ClickHouse without
//! a new code path.

use std::hash::{Hash, Hasher};

use tracing::warn;

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

/// Values must mirror the edge YAML filenames under `config/ontology/edges/`.
pub mod edge_kinds {
    pub const MENTIONS: &str = "MENTIONS";
    pub const CLOSED: &str = "CLOSED";
    pub const MERGED: &str = "MERGED";
    pub const ADDS_COMMIT: &str = "ADDS_COMMIT";
    pub const MERGED_AT_COMMIT: &str = "MERGED_AT_COMMIT";
}

/// Synthetic `Commit` node id, mirroring `compute_branch_id` in
/// `crates/indexer/src/modules/code/arrow_converter.rs`. A commit SHA is a
/// hex string, but `gl_edge.target_id` and the `gl_commit` PK are `Int64`, so
/// the node id is a deterministic hash of `(project_id, sha)`. Scoping by
/// `project_id` (not the SHA alone) matches the Branch precedent and avoids
/// cross-fork collisions where the same SHA appears in different projects.
pub fn compute_commit_id(project_id: i64, sha: &str) -> i64 {
    let mut hasher = rustc_hash::FxHasher::default();
    project_id.hash(&mut hasher);
    sha.hash(&mut hasher);
    // Mask clears the sign bit so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedEdge {
    pub traversal_path: String,
    pub relationship_kind: &'static str,
    pub source_id: i64,
    pub source_kind: &'static str,
    pub target_id: i64,
    pub target_kind: &'static str,
}

/// A `Commit` node row ready to land in `gl_commit`. Emitted inline by the
/// transform alongside the `ADDS_COMMIT` / `MERGED_AT_COMMIT` edge, since no
/// Siphon source table drives commit rows (commits live in Gitaly). Mirrors
/// the code indexer's inline `Branch`-row emission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedCommit {
    pub id: i64,
    pub traversal_path: String,
    pub project_id: i64,
    pub sha: String,
}

/// The output of [`build_edges`]: edge rows for `gl_edge` plus the commit node
/// rows for `gl_commit` that the commit-bearing actions reference.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EmittedRows {
    pub edges: Vec<EmittedEdge>,
    pub commits: Vec<EmittedCommit>,
}

#[derive(Debug, Clone)]
pub struct NoteRow {
    pub traversal_path: String,
    /// Full path of the source note's owning project (e.g. `gitlab-org/gitlab`).
    /// Substituted for same-project GFM shorthand (`#123`, `!456`) when the
    /// reference carries no explicit project prefix. Empty string when the
    /// owning project is unknown — the resolver then declines to resolve
    /// unqualified references on this row.
    pub default_project: String,
    /// Numeric id of the source note's owning project. Used to compute the
    /// synthetic `Commit` node id for `Action::Commit` / `Action::Merge`
    /// (the commits belong to the noteable's project). `0` when unknown — the
    /// commit edge is dropped in that case rather than hashed against a bogus
    /// project.
    pub project_id: i64,
    pub author_id: Option<i64>,
    pub noteable_id: i64,
    pub noteable_kind: NoteableKind,
    pub action: Action,
    pub references: Vec<Reference>,
}

/// Returns `None` for any non-lifecycle action so the caller can log-and-skip
/// rather than `unreachable!`-panic if the outer match arm and this mapping
/// ever drift.
fn lifecycle_edge_kind(action: Action) -> Option<&'static str> {
    match action {
        Action::Closed => Some(edge_kinds::CLOSED),
        Action::Merged => Some(edge_kinds::MERGED),
        _ => None,
    }
}

/// The resolver returns `None` for any unresolvable `(project_path, iid)` or
/// commit SHA — those references are silently dropped.
pub fn build_edges<R>(rows: &[NoteRow], mut resolve: R) -> EmittedRows
where
    R: FnMut(&Reference, &str) -> Option<ResolvedTarget>,
{
    let mut out = EmittedRows::default();
    let edges = &mut out.edges;
    let commits = &mut out.commits;
    for row in rows {
        match row.action {
            Action::Closed | Action::Merged => {
                let Some(author_id) = row.author_id else {
                    continue;
                };
                // Edge YAML declares `merged` only on MergeRequest, `closed`
                // on MR or WorkItem; drop anything else.
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
                // Skip (don't `unreachable!`) on an unexpected action: a
                // panic here would crash-loop the worker on one bad row.
                // See "no panics in the indexer data path" in AGENTS.md.
                let Some(kind) = lifecycle_edge_kind(row.action) else {
                    warn!(
                        action = ?row.action,
                        noteable_id = row.noteable_id,
                        "system_notes: unexpected action in lifecycle arm, skipping row"
                    );
                    continue;
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
            | Action::Duplicate => {
                for r in &row.references {
                    let Some(resolved) = resolve(r, row.default_project.as_str()) else {
                        continue;
                    };
                    // Skip self-loops (MR !100 "mentioned in !100"): they
                    // pollute degree counts.
                    let ref_kind = NoteableKind::from(r.kind);
                    if ref_kind == row.noteable_kind && resolved.id == row.noteable_id {
                        continue;
                    }
                    edges.push(EmittedEdge {
                        traversal_path: row.traversal_path.clone(),
                        relationship_kind: edge_kinds::MENTIONS,
                        source_id: resolved.id,
                        source_kind: ref_kind.as_str(),
                        target_id: row.noteable_id,
                        target_kind: row.noteable_kind.as_str(),
                    });
                }
            }

            // "added N commits" → MergeRequest --ADDS_COMMIT--> Commit.
            // `adds_commit.yaml` only declares MergeRequest → Commit, so drop
            // the (rare) case where a commit-action note lands on a non-MR
            // noteable rather than emit an undeclared variant.
            Action::Commit => {
                if row.noteable_kind != NoteableKind::MergeRequest {
                    continue;
                }
                for r in &row.references {
                    if let Some(commit) = build_commit_node(row, r) {
                        edges.push(commit_edge(row, edge_kinds::ADDS_COMMIT, commit.id));
                        commits.push(commit);
                    }
                }
            }

            // `merge` has two body shapes: a SHA-bearing auto-merge variant
            // ("enabled an automatic merge ... for <sha> pass") → MERGED_AT_COMMIT,
            // and a "created merge request !123" variant that stays MENTIONS
            // (semantically a cross-reference). The parser already returns the
            // commit ref for the first and the MR/issue ref for the second.
            Action::Merge => {
                for r in &row.references {
                    match r.kind {
                        RefKind::Commit => {
                            if let Some(commit) = build_commit_node(row, r) {
                                edges.push(commit_edge(
                                    row,
                                    edge_kinds::MERGED_AT_COMMIT,
                                    commit.id,
                                ));
                                commits.push(commit);
                            }
                        }
                        RefKind::MergeRequest | RefKind::Issue => {
                            let Some(resolved) = resolve(r, row.default_project.as_str()) else {
                                continue;
                            };
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
        }
    }
    out
}

/// Build the `Commit` node row for a commit reference on `row`, or `None` when
/// the reference carries no SHA or the project id is unknown (id `0`). The
/// synthetic id is deterministic from `(project_id, sha)`, so no datalake
/// lookup is needed.
fn build_commit_node(row: &NoteRow, r: &Reference) -> Option<EmittedCommit> {
    if r.kind != RefKind::Commit {
        return None;
    }
    let sha = r.commit_sha.as_deref()?;
    // A zero project id means the owning project never resolved; hashing
    // against it would scatter commits under a bogus node. Drop instead.
    if row.project_id == 0 {
        warn!(
            noteable_id = row.noteable_id,
            sha, "system_notes: commit ref with unknown project_id, skipping"
        );
        return None;
    }
    Some(EmittedCommit {
        id: compute_commit_id(row.project_id, sha),
        traversal_path: row.traversal_path.clone(),
        project_id: row.project_id,
        sha: sha.to_owned(),
    })
}

fn commit_edge(row: &NoteRow, kind: &'static str, commit_id: i64) -> EmittedEdge {
    EmittedEdge {
        traversal_path: row.traversal_path.clone(),
        relationship_kind: kind,
        source_id: row.noteable_id,
        source_kind: row.noteable_kind.as_str(),
        target_id: commit_id,
        target_kind: NoteableKind::Commit.as_str(),
    }
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
            project_id: 2,
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
    fn cross_reference_emits_mentioner_to_mentioned_mentions_edge() {
        let row = row_for(
            Action::CrossReference,
            "mentioned in !456",
            NoteableKind::MergeRequest,
            100,
        );
        let edges = build_edges(&[row], always_resolve(456, "3/4/")).edges;
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
        let edges = build_edges(&[row], always_resolve(99, "3/4/")).edges;
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_id, 99);
        assert_eq!(edges[0].source_kind, "WorkItem");
        assert_eq!(edges[0].target_id, 100);
        assert_eq!(edges[0].target_kind, "MergeRequest");
    }

    #[test]
    fn closed_lifecycle_emits_user_to_noteable_edge() {
        let row = row_for(Action::Closed, "closed", NoteableKind::WorkItem, 555);
        let edges = build_edges(&[row], always_resolve(0, "1/2/")).edges;
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
        // `merged.yaml` only declares User → MergeRequest, so a WorkItem
        // noteable must drop.
        let row = row_for(Action::Merged, "merged", NoteableKind::WorkItem, 42);
        let edges = build_edges(&[row], always_resolve(0, "1/2/")).edges;
        assert!(edges.is_empty());
    }

    #[test]
    fn closed_on_commit_noteable_is_dropped() {
        // `closed.yaml` declares no `Commit` target (no `Commit` node yet).
        let row = row_for(Action::Closed, "closed", NoteableKind::Commit, 5);
        let edges = build_edges(&[row], always_resolve(0, "1/2/")).edges;
        assert!(edges.is_empty());
    }

    #[test]
    fn opened_lifecycle_emits_nothing() {
        let row = row_for(Action::Opened, "opened", NoteableKind::WorkItem, 1);
        let edges = build_edges(&[row], always_resolve(0, "1/")).edges;
        assert!(edges.is_empty());
    }

    #[test]
    fn lifecycle_without_author_id_drops_edge() {
        let mut row = row_for(Action::Closed, "closed", NoteableKind::WorkItem, 555);
        row.author_id = None;
        let edges = build_edges(&[row], always_resolve(0, "1/")).edges;
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
        let edges = build_edges(&[row], |_r: &Reference, _d: &str| None).edges;
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
        let edges = build_edges(&[row], always_resolve(100, "1/2/")).edges;
        assert!(
            edges.is_empty(),
            "self-loop MR !100 → MR !100 should be filtered"
        );
    }

    #[test]
    fn commit_action_emits_adds_commit_edges_and_nodes() {
        let body = "added 2 commits\n\n\
                    * abc1234 - Fix the bug\n\
                    * def5678 - Add a test\n";
        let row = row_for(Action::Commit, body, NoteableKind::MergeRequest, 100);
        let out = build_edges(&[row], |_r: &Reference, _d: &str| None);

        assert_eq!(out.edges.len(), 2);
        assert_eq!(out.commits.len(), 2);

        for e in &out.edges {
            assert_eq!(e.relationship_kind, "ADDS_COMMIT");
            assert_eq!(e.source_id, 100);
            assert_eq!(e.source_kind, "MergeRequest");
            assert_eq!(e.target_kind, "Commit");
        }
        let shas: Vec<&str> = out.commits.iter().map(|c| c.sha.as_str()).collect();
        assert_eq!(shas, vec!["abc1234", "def5678"]);
        for c in &out.commits {
            assert_eq!(c.project_id, 2);
            assert_eq!(c.id, compute_commit_id(2, &c.sha));
        }
        // Edge target ids must match the emitted node ids.
        assert_eq!(out.edges[0].target_id, out.commits[0].id);
        assert_eq!(out.edges[1].target_id, out.commits[1].id);
    }

    #[test]
    fn commit_action_on_non_mr_noteable_is_dropped() {
        // adds_commit.yaml only declares MergeRequest → Commit.
        let body = "added 1 commit\n\n* abc1234 - Fix\n";
        let row = row_for(Action::Commit, body, NoteableKind::WorkItem, 7);
        let out = build_edges(&[row], |_r: &Reference, _d: &str| None);
        assert!(out.edges.is_empty());
        assert!(out.commits.is_empty());
    }

    #[test]
    fn merge_action_sha_variant_emits_merged_at_commit() {
        let row = row_for(
            Action::Merge,
            "enabled an automatic merge when all merge checks for 1a2b3c4d5e pass",
            NoteableKind::MergeRequest,
            100,
        );
        let out = build_edges(&[row], |_r: &Reference, _d: &str| None);
        assert_eq!(out.edges.len(), 1);
        assert_eq!(out.commits.len(), 1);
        let e = &out.edges[0];
        assert_eq!(e.relationship_kind, "MERGED_AT_COMMIT");
        assert_eq!(e.source_kind, "MergeRequest");
        assert_eq!(e.target_kind, "Commit");
        assert_eq!(e.target_id, out.commits[0].id);
        assert_eq!(out.commits[0].sha, "1a2b3c4d5e");
        assert_eq!(out.commits[0].id, compute_commit_id(2, "1a2b3c4d5e"));
    }

    #[test]
    fn merge_action_mr_ref_variant_stays_mentions() {
        let row = row_for(
            Action::Merge,
            "created merge request !123 to address this issue",
            NoteableKind::WorkItem,
            100,
        );
        let out = build_edges(&[row], always_resolve(123, "1/2/"));
        assert_eq!(out.edges.len(), 1);
        assert!(out.commits.is_empty());
        let e = &out.edges[0];
        assert_eq!(e.relationship_kind, "MENTIONS");
        assert_eq!(e.target_id, 123);
        assert_eq!(e.target_kind, "MergeRequest");
    }

    #[test]
    fn commit_ref_with_unknown_project_id_is_dropped() {
        let body = "added 1 commit\n\n* abc1234 - Fix\n";
        let mut row = row_for(Action::Commit, body, NoteableKind::MergeRequest, 100);
        row.project_id = 0;
        let out = build_edges(&[row], |_r: &Reference, _d: &str| None);
        assert!(out.edges.is_empty());
        assert!(out.commits.is_empty());
    }

    #[test]
    fn compute_commit_id_is_deterministic_and_project_scoped() {
        assert_eq!(
            compute_commit_id(1, "abc1234"),
            compute_commit_id(1, "abc1234")
        );
        assert_ne!(
            compute_commit_id(1, "abc1234"),
            compute_commit_id(2, "abc1234"),
            "same SHA in different projects must not collide"
        );
        assert!(compute_commit_id(1, "abc1234") >= 0, "id must be positive");
    }
}
