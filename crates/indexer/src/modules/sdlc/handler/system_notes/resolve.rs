//! ClickHouse batch resolvers for system-note edge materialization.
//!
//! The reference-resolution pipeline runs in two stages per indexer batch:
//!
//! 1. **Path → (source_type, source_id, traversal_path)** against
//!    `siphon_routes`. Maps the GFM project path (e.g. `gitlab-org/gitlab`)
//!    that appears inside a note body to the routed entity that owns it.
//!
//! 2. **(project_id, iid) → entity id** against `merge_requests` and
//!    `work_items` (and downstream `issues` once the work-item migration is
//!    complete). Maps the resolved project + the IID inside the GFM ref
//!    (`#123` or `!456`) to the durable entity primary key the edge writer
//!    needs.
//!
//! Both queries are parameterized with named placeholders (`{name:Type}`) so
//! they can be bound via [`ArrowQuery::param`] without string interpolation.
//! The SQL strings are exposed as constants and as `fn build_*_sql()` helpers
//! so they can be unit-tested for shape correctness even without a live
//! ClickHouse.

use std::collections::{HashMap, HashSet};

use super::parse::{RefKind, Reference};

/// The handful of source_type values from `siphon_routes` we care about when
/// resolving GFM-style project paths.
///
/// Routes also stores `Namespace`, `User`, etc., but those never appear as
/// the *owning project* of a referenced entity, so the resolver filters them
/// out at query time. Embedded as a string literal in [`ROUTES_SQL`] today;
/// the constant is the source-of-truth callers can cross-check against.
#[allow(dead_code)]
pub const ROUTABLE_SOURCE_TYPES: &[&str] = &["Project", "Namespace"];

/// SQL template for the routes-table batch lookup.
///
/// Parameters:
///   `{traversal_path:String}` — namespace scope prefix (e.g. `1/100/`).
///   `{paths:Array(String)}` — full_paths to look up.
///
/// Returns `(source_type, source_id, path, traversal_path)`. The
/// `_siphon_deleted = false` filter prevents resolving against a tombstoned
/// project route.
pub const ROUTES_SQL: &str = "\
SELECT \
    source_type, \
    source_id, \
    path, \
    traversal_path \
FROM siphon_routes \
WHERE _siphon_deleted = false \
  AND startsWith(traversal_path, {traversal_path:String}) \
  AND path IN {paths:Array(String)} \
  AND source_type IN ('Project', 'Namespace')";

/// SQL template for the merge-request entity batch lookup.
///
/// Parameters:
///   `{traversal_path:String}` — namespace scope prefix.
///   `{pairs:Array(Tuple(Int64, Int64))}` — `(target_project_id, iid)` tuples.
///
/// Returns `(id, target_project_id, iid)`.
pub const MERGE_REQUESTS_SQL: &str = "\
SELECT \
    id, \
    target_project_id, \
    iid \
FROM merge_requests \
WHERE _siphon_deleted = false \
  AND startsWith(traversal_path, {traversal_path:String}) \
  AND (target_project_id, iid) IN {pairs:Array(Tuple(Int64, Int64))}";

/// SQL template for the work-item entity batch lookup.
///
/// Parameters identical to [`MERGE_REQUESTS_SQL`] but keyed on `project_id`.
/// Returns `(id, project_id, iid)`.
pub const WORK_ITEMS_SQL: &str = "\
SELECT \
    id, \
    project_id, \
    iid \
FROM work_items \
WHERE _siphon_deleted = false \
  AND startsWith(traversal_path, {traversal_path:String}) \
  AND (project_id, iid) IN {pairs:Array(Tuple(Int64, Int64))}";

/// A row from the `siphon_routes` lookup, keyed by `path`.
///
/// Used in unit tests today; the runtime CH benchmark consumes Arrow
/// `RecordBatch` directly and does not materialize this struct. The
/// production handler will.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRow {
    pub source_type: String,
    pub source_id: i64,
    pub path: String,
    pub traversal_path: String,
}

/// A row from a `(project_id, iid) → id` lookup against a noteable table.
///
/// Used in unit tests today; see [`RouteRow`] for the runtime story.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityRow {
    pub id: i64,
    pub project_id: i64,
    pub iid: i64,
}

/// Output of the resolver: the target entity id plus the `traversal_path`
/// the edge row should land in. The traversal_path comes from the
/// *target's* route, not the source note's; this matters for cross-project
/// MENTIONS because `gl_edge`'s primary key includes traversal_path and
/// querying for inbound MENTIONS reaches them via the target's namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTarget {
    pub id: i64,
    pub traversal_path: String,
}

/// Plan summary: the distinct work the resolver needs to issue against
/// ClickHouse for a given batch. Used both for the actual runtime resolver
/// and for the per-batch metric output.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolutionPlan {
    /// Distinct project paths to look up in `siphon_routes`.
    pub paths: HashSet<String>,
    /// Distinct (project_path, iid) tuples for issues / work-items.
    pub issue_pairs: HashSet<(String, i64)>,
    /// Distinct (project_path, iid) tuples for merge requests.
    pub mr_pairs: HashSet<(String, i64)>,
    /// Number of commit SHAs in the batch (commits don't need a routes/IID
    /// lookup — they're resolved by SHA against a separate commits table or
    /// stored as standalone Commit nodes once we add them).
    pub commit_ref_count: usize,
}

impl ResolutionPlan {
    /// Build a plan from a batch of `(noteable_project_path, references)`
    /// tuples emitted by the parser. The default project path is substituted
    /// when a `Reference` carries no explicit project prefix (same-project
    /// shorthand: `#123`, `!456`).
    pub fn from_refs<'a, I>(refs: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a Reference)>,
    {
        let mut plan = ResolutionPlan::default();
        for (default_project, r) in refs {
            plan.add_ref(r, default_project);
        }
        plan
    }

    /// Add a single reference to the plan. Used by the production handler's
    /// row-by-row loop, where the default project path is computed per-row
    /// from the noteable. Skips empty default projects (the path-IN-list
    /// can't tolerate an empty string against `siphon_routes.path`).
    pub fn add_ref(&mut self, r: &Reference, default_project: &str) {
        let project = r
            .project_path
            .as_deref()
            .unwrap_or(default_project)
            .to_owned();
        let project_is_empty = project.is_empty();
        match r.kind {
            RefKind::Issue => {
                if !project_is_empty {
                    self.paths.insert(project.clone());
                    if let Some(iid) = r.iid {
                        self.issue_pairs.insert((project, iid));
                    }
                }
            }
            RefKind::MergeRequest => {
                if !project_is_empty {
                    self.paths.insert(project.clone());
                    if let Some(iid) = r.iid {
                        self.mr_pairs.insert((project, iid));
                    }
                }
            }
            RefKind::Commit => {
                self.commit_ref_count += 1;
                if !r.project_path.as_deref().unwrap_or("").is_empty() {
                    self.paths.insert(project);
                }
            }
        }
    }
}

/// In-memory join: given routes (path → source_id) and entity rows
/// (project_id, iid → id), return resolved entity IDs keyed back to the
/// original `(project_path, iid)` pairs the parser produced.
///
/// Exercised by unit tests. The runtime CH benchmark currently only counts
/// per-stage rows; the production handler will replace the test harness
/// with this join + an edge writer.
#[allow(dead_code)]
pub fn join_pairs(routes: &[RouteRow], entities: &[EntityRow]) -> HashMap<(String, i64), i64> {
    let path_to_source: HashMap<&str, i64> = routes
        .iter()
        .filter(|r| r.source_type == "Project")
        .map(|r| (r.path.as_str(), r.source_id))
        .collect();
    let pid_iid_to_id: HashMap<(i64, i64), i64> = entities
        .iter()
        .map(|e| ((e.project_id, e.iid), e.id))
        .collect();

    path_to_source
        .iter()
        .flat_map(|(path, &source_id)| {
            pid_iid_to_id
                .iter()
                .filter(move |((pid, _), _)| *pid == source_id)
                .map(move |(&(_, iid), &id)| ((path.to_string(), iid), id))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::handler::system_notes::parse::{Action, extract};

    #[test]
    fn routes_sql_uses_named_parameters() {
        assert!(ROUTES_SQL.contains("{traversal_path:String}"));
        assert!(ROUTES_SQL.contains("{paths:Array(String)}"));
        assert!(ROUTES_SQL.contains("startsWith(traversal_path"));
    }

    #[test]
    fn merge_requests_sql_uses_tuple_in_list() {
        assert!(MERGE_REQUESTS_SQL.contains("{pairs:Array(Tuple(Int64, Int64))}"));
        assert!(MERGE_REQUESTS_SQL.contains("(target_project_id, iid) IN"));
    }

    #[test]
    fn work_items_sql_uses_project_id() {
        assert!(WORK_ITEMS_SQL.contains("{pairs:Array(Tuple(Int64, Int64))}"));
        assert!(WORK_ITEMS_SQL.contains("(project_id, iid) IN"));
    }

    #[test]
    fn plan_collects_distinct_paths_and_pairs() {
        let body1 = "mentioned in gitlab-org/gitlab#123";
        let body2 = "mentioned in gitlab-org/gitlab!42";
        let body3 = "mentioned in gitlab-org/gitlab!42"; // duplicate
        let refs1 = extract(Action::CrossReference, body1);
        let refs2 = extract(Action::CrossReference, body2);
        let refs3 = extract(Action::CrossReference, body3);
        let plan = ResolutionPlan::from_refs(
            refs1
                .iter()
                .chain(refs2.iter())
                .chain(refs3.iter())
                .map(|r| ("default/proj", r)),
        );
        assert_eq!(plan.paths.len(), 1, "deduped to one project path");
        assert_eq!(plan.issue_pairs.len(), 1);
        assert_eq!(plan.mr_pairs.len(), 1, "deduped to one MR pair");
    }

    #[test]
    fn plan_uses_default_project_for_same_project_refs() {
        let refs = extract(Action::CrossReference, "mentioned in #123");
        let plan = ResolutionPlan::from_refs(refs.iter().map(|r| ("my/proj", r)));
        assert!(plan.paths.contains("my/proj"));
        assert!(plan.issue_pairs.contains(&("my/proj".to_string(), 123)));
    }

    #[test]
    fn plan_counts_commits_separately() {
        let refs = extract(Action::CrossReference, "mentioned in 54f7727c");
        let plan = ResolutionPlan::from_refs(refs.iter().map(|r| ("p", r)));
        assert_eq!(plan.commit_ref_count, 1);
        assert!(plan.issue_pairs.is_empty());
        assert!(plan.mr_pairs.is_empty());
    }

    #[test]
    fn join_pairs_returns_resolved_entity_ids() {
        let routes = vec![RouteRow {
            source_type: "Project".to_string(),
            source_id: 999,
            path: "gitlab-org/gitlab".to_string(),
            traversal_path: "1/".to_string(),
        }];
        let entities = vec![EntityRow {
            id: 8675309,
            project_id: 999,
            iid: 42,
        }];
        let resolved = join_pairs(&routes, &entities);
        assert_eq!(
            resolved.get(&("gitlab-org/gitlab".to_string(), 42)),
            Some(&8675309)
        );
    }

    #[test]
    fn join_pairs_ignores_namespace_rows() {
        // Namespace routes are returned by the routes lookup but they're not
        // project owners; the join filters them out.
        let routes = vec![RouteRow {
            source_type: "Namespace".to_string(),
            source_id: 7,
            path: "gitlab-org".to_string(),
            traversal_path: "1/".to_string(),
        }];
        let entities = vec![EntityRow {
            id: 1,
            project_id: 7,
            iid: 99,
        }];
        let resolved = join_pairs(&routes, &entities);
        assert!(resolved.is_empty());
    }
}
