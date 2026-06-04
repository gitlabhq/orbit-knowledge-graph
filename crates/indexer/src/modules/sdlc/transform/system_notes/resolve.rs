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

/// SQL template for the routes-table batch lookup.
///
/// Parameters:
///   `{paths:Array(String)}` — full_paths to look up.
///
/// Returns `(source_id, path, traversal_path)`. Only `Project` routes are
/// returned: a referenced entity's *owning route* is always a project, and
/// both consumers (`pairs_with_project_id`, `ResolvedIndex::build`) drop
/// non-project rows anyway, so filtering `source_type = 'Project'` at the
/// query keeps `Namespace`/`User`/etc. rows off the wire entirely.
///
/// `siphon_routes` is a `ReplacingMergeTree` whose sort key *includes*
/// `traversal_path`. The traversal-path reconciler re-inserts a row with the
/// reconciled `traversal_path` rather than updating in place, so a route's
/// stale (`0/`) row and reconciled (`1/22/94/`) row have **different sort
/// keys** and never collapse under `FINAL`. Deduplicating by the stable PG
/// primary key (`id`) with `argMax(..., _siphon_replicated_at)` is therefore
/// both correct and cheaper than `FINAL`; it mirrors the SDLC entity ETL
/// (`plan/input.rs`). Without it a cross-project reference can resolve to a
/// stale `0/` route and the edge lands in the wrong namespace partition.
/// `_siphon_deleted` is taken from the latest version and filtered after the
/// aggregation so a live route isn't dropped by an older tombstone (or kept
/// by a stale live row).
///
/// **Bounded to the source note's top-level namespace.** `siphon_routes` is
/// `ORDER BY (traversal_path, source_type, source_id, id)`, so the
/// `path`/`source_id` filters alone can't prune the primary index and the
/// lookup degrades to a full scan of the shared Siphon datalake. The
/// `startsWith(traversal_path, {root_prefix:String})` leg restricts the scan
/// to the source's top-level namespace partition (`{org}/{top_level_ns}/`),
/// turning the full scan into a primary-index range scan. The trade-off is
/// that v1 resolves only **same-top-level-namespace** references; a
/// cross-top-level reference (`other-group/proj#5`) lives outside the prefix
/// and is silently not resolved (under-counts, never a wrong edge). See ADR
/// 013 "Coverage and known limitations" — cross-top-level resolution is
/// deferred to the graph-DB dictionary lever, which also resolves the
/// cross-namespace edge-visibility (authz) question.
pub const ROUTES_SQL: &str = "\
SELECT \
    source_id, \
    path, \
    traversal_path \
FROM ( \
    SELECT \
        id, \
        source_id, \
        path, \
        argMax(traversal_path, _siphon_replicated_at) AS traversal_path, \
        argMax(_siphon_deleted, _siphon_replicated_at) AS _siphon_deleted \
    FROM siphon_routes \
    WHERE startsWith(siphon_routes.traversal_path, {root_prefix:String}) \
      AND path IN {paths:Array(String)} \
      AND source_type = 'Project' \
    GROUP BY id, source_id, path \
) \
WHERE _siphon_deleted = false";

/// SQL template for the reverse routes lookup: project `source_id` → path.
///
/// Used to turn each source note's owning `project_id` into the default
/// project path for unqualified GFM references on that row. Keyed on
/// `source_id` (the project id) rather than `path`, so it complements
/// [`ROUTES_SQL`] (which is keyed on `path`).
///
/// Deduplicated by PG primary key the same way as [`ROUTES_SQL`] — see that
/// constant for why `FINAL` is insufficient here. Bounded to the source's
/// top-level namespace by `startsWith(traversal_path, {root_prefix:String})`
/// for the same primary-index-pruning reason; the source notes' own projects
/// always live within their top-level namespace, so this leg never drops a
/// row we need.
///
/// Parameters:
///   `{root_prefix:String}` — the source top-level namespace prefix.
///   `{source_ids:Array(Int64)}` — project ids to resolve.
///
/// Returns `(source_id, path)`.
pub const PROJECT_PATHS_SQL: &str = "\
SELECT \
    source_id, \
    path \
FROM ( \
    SELECT \
        id, \
        source_id, \
        path, \
        argMax(_siphon_deleted, _siphon_replicated_at) AS _siphon_deleted \
    FROM siphon_routes \
    WHERE startsWith(traversal_path, {root_prefix:String}) \
      AND source_type = 'Project' \
      AND source_id IN {source_ids:Array(Int64)} \
    GROUP BY id, source_id, path \
) \
WHERE _siphon_deleted = false";

/// SQL template for the merge-request entity batch lookup.
///
/// Parameters:
///   `{project_ids:Array(Int64)}` + `{iids:Array(Int64)}` — parallel arrays
///   of `(target_project_id, iid)` to look up, zipped server-side into the
///   tuple IN-list. Two `Array(Int64)` params are used instead of a single
///   `Array(Tuple(...))` because the JSON parameter channel serializes a
///   tuple as a nested array (`[200,5]`), which ClickHouse rejects for
///   `Array(Tuple(Int64, Int64))`; `arrayZip` rebuilds the tuples from the
///   two flat arrays. The arrays must be the same length and index-aligned.
///
/// **Bounded to the source note's top-level namespace.** `merge_requests` is
/// `ORDER BY (traversal_path, id)`, so a `(target_project_id, iid)` filter
/// can't prune the primary index — without a `traversal_path` leg this is a
/// full scan of one of the largest tables in the shared Siphon datalake. The
/// `startsWith(traversal_path, {root_prefix:String})` leg restricts it to the
/// source's top-level namespace partition (primary-index range scan). v1 thus
/// resolves only same-top-level-namespace MR references; cross-top-level
/// targets are deferred (see [`ROUTES_SQL`] and ADR 013).
///
/// `merge_requests` is a `ReplacingMergeTree`, so it is deduplicated by PG
/// primary key (`id`) with `argMax(..., _siphon_replicated_at)` — same
/// rationale as [`ROUTES_SQL`]: avoid returning a stale row version (and
/// avoid `FINAL`'s full-merge cost on a large table).
///
/// Returns `(id, target_project_id, iid)`.
pub const MERGE_REQUESTS_SQL: &str = "\
SELECT \
    id, \
    target_project_id, \
    iid \
FROM ( \
    SELECT \
        id, \
        target_project_id, \
        iid, \
        argMax(_siphon_deleted, _siphon_replicated_at) AS _siphon_deleted \
    FROM merge_requests \
    WHERE startsWith(traversal_path, {root_prefix:String}) \
      AND (target_project_id, iid) IN arrayZip({project_ids:Array(Int64)}, {iids:Array(Int64)}) \
    GROUP BY id, target_project_id, iid \
) \
WHERE _siphon_deleted = false";

/// SQL template for the work-item entity batch lookup.
///
/// Parameters identical to [`MERGE_REQUESTS_SQL`] but keyed on `project_id`.
/// Deduplicated by PG primary key the same way, and bounded to the source's
/// top-level namespace by `startsWith(traversal_path, {root_prefix:String})`
/// for the same primary-index-pruning reason (`work_items` is
/// `ORDER BY (traversal_path, id)`). Returns `(id, project_id, iid)`.
pub const WORK_ITEMS_SQL: &str = "\
SELECT \
    id, \
    project_id, \
    iid \
FROM ( \
    SELECT \
        id, \
        project_id, \
        iid, \
        argMax(_siphon_deleted, _siphon_replicated_at) AS _siphon_deleted \
    FROM work_items \
    WHERE startsWith(traversal_path, {root_prefix:String}) \
      AND (project_id, iid) IN arrayZip({project_ids:Array(Int64)}, {iids:Array(Int64)}) \
    GROUP BY id, project_id, iid \
) \
WHERE _siphon_deleted = false";

/// A row from the `siphon_routes` lookup, keyed by `path`. Decoded from the
/// `ROUTES_SQL` result in `handler::query_routes`. `ROUTES_SQL` already
/// filters `source_type = 'Project'`, so every row here is a project route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRow {
    pub source_id: i64,
    pub path: String,
    pub traversal_path: String,
}

/// A row from a `(project_id, iid) → id` lookup against a noteable table.
/// Decoded from `MERGE_REQUESTS_SQL` / `WORK_ITEMS_SQL` in
/// `handler::query_entities`.
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
}

impl ResolutionPlan {
    /// Build a plan from a batch of `(noteable_project_path, references)`
    /// tuples emitted by the parser. The default project path is substituted
    /// when a `Reference` carries no explicit project prefix (same-project
    /// shorthand: `#123`, `!456`). The handler builds plans row-by-row via
    /// [`ResolutionPlan::add_ref`]; this batch constructor backs the tests.
    #[cfg(test)]
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
                // Commit references resolve to nothing today (no `Commit`
                // node type yet, see ADR 013), so they add no routes/IID
                // lookup work: `ResolvedIndex::resolve` returns `None` for
                // `RefKind::Commit` and `emit::build_edges` drops them. The
                // `project_is_empty` binding is unused on this branch but
                // kept for the shared match shape.
                let _ = project_is_empty;
            }
        }
    }
}

/// Runtime resolver index built from one routes lookup plus the per-kind
/// noteable lookups (`MERGE_REQUESTS_SQL`, `WORK_ITEMS_SQL`). Keyed on
/// `(project_path, kind, iid)` so the [`build_edges`] closure can resolve a
/// parsed [`Reference`] — substituting the note's default project path for
/// same-project shorthand — without re-querying ClickHouse per reference.
///
/// The target's `traversal_path` comes from the route row, so cross-project
/// MENTIONS land in the *target's* namespace partition (matching the
/// `gl_edge` primary key), which is what an inbound-edge query expects.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolvedIndex {
    by_key: HashMap<(String, RefKind, i64), ResolvedTarget>,
}

impl ResolvedIndex {
    /// Build the index from the routes rows and the two noteable lookups.
    /// `mr_entities` come from `MERGE_REQUESTS_SQL`, `wi_entities` from
    /// `WORK_ITEMS_SQL`; both are `(id, project_id, iid)` rows. Commit
    /// references are not indexed here (no `Commit` node yet).
    pub fn build(
        routes: &[RouteRow],
        mr_entities: &[EntityRow],
        wi_entities: &[EntityRow],
    ) -> Self {
        // `ROUTES_SQL` already restricts to `source_type = 'Project'`.
        let path_routes: HashMap<i64, (&str, &str)> = routes
            .iter()
            .map(|r| (r.source_id, (r.path.as_str(), r.traversal_path.as_str())))
            .collect();

        let mut by_key = HashMap::new();
        for (kind, entities) in [
            (RefKind::MergeRequest, mr_entities),
            (RefKind::Issue, wi_entities),
        ] {
            for e in entities {
                if let Some(&(path, traversal_path)) = path_routes.get(&e.project_id) {
                    by_key.insert(
                        (path.to_string(), kind, e.iid),
                        ResolvedTarget {
                            id: e.id,
                            traversal_path: traversal_path.to_string(),
                        },
                    );
                }
            }
        }
        Self { by_key }
    }

    /// Resolve a single parsed reference, substituting `default_project` for
    /// same-project shorthand (`#123` / `!456`). Commit references and
    /// references with no `iid` (or an unknown project/pair) return `None`.
    pub fn resolve(&self, r: &Reference, default_project: &str) -> Option<ResolvedTarget> {
        if r.kind == RefKind::Commit {
            return None;
        }
        let iid = r.iid?;
        let project = r.project_path.as_deref().unwrap_or(default_project);
        if project.is_empty() {
            return None;
        }
        self.by_key
            .get(&(project.to_string(), r.kind, iid))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::transform::system_notes::parse::{Action, extract};

    #[test]
    fn routes_sql_uses_named_parameters() {
        assert!(ROUTES_SQL.contains("{paths:Array(String)}"));
        assert!(ROUTES_SQL.contains("path IN"));
    }

    #[test]
    fn resolver_sql_is_bounded_to_source_top_level_namespace() {
        // v1 resolves only within the source note's top-level namespace: each
        // resolver query carries a `startsWith(traversal_path, {root_prefix})`
        // leg so the leading-PK `traversal_path` column prunes the scan to one
        // top-level namespace partition instead of scanning the whole shared
        // Siphon datalake table. Cross-top-level references fall outside the
        // prefix and are intentionally not resolved (deferred — see ADR 013).
        //
        // ROUTES_SQL aliases `argMax(traversal_path, …) AS traversal_path`, so
        // its bound must reference the raw `siphon_routes.traversal_path`
        // column to avoid `ILLEGAL_AGGREGATION`; the other three don't project
        // `traversal_path`, so the bare column is unambiguous.
        for sql in [PROJECT_PATHS_SQL, MERGE_REQUESTS_SQL, WORK_ITEMS_SQL] {
            assert!(
                sql.contains("startsWith(traversal_path, {root_prefix:String})"),
                "resolver query must be bounded by the source top-level prefix: {sql}"
            );
        }
        assert!(
            ROUTES_SQL.contains("startsWith(siphon_routes.traversal_path, {root_prefix:String})"),
            "routes query must bound on the raw column to avoid ILLEGAL_AGGREGATION: {ROUTES_SQL}"
        );
    }

    #[test]
    fn resolver_sql_dedups_replacing_merge_tree_by_pg_pkey() {
        // These read ReplacingMergeTree siphon tables whose stale and
        // reconciled rows can coexist (and, for routes, never collapse under
        // FINAL because traversal_path is in the sort key). Each must
        // deduplicate by the PG primary key with argMax, picking the latest
        // replicated version, and filter `_siphon_deleted` after the
        // aggregation. Regression guard for the cross-project `0/` bug.
        for sql in [
            ROUTES_SQL,
            PROJECT_PATHS_SQL,
            MERGE_REQUESTS_SQL,
            WORK_ITEMS_SQL,
        ] {
            assert!(sql.contains("GROUP BY id"), "missing GROUP BY id in: {sql}");
            assert!(
                sql.contains("argMax(_siphon_deleted, _siphon_replicated_at)"),
                "missing latest-version _siphon_deleted in: {sql}"
            );
            assert!(
                sql.trim_end().ends_with("WHERE _siphon_deleted = false"),
                "deleted filter must run after the argMax aggregation in: {sql}"
            );
        }
        // Routes' volatile reconcile column must take the latest version so
        // a stale `0/` row can't win over the reconciled traversal_path.
        assert!(
            ROUTES_SQL.contains("argMax(traversal_path, _siphon_replicated_at)"),
            "routes must take the latest traversal_path"
        );
    }

    #[test]
    fn merge_requests_sql_uses_tuple_in_list() {
        assert!(MERGE_REQUESTS_SQL.contains("{project_ids:Array(Int64)}"));
        assert!(MERGE_REQUESTS_SQL.contains("{iids:Array(Int64)}"));
        assert!(MERGE_REQUESTS_SQL.contains("(target_project_id, iid) IN arrayZip("));
    }

    #[test]
    fn routes_sql_filters_to_project_routes_at_query_time() {
        // The only routable owner of a referenced entity is a project, and
        // both consumers drop non-project rows, so the query filters
        // `source_type = 'Project'` rather than fetching every route kind and
        // discarding in Rust.
        assert!(ROUTES_SQL.contains("source_type = 'Project'"));
        assert!(
            !ROUTES_SQL.contains("'Namespace'"),
            "non-project routes must not be fetched"
        );
    }

    #[test]
    fn work_items_sql_uses_project_id() {
        assert!(WORK_ITEMS_SQL.contains("{project_ids:Array(Int64)}"));
        assert!(WORK_ITEMS_SQL.contains("{iids:Array(Int64)}"));
        assert!(WORK_ITEMS_SQL.contains("(project_id, iid) IN arrayZip("));
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
    fn plan_ignores_commit_refs() {
        // Commit references resolve to nothing today (no `Commit` node yet),
        // so they add no routes/IID lookup work — not even the default
        // project path, since no edge can ever come out of them.
        let refs = extract(Action::CrossReference, "mentioned in 54f7727c");
        let plan = ResolutionPlan::from_refs(refs.iter().map(|r| ("my/proj", r)));
        assert!(plan.paths.is_empty());
        assert!(plan.issue_pairs.is_empty());
        assert!(plan.mr_pairs.is_empty());
    }

    fn gitlab_route() -> RouteRow {
        RouteRow {
            source_id: 999,
            path: "gitlab-org/gitlab".to_string(),
            traversal_path: "1/999/".to_string(),
        }
    }

    #[test]
    fn resolved_index_resolves_cross_project_mr_to_target_traversal_path() {
        let index = ResolvedIndex::build(
            &[gitlab_route()],
            &[EntityRow {
                id: 8675309,
                project_id: 999,
                iid: 42,
            }],
            &[],
        );
        let r = Reference {
            kind: RefKind::MergeRequest,
            project_path: Some("gitlab-org/gitlab".to_string()),
            iid: Some(42),
            commit_sha: None,
        };
        let resolved = index.resolve(&r, "other/proj").unwrap();
        assert_eq!(resolved.id, 8675309);
        assert_eq!(
            resolved.traversal_path, "1/999/",
            "edge lands in the target's namespace partition"
        );
    }

    #[test]
    fn resolved_index_uses_default_project_for_same_project_ref() {
        let index = ResolvedIndex::build(
            &[gitlab_route()],
            &[],
            &[EntityRow {
                id: 555,
                project_id: 999,
                iid: 7,
            }],
        );
        // `#7` with no explicit project resolves against the default.
        let r = Reference {
            kind: RefKind::Issue,
            project_path: None,
            iid: Some(7),
            commit_sha: None,
        };
        assert_eq!(index.resolve(&r, "gitlab-org/gitlab").unwrap().id, 555);
        // Empty default project never resolves.
        assert!(index.resolve(&r, "").is_none());
    }

    #[test]
    fn resolved_index_separates_mr_and_work_item_iid_namespaces() {
        // An MR and a work item can share the same (project, iid); the index
        // must key on kind so `!7` and `#7` don't collide.
        let index = ResolvedIndex::build(
            &[gitlab_route()],
            &[EntityRow {
                id: 100,
                project_id: 999,
                iid: 7,
            }],
            &[EntityRow {
                id: 200,
                project_id: 999,
                iid: 7,
            }],
        );
        let mr = Reference {
            kind: RefKind::MergeRequest,
            project_path: Some("gitlab-org/gitlab".to_string()),
            iid: Some(7),
            commit_sha: None,
        };
        let issue = Reference {
            kind: RefKind::Issue,
            project_path: Some("gitlab-org/gitlab".to_string()),
            iid: Some(7),
            commit_sha: None,
        };
        assert_eq!(index.resolve(&mr, "").unwrap().id, 100);
        assert_eq!(index.resolve(&issue, "").unwrap().id, 200);
    }

    #[test]
    fn resolved_index_returns_none_for_commit_refs() {
        let index = ResolvedIndex::build(&[gitlab_route()], &[], &[]);
        let r = Reference {
            kind: RefKind::Commit,
            project_path: Some("gitlab-org/gitlab".to_string()),
            iid: None,
            commit_sha: Some("54f7727c".to_string()),
        };
        assert!(index.resolve(&r, "").is_none());
    }
}
