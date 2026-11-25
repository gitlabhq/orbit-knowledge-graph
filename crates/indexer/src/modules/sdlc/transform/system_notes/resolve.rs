use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use super::parse::{RefKind, Reference};
use orbit_utils::traversal_path::TraversalPath;

fn version() -> &'static str {
    ontology::siphon_version_column()
}

fn del() -> &'static str {
    ontology::siphon_deleted_column()
}

/// The clickhouse crate serializes params into the URL query string, so these
/// lookup arrays must stay bounded independently of Arrow block size.
pub(super) fn lookup_chunks<T>(items: &[T], batch_size: usize) -> impl Iterator<Item = &[T]> {
    items.chunks(batch_size.max(1))
}

pub static ROUTES_SQL: LazyLock<String> = LazyLock::new(|| {
    let (version, del) = (version(), del());
    format!(
        "SELECT source_id, path, traversal_path \
         FROM (SELECT id, source_id, path, \
         argMax(traversal_path, {version}) AS traversal_path, \
         argMax({del}, {version}) AS {del} \
         FROM siphon_routes \
         WHERE startsWith(siphon_routes.traversal_path, {{root_prefix:String}}) \
         AND path IN {{paths:Array(String)}} \
         AND source_type = 'Project' \
         GROUP BY id, source_id, path) \
         WHERE {del} = false"
    )
});

pub static PROJECT_PATHS_SQL: LazyLock<String> = LazyLock::new(|| {
    let (version, del) = (version(), del());
    format!(
        "SELECT source_id, path \
         FROM (SELECT id, source_id, path, \
         argMax({del}, {version}) AS {del} \
         FROM siphon_routes \
         WHERE startsWith(traversal_path, {{root_prefix:String}}) \
         AND source_type = 'Project' \
         AND source_id IN {{source_ids:Array(Int64)}} \
         GROUP BY id, source_id, path) \
         WHERE {del} = false"
    )
});

/// Batch `(target_project_id, iid)` -> MR id lookup. Same `argMax` dedup and
/// `root_prefix` bounding as [`ROUTES_SQL`].
///
/// `{project_ids:Array(Int64)}` and `{iids:Array(Int64)}` are passed as two
/// index-aligned flat arrays and `arrayZip`-ed server-side, because the JSON
/// param channel serializes a tuple as `[200,5]`, which ClickHouse rejects for
/// `Array(Tuple(Int64, Int64))`. Returns `(id, target_project_id, iid)`.
pub static MERGE_REQUESTS_SQL: LazyLock<String> = LazyLock::new(|| {
    let (version, del) = (version(), del());
    format!(
        "SELECT id, target_project_id, iid \
         FROM (SELECT id, target_project_id, iid, \
         argMax({del}, {version}) AS {del} \
         FROM merge_requests \
         WHERE startsWith(traversal_path, {{root_prefix:String}}) \
         AND (target_project_id, iid) IN arrayZip({{project_ids:Array(Int64)}}, {{iids:Array(Int64)}}) \
         GROUP BY id, target_project_id, iid) \
         WHERE {del} = false"
    )
});

pub static WORK_ITEMS_SQL: LazyLock<String> = LazyLock::new(|| {
    let (version, del) = (version(), del());
    format!(
        "SELECT id, project_id, iid \
         FROM (SELECT id, project_id, iid, \
         argMax({del}, {version}) AS {del} \
         FROM work_items \
         WHERE startsWith(traversal_path, {{root_prefix:String}}) \
         AND (project_id, iid) IN arrayZip({{project_ids:Array(Int64)}}, {{iids:Array(Int64)}}) \
         GROUP BY id, project_id, iid) \
         WHERE {del} = false"
    )
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRow {
    pub source_id: i64,
    pub path: String,
    pub traversal_path: TraversalPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityRow {
    pub id: i64,
    pub project_id: i64,
    pub iid: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTarget {
    pub id: i64,
    pub traversal_path: TraversalPath,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolutionPlan {
    pub paths: HashSet<String>,
    pub issue_pairs: HashSet<(String, i64)>,
    pub mr_pairs: HashSet<(String, i64)>,
}

impl ResolutionPlan {
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
                let _ = project_is_empty;
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolvedIndex {
    by_key: HashMap<(RefKind, i64), HashMap<String, ResolvedTarget>>,
}

impl ResolvedIndex {
    pub fn build(
        routes: &[RouteRow],
        mr_entities: &[EntityRow],
        wi_entities: &[EntityRow],
    ) -> Self {
        let path_routes: HashMap<i64, (&str, &TraversalPath)> = routes
            .iter()
            .map(|r| (r.source_id, (r.path.as_str(), &r.traversal_path)))
            .collect();

        let mut by_key = HashMap::new();
        for (kind, entities) in [
            (RefKind::MergeRequest, mr_entities),
            (RefKind::Issue, wi_entities),
        ] {
            for e in entities {
                if let Some(&(path, traversal_path)) = path_routes.get(&e.project_id) {
                    by_key
                        .entry((kind, e.iid))
                        .or_insert_with(HashMap::new)
                        .insert(
                            path.to_string(),
                            ResolvedTarget {
                                id: e.id,
                                traversal_path: traversal_path.clone(),
                            },
                        );
                }
            }
        }
        Self { by_key }
    }

    pub fn resolve(&self, r: &Reference, default_project: &str) -> Option<ResolvedTarget> {
        if r.kind == RefKind::Commit {
            return None;
        }
        let iid = r.iid?;
        let project = r.project_path.as_deref().unwrap_or(default_project);
        if project.is_empty() {
            return None;
        }
        self.by_key.get(&(r.kind, iid))?.get(project).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::transform::system_notes::parse::{Action, extract};

    const TEST_RESOLVE_LOOKUP_BATCH_SIZE: usize = 1_000;

    #[test]
    fn routes_sql_uses_named_parameters() {
        assert!(ROUTES_SQL.contains("{paths:Array(String)}"));
        assert!(ROUTES_SQL.contains("path IN"));
    }

    #[test]
    fn lookup_chunks_bounds_param_array_size() {
        let empty: Vec<i32> = Vec::new();
        assert_eq!(
            lookup_chunks(&empty, TEST_RESOLVE_LOOKUP_BATCH_SIZE).count(),
            0
        );

        let values: Vec<_> = (0..TEST_RESOLVE_LOOKUP_BATCH_SIZE).collect();
        let chunk_sizes: Vec<_> = lookup_chunks(&values, TEST_RESOLVE_LOOKUP_BATCH_SIZE)
            .map(<[_]>::len)
            .collect();
        assert_eq!(chunk_sizes, vec![TEST_RESOLVE_LOOKUP_BATCH_SIZE]);

        let values: Vec<_> = (0..TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1).collect();
        let chunk_sizes: Vec<_> = lookup_chunks(&values, TEST_RESOLVE_LOOKUP_BATCH_SIZE)
            .map(<[_]>::len)
            .collect();
        assert_eq!(chunk_sizes, vec![TEST_RESOLVE_LOOKUP_BATCH_SIZE, 1]);

        let chunk_sizes: Vec<_> = lookup_chunks(&values, 0).map(<[_]>::len).collect();
        assert_eq!(chunk_sizes, vec![1; TEST_RESOLVE_LOOKUP_BATCH_SIZE + 1]);
    }

    #[test]
    fn resolver_sql_is_bounded_to_source_top_level_namespace() {
        for sql in [&*PROJECT_PATHS_SQL, &*MERGE_REQUESTS_SQL, &*WORK_ITEMS_SQL] {
            assert!(
                sql.contains("startsWith(traversal_path, {root_prefix:String})"),
                "resolver query must be bounded by the source top-level prefix: {sql}"
            );
        }
        assert!(
            ROUTES_SQL.contains("startsWith(siphon_routes.traversal_path, {root_prefix:String})"),
            "routes query must bound on the raw column: {}",
            *ROUTES_SQL
        );
    }

    #[test]
    fn resolver_sql_dedups_replacing_merge_tree_by_pg_pkey() {
        for sql in [
            &*ROUTES_SQL,
            &*PROJECT_PATHS_SQL,
            &*MERGE_REQUESTS_SQL,
            &*WORK_ITEMS_SQL,
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
        assert!(
            ROUTES_SQL.contains("argMax(traversal_path, _siphon_replicated_at)"),
            "routes must take the latest traversal_path so a stale 0/ can't win"
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
        let body3 = "mentioned in gitlab-org/gitlab!42";
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
            traversal_path: TraversalPath::new_unchecked("1/999/"),
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
        let r = Reference {
            kind: RefKind::Issue,
            project_path: None,
            iid: Some(7),
            commit_sha: None,
        };
        assert_eq!(index.resolve(&r, "gitlab-org/gitlab").unwrap().id, 555);
        assert!(index.resolve(&r, "").is_none());
    }

    #[test]
    fn resolved_index_separates_mr_and_work_item_iid_namespaces() {
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
