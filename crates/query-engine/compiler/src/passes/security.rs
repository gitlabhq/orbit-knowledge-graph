//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(LCP) AND arrayExists(p -> startsWith(path, p), paths)`
//!
//! # Per-entity role scoping
//!
//! Each entity's ontology can declare a `required_role`. Before injecting
//! the `startsWith` predicate for an alias we look up the entity attached
//! to that alias's physical table and drop any traversal path where the
//! user's access level is below the entity's `required_access_level`.
//!
//! This closes the aggregation-query oracle where a Reporter-only user
//! could count or binary-search properties on a higher-privilege entity
//! (e.g. Vulnerability) by pairing a Project `group_by` with a Vulnerability
//! target. Now the target entity's scan is filtered down to zero paths
//! (producing a Bool(false) predicate) and the aggregation counts nothing.

use std::sync::OnceLock;

use regex::Regex;

use crate::ast::{ChType, Expr, Node, Query, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN, skip_security_filter_tables};
use crate::error::Result;
pub use crate::types::SecurityContext;
use ontology::Ontology;

/// Matches `gl_*` or `v{N}_gl_*`, captures the unprefixed name.
static GL_TABLE_RE: OnceLock<Regex> = OnceLock::new();

/// Inject security filters into an AST node (mutates in place).
///
/// Per-alias role floors come from `ontology.min_access_level_for_table`;
/// tables without a `redaction` block keep the historical Reporter floor.
pub fn apply_security_context(
    node: &mut Node,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    // An entirely empty security context is treated as a fail-closed bug:
    // the caller forgot to populate traversal paths. Emitting `Bool(false)`
    // here would silently return empty results, which is indistinguishable
    // from "user has no namespaces" and obscures the root cause. Note that
    // this differs from a role-mismatch empty-path set for a specific
    // alias — in that case the user has paths, just none at the required
    // role, and returning zero rows for the protected entity is the
    // intended behavior.
    if ctx.traversal_paths.is_empty() {
        return Err(crate::error::QueryError::Security(
            "security context has no traversal_path entries; refusing to compile \
             because every gl_* alias would fall back to Bool(false) and hide \
             the underlying auth misconfiguration"
                .into(),
        ));
    }
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                apply_to_query(&mut cte.query, ctx, ontology)?;
            }
            apply_to_query(q, ctx, ontology)
        }
        Node::Insert(_) => Ok(()),
    }
}

fn apply_to_query(q: &mut Query, ctx: &SecurityContext, ontology: &Ontology) -> Result<()> {
    let aliased_tables = collect_aliased_tables(&q.from);
    if !aliased_tables.is_empty() {
        let security_conds = aliased_tables.iter().map(|(alias, table)| {
            let min_role = ontology
                .min_access_level_for_table(table)
                .unwrap_or(crate::types::DEFAULT_PATH_ACCESS_LEVEL);
            let eligible_paths = ctx.paths_at_least(min_role);
            build_path_filter(alias, &eligible_paths)
        });
        q.where_clause = Expr::and_all(
            security_conds
                .map(Some)
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    // Recurse into derived tables (UNION ALL arms, subqueries) in FROM
    apply_security_to_from(&mut q.from, ctx, ontology)?;

    // Recurse into top-level UNION ALL arms
    for arm in &mut q.union_all {
        apply_to_query(arm, ctx, ontology)?;
    }

    Ok(())
}

fn build_path_filter(alias: &str, paths: &[&str]) -> Expr {
    match paths.len() {
        0 => Expr::param(ChType::Bool, false),
        1 => starts_with_expr(alias, paths[0]),
        _ => {
            let collapsed = PathTrie::from_paths(paths).to_minimal_prefixes();
            if collapsed.len() == 1 {
                return starts_with_expr(alias, &collapsed[0]);
            }
            let lcp = lowest_common_prefix(&collapsed);
            let lcp_filter = starts_with_expr(alias, &lcp);
            let collapsed_refs: Vec<&str> = collapsed.iter().map(String::as_str).collect();
            Expr::and(lcp_filter, path_array_filter(alias, &collapsed_refs))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PathTrie — segment-level trie for collapsing traversal paths
// ─────────────────────────────────────────────────────────────────────────────

/// A trie keyed on path segments (`"1"`, `"100"`, …). Each node tracks
/// whether it was explicitly inserted (i.e., the user has access to that
/// exact namespace prefix). Inserting `"1/100/"` marks the `1 → 100` node
/// as terminal.
#[derive(Default)]
struct PathTrie {
    children: std::collections::BTreeMap<String, PathTrie>,
    terminal: bool,
}

impl PathTrie {
    fn from_paths(paths: &[&str]) -> Self {
        let mut root = Self::default();
        for path in paths {
            root.insert(path);
        }
        root
    }

    fn insert(&mut self, path: &str) {
        let segments: Vec<&str> = path
            .trim_end_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        // Empty paths are impossible: SecurityContext::validate_traversal_path
        // enforces ^(\d+/)+$. Guard here to prevent the root node from being
        // marked terminal, which would emit "" and match everything.
        debug_assert!(
            !segments.is_empty(),
            "PathTrie::insert called with empty path"
        );
        if segments.is_empty() {
            return;
        }
        let mut node = self;
        for seg in segments {
            node = node.children.entry(seg.to_string()).or_default();
        }
        node.terminal = true;
    }

    /// Walk the trie and emit the minimal set of prefixes. A terminal
    /// node emits its path and prunes all descendants (subsumption).
    /// A non-terminal node with exactly one child merges into that
    /// child (prefix compression).
    fn to_minimal_prefixes(&self) -> Vec<String> {
        let mut result = Vec::new();
        self.collect(&mut String::new(), &mut result);
        result
    }

    fn collect(&self, prefix: &mut String, out: &mut Vec<String>) {
        if self.terminal {
            // This node is authorized — emit the prefix, skip children.
            let mut p = prefix.clone();
            if !p.is_empty() {
                p.push('/');
            }
            out.push(p);
            return;
        }

        for (seg, child) in &self.children {
            let restore_len = prefix.len();
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            child.collect(prefix, out);
            prefix.truncate(restore_len);
        }
    }
}

/// Find the lowest common path prefix across a set of paths.
fn lowest_common_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let segments: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| p.trim_end_matches('/').split('/').collect())
        .collect();
    let first = &segments[0];
    let common_len = (0..first.len())
        .take_while(|&i| segments.iter().all(|s| s.get(i) == first.get(i)))
        .count();
    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
}

fn starts_with_expr(alias: &str, path: &str) -> Expr {
    starts_with_value_expr(alias, Expr::string(path))
}

fn starts_with_value_expr(alias: &str, path: Expr) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), path],
    )
}

fn path_array_filter(alias: &str, paths: &[&str]) -> Expr {
    let lambda_param = "_gkg_path";
    Expr::func(
        "arrayExists",
        vec![
            Expr::lambda(
                lambda_param,
                starts_with_value_expr(alias, Expr::ident(lambda_param)),
            ),
            Expr::param(
                ChType::String.to_array(),
                serde_json::Value::Array(
                    paths
                        .iter()
                        .map(|path| serde_json::Value::String((*path).to_string()))
                        .collect(),
                ),
            ),
        ],
    )
}

pub(crate) fn collect_node_aliases(table_ref: &TableRef) -> Vec<String> {
    collect_aliased_tables(table_ref)
        .into_iter()
        .map(|(a, _)| a)
        .collect()
}

/// Collect `(alias, table)` pairs for every scan that should receive a
/// security filter. Returning the table lets the caller pick a per-entity
/// minimum role before building the `startsWith(...)` predicate.
pub(crate) fn collect_aliased_tables(table_ref: &TableRef) -> Vec<(String, String)> {
    match table_ref {
        TableRef::Scan { table, alias, .. } if should_apply_security_filter(table) => {
            vec![(alias.clone(), table.clone())]
        }
        TableRef::Scan { .. } => vec![],
        TableRef::Join { left, right, .. } => {
            let mut aliases = collect_aliased_tables(left);
            aliases.extend(collect_aliased_tables(right));
            aliases
        }
        // Derived tables don't have traversal_path columns themselves.
        // Their arms get security filters via apply_security_to_from.
        TableRef::Union { .. } | TableRef::Subquery { .. } => vec![],
    }
}

/// Recurse into derived tables (UNION ALL arms, subqueries) inside a FROM
/// clause and apply security filters to each arm's query.
fn apply_security_to_from(
    table_ref: &mut TableRef,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    match table_ref {
        TableRef::Union { queries, .. } => {
            for arm in queries {
                apply_to_query(arm, ctx, ontology)?;
            }
        }
        TableRef::Subquery { query, .. } => {
            apply_to_query(query, ctx, ontology)?;
        }
        TableRef::Join { left, right, .. } => {
            apply_security_to_from(left, ctx, ontology)?;
            apply_security_to_from(right, ctx, ontology)?;
        }
        TableRef::Scan { .. } => {}
    }
    Ok(())
}

/// Determines if a table should have traversal path security filters applied.
///
/// Handles both unprefixed (`gl_user`) and schema-version-prefixed
/// (`v1_gl_user`) table names. CTEs like `path_cte` are excluded.
fn should_apply_security_filter(table: &str) -> bool {
    let re = GL_TABLE_RE.get_or_init(|| {
        Regex::new(&format!(
            r"^(?:v\d+_)?({}.+)$",
            regex::escape(GL_TABLE_PREFIX)
        ))
        .expect("valid regex")
    });

    let unprefixed = match re.captures(table).and_then(|c| c.get(1)) {
        Some(m) => m.as_str(),
        None => return false,
    };

    // The skip list uses unprefixed names from the embedded ontology.
    !skip_security_filter_tables()
        .iter()
        .any(|t| t == unprefixed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TraversalPath;
    use crate::ast::{JoinType, Op, SelectExpr};
    use ontology::constants::EDGE_TABLE;
    use serde_json::Value;

    fn simple_query() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: None,
            limit: Some(10),
            ..Default::default()
        }))
    }

    #[test]
    fn traversal_path_validation() {
        // Valid paths
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());

        // Cross-org paths are allowed (user's home org != path org)
        assert!(SecurityContext::new(1, vec!["42/".into()]).is_ok());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_ok());

        // Invalid: format errors
        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err()); // missing trailing slash
        assert!(SecurityContext::new(1, vec!["".into()]).is_err()); // empty
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err()); // non-numeric
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err()); // mixed
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err()); // exceeds i64
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err()); // negative
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Path filter generation tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn single_path_uses_starts_with() {
        let expr = build_path_filter("u", &["42/43/"]);
        assert!(matches!(expr, Expr::FuncCall { name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths_uses_prefix_and_or_starts_with() {
        let expr = build_path_filter("u", &["1/2/4/", "1/2/5/"]);
        // Should be: startsWith(..., '1/2/') AND (startsWith(..., '1/2/4/') OR startsWith(..., '1/2/5/'))
        assert!(matches!(expr, Expr::BinaryOp { op: Op::And, .. }));
    }

    #[test]
    fn empty_paths_produces_false_literal() {
        let expr = build_path_filter("v", &[]);
        // Bool(false) guarantees zero rows for this alias without breaking the
        // overall query structure.
        assert!(matches!(
            expr,
            Expr::Param {
                data_type: ChType::Bool,
                ..
            }
        ));
    }

    // ── Per-entity role scoping ─────────────────

    /// Paths at or above the required role pass through unfiltered.
    #[test]
    fn paths_at_least_keeps_matching_roles() {
        let sc = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new("1/100/", 20),
                TraversalPath::new("1/101/", 30),
            ],
        )
        .unwrap();
        assert_eq!(sc.paths_at_least(20), vec!["1/100/", "1/101/"]);
        assert_eq!(sc.paths_at_least(30), vec!["1/101/"]);
        assert!(sc.paths_at_least(50).is_empty());
    }

    #[test]
    fn empty_access_levels_are_invalid() {
        assert!(
            SecurityContext::new_with_roles(
                1,
                vec![TraversalPath::with_access_levels("1/100/", vec![])]
            )
            .is_err()
        );
    }

    /// Build a minimal ontology where `Vulnerability` requires
    /// Security Manager. Used by the per-entity role scoping tests.
    fn ontology_with_sm_vulnerability() -> Ontology {
        Ontology::new()
            .with_nodes(["Project", "Vulnerability"])
            .with_redaction("Vulnerability", "vulnerabilities", "id")
            .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager)
    }

    fn starts_with_paths_for_alias(expr: &Expr, alias: &str) -> Vec<String> {
        let mut paths = Vec::new();
        collect_starts_with_paths(expr, alias, &mut paths);
        paths
    }

    fn collect_starts_with_paths(expr: &Expr, alias: &str, paths: &mut Vec<String>) {
        match expr {
            Expr::FuncCall { name, args } if name == "startsWith" && args.len() == 2 => {
                if let (
                    Expr::Column { table, column },
                    Expr::Param {
                        value: Value::String(path),
                        ..
                    },
                ) = (&args[0], &args[1])
                    && table == alias
                    && column == TRAVERSAL_PATH_COLUMN
                {
                    paths.push(path.clone());
                }

                for arg in args {
                    collect_starts_with_paths(arg, alias, paths);
                }
            }
            Expr::FuncCall { args, .. } => {
                for arg in args {
                    collect_starts_with_paths(arg, alias, paths);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                collect_starts_with_paths(left, alias, paths);
                collect_starts_with_paths(right, alias, paths);
            }
            Expr::UnaryOp { expr, .. } => collect_starts_with_paths(expr, alias, paths),
            Expr::InSubquery { expr, .. } => collect_starts_with_paths(expr, alias, paths),
            Expr::Lambda { body, .. } => collect_starts_with_paths(body, alias, paths),
            Expr::Identifier(_)
            | Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Param { .. }
            | Expr::Star => {}
        }
    }

    /// apply_security_context with a table requiring Security Manager drops
    /// any Reporter-only paths from that alias's filter while leaving other
    /// aliases untouched. Paths tagged at Developer (30) still qualify
    /// because 30 >= 25.
    #[test]
    fn per_entity_role_scoping_filters_vulnerability_alias() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new("1/100/", 20), // Reporter
                TraversalPath::new("1/101/", 30), // Developer (covers SM)
            ],
        )
        .unwrap();

        let ontology = ontology_with_sm_vulnerability();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_vulnerability", "v"),
                Expr::eq(Expr::col("p", "id"), Expr::col("v", "project_id")),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_sql = format!("{:?}", q.where_clause);
        // Project alias keeps both paths.
        assert!(
            where_sql.contains("1/100/"),
            "Project alias must retain Reporter path '1/100/', got: {where_sql}"
        );
        assert!(
            where_sql.contains("1/101/"),
            "Project alias must retain Developer path '1/101/', got: {where_sql}"
        );
        // Vulnerability alias is present in the join, but its path filter
        // must only reference paths where the user clears the Security
        // Manager floor.
        assert_eq!(
            starts_with_paths_for_alias(q.where_clause.as_ref().unwrap(), "v"),
            vec!["1/101/".to_string()],
            "Vulnerability alias 'v' must only keep the higher-role path, got: {where_sql}"
        );
    }

    /// When a user holds only Reporter paths and the table requires
    /// Security Manager, build_path_filter receives an empty slice and emits
    /// Bool(false). This is the predicate that closes the
    /// aggregation-query oracle.
    #[test]
    fn no_eligible_paths_compile_to_bool_false() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![TraversalPath::new("1/100/", 20)], // Reporter only
        )
        .unwrap();

        let ontology = ontology_with_sm_vulnerability();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_vulnerability", "v"),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_sql = format!("{:?}", q.where_clause);
        // A startsWith can't appear for the Vulnerability alias because
        // the eligible-path list was empty; build_path_filter bound
        // Bool(false) instead.
        assert!(
            !where_sql.contains("1/100/"),
            "no traversal path should be bound for Vulnerability, got: {where_sql}"
        );
        assert!(
            where_sql.contains("Bool") && where_sql.contains("false"),
            "where clause should compile to Bool(false) for empty path set, got: {where_sql}"
        );
    }

    #[test]
    fn lowest_common_prefix_finds_shared_path() {
        assert_eq!(
            lowest_common_prefix(&["1/2/4/".into(), "1/2/5/".into()]),
            "1/2/"
        );
        assert_eq!(lowest_common_prefix(&["1/2/".into(), "1/3/".into()]), "1/");
        assert_eq!(lowest_common_prefix(&["1/".into(), "2/".into()]), "");
        assert_eq!(lowest_common_prefix(&["42/".into()]), "42/");
    }

    #[test]
    fn path_trie_subsumes_children() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/200/", "1/100/201/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_keeps_siblings() {
        let t = PathTrie::from_paths(&["1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_siblings_under_shared_parent() {
        // Three children under 1/100/ — trie keeps all three since
        // the parent 1/100/ is not itself authorized.
        let t = PathTrie::from_paths(&["1/100/200/", "1/100/201/", "1/100/202/", "1/200/300/"]);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 4);
        assert!(result.contains(&"1/200/300/".to_string()));
    }

    #[test]
    fn path_trie_single_path() {
        let t = PathTrie::from_paths(&["1/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_deduplicates() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_deep_subsumption() {
        let t = PathTrie::from_paths(&["1/", "1/100/", "1/100/200/", "1/100/200/300/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/"]);
    }

    #[test]
    fn path_trie_mixed_orgs() {
        let t = PathTrie::from_paths(&["1/100/", "2/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "2/100/"]);
    }

    #[test]
    fn path_trie_realistic_38_paths() {
        // Simulate a user with access to 38 groups, 30 under 1/10/
        // and 8 scattered elsewhere. The trie should keep all 38
        // (no subsumption since no parent path is authorized), but
        // the LCP in build_path_filter will be "1/" which is correct.
        let mut paths: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        paths.extend((200..208).map(|i| format!("1/{i}/")));
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let t = PathTrie::from_paths(&refs);
        let result = t.to_minimal_prefixes();
        // No subsumption possible — all are leaf groups
        assert_eq!(result.len(), 38);
    }

    #[test]
    fn path_trie_parent_collapses_many_children() {
        // User has access to parent group 1/10/ plus individual
        // subgroups — parent subsumes everything underneath.
        let mut paths = vec!["1/10/"];
        let children: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        let refs: Vec<&str> = children.iter().map(|s| s.as_str()).collect();
        paths.extend(refs);
        let t = PathTrie::from_paths(&paths);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/10/"]);
    }

    #[test]
    #[should_panic(expected = "empty path")]
    fn path_trie_empty_path_panics_in_debug() {
        // Empty paths are impossible (SecurityContext validates ^(\d+/)+$).
        // The debug_assert catches misuse during development.
        PathTrie::from_paths(&[""]);
    }

    #[test]
    fn trie_collapse_after_role_filtering() {
        // Simulate paths_at_least merging paths from different role buckets.
        // User has:
        //   - Reporter on 1/100/ and 1/100/200/ (parent + child)
        //   - Developer on 1/100/200/ and 1/300/
        //
        // For a Reporter-floor entity, paths_at_least returns all four paths
        // (both Reporter and Developer qualify). The trie should collapse
        // 1/100/ + 1/100/200/ → 1/100/ (subsumption), keeping 1/300/.
        use crate::types::TraversalPath;
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new(String::from("1/100/"), 20), // Reporter
                TraversalPath::new(String::from("1/100/200/"), 20), // Reporter
                TraversalPath::new(String::from("1/100/200/"), 30), // Developer
                TraversalPath::new(String::from("1/300/"), 30), // Developer
            ],
        )
        .unwrap();

        // Reporter-floor entity (level 20): all paths qualify
        let eligible = ctx.paths_at_least(20);
        assert_eq!(eligible.len(), 4);

        // Trie collapses 1/100/ + 1/100/200/ → 1/100/
        let collapsed = PathTrie::from_paths(&eligible).to_minimal_prefixes();
        assert_eq!(collapsed, vec!["1/100/", "1/300/"]);

        // build_path_filter produces the correct SQL shape
        let filter = build_path_filter("t", &eligible);
        let sql = format!("{filter:?}");
        // LCP is "1/" wrapping two startsWith arms
        assert!(
            sql.contains("startsWith"),
            "should produce startsWith predicates: {sql}"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Security injection tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn inject_adds_security_to_simple_query() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = simple_query();
        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_filters_edge_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan(EDGE_TABLE, "e"),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_includes_edge_table() {
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_project", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "id"), Expr::col("e", "source")),
        );

        let aliases = collect_node_aliases(&from);
        assert_eq!(aliases, vec!["p", "e"]);
    }

    #[test]
    fn inject_skips_user_table() {
        // User visibility is determined through MEMBER_OF, not traversal path
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_user", "u"),
            TableRef::scan("gl_merge_request", "mr"),
            Expr::lit(true),
        );

        let aliases = collect_node_aliases(&from);
        // Should only include mr, not u (gl_user is skipped)
        assert_eq!(aliases, vec!["mr"]);
    }

    #[test]
    fn should_apply_security_filter_skips_user() {
        assert!(!should_apply_security_filter("gl_user"));
        assert!(should_apply_security_filter(EDGE_TABLE));
        assert!(should_apply_security_filter("gl_project"));
        assert!(should_apply_security_filter("gl_merge_request"));
    }

    #[test]
    fn should_apply_security_filter_skips_ctes() {
        // CTEs like path_cte don't have traversal_path column
        assert!(!should_apply_security_filter("path_cte"));
        assert!(!should_apply_security_filter("some_cte"));
        // Only gl_ prefixed tables should have security filters
        assert!(!should_apply_security_filter("nodes"));
    }

    #[test]
    fn union_aliases_are_not_collected() {
        let from = TableRef::union_all(
            vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("e", "source_id"),
                    alias: None,
                }],
                from: TableRef::scan(EDGE_TABLE, "e"),
                ..Default::default()
            }],
            "hop_e0",
        );
        let aliases = collect_node_aliases(&from);
        assert!(aliases.is_empty());
    }

    #[test]
    fn inject_recurses_into_union_from_arms() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("outer_e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "outer_e"),
                TableRef::union_all(
                    vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("e1", "source_id"),
                            alias: None,
                        }],
                        from: TableRef::scan(EDGE_TABLE, "e1"),
                        where_clause: None,
                        ..Default::default()
                    }],
                    "hop_e0",
                ),
                Expr::lit(true),
            ),
            where_clause: None,
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_some(),
            "outer query should have security filter on outer_e"
        );

        // The union arm scanning gl_edge should also get a filter
        if let TableRef::Join { right, .. } = &q.from {
            if let TableRef::Union { queries, .. } = right.as_ref() {
                assert!(
                    queries[0].where_clause.is_some(),
                    "UNION ALL arm should have security filter applied"
                );
            } else {
                panic!("expected Union");
            }
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn inject_recurses_into_union_all_arms() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            where_clause: None,
            union_all: vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: None,
                }],
                from: TableRef::scan("gl_project", "p"),
                where_clause: None,
                ..Default::default()
            }],
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_some(),
            "base query should have security filter"
        );
        assert_eq!(q.union_all.len(), 1);
        assert!(
            q.union_all[0].where_clause.is_some(),
            "UNION ALL arm should have security filter"
        );
    }
}
