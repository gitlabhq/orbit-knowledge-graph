//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(p1) OR startsWith(p2) OR ...`
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

use serde_json::Value;

use crate::ast::{Expr, Node, Query, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN, global_tables};
use crate::error::Result;
pub use crate::types::SecurityContext;
use ontology::Ontology;
use orbit_utils::traversal_path::{TraversalPath, TraversalPathTrie};

/// Matches `gl_*` or `v{N}_gl_*`, captures the unprefixed name.
static GL_TABLE_RE: OnceLock<Regex> = OnceLock::new();

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

/// A denormalized join carries one `traversal_path` per scoped table in its
/// chain, so a scan of it gets one filter per column (each at its own table's
/// role floor); any other table has exactly one. The query's resolved scope
/// prefix applies only to the unprefixed column, which is the one the prefix
/// was resolved for.
fn apply_to_query(q: &mut Query, ctx: &SecurityContext, ontology: &Ontology) -> Result<()> {
    let aliased_tables = collect_aliased_tables(&q.from);
    if !aliased_tables.is_empty() {
        let security_conds = aliased_tables.iter().flat_map(|(alias, table)| {
            let scopable = ontology.is_table_path_scopable(table);
            let scope_prefix = ctx.scope_prefixes.get(alias);
            ontology
                .traversal_path_columns(table)
                .into_iter()
                .map(move |(column, min_role)| {
                    let eligible = ctx.paths_at_least(
                        min_role.unwrap_or(crate::types::DEFAULT_PATH_ACCESS_LEVEL),
                    );
                    // Inject the resolved scope prefix as the alias's authorization
                    // filter when it sits within an eligible path; otherwise the
                    // broad path set.
                    match scope_prefix {
                        Some(prefix)
                            if scopable
                                && column == TRAVERSAL_PATH_COLUMN
                                && eligible.iter().any(|p| prefix.is_descendant_of(p)) =>
                        {
                            starts_with_expr(alias, &column, prefix.as_str())
                        }
                        Some(prefix) if scopable && column == TRAVERSAL_PATH_COLUMN => Expr::and(
                            build_path_filter(alias, &column, &eligible),
                            starts_with_expr(alias, &column, prefix.as_str()),
                        ),
                        _ => build_path_filter(alias, &column, &eligible),
                    }
                })
        });
        q.where_clause = Expr::and_all(
            security_conds
                .map(Some)
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    apply_security_to_from(&mut q.from, ctx, ontology)?;

    if let Some(where_clause) = &mut q.where_clause {
        apply_security_to_expr(where_clause, ctx, ontology)?;
    }

    for arm in &mut q.union_all {
        apply_to_query(arm, ctx, ontology)?;
    }

    Ok(())
}

fn apply_security_to_expr(
    expr: &mut Expr,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    match expr {
        Expr::InSelect { query, .. } => apply_to_query(query, ctx, ontology),
        Expr::BinaryOp { left, right, .. } => {
            apply_security_to_expr(left, ctx, ontology)?;
            apply_security_to_expr(right, ctx, ontology)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Lambda { body: expr, .. }
        | Expr::InSubquery { expr, .. } => apply_security_to_expr(expr, ctx, ontology),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                apply_security_to_expr(arg, ctx, ontology)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}

fn build_path_filter(alias: &str, column: &str, paths: &[&TraversalPath]) -> Expr {
    match paths.len() {
        0 => Expr::Literal(Value::Bool(false)),
        1 => starts_with_expr(alias, column, paths[0].as_str()),
        _ => {
            let collapsed = TraversalPathTrie::from_paths(paths).to_minimal_prefixes();
            if collapsed.len() == 1 {
                return starts_with_expr(alias, column, collapsed[0].as_str());
            }
            path_or_filter(alias, column, &collapsed)
        }
    }
}

fn starts_with_expr(alias: &str, column: &str, path: &str) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, column), Expr::string(path)],
    )
}

/// OR chain of `startsWith(alias.traversal_path, path)` for each path.
///
/// Each `startsWith` is visible to ClickHouse's PK index analyser, enabling
/// granule pruning per path prefix. This matters inside `dedup_edge_scan`
/// FINAL subqueries: PK range pruning reduces the scan from the entire LCP
/// namespace to only the user's authorized paths.
fn path_or_filter(alias: &str, column: &str, paths: &[TraversalPath]) -> Expr {
    let mut iter = paths
        .iter()
        .map(|p| starts_with_expr(alias, column, p.as_str()));
    let first = iter.next().expect("paths is non-empty (caller checks)");
    iter.fold(first, |a, b| Expr::binary(crate::ast::Op::Or, a, b))
}

#[cfg(test)]
fn collect_node_aliases(table_ref: &TableRef) -> Vec<String> {
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

    // Global hubs (User, Runner) are non-namespaced; names are unprefixed.
    !global_tables().iter().any(|t| t == unprefixed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthorizedPath;
    use crate::ast::{JoinType, Op, SelectExpr};
    use ontology::constants::EDGE_TABLE;
    use orbit_utils::traversal_path::TraversalPath;
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
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());

        // Cross-org paths are allowed (user's home org != path org)
        assert!(SecurityContext::new(1, vec!["42/".into()]).is_ok());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_ok());

        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err());
    }

    #[test]
    fn single_path_uses_starts_with() {
        let expr = build_path_filter(
            "u",
            TRAVERSAL_PATH_COLUMN,
            &[&TraversalPath::from("42/43/")],
        );
        assert!(matches!(expr, Expr::FuncCall { name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths_use_or_of_starts_with_without_common_prefix() {
        let expr = build_path_filter(
            "u",
            TRAVERSAL_PATH_COLUMN,
            &[
                &TraversalPath::from("1/2/4/"),
                &TraversalPath::from("1/2/5/"),
            ],
        );
        assert!(matches!(expr, Expr::BinaryOp { op: Op::Or, .. }));
        let mut paths = starts_with_paths_for_alias(&expr, "u");
        paths.sort();
        assert_eq!(paths, vec!["1/2/4/".to_string(), "1/2/5/".to_string()]);
    }

    #[test]
    fn many_paths_uses_or_chain() {
        let paths: Vec<TraversalPath> = (0..200u64)
            .map(|i| TraversalPath::from(format!("1/{i}/")))
            .collect();
        let refs: Vec<&TraversalPath> = paths.iter().collect();
        let expr = build_path_filter("e", TRAVERSAL_PATH_COLUMN, &refs);
        let dbg = format!("{expr:?}");
        assert!(
            !dbg.contains("arrayExists"),
            "large path sets should use OR chain, not arrayExists: {dbg}"
        );
        assert!(
            dbg.contains("startsWith"),
            "should produce startsWith predicates: {dbg}"
        );
    }

    #[test]
    fn empty_paths_produces_false_literal() {
        let expr = build_path_filter("v", TRAVERSAL_PATH_COLUMN, &[]);
        // Literal false guarantees zero rows for this alias. Using a literal
        // (not a parameterized Bool) lets ClickHouse constant-fold it at plan
        // time, avoiding full edge scans on denied entities.
        assert!(matches!(expr, Expr::Literal(Value::Bool(false))));
    }

    #[test]
    fn paths_at_least_keeps_matching_roles() {
        let sc = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new("1/100/", 20),
                AuthorizedPath::new("1/101/", 30),
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
                vec![AuthorizedPath::with_access_levels("1/100/", vec![])]
            )
            .is_err()
        );
    }

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
            Expr::InSubquery { expr, .. } | Expr::InSelect { expr, .. } => {
                collect_starts_with_paths(expr, alias, paths)
            }
            Expr::Lambda { body, .. } => collect_starts_with_paths(body, alias, paths),
            Expr::Identifier(_)
            | Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Param { .. }
            | Expr::Star => {}
        }
    }

    fn find_in_select_query(expr: &Expr) -> Option<&Query> {
        match expr {
            Expr::InSelect { query, .. } => Some(query),
            Expr::BinaryOp { left, right, .. } => {
                find_in_select_query(left).or_else(|| find_in_select_query(right))
            }
            _ => None,
        }
    }

    #[test]
    fn in_select_subquery_in_where_receives_path_filter() {
        let ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/200/".into()]).unwrap();
        let ontology = Ontology::new().with_nodes(["Project"]);

        let anchor = Query {
            select: vec![SelectExpr::col("e0p", "source_id")],
            from: TableRef::scan(EDGE_TABLE, "e0p"),
            where_clause: Some(Expr::eq(
                Expr::col("e0p", "relationship_kind"),
                Expr::string("IN_PROJECT"),
            )),
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::col("e0", "source_id")],
            from: TableRef::scan(EDGE_TABLE, "e0"),
            where_clause: Some(Expr::InSelect {
                expr: Box::new(Expr::col("e0", "target_id")),
                query: Box::new(anchor),
            }),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let anchor = find_in_select_query(q.where_clause.as_ref().unwrap())
            .expect("InSelect subquery should survive the security pass");
        let paths = starts_with_paths_for_alias(anchor.where_clause.as_ref().unwrap(), "e0p");
        assert!(
            paths.contains(&"1/100/".to_string()) && paths.contains(&"1/200/".to_string()),
            "anchor subquery must carry the caller's full path set, got {paths:?}"
        );
    }

    // Paths tagged at Developer (30) still qualify because 30 >= the
    // Security Manager floor (25).
    #[test]
    fn per_entity_role_scoping_filters_vulnerability_alias() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new("1/100/", 20), // Reporter
                AuthorizedPath::new("1/101/", 30), // Developer (covers SM)
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
        assert!(
            where_sql.contains("1/100/"),
            "Project alias must retain Reporter path '1/100/', got: {where_sql}"
        );
        assert!(
            where_sql.contains("1/101/"),
            "Project alias must retain Developer path '1/101/', got: {where_sql}"
        );
        assert_eq!(
            starts_with_paths_for_alias(q.where_clause.as_ref().unwrap(), "v"),
            vec!["1/101/".to_string()],
            "Vulnerability alias 'v' must only keep the higher-role path, got: {where_sql}"
        );
    }

    // Bool(false) for the protected alias is the predicate that closes the
    // aggregation-query oracle (see module docs).
    #[test]
    fn no_eligible_paths_compile_to_bool_false() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/100/", 20)], // Reporter only
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
    fn trie_collapse_after_role_filtering() {
        use crate::types::AuthorizedPath;
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new(String::from("1/100/"), 20),
                AuthorizedPath::new(String::from("1/100/200/"), 20),
                AuthorizedPath::new(String::from("1/100/200/"), 30),
                AuthorizedPath::new(String::from("1/300/"), 30),
            ],
        )
        .unwrap();

        let eligible = ctx.paths_at_least(20);
        assert_eq!(eligible.len(), 4);

        let collapsed = TraversalPathTrie::from_paths(&eligible).to_minimal_prefixes();
        assert_eq!(collapsed, vec!["1/100/", "1/300/"]);

        let filter = build_path_filter("t", TRAVERSAL_PATH_COLUMN, &eligible);
        let sql = format!("{filter:?}");
        assert!(
            sql.contains("startsWith"),
            "should produce startsWith predicates: {sql}"
        );
    }

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
        assert!(!should_apply_security_filter("path_cte"));
        assert!(!should_apply_security_filter("some_cte"));
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
    fn multi_path_authz_omits_redundant_common_prefix() {
        let ctx = SecurityContext::new(1, vec!["1/9970/".into(), "1/6543/".into()]).unwrap();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_edge", "e"),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let mut got = starts_with_paths_for_alias(q.where_clause.as_ref().unwrap(), "e");
        got.sort();
        assert_eq!(
            got,
            vec!["1/6543/".to_string(), "1/9970/".to_string()],
            "multi-path authz must be the OR of real prefixes with no redundant broad LCP, got:\n{got:?}"
        );
    }

    #[test]
    fn scope_prefix_replaces_broad_on_scoped_alias() {
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("p".to_string(), TraversalPath::new_unchecked("1/24/23/"));
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes(prefixes);

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_work_item", "wi"),
                Expr::eq(Expr::col("p", "id"), Expr::col("wi", "project_id")),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        let ontology = Ontology::new().with_path_scopable_nodes(["Project", "WorkItem"]);
        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_clause = q.where_clause.as_ref().unwrap();
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "p"),
            vec!["1/24/23/".to_string()],
            "scoped alias is injected with the tight prefix as its only auth filter"
        );
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "wi"),
            vec!["1/".to_string()],
            "unscoped alias gets the broad authz set"
        );
    }

    #[test]
    fn scope_prefix_below_role_floor_keeps_broad() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("v".to_string(), TraversalPath::new_unchecked("1/100/200/"));
        let ctx = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/100/", 20)])
            .unwrap()
            .with_scope_prefixes(prefixes);

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
        assert!(
            where_sql.contains("Bool") && where_sql.contains("false"),
            "a prefix below the entity role floor must keep the role-filtered (dead) broad filter: {where_sql}"
        );
    }

    #[test]
    fn scope_prefix_dropped_on_non_path_scopable_alias() {
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("g".to_string(), TraversalPath::new_unchecked("1/24/23/"));
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes(prefixes);

        let ontology = Ontology::new().with_nodes(["Global"]);

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("g", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_global", "g"),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_clause = q.where_clause.as_ref().unwrap();
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "g"),
            vec!["1/".to_string()],
            "non-path-scopable alias must drop scope_prefix and keep broad authz only"
        );
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
