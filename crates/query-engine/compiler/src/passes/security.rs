use std::sync::OnceLock;

use regex::Regex;

use serde_json::Value;

use crate::ast::{ChType, Cte, Expr, Node, Query, SelectExpr, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN, global_tables};
use crate::error::Result;
pub use crate::types::SecurityContext;
use crate::types::TokenScope;
use ontology::Ontology;
use ontology::constants::{
    SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};
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
            let mut has_edges = false;
            apply_to_query(q, ctx, ontology, &mut has_edges)?;
            if has_edges && ctx.token_scopes.is_some() {
                let mut cte = token_nodes_cte(ctx, ontology);
                let scope = ctx.clone().with_scope_prefixes(Default::default());
                apply_to_query(&mut cte.query, &scope, ontology, &mut false)?;
                q.ctes.insert(0, cte);
            }
            Ok(())
        }
        Node::Insert(_) => Ok(()),
    }
}

fn apply_to_query(
    q: &mut Query,
    ctx: &SecurityContext,
    ontology: &Ontology,
    has_edges: &mut bool,
) -> Result<()> {
    for cte in &mut q.ctes {
        apply_to_query(&mut cte.query, ctx, ontology, has_edges)?;
    }
    let aliased_tables = collect_aliased_tables(&q.from);
    if ctx.token_scopes.is_none() {
        q.where_clause = Expr::and_all(
            aliased_tables
                .iter()
                .map(|(alias, table)| Some(role_filter(alias, table, ctx, ontology)))
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    apply_security_to_from(&mut q.from, ctx, ontology, has_edges)?;

    if let Some(where_clause) = &mut q.where_clause {
        apply_security_to_expr(where_clause, ctx, ontology, has_edges)?;
    }

    for expr in q
        .select
        .iter_mut()
        .map(|select| &mut select.expr)
        .chain(q.group_by.iter_mut())
        .chain(q.order_by.iter_mut().map(|order| &mut order.expr))
        .chain(q.having.iter_mut())
    {
        apply_security_to_expr(expr, ctx, ontology, has_edges)?;
    }
    for arm in &mut q.union_all {
        apply_to_query(arm, ctx, ontology, has_edges)?;
    }

    Ok(())
}

fn apply_security_to_expr(
    expr: &mut Expr,
    ctx: &SecurityContext,
    ontology: &Ontology,
    has_edges: &mut bool,
) -> Result<()> {
    match expr {
        Expr::InSelect { query, .. } => apply_to_query(query, ctx, ontology, has_edges),
        Expr::BinaryOp { left, right, .. } => {
            apply_security_to_expr(left, ctx, ontology, has_edges)?;
            apply_security_to_expr(right, ctx, ontology, has_edges)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Lambda { body: expr, .. }
        | Expr::InSubquery { expr, .. } => apply_security_to_expr(expr, ctx, ontology, has_edges),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                apply_security_to_expr(arg, ctx, ontology, has_edges)?;
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

pub fn token_scope_filter(alias: &str, scope: Option<&TokenScope>) -> Expr {
    match scope {
        Some(TokenScope::All) => Expr::Literal(Value::Bool(true)),
        Some(TokenScope::Namespaces(paths)) => Expr::col_in(
            alias,
            TRAVERSAL_PATH_COLUMN,
            ChType::String,
            paths.iter().map(|p| Value::String(p.to_string())).collect(),
        )
        .unwrap_or(Expr::Literal(Value::Bool(false))),
        Some(TokenScope::Resources(ids)) => {
            crate::passes::shared::id_list_predicate(alias, "id", ids)
        }
        None | Some(TokenScope::Denied) => Expr::Literal(Value::Bool(false)),
    }
}

fn role_filter(alias: &str, table: &str, ctx: &SecurityContext, ontology: &Ontology) -> Expr {
    let min_role = ontology
        .min_access_level_for_table(table)
        .unwrap_or(crate::types::DEFAULT_PATH_ACCESS_LEVEL);
    let eligible = ctx.paths_at_least(min_role);
    match ctx.scope_prefixes.get(alias) {
        Some(prefix)
            if ontology.is_table_path_scopable(table)
                && eligible.iter().any(|p| prefix.is_descendant_of(p)) =>
        {
            starts_with_expr(alias, prefix.as_str())
        }
        Some(prefix) if ontology.is_table_path_scopable(table) => Expr::and(
            build_path_filter(alias, &eligible),
            starts_with_expr(alias, prefix.as_str()),
        ),
        _ => build_path_filter(alias, &eligible),
    }
}

fn token_nodes_cte(ctx: &SecurityContext, ontology: &Ontology) -> Cte {
    let mut arms = ontology
        .nodes()
        .filter(|node| {
            ctx.token_scopes.as_ref().is_some_and(|scopes| {
                scopes.get(&node.name).is_some_and(
                    |scope| matches!(scope, TokenScope::Namespaces(paths) if !paths.is_empty()),
                )
            })
        })
        .map(|node| Query {
            select: vec![
                SelectExpr::new(Expr::string(&node.name), "kind"),
                SelectExpr::col("n", "id"),
            ],
            from: TableRef::Scan {
                table: node.destination_table.clone(),
                alias: "n".into(),
                final_: true,
            },
            where_clause: Some(Expr::eq(
                Expr::col("n", "_deleted"),
                Expr::Literal(Value::Bool(false)),
            )),
            ..Default::default()
        });
    let mut query = arms.next().unwrap_or_else(|| Query {
        select: vec![
            SelectExpr::new(Expr::string(""), "kind"),
            SelectExpr::new(Expr::int(0), "id"),
        ],
        from: TableRef::Scan {
            table: "system.one".into(),
            alias: "n".into(),
            final_: false,
        },
        where_clause: Some(Expr::Literal(Value::Bool(false))),
        ..Default::default()
    });
    query.union_all.extend(arms);
    let mut cte = Cte::new("_token_nodes", query);
    cte.materialized = true;
    cte
}

fn endpoint_filter(alias: &str, kind_column: &str, id_column: &str, ctx: &SecurityContext) -> Expr {
    let membership = Expr::InSelect {
        expr: Box::new(Expr::func(
            "tuple",
            vec![Expr::col(alias, kind_column), Expr::col(alias, id_column)],
        )),
        query: Box::new(Query {
            select: vec![SelectExpr::col("t", "kind"), SelectExpr::col("t", "id")],
            from: TableRef::Scan {
                table: "_token_nodes".into(),
                alias: "t".into(),
                final_: false,
            },
            ..Default::default()
        }),
    };
    ctx.token_scopes
        .iter()
        .flat_map(|scopes| scopes.iter())
        .filter_map(|(entity, scope)| {
            let filter = match scope {
                TokenScope::All => Expr::Literal(Value::Bool(true)),
                TokenScope::Resources(ids) => {
                    crate::passes::shared::id_list_predicate(alias, id_column, ids)
                }
                _ => return None,
            };
            Some(Expr::and(
                Expr::eq(Expr::col(alias, kind_column), Expr::string(entity)),
                filter,
            ))
        })
        .fold(membership, |combined, filter| {
            Expr::binary(crate::ast::Op::Or, combined, filter)
        })
}

fn build_path_filter(alias: &str, paths: &[&TraversalPath]) -> Expr {
    match paths.len() {
        0 => Expr::Literal(Value::Bool(false)),
        1 => starts_with_expr(alias, paths[0].as_str()),
        _ => {
            let collapsed = TraversalPathTrie::from_paths(paths).to_minimal_prefixes();
            if collapsed.len() == 1 {
                return starts_with_expr(alias, collapsed[0].as_str());
            }
            path_or_filter(alias, &collapsed)
        }
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

/// OR chain of `startsWith(alias.traversal_path, path)` for each path.
///
/// Each `startsWith` is visible to ClickHouse's PK index analyser, enabling
/// granule pruning per path prefix. This matters inside `dedup_edge_scan`
/// FINAL subqueries: PK range pruning reduces the scan from the entire LCP
/// namespace to only the user's authorized paths.
fn path_or_filter(alias: &str, paths: &[TraversalPath]) -> Expr {
    let mut iter = paths.iter().map(|p| starts_with_expr(alias, p.as_str()));
    let first = iter.next().expect("paths is non-empty (caller checks)");
    iter.fold(first, |a, b| Expr::binary(crate::ast::Op::Or, a, b))
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

fn apply_security_to_from(
    table_ref: &mut TableRef,
    ctx: &SecurityContext,
    ontology: &Ontology,
    has_edges: &mut bool,
) -> Result<()> {
    match table_ref {
        TableRef::Union { queries, .. } => {
            for arm in queries {
                apply_to_query(arm, ctx, ontology, has_edges)?;
            }
        }
        TableRef::Subquery { query, .. } => apply_to_query(query, ctx, ontology, has_edges)?,
        TableRef::Join {
            left, right, on, ..
        } => {
            apply_security_to_from(left, ctx, ontology, has_edges)?;
            apply_security_to_from(right, ctx, ontology, has_edges)?;
            apply_security_to_expr(on, ctx, ontology, has_edges)?;
        }
        TableRef::Scan { table, alias, .. } => {
            let Some(scopes) = &ctx.token_scopes else {
                return Ok(());
            };
            let token_filter = if let Some(node) = ontology.node_for_table(table) {
                token_scope_filter(alias, scopes.get(&node.name))
            } else if ontology.is_edge_table(table) {
                *has_edges = true;
                Expr::and(
                    endpoint_filter(alias, SOURCE_KIND_COLUMN, SOURCE_ID_COLUMN, ctx),
                    endpoint_filter(alias, TARGET_KIND_COLUMN, TARGET_ID_COLUMN, ctx),
                )
            } else {
                return Ok(());
            };
            let predicate = if should_apply_security_filter(table) {
                Expr::and(role_filter(alias, table, ctx, ontology), token_filter)
            } else {
                token_filter
            };
            *table_ref = TableRef::Subquery {
                alias: alias.clone(),
                query: Box::new(Query {
                    select: vec![SelectExpr::star()],
                    from: table_ref.clone(),
                    where_clause: Some(predicate),
                    ..Default::default()
                }),
            };
        }
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
    fn token_denial_stays_inside_optional_input() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        ctx.token_scopes = Some(std::collections::HashMap::from([
            (
                "Project".into(),
                TokenScope::Namespaces(vec!["1/10/".into()].into()),
            ),
            ("WorkItem".into(), TokenScope::Denied),
        ]));
        let scan = |table: &str, alias: &str| TableRef::Scan {
            table: table.into(),
            alias: alias.into(),
            final_: true,
        };
        let mut ast = Node::Query(Box::new(Query {
            select: vec![SelectExpr::col("p", "id")],
            from: TableRef::Join {
                join_type: crate::ast::JoinType::Left,
                left: Box::new(scan("gl_project", "p")),
                right: Box::new(scan("gl_work_item", "w")),
                on: Expr::eq(Expr::col("p", "id"), Expr::col("w", "project_id")),
            },
            ..Default::default()
        }));
        apply_security_context(&mut ast, &ctx, &ontology).unwrap();
        let Node::Query(query) = ast else { panic!() };
        assert!(query.where_clause.is_none());
        let TableRef::Join { right, .. } = query.from else {
            panic!()
        };
        let TableRef::Subquery { query, .. } = *right else {
            panic!()
        };
        assert!(format!("{:?}", query.where_clause).contains("Bool(false)"));
    }

    #[test]
    fn token_namespace_grants_are_exact_and_operation_specific() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        ctx.token_scopes = Some(std::collections::HashMap::from([(
            "Project".into(),
            TokenScope::Namespaces(vec!["1/10/".into()].into()),
        )]));
        let json = r#"{"query_type":"aggregation","nodes":[{"id":"p","entity":"Project","node_ids":[1]}],"aggregations":[{"count":"p","as":"total"}]}"#;
        let compiled = crate::compile(json, &ontology, &ctx).unwrap();
        assert!(
            compiled.base.sql.contains("traversal_path ="),
            "{}",
            compiled.base.sql
        );
        assert!(!compiled.base.sql.contains("_token_nodes"));
        let code = r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1]}],"limit":10}"#;
        let compiled = crate::compile(code, &ontology, &ctx).unwrap();
        assert!(
            compiled.base.render().contains("false"),
            "{}",
            compiled.base.sql
        );
    }

    #[test]
    fn membership_cte_does_not_inherit_an_input_alias_prefix() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        ctx.scope_prefixes.insert("n".into(), "1/10/".into());
        ctx.token_scopes = Some(std::collections::HashMap::from([(
            "Project".into(),
            TokenScope::Namespaces(vec!["1/10/".into(), "1/20/".into()].into()),
        )]));
        let mut ast = Node::Query(Box::new(Query {
            select: vec![SelectExpr::star()],
            from: TableRef::Scan {
                table: ontology.edge_table().into(),
                alias: "e".into(),
                final_: true,
            },
            ..Default::default()
        }));
        apply_security_context(&mut ast, &ctx, &ontology).unwrap();
        let Node::Query(query) = ast else { panic!() };
        let TableRef::Subquery { query, .. } = &query.ctes[0].query.from else {
            panic!()
        };
        assert_eq!(
            starts_with_paths_for_alias(query.where_clause.as_ref().unwrap(), "n"),
            vec!["1/"]
        );
    }

    #[test]
    fn nested_edge_input_checks_both_endpoints() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        ctx.token_scopes = Some(Default::default());
        let inner = Query {
            select: vec![SelectExpr::col("e", "source_id")],
            from: TableRef::Scan {
                table: ontology.edge_table().into(),
                alias: "e".into(),
                final_: true,
            },
            ..Default::default()
        };
        let mut ast = Node::Query(Box::new(Query {
            select: vec![SelectExpr::star()],
            from: TableRef::Scan {
                table: "gl_project".into(),
                alias: "p".into(),
                final_: true,
            },
            where_clause: Some(Expr::InSelect {
                expr: Box::new(Expr::col("p", "id")),
                query: Box::new(inner),
            }),
            ..Default::default()
        }));
        apply_security_context(&mut ast, &ctx, &ontology).unwrap();
        let (sql, _) = crate::emit_simple_query(&ast).unwrap();
        assert!(sql.contains("_token_nodes AS MATERIALIZED"), "{sql}");
        assert!(sql.contains("tuple(e.source_kind, e.source_id)"), "{sql}");
        assert!(sql.contains("tuple(e.target_kind, e.target_id)"), "{sql}");
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
        let expr = build_path_filter("u", &[&TraversalPath::from("42/43/")]);
        assert!(matches!(expr, Expr::FuncCall { name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths_use_or_of_starts_with_without_common_prefix() {
        let expr = build_path_filter(
            "u",
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
        let expr = build_path_filter("e", &refs);
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
        let expr = build_path_filter("v", &[]);
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

        let filter = build_path_filter("t", &eligible);
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
