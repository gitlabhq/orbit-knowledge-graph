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

use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Function, Node, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN};
use crate::error::Result;
pub use crate::types::SecurityContext;
use orbit_utils::traversal_path::{TraversalPath, TraversalPathTrie};

static GRAPH_TABLE_PATTERN: OnceLock<Regex> = OnceLock::new();

/// Per-alias role floors come from the query data model.
/// Tables without authorization metadata keep the historical Reporter floor.
pub fn apply_security_context(
    node: &mut Node,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
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
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        let mut aliased_tables = Vec::new();
        visit_relations(&query.from, &mut |relation| {
            if let TableRef::Scan { table, alias, .. } = relation {
                let graph_table_pattern = GRAPH_TABLE_PATTERN.get_or_init(|| {
                    Regex::new(&format!(
                        r"^(?:v\d+_)?{}.+$",
                        regex::escape(GL_TABLE_PREFIX)
                    ))
                    .expect("valid regex")
                });
                if graph_table_pattern.is_match(table) && model.table_has_path_columns(table) {
                    aliased_tables.push((alias.clone(), table.clone()));
                }
            }
        });
        let security_conditions = aliased_tables.iter().map(|(alias, table)| {
            let min_role = model.table_minimum_access_level(table);
            let eligible = ctx.paths_at_least(min_role);
            build_path_filter(alias, &eligible)
        });
        query.where_clause = Expr::and_all(
            security_conditions
                .map(Some)
                .chain(std::iter::once(query.where_clause.take())),
        );
        Ok(())
    })
}

fn build_path_filter(alias: &str, paths: &[&TraversalPath]) -> Expr {
    let starts_with = |path: &str| {
        Expr::func(
            Function::StartsWith,
            vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(path)],
        )
    };
    match paths.len() {
        0 => Expr::Literal(Value::Bool(false)),
        1 => starts_with(paths[0].as_str()),
        _ => {
            let collapsed = TraversalPathTrie::from_paths(paths).to_minimal_prefixes();
            if let [path] = collapsed.as_slice() {
                return starts_with(path.as_str());
            }
            Expr::or_all(
                collapsed
                    .iter()
                    .map(|path| Some(starts_with(path.as_str()))),
            )
            .expect("paths is non-empty")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, LazyLock};

    use crate::AuthorizedPath;
    use crate::ast::visit::{visit_expressions, visit_queries};
    use crate::ast::{JoinType, Op, Query, SelectExpr};
    use ontology::constants::EDGE_TABLE;
    use orbit_utils::traversal_path::TraversalPath;
    use query_data_model::ClickHouseDataModel;
    use serde_json::Value;

    fn model() -> &'static ClickHouseDataModel {
        static MODEL: LazyLock<ClickHouseDataModel> = LazyLock::new(|| {
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap()
        });
        &MODEL
    }

    fn security_model() -> &'static ClickHouseDataModel {
        static MODEL: LazyLock<ClickHouseDataModel> = LazyLock::new(|| {
            ClickHouseDataModel::derive(Arc::new(
                ontology::Ontology::new()
                    .with_nodes(["Project", "Vulnerability"])
                    .with_redaction("Vulnerability", "vulnerabilities", "id")
                    .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager),
            ))
            .unwrap()
        });
        &MODEL
    }

    fn apply(
        node: &mut Node,
        context: &SecurityContext,
        model: &ClickHouseDataModel,
    ) -> Result<()> {
        apply_security_context(node, context, model)
    }

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
    fn security_context_validates_paths_and_roles() {
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

        let context = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new("1/100/", 20),
                AuthorizedPath::new("1/101/", 30),
            ],
        )
        .unwrap();
        assert_eq!(context.paths_at_least(20), vec!["1/100/", "1/101/"]);
        assert_eq!(context.paths_at_least(30), vec!["1/101/"]);
        assert!(context.paths_at_least(50).is_empty());
        assert!(
            SecurityContext::new_with_roles(
                1,
                vec![AuthorizedPath::with_access_levels("1/100/", vec![])]
            )
            .is_err()
        );
    }

    #[test]
    fn path_filters_cover_empty_single_multiple_and_large_sets() {
        assert!(matches!(
            build_path_filter("v", &[]),
            Expr::Literal(Value::Bool(false))
        ));
        assert!(matches!(
            build_path_filter("u", &[&TraversalPath::from("42/43/")]),
            Expr::FuncCall {
                name: Function::StartsWith,
                ..
            }
        ));

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
        assert_eq!(paths, ["1/2/4/", "1/2/5/"]);

        let paths: Vec<TraversalPath> = (0..1000u64)
            .map(|i| TraversalPath::from(format!("1/{i}/")))
            .collect();
        let refs: Vec<&TraversalPath> = paths.iter().collect();
        let expr = build_path_filter("e", &refs);
        let debug = format!("{expr:?}");
        assert!(!debug.contains("arrayExists"));
        assert!(debug.contains("StartsWith"));
        assert!(or_depth(&expr) <= 11, "OR tree is too deep: {debug}");
    }

    fn or_depth(expr: &Expr) -> usize {
        match expr {
            Expr::BinaryOp {
                op: Op::Or,
                left,
                right,
            } => 1 + or_depth(left).max(or_depth(right)),
            _ => 1,
        }
    }

    fn starts_with_paths_for_alias(expr: &Expr, alias: &str) -> Vec<String> {
        let mut paths = Vec::new();
        visit_expressions(expr, &mut |expr| {
            if let Expr::FuncCall { name, args } = expr
                && *name == Function::StartsWith
                && let [
                    Expr::Column { table, column },
                    Expr::Param {
                        value: Value::String(path),
                        ..
                    },
                ] = args.as_slice()
                && table == alias
                && column == TRAVERSAL_PATH_COLUMN
            {
                paths.push(path.clone());
            }
            Ok(())
        })
        .unwrap();
        paths
    }

    #[test]
    fn injects_security_into_nested_queries() {
        let ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/200/".into()]).unwrap();
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

        apply(&mut node, &ctx, model()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let mut paths = Vec::new();
        visit_queries(q, &mut |query| {
            if let Some(predicate) = &query.where_clause {
                paths.extend(starts_with_paths_for_alias(predicate, "e0p"));
            }
            Ok(())
        })
        .unwrap();
        assert!(
            paths.contains(&"1/100/".to_string()) && paths.contains(&"1/200/".to_string()),
            "anchor subquery must carry the caller's full path set, got {paths:?}"
        );

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
                        ..Default::default()
                    }],
                    "hop_e0",
                ),
                Expr::lit(true),
            ),
            ..Default::default()
        }));
        apply(&mut node, &ctx, model()).unwrap();
        let Node::Query(query) = &node else {
            unreachable!()
        };
        assert!(query.where_clause.is_some());
        let TableRef::Join { right, .. } = &query.from else {
            unreachable!()
        };
        let TableRef::Union { queries, .. } = right.as_ref() else {
            unreachable!()
        };
        assert!(queries[0].where_clause.is_some());
        assert!(starts_with_paths_for_alias(query.where_clause.as_ref().unwrap(), "e1").is_empty());

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            union_all: vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: None,
                }],
                from: TableRef::scan("gl_project", "p"),
                ..Default::default()
            }],
            ..Default::default()
        }));
        apply(&mut node, &ctx, model()).unwrap();
        let Node::Query(query) = &node else {
            unreachable!()
        };
        assert!(query.where_clause.is_some());
        assert!(query.union_all[0].where_clause.is_some());
    }

    // Paths tagged at Developer (30) still qualify because 30 >= the
    // Security Manager floor (25).
    #[test]
    fn role_floors_filter_and_collapse_paths() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new("1/100/", 20), // Reporter
                AuthorizedPath::new("1/101/", 30), // Developer (covers SM)
            ],
        )
        .unwrap();

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

        apply(&mut node, &ctx, security_model()).unwrap();

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

        let ctx = SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/100/", 20)], // Reporter only
        )
        .unwrap();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_vulnerability", "v"),
            limit: Some(10),
            ..Default::default()
        }));

        apply(&mut node, &ctx, security_model()).unwrap();

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
            sql.contains("StartsWith"),
            "should produce startsWith predicates: {sql}"
        );
    }

    #[test]
    fn data_model_controls_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = simple_query();
        apply(&mut node, &ctx, model()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan(EDGE_TABLE, "e"),
            ..Default::default()
        }));

        apply(&mut node, &ctx, model()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));

        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_project", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "id"), Expr::col("e", "source")),
        );
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::col("p", "id")],
            from,
            ..Default::default()
        }));
        apply(&mut node, &ctx, model()).unwrap();
        let Node::Query(query) = &node else {
            unreachable!()
        };
        let where_clause = query.where_clause.as_ref().unwrap();
        assert_eq!(starts_with_paths_for_alias(where_clause, "p"), ["1/"]);
        assert_eq!(starts_with_paths_for_alias(where_clause, "e"), ["1/"]);

        for table in ["gl_user", "path_cte", "some_cte", "nodes"] {
            let mut node = Node::Query(Box::new(Query {
                select: vec![SelectExpr::col("x", "id")],
                from: TableRef::scan(table, "x"),
                ..Default::default()
            }));
            apply(&mut node, &ctx, model()).unwrap();
            assert!(matches!(node, Node::Query(query) if query.where_clause.is_none()));
        }

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::col("u", "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::scan("gl_merge_request", "mr"),
                Expr::lit(true),
            ),
            ..Default::default()
        }));
        apply(&mut node, &ctx, model()).unwrap();
        let Node::Query(query) = &node else {
            unreachable!()
        };
        let where_clause = query.where_clause.as_ref().unwrap();
        assert!(starts_with_paths_for_alias(where_clause, "u").is_empty());
        assert_eq!(starts_with_paths_for_alias(where_clause, "mr"), ["1/"]);

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

        apply(&mut node, &ctx, model()).unwrap();

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
}
