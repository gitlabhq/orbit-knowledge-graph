//! Post-compilation policy check over the checked relational plan.
//!
//! Runs after security filter injection. Every scan of a namespaced graph
//! table needs a role-eligible traversal path predicate, or a fail-closed
//! false predicate, among the filters that directly consume its join tree.

use std::collections::HashMap;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::RelationalFacts;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Operation, Relational, Table,
};
use query_data_model::plan::tracking::{NodeId, Plan, Ref};
use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::passes::codegen::ClickHousePlan;
use crate::types::SecurityContext;

pub fn verify(
    plan: &ClickHousePlan,
    catalog: &Catalog<ClickHouse>,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let graph = &plan.plan;
    let consumers: HashMap<NodeId, NodeId> = graph
        .nodes()
        .flat_map(|node| graph.inputs(node).iter().map(move |input| (*input, node)))
        .collect();
    for node in graph.nodes() {
        let Operation::Scan { table, alias, .. } = graph.operation(node) else {
            continue;
        };
        let table = table_name(catalog, *table);
        let Some(columns) = model.table_path_columns(table) else {
            return Err(QueryError::Security(format!(
                "post-check failed: unresolved table '{table}'"
            )));
        };
        let filters = join_filters(graph, &consumers, node);
        for column in columns {
            let access_level = column
                .entity
                .and_then(|entity| model.required_access_level(entity))
                .unwrap_or(ontology::RequiredRole::Reporter.as_access_level());
            let eligible = ctx.paths_at_least(access_level);
            let output = graph
                .outputs(node)
                .find(|output| graph.name(*output) == Some(column.name.as_str()));
            let guarded = output.is_some_and(|output| {
                filters
                    .iter()
                    .any(|filter| has_valid_path_filter(filter, output, &eligible))
            });
            if !guarded {
                return Err(QueryError::Security(format!(
                    "post-check failed: column '{alias}.{}' missing valid traversal_path filter at access level {access_level}",
                    column.name
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn table_name(catalog: &Catalog<ClickHouse>, table: Table) -> &str {
    let (_, facts) = catalog
        .stores()
        .nth(table.store())
        .expect("plans scan tables of their catalog");
    &facts.relational().tables[table.index()].name
}

fn join_filters<'p>(
    graph: &'p Plan<Relational<ClickHouse>>,
    consumers: &HashMap<NodeId, NodeId>,
    scan: NodeId,
) -> Vec<&'p Expr> {
    let mut node = scan;
    while let Some(consumer) = consumers.get(&node)
        && matches!(graph.operation(*consumer), Operation::Join(_))
    {
        node = *consumer;
    }
    let mut filters = Vec::new();
    while let Some(consumer) = consumers.get(&node)
        && let Operation::Filter(predicate) = graph.operation(*consumer)
    {
        filters.push(predicate);
        node = *consumer;
    }
    filters
}

fn has_valid_path_filter(expr: &Expr, column: Ref, eligible: &[&TraversalPath]) -> bool {
    match expr {
        Expr::Bool(false)
        | Expr::Parameter {
            value: Value::Bool(false),
            ..
        } => true,
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            has_valid_path_filter(left, column, eligible)
                || has_valid_path_filter(right, column, eligible)
        }
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            has_valid_path_filter(left, column, eligible)
                && has_valid_path_filter(right, column, eligible)
        }
        Expr::Call {
            function: Function::StartsWith,
            args,
        } => match args.as_slice() {
            [
                Expr::Column(actual),
                Expr::Parameter {
                    value: Value::String(path),
                    ..
                },
            ] if *actual == column => eligible
                .iter()
                .any(|authorized| path.starts_with(authorized.as_str())),
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    use crate::ast::{Expr, Function, Node, Op, Query, SelectExpr, TableRef};
    use crate::constants::TRAVERSAL_PATH_COLUMN;
    use query_data_model::ClickHouseDataModel;

    fn model() -> &'static ClickHouseDataModel {
        static MODEL: LazyLock<ClickHouseDataModel> =
            LazyLock::new(|| query_data_model::ClickHouseDataModel::embedded().unwrap());
        &MODEL
    }

    fn derive_model(ontology: ontology::Ontology) -> ClickHouseDataModel {
        query_data_model::from_ontology::clickhouse(&ontology).unwrap()
    }

    fn secured(
        node: &Node,
        context: &SecurityContext,
        model: &ClickHouseDataModel,
    ) -> Result<ClickHousePlan> {
        let plan = crate::passes::codegen::build(node, model.catalog())?;
        let plan =
            crate::passes::security::apply_security_context(plan, model.catalog(), context, model)?;
        verify(&plan, model.catalog(), context, model)?;
        Ok(plan)
    }

    fn check(node: &Node, context: &SecurityContext, model: &ClickHouseDataModel) -> Result<()> {
        let plan = crate::passes::codegen::build(node, model.catalog())?;
        verify(&plan, model.catalog(), context, model)
    }

    fn unscoped() -> Query {
        Query {
            select: vec![SelectExpr::col("u", "id")],
            from: TableRef::scan("gl_user", "u"),
            ..Default::default()
        }
    }
    fn project_query(where_clause: Option<Expr>) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause,
            limit: Some(10),
            ..Default::default()
        }))
    }

    #[test]
    fn passes_after_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let node = project_query(None);
        assert!(secured(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn security_injection_and_check_cover_nested_query_positions() {
        use crate::ast::{Cte, JoinType, OrderExpr};

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        for position in [
            "select",
            "where",
            "having",
            "group",
            "order",
            "limit_by",
            "join",
            "in_operand",
            "lambda",
            "nested_cte",
            "union",
            "derived",
            "table_union",
        ] {
            let inner = Query {
                select: vec![SelectExpr::col("protected", "id")],
                from: TableRef::scan("gl_project", "protected"),
                ..Default::default()
            };
            let scalar = Expr::Scalar(Box::new(inner.clone()));
            let mut query = Query {
                select: vec![SelectExpr::new(Expr::int(1), "result")],
                from: TableRef::scan("gl_user", "u"),
                ..Default::default()
            };
            match position {
                "select" => query.select = vec![SelectExpr::new(scalar, "result")],
                "where" => query.where_clause = Some(Expr::eq(scalar, Expr::int(1))),
                "having" => query.having = Some(Expr::eq(scalar, Expr::int(1))),
                "group" => query.group_by.push(scalar),
                "order" => query.order_by.push(OrderExpr::asc(scalar)),
                "limit_by" => query.limit_by = Some((1, vec![scalar])),
                "join" => {
                    query.from = TableRef::join(
                        JoinType::Inner,
                        TableRef::subquery(unscoped(), "outer"),
                        TableRef::subquery(unscoped(), "other"),
                        Expr::eq(scalar, Expr::int(1)),
                    )
                }
                "in_operand" => {
                    query.where_clause = Some(Expr::InSelect {
                        expr: Box::new(scalar),
                        query: Box::new(Query {
                            select: vec![SelectExpr::col("lookup", "id")],
                            from: TableRef::subquery(unscoped(), "lookup"),
                            ..Default::default()
                        }),
                    })
                }
                "lambda" => {
                    query.select = vec![SelectExpr::new(
                        Expr::func(
                            Function::ArrayMap,
                            vec![
                                Expr::lambda("x", scalar),
                                Expr::func(Function::Array, vec![Expr::int(1)]),
                            ],
                        ),
                        "result",
                    )]
                }
                "nested_cte" => query.ctes.push(Cte::new(
                    "outer_cte",
                    Query {
                        ctes: vec![Cte::new("inner_cte", inner)],
                        from: TableRef::cte("inner_cte", "nested"),
                        ..Default::default()
                    },
                )),
                "union" => query.union_all.push(inner),
                "derived" => query.from = TableRef::subquery(inner, "derived"),
                "table_union" => query.from = TableRef::union_all(vec![inner], "arms"),
                _ => unreachable!(),
            }
            let node = Node::Query(Box::new(query));
            assert!(check(&node, &context, model()).is_err(), "{position}");
            let plan = secured(&node, &context, model())
                .unwrap_or_else(|error| panic!("{position}: {error}"));
            let sql = model().catalog().render(&plan).sql;
            assert_eq!(sql.matches("startsWith(").count(), 1, "{position}: {sql}");
            assert!(
                sql.contains("startsWith(protected.traversal_path, "),
                "{position}: {sql}"
            );
        }
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = project_query(Some(Expr::lit(true)));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn rejects_path_filters_that_do_not_restrict_every_result() {
        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let column = Expr::col("p", TRAVERSAL_PATH_COLUMN);
        let starts_with = |left, right| Expr::func(Function::StartsWith, vec![left, right]);
        let authorized = starts_with(column.clone(), Expr::string("42/43/"));
        let condition = Expr::eq(Expr::col("p", "id"), Expr::int(1));
        for (predicate, valid) in [
            (
                Expr::binary(Op::Or, authorized.clone(), Expr::lit(true)),
                false,
            ),
            (Expr::unary(Op::Not, authorized.clone()), false),
            (Expr::eq(authorized.clone(), Expr::lit(false)), false),
            (
                Expr::binary(Op::Or, authorized.clone(), condition.clone()),
                false,
            ),
            (starts_with(column.clone(), Expr::string("42/")), false),
            (starts_with(Expr::string("42/43/"), column), false),
            (
                Expr::binary(Op::Or, authorized.clone(), Expr::and(authorized, condition)),
                true,
            ),
        ] {
            let node = project_query(Some(predicate));
            assert_eq!(check(&node, &context, model()).is_ok(), valid, "{node:?}");
        }
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let wrong_filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("99/")],
        );
        let node = project_query(Some(wrong_filter));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let node = project_query(None);
        assert!(secured(&node, &ctx, model()).is_ok());
    }

    /// An AND-chain containing `Bool(false)` short-circuits to zero rows, so
    /// the post-check must accept the query even when no per-alias
    /// `startsWith` filter is present. The security pass emits this shape
    /// when an alias has no eligible traversal paths (e.g. a Reporter-only
    /// user hitting an entity that requires Security Manager).
    #[test]
    fn accepts_bool_false_as_dead_alias_filter() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let dead = Expr::param(crate::ast::SqlType::Bool, false);
        let node = project_query(Some(Expr::binary(Op::And, dead, Expr::lit(true))));
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[ignore = "fixture ontology fails the data model load checks"]
    #[test]
    fn rejects_reporter_path_for_security_manager_table() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let model = derive_model(
            ontology::Ontology::new()
                .with_nodes(["Vulnerability"])
                .with_redaction("Vulnerability", "vulnerability", "id")
                .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager),
        );
        let node = Node::Query(Box::new(Query {
            from: TableRef::scan("gl_vulnerability", "v"),
            where_clause: Some(Expr::func(
                Function::StartsWith,
                vec![
                    Expr::col("v", TRAVERSAL_PATH_COLUMN),
                    Expr::string("1/100/"),
                ],
            )),
            ..Default::default()
        }));

        let error = check(&node, &context, &model).unwrap_err();
        assert!(error.to_string().contains("access level 25"));
    }

    #[test]
    fn validates_role_floor_for_denormalized_join() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let model = derive_model(
            ontology::Ontology::load_embedded()
                .unwrap()
                .with_redaction_role("MergeRequest", ontology::RequiredRole::SecurityManager)
                .with_denormalized_join(
                    "reviewer_project",
                    &[
                        ("REVIEWER", "User", "MergeRequest", false),
                        ("IN_PROJECT", "MergeRequest", "Project", true),
                    ],
                ),
        );
        let node = Node::Query(Box::new(Query {
            from: TableRef::scan("gl_denorm_reviewer_project", "d"),
            where_clause: Some(Expr::func(
                Function::StartsWith,
                vec![
                    Expr::col("d", TRAVERSAL_PATH_COLUMN),
                    Expr::string("1/100/"),
                ],
            )),
            ..Default::default()
        }));

        let error = check(&node, &context, &model).unwrap_err();
        assert!(error.to_string().contains("d.t2_traversal_path"));
        assert!(error.to_string().contains("access level 25"));
    }

    /// A `col = false` comparison (or any other non-AND operator whose
    /// operand happens to be a boolean false literal) must NOT be treated
    /// as a proof that the alias is scoped. Bool(false) short-circuits the
    /// clause only when AND-chained into the top level. OR-ing or
    /// equality-ing against it leaves other rows reachable.
    ///
    /// Without this guard a user filter like `Project.archived = false`
    /// would bypass CheckPass defense-in-depth for any alias whose
    /// `startsWith` is missing — defeating the purpose of the post-check.
    #[test]
    fn rejects_bool_false_nested_inside_comparison() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let eq_false = Expr::binary(
            Op::Eq,
            Expr::col("p", "archived"),
            Expr::param(crate::ast::SqlType::Bool, false),
        );
        let node = project_query(Some(eq_false));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "CheckPass must still require a startsWith for alias 'p'; got: {err}"
        );
    }

    /// `Bool(false)` OR-ed with anything is not a dead clause: the OR
    /// arms can still produce rows. Treating it as proof of scoping would
    /// leak data if the security pass forgot to emit a `startsWith`.
    #[test]
    fn rejects_bool_false_ored_with_true() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let or_expr = Expr::binary(
            Op::Or,
            Expr::param(crate::ast::SqlType::Bool, false),
            Expr::lit(true),
        );
        let node = project_query(Some(or_expr));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "OR-ed Bool(false) must not satisfy the post-check; got: {err}"
        );
    }

    /// A `Bool(false)` buried in a deeply nested AND chain (the shape
    /// `Expr::and_all` typically emits for multi-alias queries) still
    /// short-circuits the clause and counts as a valid path filter.
    #[test]
    fn accepts_bool_false_in_nested_and_chain() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let dead_conjunct = Expr::binary(
            Op::And,
            Expr::binary(Op::Eq, Expr::col("p", "id"), Expr::lit(5)),
            Expr::param(crate::ast::SqlType::Bool, false),
        );
        let where_expr = Expr::binary(
            Op::And,
            Expr::func(
                Function::StartsWith,
                vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
            ),
            dead_conjunct,
        );
        let node = project_query(Some(where_expr));
        assert!(check(&node, &ctx, model()).is_ok());
    }

    /// Inverse of the previous test: if the AND chain has no Bool(false)
    /// and no matching startsWith for the alias, the check must fail even
    /// when the clause contains `col = false` (which is NOT a dead
    /// conjunct).
    #[test]
    fn rejects_and_chain_with_col_eq_false_and_no_starts_with() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let where_expr = Expr::binary(
            Op::And,
            Expr::binary(
                Op::Eq,
                Expr::col("p", "archived"),
                Expr::param(crate::ast::SqlType::Bool, false),
            ),
            Expr::binary(Op::Gt, Expr::col("p", "id"), Expr::lit(0)),
        );
        let node = project_query(Some(where_expr));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "col = false is NOT a dead conjunct — check must still require startsWith, got: {err}"
        );
    }

    #[test]
    fn skips_declared_ctes() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
            ctes: vec![crate::ast::Cte::new("path_cte", unscoped())],
            select: vec![SelectExpr {
                expr: Expr::col("c", "id"),
                alias: None,
            }],
            from: TableRef::cte("path_cte", "c"),
            where_clause: None,
            ..Default::default()
        }));
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_unknown_graph_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        for table in ["gl_unknown", "v42_gl_unknown"] {
            let node = Node::Query(Box::new(Query {
                from: TableRef::scan(table, "unknown"),
                ..Default::default()
            }));
            let error = check(&node, &ctx, model()).unwrap_err();
            assert!(error.to_string().contains(table));
        }
    }

    #[test]
    fn reports_first_invalid_scan() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "first"),
                TableRef::scan("gl_merge_request", "second"),
                Expr::lit(true),
            ),
            ..Default::default()
        }));
        let error = check(&node, &ctx, model()).unwrap_err();
        assert!(error.to_string().contains("first.traversal_path"));
    }

    fn wrap_in_subquery(inner: Query) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("sq", "id"),
                alias: None,
            }],
            from: TableRef::subquery(inner, "sq"),
            where_clause: None,
            ..Default::default()
        }))
    }

    fn inner_project_query(where_clause: Option<Expr>) -> Query {
        Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause,
            ..Default::default()
        }
    }

    #[test]
    fn rejects_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = wrap_in_subquery(inner_project_query(None));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut inner = inner_project_query(None);
        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        inner.where_clause = Some(filter);
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_aggregate_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                alias: Some("id".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "visibility_level")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_aggregate_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                alias: Some("id".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter),
            group_by: vec![Expr::col("p", "visibility_level")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn accepts_subquery_wrapping_cte() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            ctes: vec![crate::ast::Cte::new("dedup_cte", unscoped())],
            select: vec![SelectExpr::col("d", "id")],
            from: TableRef::cte("dedup_cte", "d"),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_union_all_arm_without_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("u", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            where_clause: Some(filter),
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
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_union_all_arms_with_security_filters() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
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
        assert!(secured(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_cte_with_sensitive_table_missing_filter() {
        use crate::ast::Cte;

        let node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "base",
                Query {
                    select: vec![SelectExpr {
                        expr: Expr::col("p", "id"),
                        alias: Some("node_id".into()),
                    }],
                    from: TableRef::scan("gl_project", "p"),
                    where_clause: None,
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr {
                expr: Expr::col("b", "node_id"),
                alias: None,
            }],
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "CTE scanning gl_project without filter should be rejected: {}",
            err
        );
    }

    #[test]
    fn accepts_cte_with_security_filter() {
        use crate::ast::Cte;

        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "base",
                Query {
                    select: vec![SelectExpr {
                        expr: Expr::col("p", "id"),
                        alias: Some("node_id".into()),
                    }],
                    from: TableRef::scan("gl_project", "p"),
                    where_clause: Some(filter),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr {
                expr: Expr::col("b", "node_id"),
                alias: None,
            }],
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_union_arm_missing_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("e", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "e"),
                TableRef::Union {
                    queries: vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("p", "id"),
                            alias: None,
                        }],
                        from: TableRef::scan("gl_project", "p"),
                        where_clause: None,
                        ..Default::default()
                    }],
                    alias: "bad_union".into(),
                },
                Expr::lit(true),
            ),
            where_clause: Some(filter),
            ..Default::default()
        }));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "union arm scanning gl_project without filter should be rejected, got: {err}"
        );
    }

    #[test]
    fn accepts_union_arm_with_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let outer_filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("e", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let arm_filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("e1", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "e"),
                TableRef::Union {
                    queries: vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("e1", "source_id"),
                            alias: None,
                        }],
                        from: TableRef::scan(EDGE_TABLE, "e1"),
                        where_clause: Some(arm_filter),
                        ..Default::default()
                    }],
                    alias: "hop_e0".into(),
                },
                Expr::lit(true),
            ),
            where_clause: Some(outer_filter),
            ..Default::default()
        }));
        assert!(check(&node, &ctx, model()).is_ok());
    }
}
