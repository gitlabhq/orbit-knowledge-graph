//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen. Checks that every namespaced graph
//! table alias has a role-eligible traversal path predicate or a fail-closed
//! false predicate.

use query_data_model::GitLabPolicy;
use serde_json::Value;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Expr, Function, Node, Op, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::enforce::ResultContext;
use crate::passes::lower::LoweredMetadata;
use crate::types::SecurityContext;
#[cfg(test)]
use ontology::Ontology;

pub fn check_ast(
    node: &Node,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    match node {
        Node::Query(q) => visit_queries(q, &mut |query| check_query(query, ctx, model)),
        Node::Insert(_) => Ok(()),
    }
}

pub fn check_authorization(
    input: &Input,
    node: &Node,
    metadata: &LoweredMetadata,
    result: &ResultContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    let grouped: std::collections::HashSet<_> =
        crate::input::node_group_ids(&input.aggregation.group_by).collect();
    let expected: std::collections::BTreeMap<_, _> = input
        .nodes
        .iter()
        .filter(|node| match input.query_type {
            QueryType::Traversal | QueryType::Neighbors => true,
            QueryType::Aggregation => grouped.contains(node.id.as_str()),
            QueryType::PathFinding | QueryType::Hydration => false,
        })
        .filter_map(|node| Some((node.id.as_str(), node.entity.as_deref()?)))
        .collect();
    let actual: std::collections::BTreeMap<_, _> = result
        .nodes()
        .map(|node| (node.alias.as_str(), node.entity_type.as_str()))
        .collect();
    if actual != expected {
        return Err(authorization_error(format!(
            "result nodes differ from query output: expected {expected:?}, got {actual:?}"
        )));
    }
    for redaction in result.nodes() {
        check_redaction_node(query, metadata, redaction, model)?;
    }
    match input.query_type {
        QueryType::Neighbors => {
            require_output(query, crate::constants::neighbor_id_column())?;
            require_output(query, crate::constants::neighbor_type_column())?;
        }
        QueryType::PathFinding => {
            require_output(query, crate::constants::path_column())?;
        }
        _ => {}
    }
    Ok(())
}

fn check_redaction_node(
    query: &Query,
    metadata: &LoweredMetadata,
    redaction: &crate::passes::enforce::RedactionNode,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let outputs = metadata
        .nodes
        .get(&redaction.alias)
        .and_then(|binding| redaction.expected_outputs(binding, model))
        .ok_or_else(|| {
            authorization_error(format!(
                "node '{}' has no lowered authorization identity",
                redaction.alias
            ))
        })?;
    for (column, expected) in outputs {
        let actual = query.output_expressions(column).ok_or_else(|| {
            authorization_error(format!(
                "node '{}' is missing result column '{column}'",
                redaction.alias
            ))
        })?;
        if expected
            .as_ref()
            .is_some_and(|expected| !actual.iter().all(|value| *value == expected))
        {
            return Err(authorization_error(format!(
                "node '{}' result column '{column}' has the wrong identity",
                redaction.alias,
            )));
        }
    }
    Ok(())
}

fn require_output(query: &Query, alias: &str) -> Result<()> {
    query
        .output_expressions(alias)
        .is_some()
        .then_some(())
        .ok_or_else(|| authorization_error(format!("result is missing dynamic column '{alias}'")))
}

fn authorization_error(message: impl Into<String>) -> QueryError {
    QueryError::Security(format!("post-check failed: {}", message.into()))
}

fn check_query(
    q: &Query,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let mut error = None;
    visit_relations(&q.from, &mut |relation| {
        if error.is_some() {
            return;
        }
        if let TableRef::Scan { table, alias, .. } = relation {
            if table.is_empty() {
                return;
            }
            let Some(columns) = model.table_path_columns(table) else {
                error = Some(QueryError::Security(format!(
                    "post-check failed: unresolved table '{table}'"
                )));
                return;
            };
            for column in columns {
                let access_level = column
                    .entity
                    .and_then(|entity| model.query_authorization().required_access_level(entity))
                    .unwrap_or(ontology::RequiredRole::Reporter.as_access_level());
                let eligible_paths = ctx.paths_at_least(access_level);
                if !has_valid_path_filter(
                    q.where_clause.as_ref(),
                    alias,
                    &column.name,
                    &eligible_paths,
                ) {
                    error = Some(QueryError::Security(format!(
                        "post-check failed: column '{alias}.{}' missing valid traversal_path filter at access level {access_level}",
                        column.name
                    )));
                    break;
                }
            }
        }
    });
    error.map_or(Ok(()), Err)
}

fn has_valid_path_filter(
    expr: Option<&Expr>,
    alias: &str,
    column: &str,
    eligible_paths: &[&orbit_utils::traversal_path::TraversalPath],
) -> bool {
    let Some(expr) = expr else { return false };
    match expr {
        Expr::Literal(Value::Bool(false))
        | Expr::Param {
            value: Value::Bool(false),
            ..
        } => true,
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            has_valid_path_filter(Some(left), alias, column, eligible_paths)
                || has_valid_path_filter(Some(right), alias, column, eligible_paths)
        }
        Expr::BinaryOp {
            op: Op::Or,
            left,
            right,
        } => {
            has_valid_path_filter(Some(left), alias, column, eligible_paths)
                && has_valid_path_filter(Some(right), alias, column, eligible_paths)
        }
        Expr::FuncCall {
            name: Function::StartsWith,
            args,
        } => {
            let [
                Expr::Column {
                    table,
                    column: actual,
                },
                path,
            ] = args.as_slice()
            else {
                return false;
            };
            if table != alias || actual != column {
                return false;
            }
            match path {
                Expr::Literal(Value::String(path))
                | Expr::Param {
                    value: Value::String(path),
                    ..
                } => eligible_paths
                    .iter()
                    .any(|authorized| path.starts_with(authorized.as_str())),
                _ => false,
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{SelectExpr, TableRef};
    use crate::constants::{
        TRAVERSAL_PATH_COLUMN, primary_key_column, redaction_id_column, redaction_type_column,
    };
    use crate::passes::lower::NodeBinding;
    use query_data_model::OrbitQueryModel;

    fn apply_security(node: &mut Node, context: &SecurityContext, ontology: &Ontology) {
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology.clone())).unwrap();
        crate::passes::security::apply_security_context(node, context, model.as_ref()).unwrap();
    }

    fn check(node: &Node, context: &SecurityContext, ontology: &Ontology) -> Result<()> {
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology.clone())).unwrap();
        check_ast(node, context, model.as_ref())
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

    fn project_ontology() -> Ontology {
        Ontology::new().with_nodes(["Project"])
    }

    fn authorization_input(query_type: QueryType) -> Input {
        Input {
            query_type,
            nodes: vec![crate::input::InputNode {
                id: "p".into(),
                entity: Some("Project".into()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn authorization_query(id: Expr, entity: &str) -> Node {
        Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(id, redaction_id_column("p")),
                SelectExpr::new(Expr::string(entity), redaction_type_column("p")),
            ],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }))
    }

    fn authorization_fixture(
        id_column: &str,
    ) -> (
        std::sync::Arc<query_data_model::ClickHouseDataModel>,
        Input,
        Node,
        LoweredMetadata,
        ResultContext,
    ) {
        let ontology = project_ontology().with_redaction("Project", "project", id_column);
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology)).unwrap();
        let identity = Expr::col("p", "id");
        let mut result = ResultContext::new().with_query_type(QueryType::Traversal);
        result.add_node("p", "Project");
        for (entity, auth) in model.entity_auth() {
            result.add_entity_auth(entity, auth.clone());
        }
        let metadata = LoweredMetadata {
            nodes: [(
                "p".into(),
                NodeBinding::Values {
                    identity: identity.clone(),
                    table_alias: Some("p".into()),
                    traversal_path: None,
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        (
            model,
            authorization_input(QueryType::Traversal),
            authorization_query(Expr::col("p", id_column), "Project"),
            metadata,
            result,
        )
    }

    #[test]
    fn authorization_check_validates_context_and_projection() {
        let (model, input, mut valid, metadata, result) = authorization_fixture("project_id");
        let Node::Query(query) = &mut valid else {
            unreachable!()
        };
        query.select.push(SelectExpr::new(
            Expr::col("p", "id"),
            primary_key_column("p"),
        ));
        assert!(check_authorization(&input, &valid, &metadata, &result, model.as_ref()).is_ok());

        let mut wrong = valid;
        let Node::Query(query) = &mut wrong else {
            unreachable!()
        };
        query.select[0].expr = Expr::col("p", "id");
        assert!(check_authorization(&input, &wrong, &metadata, &result, model.as_ref()).is_err());

        let mut projected = metadata.clone();
        projected.nodes.insert(
            "p".into(),
            NodeBinding::Projected {
                role_identity: None,
            },
        );
        assert!(check_authorization(&input, &wrong, &projected, &result, model.as_ref()).is_err());

        let empty = ResultContext::new().with_query_type(QueryType::Traversal);
        assert!(check_authorization(&input, &wrong, &metadata, &empty, model.as_ref()).is_err());
    }

    #[test]
    fn passes_after_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = project_ontology();
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn security_injection_and_check_cover_nested_query_positions() {
        use crate::ast::{Cte, JoinType, OrderExpr};

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = project_ontology();
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
                        TableRef::subquery(Query::default(), "outer"),
                        TableRef::subquery(Query::default(), "other"),
                        Expr::eq(scalar, Expr::int(1)),
                    )
                }
                "in_operand" => {
                    query.where_clause = Some(Expr::InSelect {
                        expr: Box::new(scalar),
                        query: Box::new(Query {
                            from: TableRef::subquery(Query::default(), "lookup"),
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
            let outer_predicate = query.where_clause.clone();
            let mut node = Node::Query(Box::new(query));
            assert!(check(&node, &context, &ontology).is_err(), "{position}");
            apply_security(&mut node, &context, &ontology);
            check(&node, &context, &ontology).unwrap_or_else(|error| panic!("{position}: {error}"));
            let Node::Query(query) = &node else {
                unreachable!()
            };
            if outer_predicate.is_none() {
                assert!(query.where_clause.is_none(), "{position}");
            }
            let mut protected_scans = 0;
            visit_queries(query, &mut |query| {
                if matches!(&query.from, TableRef::Scan { alias, .. } if alias == "protected") {
                    protected_scans += 1;
                    assert_eq!(
                        query.where_clause,
                        Some(Expr::func(
                            Function::StartsWith,
                            vec![
                                Expr::col("protected", "traversal_path"),
                                Expr::string("42/43/")
                            ],
                        )),
                        "{position}"
                    );
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(protected_scans, 1, "{position}");
        }
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = project_query(Some(Expr::lit(true)));
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn rejects_path_filters_that_do_not_restrict_every_result() {
        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = project_ontology();
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
            assert_eq!(check(&node, &context, &ontology).is_ok(), valid, "{node:?}");
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let ontology = project_ontology();
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
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
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_reporter_path_for_security_manager_table() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let ontology = Ontology::new()
            .with_nodes(["Vulnerability"])
            .with_redaction("Vulnerability", "vulnerability", "id")
            .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager);
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

        let error = check(&node, &context, &ontology).unwrap_err();
        assert!(error.to_string().contains("access level 25"));
    }

    #[test]
    fn validates_role_floor_for_denormalized_join() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let ontology = Ontology::load_embedded()
            .unwrap()
            .with_redaction_role("MergeRequest", ontology::RequiredRole::SecurityManager)
            .with_denormalized_join(
                "reviewer_project",
                &[
                    ("REVIEWER", "User", "MergeRequest", false),
                    ("IN_PROJECT", "MergeRequest", "Project", true),
                ],
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

        let error = check(&node, &context, &ontology).unwrap_err();
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
            ctes: vec![crate::ast::Cte::new("path_cte", Query::default())],
            select: vec![SelectExpr {
                expr: Expr::col("c", "id"),
                alias: None,
            }],
            from: TableRef::cte("path_cte", "c"),
            where_clause: None,
            ..Default::default()
        }));
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_unknown_graph_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        for table in ["gl_unknown", "v42_gl_unknown"] {
            let node = Node::Query(Box::new(Query {
                from: TableRef::scan(table, "unknown"),
                ..Default::default()
            }));
            let error = check(&node, &ctx, &Ontology::new()).unwrap_err();
            assert!(error.to_string().contains(table));
        }
    }

    #[test]
    fn reports_first_invalid_scan() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let ontology = project_ontology().with_nodes(["MergeRequest"]);
        let node = Node::Query(Box::new(Query {
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "first"),
                TableRef::scan("gl_merge_request", "second"),
                Expr::lit(true),
            ),
            ..Default::default()
        }));
        let error = check(&node, &ctx, &ontology).unwrap_err();
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut inner = inner_project_query(None);
        let mut wrapped = Node::Query(Box::new(inner.clone()));
        apply_security(&mut wrapped, &ctx, &ontology::Ontology::new());
        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        inner.where_clause = Some(filter);
        let node = wrap_in_subquery(inner);
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_aggregate_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn accepts_subquery_wrapping_cte() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            ctes: vec![crate::ast::Cte::new("dedup_cte", Query::default())],
            from: TableRef::cte("dedup_cte", "d"),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_union_all_arms_with_security_filters() {
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
        let ontology = project_ontology();
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
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
                expr: Expr::col("base", "node_id"),
                alias: None,
            }],
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
                expr: Expr::col("base", "node_id"),
                alias: None,
            }],
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = project_ontology();
        assert!(check(&node, &ctx, &ontology).is_ok());
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
                expr: Expr::col("hop", "source_id"),
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
        let ontology = project_ontology();
        let err = check(&node, &ctx, &ontology).unwrap_err();
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
                expr: Expr::col("hop", "source_id"),
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
        let ontology = ontology::Ontology::new();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }
}
