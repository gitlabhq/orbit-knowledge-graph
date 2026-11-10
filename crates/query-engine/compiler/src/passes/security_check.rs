//! Post-compilation policy check over the checked relational plan.
//!
//! Runs after security filter injection. Every scan of a namespaced graph
//! table needs a role-eligible traversal path predicate, or a fail-closed
//! false predicate, among the filters that directly consume its join tree.

use std::collections::HashMap;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Operation, Relational,
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
        let Some(guards) = super::guards::scan(catalog, *table) else {
            return Err(QueryError::Security(format!(
                "post-check failed: unresolved table '{}'",
                catalog.table_name(*table)
            )));
        };
        let filters = join_filters(graph, &consumers, node);
        for guard in guards {
            let eligible = ctx.paths_at_least(guard.min_role);
            let output = graph
                .outputs(node)
                .find(|output| graph.name(*output) == Some(guard.column.as_str()));
            let guarded = output.is_some_and(|output| {
                filters
                    .iter()
                    .any(|filter| has_valid_path_filter(filter, output, &eligible))
            });
            if !guarded {
                return Err(QueryError::Security(format!(
                    "post-check failed: column '{alias}.{}' missing valid traversal_path filter at access level {}",
                    guard.column, guard.min_role
                )));
            }
        }
    }
    Ok(())
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
    use std::sync::{Arc, LazyLock};

    use orbit_utils::query_types::SqlType;
    use query_data_model::implementations::clickhouse::spec::ClickHouseOptions;
    use query_data_model::implementations::relational::plan::{
        AggregateFunction, JoinKind, Measure, Order, ReadMode, Relation, RelationalBuilder,
        RelationalError, UnaryOp,
    };
    use query_data_model::plan::tracking::Declare;
    use query_data_model::spec::{Fragment, decode_options, encode_options};
    use query_data_model::{Catalog, Model, Packages, TRAVERSAL_PATH_COLUMN};

    type Built<T> = std::result::Result<T, RelationalError>;
    type Predicate = fn(&Relation) -> Expr;
    type Builder<'q, 'c> = &'q mut RelationalBuilder<'c, ClickHouse>;

    fn model() -> &'static Catalog<ClickHouse> {
        static MODEL: LazyLock<Catalog<ClickHouse>> = LazyLock::new(|| {
            query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap()
        });
        &MODEL
    }

    const REVIEWER_PROJECT: &str = r#"
sort_key: [traversal_path, t2_id, t0_id, t3_id]
sources:
- table: gl_user
  columns: {id: t0_id}
- table: gl_edge
  path_column: traversal_path
  columns:
    relationship_kind: t1_relationship_kind
    source_id: t1_source_id
    source_kind: t1_source_kind
    target_id: t1_target_id
    target_kind: t1_target_kind
    traversal_path: traversal_path
  "on": {previous: id, column: source_id}
  filter: {relationship_kind: REVIEWER, source_kind: User, target_kind: MergeRequest}
- table: gl_merge_request
  path_column: t2_traversal_path
  columns: {id: t2_id, project_id: t2_project_id, traversal_path: t2_traversal_path}
  "on": {previous: target_id, column: id}
- table: gl_project
  path_column: t3_traversal_path
  columns: {id: t3_id, traversal_path: t3_traversal_path}
  "on": {previous: project_id, column: id}
nodes:
- {entity: User, source: 0, key: t0_id}
- {entity: MergeRequest, source: 2, key: t2_id}
- {entity: Project, source: 3, key: t3_id}
relationships:
- {relationship: REVIEWER, source: 0, target: 1, edge: 1}
- {relationship: IN_PROJECT, source: 1, target: 2}
"#;

    /// The embedded model with security-manager merge requests and a reviewer-to-project join.
    fn reviewer_project_model() -> Catalog<ClickHouse> {
        let mut fragments = Packages::embedded().fragments().unwrap();
        for fragment in &mut fragments {
            match fragment {
                Fragment::Authorization(access) => {
                    if let Some(merge_request) = access.entities.get_mut("MergeRequest") {
                        merge_request.role = Some(query_data_model::RequiredRole::SecurityManager);
                    }
                }
                Fragment::Storage(storage) if storage.store == "graph" => {
                    let Some(engine) = &mut storage.engine else {
                        continue;
                    };
                    let mut options: ClickHouseOptions = decode_options(&engine.backend).unwrap();
                    options.joins.insert(
                        "gl_denorm_reviewer_project".into(),
                        orbit_utils::yaml::from_str(REVIEWER_PROJECT).unwrap(),
                    );
                    engine.backend = encode_options(options).unwrap();
                }
                _ => {}
            }
        }
        Catalog::<ClickHouse>::from_model(&Arc::new(Model::load(&fragments).unwrap())).unwrap()
    }

    fn build(
        model: &Catalog<ClickHouse>,
        construct: impl FnOnce(Builder<'_, '_>) -> Built<Relation>,
    ) -> ClickHousePlan {
        model.build(construct).unwrap()
    }

    fn secured(
        plan: ClickHousePlan,
        context: &SecurityContext,
        model: &Catalog<ClickHouse>,
    ) -> Result<ClickHousePlan> {
        let plan = crate::passes::security::apply_security_context(plan, model, context)?;
        verify(&plan, model, context)?;
        Ok(plan)
    }

    fn check(
        plan: &ClickHousePlan,
        context: &SecurityContext,
        model: &Catalog<ClickHouse>,
    ) -> Result<()> {
        verify(plan, model, context)
    }

    fn scan(q: Builder<'_, '_>, table: &str, alias: &str) -> Built<Relation> {
        let table = q.catalog().table(table)?;
        q.scan(table, ReadMode::Raw, alias)
    }

    fn col(rows: &Relation, alias: &str, name: &str) -> Expr {
        rows.qualified(alias, name).unwrap().expr()
    }

    fn text(value: &str) -> Expr {
        Expr::Parameter {
            data_type: SqlType::String,
            value: Value::String(value.into()),
        }
    }

    fn literal(value: impl Into<Value>) -> Expr {
        let value = value.into();
        Expr::Parameter {
            data_type: SqlType::from_value(&value),
            value,
        }
    }

    fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn starts_with(value: Expr, path: Expr) -> Expr {
        Expr::Call {
            function: Function::StartsWith,
            args: vec![value, path],
        }
    }

    fn path(rows: &Relation, alias: &str, prefix: &str) -> Expr {
        starts_with(col(rows, alias, TRAVERSAL_PATH_COLUMN), text(prefix))
    }

    fn filtered(q: Builder<'_, '_>, rows: Relation, predicate: Option<Expr>) -> Built<Relation> {
        match predicate {
            Some(predicate) => q.filter(rows, predicate),
            None => Ok(rows),
        }
    }

    fn ids(q: Builder<'_, '_>, rows: Relation, alias: &str) -> Built<Relation> {
        let id = rows.qualified(alias, "id")?;
        q.select(rows, vec![id])
    }

    fn table(
        q: Builder<'_, '_>,
        name: &str,
        alias: &str,
        predicate: impl FnOnce(&Relation) -> Option<Expr>,
    ) -> Built<Relation> {
        let rows = scan(q, name, alias)?;
        let predicate = predicate(&rows);
        let rows = filtered(q, rows, predicate)?;
        ids(q, rows, alias)
    }

    fn project(predicate: impl FnOnce(&Relation) -> Option<Expr>) -> ClickHousePlan {
        build(model(), |q| {
            let rows = scan(q, "gl_project", "p")?;
            let predicate = predicate(&rows);
            let rows = filtered(q, rows, predicate)?;
            let rows = q.limit(rows, Some(10), None)?;
            ids(q, rows, "p")
        })
    }

    fn wrapped(inner: impl FnOnce(Builder<'_, '_>) -> Built<Relation>) -> ClickHousePlan {
        build(model(), |q| {
            let body = q.subquery("sq", inner)?;
            let rows = q.read(&body, Some("sq"))?;
            ids(q, rows, "sq")
        })
    }

    fn rejected(result: Result<()>) {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("missing valid traversal_path filter"),
            "{error}"
        );
    }

    #[test]
    fn passes_after_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        assert!(secured(project(|_| None), &ctx, model()).is_ok());
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        rejected(check(&project(|_| Some(literal(true))), &ctx, model()));
    }

    #[test]
    fn rejects_path_filters_that_do_not_restrict_every_result() {
        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let cases: [(Predicate, bool); 7] = [
            (
                |p| binary(BinaryOp::Or, path(p, "p", "42/43/"), literal(true)),
                false,
            ),
            (
                |p| Expr::Unary {
                    op: UnaryOp::Not,
                    value: Box::new(path(p, "p", "42/43/")),
                },
                false,
            ),
            (
                |p| binary(BinaryOp::Eq, path(p, "p", "42/43/"), literal(false)),
                false,
            ),
            (
                |p| {
                    let condition = binary(BinaryOp::Eq, col(p, "p", "id"), literal(1));
                    binary(BinaryOp::Or, path(p, "p", "42/43/"), condition)
                },
                false,
            ),
            (|p| path(p, "p", "42/"), false),
            (
                |p| starts_with(text("42/43/"), col(p, "p", TRAVERSAL_PATH_COLUMN)),
                false,
            ),
            (
                |p| {
                    let condition = binary(BinaryOp::Eq, col(p, "p", "id"), literal(1));
                    let narrowed = binary(BinaryOp::And, path(p, "p", "42/43/"), condition);
                    binary(BinaryOp::Or, path(p, "p", "42/43/"), narrowed)
                },
                true,
            ),
        ];
        for (index, (predicate, valid)) in cases.into_iter().enumerate() {
            let plan = project(|p| Some(predicate(p)));
            assert_eq!(
                check(&plan, &context, model()).is_ok(),
                valid,
                "case {index}"
            );
        }
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        rejected(check(
            &project(|p| Some(path(p, "p", "99/"))),
            &ctx,
            model(),
        ));
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        assert!(secured(project(|_| None), &ctx, model()).is_ok());
    }

    /// An AND-chain containing `Bool(false)` short-circuits to zero rows, so
    /// the post-check must accept the query even when no per-alias
    /// `startsWith` filter is present. The security pass emits this shape
    /// when an alias has no eligible traversal paths (e.g. a Reporter-only
    /// user hitting an entity that requires Security Manager).
    #[test]
    fn accepts_bool_false_as_dead_alias_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = project(|_| Some(binary(BinaryOp::And, literal(false), literal(true))));
        assert!(check(&plan, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_reporter_path_for_security_manager_table() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let plan = build(model(), |q| {
            table(q, "gl_vulnerability", "v", |v| Some(path(v, "v", "1/100/")))
        });
        let error = check(&plan, &context, model()).unwrap_err();
        assert!(error.to_string().contains("access level 25"));
    }

    #[test]
    fn validates_role_floor_for_denormalized_join() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let model = reviewer_project_model();
        let plan = build(&model, |q| {
            let rows = scan(q, "gl_denorm_reviewer_project", "d")?;
            let predicate = path(&rows, "d", "1/100/");
            let rows = q.filter(rows, predicate)?;
            q.select(rows, Vec::new())
        });
        let error = check(&plan, &context, &model).unwrap_err();
        assert!(error.to_string().contains("d.t2_traversal_path"));
        assert!(error.to_string().contains("access level 25"));
    }

    /// A `col = false` comparison must NOT be treated as a proof that the
    /// alias is scoped. Bool(false) short-circuits the clause only when
    /// AND-chained into the top level.
    #[test]
    fn rejects_bool_false_nested_inside_comparison() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = project(|p| {
            Some(binary(
                BinaryOp::Eq,
                col(p, "p", "archived"),
                literal(false),
            ))
        });
        rejected(check(&plan, &ctx, model()));
    }

    /// `Bool(false)` OR-ed with anything is not a dead clause: the OR
    /// arms can still produce rows.
    #[test]
    fn rejects_bool_false_ored_with_true() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = project(|_| Some(binary(BinaryOp::Or, literal(false), literal(true))));
        rejected(check(&plan, &ctx, model()));
    }

    #[test]
    fn accepts_bool_false_in_nested_and_chain() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = project(|p| {
            let id = binary(BinaryOp::Eq, col(p, "p", "id"), literal(5));
            let dead = binary(BinaryOp::And, id, literal(false));
            Some(binary(BinaryOp::And, path(p, "p", "1/"), dead))
        });
        assert!(check(&plan, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_and_chain_with_col_eq_false_and_no_starts_with() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = project(|p| {
            let archived = binary(BinaryOp::Eq, col(p, "p", "archived"), literal(false));
            let positive = binary(BinaryOp::Gt, col(p, "p", "id"), literal(0));
            Some(binary(BinaryOp::And, archived, positive))
        });
        rejected(check(&plan, &ctx, model()));
    }

    #[test]
    fn skips_declared_ctes() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = build(model(), |q| {
            let body = q.cte("path_cte", false, |q| table(q, "gl_user", "u", |_| None))?;
            let rows = q.read(&body, Some("c"))?;
            ids(q, rows, "c")
        });
        assert!(check(&plan, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_unknown_graph_tables() {
        for table in ["gl_unknown", "v42_gl_unknown"] {
            let error = model().build(|q| scan(q, table, "unknown")).unwrap_err();
            assert!(error.to_string().contains(table));
        }
    }

    #[test]
    fn reports_first_invalid_scan() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = build(model(), |q| {
            let first = scan(q, "gl_project", "first")?;
            let second = scan(q, "gl_merge_request", "second")?;
            let rows = q.join(first, second, JoinKind::Inner(literal(true)))?;
            q.select(rows, Vec::new())
        });
        let error = check(&plan, &ctx, model()).unwrap_err();
        assert!(error.to_string().contains("first.traversal_path"));
    }

    #[test]
    fn subqueries_need_their_own_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let unfiltered = wrapped(|q| table(q, "gl_project", "p", |_| None));
        rejected(check(&unfiltered, &ctx, model()));
        let filtered = wrapped(|q| table(q, "gl_project", "p", |p| Some(path(p, "p", "42/43/"))));
        assert!(check(&filtered, &ctx, model()).is_ok());
    }

    fn counted(q: Builder<'_, '_>, guarded: bool) -> Built<Relation> {
        let rows = scan(q, "gl_project", "p")?;
        let predicate = guarded.then(|| path(&rows, "p", "42/43/"));
        let rows = filtered(q, rows, predicate)?;
        let count = || Measure {
            function: AggregateFunction::Count,
            args: vec![col(&rows, "p", "id")],
            distinct: false,
            condition: None,
        };
        let measures = vec![count()];
        let visibility = col(&rows, "p", "visibility_level");
        let aggregated = q.aggregate(rows, vec![visibility], measures)?;
        let total = aggregated.measures[0].expr();
        let rows = q.filter(
            aggregated.rows,
            binary(BinaryOp::Gt, total.clone(), literal(1)),
        )?;
        let (rows, columns) = q.compute(rows, vec![Declare::named("id", total)])?;
        q.select(rows, columns)
    }

    #[test]
    fn aggregate_subqueries_need_their_own_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        rejected(check(&wrapped(|q| counted(q, false)), &ctx, model()));
        assert!(check(&wrapped(|q| counted(q, true)), &ctx, model()).is_ok());
    }

    #[test]
    fn accepts_subquery_wrapping_cte() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = wrapped(|q| {
            let body = q.cte("dedup_cte", false, |q| table(q, "gl_user", "u", |_| None))?;
            let rows = q.read(&body, Some("d"))?;
            ids(q, rows, "d")
        });
        assert!(check(&plan, &ctx, model()).is_ok());
    }

    fn protected(q: Builder<'_, '_>) -> Built<Relation> {
        table(q, "gl_project", "protected", |_| None)
    }

    fn unscoped(q: Builder<'_, '_>, alias: &str) -> Built<Relation> {
        let body = q.subquery(alias, |q| table(q, "gl_user", "u", |_| None))?;
        q.read(&body, Some(alias))
    }

    fn result(q: Builder<'_, '_>, rows: Relation, value: Expr) -> Built<Relation> {
        let (rows, columns) = q.compute(rows, vec![Declare::named("result", value)])?;
        q.select(rows, columns)
    }

    fn nested(q: Builder<'_, '_>, position: &str) -> Built<Relation> {
        let scalar = q.subquery("subquery", protected)?.scalar();
        let one = || literal(1);
        let rows = match position {
            "join" => {
                let outer = unscoped(q, "outer")?;
                let other = unscoped(q, "other")?;
                let on = binary(BinaryOp::Eq, scalar.clone(), one());
                q.join(outer, other, JoinKind::Inner(on))?
            }
            "derived" => {
                let body = q.subquery("derived", protected)?;
                q.read(&body, Some("derived"))?
            }
            "table_union" => {
                let body = q.subquery("arms", |q| {
                    let arm = protected(q)?;
                    q.union(vec![arm])
                })?;
                q.read(&body, Some("arms"))?
            }
            _ => scan(q, "gl_user", "u")?,
        };
        match position {
            "select" => result(q, rows, scalar),
            "where" | "having" => {
                let rows = q.filter(rows, binary(BinaryOp::Eq, scalar, one()))?;
                result(q, rows, one())
            }
            "group" => {
                let aggregated = q.aggregate(rows, vec![scalar], Vec::new())?;
                result(q, aggregated.rows, one())
            }
            "order" | "limit_by" => {
                let (rows, columns) = q.compute(rows, vec![Declare::named("result", one())])?;
                let rows = match position {
                    "order" => {
                        let order = Order {
                            value: scalar,
                            descending: false,
                        };
                        q.order(rows, vec![order])?
                    }
                    _ => q.limit_by(rows, 1, vec![scalar])?,
                };
                q.select(rows, columns)
            }
            "in_operand" => {
                let lookup = q.subquery("subquery", |q| {
                    let rows = unscoped(q, "lookup")?;
                    ids(q, rows, "lookup")
                })?;
                let rows = q.filter(rows, lookup.contains(scalar))?;
                result(q, rows, one())
            }
            "lambda" => {
                let lambda = Expr::Lambda {
                    param: "x".into(),
                    body: Box::new(scalar),
                };
                let array = Expr::Call {
                    function: Function::Array,
                    args: vec![one()],
                };
                let mapped = Expr::Call {
                    function: Function::ArrayMap,
                    args: vec![lambda, array],
                };
                result(q, rows, mapped)
            }
            "nested_cte" => {
                q.cte("outer_cte", false, |q| {
                    let inner = q.cte("inner_cte", false, protected)?;
                    let rows = q.read(&inner, Some("nested"))?;
                    q.select(rows, Vec::new())
                })?;
                result(q, rows, one())
            }
            "union" => {
                let arm = result(q, rows, one())?;
                let other = protected(q)?;
                q.union(vec![arm, other])
            }
            _ => result(q, rows, one()),
        }
    }

    #[test]
    fn security_injection_and_check_cover_nested_query_positions() {
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
            let plan = build(model(), |q| nested(q, position));
            assert!(check(&plan, &context, model()).is_err(), "{position}");
            let plan = secured(build(model(), |q| nested(q, position)), &context, model())
                .unwrap_or_else(|error| panic!("{position}: {error}"));
            let sql = model().render(&plan).sql;
            assert_eq!(sql.matches("startsWith(").count(), 1, "{position}: {sql}");
            assert!(
                sql.contains("startsWith(protected.traversal_path, "),
                "{position}: {sql}"
            );
        }
    }

    fn projects(guarded: bool) -> ClickHousePlan {
        build(model(), |q| {
            let first = table(q, "gl_project", "u", |u| {
                guarded.then(|| path(u, "u", "1/"))
            })?;
            let second = table(q, "gl_project", "p", |_| None)?;
            q.union(vec![first, second])
        })
    }

    #[test]
    fn union_arms_each_need_a_security_filter() {
        let reporter = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        rejected(check(&projects(true), &reporter, model()));
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        assert!(secured(projects(false), &ctx, model()).is_ok());
    }

    fn shared(guard: Option<&str>) -> ClickHousePlan {
        build(model(), |q| {
            let body = q.cte("base", false, |q| {
                let rows = scan(q, "gl_project", "p")?;
                let predicate = guard.map(|prefix| path(&rows, "p", prefix));
                let rows = filtered(q, rows, predicate)?;
                let id = col(&rows, "p", "id");
                let (rows, columns) = q.compute(rows, vec![Declare::named("node_id", id)])?;
                q.select(rows, columns)
            })?;
            let rows = q.read(&body, Some("b"))?;
            let node = rows.qualified("b", "node_id")?;
            q.select(rows, vec![node])
        })
    }

    #[test]
    fn shared_bodies_need_a_security_filter() {
        let reporter = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        rejected(check(&shared(None), &reporter, model()));
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        assert!(check(&shared(Some("42/43/")), &ctx, model()).is_ok());
    }

    fn edge_union(
        arm: (&'static str, &'static str, &'static str),
        prefix: Option<&str>,
    ) -> ClickHousePlan {
        let (table_name, alias, column) = arm;
        build(model(), |q| {
            let edges = scan(q, "gl_edge", "e")?;
            let body = q.subquery("arms", |q| {
                let rows = scan(q, table_name, alias)?;
                let predicate = prefix.map(|prefix| path(&rows, alias, prefix));
                let rows = filtered(q, rows, predicate)?;
                let selected = rows.qualified(alias, column)?;
                let arm = q.select(rows, vec![selected])?;
                q.union(vec![arm])
            })?;
            let arms = q.read(&body, Some("arms"))?;
            let rows = q.join(edges, arms, JoinKind::Inner(literal(true)))?;
            let predicate = path(&rows, "e", prefix.unwrap_or("1/"));
            let rows = q.filter(rows, predicate)?;
            let source = rows.qualified("e", "source_id")?;
            q.select(rows, vec![source])
        })
    }

    #[test]
    fn joined_union_arms_need_a_security_filter() {
        let reporter = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        rejected(check(
            &edge_union(("gl_project", "p", "id"), None),
            &reporter,
            model(),
        ));
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let plan = edge_union(("gl_edge", "e1", "source_id"), Some("42/43/"));
        assert!(check(&plan, &ctx, model()).is_ok());
    }
}
