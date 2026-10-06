//! Hydration emit: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity latest-row scans. Each arm dedups with
//! `LIMIT 1 BY <sort_key>` over a plain scan (not `FINAL`), keeping column
//! pruning and projections, then filters `_deleted = false`.
//!
//! When the base query provided traversal paths, each arm injects a
//! `startsWith(traversal_path, tp)` predicate so ClickHouse can prune
//! granules via the primary key (sort key starts with `traversal_path`).

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::sql::{deleted_false, latest_row_dedup};
use crate::passes::plan::{HydrationNodePlan, hydration::HydrationPathFilter};

use orbit_utils::traversal_path::TraversalPath;

pub fn emit_hydration(nodes: &[HydrationNodePlan], limit: u32) -> Result<Node> {
    let mut arms = nodes.iter().map(emit_arm);
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))?;
    for arm in arms {
        first.union_all.push(arm);
    }
    first.limit = Some(limit);
    Ok(Node::Query(Box::new(first)))
}

fn emit_arm(node: &HydrationNodePlan) -> Query {
    let alias = &node.alias;
    let pk = &node.id_property;

    let json_expr = if node.columns.is_empty() {
        Expr::string("{}")
    } else {
        let map_args: Vec<Expr> = node
            .columns
            .iter()
            .flat_map(|col| {
                [
                    Expr::string(col),
                    Expr::func(Function::ToString, vec![Expr::col(alias, col)]),
                ]
            })
            .collect();
        Expr::func(
            Function::ToJson,
            vec![Expr::func(Function::Object, map_args)],
        )
    };

    let mut scan_where = Vec::new();

    let path_filter = match &node.path_filter {
        Some(HydrationPathFilter::PrefixUnion(paths)) => or_starts_with(alias, paths),
        Some(HydrationPathFilter::PrefixSet(paths)) => Some(array_exists_starts_with(alias, paths)),
        None => None,
    };
    if let Some(tp_filter) = path_filter {
        scan_where.push(tp_filter);
    }

    if let Some(id_filter) = Expr::col_in(
        alias,
        pk,
        SqlType::Int64,
        node.node_ids
            .iter()
            .map(|id| serde_json::Value::Number((*id).into()))
            .collect(),
    ) {
        scan_where.push(id_filter);
    }

    let mut inner_select = vec![
        SelectExpr::col(alias, pk),
        SelectExpr::col(alias, DELETED_COLUMN),
    ];
    for col in &node.columns {
        if col != pk && col != DELETED_COLUMN {
            inner_select.push(SelectExpr::col(alias, col));
        }
    }
    let (order_by, limit_by) = latest_row_dedup(alias, &node.sort_key);
    let keys = Query {
        select: inner_select,
        from: TableRef::scan(&node.table, alias),
        where_clause: Expr::conjoin(scan_where),
        order_by,
        limit_by,
        ..Default::default()
    };
    Query {
        select: vec![
            SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
            SelectExpr::new(Expr::string(&node.entity), format!("{alias}_entity_type")),
            SelectExpr::new(json_expr, format!("{alias}_props")),
        ],
        from: TableRef::subquery(keys, alias),
        where_clause: Some(deleted_false(alias)),
        ..Default::default()
    }
}

fn or_starts_with(alias: &str, paths: &[TraversalPath]) -> Option<Expr> {
    or_balanced(paths.iter().map(|tp| starts_with_path(alias, tp)).collect())
}

fn starts_with_path(alias: &str, tp: &TraversalPath) -> Expr {
    Expr::func(
        Function::StartsWith,
        vec![
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            Expr::string(tp.as_str()),
        ],
    )
}

fn array_exists_starts_with(alias: &str, paths: &[TraversalPath]) -> Expr {
    let lambda_param = "_gkg_path";
    Expr::func(
        Function::ArrayExists,
        vec![
            Expr::lambda(
                lambda_param,
                Expr::func(
                    Function::StartsWith,
                    vec![
                        Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                        Expr::ident(lambda_param),
                    ],
                ),
            ),
            Expr::param(
                SqlType::String.to_array(),
                serde_json::Value::Array(
                    paths
                        .iter()
                        .map(|p| serde_json::Value::String(p.as_str().to_string()))
                        .collect(),
                ),
            ),
        ],
    )
}

fn or_balanced(mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => {
            let right = exprs.split_off(exprs.len() / 2);
            let left = exprs;
            Some(Expr::binary(
                Op::Or,
                or_balanced(left)?,
                or_balanced(right)?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ColumnSelection, Input, InputNode, QueryType};
    use crate::passes::codegen::codegen;
    use crate::passes::enforce::ResultContext;
    use crate::passes::plan::{HydrationCompileOptions, plan_clickhouse};
    use orbit_server_config::QueryConfig;
    use std::sync::Arc;

    fn render(node: &Node) -> String {
        codegen(node, ResultContext::new(), QueryConfig::default())
            .unwrap()
            .sql
    }

    fn render_with_params(
        node: &Node,
    ) -> (
        String,
        std::collections::HashMap<String, crate::passes::codegen::ParamValue>,
    ) {
        let q = codegen(node, ResultContext::new(), QueryConfig::default()).unwrap();
        (q.sql, q.params)
    }

    fn plan(columns: Vec<&str>, node_ids: Vec<i64>, traversal_paths: Vec<&str>) -> InputNode {
        InputNode {
            id: "hydrate".into(),
            entity: Some("MergeRequest".into()),
            node_ids,
            columns: Some(ColumnSelection::List(
                columns.into_iter().map(String::from).collect(),
            )),
            traversal_paths: traversal_paths
                .into_iter()
                .map(TraversalPath::new_unchecked)
                .collect(),
            ..Default::default()
        }
    }

    fn emit_dynamic(plans: &[InputNode], limit: u32) -> Node {
        emit(plans, limit, true)
    }

    fn emit_static(plans: &[InputNode], limit: u32) -> Node {
        emit(plans, limit, false)
    }

    fn emit(nodes: &[InputNode], limit: u32, dynamic: bool) -> Node {
        let model = query_data_model::ClickHouseDataModel::derive(Arc::new(
            ontology::Ontology::load_embedded().unwrap(),
        ))
        .unwrap();
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: nodes.to_vec(),
            limit,
            ..Default::default()
        };
        let plan = plan_clickhouse(
            &input,
            &model,
            HydrationCompileOptions {
                dynamic,
                path_segment_budget: None,
            },
        )
        .unwrap();
        super::super::emit(&plan, &input).unwrap().ast
    }

    #[test]
    fn large_dynamic_tp_sets_emit_array_exists() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let plan = InputNode {
            traversal_paths: paths.clone(),
            ..plan(vec!["title"], vec![1], vec![])
        };

        let node = emit_dynamic(&[plan], 10);
        let (sql, params) = render_with_params(&node);

        assert!(
            sql.contains("arrayExists"),
            "large dynamic TP sets should use arrayExists: {sql}"
        );
        assert_eq!(
            sql.matches("startsWith").count(),
            1,
            "arrayExists should keep one startsWith in the lambda: {sql}"
        );
        let array_params: Vec<_> = params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .collect();
        assert_eq!(
            array_params.len(),
            1,
            "expected one traversal-path array param"
        );
        assert_eq!(array_params[0].len(), paths.len());
    }

    #[test]
    fn large_static_tp_sets_emit_or() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let plan = InputNode {
            traversal_paths: paths,
            ..plan(vec!["title"], vec![1], vec![])
        };

        let node = emit_static(&[plan], 10);
        let sql = render(&node);

        assert!(
            !sql.contains("arrayExists"),
            "static TP sets should keep OR startsWith: {sql}"
        );
        assert_eq!(sql.matches("startsWith").count(), 257);
    }

    #[test]
    fn dynamic_tp_filter_precedes_id_filter() {
        let node = emit_dynamic(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }

    #[test]
    fn static_tp_filter_precedes_id_filter() {
        let node = emit_static(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }
}
