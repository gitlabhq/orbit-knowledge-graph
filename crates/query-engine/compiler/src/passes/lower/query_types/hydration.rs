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

use super::Emitted;
use crate::passes::lower::predicates::output_value;
use crate::passes::lower::sql::{deleted_false, latest_row_dedup};
use crate::passes::plan::predicates::{Column, OutputValue};
use crate::passes::plan::{Hydration, HydrationNodePlan, Plan};

use orbit_utils::traversal_path::TraversalPath;

pub(crate) fn emit(plan: &Plan<Hydration>) -> Result<Emitted> {
    Ok(Emitted::new(emit_hydration(&plan.operation.nodes)?))
}

pub fn emit_hydration(nodes: &[HydrationNodePlan]) -> Result<Node> {
    let mut arms = nodes.iter().map(emit_arm);
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))?;
    for arm in arms {
        first.union_all.push(arm);
    }
    Ok(Node::Query(Box::new(first)))
}

fn emit_arm(node: &HydrationNodePlan) -> Query {
    let (alias, pk) = (&node.alias, &node.id_property);
    let ids = Expr::col_in(
        alias,
        pk,
        SqlType::Int64,
        node.node_ids
            .iter()
            .map(|id| serde_json::Value::Number((*id).into()))
            .collect(),
    );
    latest_row_arm(
        node,
        ids,
        &[],
        vec![
            SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
            SelectExpr::new(Expr::string(&node.entity), format!("{alias}_entity_type")),
            SelectExpr::new(properties_json(node), format!("{alias}_props")),
        ],
    )
}

pub(crate) fn properties_json(node: &HydrationNodePlan) -> Expr {
    if node.columns.is_empty() {
        return Expr::string("{}");
    }
    let map_args: Vec<Expr> = node
        .columns
        .iter()
        .flat_map(|col| {
            let value = OutputValue::property(&node.null_as_epoch, Column::new(&node.alias, col));
            [
                Expr::string(col),
                Expr::func(Function::ToString, vec![output_value(&value)]),
            ]
        })
        .collect();
    Expr::func(
        Function::ToJson,
        vec![Expr::func(Function::Object, map_args)],
    )
}

pub(crate) fn latest_row_arm(
    node: &HydrationNodePlan,
    id_filter: Option<Expr>,
    extra_columns: &[&str],
    select: Vec<SelectExpr>,
) -> Query {
    let (alias, pk) = (&node.alias, &node.id_property);
    let path_filter = node
        .path_filter
        .as_deref()
        .and_then(|paths| or_starts_with(alias, paths));
    let mut inner_columns = vec![pk.as_str(), DELETED_COLUMN];
    for column in node
        .columns
        .iter()
        .map(String::as_str)
        .chain(extra_columns.iter().copied())
    {
        if !inner_columns.contains(&column) {
            inner_columns.push(column);
        }
    }
    let (order_by, limit_by) = latest_row_dedup(alias, &node.sort_key);
    let keys = Query {
        select: inner_columns
            .into_iter()
            .map(|column| SelectExpr::col(alias, column))
            .collect(),
        from: TableRef::scan(&node.table, alias),
        where_clause: Expr::conjoin(path_filter.into_iter().chain(id_filter).collect()),
        order_by,
        limit_by,
        ..Default::default()
    };
    Query {
        select,
        from: TableRef::subquery(keys, alias),
        where_clause: Some(deleted_false(alias)),
        ..Default::default()
    }
}

fn or_starts_with(alias: &str, paths: &[TraversalPath]) -> Option<Expr> {
    Expr::or_all(paths.iter().map(|tp| Some(starts_with_path(alias, tp))))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ColumnSelection, Input, InputNode, QueryType};
    use crate::passes::codegen::codegen;
    use crate::passes::plan::{HydrationCompileOptions, plan_clickhouse};
    use crate::passes::result_contract::ResultContext;
    use orbit_server_config::QueryConfig;
    use std::sync::Arc;

    fn render(node: &Node) -> String {
        codegen(node, ResultContext::new(), QueryConfig::default())
            .unwrap()
            .sql
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

    fn emit(nodes: &[InputNode]) -> Node {
        let model = query_data_model::ClickHouseDataModel::derive(Arc::new(
            ontology::Ontology::load_embedded().unwrap(),
        ))
        .unwrap();
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: nodes.to_vec(),
            ..Default::default()
        };
        let plan = plan_clickhouse(
            &input,
            &model,
            HydrationCompileOptions {
                path_segment_budget: None,
            },
        )
        .unwrap();
        crate::passes::lower::emit(&plan, &input, None).unwrap().ast
    }

    #[test]
    fn large_tp_sets_emit_or() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let plan = InputNode {
            traversal_paths: paths,
            ..plan(vec!["title"], vec![1], vec![])
        };

        let node = emit(&[plan]);
        let sql = render(&node);

        assert!(
            !sql.contains("arrayExists"),
            "TP sets should use OR startsWith: {sql}"
        );
        assert_eq!(sql.matches("startsWith").count(), 257);
    }

    #[test]
    fn tp_filter_precedes_id_filter() {
        let node = emit(&[plan(vec!["title"], vec![1], vec!["1/9970/"])]);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }
}
