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
use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Function, Order, Relation, RelationalBuilder,
};
use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::passes::lower::build::expr::{in_list, live};
use crate::passes::lower::build::predicates::{Lowering, Shared};
use crate::passes::lower::build::{
    Built, call, col, computed, disjoin, filtered, scan, stored, string, versioning,
};
use crate::passes::plan::predicates::{Column, OutputValue};
use crate::passes::plan::query_types::hydration::HydrationPathFilter;
use crate::passes::plan::{Hydration, HydrationNodePlan, Plan};

pub(crate) fn check(plan: &Plan<Hydration>) -> Result<()> {
    match plan.operation.nodes.is_empty() {
        true => Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        )),
        false => Ok(()),
    }
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<Hydration>,
) -> Built<Relation> {
    match plan.operation.nodes.as_slice() {
        [node] => arm(q, node),
        nodes => {
            let mut arms = Vec::new();
            for node in nodes {
                arms.push(arm(q, node)?);
            }
            q.union(arms)
        }
    }
}

fn arm<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    node: &HydrationNodePlan,
) -> Built<Relation> {
    let alias = &node.alias;
    let pk = &node.id_property;
    let keys = q.subquery(alias, |q| {
        let rows = scan(q, &node.table, alias, false)?;
        let scope = [&rows];
        let mut predicates = Vec::new();
        let path_filter = match &node.path_filter {
            Some(HydrationPathFilter::PrefixUnion(paths)) => {
                let mut matches = Vec::new();
                for path in paths {
                    matches.push(starts_with(&scope, alias, path)?);
                }
                disjoin(matches)
            }
            Some(HydrationPathFilter::PrefixSet(paths)) => {
                Some(array_exists(&scope, alias, paths)?)
            }
            None => None,
        };
        predicates.extend(path_filter.map(Some));
        let ids = node.node_ids.iter().map(|id| Value::from(*id)).collect();
        let id = col(&scope, alias, pk)?;
        predicates.extend(in_list::<B>(id, SqlType::Int64, ids).map(Some));
        let rows = filtered(q, rows, predicates)?;
        let mut names = vec![pk.as_str(), DELETED_COLUMN];
        names.extend(
            node.columns
                .iter()
                .map(String::as_str)
                .filter(|column| column != pk && *column != DELETED_COLUMN),
        );
        let mut items = Vec::new();
        for name in names
            .into_iter()
            .filter(|name| !stored::<B>() || !versioning(name))
        {
            items.push((name.to_string(), col(&[&rows], alias, name)?));
        }
        let (rows, columns) = computed(q, rows, items)?;
        let keys = node
            .sort_key
            .iter()
            .map(|key| rows.qualified(alias, key))
            .collect::<Built<Vec<_>>>()?;
        let rows = match stored::<B>() {
            true => {
                let orders = keys.into_iter().map(|key| Order {
                    value: key.expr(),
                    descending: false,
                });
                q.order(rows, orders.collect())?
            }
            false => {
                let version = rows.qualified(alias, VERSION_COLUMN)?;
                q.latest(rows, keys, version)?
            }
        };
        q.select(rows, columns)
    })?;
    let rows = q.read(&keys, Some(alias))?;
    let deleted = match stored::<B>() {
        true => None,
        false => Some(live::<B>(&[&rows], alias)?),
    };
    let rows = filtered(q, rows, vec![deleted])?;
    let shared = Shared::default();
    let lowering = Lowering {
        scope: &[&rows],
        shared: &shared,
    };
    let props = match node.columns.is_empty() {
        true => string("{}"),
        false => {
            let mut args = Vec::new();
            for column in &node.columns {
                let value = OutputValue::property(&node.null_as_epoch, Column::new(alias, column));
                args.push(string(column));
                args.push(call(
                    Function::ToString,
                    vec![lowering.output::<B>(&value)?],
                ));
            }
            call(Function::ToJson, vec![call(Function::Object, args)])
        }
    };
    let items = vec![
        (format!("{alias}_{pk}"), col(&[&rows], alias, pk)?),
        (format!("{alias}_entity_type"), string(&node.entity)),
        (format!("{alias}_props"), props),
    ];
    let (rows, columns) = computed(q, rows, items)?;
    q.select(rows, columns)
}

fn starts_with(scope: &[&Relation], alias: &str, path: &TraversalPath) -> Built<Expr> {
    let column = col(scope, alias, TRAVERSAL_PATH_COLUMN)?;
    Ok(call(
        Function::StartsWith,
        vec![column, string(path.as_str())],
    ))
}

fn array_exists(scope: &[&Relation], alias: &str, paths: &[TraversalPath]) -> Built<Expr> {
    let variable = "_orbit_path";
    let column = col(scope, alias, TRAVERSAL_PATH_COLUMN)?;
    let body = call(
        Function::StartsWith,
        vec![column, Expr::Variable(variable.to_string())],
    );
    let lambda = Expr::Lambda {
        param: variable.to_string(),
        body: Box::new(body),
    };
    let paths = paths
        .iter()
        .map(|path| Value::String(path.as_str().to_string()))
        .collect();
    let paths = Expr::Parameter {
        data_type: SqlType::String.to_array(),
        value: Value::Array(paths),
    };
    Ok(call(Function::ArrayExists, vec![lambda, paths]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ColumnSelection, Input, InputNode, QueryType};
    use crate::passes::codegen::ParamValue;
    use crate::passes::plan::{HydrationCompileOptions, plan};
    use query_data_model::ClickHouseDataModel;
    use std::collections::HashMap;
    use std::sync::LazyLock;

    static MODEL: LazyLock<ClickHouseDataModel> =
        LazyLock::new(|| ClickHouseDataModel::embedded().unwrap());

    fn node(columns: Vec<&str>, node_ids: Vec<i64>, traversal_paths: Vec<&str>) -> InputNode {
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

    fn render(nodes: &[InputNode], dynamic: bool) -> (String, HashMap<String, ParamValue>) {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: nodes.to_vec(),
            ..Default::default()
        };
        let options = HydrationCompileOptions {
            dynamic,
            path_segment_budget: None,
        };
        let planned = plan(None, &input, None, &*MODEL, options).unwrap();
        let rendered = MODEL.catalog().render(&planned.plan);
        (rendered.sql, rendered.parameters)
    }

    #[test]
    fn large_dynamic_tp_sets_emit_array_exists() {
        let paths: Vec<TraversalPath> = (0..=256)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect();
        let node = InputNode {
            traversal_paths: paths.clone(),
            ..node(vec!["title"], vec![1], vec![])
        };

        let (sql, params) = render(&[node], true);

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
                Value::Array(items) => Some(items),
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
        let node = InputNode {
            traversal_paths: paths,
            ..node(vec!["title"], vec![1], vec![])
        };

        let (sql, _) = render(&[node], false);

        assert!(
            !sql.contains("arrayExists"),
            "static TP sets should keep OR startsWith: {sql}"
        );
        assert_eq!(sql.matches("startsWith").count(), 257);
    }

    #[test]
    fn tp_filter_precedes_id_filter() {
        for dynamic in [true, false] {
            let (sql, _) = render(&[node(vec!["title"], vec![1], vec!["1/9970/"])], dynamic);
            let tp_pos = sql.find("startsWith").unwrap();
            let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
            assert!(
                tp_pos < in_pos,
                "TP filter should precede ID filter for primary key pruning: {sql}"
            );
        }
    }
}
