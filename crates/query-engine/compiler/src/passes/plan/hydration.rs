//! Hydration: a UNION ALL of per-entity latest-row reads by id. Each arm dedups with
//! `LIMIT 1 BY <sort_key>` over a plain scan (not `FINAL`), keeping column pruning and
//! projections, then filters `_deleted = false`.
//!
//! When the base query provided traversal paths, each arm adds a `startsWith(traversal_path, tp)`
//! predicate so ClickHouse can prune granules via the primary key.

use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};
use query_data_model::Catalog;
use query_data_model::constants::*;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Function, Order, Relation, RelationalBuilder,
};
use query_data_model::plan::logical::{Columns, Projection};
use serde_json::Value;

use super::node::Node;
use crate::error::{QueryError, Result};
use crate::hydration::{HydrationArm, HydrationCompileOptions, HydrationRequest};
use crate::logical::Query;
use crate::passes::codegen::SqlBackend;
use crate::passes::plan::build::expr::{id_list, live};
use crate::passes::plan::build::predicates::Lowering;
use crate::passes::plan::build::{
    Built, call, col, computed, disjoin, filtered, scan, stored, string, versioning,
};
use crate::passes::plan::predicates::{Column, OutputValue};
use crate::passes::plan::sort_key;

const PREFIX_SET_PATH_THRESHOLD: usize = 256;

pub(crate) struct Arm<'p> {
    alias: &'p str,
    entity: &'p str,
    table: &'p str,
    ids: Option<Vec<orbit_authorization::Key>>,
    columns: Vec<(&'p str, Option<query_data_model::DataType>)>,
    path_filter: Option<PathFilter>,
    sort_key: &'p [String],
}

enum PathFilter {
    PrefixUnion(Vec<TraversalPath>),
    PrefixSet(Vec<TraversalPath>),
}

pub(crate) fn arms<'p, B: SqlBackend>(
    request: &'p HydrationRequest,
    model: &'p Catalog<B>,
) -> Result<Vec<Arm<'p>>> {
    if request.arms.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }
    request
        .arms
        .iter()
        .map(|arm| Arm::new(arm, request.options, model))
        .collect()
}

impl<'p> Arm<'p> {
    fn new<B: SqlBackend>(
        arm: &'p HydrationArm,
        options: HydrationCompileOptions,
        model: &'p Catalog<B>,
    ) -> Result<Self> {
        let query = Query::new(&arm.plan, model.model());
        let anchor = query
            .anchors(&query.leaves())
            .into_iter()
            .next()
            .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
        let node = Node::new(query, anchor, model);
        let table = node
            .table
            .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
        let graph = query.model.graph();
        let columns = query
            .selected()
            .into_iter()
            .filter_map(|projection| match projection {
                Projection::Graph {
                    columns: Columns::Listed(listed),
                    ..
                } => Some(listed),
                _ => None,
            })
            .flatten()
            .map(|property| {
                let declared = graph.property(*property);
                let epoch = model.null_as_epoch(*property).then_some(declared.data_type);
                (declared.name.as_str(), epoch)
            })
            .collect();
        Ok(Self {
            alias: node.alias,
            entity: node.entity,
            table,
            ids: node.pinned,
            columns,
            path_filter: path_filter(&arm.traversal_paths, options),
            sort_key: sort_key(model, table)?,
        })
    }
}

fn path_filter(paths: &[TraversalPath], options: HydrationCompileOptions) -> Option<PathFilter> {
    let mut leaves = prune_to_leaves(paths);
    if leaves.is_empty() {
        return None;
    }
    if let Some(budget) = options.path_segment_budget {
        while leaves
            .iter()
            .map(|path| path.segment_count())
            .sum::<usize>()
            > budget
        {
            let parents: Vec<_> = leaves.iter().map(|path| path.parent()).collect();
            if parents == leaves {
                break;
            }
            leaves = prune_to_leaves(&parents);
        }
    }
    Some(
        if options.dynamic && leaves.len() > PREFIX_SET_PATH_THRESHOLD {
            PathFilter::PrefixSet(leaves)
        } else {
            PathFilter::PrefixUnion(leaves)
        },
    )
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    arms: &[Arm<'_>],
) -> Built<Relation> {
    match arms {
        [single] => arm(q, single),
        arms => {
            let mut relations = Vec::new();
            for each in arms {
                relations.push(arm(q, each)?);
            }
            q.union(relations)
        }
    }
}

fn arm<B: RelationalBackend>(q: &mut RelationalBuilder<'_, B>, node: &Arm<'_>) -> Built<Relation> {
    let alias = node.alias;
    let pk = DEFAULT_PRIMARY_KEY;
    let keys = q.subquery(alias, |q| {
        let rows = scan(q, node.table, alias, false)?;
        let scope = [&rows];
        let mut predicates = Vec::new();
        let path_filter = match &node.path_filter {
            Some(PathFilter::PrefixUnion(paths)) => {
                let mut matches = Vec::new();
                for path in paths {
                    matches.push(starts_with(&scope, alias, path)?);
                }
                disjoin(matches)
            }
            Some(PathFilter::PrefixSet(paths)) => Some(array_exists(&scope, alias, paths)?),
            None => None,
        };
        predicates.extend(path_filter.map(Some));
        if let Some(ids) = &node.ids {
            predicates.push(Some(id_list::<B>(col(&scope, alias, pk)?, ids)?));
        }
        let rows = filtered(q, rows, predicates)?;
        let mut names = vec![pk, DELETED_COLUMN];
        names.extend(
            node.columns
                .iter()
                .map(|(column, _)| *column)
                .filter(|column| *column != pk && *column != DELETED_COLUMN),
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
    let lowering = Lowering { scope: &[&rows] };
    let props = match node.columns.is_empty() {
        true => string("{}"),
        false => {
            let mut args = Vec::new();
            for (column, epoch) in &node.columns {
                let column_ref = Column::new(alias, *column);
                let value = match epoch {
                    Some(data_type) => OutputValue::EpochAsNull(column_ref, *data_type),
                    None => OutputValue::Column(column_ref),
                };
                args.push(string(*column));
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
        (format!("{alias}_entity_type"), string(node.entity)),
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
    use crate::passes::codegen::ParamValue;
    use crate::passes::plan::{Request, plan};
    use query_data_model::{Catalog, ClickHouse};
    use std::collections::HashMap;
    use std::sync::LazyLock;

    static MODEL: LazyLock<Catalog<ClickHouse>> =
        LazyLock::new(|| Catalog::<ClickHouse>::embedded().unwrap());

    fn arm(paths: Vec<TraversalPath>) -> HydrationArm {
        let columns = ["title".to_string()];
        HydrationArm::new(
            MODEL.model(),
            crate::testkit::limits(),
            "MergeRequest",
            &[orbit_authorization::Key::Int(1)],
            &columns,
            paths,
        )
        .unwrap()
    }

    fn render(arm: HydrationArm, dynamic: bool) -> (String, HashMap<String, ParamValue>) {
        let request = HydrationRequest {
            arms: vec![arm],
            options: HydrationCompileOptions {
                dynamic,
                path_segment_budget: None,
            },
        };
        let planned = plan(
            Request::Hydration(&request),
            None,
            &*MODEL,
            crate::testkit::limits(),
        )
        .unwrap();
        let rendered = MODEL.render(&planned.plan);
        (rendered.sql, rendered.parameters)
    }

    fn paths(count: usize) -> Vec<TraversalPath> {
        (0..count)
            .map(|id| TraversalPath::new_unchecked(format!("1/9970/{id}/")))
            .collect()
    }

    #[test]
    fn large_dynamic_tp_sets_emit_array_exists() {
        let (sql, params) = render(arm(paths(257)), true);

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
        assert_eq!(array_params[0].len(), 257);
    }

    #[test]
    fn large_static_tp_sets_emit_or() {
        let (sql, _) = render(arm(paths(257)), false);

        assert!(
            !sql.contains("arrayExists"),
            "static TP sets should keep OR startsWith: {sql}"
        );
        assert_eq!(sql.matches("startsWith").count(), 257);
    }

    #[test]
    fn tp_filter_precedes_id_filter() {
        for dynamic in [true, false] {
            let (sql, _) = render(arm(vec![TraversalPath::new_unchecked("1/9970/")]), dynamic);
            let tp_pos = sql.find("startsWith").unwrap();
            let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
            assert!(
                tp_pos < in_pos,
                "TP filter should precede ID filter for primary key pruning: {sql}"
            );
        }
    }
}
