use serde_json::Value;

use super::request::{Metric, Node, Request};
use crate::plan::logical::{
    Binding, Columns, Expr, LogicalBuilder, Measure, Order, PlanError, Projection, Rows, TimeUnit,
};
use crate::plan::tracking::Named;
use crate::surface::SurfaceError;

type Built<'m> = Result<Rows<'m>, SurfaceError>;

pub(super) fn reference(spec: &str) -> (&str, Option<&str>) {
    match spec.split_once('.') {
        Some((node, property)) => (node, Some(property)),
        None => (spec, None),
    }
}

pub(super) fn descending(spec: &str) -> (&str, Order) {
    match spec.strip_prefix('-') {
        Some(spec) => (spec, Order::Descending),
        None => (spec, Order::Ascending),
    }
}

pub(super) fn aggregate<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    request: &Request,
) -> Built<'m> {
    let groups = request
        .group_by
        .iter()
        .map(|key| group(&rows, key))
        .collect::<Result<Vec<_>, _>>()?;
    let measures = request
        .aggregations
        .iter()
        .map(|metric| measure(&rows, metric))
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<(String, Option<&str>)> = groups
        .iter()
        .zip(&request.group_by)
        .map(|(group, key)| {
            let node = matches!(group.item, Expr::Node(_)).then(|| reference(key_spec(key)).0);
            (group.name.clone(), node)
        })
        .chain(measures.iter().map(|measure| (measure.name.clone(), None)))
        .collect();
    let mut rows = q.aggregate(rows, groups, measures)?;
    if let Some(sort) = &request.aggregation_sort {
        let (column, order) = descending(sort);
        let key = rows.column(column)?;
        rows = q.order(rows, [(key, order)])?;
    }
    let items = names
        .into_iter()
        .map(|(name, node)| match node {
            Some(alias) => {
                let binding = rows.binding(&name)?;
                let declared = request.nodes.iter().find(|node| node.id == alias);
                Ok(Projection::Graph {
                    binding: binding.output(),
                    columns: columns(binding, declared.and_then(|node| node.columns.as_ref()))?,
                }
                .named(name))
            }
            None => Ok(Projection::Value(rows.column(&name)?).named(name)),
        })
        .collect::<Result<Vec<_>, SurfaceError>>()?;
    Ok(q.select(rows, items)?)
}

fn key_spec(key: &Value) -> &str {
    key.as_str()
        .or_else(|| key.get("key").and_then(Value::as_str))
        .unwrap_or_default()
}

fn group(rows: &Rows<'_>, key: &Value) -> Result<Named<Expr>, SurfaceError> {
    let spec = key_spec(key);
    let truncate = key.get("truncate").and_then(Value::as_str);
    let alias = key.get("as").and_then(Value::as_str);
    let (node, property) = reference(spec);
    let binding = rows.binding(node)?;
    let (expr, derived) = match (property, truncate) {
        (None, None) => (binding.node(), node.to_string()),
        (None, Some(unit)) => {
            return Err(SurfaceError::schema(format!(
                "truncate \"{unit}\" target \"{spec}\" must be \"node.property\""
            )));
        }
        (Some(property), None) => (binding.property(property)?, format!("{node}_{property}")),
        (Some(property), Some(unit)) => {
            let parsed: TimeUnit = unit
                .parse()
                .map_err(|_| SurfaceError::schema(format!("unknown truncation unit {unit}")))?;
            (
                binding.truncate(property, parsed)?,
                format!("{node}_{property}_{unit}"),
            )
        }
    };
    Ok(expr.named(alias.map_or(derived, str::to_string)))
}

fn measure(rows: &Rows<'_>, metric: &Metric) -> Result<Named<Measure>, SurfaceError> {
    let functions = [
        ("count", &metric.count),
        ("sum", &metric.sum),
        ("avg", &metric.avg),
        ("min", &metric.min),
        ("max", &metric.max),
        ("collect", &metric.collect),
    ];
    let Some((function, Some(spec))) = functions.into_iter().find(|(_, spec)| spec.is_some())
    else {
        return Err(SurfaceError::schema("an aggregation names one function"));
    };
    if function == "collect" {
        return Err(SurfaceError::schema(
            "aggregation \"collect\" is not supported",
        ));
    }
    if metric.distinct && function != "count" {
        return Err(SurfaceError::schema(format!(
            "aggregation \"{function}\": \"distinct\" works only with \"count\""
        )));
    }
    let (node, property) = reference(spec);
    let binding = rows.binding(node)?;
    let input = match property {
        Some(property) => binding.property(property)?,
        None => binding.node(),
    };
    let measure = match (function, metric.distinct) {
        ("count", false) => Measure::Count(Some(input)),
        ("count", true) => Measure::CountDistinct(input),
        ("sum", _) => Measure::Sum(input),
        ("avg", _) => Measure::Avg(input),
        ("min", _) => Measure::Min(input),
        _ => Measure::Max(input),
    };
    let function = if metric.distinct {
        "count_distinct"
    } else {
        function
    };
    let derived = match property {
        Some(property) => format!("{function}_{node}_{property}"),
        None => format!("{function}_{node}"),
    };
    Ok(measure.named(metric.alias.clone().unwrap_or(derived)))
}

pub(super) fn columns(
    binding: Binding<'_>,
    columns: Option<&Value>,
) -> Result<Columns, SurfaceError> {
    match columns {
        None => Ok(binding.catalog().default_columns()),
        Some(Value::String(all)) if all == "*" => Ok(Columns::All),
        Some(Value::Array(names)) => {
            let listed = names
                .iter()
                .map(
                    |name| match binding.property(name.as_str().unwrap_or_default())? {
                        Expr::Property { property, .. } => Ok(property),
                        _ => Err(PlanError::NoEntity.into()),
                    },
                )
                .collect::<Result<Vec<_>, SurfaceError>>()?;
            Ok(Columns::Listed(listed))
        }
        Some(other) => Err(SurfaceError::schema(format!(
            "columns must be '*' or an array of column names, got {other}"
        ))),
    }
}

pub(super) fn node_projection(
    rows: &Rows<'_>,
    node: &Node,
) -> Result<Named<Projection>, SurfaceError> {
    let binding = rows.binding(&node.id)?;
    let columns = columns(binding, node.columns.as_ref())?;
    Ok(Projection::Graph {
        binding: binding.output(),
        columns,
    }
    .named(&node.id))
}
