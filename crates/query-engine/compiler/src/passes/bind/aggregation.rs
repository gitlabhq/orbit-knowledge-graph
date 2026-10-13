use std::collections::HashSet;

use ontology::DataType;
use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{
    InputAggregation, InputGroupByKey, InputNeighbors, InputOrderBy, InputPath, JoinPredicate,
};
use crate::query::{
    AggFunction, AggregationSort, Comparison, Group, Metric, Neighbors, Order, Path, Query,
    QueryType, TruncateUnit,
};
use crate::query_limits::{MAX_DEPTH_CAP, MAX_REL_TYPES};

use super::properties::{bind_property_ref, resolve_node_property};
use super::validation::{check_comparison, check_reserved_prefix, validate_identifier};

pub(super) fn bind_aggregation<M: OrbitQueryModel>(
    query: &mut Query,
    input: InputAggregation,
    model: &M,
) -> Result<()> {
    if query.query_type != QueryType::Aggregation {
        if !input.metrics.is_empty() || !input.group_by.is_empty() || input.sort.is_some() {
            return Err(QueryError::Validation(
                "aggregations, group_by, and aggregation_sort require an aggregation query".into(),
            ));
        }
        return Ok(());
    }
    if input.metrics.is_empty() {
        return Err(QueryError::Validation(
            "aggregation requires at least one metric".into(),
        ));
    }
    let mut outputs = HashSet::new();
    for (index, input_group) in input.group_by.into_iter().enumerate() {
        let alias = input_group.output_name();
        if !register_output(&mut outputs, &alias)? {
            return Err(QueryError::Validation(format!(
                "duplicate group_by output column \"{alias}\""
            )));
        }
        let node = query.node_id(input_group.node()).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "group_by[{index}] references undefined node \"{}\"",
                input_group.node()
            ))
        })?;
        let group = match input_group {
            InputGroupByKey::Node { .. } => Group::Node { node, alias },
            InputGroupByKey::Property {
                property, truncate, ..
            } => {
                let property = resolve_node_property(query, node, &property, model)?;
                if !property.filterable || !property.is_stored() {
                    return Err(QueryError::Validation(format!(
                        "group_by[{index}] field \"{}\" must be stored and filterable",
                        property.name
                    )));
                }
                if truncate.is_some()
                    && !matches!(property.data_type, DataType::Date | DataType::DateTime)
                {
                    return Err(QueryError::Validation(format!(
                        "group_by[{index}] truncation requires a Date or DateTime property"
                    )));
                }
                if truncate.is_some_and(TruncateUnit::requires_selectivity_guard)
                    && query.node(node).node_ids.is_empty()
                    && !query.node(node).has_filter(&property.name)
                {
                    return Err(QueryError::Validation(format!(
                        "group_by[{index}] truncate \"{}\" requires either node_ids or a filter on {}",
                        truncate.map(TruncateUnit::name).unwrap_or_default(),
                        property.name
                    )));
                }
                Group::Property {
                    node,
                    property: Box::new(property),
                    truncate,
                    alias,
                }
            }
        };
        query.aggregation.groups.push(group);
    }
    for input_metric in input.metrics {
        let alias = input_metric.output_name();
        if !register_output(&mut outputs, &alias)? {
            return Err(QueryError::Validation(format!(
                "aggregation \"{alias}\" conflicts with another output column"
            )));
        }
        let function = input_metric.expr.function();
        if function == AggFunction::Collect {
            return Err(QueryError::Validation("collect is not supported".into()));
        }
        let target = query.node_id(input_metric.expr.node()).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "aggregation \"{alias}\" references undefined node \"{}\"",
                input_metric.expr.node()
            ))
        })?;
        let property = input_metric
            .expr
            .property()
            .map(|name| resolve_node_property(query, target, name, model))
            .transpose()?;
        if let Some(property) = &property {
            if !property.is_stored() {
                return Err(QueryError::Validation(format!(
                    "aggregation \"{alias}\" requires a stored field"
                )));
            }
            if matches!(function, AggFunction::Sum | AggFunction::Avg)
                && !matches!(property.data_type, DataType::Int | DataType::Float)
            {
                return Err(QueryError::Validation(format!(
                    "aggregation \"{alias}\" requires a numeric field"
                )));
            }
        }
        query.aggregation.metrics.push(Metric {
            function,
            target,
            property,
            alias,
        });
    }
    if let Some(sort) = input.sort {
        if !outputs.contains(&sort.column) {
            return Err(QueryError::ReferenceError(format!(
                "aggregation_sort references unknown output column \"{}\"",
                sort.column
            )));
        }
        query.aggregation.sort = Some(AggregationSort {
            column: sort.column,
            direction: sort.direction,
        });
    }
    Ok(())
}

fn register_output(outputs: &mut HashSet<String>, alias: &str) -> Result<bool> {
    validate_identifier(alias)?;
    check_reserved_prefix("output column", alias, alias)?;
    Ok(outputs.insert(alias.to_string()))
}

pub(super) fn bind_order<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<InputOrderBy>,
    model: &M,
) -> Result<()> {
    let Some(input) = input else { return Ok(()) };
    if query.query_type != QueryType::Traversal {
        return Err(QueryError::Validation(
            "order_by is only supported for traversal queries".into(),
        ));
    }
    let node = query.node_id(&input.node).ok_or_else(|| {
        QueryError::ReferenceError(format!(
            "order_by references undefined node \"{}\"",
            input.node
        ))
    })?;
    let property = resolve_node_property(query, node, &input.property, model)?;
    if !property.is_stored() {
        return Err(QueryError::Validation(format!(
            "order_by field \"{}\" is not stored",
            property.name
        )));
    }
    query.order = Some(Order {
        node,
        property,
        direction: input.direction,
    });
    Ok(())
}

pub(super) fn bind_path<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<InputPath>,
    model: &M,
) -> Result<()> {
    if query.query_type != QueryType::PathFinding {
        if input.is_some() {
            if query.query_type == QueryType::Aggregation {
                return Err(QueryError::Validation(
                    "aggregation over shortest paths is not supported".into(),
                ));
            }
            return Err(QueryError::Validation(
                "path is only supported for path_finding queries".into(),
            ));
        }
        return Ok(());
    }
    let input =
        input.ok_or_else(|| QueryError::ReferenceError("path_finding requires path".into()))?;
    let from = query
        .node_id(&input.from)
        .ok_or_else(|| QueryError::ReferenceError(format!("undefined path node {}", input.from)))?;
    let to = query
        .node_id(&input.to)
        .ok_or_else(|| QueryError::ReferenceError(format!("undefined path node {}", input.to)))?;
    if input.max_depth == 0 || input.max_depth > MAX_DEPTH_CAP || input.rel_types.is_empty() {
        return Err(QueryError::Validation(format!(
            "path requires relationship types and max_depth between 1 and {MAX_DEPTH_CAP}"
        )));
    }
    let relationship_names = input.rel_types;
    let relationship_ids = relationship_names
        .iter()
        .filter(|name| name.as_str() != "*")
        .map(|name| {
            model.graph().relationship_id(name).ok_or_else(|| {
                QueryError::AllowlistRejected(format!("unknown relationship type {name:?}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tables = if crate::query::is_wildcard(&relationship_names) {
        model.relationship_tables(&[])
    } else {
        model.relationship_tables(&relationship_names)
    };
    query.path = Some(Path {
        from,
        to,
        max_depth: input.max_depth,
        relationship_ids,
        relationship_names,
        tables,
    });
    Ok(())
}

pub(super) fn bind_neighbors<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<InputNeighbors>,
    model: &M,
) -> Result<()> {
    if query.query_type != QueryType::Neighbors {
        if input.is_some() {
            return Err(QueryError::Validation(
                "neighbors config requires a neighbors query".into(),
            ));
        }
        return Ok(());
    }
    if query.nodes.len() != 1 {
        return Err(QueryError::Validation(
            "neighbors requires one center node".into(),
        ));
    }
    let input =
        input.ok_or_else(|| QueryError::ReferenceError("neighbors requires config".into()))?;
    if input.rel_types.len() > MAX_REL_TYPES {
        return Err(QueryError::Validation(format!(
            "neighbors relationship type count ({}) must not exceed {MAX_REL_TYPES}",
            input.rel_types.len()
        )));
    }
    let relationship_names = if crate::query::is_wildcard(&input.rel_types) {
        Vec::new()
    } else {
        input.rel_types
    };
    let relationship_ids = relationship_names
        .iter()
        .map(|name| {
            model.graph().relationship_id(name).ok_or_else(|| {
                QueryError::AllowlistRejected(format!("unknown relationship type {name:?}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tables = model.relationship_tables(&relationship_names);
    query.neighbors = Some(Neighbors {
        direction: input.direction,
        relationship_ids,
        relationship_names,
        tables,
    });
    Ok(())
}

pub(super) fn bind_comparisons<M: OrbitQueryModel>(
    query: &mut Query,
    inputs: Vec<JoinPredicate>,
    model: &M,
) -> Result<()> {
    if !inputs.is_empty() && query.query_type != QueryType::Traversal {
        return Err(QueryError::Validation(
            "property comparisons require a traversal query".into(),
        ));
    }
    for input in inputs {
        let left = bind_property_ref(query, &input.lhs_node, &input.lhs_prop, model)?;
        let right = bind_property_ref(query, &input.rhs_node, &input.rhs_prop, model)?;
        check_comparison(&left, &right)?;
        query.comparisons.push(Comparison {
            left,
            op: input.op,
            right,
        });
    }
    Ok(())
}
