use super::{cursor_column, decode, nullable_flags};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::{FilterOp, Input, OrderDirection, QueryType};
use crate::query_graph::{
    BlockId, ColumnRef, Expression, GraphError, OperationKind, QueryGraph, ReadMode, ValueType,
};
use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;
use std::convert::Infallible;

pub fn apply_graph<'a, M: QueryDataModel + ?Sized>(
    mut graph: QueryGraph<'a, M, Infallible>,
    root: BlockId,
    input: &Input,
    query_hash: u64,
) -> Result<(QueryGraph<'a, M, Infallible>, BlockId, usize)> {
    if input.cursor.is_none() && input.aggregation.sort.is_none() {
        graph = graph.rewrite_query(root, |graph, query| {
            let (operation, outputs) = query.into_projection()?;
            let OperationKind::Limit { input: source, .. } = operation.into_kind() else {
                return Err(GraphError::OperationVisibility);
            };
            graph.project_values(graph.limit_relation(*source, input.fetch_limit())?, outputs)
        })?;
        return Ok((graph, root, 0));
    }
    let keys = cursor_keys(&graph, root, input)?;
    let outputs = graph.outputs(root)?.collect::<Vec<_>>();
    let key_outputs = graph.extend_result(
        root,
        keys.iter()
            .enumerate()
            .map(|(index, (value, _))| (cursor_column(index), value.clone())),
    )?;
    graph = graph.rewrite_query(root, |graph, query| {
        let (operation, outputs) = query.into_projection()?;
        let OperationKind::Limit { input, .. } = operation.into_kind() else {
            return Err(GraphError::OperationVisibility);
        };
        let operation = if matches!(input.kind(), OperationKind::Sort { .. }) {
            let OperationKind::Sort { input, .. } = input.into_kind() else {
                unreachable!()
            };
            *input
        } else {
            *input
        };
        graph.project_values(operation, outputs)
    })?;
    let page = graph.query();
    let relation = graph.derive(page, root, "page")?;
    let keys = key_outputs
        .into_iter()
        .zip(keys)
        .map(|(output, (_, descending))| Ok((graph.output_column(relation, output)?, descending)))
        .collect::<std::result::Result<Vec<_>, GraphError>>()?;
    let mut operation = graph.read_relation(relation, ReadMode::Raw)?;
    if let Some(after) = input
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.after.as_ref())
    {
        let values = decode(after, query_hash)?;
        if values.len() != keys.len() {
            return Err(QueryError::PaginationError(
                "cursor key count mismatch".into(),
            ));
        }
        operation = graph.filter_relation(
            operation,
            seek(&graph, &keys, values, &nullable_flags(input, keys.len()))?,
        )?;
    }
    operation = graph.sort_relation(operation, keys.clone())?;
    let mut values = outputs
        .into_iter()
        .map(|output| {
            Ok((
                graph.output_label(output)?.to_owned(),
                Expression::Column(graph.output_column(relation, output)?),
            ))
        })
        .collect::<std::result::Result<Vec<_>, GraphError>>()?;
    let count = if input.cursor.is_some() {
        values.extend(keys.iter().enumerate().map(|(index, (key, _))| {
            (
                cursor_column(index),
                Expression::ToString(Box::new(Expression::Column(*key))),
            )
        }));
        keys.len()
    } else {
        0
    };
    let projection = graph.project_values(
        graph.limit_relation(operation, input.fetch_limit())?,
        values,
    )?;
    graph.finish_query(projection)?;
    Ok((graph, page, count))
}

fn cursor_keys<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, Infallible>,
    root: BlockId,
    input: &Input,
) -> Result<Vec<(Expression<'a>, bool)>> {
    let mut keys = Vec::new();
    if let Some(order) = &input.order_by {
        let index = input
            .nodes
            .iter()
            .position(|node| node.id == order.node)
            .ok_or(GraphError::MissingOutput)?;
        keys.push((
            Expression::Column(graph.column(graph.input_node(root, index)?, &order.property)?),
            order.direction == OrderDirection::Desc,
        ));
    }
    let named = |name: &str| -> Result<Expression<'a>> {
        let output = graph
            .outputs(root)?
            .find(|output| graph.output_label(*output).is_ok_and(|label| label == name))
            .ok_or_else(|| QueryError::PaginationError(format!("missing cursor output {name}")))?;
        Ok(graph.projection(output)?.clone())
    };
    match input.query_type {
        QueryType::Traversal => {
            for index in 0..input.nodes.len() {
                let value = Expression::Column(graph.input_identity(root, input, index)?);
                if !keys.iter().any(|(key, _)| *key == value) {
                    keys.push((value, false));
                }
            }
            for output in graph.outputs(root)? {
                if graph.output_label(output)?.ends_with("_path_nodes") {
                    keys.push((
                        Expression::ToString(Box::new(graph.projection(output)?.clone())),
                        false,
                    ));
                }
            }
        }
        QueryType::Aggregation => {
            if let Some(sort) = &input.aggregation.sort {
                keys.push((named(&sort.column)?, sort.direction == OrderDirection::Desc));
            }
            if input.cursor.is_some() {
                for group in graph.operation(root)?.groups() {
                    if !keys.iter().any(|(value, _)| value == group) {
                        keys.push((group.clone(), false));
                    }
                }
            }
        }
        QueryType::Neighbors => {
            let center = &input.nodes[0];
            let primary = primary_key_column(&center.id);
            let identity = if graph.outputs(root)?.any(|output| {
                graph
                    .output_label(output)
                    .is_ok_and(|label| label == primary)
            }) {
                primary
            } else {
                redaction_id_column(&center.id)
            };
            for name in [
                identity.as_str(),
                neighbor_id_column(),
                neighbor_type_column(),
                relationship_type_column(),
                neighbor_is_outgoing_column(),
            ] {
                keys.push((named(name)?, false));
            }
        }
        QueryType::PathFinding => {
            keys.push((named("depth")?, false));
            for name in [path_column(), edge_kinds_column()] {
                keys.push((Expression::ToString(Box::new(named(name)?)), false));
            }
        }
        QueryType::Hydration => {}
    }
    Ok(keys)
}

fn seek<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, Infallible>,
    keys: &[(ColumnRef<'a>, bool)],
    values: Vec<Option<String>>,
    nullable: &[bool],
) -> Result<Expression<'a>> {
    let mut prefix: Option<Expression<'a>> = None;
    let mut predicate = None;
    for (index, ((key, descending), value)) in keys.iter().zip(values).enumerate() {
        let column = Expression::Column(*key);
        let test = |operator, argument| Expression::Predicate {
            operator,
            value: Box::new(column.clone()),
            argument,
            fold_case: false,
        };
        let equal = if let Some(value) = value {
            let ValueType::Scalar(data_type) = graph.column_type(*key)? else {
                return Err(QueryError::PaginationError(
                    "cursor requires scalar keys".into(),
                ));
            };
            let value = if matches!(
                data_type,
                SqlType::Int64 | SqlType::UInt32 | SqlType::Float64 | SqlType::Bool
            ) {
                serde_json::from_str(&value)
                    .map_err(|_| QueryError::PaginationError("invalid cursor value".into()))?
            } else {
                serde_json::Value::String(value)
            };
            let value = Expression::Literal { data_type, value };
            let mut advance = test(
                if *descending {
                    FilterOp::Lt
                } else {
                    FilterOp::Gt
                },
                Some(Box::new(value.clone())),
            );
            if nullable[index] {
                advance = Expression::Or(Box::new(advance), Box::new(test(FilterOp::IsNull, None)));
            }
            if let Some(prefix) = &prefix {
                advance = Expression::And(Box::new(prefix.clone()), Box::new(advance));
            }
            predicate = Some(match predicate {
                Some(previous) => Expression::Or(Box::new(previous), Box::new(advance)),
                None => advance,
            });
            Expression::equal(column, value)
        } else {
            test(FilterOp::IsNull, None)
        };
        prefix = Some(match prefix {
            Some(prefix) => Expression::And(Box::new(prefix), Box::new(equal)),
            None => equal,
        });
    }
    Ok(predicate.unwrap_or(Expression::Boolean(false)))
}
