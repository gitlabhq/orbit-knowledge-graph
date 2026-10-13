use std::collections::HashSet;

use ontology::{DataType, TRAVERSAL_PATH_COLUMN};
use orbit_utils::traversal_path::TraversalPath;

use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::query::{Direction, FilterOp, Node, Predicate, Property, PropertyRef, Query, QueryType};
use crate::query_limits::*;
use crate::types::SecurityContext;

use super::Mode;

pub(super) fn check_shape(input: &Input) -> Result<()> {
    if input.nodes.is_empty() || input.query_type == QueryType::Hydration {
        return Err(QueryError::Validation(
            "a public query requires at least one node and cannot request internal hydration"
                .into(),
        ));
    }
    if input.limit == 0 || input.limit > MAX_LIMIT {
        return Err(QueryError::Validation(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    if let Some(cursor) = &input.cursor
        && (cursor.page_size == 0 || cursor.page_size > MAX_LIMIT)
    {
        return Err(QueryError::Validation(format!(
            "cursor.page_size must be between 1 and {MAX_LIMIT}"
        )));
    }
    if input.nodes.len() > MAX_NODES_CAP {
        return Err(QueryError::Validation(format!(
            "nodes count ({}) must not exceed {MAX_NODES_CAP}",
            input.nodes.len()
        )));
    }
    if input.relationships.len() > MAX_RELS_CAP {
        return Err(QueryError::Validation(format!(
            "relationships count ({}) must not exceed {MAX_RELS_CAP}",
            input.relationships.len()
        )));
    }
    for node in &input.nodes {
        if node.node_ids.len() > MAX_NODE_IDS {
            return Err(QueryError::Validation(format!(
                "node_ids count ({}) must not exceed {MAX_NODE_IDS}",
                node.node_ids.len()
            )));
        }
        if node.filters.len() > MAX_FILTERS_PER_NODE {
            return Err(QueryError::Validation(format!(
                "filter property count ({}) must not exceed {MAX_FILTERS_PER_NODE}",
                node.filters.len()
            )));
        }
    }
    if input.aggregation.metrics.len() > 10 {
        return Err(QueryError::Validation(
            "aggregations count must not exceed 10".into(),
        ));
    }
    if input.aggregation.group_by.len() > 4 {
        return Err(QueryError::Validation(
            "group_by count must not exceed 4".into(),
        ));
    }
    Ok(())
}

pub(super) fn check_query(query: &Query, mode: Mode) -> Result<()> {
    let predicate_count = query.comparisons.len()
        + query
            .nodes
            .iter()
            .map(|node| {
                node.filters.len()
                    + usize::from(!node.node_ids.is_empty())
                    + 2 * usize::from(node.id_range.is_some())
            })
            .sum::<usize>()
        + query
            .relationships
            .iter()
            .map(|relationship| relationship.filters.len())
            .sum::<usize>();
    if predicate_count > MAX_PREDICATES {
        return Err(QueryError::Validation(format!(
            "Query has {predicate_count} predicates; the maximum is {MAX_PREDICATES} across the entire query. Remove redundant conditions or simplify the query."
        )));
    }
    if query.query_type == QueryType::Traversal
        && !query.is_search()
        && query.relationships.len() != query.nodes.len().saturating_sub(1)
    {
        return Err(QueryError::Validation(
            "traversal requires n-1 relationships for n nodes".into(),
        ));
    }
    if query.query_type == QueryType::Aggregation
        && query
            .relationships
            .iter()
            .any(|relationship| relationship.direction == Direction::Both)
    {
        return Err(QueryError::Validation(
            "aggregation does not support direction both".into(),
        ));
    }
    if matches!(mode, Mode::Public) {
        check_selectivity(query)?;
    }
    let mut referenced = HashSet::new();
    for relationship in &query.relationships {
        referenced.extend([relationship.from, relationship.to]);
    }
    if let Some(path) = &query.path {
        referenced.extend([path.from, path.to]);
    }
    for metric in &query.aggregation.metrics {
        referenced.insert(metric.target);
    }
    for group in &query.aggregation.groups {
        referenced.insert(group.node());
    }
    if !query.is_search()
        && !matches!(
            query.query_type,
            QueryType::Neighbors | QueryType::Hydration
        )
        && let Some(node) = query
            .nodes
            .iter()
            .find(|node| !referenced.contains(&node.id))
    {
        return Err(QueryError::ReferenceError(format!(
            "node \"{}\" is not referenced by any relationship or path",
            node.alias
        )));
    }
    Ok(())
}

fn check_selectivity(query: &Query) -> Result<()> {
    let selective = |node: &Node, max_range: i64| {
        !node.node_ids.is_empty()
            || !node.filters.is_empty()
            || node.id_range.as_ref().is_some_and(|range| {
                range.start <= range.end && range.end.saturating_sub(range.start) <= max_range
            })
    };
    match query.query_type {
        QueryType::PathFinding => {
            let Some(path) = query.path.as_ref() else {
                return Err(QueryError::PipelineInvariant(
                    "bound path query has no path".into(),
                ));
            };
            for endpoint in [path.from, path.to] {
                if !selective(query.node(endpoint), 500) {
                    return Err(QueryError::Validation(format!(
                        "path_finding endpoint \"{}\" requires a bounded filter",
                        query.node(endpoint).alias
                    )));
                }
            }
        }
        QueryType::Neighbors if !selective(&query.nodes[0], 100_000) => {
            return Err(QueryError::Validation(
                "neighbors requires a bounded center node".into(),
            ));
        }
        QueryType::Traversal | QueryType::Aggregation
            if !query.nodes.iter().any(|node| selective(node, 100_000)) =>
        {
            return Err(QueryError::Validation(
                "add filters or node_ids to at least one node".into(),
            ));
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn check_predicate(
    property: &Property,
    predicate: &Predicate,
    mode: Mode,
) -> Result<()> {
    if property.name == TRAVERSAL_PATH_COLUMN {
        if !matches!(
            predicate.op,
            FilterOp::Eq | FilterOp::In | FilterOp::StartsWith
        ) {
            return Err(QueryError::Validation(
                "only eq, in, and starts_with are supported for traversal_path".into(),
            ));
        }
        let paths: Vec<&str> = match predicate.op {
            FilterOp::In => predicate
                .value
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| QueryError::Validation("traversal_path IN needs an array".into()))?
                .iter()
                .map(|value| {
                    value.as_str().ok_or_else(|| {
                        QueryError::Validation("traversal_path values must be strings".into())
                    })
                })
                .collect::<Result<_>>()?,
            _ => vec![predicate.value_str().ok_or_else(|| {
                QueryError::Validation("traversal_path value must be a string".into())
            })?],
        };
        for path in paths {
            SecurityContext::validate_traversal_path(&TraversalPath::new_unchecked(path))
                .map_err(|error| QueryError::Validation(error.to_string()))?;
        }
    }
    if let Some(source) = &property.virtual_source {
        if matches!(mode, Mode::Local) {
            return Err(QueryError::Validation(format!(
                "virtual field \"{}\" is unavailable locally",
                property.name
            )));
        }
        if source.disabled {
            return Err(QueryError::Validation(format!(
                "virtual field \"{}\" is disabled",
                property.name
            )));
        }
        if !source
            .allowed_ops
            .iter()
            .any(|op| op == predicate.op.as_ref())
        {
            return Err(QueryError::Validation(format!(
                "operator is unsupported on virtual field \"{}\"",
                property.name
            )));
        }
    }
    let like = matches!(
        predicate.op,
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith
    );
    let token = matches!(
        predicate.op,
        FilterOp::TokenMatch | FilterOp::AllTokens | FilterOp::AnyTokens
    );
    if like
        && (!property.allows_like
            || !matches!(
                property.data_type,
                DataType::String | DataType::Enum | DataType::Uuid
            ))
    {
        return Err(QueryError::Validation(format!(
            "string operators are not allowed on \"{}\"",
            property.name
        )));
    }
    if token && !property.has_text_index {
        return Err(QueryError::Validation(format!(
            "token operators require a text index on \"{}\"",
            property.name
        )));
    }
    if (like || token)
        && property.name != TRAVERSAL_PATH_COLUMN
        && predicate
            .value_str()
            .is_none_or(|value| value.chars().count() < 3)
    {
        return Err(QueryError::Validation(
            "search pattern must be at least 3 characters".into(),
        ));
    }
    check_value(predicate, property.data_type, &property.name)
}

pub(super) fn check_value(predicate: &Predicate, data_type: DataType, label: &str) -> Result<()> {
    if matches!(predicate.op, FilterOp::IsNull | FilterOp::IsNotNull) {
        return Ok(());
    }
    let value = predicate
        .value
        .as_ref()
        .ok_or_else(|| QueryError::Validation(format!("filter on \"{label}\" requires a value")))?;
    let values: Vec<&serde_json::Value> = if predicate.op == FilterOp::In {
        let values = value.as_array().ok_or_else(|| {
            QueryError::Validation(format!("IN filter on \"{label}\" requires an array"))
        })?;
        if values.is_empty() || values.len() > MAX_IN_VALUES {
            return Err(QueryError::Validation(format!(
                "IN filter on \"{label}\" needs 1-{MAX_IN_VALUES} values"
            )));
        }
        values.iter().collect()
    } else {
        vec![value]
    };
    for value in values {
        if let Some(text) = value.as_str()
            && text.chars().count() > MAX_FILTER_STRING_LEN
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{label}\" exceeds {MAX_FILTER_STRING_LEN} characters"
            )));
        }
        let valid = match data_type {
            DataType::String | DataType::Date | DataType::DateTime | DataType::Uuid => {
                value.is_string()
            }
            DataType::Enum => value.is_string() || value.as_i64().is_some(),
            DataType::Int => value.as_i64().is_some(),
            DataType::Float => value.is_number(),
            DataType::Bool => value.is_boolean(),
        };
        if !valid {
            return Err(QueryError::Validation(format!(
                "filter on \"{label}\" has the wrong value type"
            )));
        }
    }
    Ok(())
}

pub(super) fn check_comparison(left: &PropertyRef, right: &PropertyRef) -> Result<()> {
    for operand in [&left.property, &right.property] {
        if !operand.filterable || !operand.is_stored() {
            return Err(QueryError::Validation(format!(
                "property comparison field \"{}\" must be stored and filterable",
                operand.name
            )));
        }
    }
    if left.property.data_type != right.property.data_type {
        return Err(QueryError::Validation(format!(
            "property comparison type mismatch: {} and {}",
            left.property.data_type, right.property.data_type
        )));
    }
    Ok(())
}

pub(crate) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    if identifier.len() <= MAX_IDENTIFIER_LEN
        && chars
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "invalid identifier {identifier:?}"
        )))
    }
}

pub(super) fn check_reserved_prefix(kind: &str, name: &str, column: &str) -> Result<()> {
    let prefix = internal_column_prefix();
    if column.starts_with(prefix) {
        return Err(QueryError::Validation(format!(
            "{kind} {name:?} conflicts with reserved prefix {prefix:?}"
        )));
    }
    Ok(())
}
