use std::collections::HashMap;

use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, FilterOp, Input, InputFilter, InputGroupByKey, QueryType};
use crate::schema_limits::{
    MAX_COLUMNS, MAX_FILTER_ENTRIES_PER_PROPERTY, MAX_FILTER_STRING_LEN, MAX_FILTERS_PER_NODE,
    MAX_FILTERS_PER_REL, MAX_HOPS_CAP, MAX_IDENTIFIER_LEN, MAX_IN_VALUES, MAX_LIMIT, MAX_NODE_IDS,
    MAX_NODES_CAP, MAX_REL_TYPES, MAX_RELS_CAP,
};

use crate::Ontology;

pub fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let valid = identifier.len() <= MAX_IDENTIFIER_LEN
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "invalid identifier {identifier:?}"
        )))
    }
}

pub(crate) fn check(input: &Input, ontology: &Ontology) -> Result<()> {
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
        validate_identifier(&node.id)?;
        validate_identifier(&node.id_property)?;
        if node.node_ids.len() > MAX_NODE_IDS {
            return Err(QueryError::Validation(format!(
                "node_ids count ({}) for node {:?} must not exceed {MAX_NODE_IDS}",
                node.node_ids.len(),
                node.id
            )));
        }
        if node.filters.len() > MAX_FILTERS_PER_NODE {
            return Err(QueryError::Validation(format!(
                "filter property count ({}) for node {:?} must not exceed {MAX_FILTERS_PER_NODE}",
                node.filters.len(),
                node.id
            )));
        }
        let entity = node
            .entity
            .as_deref()
            .ok_or_else(|| QueryError::Validation("each node requires an entity".into()))?;
        check_input_field(ontology, entity, &node.id_property)?;
        if let Some(ColumnSelection::List(columns)) = &node.columns {
            if columns.is_empty() || columns.len() > MAX_COLUMNS {
                return Err(QueryError::Validation(format!(
                    "column selection must list between 1 and {MAX_COLUMNS} columns"
                )));
            }
            for column in columns {
                check_input_field(ontology, entity, column)?;
            }
        }
        for property in node.filters.keys() {
            check_input_field(ontology, entity, property)?;
        }
        check_filters(&node.filters)?;
    }
    for edge in &input.relationships {
        if edge.types.is_empty()
            || edge.hops.min == 0
            || edge.hops.min > edge.hops.max
            || edge.hops.max > MAX_HOPS_CAP
        {
            return Err(QueryError::Validation(format!(
                "relationships require types and ordered hop bounds between 1 and {MAX_HOPS_CAP}"
            )));
        }
        check_input_relationship_types(ontology, &edge.types)?;
        if edge.filters.len() > MAX_FILTERS_PER_REL {
            return Err(QueryError::Validation(format!(
                "relationship filter property count ({}) must not exceed {MAX_FILTERS_PER_REL}",
                edge.filters.len()
            )));
        }
        check_filters(&edge.filters)?;
    }
    if let Some(path) = &input.path {
        if path.max_depth == 0 {
            return Err(QueryError::Validation(
                "path max_depth must be positive".into(),
            ));
        }
        check_input_relationship_types(ontology, &path.rel_types)?;
    }
    if let Some(neighbors) = &input.neighbors {
        check_input_relationship_types(ontology, &neighbors.rel_types)?;
    }
    if input.query_type == QueryType::Neighbors && input.nodes.len() != 1 {
        return Err(QueryError::Validation(
            "neighbors requires exactly one center node".into(),
        ));
    }
    if input.query_type == QueryType::Aggregation && input.aggregation.metrics.is_empty() {
        return Err(QueryError::Validation(
            "aggregation requires at least one metric".into(),
        ));
    }
    for metric in &input.aggregation.metrics {
        validate_identifier(metric.expr.node())?;
        if let Some(property) = metric.expr.property() {
            validate_identifier(property)?;
        }
        if let Some(alias) = &metric.alias {
            validate_identifier(alias)?;
        }
    }
    for group in &input.aggregation.group_by {
        validate_identifier(group.node())?;
        if let Some(property) = group.property() {
            validate_identifier(property)?;
        }
        let alias = match group {
            InputGroupByKey::Node { alias, .. } | InputGroupByKey::Property { alias, .. } => alias,
        };
        if let Some(alias) = alias {
            validate_identifier(alias)?;
        }
    }
    if let Some(order) = &input.order_by {
        validate_identifier(&order.node)?;
        validate_identifier(&order.property)?;
    }
    if let Some(sort) = &input.aggregation.sort {
        validate_identifier(&sort.column)?;
    }
    Ok(())
}

fn check_input_field(ontology: &Ontology, entity: &str, property: &str) -> Result<()> {
    validate_identifier(property)?;
    ontology
        .validate_field(entity, property)
        .map_err(|error| QueryError::AllowlistRejected(error.to_string()))
}

fn check_input_relationship_types(ontology: &Ontology, types: &[String]) -> Result<()> {
    if types.len() > MAX_REL_TYPES {
        return Err(QueryError::Validation(format!(
            "relationship types must not exceed {MAX_REL_TYPES}"
        )));
    }
    for kind in types {
        if kind != "*" && !ontology.has_edge(kind) {
            return Err(QueryError::AllowlistRejected(format!(
                "unknown relationship type {kind:?}"
            )));
        }
    }
    Ok(())
}

fn is_valid_filter_value(value: &Value) -> bool {
    match value {
        Value::Number(_) | Value::Bool(_) => true,
        Value::String(text) => text.chars().count() <= MAX_FILTER_STRING_LEN,
        Value::Array(values) => {
            values.len() <= MAX_IN_VALUES && values.iter().all(is_valid_filter_value)
        }
        Value::Null | Value::Object(_) => false,
    }
}

fn check_filters(filters: &HashMap<String, Vec<InputFilter>>) -> Result<()> {
    for (property, predicates) in filters {
        validate_identifier(property)?;
        if predicates.is_empty() || predicates.len() > MAX_FILTER_ENTRIES_PER_PROPERTY {
            return Err(QueryError::Validation(format!(
                "filter on {property:?} needs between 1 and {MAX_FILTER_ENTRIES_PER_PROPERTY} predicates"
            )));
        }
        for filter in predicates {
            let op = filter.op.unwrap_or(FilterOp::Eq);
            if matches!(op, FilterOp::IsNull | FilterOp::IsNotNull) {
                if filter.value.is_some() {
                    return Err(QueryError::Validation(
                        "null checks cannot have a value".into(),
                    ));
                }
                continue;
            }
            let value = filter
                .value
                .as_ref()
                .ok_or_else(|| QueryError::Validation("predicate requires a value".into()))?;
            if !is_valid_filter_value(value) {
                return Err(QueryError::Validation(format!(
                    "invalid filter value for {property:?}; values must be numbers, booleans, strings up to {MAX_FILTER_STRING_LEN} characters, or lists of up to {MAX_IN_VALUES} such values"
                )));
            }
            if op == FilterOp::In
                && !value
                    .as_array()
                    .is_some_and(|values| !values.is_empty() && values.len() <= MAX_IN_VALUES)
            {
                return Err(QueryError::Validation(format!(
                    "IN requires 1-{MAX_IN_VALUES} values"
                )));
            }
            if matches!(
                op,
                FilterOp::Contains
                    | FilterOp::StartsWith
                    | FilterOp::EndsWith
                    | FilterOp::TokenMatch
                    | FilterOp::AllTokens
                    | FilterOp::AnyTokens
            ) && !value.is_string()
            {
                return Err(QueryError::Validation(
                    "text predicates require a string".into(),
                ));
            }
        }
    }
    Ok(())
}
