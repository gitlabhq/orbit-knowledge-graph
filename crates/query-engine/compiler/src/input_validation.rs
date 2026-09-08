use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, FilterOp, Input, InputFilter, InputGroupByKey, QueryType};
use crate::schema_limits::{MAX_IN_VALUES, MAX_REL_TYPES};

use crate::Ontology;
use crate::passes::validate::{BASE_SCHEMA_JSON, node_ref_regex};

pub fn validate_identifier(identifier: &str) -> Result<()> {
    if node_ref_regex()
        .captures(identifier)
        .is_some_and(|parts| parts.name("property").is_none())
    {
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
    let max_limit = schema()["properties"]["limit"]["maximum"]
        .as_u64()
        .expect("limit maximum");
    if input.limit == 0 || u64::from(input.limit) > max_limit {
        return Err(QueryError::LimitExceeded(format!(
            "limit must be between 1 and {max_limit}"
        )));
    }
    for node in &input.nodes {
        validate_identifier(&node.id)?;
        validate_identifier(&node.id_property)?;
        let entity = node
            .entity
            .as_deref()
            .ok_or_else(|| QueryError::Validation("each node requires an entity".into()))?;
        check_input_field(ontology, entity, &node.id_property)?;
        if let Some(ColumnSelection::List(columns)) = &node.columns {
            if columns.is_empty() {
                return Err(QueryError::Validation(
                    "column selection must not be empty".into(),
                ));
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
        if edge.types.is_empty() || edge.hops.min == 0 || edge.hops.min > edge.hops.max {
            return Err(QueryError::Validation(
                "relationships require types and positive, ordered hop bounds".into(),
            ));
        }
        check_input_relationship_types(ontology, &edge.types)?;
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
        return Err(QueryError::LimitExceeded(format!(
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

fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| serde_json::from_str(BASE_SCHEMA_JSON).expect("valid query schema"))
}

fn filter_value_validator() -> &'static jsonschema::Validator {
    static VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();
    VALIDATOR.get_or_init(|| {
        let mut schema = schema().clone();
        schema
            .as_object_mut()
            .expect("schema object")
            .retain(|key, _| matches!(key.as_str(), "$schema" | "$defs"));
        schema["$ref"] = Value::from("#/$defs/FilterValue");
        jsonschema::validator_for(&schema).expect("valid filter value schema")
    })
}

fn check_filters(filters: &HashMap<String, Vec<InputFilter>>) -> Result<()> {
    for (property, predicates) in filters {
        validate_identifier(property)?;
        if predicates.is_empty() {
            return Err(QueryError::Validation(
                "filter needs at least one predicate".into(),
            ));
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
            if !filter_value_validator().is_valid(value) {
                return Err(QueryError::Validation(format!(
                    "invalid filter value for {property:?}; values must obey the query schema's type, string, and list bounds"
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
