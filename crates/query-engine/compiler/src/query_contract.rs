mod config;

use std::collections::HashMap;
use std::sync::LazyLock;

use config::{Direction, Presence, QueryContract, QueryTypeContract, ValueType};
use strum::VariantNames;

use crate::input::{AggFunction, ColumnSelection, FilterOp, Input, PredicateTarget, QueryType};
use crate::{QueryError, Result};

static CONTRACT: LazyLock<QueryContract> = LazyLock::new(|| {
    let contract: QueryContract = orbit_utils::yaml::from_str(include_str!(concat!(
        env!("CONFIG_DIR"),
        "/query_contract.yaml"
    )))
    .expect("config/query_contract.yaml");
    assert_eq!(
        contract
            .path_types
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        crate::input::PathType::VARIANTS
    );
    contract
});

pub fn limits() -> &'static config::Limits {
    &CONTRACT.limits
}

pub fn validate_input(input: &Input) -> Result<()> {
    let limits = limits();
    let rules = query_type(input.query_type)?;

    check_range("nodes", input.nodes.len(), rules.nodes)?;
    check_range(
        "relationships",
        input.relationships.len(),
        rules.relationships,
    )?;
    check_maximum(
        "relationships count",
        input.relationships.len(),
        limits.max_relationships,
    )?;
    let query_type = query_type_name(input.query_type);
    check_presence(query_type, "path", input.path.is_some(), rules.path)?;
    check_presence(
        query_type,
        "neighbors",
        input.neighbors.is_some(),
        rules.neighbors,
    )?;
    check_presence(
        query_type,
        "aggregations",
        !input.aggregation.metrics.is_empty(),
        rules.aggregations,
    )?;

    if rules.relationship_shape.as_deref() == Some("tree")
        && input.nodes.len() > 1
        && input.relationships.len() + 1 != input.nodes.len()
    {
        return Err(QueryError::Validation(format!(
            "traversal requires exactly n-1 relationships for n nodes (got {} nodes, {} relationships)",
            input.nodes.len(),
            input.relationships.len()
        )));
    }
    if !rules.relationship_directions.is_empty()
        && input.relationships.iter().any(|relationship| {
            !rules
                .relationship_directions
                .iter()
                .any(|direction| direction.matches(relationship.direction))
        })
    {
        return Err(QueryError::Validation(format!(
            "{} does not support direction: both",
            query_type_name(input.query_type)
        )));
    }

    let predicate_count = input.conditions().count()
        + input
            .nodes
            .iter()
            .map(|node| {
                node.filters.values().map(Vec::len).sum::<usize>()
                    + usize::from(!node.node_ids.is_empty())
                    + 2 * usize::from(node.id_range.is_some())
            })
            .sum::<usize>()
        + input
            .relationships
            .iter()
            .flat_map(|relationship| relationship.filters.values())
            .map(Vec::len)
            .sum::<usize>();
    if predicate_count > limits.max_predicates {
        return Err(QueryError::Validation(format!(
            "Query has {predicate_count} predicates; the maximum is {} across the entire query. Remove redundant conditions or simplify the query.",
            limits.max_predicates
        )));
    }
    if input.limit == 0 || input.limit > limits.max_limit {
        return Err(QueryError::Validation(format!(
            "limit must be between 1 and {}",
            limits.max_limit
        )));
    }
    if input.skip.is_some_and(|skip| skip > limits.max_skip) {
        return Err(QueryError::Validation(format!(
            "skip must be at most {}; to page deeper, ORDER BY a unique key such as id and filter past the last value from the previous page",
            limits.max_skip
        )));
    }
    check_maximum(
        "aggregations",
        input.aggregation.metrics.len(),
        limits.max_aggregations,
    )?;
    check_maximum(
        "group_by",
        input.aggregation.group_by.len(),
        limits.max_group_by_keys,
    )?;

    for relationship in &input.relationships {
        check_maximum(
            "hops upper bound",
            relationship.hops.max as usize,
            limits.max_hops as usize,
        )?;
        check_maximum(
            "relationship type count",
            relationship.types.len(),
            limits.max_relationship_types,
        )?;
    }
    if let Some(path) = &input.path {
        check_maximum(
            "max_depth",
            path.max_depth as usize,
            limits.max_depth as usize,
        )?;
        check_maximum(
            "path rel_types count",
            path.rel_types.len(),
            limits.max_relationship_types,
        )?;
    }
    if let Some(neighbors) = &input.neighbors {
        check_maximum(
            "neighbors rel_types count",
            neighbors.rel_types.len(),
            limits.max_relationship_types,
        )?;
    }
    for node in &input.nodes {
        check_maximum(
            &format!("node_ids count for node \"{}\"", node.id),
            node.node_ids.len(),
            limits.max_node_ids,
        )?;
        if let Some(ColumnSelection::List(columns)) = &node.columns {
            check_maximum(
                &format!("columns count for node \"{}\"", node.id),
                columns.len(),
                limits.max_columns,
            )?;
        }
        for (property, filters) in &node.filters {
            for filter in filters {
                if filter.op == Some(FilterOp::In) {
                    let count = filter
                        .value
                        .as_ref()
                        .and_then(serde_json::Value::as_array)
                        .map(Vec::len)
                        .unwrap_or(0);
                    if count > limits.max_in_values {
                        return Err(QueryError::Validation(format!(
                            "IN filter on \"{property}\" for node \"{}\" has {count} values, must not exceed {}",
                            node.id, limits.max_in_values
                        )));
                    }
                }
            }
        }
    }
    check_condition_counts(input)
}

pub fn filter_operator_accepts(op: FilterOp, data_type: ontology::DataType) -> bool {
    CONTRACT
        .filter_operators
        .get(op.as_ref())
        .is_some_and(|types| types.contains(&data_type.into()))
}

pub fn aggregation_accepts(function: AggFunction, data_type: ontology::DataType) -> bool {
    CONTRACT
        .aggregations
        .get(&function.to_string())
        .is_some_and(|types| types.contains(&data_type.into()))
}

pub fn ordering_accepts(data_type: ontology::DataType) -> bool {
    CONTRACT.ordering.contains(&data_type.into())
}

fn query_type(query_type: QueryType) -> Result<&'static QueryTypeContract> {
    match query_type {
        QueryType::Traversal => Ok(&CONTRACT.query_types.traversal),
        QueryType::Aggregation => Ok(&CONTRACT.query_types.aggregation),
        QueryType::PathFinding => Ok(&CONTRACT.query_types.path_finding),
        QueryType::Neighbors => Ok(&CONTRACT.query_types.neighbors),
        QueryType::Hydration => Err(QueryError::Validation(
            "a public query cannot request internal hydration".into(),
        )),
    }
}

fn query_type_name(query_type: QueryType) -> &'static str {
    query_type.into()
}

fn check_range(name: &str, actual: usize, [minimum, maximum]: [usize; 2]) -> Result<()> {
    if (minimum..=maximum).contains(&actual) {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "{name} count ({actual}) must be between {minimum} and {maximum}"
        )))
    }
}

fn check_presence(query_type: &str, name: &str, present: bool, expected: Presence) -> Result<()> {
    match (present, expected) {
        (true, Presence::Forbidden) if query_type == "aggregation" && name == "path" => Err(
            QueryError::Validation("aggregation over shortest paths is not supported".into()),
        ),
        (true, Presence::Forbidden) => Err(QueryError::Validation(format!(
            "{query_type} does not support {name}"
        ))),
        (false, Presence::Required) => Err(QueryError::Validation(format!(
            "{query_type} requires {name}"
        ))),
        _ => Ok(()),
    }
}

fn check_maximum(name: &str, actual: usize, maximum: usize) -> Result<()> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "{name} ({actual}) must not exceed {maximum}"
        )))
    }
}

fn check_condition_counts(input: &Input) -> Result<()> {
    let limits = limits();
    let nodes = input.nodes.iter().map(|node| {
        (
            PredicateTarget::Node(node.id.clone()),
            &node.filters,
            limits.max_node_filters,
        )
    });
    let relationships = input.relationships.iter().enumerate().map(|(index, rel)| {
        (
            PredicateTarget::Relationship(index),
            &rel.filters,
            limits.max_relationship_filters,
        )
    });
    for (target, filters, maximum) in nodes.chain(relationships) {
        let mut counts: HashMap<&str, usize> = filters
            .iter()
            .map(|(property, filters)| (property.as_str(), filters.len()))
            .collect();
        for predicate in input
            .conditions()
            .filter(|predicate| predicate.target == target && !predicate.compares_aliases())
        {
            *counts.entry(&predicate.property).or_default() += 1;
        }
        let owner = match &target {
            PredicateTarget::Node(alias) => format!("node \"{alias}\""),
            PredicateTarget::Relationship(index) => format!("relationship[{index}]"),
        };
        if counts.len() > maximum {
            return Err(QueryError::Validation(format!(
                "filter property count ({}) for {owner} must not exceed {maximum}",
                counts.len()
            )));
        }
        if let Some((property, count)) = counts
            .into_iter()
            .find(|(_, count)| *count > limits.max_filter_entries_per_property)
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" needs between 1 and {} predicates ({count} on {owner})",
                limits.max_filter_entries_per_property
            )));
        }
    }
    Ok(())
}

impl Direction {
    fn matches(self, direction: crate::input::Direction) -> bool {
        matches!(
            (self, direction),
            (Self::Outgoing, crate::input::Direction::Outgoing)
                | (Self::Incoming, crate::input::Direction::Incoming)
                | (Self::Both, crate::input::Direction::Both)
        )
    }
}

impl From<ontology::DataType> for ValueType {
    fn from(data_type: ontology::DataType) -> Self {
        match data_type {
            ontology::DataType::String => Self::String,
            ontology::DataType::Int => Self::Integer,
            ontology::DataType::Float => Self::Number,
            ontology::DataType::Bool => Self::Boolean,
            ontology::DataType::Date => Self::Date,
            ontology::DataType::DateTime => Self::DateTime,
            ontology::DataType::Enum => Self::Enum,
            ontology::DataType::Uuid => Self::Uuid,
        }
    }
}
