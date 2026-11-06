use ontology::constants::{SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN};
use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{Condition, Direction, FilterOp, Input, InputFilter, PredicateTarget};

pub(super) fn types(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    for root in &input.predicates {
        let condition = match root.relationship_type() {
            Some((condition, _)) => condition,
            None if root.leaves().any(Condition::is_relationship_type) => {
                return Err(QueryError::Validation(
                    "type(r) cannot be used inside a NOT group; write it as its own condition joined with AND, such as NOT type(r) = 'CLOSES'".into(),
                ));
            }
            None => continue,
        };
        let names = condition.relationship_type_names().ok_or_else(|| {
            QueryError::Validation(
                "type(r) supports =, <>, or IN with relationship type names, such as type(r) = 'CLOSES' or type(r) IN ['CLOSES', 'MENTIONS']".into(),
            )
        })?;
        if names.iter().any(|name| !model.relationship_exists(name)) {
            return Err(QueryError::Validation(
                "type() names a relationship type that the schema does not have; relationship type names are uppercase and case-sensitive, and CALL db.schema() lists them".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn columns(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    for (index, property, _) in filters(input) {
        let relationship = &input.relationships[index];
        let endpoints = input
            .entity_of(&relationship.from)
            .zip(input.entity_of(&relationship.to))
            .filter(|_| relationship.hops.max == 1);
        let mut tables = Vec::new();
        if let Some((from, to)) = endpoints {
            if relationship.direction != Direction::Incoming {
                tables.extend(model.relationship_tables_between(&relationship.types, from, to));
            }
            if relationship.direction != Direction::Outgoing {
                tables.extend(model.relationship_tables_between(&relationship.types, to, from));
            }
        }
        if tables.is_empty() {
            tables = model.relationship_tables(&relationship.types);
        }
        let mut types = tables
            .iter()
            .map(|table| model.table_column_type(table, property));
        let first = types.next().flatten();
        if first.is_none() || !types.all(|data_type| data_type == first) {
            return Err(QueryError::Validation(format!(
                "relationship[{index}] filter on unknown edge column \"{property}\" or incompatible column types across tables {tables:?}"
            )));
        }
    }
    Ok(())
}

fn filters(input: &Input) -> impl Iterator<Item = (usize, &str, &InputFilter)> {
    let attached = input
        .relationships
        .iter()
        .enumerate()
        .flat_map(|(index, rel)| {
            rel.filters.iter().flat_map(move |(property, filters)| {
                filters
                    .iter()
                    .map(move |filter| (index, property.as_str(), filter))
            })
        });
    let conditions = input
        .conditions()
        .filter_map(|condition| match condition.target {
            PredicateTarget::Relationship(_) if condition.is_relationship_type() => None,
            PredicateTarget::Relationship(index) => {
                Some((index, condition.property.as_str(), &condition.filter))
            }
            PredicateTarget::Node(_) => None,
        });
    attached.chain(conditions)
}

pub(super) fn endpoint_kinds(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    for (index, property, filter) in filters(input) {
        if matches!(property, SOURCE_KIND_COLUMN | TARGET_KIND_COLUMN) {
            kind(input, model, index, property, filter)?;
        }
    }
    Ok(())
}

fn kind(
    input: &Input,
    model: &impl OrbitQueryModel,
    index: usize,
    property: &str,
    filter: &InputFilter,
) -> Result<()> {
    let op = filter.op.unwrap_or(FilterOp::Eq);
    let (Some(value), true) = (
        filter.value.as_ref(),
        matches!(op, FilterOp::Eq | FilterOp::Ne | FilterOp::In),
    ) else {
        return Ok(());
    };
    let relationship = &input.relationships[index];
    let (from, to) = (
        input.entity_of(&relationship.from),
        input.entity_of(&relationship.to),
    );
    let (near, far) = match property {
        SOURCE_KIND_COLUMN => (from, to),
        _ => (to, from),
    };
    let endpoints = match relationship.direction {
        Direction::Outgoing => vec![near],
        Direction::Incoming => vec![far],
        Direction::Both => vec![near, far],
    };
    let mut allowed: Vec<String> = match relationship.hops.max > 1 || endpoints.contains(&None) {
        true => model.graph().entities().map(|e| e.name.clone()).collect(),
        false => endpoints
            .into_iter()
            .flatten()
            .map(str::to_string)
            .collect(),
    };
    allowed.dedup();
    if op == FilterOp::Ne && allowed.len() == 1 && value.as_str() == Some(&allowed[0]) {
        return Err(QueryError::Validation(format!(
            "relationship[{index}] filter on \"{property}\": <> {value} excludes the only value this pattern allows; remove the filter"
        )));
    }
    let values = value
        .as_array()
        .map_or(std::slice::from_ref(value), Vec::as_slice);
    let Some(invalid) = values.iter().find(|value| {
        !value
            .as_str()
            .is_some_and(|name| allowed.iter().any(|kind| kind == name))
    }) else {
        return Ok(());
    };
    Err(QueryError::Validation(format!(
        "relationship[{index}] filter on \"{property}\": {invalid} cannot match this pattern{}",
        ontology::errors::format_candidate_list("values", &allowed)
    )))
}
