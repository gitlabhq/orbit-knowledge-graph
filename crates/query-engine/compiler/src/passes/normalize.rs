use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Direction, Input, InputFilter, PredicateTarget, QueryType};
use ontology::EnumType;
use query_data_model::ClickHouseDataModel;
use query_data_model::EntityAuthConfig;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Build the entity auth map for every entity type in the ontology that has a
/// redaction config. This is the single source of truth consumed by both the
/// compilation pipeline (via `normalize`) and tests that construct `ResultContext`
/// directly without going through `compile()`.
pub fn build_entity_auth(ontology: &ontology::Ontology) -> HashMap<String, EntityAuthConfig> {
    ClickHouseDataModel::derive(std::sync::Arc::new(ontology.clone()))
        .map(|model| model.authorization().entity_auth().clone())
        .unwrap_or_default()
}

pub fn normalize<M: query_data_model::OrbitQueryModel>(input: Input, model: &M) -> Result<Input> {
    let mut input = input;
    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        let entity_record = model
            .entity(entity)
            .ok_or_else(|| QueryError::AllowlistRejected(format!("unknown entity '{entity}'")))?;
        if model.entity_table(entity).is_none() {
            return Err(QueryError::AllowlistRejected(format!(
                "entity '{entity}' is not available in this data model"
            )));
        }

        if !matches!(node.columns, Some(ColumnSelection::List(_))) {
            let defaults = model.default_properties(entity_record.id);
            let properties = if node.columns.is_none() && !defaults.is_empty() {
                defaults
            } else {
                &entity_record.properties
            };
            let columns = properties
                .iter()
                .filter(|property| model.property_realization(**property).is_some())
                .filter(|property| !model.property_is_hidden(**property))
                .map(|property| model.graph().property(*property).name.clone())
                .collect();
            node.columns = Some(ColumnSelection::List(columns));
        }

        for (column, filters) in &mut node.filters {
            for filter in filters {
                coerce_filter(filter, model.property(entity, column));
            }
        }
    }
    let nodes = &input.nodes;
    for root in &mut input.predicates {
        root.visit_mut(&mut |predicate| {
            if let PredicateTarget::Node(alias) = &predicate.target
                && let Some(entity) = nodes
                    .iter()
                    .find(|node| node.id == *alias)
                    .and_then(|node| node.entity.as_deref())
            {
                coerce_filter(
                    &mut predicate.filter,
                    model.property(entity, &predicate.property),
                );
            }
        });
    }
    infer_wildcard_relationship_kinds(&mut input, model);
    resolve_relationship_types(&mut input, model)?;
    check_shared_foreign_keys(&input, model)?;
    Ok(input)
}

fn resolve_relationship_types(
    input: &mut Input,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let (conditions, predicates) = std::mem::take(&mut input.predicates)
        .into_iter()
        .partition::<Vec<_>, _>(|root| root.relationship_type().is_some());
    input.predicates = predicates;
    for root in &conditions {
        let Some((condition, negated)) = root.relationship_type() else {
            continue;
        };
        let PredicateTarget::Relationship(index) = condition.target else {
            continue;
        };
        let names = condition.relationship_type_names().unwrap_or_default();
        let types = match (&mut input.path, &mut input.neighbors) {
            (Some(path), _) => &mut path.rel_types,
            (_, Some(neighbors)) => &mut neighbors.rel_types,
            _ => &mut input.relationships[index].types,
        };
        if is_wildcard(types) {
            *types = match negated {
                true => model.graph().relationship_names(None, None),
                false => names.iter().map(|name| name.to_string()).collect(),
            };
        }
        types.retain(|kind| names.contains(&kind.as_str()) != negated);
        if types.is_empty() {
            return Err(QueryError::Validation(
                "type(r) excludes every relationship type the pattern allows".into(),
            ));
        }
    }
    Ok(())
}

fn check_shared_foreign_keys(
    input: &Input,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    for (index, rel) in input.relationships.iter().enumerate() {
        if rel.types.len() > 1
            && input.relationship_return.uses(index)
            && let (Some(from), Some(to)) = (input.entity_of(&rel.from), input.entity_of(&rel.to))
            && model.foreign_key(&rel.types, from, to).is_some()
        {
            return Err(QueryError::Validation(format!(
                "{} share one foreign key between {from} and {to}, so type() and a returned relationship cannot tell them apart; match one relationship type, such as -[r:{}]->",
                rel.types.join("|"),
                rel.types[0]
            )));
        }
    }
    Ok(())
}

pub(crate) fn is_wildcard(types: &[String]) -> bool {
    types.is_empty() || (types.len() == 1 && types[0] == "*")
}

fn infer_wildcard_relationship_kinds(
    input: &mut Input,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) {
    let entity_for: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|n| Some((n.id.as_str(), n.entity.as_deref()?)))
        .collect();
    let matching = |source: Option<&str>, target: Option<&str>| {
        model.graph().relationship_names(source, target)
    };
    let infer = |direction: Direction,
                 outgoing: (Option<&str>, Option<&str>),
                 incoming: (Option<&str>, Option<&str>)| match direction {
        Direction::Outgoing => matching(outgoing.0, outgoing.1),
        Direction::Incoming => matching(incoming.0, incoming.1),
        Direction::Both => matching(outgoing.0, outgoing.1)
            .into_iter()
            .chain(matching(incoming.0, incoming.1))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };

    for rel in &mut input.relationships {
        if rel.hops.max > 1 {
            continue;
        }
        let Some((from_entity, to_entity)) = entity_for
            .get(rel.from.as_str())
            .copied()
            .zip(entity_for.get(rel.to.as_str()).copied())
        else {
            continue;
        };
        specialize_wildcard(
            &mut rel.types,
            infer(
                rel.direction,
                (Some(from_entity), Some(to_entity)),
                (Some(to_entity), Some(from_entity)),
            ),
        );
    }

    if input.query_type == QueryType::Neighbors
        && let Some(neighbors) = input.neighbors.as_mut()
        && let [center] = input.nodes.as_slice()
        && let Some(center_entity) = center.entity.as_deref()
    {
        specialize_wildcard(
            &mut neighbors.rel_types,
            infer(
                neighbors.direction,
                (Some(center_entity), None),
                (None, Some(center_entity)),
            ),
        );
    }
}

fn specialize_wildcard(types: &mut Vec<String>, inferred: Vec<String>) {
    if is_wildcard(types) && !inferred.is_empty() {
        *types = inferred;
    }
}

fn coerce_value(value: &Value, enum_values: &BTreeMap<i64, String>) -> Value {
    match value {
        Value::Number(n) => {
            if let Some(key) = n.as_i64()
                && let Some(label) = enum_values.get(&key)
            {
                return Value::String(label.clone());
            }
            value.clone()
        }
        Value::Array(arr) => {
            let coerced: Vec<Value> = arr.iter().map(|v| coerce_value(v, enum_values)).collect();
            Value::Array(coerced)
        }
        _ => value.clone(),
    }
}

fn coerce_filter(filter: &mut InputFilter, property: Option<&query_data_model::Property>) {
    if let Some(property) = property
        && property.enum_type == EnumType::Int
        && let Some(enum_values) = &property.enum_values
        && let Some(value) = &filter.value
    {
        filter.value = Some(coerce_value(value, enum_values));
    }
}
