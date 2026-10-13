use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use ontology::constants::TRAVERSAL_PATH_COLUMN;
use ontology::{DataType, EnumType};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::{OrbitQueryModel, PropertyRealization};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, InputAggregation, InputFilter, InputGroupByKey, InputNode, InputRelationship,
    JoinPredicate,
};
use crate::query::*;
use crate::query_limits::*;

use super::validate_identifier;

pub(super) fn node<M: OrbitQueryModel>(input: InputNode, model: &M, local: bool) -> Result<Node> {
    validate_identifier(&input.id)?;
    let entity_name = input
        .entity
        .ok_or_else(|| QueryError::Validation("each node requires an entity".into()))?;
    let entity = model
        .entity(&entity_name)
        .ok_or_else(|| QueryError::AllowlistRejected(format!("unknown entity '{entity_name}'")))?;
    let table = model.entity_table(&entity_name).ok_or_else(|| {
        QueryError::AllowlistRejected(format!(
            "entity '{entity_name}' is not available in this data model"
        ))
    })?;
    let id_property = stored(
        model,
        entity.id,
        &entity_name,
        &input.id_property,
        "id_property",
        local,
    )?;
    if id_property.data_type != DataType::Int {
        return Err(QueryError::Validation(format!(
            "id_property \"{}\" on {entity_name} must be an Int64 field",
            input.id_property
        )));
    }
    let (names, explicit) = match input.columns {
        Some(ColumnSelection::List(names)) => (names, true),
        selection => {
            let defaults = model.default_properties(entity.id);
            let ids = if selection.is_none() && !defaults.is_empty() {
                defaults
            } else {
                &entity.properties
            };
            (
                ids.iter()
                    .filter(|id| !model.property_is_hidden(**id))
                    .filter(|id| model.property_is_stored(**id))
                    .map(|id| model.graph().property(*id).name.clone())
                    .collect(),
                false,
            )
        }
    };
    if explicit && names.is_empty() || names.len() > MAX_COLUMNS {
        return Err(QueryError::Validation(
            "column selection has an unsupported size".into(),
        ));
    }
    let mut seen = HashSet::new();
    let columns = names
        .into_iter()
        .map(|name| {
            if !seen.insert(name.clone()) {
                return Err(QueryError::Validation(format!(
                    "duplicate column \"{name}\" on {entity_name}"
                )));
            }
            let property = property(model, entity.id, &entity_name, &name, local)?;
            if property.is_stored() || explicit && property.is_virtual() && !local {
                Ok(property)
            } else {
                Err(QueryError::Validation(format!(
                    "{entity_name}.{name} is unavailable in this data model"
                )))
            }
        })
        .collect::<Result<_>>()?;
    let id_range = input
        .id_range
        .map(|range| {
            (range.start <= range.end)
                .then_some(IdRange {
                    start: range.start,
                    end: range.end,
                })
                .ok_or_else(|| QueryError::Validation("id_range start must not exceed end".into()))
        })
        .transpose()?;
    Ok(Node {
        id: NodeId::default(),
        alias: input.id,
        entity: entity.id,
        entity_name: entity_name.clone(),
        table: table.to_string(),
        columns,
        filters: Vec::new(),
        node_ids: input.node_ids,
        id_range,
        id_property,
        traversal_paths: Vec::new(),
        has_traversal_path: model.entity_has_traversal_path(&entity_name),
        is_global: model.entity_is_global(&entity_name),
        redaction_id_column: model
            .redaction_id_column(entity.id)
            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
            .to_string(),
    })
}

pub(super) fn hydration_node<M: OrbitQueryModel>(
    entity_name: String,
    columns: Vec<String>,
    node_ids: Vec<i64>,
    traversal_paths: Vec<TraversalPath>,
    model: &M,
) -> Result<Node> {
    let entity = model
        .entity(&entity_name)
        .ok_or_else(|| QueryError::AllowlistRejected(format!("unknown entity '{entity_name}'")))?;
    let table = model.entity_table(&entity_name).ok_or_else(|| {
        QueryError::AllowlistRejected(format!(
            "entity '{entity_name}' is not available in this data model"
        ))
    })?;
    Ok(Node {
        id: NodeId::default(),
        alias: crate::constants::HYDRATION_NODE_ALIAS.into(),
        entity: entity.id,
        entity_name: entity_name.clone(),
        table: table.to_string(),
        columns: columns
            .into_iter()
            .map(|name| property(model, entity.id, &entity_name, &name, false))
            .collect::<Result<_>>()?,
        filters: Vec::new(),
        node_ids,
        id_range: None,
        id_property: stored(
            model,
            entity.id,
            &entity_name,
            ontology::constants::DEFAULT_PRIMARY_KEY,
            "id_property",
            false,
        )?,
        traversal_paths,
        has_traversal_path: model.entity_has_traversal_path(&entity_name),
        is_global: model.entity_is_global(&entity_name),
        redaction_id_column: model
            .redaction_id_column(entity.id)
            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
            .to_string(),
    })
}

pub(super) fn stored_on<M: OrbitQueryModel>(
    node: &Node,
    model: &M,
    name: &str,
    usage: &str,
    local: bool,
) -> Result<StoredProperty> {
    stored(model, node.entity, &node.entity_name, name, usage, local)
}

fn property<M: OrbitQueryModel>(
    model: &M,
    entity: query_data_model::EntityId,
    entity_name: &str,
    name: &str,
    local: bool,
) -> Result<Property> {
    validate_identifier(name)?;
    let value = model.property_for_entity_id(entity, name).ok_or_else(|| {
        let candidates: Vec<_> = model
            .graph()
            .entity(entity)
            .properties
            .iter()
            .filter(|id| !model.property_is_hidden(**id))
            .map(|id| model.graph().property(*id).name.as_str())
            .collect();
        QueryError::AllowlistRejected(format!(
            "field \"{name}\" does not exist on node type \"{entity_name}\"{}",
            ontology::errors::format_candidate_list("fields", &candidates)
        ))
    })?;
    let source = match model.property_realization(value.id) {
        Some(PropertyRealization::Stored { column }) => PropertySource::Stored(column.clone()),
        Some(PropertyRealization::Virtual(source)) => PropertySource::Virtual(source.clone()),
        None if local && name == ontology::constants::DEFAULT_PRIMARY_KEY => {
            PropertySource::Stored(name.to_string())
        }
        None => {
            return Err(QueryError::Validation(format!(
                "{entity_name}.{name} is unavailable in this data model"
            )));
        }
    };
    Ok(Property {
        id: value.id,
        name: value.name.clone(),
        data_type: value.data_type,
        enum_values: value.enum_values.clone(),
        enum_type: value.enum_type,
        source,
        filterable: model.property_is_filterable(entity_name, name),
        admin_only: model.property_is_admin_only(value.id),
        hidden: model.property_is_hidden(value.id),
        allows_like: model.property_allows_like(entity_name, name),
        has_text_index: model.has_text_index(value.id),
        selectivity: model.property_selectivity(value.id),
    })
}

fn stored<M: OrbitQueryModel>(
    model: &M,
    entity: query_data_model::EntityId,
    entity_name: &str,
    name: &str,
    usage: &str,
    local: bool,
) -> Result<StoredProperty> {
    let property = property(model, entity, entity_name, name, local)?;
    let column = property.column().ok_or_else(|| {
        QueryError::Validation(format!(
            "{usage} field \"{name}\" must be stored in this data model"
        ))
    })?;
    Ok(StoredProperty::new(property.clone(), column.to_string()))
}

pub(super) fn filters<M: OrbitQueryModel>(
    node: &mut Node,
    filters: HashMap<String, Vec<InputFilter>>,
    model: &M,
    local: bool,
) -> Result<Vec<(String, String, String, FilterOp)>> {
    let mut comparisons = Vec::new();
    let mut filters: Vec<_> = filters.into_iter().collect();
    filters.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, entries) in filters {
        if entries.is_empty() || entries.len() > MAX_FILTER_ENTRIES_PER_PROPERTY {
            return Err(QueryError::Validation(format!(
                "filter on {name:?} has an unsupported size"
            )));
        }
        for input in entries {
            let op = input.op.unwrap_or(FilterOp::Eq);
            if let Some((alias, right)) = input.rhs_column {
                comparisons.push((name.clone(), alias, right, op));
                continue;
            }
            let property = filter_property(node, model, &name, op, local)?;
            node.filters.push(Filter {
                name: property.name.clone(),
                property: Some(property.id),
                data_type: property.data_type,
                stored: property.is_stored(),
                selectivity: property.selectivity,
                in_sort_key: model
                    .in_sort_key(&node.table, property.column().unwrap_or(&property.name)),
                predicate: predicate(op, input.value, &property)?,
            });
        }
    }
    Ok(comparisons)
}

pub(super) fn comparable<M: OrbitQueryModel>(
    node: &Node,
    model: &M,
    name: &str,
    op: FilterOp,
    local: bool,
) -> Result<StoredProperty> {
    let property = stored_on(node, model, name, "property comparison", local)?;
    if !property.filterable {
        return Err(QueryError::Validation(format!(
            "property comparison field \"{name}\" must be filterable"
        )));
    }
    operator(&property, op)?;
    Ok(property)
}

fn filter_property<M: OrbitQueryModel>(
    node: &Node,
    model: &M,
    name: &str,
    op: FilterOp,
    local: bool,
) -> Result<Property> {
    let property = property(model, node.entity, &node.entity_name, name, local)?;
    if !property.filterable && name != TRAVERSAL_PATH_COLUMN {
        return Err(QueryError::Validation(format!(
            "filter on \"{name}\": field is not filterable"
        )));
    }
    if let Some(source) = property.virtual_source() {
        if local || source.disabled {
            return Err(QueryError::Validation(format!(
                "virtual field \"{name}\" is unavailable"
            )));
        }
        if !source
            .allowed_ops
            .iter()
            .any(|allowed| allowed == op.as_ref())
        {
            return Err(QueryError::Validation(format!(
                "operator is unsupported on virtual field \"{name}\""
            )));
        }
    }
    if name == TRAVERSAL_PATH_COLUMN
        && !matches!(op, FilterOp::Eq | FilterOp::In | FilterOp::StartsWith)
    {
        return Err(QueryError::Validation(
            "only eq, in, and starts_with are supported for traversal_path".into(),
        ));
    }
    operator(&property, op)?;
    Ok(property)
}

fn predicate(op: FilterOp, value: Option<Value>, property: &Property) -> Result<Predicate> {
    let value = typed_value(op, value, property.data_type, Some(property))?;
    if property.name == TRAVERSAL_PATH_COLUMN {
        let paths: Vec<_> = if op == FilterOp::In {
            value.as_ref().unwrap().as_array().unwrap().iter().collect()
        } else {
            value.iter().collect()
        };
        for path in paths {
            let path = path.as_str().ok_or_else(|| {
                QueryError::Validation("traversal_path values must be strings".into())
            })?;
            crate::types::SecurityContext::validate_traversal_path(&TraversalPath::new_unchecked(
                path,
            ))
            .map_err(|error| QueryError::Validation(error.to_string()))?;
        }
    }
    Ok(Predicate { op, value })
}

fn edge_predicate(op: FilterOp, value: Option<Value>, data_type: DataType) -> Result<Predicate> {
    operator_type(data_type, op, "edge column", true, true)?;
    Ok(Predicate {
        op,
        value: typed_value(op, value, data_type, None)?,
    })
}

fn typed_value(
    op: FilterOp,
    value: Option<Value>,
    data_type: DataType,
    property: Option<&Property>,
) -> Result<Option<Value>> {
    if matches!(op, FilterOp::IsNull | FilterOp::IsNotNull) {
        return Ok(None);
    }
    let value = value.ok_or_else(|| QueryError::Validation("filter requires a value".into()))?;
    let mut value = if op == FilterOp::In {
        let values = value
            .as_array()
            .ok_or_else(|| QueryError::Validation("IN filter requires an array".into()))?;
        if values.is_empty() || values.len() > MAX_IN_VALUES {
            return Err(QueryError::Validation(format!(
                "IN filter needs 1-{MAX_IN_VALUES} values"
            )));
        }
        Value::Array(
            values
                .iter()
                .cloned()
                .map(|value| scalar(value, data_type, property))
                .collect::<Result<_>>()?,
        )
    } else {
        scalar(value, data_type, property)?
    };
    if let Some(property) = property
        && property.enum_type == EnumType::Int
        && let Some(values) = &property.enum_values
    {
        value = enum_value(value, values);
    }
    if value.as_str().is_some_and(|text| {
        matches!(
            op,
            FilterOp::Contains
                | FilterOp::StartsWith
                | FilterOp::EndsWith
                | FilterOp::TokenMatch
                | FilterOp::AllTokens
                | FilterOp::AnyTokens
        ) && property.is_none_or(|property| property.name != TRAVERSAL_PATH_COLUMN)
            && text.chars().count() < 3
    }) {
        return Err(QueryError::Validation(
            "search pattern must be at least 3 characters".into(),
        ));
    }
    Ok(Some(value))
}

fn scalar(value: Value, data_type: DataType, property: Option<&Property>) -> Result<Value> {
    if value
        .as_str()
        .is_some_and(|text| text.chars().count() > MAX_FILTER_STRING_LEN)
    {
        return Err(QueryError::Validation(format!(
            "filter value exceeds {MAX_FILTER_STRING_LEN} characters"
        )));
    }
    let valid = match data_type {
        DataType::String => value.is_string(),
        DataType::Int => value.as_i64().is_some(),
        DataType::Float => value.is_number(),
        DataType::Bool => value.is_boolean(),
        DataType::Date => value
            .as_str()
            .is_some_and(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()),
        DataType::DateTime => value.as_str().is_some_and(|value| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
                || DateTime::parse_from_rfc3339(value).is_ok()
                || NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").is_ok()
        }),
        DataType::Uuid => value
            .as_str()
            .is_some_and(|value| Uuid::parse_str(value).is_ok()),
        DataType::Enum => match property
            .map(|property| property.enum_type)
            .unwrap_or_default()
        {
            EnumType::String => value.is_string(),
            EnumType::Int => value.is_string() || value.as_i64().is_some(),
        },
    };
    if valid {
        Ok(value)
    } else {
        Err(QueryError::Validation(
            "filter has the wrong value type".into(),
        ))
    }
}

fn operator(property: &Property, op: FilterOp) -> Result<()> {
    operator_type(
        property.data_type,
        op,
        &property.name,
        property.allows_like,
        property.has_text_index,
    )
}

fn operator_type(
    data_type: DataType,
    op: FilterOp,
    name: &str,
    allows_like: bool,
    has_text_index: bool,
) -> Result<()> {
    let supported = match op {
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => matches!(
            data_type,
            DataType::Int | DataType::Float | DataType::Date | DataType::DateTime
        ),
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith => {
            allows_like
                && matches!(
                    data_type,
                    DataType::String | DataType::Enum | DataType::Uuid
                )
        }
        FilterOp::TokenMatch | FilterOp::AllTokens | FilterOp::AnyTokens => {
            data_type == DataType::String && has_text_index
        }
        _ => true,
    };
    if supported {
        Ok(())
    } else if matches!(
        op,
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith
    ) && !allows_like
    {
        Err(QueryError::Validation(format!(
            "string operators are not allowed on \"{name}\""
        )))
    } else {
        Err(QueryError::Validation(format!(
            "operator {} is not supported for {name}",
            op.as_ref()
        )))
    }
}

fn enum_value(value: Value, values: &BTreeMap<i64, String>) -> Value {
    match value {
        Value::Number(number) => number
            .as_i64()
            .and_then(|number| values.get(&number))
            .map_or(Value::Number(number), |label| Value::String(label.clone())),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| enum_value(item, values))
                .collect(),
        ),
        value => value,
    }
}

pub(super) fn relationship<M: OrbitQueryModel>(
    model: &M,
    index: usize,
    input: InputRelationship,
    source: &Node,
    target: &Node,
    from: NodeId,
    to: NodeId,
) -> Result<Relationship> {
    if input.types.is_empty()
        || input.types.len() > MAX_REL_TYPES
        || input.hops.min == 0
        || input.hops.min > input.hops.max
        || input.hops.max > MAX_HOPS_CAP
    {
        return Err(QueryError::Validation(format!(
            "relationship[{index}] has an unsupported shape"
        )));
    }
    if input.hops.max > 1 && !input.filters.is_empty() {
        return Err(QueryError::Validation(
            "property filters on variable-length relationships are unsupported".into(),
        ));
    }
    let routes = edge_routes(
        model,
        source,
        target,
        input.direction,
        input.hops,
        input.types,
    )?;
    let tables: HashSet<_> = routes.iter().map(|route| route.table.as_str()).collect();
    if tables.len() > 1 {
        return Err(QueryError::Validation(format!(
            "relationship alternatives span unsupported edge tables: {}",
            tables.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    if input.filters.len() > MAX_FILTERS_PER_REL {
        return Err(QueryError::Validation("too many edge filters".into()));
    }
    let table = routes
        .first()
        .map(|route| route.table.as_str())
        .unwrap_or_else(|| model.default_edge_table());
    let mut filters = Vec::new();
    for (name, predicates) in input.filters {
        let data_type = model.table_column_type(table, &name).ok_or_else(|| {
            QueryError::Validation(format!(
                "relationship[{index}] filter on unknown edge column \"{name}\""
            ))
        })?;
        for input in predicates {
            if input.rhs_column.is_some() {
                return Err(QueryError::Validation(
                    "relationship filters cannot compare properties".into(),
                ));
            }
            filters.push(Filter {
                name: name.clone(),
                property: None,
                data_type,
                stored: true,
                selectivity: Default::default(),
                in_sort_key: model.in_sort_key(table, &name),
                predicate: edge_predicate(
                    input.op.unwrap_or(FilterOp::Eq),
                    input.value,
                    data_type,
                )?,
            });
        }
    }
    Ok(Relationship {
        routes,
        from,
        to,
        hops: input.hops,
        direction: input.direction,
        filters,
    })
}

pub(super) fn path_routes<M: OrbitQueryModel>(
    model: &M,
    names: Vec<String>,
) -> Result<Vec<RelationshipRoute>> {
    if names.is_empty() || names.iter().any(|name| name == "*") && names.as_slice() != ["*"] {
        return Err(QueryError::Validation(
            "path requires concrete relationship types or one wildcard".into(),
        ));
    }
    routes(
        model,
        if names.as_slice() == ["*"] {
            all_relationships(model)
        } else {
            names
        },
    )
}

fn edge_routes<M: OrbitQueryModel>(
    model: &M,
    source: &Node,
    target: &Node,
    direction: Direction,
    hops: HopRange,
    names: Vec<String>,
) -> Result<Vec<RelationshipRoute>> {
    let wildcard = names.as_slice() == ["*"];
    if names.iter().any(|name| name == "*") && !wildcard {
        return Err(QueryError::Validation(
            "relationship wildcard cannot be combined with concrete types".into(),
        ));
    }
    let names = if wildcard && hops.max == 1 {
        let names = relationship_names(model, source, target, direction);
        if names.is_empty() {
            vec!["*".into()]
        } else {
            names
        }
    } else if wildcard {
        all_relationships(model)
    } else {
        names
    };
    if hops.max == 1 && names.as_slice() != ["*"] {
        check_variants(model, &names, source, target, direction)?;
    }
    routes(model, names)
}

fn routes<M: OrbitQueryModel>(model: &M, names: Vec<String>) -> Result<Vec<RelationshipRoute>> {
    names
        .into_iter()
        .map(|name| {
            if name == "*" {
                let relationship = model
                    .graph()
                    .relationships()
                    .next()
                    .expect("graph has relationships");
                Ok(RelationshipRoute {
                    id: relationship.id,
                    name,
                    table: model.default_edge_table().to_string(),
                })
            } else {
                let route = model.relationship_route(&name).ok_or_else(|| {
                    QueryError::AllowlistRejected(format!("unknown relationship type {name:?}"))
                })?;
                Ok(RelationshipRoute {
                    id: model.graph().relationship_id(&name).unwrap(),
                    name,
                    table: route.table.to_string(),
                })
            }
        })
        .collect()
}

fn relationship_names<M: OrbitQueryModel>(
    model: &M,
    source: &Node,
    target: &Node,
    direction: Direction,
) -> Vec<String> {
    match direction {
        Direction::Outgoing => model
            .graph()
            .relationship_names(Some(&source.entity_name), Some(&target.entity_name)),
        Direction::Incoming => model
            .graph()
            .relationship_names(Some(&target.entity_name), Some(&source.entity_name)),
        Direction::Both => model
            .graph()
            .relationship_names(Some(&source.entity_name), Some(&target.entity_name))
            .into_iter()
            .chain(
                model
                    .graph()
                    .relationship_names(Some(&target.entity_name), Some(&source.entity_name)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn all_relationships<M: OrbitQueryModel>(model: &M) -> Vec<String> {
    model
        .graph()
        .relationships()
        .map(|relationship| relationship.name.clone())
        .collect()
}

fn check_variants<M: OrbitQueryModel>(
    model: &M,
    names: &[String],
    source: &Node,
    target: &Node,
    direction: Direction,
) -> Result<()> {
    if let Some(name) = names
        .iter()
        .find(|name| model.graph().relationship_id(name).is_none())
    {
        return Err(QueryError::AllowlistRejected(format!(
            "unknown relationship type {name:?}"
        )));
    }
    let (source_id, target_id) = match direction {
        Direction::Outgoing => (source.entity, target.entity),
        Direction::Incoming => (target.entity, source.entity),
        Direction::Both => return Ok(()),
    };
    let invalid: Vec<_> = names
        .iter()
        .filter(|name| {
            let relationship = model.graph().relationship_id(name).unwrap();
            let variants = &model.graph().relationship(relationship).variants;
            !variants.is_empty()
                && model
                    .graph()
                    .variant_id(relationship, source_id, target_id)
                    .is_none()
        })
        .collect();
    if invalid.len() < names.len() {
        return Ok(());
    }
    let reversed: Vec<_> = invalid
        .iter()
        .copied()
        .filter(|name| {
            model
                .graph()
                .relationship_id(name)
                .is_some_and(|relationship| {
                    model
                        .graph()
                        .variant_id(relationship, target_id, source_id)
                        .is_some()
                })
        })
        .collect();
    let unconnected: Vec<_> = invalid
        .iter()
        .copied()
        .filter(|name| !reversed.contains(name))
        .collect();
    let join = |names: &[&String]| {
        names
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join("|")
    };
    if reversed.is_empty() {
        return Err(QueryError::Validation(format!(
            "{} does not connect {} and {} in either direction",
            join(&unconnected),
            source.entity_name,
            target.entity_name
        )));
    }
    let reversed = join(&reversed);
    let fix = format!(
        "{reversed} goes from {} to {}; reverse the arrow: ({})-[:{reversed}]->({})",
        target.entity_name, source.entity_name, target.alias, source.alias
    );
    if unconnected.is_empty() {
        Err(QueryError::Validation(fix))
    } else {
        Err(QueryError::Validation(format!(
            "{fix}; {} does not connect {} and {}",
            join(&unconnected),
            source.entity_name,
            target.entity_name
        )))
    }
}

pub(super) fn aggregation<M: OrbitQueryModel>(
    input: InputAggregation,
    nodes: &[Node],
    aliases: &HashMap<String, NodeId>,
    model: &M,
    local: bool,
) -> Result<Aggregation> {
    if input.metrics.is_empty() {
        return Err(QueryError::Validation(
            "aggregation requires at least one metric".into(),
        ));
    }
    let node_id = |alias: &str, usage: &str| {
        aliases.get(alias).copied().ok_or_else(|| {
            QueryError::ReferenceError(format!("{usage} references undefined node \"{alias}\""))
        })
    };
    let mut outputs = HashSet::new();
    let mut groups = Vec::new();
    for (index, input) in input.group_by.into_iter().enumerate() {
        let alias = input.output_name();
        output(&mut outputs, &alias)?;
        let node = node_id(input.node(), &format!("group_by[{index}]"))?;
        groups.push(match input {
            InputGroupByKey::Node { .. } => Group::Node { node, alias },
            InputGroupByKey::Property {
                property, truncate, ..
            } => {
                let property =
                    stored_on(&nodes[node.index()], model, &property, "group_by", local)?;
                if !property.filterable {
                    return Err(QueryError::Validation(
                        "group_by property must be filterable".into(),
                    ));
                }
                if truncate.is_some()
                    && !matches!(property.data_type, DataType::Date | DataType::DateTime)
                {
                    return Err(QueryError::Validation(
                        "group_by truncation requires a Date or DateTime property".into(),
                    ));
                }
                if truncate.is_some_and(TruncateUnit::requires_selectivity_guard)
                    && nodes[node.index()].node_ids.is_empty()
                    && !nodes[node.index()].has_filter(&property.name)
                {
                    return Err(QueryError::Validation(format!(
                        "group_by truncate \"{}\" requires either node_ids or a filter on {}",
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
        });
    }
    let mut metrics = Vec::new();
    for input in input.metrics {
        let alias = input.output_name();
        output(&mut outputs, &alias)?;
        let function = input.expr.function();
        if function == AggFunction::Collect {
            return Err(QueryError::Validation("collect is not supported".into()));
        }
        let target = node_id(input.expr.node(), &format!("aggregation {alias:?}"))?;
        let property = input
            .expr
            .property()
            .map(|name| stored_on(&nodes[target.index()], model, name, "aggregation", local))
            .transpose()?;
        if property.as_ref().is_some_and(|property| {
            matches!(function, AggFunction::Sum | AggFunction::Avg)
                && !matches!(property.data_type, DataType::Int | DataType::Float)
        }) {
            return Err(QueryError::Validation(format!(
                "{function} requires a numeric field"
            )));
        }
        metrics.push(Metric {
            function,
            target,
            property,
            alias,
        });
    }
    let sort = input
        .sort
        .map(|sort| {
            outputs
                .contains(&sort.column)
                .then_some(AggregationSort {
                    column: sort.column.clone(),
                    direction: sort.direction,
                })
                .ok_or_else(|| {
                    QueryError::ReferenceError(format!(
                        "aggregation_sort references unknown output column \"{}\"",
                        sort.column
                    ))
                })
        })
        .transpose()?;
    Ok(Aggregation {
        metrics,
        groups,
        sort,
    })
}

pub(super) fn comparison<M: OrbitQueryModel>(
    input: JoinPredicate,
    nodes: &[Node],
    aliases: &HashMap<String, NodeId>,
    model: &M,
    local: bool,
) -> Result<Comparison> {
    let resolve = |alias: &str, name: &str| {
        let node = aliases.get(alias).copied().ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "property comparison references undefined node \"{alias}\""
            ))
        })?;
        Ok::<_, QueryError>(PropertyRef {
            node,
            property: comparable(&nodes[node.index()], model, name, input.op, local)?,
        })
    };
    let left = resolve(&input.lhs_node, &input.lhs_prop)?;
    let right = resolve(&input.rhs_node, &input.rhs_prop)?;
    if left.property.data_type != right.property.data_type {
        return Err(QueryError::Validation(format!(
            "property comparison type mismatch: {} and {}",
            left.property.data_type, right.property.data_type
        )));
    }
    Ok(Comparison {
        left,
        op: input.op,
        right,
    })
}

fn output(outputs: &mut HashSet<String>, name: &str) -> Result<()> {
    validate_identifier(name)?;
    if outputs.insert(name.to_string()) {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "duplicate output column \"{name}\""
        )))
    }
}
