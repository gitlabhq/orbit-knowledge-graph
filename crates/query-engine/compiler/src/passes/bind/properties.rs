use std::collections::{HashMap, HashSet};

use ontology::{EnumType, TRAVERSAL_PATH_COLUMN};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::{OrbitQueryModel, PropertyRealization};

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, InputFilter, InputNode};
use crate::query::{
    Comparison, Filter, FilterOp, IdRange, Node, NodeId, Predicate, Property, PropertyRef, Query,
};
use crate::query_limits::{MAX_COLUMNS, MAX_FILTER_ENTRIES_PER_PROPERTY};

use super::Mode;
use super::validation::{
    check_comparison, check_predicate, check_reserved_prefix, validate_identifier,
};

pub(super) fn bind_node<M: OrbitQueryModel>(
    query: &mut Query,
    input: InputNode,
    model: &M,
    mode: Mode,
) -> Result<()> {
    validate_identifier(&input.id)?;
    check_reserved_prefix("node id", &input.id, &format!("{}_", input.id))?;
    if query.node_id(&input.id).is_some() {
        return Err(QueryError::ReferenceError(format!(
            "duplicate node id \"{}\"",
            input.id
        )));
    }
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
    let mut id_property = resolve_property(model, entity.id, &entity_name, &input.id_property)?;
    if matches!(mode, Mode::Local) && id_property.column.is_none() && id_property.name == "id" {
        id_property.column = Some("id".into());
    }
    if !id_property.is_stored() {
        return Err(QueryError::Validation(format!(
            "id_property \"{}\" on {entity_name} is not stored in this data model",
            input.id_property
        )));
    }
    let columns = bind_columns(
        model,
        entity.id,
        &entity_name,
        input.columns,
        matches!(mode, Mode::Local),
    )?;
    query.add_node(Node {
        id: NodeId::default(),
        alias: input.id,
        entity: entity.id,
        entity_name: entity_name.clone(),
        table: table.to_string(),
        columns,
        filters: Vec::new(),
        node_ids: input.node_ids,
        id_range: input.id_range.map(|range| IdRange {
            start: range.start,
            end: range.end,
        }),
        id_property,
        traversal_paths: Vec::new(),
        has_traversal_path: model.entity_has_traversal_path(&entity_name),
        is_global: model.entity_is_global(&entity_name),
        redaction_id_column: model
            .redaction_id_column(entity.id)
            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
            .to_string(),
    });
    Ok(())
}

pub(super) fn bind_hydration_node<M: OrbitQueryModel>(
    query: &mut Query,
    entity_name: String,
    columns: Vec<String>,
    node_ids: Vec<i64>,
    traversal_paths: Vec<TraversalPath>,
    model: &M,
) -> Result<()> {
    let entity = model
        .entity(&entity_name)
        .ok_or_else(|| QueryError::AllowlistRejected(format!("unknown entity '{entity_name}'")))?;
    let table = model.entity_table(&entity_name).ok_or_else(|| {
        QueryError::AllowlistRejected(format!(
            "entity '{entity_name}' is not available in this data model"
        ))
    })?;
    let id_property = resolve_property(
        model,
        entity.id,
        &entity_name,
        ontology::constants::DEFAULT_PRIMARY_KEY,
    )?;
    let columns = if columns.is_empty() {
        Vec::new()
    } else {
        bind_columns(
            model,
            entity.id,
            &entity_name,
            Some(ColumnSelection::List(columns)),
            false,
        )?
    };
    query.add_node(Node {
        id: NodeId::default(),
        alias: crate::constants::HYDRATION_NODE_ALIAS.into(),
        entity: entity.id,
        entity_name: entity_name.clone(),
        table: table.to_string(),
        columns,
        filters: Vec::new(),
        node_ids,
        id_range: None,
        id_property,
        traversal_paths,
        has_traversal_path: model.entity_has_traversal_path(&entity_name),
        is_global: model.entity_is_global(&entity_name),
        redaction_id_column: model
            .redaction_id_column(entity.id)
            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
            .to_string(),
    });
    Ok(())
}

fn bind_columns<M: OrbitQueryModel>(
    model: &M,
    entity: query_data_model::EntityId,
    entity_name: &str,
    selection: Option<ColumnSelection>,
    local: bool,
) -> Result<Vec<Property>> {
    let entity_record = model.graph().entity(entity);
    let explicit = matches!(selection, Some(ColumnSelection::List(_)));
    let properties: Vec<_> = match selection {
        Some(ColumnSelection::List(columns)) => {
            if columns.is_empty() {
                return Err(QueryError::Validation(
                    "column selection must list at least one column".into(),
                ));
            }
            if columns.len() > MAX_COLUMNS {
                return Err(QueryError::Validation(format!(
                    "columns count ({}) must not exceed {MAX_COLUMNS}",
                    columns.len()
                )));
            }
            let mut seen = HashSet::new();
            columns
                .into_iter()
                .map(|name| {
                    if !seen.insert(name.clone()) {
                        return Err(QueryError::Validation(format!(
                            "duplicate column \"{name}\" on {entity_name}"
                        )));
                    }
                    resolve_property(model, entity, entity_name, &name)
                })
                .collect::<Result<Vec<_>>>()?
        }
        selection => {
            let defaults = model.default_properties(entity);
            let ids = if selection.is_none() && !defaults.is_empty() {
                defaults
            } else {
                &entity_record.properties
            };
            ids.iter()
                .filter(|property| !model.property_is_hidden(**property))
                .filter_map(|property| {
                    let property = model.graph().property(*property);
                    resolve_property(model, entity, entity_name, &property.name).ok()
                })
                .filter(|property| property.is_stored())
                .collect()
        }
    };
    if explicit && local && properties.iter().any(|property| !property.is_stored()) {
        return Err(QueryError::Validation(format!(
            "{entity_name} contains a requested property unavailable in the local data model"
        )));
    }
    Ok(properties)
}

fn resolve_property<M: OrbitQueryModel>(
    model: &M,
    entity: query_data_model::EntityId,
    entity_name: &str,
    name: &str,
) -> Result<Property> {
    validate_identifier(name)?;
    let property = model.property_for_entity_id(entity, name).ok_or_else(|| {
        let candidates: Vec<_> = model
            .graph()
            .entity(entity)
            .properties
            .iter()
            .filter(|property| !model.property_is_hidden(**property))
            .map(|property| model.graph().property(*property).name.as_str())
            .collect();
        QueryError::AllowlistRejected(format!(
            "field \"{name}\" does not exist on node type \"{entity_name}\"{}",
            ontology::errors::format_candidate_list("fields", &candidates)
        ))
    })?;
    let (column, virtual_source) = match model.property_realization(property.id) {
        Some(PropertyRealization::Stored { column }) => (Some(column.clone()), None),
        Some(PropertyRealization::Virtual(source)) => (None, Some(source.clone())),
        None => (None, None),
    };
    Ok(Property {
        id: property.id,
        name: property.name.clone(),
        data_type: property.data_type,
        enum_values: property.enum_values.clone(),
        enum_type: property.enum_type,
        column,
        virtual_source,
        filterable: model.property_is_filterable(entity_name, name),
        admin_only: model.property_is_admin_only(property.id),
        hidden: model.property_is_hidden(property.id),
        allows_like: model.property_allows_like(entity_name, name),
        has_text_index: model.has_text_index(property.id),
        selectivity: model.property_selectivity(property.id),
    })
}

pub(super) fn resolve_node_property<M: OrbitQueryModel>(
    query: &Query,
    node: NodeId,
    property: &str,
    model: &M,
) -> Result<Property> {
    let node = query.node(node);
    resolve_property(model, node.entity, &node.entity_name, property)
}

pub(super) fn bind_property_ref<M: OrbitQueryModel>(
    query: &Query,
    node: &str,
    property: &str,
    model: &M,
) -> Result<PropertyRef> {
    let node = query
        .node_id(node)
        .ok_or_else(|| QueryError::ReferenceError(format!("undefined node \"{node}\"")))?;
    let property = resolve_node_property(query, node, property, model)?;
    Ok(PropertyRef { node, property })
}

pub(super) fn bind_filters<M: OrbitQueryModel>(
    query: &mut Query,
    node: NodeId,
    filters: HashMap<String, Vec<InputFilter>>,
    model: &M,
    mode: Mode,
) -> Result<()> {
    let entity = query.node(node).entity;
    let entity_name = query.node(node).entity_name.clone();
    let table = query.node(node).table.clone();
    let mut filters: Vec<_> = filters.into_iter().collect();
    filters.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, predicates) in filters {
        let property = resolve_property(model, entity, &entity_name, &name)?;
        if predicates.is_empty() || predicates.len() > MAX_FILTER_ENTRIES_PER_PROPERTY {
            return Err(QueryError::Validation(format!(
                "filter on {:?} needs between 1 and {MAX_FILTER_ENTRIES_PER_PROPERTY} predicates",
                property.name
            )));
        }
        if !property.filterable && property.name != TRAVERSAL_PATH_COLUMN {
            return Err(QueryError::Validation(format!(
                "filter on \"{}\": field is not filterable",
                property.name
            )));
        }
        if property.column.is_none() && property.virtual_source.is_none() {
            return Err(QueryError::Validation(format!(
                "filter on \"{}\": field is unavailable in this data model",
                property.name
            )));
        }
        for predicate in predicates {
            if let Some((rhs_node, rhs_property)) = predicate.rhs_column {
                let left = PropertyRef {
                    node,
                    property: property.clone(),
                };
                let right = bind_property_ref(query, &rhs_node, &rhs_property, model)?;
                check_comparison(&left, &right)?;
                query.comparisons.push(Comparison {
                    left,
                    op: predicate.op.unwrap_or(FilterOp::Eq),
                    right,
                });
                continue;
            }
            let mut predicate = bind_predicate(predicate);
            coerce_enum(&mut predicate, &property);
            check_predicate(&property, &predicate, mode)?;
            query.node_mut(node).filters.push(Filter {
                name: property.name.clone(),
                property: Some(property.id),
                data_type: property.data_type,
                stored: property.is_stored(),
                selectivity: property.selectivity,
                in_sort_key: model
                    .in_sort_key(&table, property.column.as_deref().unwrap_or(&property.name)),
                predicate,
            });
        }
    }
    Ok(())
}

fn coerce_enum(predicate: &mut Predicate, property: &Property) {
    if property.enum_type != EnumType::Int {
        return;
    }
    let Some(values) = &property.enum_values else {
        return;
    };
    let Some(value) = predicate.value.take() else {
        return;
    };
    predicate.value = Some(coerce_enum_value(value, values));
}

fn coerce_enum_value(
    value: serde_json::Value,
    values: &std::collections::BTreeMap<i64, String>,
) -> serde_json::Value {
    match value {
        serde_json::Value::Number(number) => number
            .as_i64()
            .and_then(|number| values.get(&number))
            .map_or(serde_json::Value::Number(number), |label| {
                serde_json::Value::String(label.clone())
            }),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| coerce_enum_value(item, values))
                .collect(),
        ),
        value => value,
    }
}

pub(super) fn bind_predicate(input: InputFilter) -> Predicate {
    Predicate {
        op: input.op.unwrap_or(FilterOp::Eq),
        value: input.value,
    }
}
