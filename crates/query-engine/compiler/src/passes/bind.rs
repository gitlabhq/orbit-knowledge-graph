use std::collections::{HashMap, HashSet};

use ontology::{DataType, EnumType, TRAVERSAL_PATH_COLUMN};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::{OrbitQueryModel, PropertyRealization};

use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input, InputFilter, InputGroupByKey, group_by_output_names};
use crate::query::{
    AggFunction, AggregationSort, Comparison, Cursor, Direction, Filter, FilterOp, Group, IdRange,
    Metric, Neighbors, Node, NodeId, Order, Path, Predicate, Property, PropertyRef, Query,
    QueryType, Relationship, TruncateUnit,
};
use crate::query_limits::*;
use crate::types::SecurityContext;
#[derive(Clone, Copy)]
pub enum Mode {
    Public,
    Local,
    Hydration,
}

pub fn bind<M: OrbitQueryModel>(mut input: Input, model: &M, mode: Mode) -> Result<Query> {
    check_shape(&input, mode)?;
    let mut query = Query::new(input.query_type);
    query.limit = input.limit;
    query.cursor = input.cursor.take().map(|cursor| Cursor {
        page_size: cursor.page_size,
        after: cursor.after,
    });
    query.dynamic_columns = input.options.dynamic_columns;
    query.include_debug_sql = input.options.include_debug_sql;

    let mut node_filters = Vec::new();
    for mut input_node in input.nodes {
        node_filters.push(std::mem::take(&mut input_node.filters));
        bind_node(&mut query, input_node, model, mode)?;
    }
    for (index, filters) in node_filters.into_iter().enumerate() {
        bind_filters(&mut query, NodeId(index), filters, model, mode)?;
    }
    bind_relationships(&mut query, input.relationships, model)?;
    bind_aggregation(&mut query, input.aggregation, model)?;
    bind_order(&mut query, input.order_by, model)?;
    bind_path(&mut query, input.path, model)?;
    bind_neighbors(&mut query, input.neighbors, model)?;
    bind_comparisons(&mut query, input.join_predicates, model)?;
    check_query(&query, mode)?;
    Ok(query)
}

fn check_shape(input: &Input, mode: Mode) -> Result<()> {
    if input.nodes.is_empty()
        || matches!(mode, Mode::Public | Mode::Local) && input.query_type == QueryType::Hydration
    {
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

fn bind_node<M: OrbitQueryModel>(
    query: &mut Query,
    input: crate::input::InputNode,
    model: &M,
    mode: Mode,
) -> Result<()> {
    validate_identifier(&input.id)?;
    check_reserved_prefix("node id", &input.id, &format!("{}_", input.id))?;
    if !matches!(mode, Mode::Hydration) && query.node_id(&input.id).is_some() {
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
    let columns = bind_columns(model, entity.id, &entity_name, input.columns, mode)?;
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
        traversal_paths: input.traversal_paths,
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
    mode: Mode,
) -> Result<Vec<Property>> {
    let entity_record = model.graph().entity(entity);
    let explicit = matches!(selection, Some(ColumnSelection::List(_)));
    let properties: Vec<_> = match selection {
        Some(ColumnSelection::List(columns)) => {
            if columns.is_empty() && !matches!(mode, Mode::Hydration) {
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
    if explicit
        && matches!(mode, Mode::Local)
        && properties.iter().any(|property| !property.is_stored())
    {
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

fn bind_filters<M: OrbitQueryModel>(
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

fn bind_predicate(input: InputFilter) -> Predicate {
    Predicate {
        op: input.op.unwrap_or(FilterOp::Eq),
        value: input.value,
    }
}

fn bind_relationships<M: OrbitQueryModel>(
    query: &mut Query,
    relationships: Vec<crate::input::InputRelationship>,
    model: &M,
) -> Result<()> {
    for (index, input) in relationships.into_iter().enumerate() {
        let from = query.node_id(&input.from).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "relationship[{index}] references undefined node \"{}\" in 'from'",
                input.from
            ))
        })?;
        let to = query.node_id(&input.to).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "relationship[{index}] references undefined node \"{}\" in 'to'",
                input.to
            ))
        })?;
        if input.types.is_empty() {
            return Err(QueryError::Validation(
                "relationships require at least one type".into(),
            ));
        }
        if input.hops.min == 0 || input.hops.min > input.hops.max || input.hops.max > MAX_HOPS_CAP {
            return Err(QueryError::Validation(format!(
                "relationship hop bounds must be ordered between 1 and {MAX_HOPS_CAP}"
            )));
        }
        if input.types.len() > MAX_REL_TYPES {
            return Err(QueryError::Validation(format!(
                "relationship type count ({}) must not exceed {MAX_REL_TYPES}",
                input.types.len()
            )));
        }
        if input.filters.len() > MAX_FILTERS_PER_REL {
            return Err(QueryError::Validation(format!(
                "relationship filter property count ({}) must not exceed {MAX_FILTERS_PER_REL}",
                input.filters.len()
            )));
        }
        if input.hops.max > 1 && !input.filters.is_empty() {
            return Err(QueryError::Validation(
                "property filters on variable-length relationships are unsupported".into(),
            ));
        }
        let type_count = input.types.len();
        let names = specialize_relationships(
            query,
            from,
            to,
            input.direction,
            input.hops,
            input.types,
            model,
        )?;
        let ids: Vec<_> = names
            .iter()
            .filter(|name| name.as_str() != "*")
            .map(|name| {
                model.graph().relationship_id(name).ok_or_else(|| {
                    QueryError::AllowlistRejected(format!("unknown relationship type {name:?}"))
                })
            })
            .collect::<Result<_>>()?;
        let tables = model.relationship_tables(&names);
        if tables.len() > 1 && type_count <= 2 && !names.iter().any(|name| name == "*") {
            return Err(QueryError::Validation(format!(
                "relationship alternatives span unsupported edge tables: {}",
                tables.join(", ")
            )));
        }
        if type_count <= 2 {
            check_relationship_variants(
                query,
                from,
                to,
                input.direction,
                input.hops,
                &names,
                model,
            )?;
        }
        let table = tables
            .first()
            .map(String::as_str)
            .unwrap_or_else(|| model.default_edge_table());
        let mut filters = Vec::new();
        for (name, predicates) in input.filters {
            let data_type = model.table_column_type(table, &name).ok_or_else(|| {
                QueryError::Validation(format!(
                    "relationship[{index}] filter on unknown edge column \"{name}\""
                ))
            })?;
            for predicate in predicates {
                let predicate = bind_predicate(predicate);
                check_value(&predicate, data_type, &name)?;
                filters.push(Filter {
                    name: name.clone(),
                    property: None,
                    data_type,
                    stored: true,
                    selectivity: ontology::FieldSelectivity::default(),
                    in_sort_key: model.in_sort_key(table, &name),
                    predicate,
                });
            }
        }
        query.relationships.push(Relationship {
            ids,
            names,
            tables,
            from,
            to,
            hops: input.hops,
            direction: input.direction,
            filters,
        });
    }
    Ok(())
}

fn specialize_relationships<M: OrbitQueryModel>(
    query: &Query,
    from: NodeId,
    to: NodeId,
    direction: Direction,
    hops: crate::query::HopRange,
    types: Vec<String>,
    model: &M,
) -> Result<Vec<String>> {
    let wildcard = types.len() == 1 && types[0] == "*";
    if types.iter().any(|kind| kind == "*") && !wildcard {
        return Err(QueryError::Validation(
            "relationship wildcard cannot be combined with concrete types".into(),
        ));
    }
    if !wildcard || hops.max > 1 {
        return Ok(types);
    }
    let from = &query.node(from).entity_name;
    let to = &query.node(to).entity_name;
    let names = match direction {
        Direction::Outgoing => model.graph().relationship_names(Some(from), Some(to)),
        Direction::Incoming => model.graph().relationship_names(Some(to), Some(from)),
        Direction::Both => model
            .graph()
            .relationship_names(Some(from), Some(to))
            .into_iter()
            .chain(model.graph().relationship_names(Some(to), Some(from)))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    if names.is_empty() {
        return Ok(types);
    }
    Ok(names)
}

fn check_relationship_variants<M: OrbitQueryModel>(
    query: &Query,
    from: NodeId,
    to: NodeId,
    direction: Direction,
    hops: crate::query::HopRange,
    names: &[String],
    model: &M,
) -> Result<()> {
    if hops.max > 1 || direction == Direction::Both {
        return Ok(());
    }
    let (source, target) = match direction {
        Direction::Outgoing => (query.node(from), query.node(to)),
        Direction::Incoming => (query.node(to), query.node(from)),
        Direction::Both => unreachable!(),
    };
    let from_alias = &query.node(from).alias;
    let to_alias = &query.node(to).alias;
    let invalid: Vec<_> = names
        .iter()
        .filter(|name| name.as_str() != "*")
        .filter(|name| {
            let Some(relationship) = model.graph().relationship_id(name) else {
                return true;
            };
            let variants = &model.graph().relationship(relationship).variants;
            !variants.is_empty()
                && model
                    .graph()
                    .variant_id(relationship, source.entity, target.entity)
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
                        .variant_id(relationship, target.entity, source.entity)
                        .is_some()
                })
        })
        .collect();
    let unconnected: Vec<_> = invalid
        .iter()
        .copied()
        .filter(|name| !reversed.contains(name))
        .collect();
    if !reversed.is_empty() {
        let reversed = reversed
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("|");
        let fix = format!(
            "{reversed} goes from {} to {}; reverse the arrow: ({to_alias})-[:{reversed}]->({from_alias})",
            target.entity_name, source.entity_name
        );
        if unconnected.is_empty() {
            return Err(QueryError::Validation(fix));
        }
        return Err(QueryError::Validation(format!(
            "{fix}; {} does not connect {} and {}",
            unconnected
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join("|"),
            source.entity_name,
            target.entity_name
        )));
    }
    Err(QueryError::Validation(format!(
        "{} does not connect {} and {} in either direction",
        unconnected
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("|"),
        source.entity_name,
        target.entity_name
    )))
}

fn bind_aggregation<M: OrbitQueryModel>(
    query: &mut Query,
    input: crate::input::InputAggregation,
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
    let group_names = group_by_output_names(&input.group_by);
    let mut outputs = HashSet::new();
    for (index, (input_group, alias)) in input.group_by.into_iter().zip(group_names).enumerate() {
        validate_identifier(&alias)?;
        check_reserved_prefix("output column", &alias, &alias)?;
        if !outputs.insert(alias.clone()) {
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
                let entity = query.node(node);
                let property =
                    resolve_property(model, entity.entity, &entity.entity_name, &property)?;
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
        validate_identifier(&alias)?;
        check_reserved_prefix("output column", &alias, &alias)?;
        if !outputs.insert(alias.clone()) {
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
            .map(|name| {
                let entity = query.node(target);
                resolve_property(model, entity.entity, &entity.entity_name, name)
            })
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

fn bind_order<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<crate::input::InputOrderBy>,
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
    let entity = query.node(node);
    let property = resolve_property(model, entity.entity, &entity.entity_name, &input.property)?;
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

fn bind_path<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<crate::input::InputPath>,
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

fn bind_neighbors<M: OrbitQueryModel>(
    query: &mut Query,
    input: Option<crate::input::InputNeighbors>,
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

fn bind_comparisons<M: OrbitQueryModel>(
    query: &mut Query,
    inputs: Vec<crate::input::JoinPredicate>,
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

fn bind_property_ref<M: OrbitQueryModel>(
    query: &Query,
    node: &str,
    property: &str,
    model: &M,
) -> Result<PropertyRef> {
    let node = query
        .node_id(node)
        .ok_or_else(|| QueryError::ReferenceError(format!("undefined node \"{node}\"")))?;
    let entity = query.node(node);
    let property = resolve_property(model, entity.entity, &entity.entity_name, property)?;
    Ok(PropertyRef { node, property })
}

fn check_comparison(left: &PropertyRef, right: &PropertyRef) -> Result<()> {
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

fn check_query(query: &Query, mode: Mode) -> Result<()> {
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

fn check_predicate(property: &Property, predicate: &Predicate, mode: Mode) -> Result<()> {
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

fn check_value(predicate: &Predicate, data_type: DataType, label: &str) -> Result<()> {
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

fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    if identifier.len() <= MAX_IDENTIFIER_LEN
        && chars
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Ok(());
    }
    Err(QueryError::Validation(format!(
        "invalid identifier {identifier:?}"
    )))
}

fn check_reserved_prefix(kind: &str, name: &str, column: &str) -> Result<()> {
    let prefix = internal_column_prefix();
    if column.starts_with(prefix) {
        return Err(QueryError::Validation(format!(
            "{kind} {name:?} conflicts with reserved prefix {prefix:?}"
        )));
    }
    Ok(())
}
