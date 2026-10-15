use std::collections::{BTreeMap, HashMap};

use query_engine::compiler;
use serde_json::{Value, json};

use compiler::input::{
    AggExpr, ColumnSelection, Direction, InputFilter, InputGroupByKey, OrderDirection,
};
use compiler::{HydrationPlan, Input, QueryError, ResultContext};

pub fn error_detail(error: &QueryError) -> String {
    match error {
        QueryError::Parse(inner) => inner.to_string(),
        QueryError::Ontology(inner) => inner.to_string(),
        QueryError::Validation(message)
        | QueryError::ReferenceError(message)
        | QueryError::PaginationError(message)
        | QueryError::AllowlistRejected(message)
        | QueryError::Authorization(message)
        | QueryError::Restrict(message)
        | QueryError::DepthExceeded(message)
        | QueryError::LimitExceeded(message)
        | QueryError::Lowering(message)
        | QueryError::Enforcement(message)
        | QueryError::Codegen(message)
        | QueryError::Security(message)
        | QueryError::PipelineInvariant(message) => message.clone(),
    }
}

fn filter(value: &InputFilter) -> Value {
    json!({
        "op": value.op.map(|op| op.as_ref().to_string()),
        "value": value.value.clone(),
        "column": value.rhs_column.as_ref().map(|(node, property)| format!("{node}.{property}")),
    })
}

fn filters<'a>(values: impl IntoIterator<Item = (&'a String, &'a Vec<InputFilter>)>) -> Value {
    Value::Object(
        values
            .into_iter()
            .map(|(name, entries)| (name.clone(), entries.iter().map(filter).collect()))
            .collect(),
    )
}

fn sorted(values: &HashMap<String, Vec<InputFilter>>) -> BTreeMap<&String, &Vec<InputFilter>> {
    values.iter().collect()
}

fn direction(value: Direction) -> &'static str {
    match value {
        Direction::Outgoing => "outgoing",
        Direction::Incoming => "incoming",
        Direction::Both => "both",
    }
}

fn order(value: OrderDirection) -> &'static str {
    match value {
        OrderDirection::Asc => "asc",
        OrderDirection::Desc => "desc",
    }
}

pub fn input(input: &Input) -> Value {
    let nodes: Vec<Value> = input
        .nodes
        .iter()
        .map(|node| {
            json!({
                "id": node.id,
                "entity": node.entity,
                "columns": match &node.columns {
                    None => Value::Null,
                    Some(ColumnSelection::All) => json!("*"),
                    Some(ColumnSelection::List(columns)) => json!(columns),
                },
                "filters": filters(sorted(&node.filters)),
                "filter_keys": sorted(&node.filters).keys().collect::<Vec<_>>(),
                "not": filters(&node.negated_filters),
                "node_ids": node.node_ids,
                "id_range": node.id_range.as_ref().map(|range| json!({"start": range.start, "end": range.end})),
                "id_property": node.id_property,
            })
        })
        .collect();
    let relationships: Vec<Value> = input
        .relationships
        .iter()
        .map(|edge| {
            json!({
                "types": edge.types,
                "from": edge.from,
                "to": edge.to,
                "hops": [edge.hops.min, edge.hops.max],
                "direction": direction(edge.direction),
                "filters": filters(sorted(&edge.filters)),
            })
        })
        .collect();
    let aggregations: Vec<Value> = input
        .aggregation
        .metrics
        .iter()
        .map(|metric| {
            json!({
                "function": metric.expr.function().to_string(),
                "node": metric.expr.node(),
                "property": metric.expr.property(),
                "name": metric.output_name(),
                "kind": match metric.expr {
                    AggExpr::Count(_) => "target",
                    _ => "property",
                },
            })
        })
        .collect();
    let group_by: Vec<Value> = input
        .aggregation
        .group_by
        .iter()
        .map(|group| match group {
            InputGroupByKey::Node { node, alias } => json!({
                "kind": "node", "node": node, "alias": alias, "name": group.output_name(),
            }),
            InputGroupByKey::Property {
                node,
                property,
                truncate,
                alias,
            } => json!({
                "kind": "property", "node": node, "property": property,
                "truncate": truncate.map(|unit| unit.name()), "alias": alias,
                "name": group.output_name(),
            }),
        })
        .collect();
    json!({
        "query_type": input.query_type.to_string(),
        "search": input.is_search(),
        "limit": input.limit,
        "nodes": nodes,
        "relationships": relationships,
        "aggregations": aggregations,
        "group_by": group_by,
        "aggregation_sort": input.aggregation.sort.as_ref().map(|sort| json!({
            "column": sort.column, "direction": order(sort.direction),
        })),
        "path": input.path.as_ref().map(|path| json!({
            "type": "shortest", "from": path.from, "to": path.to,
            "max_depth": path.max_depth, "rel_types": path.rel_types,
        })),
        "neighbors": input.neighbors.as_ref().map(|neighbors| json!({
            "direction": direction(neighbors.direction), "rel_types": neighbors.rel_types,
        })),
        "order_by": input.order_by.as_ref().map(|order_by| json!({
            "node": order_by.node, "property": order_by.property,
            "direction": order(order_by.direction),
        })),
        "cursor": input.cursor.as_ref().map(|cursor| json!({
            "page_size": cursor.page_size, "after": cursor.after.is_some(),
        })),
        "options": {
            "dynamic_columns": <&str>::from(input.options.dynamic_columns),
            "include_debug_sql": input.options.include_debug_sql,
        },
        "predicates": input.predicates.len(),
    })
}

pub fn hydration(plan: &HydrationPlan) -> Value {
    let virtual_columns = |columns: &[compiler::VirtualColumnRequest]| -> Value {
        columns
            .iter()
            .map(|column| {
                json!({"name": column.column_name, "service": column.service, "lookup": column.lookup})
            })
            .collect()
    };
    match plan {
        HydrationPlan::None => json!({"kind": "none"}),
        HydrationPlan::Static(templates) => json!({
            "kind": "static",
            "aliases": templates.iter().map(|template| &template.node_alias).collect::<Vec<_>>(),
            "templates": templates.iter().map(|template| (template.node_alias.clone(), json!({
                "entity": template.entity_type,
                "table": template.destination_table,
                "columns": template.columns,
                "virtual_columns": virtual_columns(&template.virtual_columns),
                "injected_columns": template.injected_columns,
                "has_traversal_path": template.has_traversal_path,
                "virtual_filters": template.virtual_filters.iter().map(|(property, value)| {
                    let mut entry = filter(value);
                    entry["property"] = json!(property);
                    entry
                }).collect::<Vec<_>>(),
                "filter_injected_columns": template.filter_injected_columns,
            }))).collect::<serde_json::Map<_, _>>(),
        }),
        HydrationPlan::Dynamic(entities) => json!({
            "kind": "dynamic",
            "entities": entities.iter().map(|entity| (entity.entity_type.clone(), json!({
                "table": entity.destination_table,
                "columns": entity.columns,
                "virtual_columns": virtual_columns(&entity.virtual_columns),
                "injected_columns": entity.injected_columns,
                "has_traversal_path": entity.has_traversal_path,
            }))).collect::<serde_json::Map<_, _>>(),
        }),
    }
}

pub fn result(context: &ResultContext) -> Value {
    let mut nodes: Vec<_> = context.nodes().collect();
    nodes.sort_by(|a, b| a.alias.cmp(&b.alias));
    let mut auth: Vec<_> = context.entity_auth().collect();
    auth.sort_by(|a, b| a.0.cmp(b.0));
    json!({
        "query_type": context.query_type.map(|query_type| query_type.to_string()),
        "aliases": nodes.iter().map(|node| &node.alias).collect::<Vec<_>>(),
        "nodes": nodes.iter().map(|node| (node.alias.clone(), json!({
            "entity": node.entity_type,
            "pk_column": node.pk_column,
            "id_column": node.id_column,
            "type_column": node.type_column,
        }))).collect::<serde_json::Map<_, _>>(),
        "entity_auth": auth.iter().map(|(entity, config)| (entity.to_string(), json!({
            "resource_type": config.resource_type,
            "ability": config.ability,
            "auth_id_column": config.auth_id_column,
            "owner_entity": config.owner_entity,
            "required_access_level": config.required_access_level,
        }))).collect::<serde_json::Map<_, _>>(),
        "edges": context.edges().iter().map(|edge| json!({
            "rel_types": edge.rel_types,
            "from": edge.from_alias,
            "to": edge.to_alias,
            "path_column": edge.path_column,
            "column_prefix": edge.column_prefix,
        })).collect::<Vec<_>>(),
    })
}

pub fn settings(config: &orbit_server_config::query::QueryConfig) -> Value {
    let mut settings = config.to_clickhouse_settings().unwrap_or_default();
    settings.extend(config.compiler_derived.to_clickhouse_settings());
    Value::Object(
        settings
            .into_iter()
            .map(|(name, value)| (name, Value::String(value)))
            .collect(),
    )
}

pub fn schema(
    response: &ontology::introspection::SchemaResponse,
    ontology: &ontology::Ontology,
    scope: ontology::introspection::IntrospectionScope,
) -> Value {
    use ontology::introspection::{SchemaNode, build_schema_response};
    let nodes: Vec<(&str, Option<usize>)> = response
        .domains
        .iter()
        .flat_map(|domain| &domain.nodes)
        .map(|node| match node {
            SchemaNode::Name(name) => (name.as_str(), None),
            SchemaNode::Expanded { name, props, .. } => (name.as_str(), Some(props.len())),
        })
        .collect();
    let expanded: Vec<&str> = nodes
        .iter()
        .filter(|(_, props)| props.is_some())
        .map(|(name, _)| *name)
        .collect();
    let full = build_schema_response(ontology, scope, &[])
        .ok()
        .and_then(|full| serde_json::to_value(full).ok());
    json!({
        "full": full == serde_json::to_value(response).ok(),
        "domains": response.domains.len(),
        "nodes": nodes.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "expanded_with_properties": nodes
            .iter()
            .filter(|(_, props)| props.is_some_and(|count| count > 0))
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        "edges": response.edges.len(),
        "edges_touch_expanded_nodes": !expanded.is_empty() && response.edges.iter().all(|edge| {
            ontology.get_edge(edge).is_some_and(|variants| variants.iter().any(|variant| {
                expanded.contains(&variant.source_kind.as_str())
                    || expanded.contains(&variant.target_kind.as_str())
            }))
        }),
    })
}
