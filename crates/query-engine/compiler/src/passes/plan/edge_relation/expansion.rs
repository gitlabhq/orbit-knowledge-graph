use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::conditions::denorm_hop;
use super::{EdgeRelation, PlanningContext};
use crate::constants::*;
use crate::input::Direction;
use crate::passes::plan::physical::{RelationGraph, RelationId};
use crate::passes::plan::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, live, property_filter, relationship_kinds,
    tag_filter,
};

pub(super) fn multi_hop<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    endpoint_ids: [Option<(RelationId, String)>; 2],
    reverse_output: bool,
) -> RelationId {
    let [source_ids, target_ids] = endpoint_ids;
    let edge_columns = relation
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table))
        .cloned();
    let mut required_columns = Vec::new();
    for node in [&relation.from_node, &relation.to_node] {
        for (property, _) in &facts.nodes[node].filters {
            if edge_columns
                .as_ref()
                .is_some_and(|columns| columns.contains(property))
                && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                && !required_columns.contains(property)
            {
                required_columns.push(property.clone());
            }
        }
    }

    let start_id = relation.direction.edge_columns().0;
    let start_kind = match relation.direction {
        Direction::Outgoing | Direction::Both => SOURCE_KIND_COLUMN,
        Direction::Incoming => TARGET_KIND_COLUMN,
    };
    let mut first_predicates = vec![live("e1")];
    if let Some(entity) = facts.nodes[&relation.from_node].entity.as_ref() {
        first_predicates.push(Predicate::EntityKind {
            column: Column::new("e1", start_kind),
            entity: entity.clone(),
        });
    }
    if let Some((ids, key)) = source_ids {
        first_predicates.push(Predicate::Membership {
            columns: vec![Column::new("e1", start_id)],
            relation: ids,
            keys: vec![key],
        });
    }
    first_predicates.extend(endpoint_predicates(
        &facts.nodes[&relation.from_node],
        "e1",
        start_id,
    ));
    for (_, filter) in &facts.nodes[&relation.from_node].filters {
        if let Some((_, denormalized)) = denorm_hop(
            filter,
            &relation.from_node,
            std::slice::from_ref(relation),
            &facts.denormalized,
        ) && let Some(predicate) = tag_filter(
            "e1",
            &denormalized.edge_column,
            &denormalized.tag_key,
            &filter.filter,
        ) {
            first_predicates.push(predicate);
        }
    }
    if let Some(columns) = &edge_columns {
        for node in [&relation.from_node, &relation.to_node] {
            first_predicates.extend(
                facts.nodes[node]
                    .filters
                    .iter()
                    .filter(|(property, _)| {
                        columns.contains(property)
                            && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                    })
                    .map(|(property, filter)| property_filter("e1", property, filter)),
            );
        }
    }
    let first = edge_read(graph, facts, relation, "e1", first_predicates);
    let mut previous =
        project_first_depth(graph, relation, first, &required_columns, reverse_output);
    let mut depths = Vec::new();
    if relation.min_hops <= 1 {
        depths.push(
            complete_depth(graph, facts, relation, alias, previous, target_ids.as_ref()).into(),
        );
    }
    for depth in 2..=relation.max_hops {
        let previous_alias = format!("_depth_{}", depth - 1);
        let current_alias = format!("e{depth}");
        let (previous_id, previous_kind, current_id, current_kind) = match relation.direction {
            Direction::Outgoing | Direction::Both => (
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
            ),
            Direction::Incoming => (
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
            ),
        };
        let current = edge_read(
            graph,
            facts,
            relation,
            &current_alias,
            vec![
                live(&current_alias),
                Predicate::Membership {
                    columns: vec![Column::new(&current_alias, current_id)],
                    relation: previous,
                    keys: vec![previous_id.to_string()],
                },
            ],
        );
        let joined = graph.join(
            previous.alias(&previous_alias),
            current.alias(&current_alias),
            (
                Column::new(&previous_alias, previous_id),
                Column::new(&current_alias, current_id),
            ),
            vec![Predicate::ColumnEq {
                left: Column::new(&previous_alias, previous_kind),
                right: Column::new(&current_alias, current_kind),
            }],
        );
        previous = project_next_depth(
            graph,
            relation,
            joined,
            depth,
            (&previous_alias, &current_alias),
            &required_columns,
            reverse_output,
        );
        if depth >= relation.min_hops.max(1) {
            depths.push(
                complete_depth(graph, facts, relation, alias, previous, target_ids.as_ref()).into(),
            );
        }
    }
    graph.union_all(depths, alias)
}

fn complete_depth<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    depth: RelationId,
    target_ids: Option<&(RelationId, String)>,
) -> RelationId {
    let end_id = relation.direction.edge_columns().1;
    let end_kind = match relation.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let mut predicates = Vec::new();
    if let Some(entity) = facts.nodes[&relation.to_node].entity.as_ref() {
        predicates.push(Predicate::EntityKind {
            column: Column::new(alias, end_kind),
            entity: entity.clone(),
        });
    }
    if let Some((ids, key)) = target_ids {
        predicates.push(Predicate::Membership {
            columns: vec![Column::new(alias, end_id)],
            relation: *ids,
            keys: vec![key.clone()],
        });
    }
    predicates.extend(endpoint_predicates(
        &facts.nodes[&relation.to_node],
        alias,
        end_id,
    ));
    graph.filter(depth.alias(alias), predicates)
}

fn endpoint_predicates(node: &super::NodePlan, alias: &str, column: &str) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    if !node.node_ids.is_empty() {
        predicates.push(id_list(alias, column, &node.node_ids));
    }
    if let Some(range) = &node.id_range {
        predicates.push(Predicate::IdRange {
            column: Column::new(alias, column),
            start: range.start,
            end: range.end,
        });
    }
    predicates
}

fn edge_read<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    mut predicates: Vec<Predicate>,
) -> RelationId {
    predicates.extend(relationship_kinds(alias, &relation.rel_types));
    predicates.extend(
        relation
            .filters
            .iter()
            .map(|(property, filter)| property_filter(alias, property, filter)),
    );
    if let Some(table) = relation.single_edge_table() {
        graph.current_rows(
            table,
            alias,
            crate::passes::plan::physical::ReadOwner::Relationship(relation.input_index),
            facts.latest_row_key(table).expect("edge table sort key"),
            predicates,
            facts.model.supports_selective_latest_rows(),
        )
    } else {
        let routes = relation
            .edge_tables
            .iter()
            .map(|table| {
                let owned = graph.current_rows(
                    table,
                    alias,
                    crate::passes::plan::physical::ReadOwner::Relationship(relation.input_index),
                    facts.latest_row_key(table).expect("edge table sort key"),
                    predicates.clone(),
                    facts.model.supports_selective_latest_rows(),
                );
                let outputs = EDGE_RESERVED_COLUMNS
                    .iter()
                    .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN, &DELETED_COLUMN])
                    .map(|column| Projection::col(alias, column))
                    .collect();
                graph.from(owned).project(outputs).finish().into()
            })
            .collect();
        graph.union_all(routes, alias)
    }
}

fn project_first_depth(
    graph: &mut RelationGraph,
    relation: &EdgeRelation,
    input: RelationId,
    required_columns: &[String],
    reverse_output: bool,
) -> RelationId {
    let path_endpoint = match (relation.direction, reverse_output) {
        (Direction::Outgoing | Direction::Both, false) => (TARGET_ID_COLUMN, TARGET_KIND_COLUMN),
        (Direction::Incoming, false) => (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN),
        (Direction::Outgoing | Direction::Both, true) => (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN),
        (Direction::Incoming, true) => (TARGET_ID_COLUMN, TARGET_KIND_COLUMN),
    };
    let mut outputs = vec![
        Projection::col("e1", RELATIONSHIP_KIND_COLUMN),
        Projection::col("e1", SOURCE_ID_COLUMN),
        Projection::col("e1", SOURCE_KIND_COLUMN),
        Projection::col("e1", SOURCE_TAGS_COLUMN),
        Projection::col("e1", TARGET_ID_COLUMN),
        Projection::col("e1", TARGET_KIND_COLUMN),
        Projection::col("e1", TARGET_TAGS_COLUMN),
        Projection::new(
            OutputValue::Path(vec![(
                Column::new("e1", path_endpoint.0),
                Column::new("e1", path_endpoint.1),
            )]),
            PATH_NODES_COLUMN,
        ),
        Projection::new(OutputValue::Depth(1), DEPTH_COLUMN),
        Projection::col("e1", DELETED_COLUMN),
        Projection::col("e1", TRAVERSAL_PATH_COLUMN),
    ];
    outputs.extend(
        required_columns
            .iter()
            .map(|column| Projection::col("e1", column)),
    );
    graph.from(input).project(outputs).finish()
}

fn project_next_depth(
    graph: &mut RelationGraph,
    relation: &EdgeRelation,
    input: RelationId,
    depth: u32,
    aliases: (&str, &str),
    required_columns: &[String],
    reverse_output: bool,
) -> RelationId {
    let (previous, current) = aliases;
    let (source, target, path_endpoint) = match relation.direction {
        Direction::Outgoing | Direction::Both => {
            (previous, current, (TARGET_ID_COLUMN, TARGET_KIND_COLUMN))
        }
        Direction::Incoming => (current, previous, (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN)),
    };
    let path = if reverse_output {
        let path_endpoint = match relation.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN),
            Direction::Incoming => (TARGET_ID_COLUMN, TARGET_KIND_COLUMN),
        };
        OutputValue::PathPrepend {
            path: Column::new(previous, PATH_NODES_COLUMN),
            id: Column::new(current, path_endpoint.0),
            kind: Column::new(current, path_endpoint.1),
        }
    } else {
        OutputValue::PathAppend {
            path: Column::new(previous, PATH_NODES_COLUMN),
            id: Column::new(current, path_endpoint.0),
            kind: Column::new(current, path_endpoint.1),
        }
    };
    let mut outputs = vec![
        Projection::col(previous, RELATIONSHIP_KIND_COLUMN),
        Projection::col(source, SOURCE_ID_COLUMN),
        Projection::col(source, SOURCE_KIND_COLUMN),
        Projection::col(source, SOURCE_TAGS_COLUMN),
        Projection::col(target, TARGET_ID_COLUMN),
        Projection::col(target, TARGET_KIND_COLUMN),
        Projection::col(target, TARGET_TAGS_COLUMN),
        Projection::new(path, PATH_NODES_COLUMN),
        Projection::new(OutputValue::Depth(depth), DEPTH_COLUMN),
        Projection::col(previous, DELETED_COLUMN),
        Projection::col(previous, TRAVERSAL_PATH_COLUMN),
    ];
    outputs.extend(
        required_columns
            .iter()
            .map(|column| Projection::col(previous, column)),
    );
    graph.from(input).project(outputs).finish()
}
