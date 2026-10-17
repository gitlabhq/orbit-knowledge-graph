use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, PlanningContext};
use crate::constants::*;
use crate::input::Direction;
use crate::passes::plan::physical::{RelationGraph, RelationId};
use crate::passes::plan::requirements::{
    Column, OutputValue, Predicate, Projection, live, relationship_kinds,
};

pub(super) fn multi_hop<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
) -> RelationId {
    let edge_columns = relation
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table));
    let mut required_columns = Vec::new();
    for node in [&relation.from_node, &relation.to_node] {
        for (property, _) in &facts.nodes[node].filters {
            if edge_columns.is_some_and(|columns| columns.contains(property))
                && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                && !required_columns.contains(property)
            {
                required_columns.push(property.clone());
            }
        }
    }

    let first = edge_read(graph, relation, "e1");
    let mut previous = project_first_depth(graph, relation, first, &required_columns);
    let mut depths = Vec::new();
    if relation.min_hops <= 1 {
        depths.push(previous.into());
    }
    for depth in 2..=relation.max_hops {
        let previous_alias = format!("_depth_{}", depth - 1);
        let current_alias = format!("e{depth}");
        let current = edge_read(graph, relation, &current_alias);
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
            &previous_alias,
            &current_alias,
            &required_columns,
        );
        if depth >= relation.min_hops.max(1) {
            depths.push(previous.into());
        }
    }
    let union = graph.union_all(depths, alias);
    let (from_kind, to_kind) = match relation.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    let mut predicates = Vec::new();
    for (node, column) in [
        (&relation.from_node, from_kind),
        (&relation.to_node, to_kind),
    ] {
        if let Some(entity) = facts.nodes[node].entity.as_ref() {
            predicates.push(Predicate::EntityKind {
                column: Column::new(alias, column),
                entity: entity.clone(),
            });
        }
    }
    predicates.push(live(alias));
    graph.from(union).filter(predicates).finish()
}

fn edge_read(graph: &mut RelationGraph, relation: &EdgeRelation, alias: &str) -> RelationId {
    let edge = if let Some(table) = relation.single_edge_table() {
        let read = graph.shared_read(table, alias, true);
        graph.owned(
            read,
            crate::passes::plan::physical::ReadOwner::Relationship(relation.input_index),
            alias,
        )
    } else {
        let routes = relation
            .edge_tables
            .iter()
            .map(|table| {
                let read = graph.shared_read(table, alias, true);
                graph
                    .owned(
                        read,
                        crate::passes::plan::physical::ReadOwner::Relationship(
                            relation.input_index,
                        ),
                        alias,
                    )
                    .into()
            })
            .collect();
        graph.union_all(routes, alias)
    };
    let predicates = relationship_kinds(alias, &relation.rel_types)
        .into_iter()
        .collect();
    graph.from(edge).filter(predicates).finish()
}

fn project_first_depth(
    graph: &mut RelationGraph,
    relation: &EdgeRelation,
    input: RelationId,
    required_columns: &[String],
) -> RelationId {
    let path_endpoint = match relation.direction {
        Direction::Outgoing | Direction::Both => (TARGET_ID_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN),
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
    previous: &str,
    current: &str,
    required_columns: &[String],
) -> RelationId {
    let (source, target, path_endpoint) = match relation.direction {
        Direction::Outgoing | Direction::Both => {
            (previous, current, (TARGET_ID_COLUMN, TARGET_KIND_COLUMN))
        }
        Direction::Incoming => (current, previous, (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN)),
    };
    let mut outputs = vec![
        Projection::col(previous, RELATIONSHIP_KIND_COLUMN),
        Projection::col(source, SOURCE_ID_COLUMN),
        Projection::col(source, SOURCE_KIND_COLUMN),
        Projection::col(source, SOURCE_TAGS_COLUMN),
        Projection::col(target, TARGET_ID_COLUMN),
        Projection::col(target, TARGET_KIND_COLUMN),
        Projection::col(target, TARGET_TAGS_COLUMN),
        Projection::new(
            OutputValue::PathAppend {
                path: Column::new(previous, PATH_NODES_COLUMN),
                id: Column::new(current, path_endpoint.0),
                kind: Column::new(current, path_endpoint.1),
            },
            PATH_NODES_COLUMN,
        ),
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
