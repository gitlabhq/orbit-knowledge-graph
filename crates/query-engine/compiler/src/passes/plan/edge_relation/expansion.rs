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
    let mut previous = first;
    let mut depths = Vec::new();
    for depth in 1..=relation.max_hops {
        if depth > 1 {
            let current_alias = format!("e{depth}");
            let current = edge_read(graph, relation, &current_alias);
            let previous_alias = format!("e{}", depth - 1);
            let (start, end) = relation.direction.edge_columns();
            let (end_kind, start_kind) = match relation.direction {
                Direction::Outgoing | Direction::Both => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
                Direction::Incoming => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            };
            previous = graph
                .from(previous)
                .join(
                    current.alias(current_alias.clone()),
                    (
                        Column::new(&previous_alias, end),
                        Column::new(&current_alias, start),
                    ),
                    vec![Predicate::ColumnEq {
                        left: Column::new(&previous_alias, end_kind),
                        right: Column::new(&current_alias, start_kind),
                    }],
                )
                .finish();
        }
        if depth >= relation.min_hops.max(1) {
            depths.push(project_depth(graph, relation, previous, depth, &required_columns).into());
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

fn project_depth(
    graph: &mut RelationGraph,
    relation: &EdgeRelation,
    input: RelationId,
    depth: u32,
    required_columns: &[String],
) -> RelationId {
    let (start, end) = relation.direction.edge_columns();
    let end_kind = match relation.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let last = format!("e{depth}");
    let (source_alias, target_alias, kind_alias) = match relation.direction {
        Direction::Outgoing | Direction::Both => ("e1", last.as_str(), "e1"),
        Direction::Incoming => (last.as_str(), "e1", last.as_str()),
    };
    let path = OutputValue::Path(
        (1..=depth)
            .map(|index| {
                let alias = format!("e{index}");
                (Column::new(&alias, end), Column::new(&alias, end_kind))
            })
            .collect(),
    );
    let mut outputs = vec![
        Projection::col("e1", start),
        Projection::col(&last, end),
        Projection::col(kind_alias, RELATIONSHIP_KIND_COLUMN),
        Projection::col(source_alias, SOURCE_ID_COLUMN),
        Projection::col(source_alias, SOURCE_KIND_COLUMN),
        Projection::col(source_alias, SOURCE_TAGS_COLUMN),
        Projection::col(target_alias, TARGET_ID_COLUMN),
        Projection::col(target_alias, TARGET_KIND_COLUMN),
        Projection::col(target_alias, TARGET_TAGS_COLUMN),
        Projection::new(path, PATH_NODES_COLUMN),
        Projection::new(OutputValue::Depth(depth), DEPTH_COLUMN),
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
