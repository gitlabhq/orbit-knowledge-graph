use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, PlanningContext};
use crate::constants::*;
use crate::input::Direction;
use crate::passes::plan::physical::{PhysicalPlan, PhysicalSource};
use crate::passes::plan::requirements::{
    Column, OutputValue, Predicate, Projection, live, relationship_kinds,
};

pub(super) fn multi_hop<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
) -> PhysicalSource {
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
    let arms = (relation.min_hops.max(1)..=relation.max_hops)
        .map(|depth| depth_arm(relation, depth, &required_columns))
        .collect();
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
    PhysicalSource::Filter {
        predicates,
        input: Box::new(PhysicalSource::Union {
            alias: alias.into(),
            arms,
            relationship: relation.input_index,
        }),
    }
}

fn depth_arm(relation: &EdgeRelation, depth: u32, required_columns: &[String]) -> PhysicalPlan {
    let (start, end) = relation.direction.edge_columns();
    let end_kind = match relation.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let scan = |alias: &str| PhysicalSource::edge(relation, alias, true);
    let mut root_predicates = relationship_kinds("e1", &relation.rel_types)
        .into_iter()
        .collect();
    let mut source = scan("e1");
    for index in 2..=depth {
        let previous = format!("e{}", index - 1);
        let current = format!("e{index}");
        let mut predicates: Vec<_> = relationship_kinds(&current, &relation.rel_types)
            .into_iter()
            .collect();
        predicates.push(Predicate::ColumnEq {
            left: Column::new(&previous, end_kind),
            right: Column::new(
                &current,
                match relation.direction {
                    Direction::Outgoing | Direction::Both => SOURCE_KIND_COLUMN,
                    Direction::Incoming => TARGET_KIND_COLUMN,
                },
            ),
        });
        source = PhysicalSource::Join {
            endpoints: (Column::new(&previous, end), Column::new(&current, start)),
            predicates,
            left: Box::new(source),
            right: Box::new(scan(&current)),
        };
    }
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
    PhysicalPlan::Project {
        source: PhysicalSource::Filter {
            predicates: std::mem::take(&mut root_predicates),
            input: Box::new(source),
        },
        outputs,
    }
}
