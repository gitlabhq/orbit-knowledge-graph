use ontology::constants::*;

use super::requirements::{Column, OutputValue, Predicate, Projection, live, relationship_kinds};
use crate::constants::{DEPTH_COLUMN, PATH_NODES_COLUMN};
use crate::error::Result;
use crate::input::Direction;
use query_data_model::QueryDataModel;

use super::context::PlanningContext;
use super::physical::{PhysicalPlan, PhysicalSource};

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub(super) fn multi_hop(&mut self, index: usize, alias: &str) -> Result<PhysicalSource> {
        let hop = &self.hops[index];
        let arms = (hop.min_hops.max(1)..=hop.max_hops)
            .map(|depth| self.depth_arm(index, depth))
            .collect::<Result<Vec<_>>>()?;
        let hop = &self.hops[index];
        let (from_kind, to_kind) = match hop.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
        };
        let mut predicates = Vec::new();
        for (node, column) in [(&hop.from_node, from_kind), (&hop.to_node, to_kind)] {
            if let Some(entity) = self.nodes.get(node).and_then(|node| node.entity.as_ref()) {
                predicates.push(Predicate::EntityKind {
                    column: Column::new(alias, column),
                    entity: entity.clone(),
                });
            }
        }
        predicates.push(live(alias));
        Ok(PhysicalSource::Filter {
            predicates,
            input: Box::new(PhysicalSource::Union {
                alias: alias.into(),
                arms,
                relationship: hop.input_index,
            }),
        })
    }

    fn depth_arm(&mut self, index: usize, depth: u32) -> Result<PhysicalPlan> {
        let hop = &self.hops[index];
        let (start, end) = hop.direction.edge_columns();
        let end_kind = match hop.direction {
            Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
            Direction::Incoming => SOURCE_KIND_COLUMN,
        };
        let mut predicate = vec![];
        if let Some(kind) = relationship_kinds("e1", &hop.rel_types) {
            predicate.push(kind);
        }
        predicate.push(live("e1"));
        let table = self.model.table(&hop.edge_table).ok_or_else(|| {
            crate::error::QueryError::Lowering(format!("unknown edge table '{}'", hop.edge_table))
        })?;
        let mut source = self.scan(table.name(), "e1", false, None)?;
        for step in 2..=depth {
            let previous = format!("e{}", step - 1);
            let current = format!("e{step}");
            let mut predicates = vec![live(&current)];
            if let Some(kind) = relationship_kinds(&current, &self.hops[index].rel_types) {
                predicates.push(kind);
            }
            source = PhysicalSource::Join {
                endpoints: (Column::new(&previous, end), Column::new(&current, start)),
                predicates,
                left: Box::new(source),
                right: Box::new(self.scan(table.name(), &current, false, None)?),
            };
        }
        let last = format!("e{depth}");
        let (source_alias, target_alias, kind_alias) = match self.hops[index].direction {
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
        Ok(PhysicalPlan {
            source: PhysicalSource::Filter {
                predicates: predicate,
                input: Box::new(source),
            },
            outputs: vec![
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
            ],
        })
    }
}
