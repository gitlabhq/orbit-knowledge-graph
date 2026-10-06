use ontology::constants::*;

use super::requirements::{OutputValue, Predicate, live, relationship_kinds};
use crate::constants::{DEPTH_COLUMN, PATH_NODES_COLUMN};
use crate::error::Result;
use crate::input::Direction;
use query_data_model::{QueryDataModel, bindings::ScopeId};

use super::context::PlanningContext;
use super::physical::{PhysicalPlan, PhysicalSource};

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub(super) fn multi_hop(
        &mut self,
        scope: ScopeId,
        index: usize,
        alias: &str,
    ) -> Result<(PhysicalSource, query_data_model::bindings::ColumnRef)> {
        let hop = &self.hops[index];
        let arms = (hop.min_hops.max(1)..=hop.max_hops)
            .map(|depth| self.depth_arm(scope, index, depth))
            .collect::<Result<Vec<_>>>()?;
        let relation = self
            .bindings
            .union(scope, &arms.iter().map(|arm| arm.scope).collect::<Vec<_>>())
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(self.bindings, relation, alias)?;
        let path_export = arms[0]
            .outputs
            .iter()
            .find(|output| matches!(output.value, OutputValue::Path(_)))
            .expect("depth arm declares its path")
            .name;
        let path = self
            .bindings
            .column(scope, relation, path_export)
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        let hop = &self.hops[index];
        let (from_kind, to_kind) = match hop.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
        };
        let mut predicates = Vec::new();
        for (node, column) in [(&hop.from_node, from_kind), (&hop.to_node, to_kind)] {
            if let Some(entity) = self.nodes.get(node).and_then(|node| node.entity.as_ref()) {
                predicates.push(Predicate::EntityKind {
                    column: self.column(relation, column)?,
                    entity: entity.clone(),
                });
            }
        }
        if let Some(column) = self.deletion_column(relation, &hop.edge_table)? {
            predicates.push(live(column));
        }
        Ok((
            PhysicalSource::Filter {
                predicates,
                input: Box::new(PhysicalSource::Union {
                    relation,
                    arms,
                    relationship: hop.input_index,
                }),
            },
            path,
        ))
    }

    fn depth_arm(&mut self, parent: ScopeId, index: usize, depth: u32) -> Result<PhysicalPlan> {
        let scope = self.child_scope(parent)?;
        let hop = &self.hops[index];
        let (start, end) = hop.direction.edge_columns();
        let end_kind = match hop.direction {
            Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
            Direction::Incoming => SOURCE_KIND_COLUMN,
        };
        let table = self.model.table(&hop.edge_table).ok_or_else(|| {
            crate::error::QueryError::Lowering(format!("unknown edge table '{}'", hop.edge_table))
        })?;
        let mut source = self.scan(scope, table.name(), "e1", false, None)?;
        let first = source.relation();
        let mut scans = vec![first];
        let mut predicate = vec![];
        if let Some(kind) = relationship_kinds(
            self.column(first, RELATIONSHIP_KIND_COLUMN)?,
            &self.hops[index].rel_types,
        ) {
            predicate.push(kind);
        }
        if let Some(column) = self.deletion_column(first, table.name())? {
            predicate.push(live(column));
        }
        for step in 2..=depth {
            let current = format!("e{step}");
            let next = self.scan(scope, table.name(), &current, false, None)?;
            let relation = next.relation();
            let mut predicates = vec![];
            if let Some(column) = self.deletion_column(relation, table.name())? {
                predicates.push(live(column));
            }
            if let Some(kind) = relationship_kinds(
                self.column(relation, RELATIONSHIP_KIND_COLUMN)?,
                &self.hops[index].rel_types,
            ) {
                predicates.push(kind);
            }
            source = PhysicalSource::Join {
                endpoints: (
                    self.column(*scans.last().unwrap(), end)?,
                    self.column(relation, start)?,
                ),
                predicates,
                left: Box::new(source),
                right: Box::new(next),
            };
            scans.push(relation);
        }
        let last = *scans.last().unwrap();
        let (source_alias, target_alias, kind_alias) = match self.hops[index].direction {
            Direction::Outgoing | Direction::Both => (first, last, first),
            Direction::Incoming => (last, first, last),
        };
        let path = OutputValue::Path(
            scans
                .iter()
                .map(|relation| {
                    Ok((
                        self.column(*relation, end)?,
                        self.column(*relation, end_kind)?,
                    ))
                })
                .collect::<Result<_>>()?,
        );
        let mut outputs = Vec::new();
        for (relation, name) in [
            (first, start),
            (last, end),
            (kind_alias, RELATIONSHIP_KIND_COLUMN),
            (source_alias, SOURCE_ID_COLUMN),
            (source_alias, SOURCE_KIND_COLUMN),
            (source_alias, SOURCE_TAGS_COLUMN),
            (target_alias, TARGET_ID_COLUMN),
            (target_alias, TARGET_KIND_COLUMN),
            (target_alias, TARGET_TAGS_COLUMN),
        ] {
            let column = if matches!(name, SOURCE_TAGS_COLUMN | TARGET_TAGS_COLUMN) {
                self.optional_column(relation, name)?
            } else {
                Some(self.column(relation, name)?)
            };
            if let Some(column) = column {
                outputs.push(self.projection(scope, OutputValue::Column(column), name)?);
            }
        }
        outputs.push(self.projection(scope, path, PATH_NODES_COLUMN)?);
        outputs.push(self.projection(scope, OutputValue::Depth(depth), DEPTH_COLUMN)?);
        if let Some(column) = self.deletion_column(first, table.name())? {
            outputs.push(self.projection(
                scope,
                OutputValue::Column(column),
                self.names.exports[&column.export()].clone(),
            )?);
        }
        if let Some(column) = self.optional_column(first, TRAVERSAL_PATH_COLUMN)? {
            outputs.push(self.projection(
                scope,
                OutputValue::Column(column),
                TRAVERSAL_PATH_COLUMN,
            )?);
        }
        Ok(PhysicalPlan {
            scope,
            source: source.filter(predicate),
            outputs,
        })
    }
}
