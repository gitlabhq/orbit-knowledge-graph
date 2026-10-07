use super::*;

mod access;
mod aggregation;
mod edges;
mod foreign_keys;
mod hops;
mod hydration;
mod keys;
mod neighbors;
mod pathfinding;
mod predicates;
mod requirements;

struct KeyScan<'a> {
    relation: RelationId,
    predicates: Vec<Expression<'a>>,
    memberships: Vec<(&'a str, (DefinitionId, OutputId))>,
}

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub(super) fn requires_authorization_scan(&self, entity: &str) -> bool {
        self.catalog
            .entity_minimum_access_level(entity)
            .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
    }

    pub fn bind_scan(&mut self, relation: RelationId, input: ScanInput) -> Result<()> {
        self.require_building(relation.block)?;
        let declaration = self
            .block_mut(relation.block)?
            .relations
            .get_mut(relation.slot)
            .ok_or(GraphError::MissingOutput)?;
        if declaration.input.is_some() {
            return Err(GraphError::ReusedRelation);
        }
        declaration.input = Some(input);
        Ok(())
    }

    pub fn input_node(&self, root: BlockId, index: usize) -> Result<RelationId> {
        self.relations(root)?
            .find(|relation| {
                self.relation(*relation)
                    .is_ok_and(|relation| relation.input == Some(ScanInput::Node(index)))
            })
            .ok_or(GraphError::MissingOutput)
    }

    pub fn input_identity(
        &self,
        root: BlockId,
        input: &crate::input::Input,
        index: usize,
    ) -> Result<ColumnRef<'a>> {
        if let Ok(relation) = self.input_node(root, index) {
            return self.stored_column(relation, "id");
        }
        let node = input.nodes.get(index).ok_or(GraphError::MissingOutput)?;
        for relation in self.relations(root)? {
            let Some(ScanInput::Relationship(index)) = self.relation(relation)?.input else {
                continue;
            };
            let relationship = input
                .relationships
                .get(index)
                .ok_or(GraphError::MissingOutput)?;
            let (start, end) = relationship.direction.edge_columns();
            if relationship.from == node.id {
                return self.column(relation, start);
            }
            if relationship.to == node.id {
                return self.column(relation, end);
            }
        }
        Err(GraphError::MissingOutput)
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub fn plan(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        self.plan_with_options(
            input,
            crate::passes::plan::HydrationCompileOptions::default(),
        )
    }

    pub fn plan_with_options(
        &mut self,
        input: &crate::input::Input,
        options: crate::passes::plan::HydrationCompileOptions,
    ) -> Result<BlockId> {
        use crate::input::{NodeExistence, QueryType};
        let mut prepared = input.clone();
        for (node, original) in prepared.nodes.iter_mut().zip(&input.nodes) {
            if requirements::node_requirements(self.catalog, input, original)?.needs_stored_row() {
                node.existence = NodeExistence::CurrentRow;
            }
        }
        match prepared.query_type {
            QueryType::PathFinding => self.pathfinding(&prepared),
            QueryType::Neighbors => self.neighbors(&prepared),
            QueryType::Hydration => self.hydration(&prepared, options),
            QueryType::Aggregation => self.aggregation(&prepared),
            QueryType::Traversal => self.traversal(&prepared),
        }
    }

    pub fn traversal(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        if input.query_type != crate::input::QueryType::Traversal {
            return Err(GraphError::UnsupportedInput("expected traversal".into()));
        }
        if input
            .relationships
            .iter()
            .any(|relationship| relationship.hops.max > 1)
        {
            self.variable_hops(input)
        } else {
            self.access(input)
        }
    }
}
