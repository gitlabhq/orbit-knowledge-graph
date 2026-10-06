use super::*;

mod access;
mod aggregation;
mod edges;
mod foreign_keys;
mod keys;
mod predicates;

struct KeyScan<'a> {
    relation: RelationId,
    predicates: Vec<Expression<'a>>,
    memberships: Vec<(&'a str, (DefinitionId, OutputId))>,
}

impl<'catalog, M: QueryDataModel + ?Sized, E, O> QueryGraph<'catalog, M, E, O> {
    pub fn bind_scan(&mut self, relation: RelationId, input: ScanInput) -> Result<()> {
        let Body::Select { relations, .. } = &mut self.block_mut(relation.block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        relations[relation.slot].input = Some(input);
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
    ) -> Result<ColumnRef<'catalog>> {
        if let Ok(relation) = self.input_node(root, index) {
            return self.stored_column(relation, "id");
        }
        let node = &input.nodes[index];
        for relation in self.relations(root)? {
            let Some(ScanInput::Relationship(index)) = self.relation(relation)?.input else {
                continue;
            };
            let relationship = &input.relationships[index];
            let (start, end) = relationship.direction.edge_columns();
            if relationship.from == node.id {
                return self.stored_column(relation, start);
            }
            if relationship.to == node.id {
                return self.stored_column(relation, end);
            }
        }
        Err(GraphError::MissingOutput)
    }
}

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub fn plan(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        match input.query_type {
            crate::input::QueryType::Aggregation => self.aggregation(input),
            _ => self.traversal(input),
        }
    }

    pub fn traversal(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        if input.query_type != crate::input::QueryType::Traversal {
            return Err(GraphError::UnsupportedInput("expected traversal".into()));
        }
        self.access(input)
    }
}
