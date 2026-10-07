use super::*;

pub struct BlockView<'graph, 'catalog, M: QueryDataModel + ?Sized> {
    pub catalog: &'catalog M,
    id: BlockId,
    relations: &'graph [Relation<'catalog>],
}

impl<'catalog, M: QueryDataModel + ?Sized> BlockView<'_, 'catalog, M> {
    pub fn relation(&self, id: RelationId) -> Result<&Relation<'catalog>> {
        if id.block != self.id {
            return Err(GraphError::OutsideBlock);
        }
        Ok(&self.relations[id.slot])
    }

    pub fn stored_column(&self, relation: RelationId, name: &str) -> Result<ColumnRef<'catalog>> {
        let Source::Stored(table) = self.relation(relation)?.source else {
            return Err(GraphError::MissingOutput);
        };
        let column = table
            .column(name)
            .ok_or_else(|| GraphError::UnknownStored(format!("{}.{name}", table.name())))?;
        Ok(ColumnRef {
            relation,
            port: Port::Stored(column),
        })
    }
}

impl<'catalog, M: QueryDataModel + ?Sized, E, O> QueryGraph<'catalog, M, E, O> {
    pub fn reachable_blocks(&self, root: BlockId) -> Result<Vec<BlockId>> {
        let mut pending = vec![root];
        let mut visited = HashSet::new();
        let mut blocks = Vec::new();
        while let Some(id) = pending.pop() {
            let block = self.block(id)?;
            if !visited.insert(id) {
                continue;
            }
            blocks.push(id);
            match &block.body {
                Body::Select { relations, .. } => {
                    for relation in relations.iter().rev() {
                        match relation.source {
                            Source::Derived(body) => pending.push(body),
                            Source::Definition(definition) => {
                                pending.push(self.definition(definition)?.body)
                            }
                            Source::Stored(_) => {}
                        }
                    }
                }
                Body::UnionAll { arms, .. } => pending.extend(arms.iter().rev().copied()),
            }
            pending.extend(
                block
                    .definitions
                    .iter()
                    .rev()
                    .map(|definition| definition.body),
            );
        }
        Ok(blocks)
    }
}

impl<'catalog, M: QueryDataModel + ?Sized, E, L>
    QueryGraph<'catalog, M, E, Relational<'catalog, L>>
{
    pub fn walk_operations<Error: From<GraphError>>(
        &self,
        root: BlockId,
        mut visit: impl FnMut(
            &BlockView<'_, 'catalog, M>,
            &Relational<'catalog, L>,
            Option<&Relational<'catalog, L>>,
        ) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for id in self.reachable_blocks(root)? {
            if let Body::Select {
                relations,
                operation,
                ..
            } = &self.block(id)?.body
            {
                let view = BlockView {
                    catalog: self.catalog,
                    id,
                    relations,
                };
                operation.walk(None, &mut |operation, parent| {
                    visit(&view, operation, parent)
                })?;
            }
        }
        Ok(())
    }

    pub fn walk_operations_mut<Error: From<GraphError>>(
        &mut self,
        root: BlockId,
        mut visit: impl FnMut(
            &BlockView<'_, 'catalog, M>,
            &mut Relational<'catalog, L>,
        ) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        let catalog = self.catalog;
        for id in self.reachable_blocks(root)? {
            if let Body::Select {
                relations,
                operation,
                ..
            } = &mut self.block_mut(id)?.body
            {
                let view = BlockView {
                    catalog,
                    id,
                    relations,
                };
                operation.walk_mut(&mut |operation| visit(&view, operation))?;
            }
        }
        Ok(())
    }
}

impl<'catalog, L> Relational<'catalog, L> {
    pub fn inputs(&self) -> impl Iterator<Item = &Self> {
        let inputs = match self {
            Self::Join { left, right, .. } => [Some(left.as_ref()), Some(right.as_ref())],
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => [Some(input.as_ref()), None],
            Self::One | Self::Source { .. } => [None, None],
        };
        inputs.into_iter().flatten()
    }

    pub fn inputs_mut(&mut self) -> impl Iterator<Item = &mut Self> {
        let inputs = match self {
            Self::Join { left, right, .. } => [Some(left.as_mut()), Some(right.as_mut())],
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => [Some(input.as_mut()), None],
            Self::One | Self::Source { .. } => [None, None],
        };
        inputs.into_iter().flatten()
    }

    pub fn walk<Error>(
        &self,
        parent: Option<&Self>,
        visit: &mut impl FnMut(&Self, Option<&Self>) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for input in self.inputs() {
            input.walk(Some(self), visit)?;
        }
        visit(self, parent)
    }

    pub fn walk_mut<Error>(
        &mut self,
        visit: &mut impl FnMut(&mut Self) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for input in self.inputs_mut() {
            input.walk_mut(visit)?;
        }
        visit(self)
    }
}
