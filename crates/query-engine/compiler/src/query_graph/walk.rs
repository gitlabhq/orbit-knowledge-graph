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
        self.relations.get(id.slot).ok_or(GraphError::MissingOutput)
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

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
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
            for relation in block.relations.iter().rev() {
                match relation.source {
                    Source::Derived(body) => pending.push(body),
                    Source::Definition(definition) => {
                        pending.push(self.definition(definition)?.body)
                    }
                    Source::Stored(_) => {}
                }
            }
            if let QueryKind::UnionAll(arms) = &self.query_operation(id)?.kind {
                pending.extend(arms.iter().rev().copied());
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

    pub fn walk_blocks<Error: From<GraphError>>(
        &self,
        root: BlockId,
        mut visit: impl FnMut(BlockId, &QueryOperation<'a, L>) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for id in self.reachable_blocks(root)? {
            visit(id, self.query_operation(id)?)?;
        }
        Ok(())
    }

    pub fn walk_operations<Error: From<GraphError>>(
        &self,
        root: BlockId,
        mut visit: impl FnMut(
            &BlockView<'_, 'a, M>,
            &Relational<'a, L>,
            Option<&Relational<'a, L>>,
        ) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for id in self.reachable_blocks(root)? {
            let block = self.block(id)?;
            if let QueryKind::Project(operation) = &self.query_operation(id)?.kind {
                let view = BlockView {
                    catalog: self.catalog,
                    id,
                    relations: &block.relations,
                };
                operation.walk(None, &mut |operation, parent| {
                    visit(&view, operation, parent)
                })?;
            }
        }
        Ok(())
    }
}

impl<'a, L> Relational<'a, L> {
    pub fn inputs(&self) -> impl Iterator<Item = &Self> {
        let inputs = match &self.kind {
            OperationKind::Join { left, right, .. } => [Some(left.as_ref()), Some(right.as_ref())],
            OperationKind::Filter { input, .. }
            | OperationKind::Aggregate { input, .. }
            | OperationKind::Expand { input, .. }
            | OperationKind::Materialize { input, .. }
            | OperationKind::Latest { input, .. }
            | OperationKind::Sort { input, .. }
            | OperationKind::FirstBy { input, .. }
            | OperationKind::Limit { input, .. } => [Some(input.as_ref()), None],
            OperationKind::One | OperationKind::Source { .. } => [None, None],
        };
        inputs.into_iter().flatten()
    }

    pub(super) fn inputs_mut(&mut self) -> impl Iterator<Item = &mut Self> {
        let inputs = match &mut self.kind {
            OperationKind::Join { left, right, .. } => [Some(left.as_mut()), Some(right.as_mut())],
            OperationKind::Filter { input, .. }
            | OperationKind::Aggregate { input, .. }
            | OperationKind::Expand { input, .. }
            | OperationKind::Materialize { input, .. }
            | OperationKind::Latest { input, .. }
            | OperationKind::Sort { input, .. }
            | OperationKind::FirstBy { input, .. }
            | OperationKind::Limit { input, .. } => [Some(input.as_mut()), None],
            OperationKind::One | OperationKind::Source { .. } => [None, None],
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
}
