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

    pub fn rewrite_operations<Error: From<GraphError>>(
        mut self,
        root: BlockId,
        mut rewrite: impl FnMut(
            &mut Self,
            Relational<'a, L>,
        ) -> std::result::Result<Relational<'a, L>, Error>,
    ) -> std::result::Result<Self, Error> {
        for block in self.reachable_blocks(root)? {
            let query = self
                .block_mut(block)?
                .operation
                .take()
                .ok_or(GraphError::EmptyProjection)?;
            let dependencies = self.block(block)?.required.clone();
            let kind = match query.kind {
                QueryKind::Project(input) => {
                    QueryKind::Project(self.rewrite_operation(input, &mut rewrite)?)
                }
                QueryKind::UnionAll(arms) => QueryKind::UnionAll(arms),
            };
            if !self.block(block)?.required.is_subset(&dependencies) {
                return Err(GraphError::DefinitionVisibility.into());
            }
            if self.block(block)?.operation.is_some() {
                return Err(GraphError::FinishedQuery.into());
            }
            self.block_mut(block)?.operation = Some(QueryOperation { kind, ..query });
        }
        Ok(self)
    }

    fn rewrite_operation<Error: From<GraphError>>(
        &mut self,
        operation: Relational<'a, L>,
        rewrite: &mut impl FnMut(
            &mut Self,
            Relational<'a, L>,
        ) -> std::result::Result<Relational<'a, L>, Error>,
    ) -> std::result::Result<Relational<'a, L>, Error> {
        use OperationKind::*;
        let Relational {
            block,
            kind,
            columns,
            occurrences,
            expanded,
        } = operation;
        let mut child =
            |input: Box<Relational<'a, L>>| self.rewrite_operation(*input, rewrite).map(Box::new);
        let kind = match kind {
            One => One,
            Source { relation, read } => Source { relation, read },
            Filter { input, predicate } => Filter {
                input: child(input)?,
                predicate,
            },
            Join {
                left,
                right,
                kind,
                condition,
            } => Join {
                left: child(left)?,
                right: child(right)?,
                kind,
                condition,
            },
            Aggregate { input, groups } => Aggregate {
                input: child(input)?,
                groups,
            },
            Expand { input, column } => Expand {
                input: child(input)?,
                column,
            },
            Materialize { input, relation } => Materialize {
                input: child(input)?,
                relation,
            },
            Latest { input, requirement } => Latest {
                input: child(input)?,
                requirement,
            },
            Sort { input, keys } => Sort {
                input: child(input)?,
                keys,
            },
            FirstBy { input, keys } => FirstBy {
                input: child(input)?,
                keys,
            },
            Limit { input, count } => Limit {
                input: child(input)?,
                count,
            },
        };
        let operation = Relational {
            block,
            kind,
            columns,
            occurrences,
            expanded,
        };
        let columns = operation.columns.clone();
        let occurrences = operation.occurrences.clone();
        let expanded = operation.expanded.clone();
        let groups = operation.groups().to_vec();
        let aggregate = operation
            .aggregate_input()
            .map(|input| (input.columns.clone(), input.expanded.clone()));
        let replacement = rewrite(self, operation)?;
        let same_aggregate = match (aggregate, replacement.aggregate_input()) {
            (None, None) => true,
            (Some((columns, expanded)), Some(input)) => {
                columns == input.columns && expanded == input.expanded
            }
            _ => false,
        };
        if replacement.block != block
            || replacement.columns != columns
            || replacement.occurrences != occurrences
            || replacement.expanded != expanded
            || replacement.groups() != groups
            || !same_aggregate
        {
            return Err(GraphError::OutputContract.into());
        }
        Ok(replacement)
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
