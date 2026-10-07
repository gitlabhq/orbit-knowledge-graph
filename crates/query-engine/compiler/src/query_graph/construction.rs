use super::*;

impl<'a, L> QueryOperation<'a, L> {
    pub fn outputs(&self) -> impl Iterator<Item = (OutputId, &Output<'a>)> {
        self.outputs.iter().enumerate().map(|(slot, output)| {
            (
                OutputId {
                    block: self.block,
                    slot,
                },
                output,
            )
        })
    }

    pub fn input(&self) -> Result<&Relational<'a, L>> {
        match &self.kind {
            QueryKind::Project(input) => Ok(input),
            QueryKind::UnionAll(_) => Err(GraphError::ExpectedSelect),
        }
    }

    pub fn into_projection(self) -> Result<(Relational<'a, L>, Vec<(String, Expression<'a>)>)> {
        let QueryKind::Project(input) = self.kind else {
            return Err(GraphError::ExpectedSelect);
        };
        let outputs = self
            .outputs
            .into_iter()
            .map(|output| Ok((output.label, output.value.ok_or(GraphError::MissingOutput)?)))
            .collect::<Result<_>>()?;
        Ok((input, outputs))
    }

    fn preserves_contract(&self, existing: &Self) -> bool {
        let (Ok(input), Ok(previous)) = (self.input(), existing.input()) else {
            return false;
        };
        let scalar = |input: &Relational<'a, L>| {
            input.aggregate_input().is_some() && input.groups().is_empty()
        };
        self.block == existing.block
            && scalar(input) == scalar(previous)
            && self.outputs.len() == existing.outputs.len()
            && self
                .outputs
                .iter()
                .zip(&existing.outputs)
                .all(|(new, old)| new.label == old.label && new.data_type == old.data_type)
    }
}

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub fn read_relation(&self, relation: RelationId, read: ReadMode) -> Result<Relational<'a, L>> {
        if matches!(read, ReadMode::Current)
            && !matches!(self.relation(relation)?.source, Source::Stored(_))
        {
            return Err(GraphError::LatestShape);
        }
        Ok(Relational {
            block: relation.block,
            kind: OperationKind::Source { relation, read },
            columns: self.relation_columns(relation)?,
            occurrences: HashSet::from([relation]),
            expanded: vec![],
        })
    }

    pub fn unit_relation(&self, block: BlockId) -> Result<Relational<'a, L>> {
        self.block(block)?;
        Ok(Relational {
            block,
            kind: OperationKind::One,
            columns: vec![],
            occurrences: HashSet::new(),
            expanded: vec![],
        })
    }

    pub fn filter_relation(
        &self,
        input: Relational<'a, L>,
        predicate: Expression<'a>,
    ) -> Result<Relational<'a, L>> {
        if input.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        self.require_predicate(&input, &predicate)?;
        Ok(input.wrap(|input| OperationKind::Filter { input, predicate }))
    }

    pub fn join_relations(
        &self,
        left: Relational<'a, L>,
        right: Relational<'a, L>,
        kind: JoinKind,
        condition: Expression<'a>,
    ) -> Result<Relational<'a, L>> {
        self.block(left.block)?;
        self.block(right.block)?;
        if left.aggregate_input().is_some() || right.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        if left.block != right.block {
            return Err(GraphError::OutsideBlock);
        }
        if !left.occurrences.is_disjoint(&right.occurrences) {
            return Err(GraphError::ReusedRelation);
        }
        if matches!(kind, JoinKind::Membership) {
            let Expression::Equal(value, key) = &condition else {
                return Err(GraphError::JoinShape);
            };
            let (Expression::Column(value), Expression::Column(key)) =
                (value.as_ref(), key.as_ref())
            else {
                return Err(GraphError::JoinShape);
            };
            if !left.columns.contains(value) || !right.columns.contains(key) {
                return Err(GraphError::JoinShape);
            }
        }
        if matches!(kind, JoinKind::Cross) && condition != Expression::Boolean(true) {
            return Err(GraphError::JoinShape);
        }
        let mut columns = left.columns.clone();
        columns.extend_from_slice(&right.columns);
        let mut expanded = left.expanded.clone();
        expanded.extend_from_slice(&right.expanded);
        self.require_references(left.block, &columns, &condition)?;
        if condition.aggregate() {
            return Err(GraphError::AggregatePlacement);
        }
        if self.expression_type_in(&condition, &expanded)? != ValueType::Scalar(SqlType::Bool) {
            return Err(GraphError::ExpressionType);
        }
        if matches!(kind, JoinKind::Semi | JoinKind::Membership) {
            columns.clone_from(&left.columns);
            expanded.clone_from(&left.expanded);
        }
        let mut occurrences = left.occurrences.clone();
        occurrences.extend(right.occurrences.iter().copied());
        Ok(Relational {
            block: left.block,
            columns,
            occurrences,
            expanded,
            kind: OperationKind::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                condition,
            },
        })
    }

    pub fn membership_relation(
        &self,
        input: Relational<'a, L>,
        value: ColumnRef<'a>,
        key: ColumnRef<'a>,
    ) -> Result<Relational<'a, L>> {
        self.join_relations(
            input,
            self.read_relation(key.relation, ReadMode::Raw)?,
            JoinKind::Membership,
            Expression::equal(Expression::Column(value), Expression::Column(key)),
        )
    }

    pub fn sort_relation(
        &self,
        input: Relational<'a, L>,
        keys: Vec<(ColumnRef<'a>, bool)>,
    ) -> Result<Relational<'a, L>> {
        self.block(input.block)?;
        if keys.is_empty() {
            return Ok(input);
        }
        for (column, _) in &keys {
            self.require_value(&input, &Expression::Column(*column), false)?;
        }
        Ok(input.wrap(|input| OperationKind::Sort { input, keys }))
    }

    pub fn limit_relation(
        &self,
        input: Relational<'a, L>,
        count: u32,
    ) -> Result<Relational<'a, L>> {
        self.block(input.block)?;
        Ok(input.wrap(|input| OperationKind::Limit { input, count }))
    }

    pub fn aggregate_relation(
        &self,
        input: Relational<'a, L>,
        groups: Vec<Expression<'a>>,
    ) -> Result<Relational<'a, L>> {
        self.block(input.block)?;
        if input.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        for group in &groups {
            if group.aggregate() {
                return Err(GraphError::AggregatePlacement);
            }
            self.require_value(&input, group, false)?;
        }
        let columns = groups
            .iter()
            .filter_map(|group| match group {
                Expression::Column(column) => Some(*column),
                _ => None,
            })
            .collect();
        let mut operation = input.wrap(|input| OperationKind::Aggregate { input, groups });
        operation.columns = columns;
        Ok(operation)
    }

    pub fn expand_relation(
        &self,
        input: Relational<'a, L>,
        column: ColumnRef<'a>,
    ) -> Result<Relational<'a, L>> {
        if input.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        if !matches!(
            self.require_value(&input, &Expression::Column(column), false)?,
            ValueType::Array(_)
        ) {
            return Err(GraphError::ExpectedArray);
        }
        if input.expanded.contains(&column) {
            return Err(GraphError::ExpectedArray);
        }
        let mut operation = input.wrap(|input| OperationKind::Expand { input, column });
        operation.expanded.push(column);
        Ok(operation)
    }

    pub fn materialize_relation(
        &self,
        input: Relational<'a, L>,
        relation: RelationId,
    ) -> Result<Relational<'a, L>> {
        self.block(input.block)?;
        if input.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        if !input.occurrences.contains(&relation) {
            return Err(GraphError::OperationVisibility);
        }
        Ok(input.wrap(|input| OperationKind::Materialize { input, relation }))
    }

    pub fn project_values(
        &self,
        input: Relational<'a, L>,
        values: impl IntoIterator<Item = (String, Expression<'a>)>,
    ) -> Result<QueryOperation<'a, L>> {
        self.block(input.block)?;
        let mut operation = QueryOperation {
            block: input.block,
            kind: QueryKind::Project(input),
            outputs: vec![],
        };
        for (label, value) in values {
            self.append_projection(&mut operation, label, value)?;
        }
        if operation.outputs.is_empty() {
            return Err(GraphError::EmptyProjection);
        }
        Ok(operation)
    }

    pub fn append_projection(
        &self,
        operation: &mut QueryOperation<'a, L>,
        label: impl Into<String>,
        value: Expression<'a>,
    ) -> Result<OutputId> {
        let data_type = self.require_value(operation.input()?, &value, false)?;
        let id = OutputId {
            block: operation.block,
            slot: operation.outputs.len(),
        };
        operation.outputs.push(Output {
            label: label.into(),
            data_type,
            value: Some(value),
        });
        Ok(id)
    }

    pub fn finish_query(&mut self, operation: QueryOperation<'a, L>) -> Result<BlockId> {
        let block = operation.block;
        self.require_building(block)?;
        self.block_mut(block)?.operation = Some(operation);
        Ok(block)
    }

    pub fn substitute_query(&mut self, replacement: QueryOperation<'a, L>) -> Result<()> {
        let block = replacement.block;
        if !replacement.preserves_contract(self.query_operation(block)?) {
            return Err(GraphError::OutputContract);
        }
        self.block_mut(block)?.operation = Some(replacement);
        Ok(())
    }

    pub(super) fn require_value(
        &self,
        input: &Relational<'a, L>,
        value: &Expression<'a>,
        inside_aggregate: bool,
    ) -> Result<ValueType> {
        let source = input.aggregate_input().unwrap_or(input);
        self.require_references(input.block, &source.columns, value)?;
        if input.groups().contains(value) {
            return self.expression_type_in(value, &input.expanded);
        }
        match value {
            Expression::Count
            | Expression::CountIf(_)
            | Expression::Sum { .. }
            | Expression::Aggregate { .. }
            | Expression::LatestPath { .. } => {
                if input.aggregate_input().is_none() || inside_aggregate {
                    return Err(GraphError::AggregatePlacement);
                }
                for child in value.children() {
                    self.require_value(input, child, true)?;
                }
            }
            Expression::Column(_) if input.aggregate_input().is_some() && !inside_aggregate => {
                return Err(GraphError::Grouping);
            }
            _ => {
                for child in value.children() {
                    self.require_value(input, child, inside_aggregate)?;
                }
            }
        }
        self.expression_type_in(value, &input.expanded)
    }

    fn require_references(
        &self,
        block: BlockId,
        columns: &[ColumnRef<'a>],
        value: &Expression<'a>,
    ) -> Result<()> {
        self.block(block)?;
        value.references(&mut |column, subquery| {
            self.check_column(block, column)?;
            if subquery || columns.contains(&column) {
                Ok(())
            } else {
                Err(GraphError::OperationVisibility)
            }
        })
    }

    fn require_predicate(
        &self,
        input: &Relational<'a, L>,
        predicate: &Expression<'a>,
    ) -> Result<()> {
        self.require_references(input.block, &input.columns, predicate)?;
        if predicate.aggregate() {
            return Err(GraphError::AggregatePlacement);
        }
        if self.expression_type_in(predicate, &input.expanded)? != ValueType::Scalar(SqlType::Bool)
        {
            return Err(GraphError::ExpressionType);
        }
        Ok(())
    }
}

impl<'a, L> Relational<'a, L> {
    pub(super) fn wrap(self, build: impl FnOnce(Box<Self>) -> OperationKind<'a, L>) -> Self {
        Self {
            block: self.block,
            columns: self.columns.clone(),
            occurrences: self.occurrences.clone(),
            expanded: self.expanded.clone(),
            kind: build(Box::new(self)),
        }
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub fn latest_relation(
        &self,
        input: PhysicalOperation<'a>,
        version: ColumnRef<'a>,
        deletion: Option<ColumnRef<'a>>,
    ) -> Result<PhysicalOperation<'a>> {
        self.block(input.block)?;
        if input.aggregate_input().is_some() || !input.columns.contains(&version) {
            return Err(GraphError::LatestShape);
        }
        let mut source = &input;
        loop {
            match source.kind() {
                OperationKind::Filter { input, .. } => source = input,
                OperationKind::Join {
                    left,
                    kind: JoinKind::Membership | JoinKind::Semi,
                    ..
                } => source = left,
                _ => break,
            }
        }
        if !matches!(source.kind(), OperationKind::Source { relation, read: ReadMode::Raw } if *relation == version.relation)
        {
            return Err(GraphError::LatestShape);
        }
        let Source::Stored(table) = self.relation(version.relation)?.source else {
            return Err(GraphError::LatestShape);
        };
        let keys = self
            .catalog
            .table_sort_key(table.name())
            .filter(|keys| !keys.is_empty())
            .ok_or(GraphError::LatestShape)?;
        let keys = keys
            .iter()
            .map(|key| self.stored_column(version.relation, key))
            .collect::<Result<Vec<_>>>()?;
        if keys.iter().any(|key| !input.columns.contains(key)) {
            return Err(GraphError::LatestShape);
        }
        if let Some(deleted) = deletion
            && (deleted.relation != version.relation
                || !input.columns.contains(&deleted)
                || self.column_type(deleted)? != ValueType::Scalar(SqlType::Bool))
        {
            return Err(GraphError::LatestShape);
        }
        Ok(input.wrap(|input| OperationKind::Latest {
            input,
            requirement: LatestRows {
                version,
                keys,
                deletion,
            },
        }))
    }
}
