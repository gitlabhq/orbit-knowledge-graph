use super::*;

impl<'catalog, M: QueryDataModel + ?Sized, E, O> QueryGraph<'catalog, M, E, O> {
    pub fn validate(
        &self,
        root: BlockId,
        check: impl Fn(BlockId, &[Projection<E>], &O) -> Result<()>,
    ) -> Result<()> {
        self.visit(root, &HashSet::new(), &mut HashSet::new(), &check)
    }

    fn visit(
        &self,
        id: BlockId,
        inherited: &HashSet<DefinitionId>,
        owned: &mut HashSet<BlockId>,
        check: &impl Fn(BlockId, &[Projection<E>], &O) -> Result<()>,
    ) -> Result<()> {
        let block = self.block(id)?;
        if !owned.insert(id) {
            return Err(GraphError::BlockOwnership);
        }
        let mut visible = inherited.clone();
        for (slot, definition) in block.definitions.iter().enumerate() {
            let declaration = DefinitionId { block: id, slot };
            if definition.recursive {
                visible.insert(declaration);
            }
            self.visit(definition.body, &visible, owned, check)?;
            visible.insert(declaration);
        }
        match &block.body {
            Body::Select {
                relations,
                outputs,
                operation,
                ..
            } => {
                for relation in relations {
                    match relation.source {
                        Source::Derived(body) => self.visit(body, &visible, owned, check)?,
                        Source::Definition(definition) if !visible.contains(&definition) => {
                            return Err(GraphError::DefinitionVisibility);
                        }
                        _ => {}
                    }
                }
                check(id, outputs, operation)?;
            }
            Body::UnionAll { arms, labels } => {
                for arm in arms {
                    if self.output_count(*arm)? != labels.len() {
                        return Err(GraphError::UnionShape);
                    }
                    self.visit(*arm, &visible, owned, check)?;
                }
            }
        }
        Ok(())
    }
}

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, LoweredOperation<'catalog>>
{
    fn source_columns(&self, relation: RelationId) -> Result<Vec<ColumnRef<'catalog>>> {
        match self.relation(relation)?.source {
            Source::Stored(table) => table
                .columns()
                .map(|column| self.stored_port(relation, column))
                .collect(),
            Source::Derived(body) => self
                .outputs(body)?
                .map(|output| self.output_column(relation, output))
                .collect(),
            Source::Definition(definition) => self
                .outputs(self.definition(definition)?.body)?
                .map(|output| self.output_column(relation, output))
                .collect(),
        }
    }

    pub(super) fn operation_columns(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        used: &mut HashSet<RelationId>,
    ) -> Result<Vec<ColumnRef<'catalog>>> {
        use Relational::*;
        let check = |expression: &Expression<'catalog>, columns: &[ColumnRef<'catalog>]| {
            expression.columns(&mut |column| {
                if columns.contains(&column) {
                    Ok(())
                } else {
                    Err(GraphError::OperationVisibility)
                }
            })?;
            if expression.aggregate() {
                return Err(GraphError::AggregatePlacement);
            }
            if self.expression_type(expression, &mut HashSet::new())?
                != ValueType::Scalar(SqlType::Bool)
            {
                return Err(GraphError::ExpressionType);
            }
            Ok(())
        };
        match operation {
            One => Ok(vec![]),
            Source { relation, read } => {
                if relation.block != block {
                    return Err(GraphError::OutsideBlock);
                }
                if !used.insert(*relation) {
                    return Err(GraphError::ReusedRelation);
                }
                if matches!(read, ReadMode::Current)
                    && !matches!(
                        self.relation(*relation)?.source,
                        crate::query_graph::Source::Stored(_)
                    )
                {
                    return Err(GraphError::LatestShape);
                }
                self.source_columns(*relation)
            }
            Filter { input, predicate } => {
                let columns = self.operation_columns(block, input, used)?;
                check(predicate, &columns)?;
                Ok(columns)
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left = self.operation_columns(block, left, used)?;
                let right = self.operation_columns(block, right, used)?;
                if matches!(kind, JoinKind::Membership) {
                    let Expression::Equal(value, key) = condition else {
                        return Err(GraphError::JoinShape);
                    };
                    let (Expression::Column(value), Expression::Column(key)) =
                        (value.as_ref(), key.as_ref())
                    else {
                        return Err(GraphError::JoinShape);
                    };
                    if !left.contains(value) || !right.contains(key) {
                        return Err(GraphError::JoinShape);
                    }
                    if self.column_type(*value, &mut HashSet::new())?
                        != self.column_type(*key, &mut HashSet::new())?
                    {
                        return Err(GraphError::ExpressionType);
                    }
                }
                let mut all = left.clone();
                all.extend(right);
                check(condition, &all)?;
                Ok(if matches!(kind, JoinKind::Semi | JoinKind::Membership) {
                    left
                } else {
                    all
                })
            }
            Aggregate { input, groups } => {
                let columns = self.operation_columns(block, input, used)?;
                for group in groups {
                    if group.aggregate() {
                        return Err(GraphError::AggregatePlacement);
                    }
                    self.expression_type(group, &mut HashSet::new())?;
                    group.columns(&mut |column| {
                        if columns.contains(&column) {
                            Ok(())
                        } else {
                            Err(GraphError::Grouping)
                        }
                    })?;
                }
                Ok(groups
                    .iter()
                    .filter_map(|group| match group {
                        Expression::Column(column) => Some(*column),
                        _ => None,
                    })
                    .collect())
            }
            Expand { input, column } => {
                let columns = self.operation_columns(block, input, used)?;
                if !columns.contains(column) {
                    return Err(GraphError::OperationVisibility);
                }
                if !matches!(
                    self.column_type(*column, &mut HashSet::new())?,
                    ValueType::Array(_)
                ) {
                    return Err(GraphError::ExpectedArray);
                }
                Ok(columns)
            }
            Materialize { input, relation } => {
                let columns = self.operation_columns(block, input, used)?;
                if !used.contains(relation) || input.aggregate_input().is_some() {
                    return Err(GraphError::OperationVisibility);
                }
                Ok(columns)
            }
            Sort { input, keys } => {
                let columns = self.operation_columns(block, input, used)?;
                if keys.iter().any(|(column, _)| !columns.contains(column)) {
                    return Err(GraphError::OperationVisibility);
                }
                Ok(columns)
            }
            FirstBy { input, keys } => {
                let columns = self.operation_columns(block, input, used)?;
                if keys.is_empty() || keys.iter().any(|column| !columns.contains(column)) {
                    return Err(GraphError::LatestShape);
                }
                Ok(columns)
            }
            Limit { input, .. } => self.operation_columns(block, input, used),
            Latest { requirement, .. } => match *requirement {},
        }
    }

    fn expression_type(
        &self,
        expression: &Expression<'catalog>,
        visiting: &mut HashSet<OutputId>,
    ) -> Result<ValueType> {
        match expression {
            Expression::Predicate {
                operator,
                value,
                argument,
                ..
            } => {
                use crate::input::FilterOp;
                let value_type = self.expression_type(value, visiting)?;
                if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
                    if argument.is_some() {
                        return Err(GraphError::ExpressionType);
                    }
                } else {
                    let argument = argument.as_ref().ok_or(GraphError::ExpressionType)?;
                    let argument_type = self.expression_type(argument, visiting)?;
                    let valid = match operator {
                        FilterOp::In => argument_type == ValueType::Array(Box::new(value_type)),
                        FilterOp::Eq
                        | FilterOp::Ne
                        | FilterOp::Gt
                        | FilterOp::Lt
                        | FilterOp::Gte
                        | FilterOp::Lte => value_type == argument_type,
                        _ => {
                            value_type == ValueType::Scalar(SqlType::String)
                                && argument_type == value_type
                        }
                    };
                    if !valid {
                        return Err(GraphError::ExpressionType);
                    }
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
            Expression::Integer(_) | Expression::Count => Ok(ValueType::Scalar(SqlType::Int64)),
            Expression::LatestPath {
                path,
                version,
                deletion,
            } => {
                if self.column_type(*path, visiting)? != ValueType::Scalar(SqlType::String)
                    || self.column_type(*deletion, visiting)? != ValueType::Scalar(SqlType::Bool)
                {
                    return Err(GraphError::ExpressionType);
                }
                self.column_type(*version, visiting)?;
                Ok(ValueType::Scalar(SqlType::String))
            }
            Expression::Boolean(_) => Ok(ValueType::Scalar(SqlType::Bool)),
            Expression::Text(_) => Ok(ValueType::Scalar(SqlType::String)),
            Expression::Excerpt { value, .. } => {
                if self.expression_type(value, visiting)? != ValueType::Scalar(SqlType::String) {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::String))
            }
            Expression::ToString(value) => {
                self.expression_type(value, visiting)?;
                Ok(ValueType::Scalar(SqlType::String))
            }
            Expression::Bucket { value, .. } => {
                let ty = self.expression_type(value, visiting)?;
                if !matches!(
                    ty,
                    ValueType::Scalar(SqlType::Date | SqlType::Timestamp { .. })
                ) {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ty)
            }
            Expression::Parameter { data_type, .. } => Ok(match data_type {
                SqlType::Array(element) => {
                    ValueType::Array(Box::new(ValueType::Scalar((*element).into())))
                }
                ty => ValueType::Scalar(*ty),
            }),
            Expression::Integers(_) => Ok(ValueType::Array(Box::new(ValueType::Scalar(
                SqlType::Int64,
            )))),
            Expression::Tuple(values) => Ok(ValueType::Tuple(
                values
                    .iter()
                    .map(|value| self.expression_type(value, visiting))
                    .collect::<Result<_>>()?,
            )),
            Expression::Array(values) => {
                let first = values.first().ok_or(GraphError::ExpectedArray)?;
                let element = self.expression_type(first, visiting)?;
                for value in &values[1..] {
                    if self.expression_type(value, visiting)? != element {
                        return Err(GraphError::ExpressionType);
                    }
                }
                Ok(ValueType::Array(Box::new(element)))
            }
            Expression::Field { tuple, index } => {
                let ValueType::Tuple(fields) = self.expression_type(tuple, visiting)? else {
                    return Err(GraphError::ExpressionType);
                };
                fields
                    .get(*index)
                    .cloned()
                    .ok_or(GraphError::ExpressionType)
            }
            Expression::Keep { condition, value } => {
                if self.expression_type(condition, visiting)? != ValueType::Scalar(SqlType::Bool) {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Array(Box::new(
                    self.expression_type(value, visiting)?,
                )))
            }
            Expression::Concat(arrays) => {
                let ty = self
                    .expression_type(arrays.first().ok_or(GraphError::ExpectedArray)?, visiting)?;
                if !matches!(ty, ValueType::Array(_)) {
                    return Err(GraphError::ExpectedArray);
                }
                for array in &arrays[1..] {
                    if self.expression_type(array, visiting)? != ty {
                        return Err(GraphError::ExpressionType);
                    }
                }
                Ok(ty)
            }
            Expression::CountIf(condition) => {
                if condition.aggregate()
                    || self.expression_type(condition, visiting)?
                        != ValueType::Scalar(SqlType::Bool)
                {
                    return Err(GraphError::AggregatePlacement);
                }
                Ok(ValueType::Scalar(SqlType::Int64))
            }
            Expression::Sum { value, condition } => {
                if value.aggregate() {
                    return Err(GraphError::AggregatePlacement);
                }
                let ty = self.expression_type(value, visiting)?;
                if !matches!(
                    ty,
                    ValueType::Scalar(SqlType::Int64 | SqlType::UInt32 | SqlType::Float64)
                ) {
                    return Err(GraphError::ExpressionType);
                }
                if let Some(condition) = condition
                    && (condition.aggregate()
                        || self.expression_type(condition, visiting)?
                            != ValueType::Scalar(SqlType::Bool))
                {
                    return Err(GraphError::AggregatePlacement);
                }
                Ok(ty)
            }
            Expression::Column(column) => {
                let ty = self.column_type(*column, visiting)?;
                if let Body::Select { operation, .. } = &self.block(column.relation.block)?.body
                    && operation.expands(*column)
                {
                    let ValueType::Array(element) = ty else {
                        return Err(GraphError::ExpectedArray);
                    };
                    return Ok(*element);
                }
                Ok(ty)
            }
            Expression::In(left, right) => {
                let element = self.expression_type(left, visiting)?;
                let ValueType::Array(expected) = self.expression_type(right, visiting)? else {
                    return Err(GraphError::ExpressionType);
                };
                if element != *expected {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
            Expression::HasAny(left, right) => {
                let left = self.expression_type(left, visiting)?;
                if !matches!(left, ValueType::Array(_))
                    || left != self.expression_type(right, visiting)?
                {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
            Expression::Equal(left, right)
            | Expression::Greater(left, right)
            | Expression::GreaterEqual(left, right)
            | Expression::LessEqual(left, right)
            | Expression::And(left, right)
            | Expression::Or(left, right)
            | Expression::StartsWith(left, right) => {
                let left = self.expression_type(left, visiting)?;
                let right = self.expression_type(right, visiting)?;
                if left != right
                    || matches!(expression, Expression::And(..) | Expression::Or(..))
                        && left != ValueType::Scalar(SqlType::Bool)
                    || matches!(expression, Expression::StartsWith(..))
                        && left != ValueType::Scalar(SqlType::String)
                {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
        }
    }

    pub(super) fn column_type(
        &self,
        column: ColumnRef<'catalog>,
        visiting: &mut HashSet<OutputId>,
    ) -> Result<ValueType> {
        match column.port {
            Port::Stored(stored) => {
                self.stored_port(column.relation, stored)?;
                let ty = stored.data_type().ok_or_else(|| {
                    GraphError::UnknownStored(format!(
                        "{}.{}",
                        stored.table().name(),
                        stored.name()
                    ))
                })?;
                let value = ValueType::Scalar(match ty {
                    ontology::DataType::Int => SqlType::Int64,
                    ontology::DataType::Bool => SqlType::Bool,
                    ontology::DataType::Float => SqlType::Float64,
                    ontology::DataType::Date => SqlType::Date,
                    ontology::DataType::DateTime => SqlType::Timestamp {
                        precision: 6,
                        timezone: None,
                    },
                    _ => SqlType::String,
                });
                Ok(if stored.is_array() {
                    ValueType::Array(Box::new(value))
                } else {
                    value
                })
            }
            Port::Output(output) => self.output_type(output, visiting),
        }
    }

    fn output_type(&self, output: OutputId, visiting: &mut HashSet<OutputId>) -> Result<ValueType> {
        if !visiting.insert(output) {
            return Err(GraphError::RecursiveType);
        }
        let result = match &self.block(output.block)?.body {
            Body::Select { outputs, .. } => {
                self.expression_type(&outputs[output.slot].value, visiting)
            }
            Body::UnionAll { arms, .. } => {
                let first = self.output_type(
                    OutputId {
                        block: arms[0],
                        slot: output.slot,
                    },
                    visiting,
                )?;
                for arm in &arms[1..] {
                    if self.output_type(
                        OutputId {
                            block: *arm,
                            slot: output.slot,
                        },
                        visiting,
                    )? != first
                    {
                        return Err(GraphError::UnionType);
                    }
                }
                Ok(first)
            }
        };
        visiting.remove(&output);
        result
    }

    fn check_union_types(&self) -> Result<()> {
        for (slot, block) in self.blocks.iter().enumerate() {
            if let Body::UnionAll { labels, .. } = &block.body {
                for output in 0..labels.len() {
                    self.output_type(
                        OutputId {
                            block: BlockId {
                                owner: self.owner,
                                slot,
                            },
                            slot: output,
                        },
                        &mut HashSet::new(),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn validate_lowered(&self, root: BlockId) -> Result<()> {
        self.validate(root, |block, outputs, operation| {
            if outputs.is_empty() {
                return Err(GraphError::EmptyProjection);
            }
            for output in outputs {
                output
                    .value
                    .columns(&mut |column| self.check_column(block, column))?;
                self.expression_type(&output.value, &mut HashSet::new())?;
            }
            let mut used = HashSet::new();
            let available = self.operation_columns(block, operation, &mut used)?;
            let aggregate_input = operation
                .aggregate_input()
                .map(|input| self.operation_columns(block, input, &mut HashSet::new()))
                .transpose()?;
            let Body::Select { relations, .. } = &self.block(block)?.body else {
                unreachable!()
            };
            if used.len() != relations.len() {
                return Err(GraphError::JoinShape);
            }
            for output in outputs {
                if operation.groups().contains(&output.value) {
                    continue;
                }
                self.check_projection(&output.value, &available, aggregate_input.as_deref())?;
            }
            Ok(())
        })?;
        self.check_union_types()
    }

    fn check_projection(
        &self,
        expression: &Expression<'catalog>,
        available: &[ColumnRef<'catalog>],
        aggregate_input: Option<&[ColumnRef<'catalog>]>,
    ) -> Result<()> {
        match expression {
            Expression::Predicate {
                value, argument, ..
            } => {
                self.check_projection(value, available, aggregate_input)?;
                if let Some(argument) = argument {
                    self.check_projection(argument, available, aggregate_input)?;
                }
                Ok(())
            }
            Expression::Excerpt { value, .. }
            | Expression::Bucket { value, .. }
            | Expression::ToString(value) => {
                self.check_projection(value, available, aggregate_input)
            }
            Expression::Count
            | Expression::CountIf(_)
            | Expression::Sum { .. }
            | Expression::LatestPath { .. } => {
                let input = aggregate_input.ok_or(GraphError::AggregatePlacement)?;
                expression.columns(&mut |column| {
                    if input.contains(&column) {
                        Ok(())
                    } else {
                        Err(GraphError::OperationVisibility)
                    }
                })
            }
            Expression::Equal(left, right)
            | Expression::And(left, right)
            | Expression::Or(left, right)
            | Expression::In(left, right)
            | Expression::HasAny(left, right)
            | Expression::Greater(left, right)
            | Expression::GreaterEqual(left, right)
            | Expression::LessEqual(left, right)
            | Expression::StartsWith(left, right) => {
                self.check_projection(left, available, aggregate_input)?;
                self.check_projection(right, available, aggregate_input)
            }
            Expression::Tuple(values) | Expression::Array(values) | Expression::Concat(values) => {
                for value in values {
                    self.check_projection(value, available, aggregate_input)?;
                }
                Ok(())
            }
            Expression::Field { tuple, .. } => {
                self.check_projection(tuple, available, aggregate_input)
            }
            Expression::Keep { condition, value } => {
                self.check_projection(condition, available, aggregate_input)?;
                self.check_projection(value, available, aggregate_input)
            }
            _ => expression.columns(&mut |column| {
                if available.contains(&column) {
                    Ok(())
                } else {
                    Err(GraphError::OperationVisibility)
                }
            }),
        }
    }
}
