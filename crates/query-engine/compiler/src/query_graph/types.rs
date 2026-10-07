use super::*;

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub fn column_type(&self, column: ColumnRef<'a>) -> Result<ValueType> {
        match column.port {
            Port::Output(output) => {
                self.output_column(column.relation, output)?;
                Ok(self.output(output)?.data_type.clone())
            }
            Port::Stored(stored) => {
                self.stored_port(column.relation, stored)?;
                let scalar = match stored.data_type().ok_or(GraphError::ExpressionType)? {
                    ontology::DataType::Int => SqlType::Int64,
                    ontology::DataType::Float => SqlType::Float64,
                    ontology::DataType::Bool => SqlType::Bool,
                    ontology::DataType::Date => SqlType::Date,
                    ontology::DataType::DateTime => SqlType::Timestamp {
                        precision: 6,
                        timezone: None,
                    },
                    _ => SqlType::String,
                };
                let value = ValueType::Scalar(scalar);
                Ok(if stored.is_array() {
                    ValueType::Array(Box::new(value))
                } else {
                    value
                })
            }
        }
    }

    pub(super) fn expression_type_in(
        &self,
        expression: &Expression<'a>,
        expanded: &[ColumnRef<'a>],
    ) -> Result<ValueType> {
        use Expression::*;
        let infer = |value: &Expression<'a>| self.expression_type_in(value, expanded);
        let boolean = ValueType::Scalar(SqlType::Bool);
        let string = ValueType::Scalar(SqlType::String);
        let integer = ValueType::Scalar(SqlType::Int64);
        Ok(match expression {
            Column(column) => {
                let ty = self.column_type(*column)?;
                if expanded.contains(column) {
                    let ValueType::Array(element) = ty else {
                        return Err(GraphError::ExpectedArray);
                    };
                    *element
                } else {
                    ty
                }
            }
            Integer(_) | Count => integer,
            Boolean(_) => boolean,
            Text(_) => string,
            Strings(_) => ValueType::Array(Box::new(string)),
            Integers(_) => ValueType::Array(Box::new(integer)),
            Literal { data_type, .. } | Parameter { data_type, .. } => match data_type {
                SqlType::Array(element) => {
                    ValueType::Array(Box::new(ValueType::Scalar((*element).into())))
                }
                ty => ValueType::Scalar(*ty),
            },
            EmptyArray(element) => ValueType::Array(Box::new(element.clone())),
            ScalarQuery(column) => {
                let Source::Derived(body) = self.relation(column.relation)?.source else {
                    return Err(GraphError::ExpressionType);
                };
                let operation = self.operation(body)?;
                if operation.aggregate_input().is_none() || !operation.groups().is_empty() {
                    return Err(GraphError::AggregatePlacement);
                }
                self.column_type(*column)?
            }
            InQuery { value, key } => {
                if !matches!(self.relation(key.relation)?.source, Source::Definition(_))
                    || infer(value)? != self.column_type(*key)?
                {
                    return Err(GraphError::ExpressionType);
                }
                boolean
            }
            PathDepth(value) | Excerpt { value, .. } => {
                if infer(value)? != string {
                    return Err(GraphError::ExpressionType);
                }
                if matches!(expression, PathDepth(_)) {
                    integer
                } else {
                    string
                }
            }
            ToString(value) => {
                infer(value)?;
                string
            }
            Bucket { unit, value } => {
                if !matches!(
                    infer(value)?,
                    ValueType::Scalar(SqlType::Date | SqlType::Timestamp { .. })
                ) {
                    return Err(GraphError::ExpressionType);
                }
                ValueType::Scalar(unit.result_type())
            }
            LatestPath {
                path,
                version,
                deletion,
            } => {
                if self.column_type(*path)? != string || self.column_type(*deletion)? != boolean {
                    return Err(GraphError::ExpressionType);
                }
                self.column_type(*version)?;
                string
            }
            CountIf(condition) => {
                if condition.aggregate() || infer(condition)? != boolean {
                    return Err(GraphError::AggregatePlacement);
                }
                integer
            }
            Sum { value, condition } => {
                let ty = infer(value)?;
                if value.aggregate() || !numeric(&ty) {
                    return Err(GraphError::ExpressionType);
                }
                if let Some(condition) = condition
                    && (condition.aggregate() || infer(condition)? != boolean)
                {
                    return Err(GraphError::AggregatePlacement);
                }
                ty
            }
            Aggregate { function, value } => {
                use crate::input::AggFunction;
                if value.aggregate() {
                    return Err(GraphError::AggregatePlacement);
                }
                let ty = infer(value)?;
                if matches!(function, AggFunction::Avg | AggFunction::Sum) && !numeric(&ty) {
                    return Err(GraphError::ExpressionType);
                }
                match function {
                    AggFunction::Count => integer,
                    AggFunction::Avg => ValueType::Scalar(SqlType::Float64),
                    AggFunction::Collect => ValueType::Array(Box::new(ty)),
                    _ => ty,
                }
            }
            JsonObject(fields) => {
                for (_, value) in fields {
                    if infer(value)? != string {
                        return Err(GraphError::ExpressionType);
                    }
                }
                string
            }
            Tuple(values) => ValueType::Tuple(values.iter().map(infer).collect::<Result<_>>()?),
            Array(values) | Concat(values) => {
                let ty = infer(values.first().ok_or(GraphError::ExpectedArray)?)?;
                for value in &values[1..] {
                    if infer(value)? != ty {
                        return Err(GraphError::ExpressionType);
                    }
                }
                if matches!(expression, Concat(_)) {
                    if !matches!(ty, ValueType::Array(_)) {
                        return Err(GraphError::ExpectedArray);
                    }
                    ty
                } else {
                    ValueType::Array(Box::new(ty))
                }
            }
            Reverse(value) => {
                let ty = infer(value)?;
                if !matches!(ty, ValueType::Array(_)) {
                    return Err(GraphError::ExpectedArray);
                }
                ty
            }
            Field { tuple, index } => {
                let ValueType::Tuple(fields) = infer(tuple)? else {
                    return Err(GraphError::ExpressionType);
                };
                fields
                    .get(*index)
                    .cloned()
                    .ok_or(GraphError::ExpressionType)?
            }
            Keep { condition, value } => {
                if infer(condition)? != boolean {
                    return Err(GraphError::ExpressionType);
                }
                ValueType::Array(Box::new(infer(value)?))
            }
            Prefixes { value, paths, .. } => {
                if infer(value)? != string || infer(paths)? != ValueType::Array(Box::new(string)) {
                    return Err(GraphError::ExpressionType);
                }
                boolean
            }
            Predicate {
                operator,
                value,
                argument,
                ..
            } => {
                use crate::input::FilterOp;
                let ty = infer(value)?;
                if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
                    if argument.is_some() {
                        return Err(GraphError::ExpressionType);
                    }
                } else {
                    let right = infer(argument.as_ref().ok_or(GraphError::ExpressionType)?)?;
                    let valid = match operator {
                        FilterOp::In => right == ValueType::Array(Box::new(ty)),
                        FilterOp::Eq
                        | FilterOp::Ne
                        | FilterOp::Gt
                        | FilterOp::Lt
                        | FilterOp::Gte
                        | FilterOp::Lte => ty == right,
                        _ => ty == string && right == string,
                    };
                    if !valid {
                        return Err(GraphError::ExpressionType);
                    }
                }
                boolean
            }
            Equal(left, right)
            | Greater(left, right)
            | GreaterEqual(left, right)
            | LessEqual(left, right)
            | And(left, right)
            | Or(left, right)
            | StartsWith(left, right)
            | Add(left, right)
            | In(left, right)
            | HasAny(left, right) => {
                let left = infer(left)?;
                let right = infer(right)?;
                let valid = match expression {
                    In(..) => right == ValueType::Array(Box::new(left.clone())),
                    HasAny(..) => matches!(left, ValueType::Array(_)) && left == right,
                    And(..) | Or(..) => left == boolean && right == boolean,
                    StartsWith(..) => left == string && right == string,
                    Add(..) => numeric(&left) && left == right,
                    _ => left == right,
                };
                if !valid {
                    return Err(GraphError::ExpressionType);
                }
                if matches!(expression, Add(..)) {
                    left
                } else {
                    boolean
                }
            }
        })
    }
}

fn numeric(ty: &ValueType) -> bool {
    matches!(
        ty,
        ValueType::Scalar(SqlType::Int64 | SqlType::UInt32 | SqlType::Float64)
    )
}
