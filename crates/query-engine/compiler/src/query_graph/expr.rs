use super::*;
use orbit_utils::query_types::{ParamValue, SqlType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType {
    Scalar(SqlType),
    Array(Box<ValueType>),
    Tuple(Vec<ValueType>),
}

#[derive(Debug)]
pub(super) struct ColumnData {
    pub scope: QueryId,
    pub source: u64,
    pub slot: usize,
    pub name: String,
    pub data_type: ValueType,
}

#[derive(Clone, Debug)]
pub struct Column(pub(super) Arc<ColumnData>);

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.0.scope == other.0.scope
            && self.0.source == other.0.source
            && self.0.slot == other.0.slot
    }
}
impl Eq for Column {}

#[derive(Clone, Debug, PartialEq)]
pub struct Expr(pub(super) Box<ExprKind>);

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Column(Column),
    Literal(ParamValue),
    Binary {
        operator: Operator,
        left: Expr,
        right: Expr,
    },
    Call {
        function: Function,
        arguments: Vec<Expr>,
    },
    Aggregate {
        function: Aggregate,
        arguments: Vec<Expr>,
        filter: Option<Expr>,
    },
    Scalar {
        query: QueryId,
        column: Column,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    And,
    Or,
    Add,
    In,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Function {
    StartsWith,
    StartsWithAny,
    EndsWith,
    Contains,
    HasAny,
    IsNull,
    IsNotNull,
    Lower,
    ToString,
    Tuple,
    Array,
    EmptyArray(ValueType),
    ArrayConcat,
    ArrayReverse,
    TupleField(usize),
    SingletonIf,
    TimeBucket(crate::input::TruncateUnit),
    Excerpt(u32),
    PathDepth,
    JsonObject(Vec<String>),
    Coalesce,
    If,
    TokenMatch,
    AllTokens,
    AnyTokens,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Aggregate {
    Count,
    Sum,
    Average,
    Min,
    Max,
    Collect,
    ArgMax,
}

#[derive(Clone, Debug)]
pub struct Named {
    pub name: String,
    pub value: Expr,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub value: Expr,
    pub descending: bool,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.0.name
    }
    pub fn data_type(&self) -> &ValueType {
        &self.0.data_type
    }
    pub fn expr(&self) -> Expr {
        Expr(Box::new(ExprKind::Column(self.clone())))
    }
    pub fn named(&self, name: impl Into<String>) -> Named {
        self.expr().named(name)
    }
    pub fn eq(&self, value: impl Into<Expr>) -> Expr {
        self.expr().eq(value)
    }
    pub fn ne(&self, value: impl Into<Expr>) -> Expr {
        self.expr().ne(value)
    }
    pub fn gt(&self, value: impl Into<Expr>) -> Expr {
        self.expr().gt(value)
    }
    pub fn ge(&self, value: impl Into<Expr>) -> Expr {
        self.expr().ge(value)
    }
    pub fn lt(&self, value: impl Into<Expr>) -> Expr {
        self.expr().lt(value)
    }
    pub fn le(&self, value: impl Into<Expr>) -> Expr {
        self.expr().le(value)
    }
    pub fn add(&self, value: impl Into<Expr>) -> Expr {
        self.expr().add(value)
    }
    pub fn starts_with(&self, value: impl Into<Expr>) -> Expr {
        self.expr().starts_with(value)
    }
    pub fn in_values<T: Into<Expr>>(&self, values: impl IntoIterator<Item = T>) -> Expr {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.is_empty() {
            return lit(false);
        }
        self.expr().binary(Operator::In, array(values))
    }
    pub fn has_any<T: Into<Expr>>(&self, values: impl IntoIterator<Item = T>) -> Expr {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.is_empty() {
            return lit(false);
        }
        Expr::call(Function::HasAny, [self.expr(), array(values)])
    }
    pub fn field(&self, index: usize) -> Expr {
        self.expr().field(index)
    }
    pub fn asc(&self) -> Order {
        self.expr().asc()
    }
    pub fn desc(&self) -> Order {
        self.expr().desc()
    }
}

impl From<Column> for Expr {
    fn from(value: Column) -> Self {
        value.expr()
    }
}
impl From<&Column> for Expr {
    fn from(value: &Column) -> Self {
        value.expr()
    }
}
impl From<i64> for Expr {
    fn from(value: i64) -> Self {
        Self::literal(SqlType::Int64, value.into())
    }
}
impl From<i32> for Expr {
    fn from(value: i32) -> Self {
        Self::from(i64::from(value))
    }
}
impl From<bool> for Expr {
    fn from(value: bool) -> Self {
        Self::literal(SqlType::Bool, value.into())
    }
}
impl From<&str> for Expr {
    fn from(value: &str) -> Self {
        Self::literal(SqlType::String, value.into())
    }
}
impl From<String> for Expr {
    fn from(value: String) -> Self {
        Self::literal(SqlType::String, value.into())
    }
}

pub fn lit(value: impl Into<Expr>) -> Expr {
    value.into()
}
pub fn count() -> Expr {
    Expr::aggregate(Aggregate::Count, [])
}
pub fn tuple(values: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::call(Function::Tuple, values)
}
pub fn array<T: Into<Expr>>(values: impl IntoIterator<Item = T>) -> Expr {
    Expr::call(Function::Array, values.into_iter().map(Into::into))
}
pub fn array_concat(values: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::call(Function::ArrayConcat, values)
}
pub fn singleton_if(condition: Expr, value: Expr) -> Expr {
    Expr::call(Function::SingletonIf, [condition, value])
}

impl Expr {
    pub fn kind(&self) -> &ExprKind {
        &self.0
    }
    pub fn literal(data_type: SqlType, value: serde_json::Value) -> Self {
        Self(Box::new(ExprKind::Literal(ParamValue { data_type, value })))
    }
    pub fn call(function: Function, arguments: impl IntoIterator<Item = Expr>) -> Self {
        Self(Box::new(ExprKind::Call {
            function,
            arguments: arguments.into_iter().collect(),
        }))
    }
    pub fn aggregate(function: Aggregate, arguments: impl IntoIterator<Item = Expr>) -> Self {
        Self(Box::new(ExprKind::Aggregate {
            function,
            arguments: arguments.into_iter().collect(),
            filter: None,
        }))
    }
    pub fn named(self, name: impl Into<String>) -> Named {
        Named {
            name: name.into(),
            value: self,
        }
    }
    pub fn binary(self, operator: Operator, right: impl Into<Expr>) -> Self {
        Self(Box::new(ExprKind::Binary {
            operator,
            left: self,
            right: right.into(),
        }))
    }
    pub fn eq(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::Equal, right)
    }
    pub fn ne(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::NotEqual, right)
    }
    pub fn gt(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::Greater, right)
    }
    pub fn ge(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::GreaterEqual, right)
    }
    pub fn lt(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::Less, right)
    }
    pub fn le(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::LessEqual, right)
    }
    pub fn add(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::Add, right)
    }
    pub fn and(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::And, right)
    }
    pub fn or(self, right: impl Into<Expr>) -> Self {
        self.binary(Operator::Or, right)
    }
    pub fn starts_with(self, right: impl Into<Expr>) -> Self {
        Self::call(Function::StartsWith, [self, right.into()])
    }
    pub fn field(self, index: usize) -> Self {
        Self::call(Function::TupleField(index), [self])
    }
    pub fn asc(self) -> Order {
        Order {
            value: self,
            descending: false,
        }
    }
    pub fn desc(self) -> Order {
        Order {
            value: self,
            descending: true,
        }
    }
    pub fn filter(mut self, predicate: Expr) -> Self {
        if let ExprKind::Aggregate { filter, .. } = self.0.as_mut() {
            *filter = Some(match filter.take() {
                Some(previous) => previous.and(predicate),
                None => predicate,
            });
            self
        } else {
            Self::call(Function::If, [predicate, self])
        }
    }

    pub fn walk<E>(
        &self,
        visit: &mut impl FnMut(&Expr) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self.kind() {
            ExprKind::Binary { left, right, .. } => {
                left.walk(visit)?;
                right.walk(visit)?;
            }
            ExprKind::Call { arguments, .. } => {
                for value in arguments {
                    value.walk(visit)?;
                }
            }
            ExprKind::Aggregate {
                arguments, filter, ..
            } => {
                for value in arguments {
                    value.walk(visit)?;
                }
                if let Some(filter) = filter {
                    filter.walk(visit)?;
                }
            }
            _ => {}
        }
        visit(self)
    }

    pub fn rewrite(self, edit: &mut impl FnMut(Expr) -> Result<Expr>) -> Result<Expr> {
        let kind = match *self.0 {
            ExprKind::Binary {
                operator,
                left,
                right,
            } => ExprKind::Binary {
                operator,
                left: left.rewrite(edit)?,
                right: right.rewrite(edit)?,
            },
            ExprKind::Call {
                function,
                arguments,
            } => ExprKind::Call {
                function,
                arguments: arguments
                    .into_iter()
                    .map(|value| value.rewrite(edit))
                    .collect::<Result<_>>()?,
            },
            ExprKind::Aggregate {
                function,
                arguments,
                filter,
            } => ExprKind::Aggregate {
                function,
                arguments: arguments
                    .into_iter()
                    .map(|value| value.rewrite(edit))
                    .collect::<Result<_>>()?,
                filter: filter.map(|value| value.rewrite(edit)).transpose()?,
            },
            kind => kind,
        };
        edit(Expr(Box::new(kind)))
    }

    pub(super) fn infer(
        &self,
        scope: &QueryScope<'_, '_, impl QueryDataModel + ?Sized>,
        columns: &[Column],
        measure: bool,
    ) -> Result<ValueType> {
        let boolean = ValueType::Scalar(SqlType::Bool);
        let string = ValueType::Scalar(SqlType::String);
        let integer = ValueType::Scalar(SqlType::Int64);
        let infer = |value: &Expr| value.infer(scope, columns, measure);
        match self.kind() {
            ExprKind::Column(column) => {
                if column.0.scope != scope.id {
                    return Err(Error::Scope);
                }
                if !columns.contains(column) {
                    return Err(Error::Column);
                }
                Ok(column.data_type().clone())
            }
            ExprKind::Literal(value) => {
                if !literal_matches(value.data_type, &value.value) {
                    return Err(Error::Type);
                }
                Ok(match value.data_type {
                    SqlType::Array(element) => {
                        ValueType::Array(Box::new(ValueType::Scalar(element.into())))
                    }
                    scalar => ValueType::Scalar(scalar),
                })
            }
            ExprKind::Scalar { query, column } => {
                if scope.graph.get(*query)?.parent != Some(scope.id) {
                    return Err(Error::Scope);
                }
                let rows = scope.graph.rows(*query)?;
                if !rows.columns.contains(column) || !rows.scalar() {
                    return Err(Error::Column);
                }
                Ok(column.data_type().clone())
            }
            ExprKind::Binary {
                operator,
                left,
                right,
            } => {
                let left = infer(left)?;
                let right = infer(right)?;
                let valid = match operator {
                    Operator::In => right == ValueType::Array(Box::new(left.clone())),
                    Operator::And | Operator::Or => left == boolean && right == boolean,
                    Operator::Add => numeric(&left) && left == right,
                    _ => left == right,
                };
                if !valid {
                    return Err(Error::Type);
                }
                Ok(if *operator == Operator::Add {
                    left
                } else {
                    boolean
                })
            }
            ExprKind::Aggregate {
                function,
                arguments,
                filter,
            } => {
                if !measure {
                    return Err(Error::Aggregate);
                }
                let values = arguments
                    .iter()
                    .map(|value| value.infer(scope, columns, false))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(filter) = filter
                    && filter.infer(scope, columns, false)? != boolean
                {
                    return Err(Error::Type);
                }
                match (function, values.as_slice()) {
                    (Aggregate::Count, [] | [_]) => Ok(integer),
                    (Aggregate::Average, [value]) if numeric(value) => {
                        Ok(ValueType::Scalar(SqlType::Float64))
                    }
                    (Aggregate::Sum, [value]) if numeric(value) => Ok(value.clone()),
                    (Aggregate::Min | Aggregate::Max, [value]) => Ok(value.clone()),
                    (Aggregate::Collect, [value]) => Ok(ValueType::Array(Box::new(value.clone()))),
                    (Aggregate::ArgMax, [value, _]) => Ok(value.clone()),
                    _ => Err(Error::Type),
                }
            }
            ExprKind::Call {
                function,
                arguments,
            } => {
                let values = arguments.iter().map(infer).collect::<Result<Vec<_>>>()?;
                match (function, values.as_slice()) {
                    (Function::Tuple, _) => Ok(ValueType::Tuple(values)),
                    (Function::Array, [first, rest @ ..])
                        if rest.iter().all(|value| value == first) =>
                    {
                        Ok(ValueType::Array(Box::new(first.clone())))
                    }
                    (Function::EmptyArray(element), []) => {
                        Ok(ValueType::Array(Box::new(element.clone())))
                    }
                    (Function::ArrayConcat, [first @ ValueType::Array(_), rest @ ..])
                        if rest.iter().all(|value| value == first) =>
                    {
                        Ok(first.clone())
                    }
                    (Function::ArrayReverse, [value @ ValueType::Array(_)]) => Ok(value.clone()),
                    (Function::TupleField(index), [ValueType::Tuple(fields)]) => {
                        fields.get(*index).cloned().ok_or(Error::Type)
                    }
                    (Function::SingletonIf, [condition, value]) if *condition == boolean => {
                        Ok(ValueType::Array(Box::new(value.clone())))
                    }
                    (Function::HasAny, [left @ ValueType::Array(_), right]) if left == right => {
                        Ok(boolean)
                    }
                    (Function::StartsWithAny, [value, ValueType::Array(prefix)])
                        if *value == string && **prefix == string =>
                    {
                        Ok(boolean)
                    }
                    (
                        Function::StartsWith
                        | Function::EndsWith
                        | Function::Contains
                        | Function::TokenMatch
                        | Function::AllTokens
                        | Function::AnyTokens,
                        [left, right],
                    ) if *left == string && *right == string => Ok(boolean),
                    (Function::IsNull | Function::IsNotNull, [_]) => Ok(boolean),
                    (Function::Lower | Function::Excerpt(_), [value]) if *value == string => {
                        Ok(string)
                    }
                    (Function::ToString, [_]) => Ok(string),
                    (Function::PathDepth, [value]) if *value == string => Ok(integer),
                    (
                        Function::TimeBucket(unit),
                        [ValueType::Scalar(SqlType::Date | SqlType::Timestamp { .. })],
                    ) => Ok(ValueType::Scalar(unit.result_type())),
                    (Function::JsonObject(names), _)
                        if names.len() == values.len()
                            && values.iter().all(|value| *value == string) =>
                    {
                        Ok(string)
                    }
                    (Function::If, [condition, left, right])
                        if *condition == boolean && left == right =>
                    {
                        Ok(left.clone())
                    }
                    (Function::Coalesce, [first, rest @ ..])
                        if rest.iter().all(|value| value == first) =>
                    {
                        Ok(first.clone())
                    }
                    _ => Err(Error::Type),
                }
            }
        }
    }
}

fn numeric(value: &ValueType) -> bool {
    matches!(
        value,
        ValueType::Scalar(SqlType::Int64 | SqlType::UInt32 | SqlType::Float64)
    )
}

fn literal_matches(data_type: SqlType, value: &serde_json::Value) -> bool {
    if value.is_null() {
        return !matches!(data_type, SqlType::Array(_));
    }
    match data_type {
        SqlType::String | SqlType::Date | SqlType::Timestamp { .. } => value.is_string(),
        SqlType::Int64 => value.as_i64().is_some(),
        SqlType::UInt32 => value
            .as_u64()
            .is_some_and(|value| u32::try_from(value).is_ok()),
        SqlType::Float64 => value.is_number(),
        SqlType::Bool => value.is_boolean(),
        SqlType::Array(element) => value.as_array().is_some_and(|values| {
            values
                .iter()
                .all(|value| literal_matches(element.into(), value))
        }),
    }
}
