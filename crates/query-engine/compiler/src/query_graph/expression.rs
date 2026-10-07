use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression<'catalog> {
    Column(ColumnRef<'catalog>),
    Integer(i64),
    Boolean(bool),
    Text(String),
    Literal {
        data_type: SqlType,
        value: serde_json::Value,
    },
    Strings(Vec<String>),
    JsonObject(Vec<(String, Self)>),
    Prefixes {
        value: Box<Self>,
        paths: Box<Self>,
        array: bool,
    },
    Count,
    InQuery {
        value: Box<Self>,
        key: ColumnRef<'catalog>,
    },
    ScalarQuery(ColumnRef<'catalog>),
    PathDepth(Box<Self>),
    Add(Box<Self>, Box<Self>),
    Reverse(Box<Self>),
    EmptyArray(ValueType),
    CountIf(Box<Self>),
    LatestPath {
        path: ColumnRef<'catalog>,
        version: ColumnRef<'catalog>,
        deletion: ColumnRef<'catalog>,
    },
    Sum {
        value: Box<Self>,
        condition: Option<Box<Self>>,
    },
    Aggregate {
        function: crate::input::AggFunction,
        value: Box<Self>,
    },
    Integers(Vec<i64>),
    Tuple(Vec<Self>),
    Array(Vec<Self>),
    Field {
        tuple: Box<Self>,
        index: usize,
    },
    Keep {
        condition: Box<Self>,
        value: Box<Self>,
    },
    Concat(Vec<Self>),
    Greater(Box<Self>, Box<Self>),
    GreaterEqual(Box<Self>, Box<Self>),
    LessEqual(Box<Self>, Box<Self>),
    Bucket {
        unit: crate::input::TruncateUnit,
        value: Box<Self>,
    },
    Predicate {
        operator: crate::input::FilterOp,
        value: Box<Self>,
        argument: Option<Box<Self>>,
        fold_case: bool,
    },
    StartsWith(Box<Self>, Box<Self>),
    Equal(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    In(Box<Self>, Box<Self>),
    HasAny(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Excerpt {
        value: Box<Self>,
        max_chars: u32,
    },
    ToString(Box<Self>),
    Parameter {
        name: String,
        data_type: SqlType,
    },
}

impl<'catalog> Expression<'catalog> {
    pub fn children(&self) -> impl Iterator<Item = &Self> {
        let mut pair = [None, None];
        let mut values: &[Self] = &[];
        let mut fields: &[(String, Self)] = &[];
        match self {
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::In(left, right)
            | Self::HasAny(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::LessEqual(left, right)
            | Self::StartsWith(left, right)
            | Self::Add(left, right)
            | Self::Prefixes {
                value: left,
                paths: right,
                ..
            }
            | Self::Keep {
                condition: left,
                value: right,
            } => pair = [Some(left.as_ref()), Some(right.as_ref())],
            Self::Predicate {
                value, argument, ..
            }
            | Self::Sum {
                value,
                condition: argument,
            } => pair = [Some(value.as_ref()), argument.as_deref()],
            Self::InQuery { value, .. }
            | Self::PathDepth(value)
            | Self::Reverse(value)
            | Self::CountIf(value)
            | Self::Aggregate { value, .. }
            | Self::Field { tuple: value, .. }
            | Self::Bucket { value, .. }
            | Self::Excerpt { value, .. }
            | Self::ToString(value) => pair[0] = Some(value.as_ref()),
            Self::Tuple(items) | Self::Array(items) | Self::Concat(items) => values = items,
            Self::JsonObject(items) => fields = items,
            Self::Column(_)
            | Self::Integer(_)
            | Self::Boolean(_)
            | Self::Text(_)
            | Self::Literal { .. }
            | Self::Strings(_)
            | Self::Count
            | Self::ScalarQuery(_)
            | Self::EmptyArray(_)
            | Self::LatestPath { .. }
            | Self::Integers(_)
            | Self::Parameter { .. } => {}
        }
        pair.into_iter()
            .flatten()
            .chain(values)
            .chain(fields.iter().map(|(_, value)| value))
    }

    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut Self> {
        let mut pair = [None, None];
        let mut values: &mut [Self] = &mut [];
        let mut fields: &mut [(String, Self)] = &mut [];
        match self {
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::In(left, right)
            | Self::HasAny(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::LessEqual(left, right)
            | Self::StartsWith(left, right)
            | Self::Add(left, right)
            | Self::Prefixes {
                value: left,
                paths: right,
                ..
            }
            | Self::Keep {
                condition: left,
                value: right,
            } => pair = [Some(left.as_mut()), Some(right.as_mut())],
            Self::Predicate {
                value, argument, ..
            }
            | Self::Sum {
                value,
                condition: argument,
            } => pair = [Some(value.as_mut()), argument.as_deref_mut()],
            Self::InQuery { value, .. }
            | Self::PathDepth(value)
            | Self::Reverse(value)
            | Self::CountIf(value)
            | Self::Aggregate { value, .. }
            | Self::Field { tuple: value, .. }
            | Self::Bucket { value, .. }
            | Self::Excerpt { value, .. }
            | Self::ToString(value) => pair[0] = Some(value.as_mut()),
            Self::Tuple(items) | Self::Array(items) | Self::Concat(items) => values = items,
            Self::JsonObject(items) => fields = items,
            Self::Column(_)
            | Self::Integer(_)
            | Self::Boolean(_)
            | Self::Text(_)
            | Self::Literal { .. }
            | Self::Strings(_)
            | Self::Count
            | Self::ScalarQuery(_)
            | Self::EmptyArray(_)
            | Self::LatestPath { .. }
            | Self::Integers(_)
            | Self::Parameter { .. } => {}
        }
        pair.into_iter()
            .flatten()
            .chain(values)
            .chain(fields.iter_mut().map(|(_, value)| value))
    }

    pub fn walk<Error>(
        &self,
        visit: &mut impl FnMut(&Self) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for child in self.children() {
            child.walk(visit)?;
        }
        visit(self)
    }

    pub fn walk_mut<Error>(
        &mut self,
        visit: &mut impl FnMut(&mut Self) -> std::result::Result<(), Error>,
    ) -> std::result::Result<(), Error> {
        for child in self.children_mut() {
            child.walk_mut(visit)?;
        }
        visit(self)
    }

    pub(super) fn membership(column: ColumnRef<'catalog>, values: &[i64]) -> Self {
        if let [value] = values {
            Self::equal(Self::Column(column), Self::Integer(*value))
        } else {
            Self::In(
                Box::new(Self::Column(column)),
                Box::new(Self::Integers(values.to_vec())),
            )
        }
    }

    pub(super) fn identity_predicates(
        column: ColumnRef<'catalog>,
        node: &crate::input::InputNode,
    ) -> Vec<Self> {
        let mut predicates = Vec::new();
        if !node.node_ids.is_empty() {
            predicates.push(Self::membership(column, &node.node_ids));
        }
        if let Some(range) = &node.id_range {
            predicates.push(Self::GreaterEqual(
                Box::new(Self::Column(column)),
                Box::new(Self::Integer(range.start)),
            ));
            predicates.push(Self::LessEqual(
                Box::new(Self::Column(column)),
                Box::new(Self::Integer(range.end)),
            ));
        }
        predicates
    }

    pub(super) fn bind_parameters(
        &mut self,
        bindings: &mut orbit_utils::query_types::ParamBindings,
    ) {
        if let Self::Prefixes {
            paths,
            array: false,
            ..
        } = self
            && let Self::Strings(values) = paths.as_ref()
        {
            **paths = Self::Array(values.iter().cloned().map(Self::Text).collect());
        }
        for child in self.children_mut() {
            child.bind_parameters(bindings);
        }
        let literal = match self {
            Self::Literal { data_type, value } => Some((*data_type, value.clone())),
            Self::Strings(values) => Some((SqlType::String.to_array(), serde_json::json!(values))),
            Self::Integer(value) => Some((SqlType::Int64, serde_json::json!(*value))),
            Self::Boolean(value) => Some((SqlType::Bool, serde_json::json!(*value))),
            Self::Text(value) => Some((SqlType::String, serde_json::json!(value))),
            Self::Integers(values) => Some((SqlType::Int64.to_array(), serde_json::json!(values))),
            _ => None,
        };
        if let Some((data_type, value)) = literal {
            *self = Self::Parameter {
                name: bindings.intern(data_type, &value),
                data_type,
            };
        }
    }

    pub fn equal(left: Self, right: Self) -> Self {
        Self::Equal(Box::new(left), Box::new(right))
    }

    pub fn rebind(
        &self,
        map: &impl Fn(ColumnRef<'catalog>) -> Result<ColumnRef<'catalog>>,
    ) -> Result<Self> {
        let mut expression = self.clone();
        expression.walk_mut(&mut |expression| {
            match expression {
                Self::Column(column)
                | Self::ScalarQuery(column)
                | Self::InQuery { key: column, .. } => *column = map(*column)?,
                Self::LatestPath {
                    path,
                    version,
                    deletion,
                } => {
                    *path = map(*path)?;
                    *version = map(*version)?;
                    *deletion = map(*deletion)?;
                }
                _ => {}
            }
            Ok(())
        })?;
        Ok(expression)
    }

    pub(super) fn columns(
        &self,
        visit: &mut impl FnMut(ColumnRef<'catalog>) -> Result<()>,
    ) -> Result<()> {
        self.references(&mut |column, subquery| if subquery { Ok(()) } else { visit(column) })
    }

    pub(super) fn references(
        &self,
        visit: &mut impl FnMut(ColumnRef<'catalog>, bool) -> Result<()>,
    ) -> Result<()> {
        self.walk(&mut |expression| match expression {
            Self::Column(column) => visit(*column, false),
            Self::ScalarQuery(column) | Self::InQuery { key: column, .. } => visit(*column, true),
            Self::LatestPath {
                path,
                version,
                deletion,
            } => {
                visit(*path, false)?;
                visit(*version, false)?;
                visit(*deletion, false)
            }
            _ => Ok(()),
        })
    }

    pub(super) fn aggregate(&self) -> bool {
        matches!(
            self,
            Self::Count
                | Self::CountIf(_)
                | Self::Sum { .. }
                | Self::Aggregate { .. }
                | Self::LatestPath { .. }
        ) || self.children().any(Self::aggregate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueType {
    Scalar(SqlType),
    Tuple(Vec<Self>),
    Array(Box<Self>),
}
