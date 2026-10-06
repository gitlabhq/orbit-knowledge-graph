use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression<'catalog> {
    Column(ColumnRef<'catalog>),
    Integer(i64),
    Boolean(bool),
    Text(String),
    Count,
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
        let literal = match self {
            Self::Predicate {
                value, argument, ..
            } => {
                value.bind_parameters(bindings);
                if let Some(argument) = argument {
                    argument.bind_parameters(bindings);
                }
                None
            }
            Self::Integer(value) => Some((SqlType::Int64, serde_json::json!(*value))),
            Self::Boolean(value) => Some((SqlType::Bool, serde_json::json!(*value))),
            Self::Text(value) => Some((SqlType::String, serde_json::json!(value))),
            Self::Integers(values) => Some((SqlType::Int64.to_array(), serde_json::json!(values))),
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::In(left, right)
            | Self::HasAny(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::LessEqual(left, right)
            | Self::StartsWith(left, right) => {
                left.bind_parameters(bindings);
                right.bind_parameters(bindings);
                None
            }
            Self::Tuple(values) | Self::Array(values) | Self::Concat(values) => {
                for value in values {
                    value.bind_parameters(bindings);
                }
                None
            }
            Self::Excerpt { value, .. }
            | Self::Bucket { value, .. }
            | Self::ToString(value)
            | Self::CountIf(value)
            | Self::Field { tuple: value, .. } => {
                value.bind_parameters(bindings);
                None
            }
            Self::Keep { condition, value } => {
                condition.bind_parameters(bindings);
                value.bind_parameters(bindings);
                None
            }
            Self::Sum { value, condition } => {
                value.bind_parameters(bindings);
                if let Some(condition) = condition {
                    condition.bind_parameters(bindings);
                }
                None
            }
            Self::Column(_) | Self::Count | Self::LatestPath { .. } | Self::Parameter { .. } => {
                None
            }
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

    pub(super) fn rebind(
        &self,
        map: &impl Fn(ColumnRef<'catalog>) -> Result<ColumnRef<'catalog>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Predicate {
                operator,
                value,
                argument,
                fold_case,
            } => Self::Predicate {
                operator: *operator,
                value: Box::new(value.rebind(map)?),
                argument: argument
                    .as_ref()
                    .map(|argument| argument.rebind(map).map(Box::new))
                    .transpose()?,
                fold_case: *fold_case,
            },
            Self::Array(values) => Self::Array(
                values
                    .iter()
                    .map(|value| value.rebind(map))
                    .collect::<Result<_>>()?,
            ),
            Self::Column(column) => Self::Column(map(*column)?),
            Self::Equal(left, right) => Self::equal(left.rebind(map)?, right.rebind(map)?),
            Self::In(left, right) => {
                Self::In(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::HasAny(left, right) => {
                Self::HasAny(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::And(left, right) => {
                Self::And(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::Greater(left, right) => {
                Self::Greater(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::GreaterEqual(left, right) => {
                Self::GreaterEqual(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::LessEqual(left, right) => {
                Self::LessEqual(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::Bucket { unit, value } => Self::Bucket {
                unit: *unit,
                value: Box::new(value.rebind(map)?),
            },
            Self::StartsWith(left, right) => {
                Self::StartsWith(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::Integer(_) | Self::Boolean(_) | Self::Text(_) | Self::Integers(_) => self.clone(),
            _ => {
                return Err(GraphError::UnsupportedInput(
                    "non-scalar candidate predicate".into(),
                ));
            }
        })
    }

    pub(super) fn columns(
        &self,
        visit: &mut impl FnMut(ColumnRef<'catalog>) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Predicate {
                value, argument, ..
            } => {
                value.columns(visit)?;
                if let Some(argument) = argument {
                    argument.columns(visit)?;
                }
                Ok(())
            }
            Self::Column(column) => visit(*column),
            Self::LatestPath {
                path,
                version,
                deletion,
            } => {
                visit(*path)?;
                visit(*version)?;
                visit(*deletion)
            }
            Self::Excerpt { value, .. } | Self::Bucket { value, .. } | Self::ToString(value) => {
                value.columns(visit)
            }
            Self::CountIf(condition) => condition.columns(visit),
            Self::Sum { value, condition } => {
                value.columns(visit)?;
                if let Some(condition) = condition {
                    condition.columns(visit)?;
                }
                Ok(())
            }
            Self::Tuple(values) | Self::Array(values) | Self::Concat(values) => {
                for value in values {
                    value.columns(visit)?;
                }
                Ok(())
            }
            Self::Field { tuple, .. } => tuple.columns(visit),
            Self::Keep { condition, value } => {
                condition.columns(visit)?;
                value.columns(visit)
            }
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::In(left, right)
            | Self::HasAny(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::LessEqual(left, right)
            | Self::StartsWith(left, right) => {
                left.columns(visit)?;
                right.columns(visit)
            }
            _ => Ok(()),
        }
    }

    pub(super) fn aggregate(&self) -> bool {
        match self {
            Self::Predicate {
                value, argument, ..
            } => {
                value.aggregate()
                    || argument
                        .as_ref()
                        .is_some_and(|argument| argument.aggregate())
            }
            Self::Count | Self::CountIf(_) | Self::Sum { .. } | Self::LatestPath { .. } => true,
            Self::Tuple(values) | Self::Array(values) | Self::Concat(values) => {
                values.iter().any(Self::aggregate)
            }
            Self::Field { tuple, .. } => tuple.aggregate(),
            Self::Excerpt { value, .. } | Self::Bucket { value, .. } | Self::ToString(value) => {
                value.aggregate()
            }
            Self::Keep { condition, value } => condition.aggregate() || value.aggregate(),
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::In(left, right)
            | Self::HasAny(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::LessEqual(left, right)
            | Self::StartsWith(left, right) => left.aggregate() || right.aggregate(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueType {
    Scalar(SqlType),
    Tuple(Vec<Self>),
    Array(Box<Self>),
}
