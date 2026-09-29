use std::convert::Infallible;

use query_data_model::{EdgeField, QueryBackendCatalog, QueryDataModel};

use super::bind::Source;
use super::generic::{Expr, Function, Node, Op, Operation, Schema, ValueId, ValueType, Values};
use crate::error::{QueryError, Result};

#[derive(Clone, PartialEq)]
pub struct Read {
    pub table: String,
    pub columns: Vec<(ValueId, String)>,
    pub current_rows: CurrentRows,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CurrentRows {
    Snapshot,
    Final,
}

pub type PhysicalPlan = Node<Read, Scalar, Infallible>;

pub fn select_source(
    source: Source,
    model: &impl QueryDataModel,
    current_rows: CurrentRows,
    values: &mut Values,
) -> Result<PhysicalPlan> {
    let Source::Edge {
        relationships,
        fields,
        ..
    } = source
    else {
        return Ok(Node {
            op: Op::Read(Read::select(source, model, current_rows)?),
            inputs: vec![],
        });
    };

    let mut tables = model.query_backend().edge_tables(&relationships);
    tables.sort();
    tables.dedup();
    if tables.is_empty() {
        return Err(QueryError::ReferenceError(
            "edge source has no storage table".into(),
        ));
    }

    let outputs: Vec<_> = fields.iter().map(|(value, _)| *value).collect();
    let mut inputs = Vec::new();
    let mut arms = Vec::new();

    for table in tables {
        let mut columns = Vec::new();
        let mut arm = Vec::new();
        let mut kind_value = None;

        for (output, field) in &fields {
            let column = model
                .query_backend()
                .edge_field_column(&table, *field)
                .ok_or_else(|| {
                    QueryError::ReferenceError(format!(
                        "edge field {field:?} is unavailable in {table}"
                    ))
                })?;
            let value = values.allocate(values.data_type(*output)?.clone());
            columns.push((value, column.to_string()));
            arm.push(value);

            if *field == EdgeField::RelationshipKind {
                kind_value = Some(value);
            }
        }

        let mut root = Node {
            op: Op::Read(Read {
                table: table.clone(),
                columns,
                current_rows,
            }),
            inputs: vec![],
        };

        if !relationships.is_empty() {
            let kind = kind_value.ok_or_else(|| {
                QueryError::ReferenceError("edge source requires its relationship kind".into())
            })?;
            let predicate = relationships
                .iter()
                .filter(|id| model.query_backend().relationship_table(**id) == Some(table.as_str()))
                .map(|id| Expr::Call {
                    function: Scalar::Equal,
                    arguments: vec![
                        Expr::Value(kind),
                        Expr::String(model.graph().relationship(*id).name.clone()),
                    ],
                })
                .reduce(|left, right| Expr::Call {
                    function: Scalar::Or,
                    arguments: vec![left, right],
                })
                .unwrap_or(Expr::Bool(false));

            root = Node {
                op: Op::Filter(predicate),
                inputs: vec![root],
            };
        }

        arms.push(arm);
        inputs.push(root);
    }

    Ok(Node {
        op: Op::Union { outputs, arms },
        inputs,
    })
}

impl Read {
    pub fn explain(&self) -> super::explain::SExpression {
        use super::explain::{SExpression, value};

        SExpression::node(
            "Scan",
            [
                SExpression::atom(&self.table),
                SExpression::atom(match self.current_rows {
                    CurrentRows::Snapshot => "Snapshot",
                    CurrentRows::Final => "Final",
                }),
                SExpression::node(
                    "Columns",
                    self.columns.iter().map(|(id, column)| {
                        SExpression::node("Column", [value(*id), SExpression::atom(column)])
                    }),
                ),
            ],
        )
    }

    pub fn select(
        source: Source,
        model: &impl QueryDataModel,
        current_rows: CurrentRows,
    ) -> Result<Self> {
        let Source::Entity {
            entity, properties, ..
        } = source
        else {
            return Err(QueryError::ReferenceError(
                "edge sources require plan selection".into(),
            ));
        };

        let entity_id = entity;
        let entity = &model.graph().entity(entity_id).name;
        let table = model
            .entity_table(entity)
            .ok_or_else(|| QueryError::ReferenceError(format!("entity {entity} is unavailable")))?;
        let columns = properties
            .into_iter()
            .map(|(value, property)| {
                if model.graph().property(property).entity != entity_id {
                    return Err(QueryError::ReferenceError(
                        "read properties must belong to one entity".into(),
                    ));
                }

                let column = model.property_column(property).ok_or_else(|| {
                    QueryError::ReferenceError("selected property is not stored".into())
                })?;
                Ok((value, column.to_string()))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            table: table.to_string(),
            columns,
            current_rows,
        })
    }
}

impl Operation for Read {
    fn retain_outputs(&mut self, required: &Schema) -> bool {
        if !self
            .columns
            .iter()
            .any(|(value, _)| required.contains(value))
        {
            return false;
        }

        let before = self.columns.len();
        self.columns.retain(|(value, _)| required.contains(value));
        self.columns.len() != before
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        self.columns.iter_mut().for_each(|(value, _)| map(value));
    }

    fn output(&self, inputs: &[Schema], _: &Values) -> Result<Schema> {
        if !inputs.is_empty() {
            return Err(QueryError::PipelineInvariant(
                "read cannot have inputs".into(),
            ));
        }

        Ok(self.columns.iter().map(|(value, _)| *value).collect())
    }
}

impl Operation for Infallible {
    fn map_values(&mut self, _: &mut impl FnMut(&mut ValueId)) {
        match *self {}
    }

    fn output(&self, _: &[Schema], _: &Values) -> Result<Schema> {
        match *self {}
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scalar {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    And,
    Or,
    IsNull,
    IsNotNull,
    Contains,
    StartsWith,
    EndsWith,
    Record,
    List,
    Truncate(crate::input::TruncateUnit),
}

impl Function for Scalar {
    fn return_type(&self, arguments: &[ValueType]) -> Result<ValueType> {
        if matches!(self, Self::Record) {
            return Ok(ValueType::Record(arguments.to_vec()));
        }
        if matches!(self, Self::List) {
            let Some(first) = arguments.first() else {
                return Err(QueryError::PipelineInvariant(
                    "list requires an element type".into(),
                ));
            };
            if arguments.iter().any(|argument| argument != first) {
                return Err(QueryError::PipelineInvariant(
                    "list elements require matching types".into(),
                ));
            }
            return Ok(ValueType::List(Box::new(first.clone())));
        }
        let base = |data_type: &ValueType| match data_type {
            ValueType::Nullable(inner) => inner.as_ref().clone(),
            other => other.clone(),
        };

        if let Self::Truncate(_) = self {
            let [argument] = arguments else {
                return Err(QueryError::PipelineInvariant(
                    "time bucket requires one argument".into(),
                ));
            };

            if !matches!(base(argument), ValueType::Date | ValueType::DateTime) {
                return Err(QueryError::PipelineInvariant(
                    "time bucket requires a date or timestamp".into(),
                ));
            }

            return Ok(if matches!(argument, ValueType::Nullable(_)) {
                ValueType::Nullable(Box::new(ValueType::DateTime))
            } else {
                ValueType::DateTime
            });
        }

        let valid = match (self, arguments) {
            (
                Self::Equal
                | Self::NotEqual
                | Self::Greater
                | Self::GreaterEqual
                | Self::Less
                | Self::LessEqual,
                [left, right],
            ) => base(left) == base(right),
            (Self::And | Self::Or, [left, right]) => {
                base(left) == ValueType::Bool && base(right) == ValueType::Bool
            }
            (Self::IsNull | Self::IsNotNull, [_]) => true,
            (Self::Contains | Self::StartsWith | Self::EndsWith, [left, right]) => {
                base(left) == ValueType::String && base(right) == ValueType::String
            }
            _ => false,
        };

        if !valid {
            return Err(QueryError::PipelineInvariant(
                "invalid scalar argument types".into(),
            ));
        }

        Ok(
            if !matches!(self, Self::IsNull | Self::IsNotNull)
                && arguments
                    .iter()
                    .any(|t| matches!(t, ValueType::Nullable(_)))
            {
                ValueType::Nullable(Box::new(ValueType::Bool))
            } else {
                ValueType::Bool
            },
        )
    }
}
