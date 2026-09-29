use std::collections::BTreeMap;
use std::convert::Infallible;

use query_data_model::{EdgeField, EntityId, PropertyId, QueryDataModel, RelationshipId};
use serde_json::Value;

use super::generic::{
    Assignment, Expr, Node, Op, Operation, Schema, SortKey, ValueId, ValueType, Values,
};
use super::physical::Scalar;
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, FilterOp, Input, InputFilter, OrderDirection};

pub type Plan = Node<Source, Scalar, Infallible>;

impl From<ontology::DataType> for ValueType {
    fn from(data_type: ontology::DataType) -> Self {
        match data_type {
            ontology::DataType::Bool => Self::Bool,
            ontology::DataType::Int => Self::Int64,
            ontology::DataType::Float => Self::Float64,
            ontology::DataType::Date => Self::Date,
            ontology::DataType::DateTime => Self::DateTime,
            ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid => {
                Self::String
            }
        }
    }
}

pub enum Source {
    Entity {
        binding: String,
        entity: EntityId,
        properties: Vec<(ValueId, PropertyId)>,
    },
    Edge {
        relationship: usize,
        endpoints: Option<(EntityId, EntityId)>,
        relationships: Vec<RelationshipId>,
        fields: Vec<(ValueId, EdgeField)>,
    },
}

impl Operation for Source {
    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        match self {
            Self::Entity { properties, .. } => {
                properties.iter_mut().for_each(|(value, _)| map(value))
            }
            Self::Edge { fields, .. } => fields.iter_mut().for_each(|(value, _)| map(value)),
        }
    }

    fn output(&self, _: &[Schema], _: &Values) -> Result<Schema> {
        Ok(match self {
            Self::Entity { properties, .. } => properties.iter().map(|(value, _)| *value).collect(),
            Self::Edge { fields, .. } => fields.iter().map(|(value, _)| *value).collect(),
        })
    }
}

impl Source {
    pub fn explain(&self, model: &impl QueryDataModel) -> super::explain::SExpression {
        use super::explain::{SExpression, value};

        match self {
            Self::Entity {
                binding,
                entity,
                properties,
            } => SExpression::node(
                "Entity",
                [
                    SExpression::atom(&model.graph().entity(*entity).name),
                    SExpression::atom(binding),
                    SExpression::node(
                        "Columns",
                        properties.iter().map(|(id, property)| {
                            SExpression::node(
                                "Column",
                                [
                                    value(*id),
                                    SExpression::atom(&model.graph().property(*property).name),
                                ],
                            )
                        }),
                    ),
                ],
            ),
            Self::Edge {
                relationship,
                relationships,
                fields,
                ..
            } => SExpression::node(
                "Edge",
                [
                    SExpression::atom(relationship),
                    SExpression::node(
                        "Kinds",
                        relationships
                            .iter()
                            .map(|id| SExpression::atom(&model.graph().relationship(*id).name)),
                    ),
                    SExpression::node(
                        "Fields",
                        fields.iter().map(|(id, field)| {
                            SExpression::node(
                                "Field",
                                [value(*id), SExpression::atom(format!("{field:?}"))],
                            )
                        }),
                    ),
                ],
            ),
        }
    }
}

pub struct BoundQuery {
    pub root: Plan,
    pub values: Values,
    pub outputs: Vec<String>,
    pub required: Vec<ValueId>,
}

pub fn traversal(
    input: &Input,
    model: &impl QueryDataModel,
    required: &[(String, String)],
    limit: Option<u32>,
) -> Result<BoundQuery> {
    bind_node(input, model, required, limit, Values::default())
}

pub(super) fn bind_node(
    input: &Input,
    model: &impl QueryDataModel,
    required: &[(String, String)],
    limit: Option<u32>,
    mut values: Values,
) -> Result<BoundQuery> {
    if !input.is_search() || !input.join_predicates.is_empty() {
        return Err(QueryError::Validation(
            "binding currently requires a single-node traversal".into(),
        ));
    }

    if limit.is_some() && input.cursor.is_some() {
        return Err(QueryError::Validation(
            "cursor queries require the pagination pipeline".into(),
        ));
    }

    let node = &input.nodes[0];
    let entity = node
        .entity
        .as_deref()
        .and_then(|name| model.entity(name))
        .ok_or_else(|| QueryError::ReferenceError("node requires an available entity".into()))?;
    let property = |name: &str| {
        model
            .property_for_entity_id(entity.id, name)
            .filter(|property| model.property_is_stored(property.id))
            .ok_or_else(|| {
                QueryError::ReferenceError(format!(
                    "{}.{} is not a stored property",
                    entity.name, name
                ))
            })
    };

    let selected: Vec<_> = match &node.columns {
        Some(ColumnSelection::List(names)) => names
            .iter()
            .map(|name| property(name))
            .collect::<Result<_>>()?,
        selection => {
            let defaults = model.default_properties(entity.id);
            let properties = if selection.is_none() && !defaults.is_empty() {
                defaults
            } else {
                &entity.properties
            };
            properties
                .iter()
                .filter(|id| model.property_is_stored(**id))
                .map(|id| model.graph().property(*id))
                .collect()
        }
    };

    if selected.is_empty() && required.is_empty() {
        return Err(QueryError::Validation(
            "traversal requires a stored output property".into(),
        ));
    }

    let mut needed: BTreeMap<_, _> = selected
        .iter()
        .map(|property| (property.name.as_str(), property.id))
        .collect();

    for (name, _) in required {
        needed.insert(name, property(name)?.id);
    }

    for name in node.filters.keys() {
        needed.insert(name, property(name)?.id);
    }

    if !node.node_ids.is_empty() || node.id_range.is_some() {
        needed.insert(&node.id_property, property(&node.id_property)?.id);
    }

    if let Some(order) = &input.order_by {
        if order.node != node.id {
            return Err(QueryError::ReferenceError(
                "sort binding does not exist".into(),
            ));
        }
        needed.insert(&order.property, property(&order.property)?.id);
    }

    let properties: Vec<_> = needed.values().copied().collect();
    let properties = properties
        .into_iter()
        .map(|property| {
            let data_type = ValueType::from(model.graph().property(property).data_type);
            (
                values.allocate(ValueType::Nullable(Box::new(data_type))),
                property,
            )
        })
        .collect::<Vec<_>>();
    let bindings: BTreeMap<_, _> = needed
        .keys()
        .copied()
        .zip(properties.iter().map(|(value, _)| *value))
        .collect();
    let read = Source::Entity {
        binding: node.id.clone(),
        entity: entity.id,
        properties,
    };

    let mut predicates = Vec::new();
    for (name, value) in &bindings {
        for filter in node.filters.get(*name).into_iter().flatten() {
            predicates.push(bind_filter(*value, filter, &values)?);
        }
    }

    if !node.node_ids.is_empty() {
        let id = bindings[node.id_property.as_str()];
        predicates.push(
            node.node_ids
                .iter()
                .map(|value| call(Scalar::Equal, vec![Expr::Value(id), Expr::Int64(*value)]))
                .reduce(|left, right| call(Scalar::Or, vec![left, right]))
                .expect("nonempty IDs"),
        );
    }

    if let Some(range) = &node.id_range {
        let id = bindings[node.id_property.as_str()];
        predicates.push(call(
            Scalar::GreaterEqual,
            vec![Expr::Value(id), Expr::Int64(range.start)],
        ));
        predicates.push(call(
            Scalar::LessEqual,
            vec![Expr::Value(id), Expr::Int64(range.end)],
        ));
    }

    let mut root = Node {
        op: Op::Read(read),
        inputs: vec![],
    };

    if let Some(predicate) = predicates
        .into_iter()
        .reduce(|left, right| call(Scalar::And, vec![left, right]))
    {
        root = Node {
            op: Op::Filter(predicate),
            inputs: vec![root],
        };
    }
    if let Some(order) = &input.order_by {
        root = Node {
            op: Op::Sort(vec![SortKey {
                value: bindings[order.property.as_str()],
                descending: order.direction == OrderDirection::Desc,
                nulls_first: false,
            }]),
            inputs: vec![root],
        };
    }

    let mut assignments: Vec<_> = selected
        .iter()
        .map(|property| {
            let value = bindings[property.name.as_str()];
            Ok(Assignment {
                output: values.allocate(values.data_type(value)?.clone()),
                expression: Expr::Value(value),
            })
        })
        .collect::<Result<_>>()?;
    let mut outputs: Vec<_> = selected
        .iter()
        .map(|property| format!("{}_{}", node.id, property.name))
        .collect();
    let mut required_values = Vec::new();

    for (property, name) in required {
        let source = bindings[property.as_str()];
        let output = values.allocate(values.data_type(source)?.clone());
        assignments.push(Assignment {
            output,
            expression: Expr::Value(source),
        });
        outputs.push(name.clone());
        required_values.push(output);
    }

    root = Node {
        op: Op::Project(assignments),
        inputs: vec![root],
    };

    if let Some(limit) = limit {
        root = Node {
            op: Op::Limit(limit),
            inputs: vec![root],
        };
    }

    root.output(&values)?;
    Ok(BoundQuery {
        root,
        values,
        outputs,
        required: required_values,
    })
}

pub(super) fn call(function: Scalar, arguments: Vec<Expr<Scalar>>) -> Expr<Scalar> {
    Expr::Call {
        function,
        arguments,
    }
}

pub(super) fn bind_filter(
    value: ValueId,
    filter: &InputFilter,
    values: &Values,
) -> Result<Expr<Scalar>> {
    if filter.rhs_column.is_some() {
        return Err(QueryError::Validation(
            "column comparisons require binding both operands".into(),
        ));
    }

    if filter.op == Some(FilterOp::In) {
        let Some(Value::Array(items)) = &filter.value else {
            return Err(QueryError::Validation("IN requires an array".into()));
        };
        return Ok(items
            .iter()
            .map(|item| {
                Ok(call(
                    Scalar::Equal,
                    vec![Expr::Value(value), literal(item, values.data_type(value)?)?],
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| call(Scalar::Or, vec![left, right]))
            .unwrap_or(Expr::Bool(false)));
    }

    let function = match filter.op.unwrap_or(FilterOp::Eq) {
        FilterOp::Eq => Scalar::Equal,
        FilterOp::Ne => Scalar::NotEqual,
        FilterOp::Gt => Scalar::Greater,
        FilterOp::Gte => Scalar::GreaterEqual,
        FilterOp::Lt => Scalar::Less,
        FilterOp::Lte => Scalar::LessEqual,
        FilterOp::IsNull => return Ok(call(Scalar::IsNull, vec![Expr::Value(value)])),
        FilterOp::IsNotNull => return Ok(call(Scalar::IsNotNull, vec![Expr::Value(value)])),
        _ => {
            return Err(QueryError::Validation(
                "filter operator has no typed binding yet".into(),
            ));
        }
    };

    let argument = filter
        .value
        .as_ref()
        .ok_or_else(|| QueryError::Validation("filter requires a value".into()))?;
    Ok(call(
        function,
        vec![
            Expr::Value(value),
            literal(argument, values.data_type(value)?)?,
        ],
    ))
}

fn literal(value: &Value, data_type: &ValueType) -> Result<Expr<Scalar>> {
    let base = match data_type {
        ValueType::Nullable(inner) => inner.as_ref(),
        other => other,
    };
    Ok(match (base, value) {
        (_, Value::Null) => Expr::Null(data_type.clone()),
        (ValueType::Bool, Value::Bool(value)) => Expr::Bool(*value),
        (ValueType::Int64, Value::Number(value)) if value.is_i64() => {
            Expr::Int64(value.as_i64().unwrap())
        }
        (ValueType::Float64, Value::Number(value)) => Expr::Float64(
            value
                .as_f64()
                .ok_or_else(|| QueryError::Validation("invalid float literal".into()))?,
        ),
        (ValueType::String, Value::String(value)) => Expr::String(value.clone()),
        _ => {
            return Err(QueryError::Validation(
                "filter literal does not match property type".into(),
            ));
        }
    })
}
