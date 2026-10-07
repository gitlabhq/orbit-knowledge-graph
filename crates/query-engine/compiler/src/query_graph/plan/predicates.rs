use super::*;

pub(super) fn edge_tag<'a>(
    model: &'a (impl QueryDataModel + ?Sized),
    node: &crate::input::InputNode,
    property: &str,
    filters: &[crate::input::InputFilter],
    relationship: &crate::input::InputRelationship,
) -> Option<(&'a str, Vec<Vec<String>>)> {
    use query_data_model::{DenormalizedDirection, DenormalizedKey};
    if relationship.direction == crate::input::Direction::Both
        || relationship.types.is_any()
        || relationship.types.is_empty()
    {
        return None;
    }
    let column = if relationship.from == node.id {
        relationship.direction.edge_columns().0
    } else if relationship.to == node.id {
        relationship.direction.edge_columns().1
    } else {
        return None;
    };
    let property = model.property(node.entity.as_deref()?, property)?;
    let direction = if column == "source_id" {
        DenormalizedDirection::Source
    } else {
        DenormalizedDirection::Target
    };
    let facts = model.denormalized().property(DenormalizedKey {
        property: property.id,
        direction,
    })?;
    if !relationship.types.iter().all(|kind| {
        model
            .graph()
            .relationship_id(kind)
            .is_some_and(|id| facts.relationships.contains(&id))
    }) {
        return None;
    }
    let values = filters
        .iter()
        .map(|filter| crate::passes::plan::helpers::denorm_tag_values(&facts.tag_key, filter))
        .collect::<Option<Vec<_>>>()?;
    Some((&facts.edge_column, values))
}

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn node_source(
        &self,
        relation: RelationId,
        node: &crate::input::InputNode,
    ) -> Result<PhysicalOperation<'catalog>> {
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let mut operation = PhysicalOperation::current(relation);
        for predicate in
            Expression::identity_predicates(self.stored_column(relation, &node.id_property)?, node)
        {
            operation = operation.filter(predicate);
        }
        let mut properties = node.filters.iter().collect::<Vec<_>>();
        properties.sort_by_key(|(name, _)| *name);
        for (name, filters) in properties {
            if self.catalog.virtual_source(entity, name).is_some() {
                continue;
            }
            let column = self
                .catalog
                .property_column_named(entity, name)
                .ok_or_else(|| GraphError::UnknownStored(name.clone()))?;
            for filter in filters {
                let Source::Stored(table) = self.relation(relation)?.source else {
                    return Err(GraphError::MissingOutput);
                };
                operation = operation.filter(self.filter_predicate(
                    self.stored_column(relation, column)?,
                    table.column(column).ok_or(GraphError::MissingOutput)?,
                    filter,
                )?);
            }
        }
        Ok(operation.filter(Expression::equal(
            Expression::Column(self.stored_column(relation, "_deleted")?),
            Expression::Boolean(false),
        )))
    }

    pub(super) fn filter_predicate(
        &self,
        column: ColumnRef<'catalog>,
        property: StoredColumnRef<'catalog>,
        filter: &crate::input::InputFilter,
    ) -> Result<Expression<'catalog>> {
        use crate::input::FilterOp;
        if filter.rhs_column.is_some() {
            return Err(GraphError::UnsupportedInput("nonliteral filter".into()));
        }
        let literal = |value: &serde_json::Value| match (property.data_type(), value) {
            (
                Some(
                    ontology::DataType::Date
                    | ontology::DataType::DateTime
                    | ontology::DataType::Float,
                ),
                _,
            ) => Ok(Expression::Literal {
                data_type: match property.data_type() {
                    Some(ontology::DataType::Date) => SqlType::Date,
                    Some(ontology::DataType::DateTime) => SqlType::Timestamp {
                        precision: 6,
                        timezone: None,
                    },
                    _ => SqlType::Float64,
                },
                value: value.clone(),
            }),
            (_, serde_json::Value::String(value)) => Ok(Expression::Text(value.clone())),
            (_, serde_json::Value::Bool(value)) => Ok(Expression::Boolean(*value)),
            (_, serde_json::Value::Number(value)) if value.as_i64().is_some() => {
                Ok(Expression::Integer(value.as_i64().unwrap()))
            }
            _ => Err(GraphError::UnsupportedInput("filter value".into())),
        };
        let operator = filter.op.unwrap_or(FilterOp::Eq);
        let argument = match filter.value.as_ref() {
            _ if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) => None,
            Some(serde_json::Value::Array(values)) if operator == FilterOp::In => {
                if values.is_empty() {
                    return Ok(Expression::Boolean(false));
                }
                Some(Expression::Array(
                    values.iter().map(literal).collect::<Result<_>>()?,
                ))
            }
            Some(value) => Some(literal(value)?),
            None => return Err(GraphError::UnsupportedInput("filter value".into())),
        };
        let value = Expression::Column(column);
        Ok(if operator == FilterOp::Eq {
            Expression::equal(value, argument.ok_or(GraphError::ExpressionType)?)
        } else {
            Expression::Predicate {
                operator,
                value: Box::new(value),
                argument: argument.map(Box::new),
                fold_case: !self
                    .catalog
                    .in_sort_key(property.table().name(), property.name()),
            }
        })
    }
}
