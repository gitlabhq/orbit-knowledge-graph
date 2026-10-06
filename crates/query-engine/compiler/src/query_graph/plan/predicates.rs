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
        || crate::passes::normalize::is_wildcard(&relationship.types)
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
        use crate::input::FilterOp;
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
                if filter.rhs_column.is_some() {
                    return Err(GraphError::UnsupportedInput(
                        "nonliteral equality filter".into(),
                    ));
                }
                let stored = self.stored_column(relation, column)?;
                let Port::Stored(property) = stored.port else {
                    return Err(GraphError::MissingOutput);
                };
                let literal = |value: &serde_json::Value| match value {
                    _ if matches!(
                        property.data_type(),
                        Some(
                            ontology::DataType::Date
                                | ontology::DataType::DateTime
                                | ontology::DataType::Float
                        )
                    ) =>
                    {
                        Ok(Expression::Literal {
                            data_type: match property.data_type() {
                                Some(ontology::DataType::Date) => SqlType::Date,
                                Some(ontology::DataType::DateTime) => SqlType::Timestamp {
                                    precision: 6,
                                    timezone: None,
                                },
                                _ => SqlType::Float64,
                            },
                            value: value.clone(),
                        })
                    }
                    serde_json::Value::String(value) => Ok(Expression::Text(value.clone())),
                    serde_json::Value::Bool(value) => Ok(Expression::Boolean(*value)),
                    serde_json::Value::Number(value) if value.as_i64().is_some() => {
                        Ok(Expression::Integer(value.as_i64().unwrap()))
                    }
                    _ => Err(GraphError::UnsupportedInput("filter value".into())),
                };
                let operator = filter.op.unwrap_or(FilterOp::Eq);
                let value = match filter.value.as_ref() {
                    _ if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) => None,
                    Some(serde_json::Value::Array(values)) if operator == FilterOp::In => {
                        if values.is_empty() {
                            operation = operation.filter(Expression::Boolean(false));
                            continue;
                        }
                        Some(Expression::Array(
                            values.iter().map(literal).collect::<Result<_>>()?,
                        ))
                    }
                    Some(value) => Some(literal(value)?),
                    _ => return Err(GraphError::UnsupportedInput("filter value".into())),
                };
                let value_column = Expression::Column(self.stored_column(relation, column)?);
                let predicate = if operator == FilterOp::Eq {
                    Expression::equal(value_column, value.expect("equality operand"))
                } else {
                    let Source::Stored(table) = self.relation(relation)?.source else {
                        return Err(GraphError::MissingOutput);
                    };
                    Expression::Predicate {
                        operator,
                        value: Box::new(value_column),
                        argument: value.map(Box::new),
                        fold_case: !self.catalog.in_sort_key(table.name(), column),
                    }
                };
                operation = operation.filter(predicate);
            }
        }
        Ok(operation.filter(Expression::equal(
            Expression::Column(self.stored_column(relation, "_deleted")?),
            Expression::Boolean(false),
        )))
    }
}
