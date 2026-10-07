use super::*;
use crate::input::{Direction, FilterOp, InputFilter, InputNode, InputRelationship};
use query_data_model::{DenormalizedDirection, DenormalizedKey};

pub(super) fn edge_tag<'a>(
    model: &'a (impl QueryDataModel + ?Sized),
    node: &InputNode,
    property: &str,
    filters: &[InputFilter],
    relationship: &InputRelationship,
) -> Option<(&'a str, Vec<Vec<String>>)> {
    if relationship.direction == Direction::Both
        || relationship.types.is_any()
        || relationship.types.is_empty()
    {
        return None;
    }
    let endpoint = if relationship.from == node.id {
        relationship.direction.edge_columns().0
    } else if relationship.to == node.id {
        relationship.direction.edge_columns().1
    } else {
        return None;
    };
    let property = model.property(node.entity.as_deref()?, property)?;
    let direction = if endpoint == "source_id" {
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
        .collect::<Option<_>>()?;
    Some((&facts.edge_column, values))
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn node_source(
        &self,
        relation: RelationId,
        node: &InputNode,
    ) -> Result<PhysicalOperation<'a>> {
        let mut operation = self.read_relation(relation, ReadMode::Current)?;
        for predicate in self.node_predicates(relation, node)? {
            operation = self.filter_relation(operation, predicate)?;
        }
        Ok(operation)
    }

    pub(super) fn node_predicates(
        &self,
        relation: RelationId,
        node: &InputNode,
    ) -> Result<Vec<Expression<'a>>> {
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let Source::Stored(table) = self.relation(relation)?.source else {
            return Err(GraphError::MissingOutput);
        };
        let mut predicates =
            Expression::identity_predicates(self.stored_column(relation, &node.id_property)?, node);
        let mut properties = node.filters.iter().collect::<Vec<_>>();
        properties.sort_by_key(|(name, _)| *name);
        for (name, filters) in properties {
            if self.catalog.virtual_source(entity, name).is_some() {
                continue;
            }
            let name = self
                .catalog
                .property_column_named(entity, name)
                .ok_or_else(|| GraphError::UnknownStored(name.clone()))?;
            let stored = table.column(name).ok_or(GraphError::MissingOutput)?;
            let column = self.stored_port(relation, stored)?;
            for filter in filters {
                predicates.push(self.filter_predicate(column, stored, filter)?);
            }
        }
        predicates.push(Expression::equal(
            Expression::Column(self.stored_column(relation, ontology::DELETED_COLUMN)?),
            Expression::Boolean(false),
        ));
        Ok(predicates)
    }

    pub(super) fn narrowed_node(
        &mut self,
        root: BlockId,
        relation: RelationId,
        key: ColumnRef<'a>,
        candidate: (DefinitionId, OutputId),
        predicates: &[Expression<'a>],
    ) -> Result<PhysicalOperation<'a>> {
        let mut operation = self.narrow(
            root,
            self.read_relation(relation, ReadMode::Raw)?,
            key,
            candidate,
        )?;
        for predicate in predicates {
            if self.sort_key_predicate(predicate)? {
                operation = self.filter_relation(operation, predicate.clone())?;
            }
        }
        operation = self.latest_relation(
            operation,
            self.stored_column(relation, ontology::VERSION_COLUMN)?,
            None,
        )?;
        for predicate in predicates {
            operation = self.filter_relation(operation, predicate.clone())?;
        }
        self.materialize_relation(operation, relation)
    }

    pub(super) fn sort_key_predicate(&self, predicate: &Expression<'a>) -> Result<bool> {
        let mut immutable = true;
        predicate.columns(&mut |column| {
            immutable &= matches!(column.port, Port::Stored(stored) if self.catalog.in_sort_key(stored.table().name(), stored.name()));
            Ok(())
        })?;
        Ok(immutable)
    }

    pub(super) fn filter_predicate(
        &self,
        column: ColumnRef<'a>,
        property: StoredColumnRef<'a>,
        filter: &InputFilter,
    ) -> Result<Expression<'a>> {
        if filter.rhs_column.is_some() {
            return Err(GraphError::UnsupportedInput("nonliteral filter".into()));
        }
        let operator = filter.op.unwrap_or(FilterOp::Eq);
        let argument = match filter.value.as_ref() {
            _ if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) => None,
            Some(serde_json::Value::Array(values)) if operator == FilterOp::In => {
                if values.is_empty() {
                    return Ok(Expression::Boolean(false));
                }
                Some(Expression::Array(
                    values
                        .iter()
                        .map(|value| filter_literal(property, value))
                        .collect::<Result<_>>()?,
                ))
            }
            Some(value) => Some(filter_literal(property, value)?),
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

fn filter_literal<'a>(
    property: StoredColumnRef<'a>,
    value: &serde_json::Value,
) -> Result<Expression<'a>> {
    let typed = match property.data_type() {
        Some(ontology::DataType::Date) => Some(SqlType::Date),
        Some(ontology::DataType::DateTime) => Some(SqlType::Timestamp {
            precision: 6,
            timezone: None,
        }),
        Some(ontology::DataType::Float) => Some(SqlType::Float64),
        _ => None,
    };
    if let Some(data_type) = typed {
        return Ok(Expression::Literal {
            data_type,
            value: value.clone(),
        });
    }
    match value {
        serde_json::Value::String(value) => Ok(Expression::Text(value.clone())),
        serde_json::Value::Bool(value) => Ok(Expression::Boolean(*value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(Expression::Integer)
            .ok_or_else(|| GraphError::UnsupportedInput("filter value".into())),
        _ => Err(GraphError::UnsupportedInput("filter value".into())),
    }
}
