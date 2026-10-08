use super::super::api::*;
use crate::input::{Direction, FilterOp, InputFilter, InputNode, InputRelationship};
use orbit_utils::query_types::SqlType;
use query_data_model::{DenormalizedDirection, DenormalizedKey, QueryDataModel};

pub(super) fn compare(left: Expr, operator: FilterOp, right: Expr) -> Result<Expr> {
    let operator = match operator {
        FilterOp::Eq => Operator::Equal,
        FilterOp::Ne => Operator::NotEqual,
        FilterOp::Gt => Operator::Greater,
        FilterOp::Gte => Operator::GreaterEqual,
        FilterOp::Lt => Operator::Less,
        FilterOp::Lte => Operator::LessEqual,
        FilterOp::In => Operator::In,
        _ => return Err(Error::Type),
    };
    Ok(left.binary(operator, right))
}

pub(super) fn property(column: &Column, filter: &InputFilter, in_sort_key: bool) -> Result<Expr> {
    if filter.rhs_column.is_some() {
        return Err(Error::Column);
    }
    let operator = filter.op.unwrap_or(FilterOp::Eq);
    if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
        let function = if operator == FilterOp::IsNull {
            Function::IsNull
        } else {
            Function::IsNotNull
        };
        return Ok(Expr::call(function, [column.expr()]));
    }
    let value = filter.value.as_ref().ok_or(Error::Type)?;
    let ValueType::Scalar(data_type) = column.data_type() else {
        return Err(Error::Type);
    };
    let argument = if operator == FilterOp::In {
        let values = value.as_array().ok_or(Error::Type)?;
        if values.is_empty() {
            return Ok(lit(false));
        }
        Expr::literal(data_type.to_array(), value.clone())
    } else {
        Expr::literal(*data_type, value.clone())
    };
    let function = match operator {
        FilterOp::Contains => Function::Contains,
        FilterOp::StartsWith => Function::StartsWith,
        FilterOp::EndsWith => Function::EndsWith,
        FilterOp::TokenMatch => Function::TokenMatch,
        FilterOp::AllTokens => Function::AllTokens,
        FilterOp::AnyTokens => Function::AnyTokens,
        _ => return compare(column.expr(), operator, argument),
    };
    let fold = |value| {
        if in_sort_key {
            value
        } else {
            Expr::call(Function::Lower, [value])
        }
    };
    Ok(Expr::call(function, [fold(column.expr()), fold(argument)]))
}

pub(super) fn identity(rows: &Rows<'_>, node: &InputNode) -> Result<Vec<Expr>> {
    let column = rows.column(&node.id_property)?;
    let mut predicates = Vec::new();
    if let [id] = node.node_ids.as_slice() {
        predicates.push(column.eq(*id));
    } else if !node.node_ids.is_empty() {
        predicates.push(column.expr().binary(
            Operator::In,
            Expr::literal(SqlType::Int64.to_array(), node.node_ids.clone().into()),
        ));
    }
    if let Some(range) = &node.id_range {
        predicates.push(column.ge(range.start).and(column.le(range.end)));
    }
    Ok(predicates)
}

pub(super) fn node(
    model: &(impl QueryDataModel + ?Sized),
    rows: &Rows<'_>,
    node: &InputNode,
) -> Result<Vec<Expr>> {
    let entity = node.entity.as_deref().ok_or(Error::Outputs)?;
    let table = model
        .entity_table(entity)
        .ok_or_else(|| Error::Unknown(entity.into()))?;
    let mut predicates = identity(rows, node)?;
    let mut properties = node.filters.iter().collect::<Vec<_>>();
    properties.sort_by_key(|(name, _)| *name);
    for (name, filters) in properties {
        if model.virtual_source(entity, name).is_some() {
            continue;
        }
        let name = model
            .property_column_named(entity, name)
            .ok_or_else(|| Error::Unknown(name.clone()))?;
        let column = rows.column(name)?;
        for filter in filters {
            predicates.push(property(&column, filter, model.in_sort_key(table, name))?);
        }
    }
    predicates.push(rows.column(ontology::DELETED_COLUMN)?.eq(false));
    Ok(predicates)
}

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
