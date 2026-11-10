use std::collections::HashMap;

use crate::passes::codegen::SqlBackend;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{Order, Relation, RelationalBuilder};
use query_data_model::plan::logical::{EdgeField, Expr, Order as Sort};

use crate::constants::EDGE_KIND_SUFFIX;
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::plan::build::computed;
use crate::passes::plan::build::predicates::Lowering;
use crate::passes::plan::pattern::Pattern;
use crate::passes::plan::predicates::OutputValue;
use crate::passes::plan::{EdgeRelation, Matched, property_value};
use crate::passes::plan::{NodeBinding, Term};

pub(crate) struct Shape {
    order: Option<(OutputValue, bool)>,
    limit: u32,
}

pub(crate) fn shape<B: SqlBackend>(
    pattern: &Pattern<'_, B>,
    matched: &Matched,
    query: Query<'_>,
    limit: u32,
) -> Result<Shape> {
    let keys = query.sort_keys();
    let property = keys.iter().rev().find_map(|(key, order)| match key {
        Expr::Property { binding, property } => Some((
            query.name(*binding),
            query.property_name(*property),
            *order == Sort::Descending,
        )),
        _ => None,
    });
    let kind = keys.iter().rev().find_map(|(key, order)| match key {
        Expr::Edge {
            binding,
            field: EdgeField::Kind,
        } => Some((*binding, *order == Sort::Descending)),
        _ => None,
    });
    let order = match (property, kind) {
        (Some((node, property, descending)), _) => Some((
            property_value(&pattern.nodes, node, matched.property(node, property)),
            descending,
        )),
        (None, Some((edge, descending))) => {
            let relationship = query.steps().iter().position(|step| step.edge == edge);
            let edge_type = pattern
                .relations
                .iter()
                .find(|hop| Some(hop.input_index) == relationship)
                .and_then(|hop| edge_type(matched, hop))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            Some((edge_type.clone(), descending))
        }
        (None, None) => None,
    };
    Ok(Shape { order, limit })
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    matched: Matched,
    shape: &Shape,
) -> Result<Relation> {
    let rows = matched.rows;
    let items = Lowering { scope: &[&rows] }.projections::<B>(&matched.outputs)?;
    let (rows, columns) = computed(q, rows, items)?;
    let rows = match &shape.order {
        Some((value, descending)) => {
            let value = Lowering { scope: &[&rows] }.output::<B>(value)?;
            let order = Order {
                value,
                descending: *descending,
            };
            q.order(rows, vec![order])?
        }
        None => rows,
    };
    let rows = q.limit(rows, Some(shape.limit), None)?;
    Ok(q.select(rows, columns)?)
}

pub(crate) fn stable_order<B: SqlBackend>(
    pattern: &Pattern<'_, B>,
    matched: &Matched,
    query: Query<'_>,
    nodes: &HashMap<String, NodeBinding>,
) -> Vec<Term> {
    let identities = query
        .bindings()
        .into_iter()
        .filter_map(|binding| nodes.get(binding.name))
        .map(|binding| binding.identity().clone());
    if query.steps().is_empty() {
        return identities.collect();
    }
    let mut order: Vec<Term> = Vec::new();
    for identity in identities {
        if !order.contains(&identity) {
            order.push(identity);
        }
    }
    let edge_types = pattern
        .relations
        .iter()
        .filter(|hop| hop.max_hops == 1 && hop.relationships.len() != 1)
        .filter_map(|hop| edge_type(matched, hop))
        .map(|value| Term::Value(Box::new(value.clone())));
    order.into_iter().chain(edge_types).collect()
}

fn edge_type<'p>(matched: &'p Matched, relation: &EdgeRelation) -> Option<&'p OutputValue> {
    let name = format!("{}{}", relation.column_prefix(), EDGE_KIND_SUFFIX);
    matched
        .outputs
        .iter()
        .find(|output| output.name == name)
        .map(|output| &output.value)
}
