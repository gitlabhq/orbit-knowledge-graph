use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{Order, Relation, RelationalBuilder};
use query_data_model::plan::logical::{EdgeField, Expr, Order as Sort};

use crate::constants::EDGE_KIND_SUFFIX;
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::lower::build::predicates::Lowering;
use crate::passes::lower::build::{Built, computed};
use crate::passes::lower::{NodeBinding, Term, graph};
use crate::passes::plan::predicates::{Column, OutputValue};
use crate::passes::plan::{EdgeRelation, Plan, Traversal, property_value};
use crate::tp_bounds::TpBounds;

pub(crate) struct Shape {
    order: Option<(OutputValue, bool)>,
    sorted: Option<Column>,
    limit: u32,
}

pub(crate) fn shape(plan: &Plan<Traversal>, query: Query<'_>, limit: u32) -> Result<Shape> {
    let execution = &plan.operation.execution;
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
    let sorted = property.map(|(node, property, _)| execution.property(node, property));
    let order = match (property, kind) {
        (Some((node, property, descending)), _) => Some((
            property_value(&plan.nodes, node, execution.property(node, property)),
            descending,
        )),
        (None, Some((edge, descending))) => {
            let relationship = query.steps().iter().position(|step| step.edge == edge);
            let edge_type = plan
                .relations
                .iter()
                .find(|hop| Some(hop.input_index) == relationship)
                .and_then(|hop| edge_type(plan, hop))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            Some((edge_type.clone(), descending))
        }
        (None, None) => None,
    };
    Ok(Shape {
        order,
        sorted,
        limit,
    })
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<Traversal>,
    shape: &Shape,
    tp_bounds: Option<&TpBounds>,
) -> Built<Relation> {
    let execution = &plan.operation.execution;
    let mut read = super::super::redaction_columns(plan, execution);
    read.extend(shape.sorted.iter().cloned());
    let executed = graph::execute(q, execution, &read, tp_bounds)?;
    let rows = executed.reads.filter(q, executed.source)?;
    let lowering = Lowering {
        scope: &[&rows],
        shared: &executed.reads.shared,
    };
    let items = lowering.projections::<B>(&execution.outputs)?;
    let (rows, columns) = computed(q, rows, items)?;
    let rows = match &shape.order {
        Some((value, descending)) => {
            let lowering = Lowering {
                scope: &[&rows],
                shared: &executed.reads.shared,
            };
            let value = lowering.output::<B>(value)?;
            let order = Order {
                value,
                descending: *descending,
            };
            q.order(rows, vec![order])?
        }
        None => rows,
    };
    let rows = q.limit(rows, Some(shape.limit), None)?;
    q.select(rows, columns)
}

pub(crate) fn stable_order(
    plan: &Plan<Traversal>,
    query: Query<'_>,
    nodes: &std::collections::HashMap<String, NodeBinding>,
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
    let edge_types = plan
        .relations
        .iter()
        .filter(|hop| {
            hop.max_hops == 1
                && (hop.rel_types.len() > 1 || crate::input::is_wildcard(&hop.rel_types))
        })
        .filter_map(|hop| edge_type(plan, hop))
        .map(|value| Term::Value(Box::new(value.clone())));
    order.into_iter().chain(edge_types).collect()
}

fn edge_type<'p>(plan: &'p Plan<Traversal>, relation: &EdgeRelation) -> Option<&'p OutputValue> {
    let name = format!("{}{}", relation.column_prefix(), EDGE_KIND_SUFFIX);
    plan.operation
        .execution
        .outputs
        .iter()
        .find(|output| output.name == name)
        .map(|output| &output.value)
}
