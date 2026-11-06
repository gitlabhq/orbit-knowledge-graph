use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{Order, Relation, RelationalBuilder};

use crate::constants::EDGE_KIND_SUFFIX;
use crate::error::{QueryError, Result};
use crate::input::{Input, OrderDirection};
use crate::passes::lower::build::predicates::Lowering;
use crate::passes::lower::build::{Built, computed};
use crate::passes::lower::{NodeBinding, Term, graph};
use crate::passes::plan::predicates::OutputValue;
use crate::passes::plan::{EdgeRelation, Plan, Traversal, property_value};
use crate::tp_bounds::TpBounds;

pub(crate) struct Shape {
    order: Option<(OutputValue, bool)>,
}

pub(crate) fn shape(plan: &Plan<Traversal>, input: &Input) -> Result<Shape> {
    let execution = &plan.operation.execution;
    let order = match (&input.order_by, &input.relationship_return.sort) {
        (Some(order), _) => Some((
            property_value(
                &plan.nodes,
                &order.node,
                execution.property(&order.node, &order.property),
            ),
            order.direction == OrderDirection::Desc,
        )),
        (None, Some(sort)) => {
            let edge_type = plan
                .relations
                .iter()
                .find(|hop| hop.input_index == sort.relationship)
                .and_then(|hop| edge_type(plan, hop))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            Some((edge_type.clone(), sort.direction == OrderDirection::Desc))
        }
        (None, None) => None,
    };
    Ok(Shape { order })
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<Traversal>,
    input: &Input,
    shape: &Shape,
    tp_bounds: Option<&TpBounds>,
) -> Built<Relation> {
    let execution = &plan.operation.execution;
    let mut read = super::super::redaction_columns(plan, execution);
    read.extend(
        input
            .order_by
            .iter()
            .map(|order| execution.property(&order.node, &order.property)),
    );
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
    let rows = q.limit(rows, Some(input.limit), None)?;
    q.select(rows, columns)
}

pub(crate) fn stable_order(
    plan: &Plan<Traversal>,
    input: &Input,
    nodes: &std::collections::HashMap<String, NodeBinding>,
) -> Vec<Term> {
    let identities = input
        .nodes
        .iter()
        .filter_map(|node| nodes.get(&node.id))
        .map(|binding| binding.identity().clone());
    if input.relationships.is_empty() {
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
