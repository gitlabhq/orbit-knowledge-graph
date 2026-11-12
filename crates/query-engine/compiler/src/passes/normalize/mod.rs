//! Rewrites the logical plan into the form the planner reads: enum codes
//! become labels, wildcard relationships become the variants the pattern can
//! reach, and `type(r)` conditions narrow the relationships they name.

mod enums;
mod kinds;

use crate::passes::codegen::SqlBackend;
use orbit_server_config::QueryLimits;
use query_data_model::Catalog;
use std::collections::HashMap;

use query_data_model::plan::logical::{
    EdgeField, Expr, LogicalBuilder, LogicalOperation, LogicalPlan, PlanError, Projection, Rows,
};
use query_data_model::plan::tracking::{Ref, Step};
use query_data_model::{Direction, Model, RelationshipId};

use crate::dag::Remotes;
use crate::error::{QueryError, Result};
use crate::logical::Query;
use kinds::Narrowed;

pub fn normalize(
    plan: LogicalPlan,
    model: &Model,
    data: &Catalog<impl SqlBackend>,
    remotes: &Remotes,
    limits: &QueryLimits,
) -> Result<LogicalPlan> {
    let query = Query::new(&plan, model);
    available(query, data, remotes)?;
    let narrowed = kinds::narrowed(query)?;
    shared_keys(query, &narrowed, data)?;
    let catalog = model.logical(limits).as_admin();
    Ok(catalog.rebuild(plan, |q, step| rewrite(q, step, &narrowed, model))?)
}

fn available(query: Query<'_>, data: &Catalog<impl SqlBackend>, remotes: &Remotes) -> Result<()> {
    let entities = query.plan.nodes().flat_map(|node| query.plan.outputs(node));
    for entity in entities.filter_map(|output| query.entity(output)) {
        if data.table_of(entity).is_none() && remotes.holding(entity).is_none() {
            let entity = &query.model.graph().entity(entity).name;
            return Err(QueryError::AllowlistRejected(format!(
                "entity '{entity}' is not available in this data model"
            )));
        }
    }
    Ok(())
}

fn shared_keys(
    query: Query<'_>,
    narrowed: &HashMap<Ref, Narrowed>,
    data: &Catalog<impl SqlBackend>,
) -> Result<()> {
    let returned = |edge: Ref| {
        let selected = query.selected().into_iter().any(|item| match item {
            Projection::Graph { binding, .. } => *binding == edge,
            Projection::Value(expr) => *expr == kind(edge),
        });
        selected || query.ordered().contains(&kind(edge))
    };
    for step in query.steps() {
        let relationships = match narrowed.get(&step.edge) {
            Some(narrowed) => &narrowed.ids,
            None => step.relationships,
        };
        let (Some(near), Some(far)) = (query.entity(step.from), query.entity(step.to)) else {
            continue;
        };
        let (from, to) = match step.direction {
            Direction::Incoming => (far, near),
            _ => (near, far),
        };
        let shared = || data.shared_foreign_key(relationships, from, to).is_some();
        if relationships.len() > 1 && returned(step.edge) && shared() {
            let graph = query.model.graph();
            let names: Vec<&str> = relationships
                .iter()
                .map(|id| graph.relationship(*id).name.as_str())
                .collect();
            let (from, to) = (&graph.entity(from).name, &graph.entity(to).name);
            return Err(QueryError::Validation(format!(
                "{} share one foreign key between {from} and {to}, so type() and a returned relationship cannot tell them apart; match one relationship type, such as -[r:{}]->",
                names.join("|"),
                names[0]
            )));
        }
    }
    Ok(())
}

fn kind(edge: Ref) -> Expr {
    Expr::Edge {
        binding: edge,
        field: EdgeField::Kind,
    }
}

fn rewrite<'m>(
    q: &mut LogicalBuilder<'m>,
    step: &mut Step<'_, '_, LogicalBuilder<'m>>,
    narrowed: &HashMap<Ref, Narrowed>,
    model: &Model,
) -> std::result::Result<Rows<'m>, PlanError> {
    if let LogicalOperation::Filter(predicate) = &step.operation {
        let rewritten = enums::coerce(kinds::without_types(predicate), model);
        if rewritten.as_ref() != Some(predicate) {
            let input = step.inputs.remove(0);
            return match rewritten {
                Some(predicate) => q.filter(input, predicate),
                None => Ok(input),
            };
        }
    }
    let outputs = step.outputs();
    if let [.., edge, _] = outputs[..]
        && let Some(narrowed) = narrowed.get(&edge)
    {
        retype(&mut step.operation, &narrowed.ids);
    }
    step.keep(q)
}

fn retype(operation: &mut LogicalOperation, narrowed: &[RelationshipId]) {
    match operation {
        LogicalOperation::Follow {
            relationships,
            hops,
            ..
        } => {
            hops.retain(|hop| narrowed.contains(&hop.relationship));
            *relationships = narrowed.to_vec();
        }
        LogicalOperation::Neighbors { relationships, .. }
        | LogicalOperation::Shortest { relationships, .. } => *relationships = narrowed.to_vec(),
        _ => {}
    }
}

#[cfg(test)]
mod tests;
