//! Rewrites the logical plan into the form the planner reads: enum codes
//! become labels, wildcard relationships become the variants the pattern can
//! reach, and `type(r)` conditions narrow the relationships they name.

mod enums;
mod kinds;

use crate::passes::codegen::RelationalModel;
use std::collections::HashMap;

use query_data_model::plan::logical::{
    EdgeField, Expr, LogicalBuilder, LogicalOperation, LogicalPlan, PlanError, Projection, Rows,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, Model};

use crate::error::{QueryError, Result};
use crate::logical::Query;
use kinds::Narrowed;

pub fn normalize(
    plan: LogicalPlan,
    model: &Model,
    data: &impl RelationalModel,
) -> Result<LogicalPlan> {
    let query = Query::new(&plan, model);
    available(query, data)?;
    let narrowed = kinds::narrowed(query)?;
    shared_keys(query, &narrowed, data)?;
    let catalog = model.logical().as_admin();
    Ok(catalog.rewrite(plan, |q, rows| rewrite(q, rows, &narrowed, model))?)
}

fn available(query: Query<'_>, data: &impl RelationalModel) -> Result<()> {
    let entities = query.plan.nodes().flat_map(|node| query.plan.outputs(node));
    for entity in entities.filter_map(|output| query.entity(output)) {
        if data.catalog().table_of(entity).is_none() {
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
    data: &impl RelationalModel,
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
        let shared = || {
            data.catalog()
                .shared_foreign_key(relationships, from, to)
                .is_some()
        };
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
    rows: Rows<'m>,
    narrowed: &HashMap<Ref, Narrowed>,
    model: &Model,
) -> std::result::Result<Rows<'m>, PlanError> {
    let operation = q.operation(&rows)?.clone();
    match operation {
        LogicalOperation::Filter(predicate) => {
            let rewritten = enums::coerce(kinds::without_types(&predicate), model);
            if rewritten.as_ref() == Some(&predicate) {
                return Ok(rows);
            }
            let mut detached = q.detach(rows)?;
            let input = detached.inputs.remove(0);
            match rewritten {
                Some(predicate) => q.filter(input, predicate),
                None => Ok(input),
            }
        }
        LogicalOperation::Follow { .. } | LogicalOperation::Neighbors { .. } => {
            let outputs = q.outputs(&rows)?;
            let [.., edge, _] = outputs[..] else {
                return Ok(rows);
            };
            match narrowed.get(&edge) {
                Some(narrowed) => q.retype(rows, narrowed.ids.clone()),
                None => Ok(rows),
            }
        }
        _ => Ok(rows),
    }
}

#[cfg(test)]
mod tests;
