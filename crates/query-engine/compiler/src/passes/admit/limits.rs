use std::collections::HashMap;

use query_data_model::plan::logical::Expr;

use super::facts::{Anchor, Fact};
use crate::error::{QueryError, Result};
use crate::logical::Query;

pub(super) const MAX_PREDICATES: usize = 16;
pub(super) const MAX_FILTERS_PER_NODE: usize = 10;
pub(super) const MAX_FILTERS_PER_REL: usize = 5;
pub(super) const MAX_FILTER_ENTRIES_PER_PROPERTY: usize = 10;

pub(super) fn predicates(facts: &[Fact<'_>]) -> Result<()> {
    let count = facts.len();
    if count > MAX_PREDICATES {
        return Err(QueryError::Validation(format!(
            "Query has {count} predicates; the maximum is {MAX_PREDICATES} across the entire query. Remove redundant conditions or simplify the query."
        )));
    }
    Ok(())
}

pub(super) fn filters(query: Query<'_>, facts: &[Fact<'_>], anchors: &[Anchor<'_>]) -> Result<()> {
    let nodes = anchors.iter().map(|anchor| {
        let owner = format!("node \"{}\"", anchor.binding.name);
        (
            anchor.binding.output,
            owner,
            anchor.filters.clone(),
            MAX_FILTERS_PER_NODE,
        )
    });
    let edges = query.steps().into_iter().enumerate().map(|(index, step)| {
        let attached = facts
            .iter()
            .copied()
            .filter(|fact| fact.attached && fact.binding() == step.edge)
            .collect();
        (
            step.edge,
            format!("relationship[{index}]"),
            attached,
            MAX_FILTERS_PER_REL,
        )
    });
    for (target, owner, attached, maximum) in nodes.chain(edges) {
        let conditions = facts
            .iter()
            .filter(|fact| !fact.attached && !fact.compares_bindings() && fact.binding() == target);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for fact in attached.iter().chain(conditions) {
            *counts.entry(name(query, fact.left)).or_default() += 1;
        }
        if counts.len() > maximum {
            return Err(QueryError::Validation(format!(
                "filter property count ({}) for {owner} must not exceed {maximum}",
                counts.len()
            )));
        }
        if let Some((property, count)) = counts
            .into_iter()
            .find(|(_, count)| *count > MAX_FILTER_ENTRIES_PER_PROPERTY)
        {
            return Err(QueryError::Validation(format!(
                "filter entry count ({count}) on property \"{property}\" for {owner} must not exceed {MAX_FILTER_ENTRIES_PER_PROPERTY}"
            )));
        }
    }
    Ok(())
}

fn name(query: Query<'_>, expr: Expr) -> String {
    match expr {
        Expr::Property { property, .. } | Expr::Truncate { property, .. } => {
            query.property_name(property).to_string()
        }
        Expr::Edge { field, .. } => format!("{field:?}"),
        Expr::Attribute { attribute, .. } => query.model.graph().attribute(attribute).name.clone(),
        Expr::Node(_) | Expr::Output(_) => String::new(),
    }
}
