//! Rejects queries past the engine limits: predicate and filter caps,
//! virtual-column and text-index rules, and scans with no selective node.

#[cfg(test)]
mod contract;
mod limits;
mod rules;
mod selectivity;

use crate::passes::codegen::RelationalModel;
use query_data_model::plan::logical::{Expr, Predicate, TimeUnit};

use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Query;
use crate::logical::facts::{self, Anchor, Fact};

pub fn admit(query: Query<'_>, model: &impl RelationalModel, selectivity: bool) -> Result<()> {
    let facts = facts::facts(query);
    let anchors = facts::anchors(query, &facts);
    limits::predicates(&facts)?;
    limits::filters(query, &facts, &anchors)?;
    rules::negated(query, &facts, model)?;
    filters(query, &facts, &anchors, model)?;
    rules::epoch(query, &facts, model)?;
    rules::aggregation_text(query, &facts, model)?;
    rules::order(query, model)?;
    buckets(query, &anchors)?;
    if selectivity {
        selectivity::selectivity(query, &facts, &anchors)?;
    }
    Ok(())
}

/// Whether a lookup serves the property an expression reads.
fn served(model: &impl RelationalModel, expr: Expr) -> bool {
    matches!(expr, Expr::Property { property, .. } if model.catalog().lookup(property).is_some())
}

fn node_property<'p>(query: Query<'p>, expr: Expr) -> Option<(&'p str, &'p str, &'p str)> {
    let Expr::Property { binding, property } = expr else {
        return None;
    };
    Some((
        query.name(binding),
        query.entity_name(binding)?,
        query.property_name(property),
    ))
}

fn filters(
    query: Query<'_>,
    facts: &[Fact<'_>],
    anchors: &[Anchor<'_>],
    model: &impl RelationalModel,
) -> Result<()> {
    let kind = query.kind();
    for fact in facts {
        let Some((alias, entity, property)) = node_property(query, fact.left) else {
            continue;
        };
        if served(model, fact.left) {
            if matches!(
                kind,
                Kind::Aggregation | Kind::Neighbors | Kind::PathFinding
            ) {
                let kind: &str = kind.into();
                return Err(QueryError::Validation(format!(
                    "filter on \"{property}\" for {entity}: virtual columns cannot be \
                     filtered in {kind} queries"
                )));
            }
            let pinned = anchors
                .iter()
                .any(|anchor| anchor.binding.name == alias && !anchor.pinned.is_empty());
            if !pinned {
                return Err(QueryError::Validation(format!(
                    "filter on \"{property}\" for {entity}: virtual columns can only be \
                     filtered on nodes with explicit node_ids; searching by virtual \
                     column content is not yet supported"
                )));
            }
        }
        let tokens = matches!(fact.leaf, Predicate::Text { matching, .. } if matching.tokens());
        let indexed = model
            .property(entity, property)
            .is_some_and(|property| model.has_text_index(property.id));
        if tokens && !indexed {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: \
                 token operators (token_match/all_tokens/any_tokens) require a text index on the field"
            )));
        }
    }
    Ok(())
}

fn buckets(query: Query<'_>, anchors: &[Anchor<'_>]) -> Result<()> {
    let Some((groups, _)) = query.aggregate() else {
        return Ok(());
    };
    for (index, group) in groups.iter().enumerate() {
        let Expr::Truncate {
            binding,
            property,
            unit: unit @ (TimeUnit::Minute | TimeUnit::Hour),
        } = group
        else {
            continue;
        };
        let Some(anchor) = anchors
            .iter()
            .find(|anchor| anchor.binding.output == *binding)
        else {
            continue;
        };
        let filtered = anchor.filters.iter().any(|fact| {
            matches!(fact.left, Expr::Property { property: filtered, .. } if filtered == *property)
        });
        if anchor.pinned.is_empty() && !filtered {
            let property = query.property_name(*property);
            let unit: &str = unit.into();
            return Err(QueryError::Validation(format!(
                "group_by[{index}]: truncate \"{unit}\" on \"{property}\" requires either node_ids on \"{}\" \
                 or at least one filter on \"{property}\" to bound bucket cardinality",
                anchor.binding.name,
            )));
        }
    }
    Ok(())
}
