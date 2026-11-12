//! Rejects queries past the engine limits: predicate and filter caps,
//! virtual-column and text-index rules, and scans with no selective node.

mod limits;
mod rules;
mod selectivity;

use crate::passes::codegen::SqlBackend;
use orbit_server_config::QueryLimits;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::logical::{Expr, LogicalOperation, Predicate, TimeUnit};
use query_data_model::{Catalog, StorageBackend};

use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Leaf;
use crate::logical::Query;
use crate::logical::anchors::Anchor;

pub fn admit(
    query: Query<'_>,
    model: &Catalog<impl SqlBackend>,
    remotes: &crate::dag::Remotes,
    limits: &QueryLimits,
    selectivity: bool,
) -> Result<()> {
    let searched =
        |structural: &query_data_model::plan::logical::Structural| match structural.target {
            Expr::Property { binding, property } => query
                .entity(binding)
                .is_some_and(|entity| remotes.searching(entity, property).is_some()),
            _ => false,
        };
    if query
        .operations()
        .any(|(_, operation)| matches!(operation, LogicalOperation::Structural(structural) if !searched(structural)))
    {
        return Err(QueryError::Validation(format!(
            "structural matches run on an {} connection, not in SQL",
            OrbitCode::KIND
        )));
    }
    let facts = query.leaves();
    let anchors = query.anchors(&facts);
    self::limits::predicates(&facts, limits.predicates)?;
    self::limits::filters(query, &facts, &anchors, limits)?;
    rules::negated(query, &facts, model)?;
    filters(query, &facts, &anchors, model)?;
    rules::epoch(query, &facts, model)?;
    rules::aggregation_text(query, &facts, model)?;
    rules::order(query, model)?;
    buckets(query, &anchors)?;
    if selectivity {
        selectivity::selectivity(query, &facts, &anchors, limits)?;
    }
    Ok(())
}

/// Whether a lookup serves the property an expression reads.
fn served(model: &Catalog<impl SqlBackend>, expr: Expr) -> bool {
    matches!(expr, Expr::Property { property, .. } if model.lookup(property).is_some())
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
    facts: &[Leaf<'_>],
    anchors: &[Anchor<'_>],
    model: &Catalog<impl SqlBackend>,
) -> Result<()> {
    let kind = query.kind();
    for fact in facts {
        let Some((alias, entity, property)) = node_property(query, fact.left()) else {
            continue;
        };
        if served(model, fact.left()) {
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
                .any(|anchor| anchor.binding.name == alias && anchor.pinned.is_some());
            if !pinned {
                return Err(QueryError::Validation(format!(
                    "filter on \"{property}\" for {entity}: virtual columns can only be \
                     filtered on nodes with explicit node_ids; searching by virtual \
                     column content is not yet supported"
                )));
            }
        }
        let tokens =
            matches!(&*fact.predicate, Predicate::Text { matching, .. } if matching.tokens());
        let indexed =
            matches!(fact.left(), Expr::Property { property, .. } if model.text_index(property));
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
            matches!(fact.left(), Expr::Property { property: filtered, .. } if filtered == *property)
        });
        if anchor.pinned.is_none() && !filtered {
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
