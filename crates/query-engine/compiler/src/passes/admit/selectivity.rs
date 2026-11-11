use orbit_server_config::QueryLimits;
use query_data_model::plan::logical::LogicalOperation;
use query_data_model::plan::tracking::Ref;

use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Leaf;
use crate::logical::Query;
use crate::logical::anchors::Anchor;

pub(super) fn selectivity(
    query: Query<'_>,
    facts: &[Leaf<'_>],
    anchors: &[Anchor<'_>],
    limits: &QueryLimits,
) -> Result<()> {
    let (span, path_span) = (limits.id_range_span, limits.path_anchor_span);
    let anchor = |binding: Ref| {
        anchors
            .iter()
            .find(|anchor| anchor.binding.output == binding)
    };
    let operations = || query.operations().map(|(_, operation)| operation);
    match query.kind() {
        Kind::PathFinding => {
            let ends = operations().find_map(|operation| match operation {
                LogicalOperation::Shortest { from, to, .. } => Some([*from, *to]),
                _ => None,
            });
            for endpoint in ends.into_iter().flatten() {
                if anchor(endpoint).is_none_or(|anchor| !anchored(anchor, path_span)) {
                    return Err(QueryError::Validation(format!(
                        "path_finding requires node_ids, filters, or id_range \
                         (max span {path_span}) on endpoint \"{}\"",
                        query.name(endpoint)
                    )));
                }
            }
        }
        Kind::Neighbors => {
            let center = operations().find_map(|operation| match operation {
                LogicalOperation::Neighbors { from, .. } => anchor(*from),
                _ => None,
            });
            if center.is_none_or(|center| !selective(center, span)) {
                let hint = hint(center.is_some_and(|center| wide(center, span)), span);
                return Err(QueryError::Validation(format!(
                    "neighbors requires filters or node_ids on the center node \
                     to avoid scanning all edges{hint}"
                )));
            }
        }
        _ if !anchors.iter().any(|anchor| selective(anchor, span)) => {
            let hint = hint(anchors.iter().any(|anchor| wide(anchor, span)), span);
            let negation = match facts.iter().any(|fact| fact.negated) {
                true => " (NOT conditions do not count)",
                false => "",
            };
            let example = anchors.first().map_or(String::new(), |anchor| {
                format!(
                    ", such as WHERE {}.id = 42",
                    query.name(anchor.binding.output)
                )
            });
            return Err(QueryError::Validation(format!(
                "add filters or node_ids to at least one node{example}{hint}{negation}"
            )));
        }
        _ => {}
    }
    Ok(())
}

fn span(anchor: &Anchor<'_>) -> Option<i64> {
    anchor.range.map(|(start, end)| end.saturating_sub(start))
}

fn anchored(anchor: &Anchor<'_>, max: i64) -> bool {
    anchor.pinned.is_some()
        || !anchor.filters.is_empty()
        || span(anchor).is_some_and(|span| span <= max)
}

fn selective(anchor: &Anchor<'_>, max: i64) -> bool {
    anchor.pinned.is_some()
        || anchor.filters.iter().any(|fact| fact.right().is_none())
        || span(anchor).is_some_and(|span| span <= max)
}

fn wide(anchor: &Anchor<'_>, max: i64) -> bool {
    span(anchor).is_some_and(|span| span > max)
}

fn hint(wide: bool, max: i64) -> String {
    match wide {
        true => format!(" (an id_range counts only with a span of at most {max})"),
        false => String::new(),
    }
}
