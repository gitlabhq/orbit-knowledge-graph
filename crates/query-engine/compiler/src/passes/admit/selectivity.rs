use query_data_model::plan::logical::LogicalOperation;
use query_data_model::plan::tracking::Ref;

use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Leaf;
use crate::logical::Query;
use crate::logical::anchors::Anchor;
use crate::query_limits::{MAX_ID_RANGE_SPAN, MAX_PATH_ANCHOR_RANGE};

pub(super) fn selectivity(
    query: Query<'_>,
    facts: &[Leaf<'_>],
    anchors: &[Anchor<'_>],
) -> Result<()> {
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
                if anchor(endpoint).is_none_or(|anchor| !anchored(anchor)) {
                    return Err(QueryError::Validation(format!(
                        "path_finding requires node_ids, filters, or id_range \
                         (max span {MAX_PATH_ANCHOR_RANGE}) on endpoint \"{}\"",
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
            if center.is_none_or(|center| !selective(center)) {
                let hint = hint(center.is_some_and(wide));
                return Err(QueryError::Validation(format!(
                    "neighbors requires filters or node_ids on the center node \
                     to avoid scanning all edges{hint}"
                )));
            }
        }
        _ if !anchors.iter().any(selective) => {
            let hint = hint(anchors.iter().any(wide));
            let negation = match facts.iter().any(|fact| fact.negated) {
                true => " (NOT conditions do not count)",
                false => "",
            };
            return Err(QueryError::Validation(format!(
                "add filters or node_ids to at least one node, such as \
                 (p:Project {{full_path: 'group/project'}}) or WHERE mr.project_id = \
                 42{hint}{negation}"
            )));
        }
        _ => {}
    }
    Ok(())
}

fn span(anchor: &Anchor<'_>) -> Option<i64> {
    anchor.range.map(|(start, end)| end.saturating_sub(start))
}

fn anchored(anchor: &Anchor<'_>) -> bool {
    anchor.pinned.is_some()
        || !anchor.filters.is_empty()
        || span(anchor).is_some_and(|span| span <= MAX_PATH_ANCHOR_RANGE)
}

fn selective(anchor: &Anchor<'_>) -> bool {
    anchor.pinned.is_some()
        || anchor.filters.iter().any(|fact| fact.right().is_none())
        || span(anchor).is_some_and(|span| span <= MAX_ID_RANGE_SPAN)
}

fn wide(anchor: &Anchor<'_>) -> bool {
    span(anchor).is_some_and(|span| span > MAX_ID_RANGE_SPAN)
}

fn hint(wide: bool) -> String {
    match wide {
        true => format!(" (an id_range counts only with a span of at most {MAX_ID_RANGE_SPAN})"),
        false => String::new(),
    }
}
