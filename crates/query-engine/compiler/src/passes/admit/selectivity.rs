use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};
use crate::query_limits::{MAX_ID_RANGE_SPAN, MAX_PATH_ANCHOR_RANGE};

pub(super) fn selectivity(input: &Input) -> Result<()> {
    match input.query_type {
        QueryType::PathFinding => {
            if let Some(path) = &input.path {
                for endpoint in [&path.from, &path.to] {
                    if input.node(endpoint).is_none_or(|n| !anchored(n)) {
                        return Err(QueryError::Validation(format!(
                            "path_finding requires node_ids, filters, or id_range \
                             (max span {MAX_PATH_ANCHOR_RANGE}) on endpoint \"{endpoint}\""
                        )));
                    }
                }
            }
        }
        QueryType::Neighbors if input.nodes.first().is_none_or(|n| !selective(n)) => {
            let hint = hint(input.nodes.first().is_some_and(wide));
            return Err(QueryError::Validation(format!(
                "neighbors requires filters or node_ids on the center node \
                 to avoid scanning all edges{hint}"
            )));
        }
        QueryType::Traversal | QueryType::Aggregation if !input.nodes.iter().any(selective) => {
            let hint = hint(input.nodes.iter().any(wide));
            let negation = match input.has_negation() {
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

fn anchored(node: &InputNode) -> bool {
    if !node.node_ids.is_empty() || !node.filters.is_empty() {
        return true;
    }
    node.id_range
        .as_ref()
        .is_some_and(|range| range.end.saturating_sub(range.start) <= MAX_PATH_ANCHOR_RANGE)
}

fn selective(node: &InputNode) -> bool {
    if !node.node_ids.is_empty() {
        return true;
    }
    if node
        .filters
        .values()
        .flatten()
        .any(|f| f.rhs_column.is_none())
    {
        return true;
    }
    node.id_range
        .as_ref()
        .is_some_and(|range| range.end.saturating_sub(range.start) <= MAX_ID_RANGE_SPAN)
}

fn wide(node: &InputNode) -> bool {
    node.id_range
        .as_ref()
        .is_some_and(|range| range.end.saturating_sub(range.start) > MAX_ID_RANGE_SPAN)
}

fn hint(wide: bool) -> String {
    match wide {
        true => format!(" (an id_range counts only with a span of at most {MAX_ID_RANGE_SPAN})"),
        false => String::new(),
    }
}
