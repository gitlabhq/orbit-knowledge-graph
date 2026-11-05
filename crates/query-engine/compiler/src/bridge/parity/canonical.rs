use std::collections::BTreeMap;
use std::fmt::Write;

use query_data_model::plan::logical::{ErrorClass, PlanError};
use query_data_model::surface::SurfaceError;

use crate::input::{Direction, FilterOp, InputFilter};
use crate::{Input, QueryError};

pub(super) fn old_class(error: &QueryError) -> &'static str {
    match error {
        QueryError::Parse(_) => "parse",
        QueryError::Validation(_) => "validation",
        QueryError::ReferenceError(_) => "reference",
        QueryError::AllowlistRejected(_) => "allowlist",
        QueryError::LimitExceeded(_) => "limit",
        QueryError::DepthExceeded(_) => "depth",
        QueryError::PipelineInvariant(_) => "invariant",
        _ => "other",
    }
}

pub(super) fn new_class(error: &SurfaceError) -> &'static str {
    match error {
        SurfaceError::Syntax(_) => "parse",
        SurfaceError::Schema(_) => "validation",
        SurfaceError::Limit(_) => "limit",
        SurfaceError::Plan(PlanError::Hidden(_) | PlanError::AdminOnly(_)) => "allowlist",
        SurfaceError::Plan(error) => match error.class() {
            ErrorClass::Name => "allowlist",
            ErrorClass::Reference => "reference",
            ErrorClass::Construction => "invariant",
            _ => "validation",
        },
    }
}

fn filters<'f>(
    filters: impl Iterator<Item = (&'f String, &'f Vec<InputFilter>)>,
) -> BTreeMap<&'f String, Vec<String>> {
    filters
        .map(|(property, entries)| {
            let entries = entries
                .iter()
                .map(|filter| {
                    format!(
                        "{:?} {:?} {:?} {}",
                        filter.op.unwrap_or(FilterOp::Eq),
                        filter.value,
                        filter.rhs_column,
                        filter.from_temporal_literal
                    )
                })
                .collect();
            (property, entries)
        })
        .collect()
}

fn types(types: &[String]) -> Vec<String> {
    match types {
        [] => vec!["*".to_string()],
        types => types.to_vec(),
    }
}

pub(super) fn canonical(input: &Input) -> String {
    let mut out = String::new();
    let options = &input.options;
    let _ = writeln!(
        out,
        "{} limit {} skip {:?} order {:?} dynamic {:?} debug {}",
        input.query_type,
        input.limit,
        input.skip,
        input.order_by,
        options.dynamic_columns,
        options.include_debug_sql
    );
    for node in &input.nodes {
        let range = node.id_range.as_ref().map(|range| (range.start, range.end));
        let _ = writeln!(
            out,
            "node {} {:?} columns {:?} ids {:?} range {range:?} key {} filters {:?}",
            node.id,
            node.entity,
            node.columns,
            node.node_ids,
            node.id_property,
            filters(node.filters.iter())
        );
    }
    for edge in &input.relationships {
        let (from, to, direction) = match edge.direction {
            Direction::Incoming => (&edge.to, &edge.from, Direction::Outgoing),
            Direction::Both if edge.to < edge.from => (&edge.to, &edge.from, Direction::Both),
            direction => (&edge.from, &edge.to, direction),
        };
        let _ = writeln!(
            out,
            "edge {from} {to} {direction:?} {:?} {:?} filters {:?}",
            types(&edge.types),
            edge.hops,
            filters(edge.filters.iter())
        );
    }
    let aggregation = &input.aggregation;
    for metric in &aggregation.metrics {
        let _ = writeln!(
            out,
            "metric {} {:?} {}",
            metric.output_name(),
            metric.expr,
            metric.distinct
        );
    }
    for group in &aggregation.group_by {
        let _ = writeln!(out, "group {} {group:?}", group.output_name());
    }
    if let Some(path) = &input.path {
        let _ = writeln!(
            out,
            "path {} {} {} {:?}",
            path.from,
            path.to,
            path.max_depth,
            types(&path.rel_types)
        );
    }
    if let Some(neighbors) = &input.neighbors {
        let _ = writeln!(
            out,
            "neighbors {:?} {:?}",
            neighbors.direction,
            types(&neighbors.rel_types)
        );
    }
    let _ = writeln!(
        out,
        "sort {:?} predicates {:?} returns {:?}",
        aggregation.sort, input.predicates, input.relationship_return
    );
    out
}
