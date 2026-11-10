//! Planning turns a validated query into a relational plan. Path finding, neighbors, and hydration
//! read logical plans and build through the catalog in one step.
//!
//! Traversal and aggregation plan a `pattern` (nodes, relations, conditions) whose steps build the
//! joined rows through `rows`, sharing what later steps read again; `filters` and `predicates` are
//! the shared condition vocabulary.

pub mod aggregation;
pub(crate) mod build;
mod current;
pub(crate) mod filters;
mod hydration;
mod metadata;
mod neighbors;
mod node;
mod path_finding;
pub(crate) mod pattern;
pub mod predicates;
pub mod rows;
mod traversal;

use query_data_model::Catalog;
use query_data_model::implementations::relational::plan::RelationalPlan;
use std::collections::HashMap;

pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::logical::{Kind, Query};
use crate::passes::codegen::{SqlBackend, rejected};
use crate::tp_bounds::TpBounds;

pub use filters::{BoundFilter, Test};
pub use metadata::{NodeBinding, PlanMetadata, PlannedEdge, Term};
pub use pattern::{EdgeRelation, Matched, NodePlan};

pub(crate) fn property_value(
    nodes: &HashMap<String, NodePlan>,
    node: &str,
    column: predicates::Column,
) -> predicates::OutputValue {
    match nodes.get(node) {
        Some(plan) => predicates::OutputValue::property(&plan.null_as_epoch, column),
        None => predicates::OutputValue::Column(column),
    }
}

pub(crate) fn sort_key<'m>(
    model: &'m Catalog<impl SqlBackend>,
    table: &str,
) -> Result<&'m [String]> {
    model
        .table_facts(table)
        .map(|facts| facts.sort_key.as_slice())
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "table '{table}' has no sort key for latest-row resolution"
            ))
        })
}

pub struct Planned<P> {
    pub plan: P,
    pub metadata: PlanMetadata,
}

pub enum Request<'a> {
    Query(Query<'a>),
    Hydration(&'a HydrationRequest),
}

pub fn plan<B: SqlBackend>(
    request: Request<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &Catalog<B>,
) -> Result<Planned<RelationalPlan<B>>> {
    let query = match request {
        Request::Query(query) => query,
        Request::Hydration(hydration) => {
            let arms = hydration::arms(hydration, model)?;
            let plan = model
                .build(|q| hydration::emit(q, &arms))
                .map_err(rejected)?;
            return Ok(Planned {
                plan,
                metadata: PlanMetadata::default(),
            });
        }
    };
    match query.kind() {
        Kind::PathFinding => {
            let path = path_finding::PathFinding::new(query, model)?;
            let plan = model
                .build(|q| path_finding::emit(q, &path))
                .map_err(rejected)?;
            let metadata = PlanMetadata {
                stable_order: path_finding::stable_order(),
                ..PlanMetadata::default()
            };
            Ok(Planned { plan, metadata })
        }
        Kind::Neighbors => {
            let neighbors = neighbors::Neighbors::new(query, model)?;
            let plan = model
                .build(|q| neighbors::emit(q, &neighbors))
                .map_err(rejected)?;
            let metadata = PlanMetadata {
                nodes: HashMap::from([(neighbors.center().to_string(), NodeBinding::Projected)]),
                stable_order: neighbors::stable_order(&neighbors),
                ..PlanMetadata::default()
            };
            Ok(Planned { plan, metadata })
        }
        Kind::Traversal | Kind::Aggregation => pattern(query, tp_bounds, model),
    }
}

/// Traversal and aggregation: the matched pattern, then the result rows built on it.
fn pattern<B: SqlBackend>(
    query: Query<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &Catalog<B>,
) -> Result<Planned<RelationalPlan<B>>> {
    let limit = query
        .row_limit()
        .ok_or_else(|| QueryError::PipelineInvariant("queries carry a row limit".into()))?;
    let pattern = pattern::Pattern::new(query, model)?;
    let mut planned = None;
    let plan = rows::build(model, |q| {
        let matched = pattern.execution(q, tp_bounds)?;
        let nodes = metadata::bindings(&pattern.nodes, query, &matched.bindings)?;
        let (rows, stable_order) = match query.kind() {
            Kind::Aggregation => {
                let result = aggregation::aggregation_plan(query, &pattern.nodes, &matched);
                let stable_order = aggregation::emit::stable_order(&result);
                let rows = aggregation::emit::emit(q, matched, &result, limit)?;
                (rows, stable_order)
            }
            _ => {
                let shape = traversal::shape(&pattern, &matched, query, limit)?;
                let stable_order = traversal::stable_order(&pattern, &matched, query, &nodes);
                let rows = traversal::emit(q, matched, &shape)?;
                (rows, stable_order)
            }
        };
        planned = Some((nodes, stable_order));
        Ok(rows)
    })?;
    let (nodes, stable_order) =
        planned.ok_or_else(|| QueryError::PipelineInvariant("a built plan has metadata".into()))?;
    let edges = pattern
        .relations
        .iter()
        .map(|hop| PlannedEdge {
            path_column: (hop.max_hops > 1).then(|| hop.path_nodes_column()),
            column_prefix: hop.column_prefix(),
            rel_types: hop.rel_types.clone(),
            relationship: hop.input_index,
        })
        .collect();
    let point_lookup = !pattern.nodes.is_empty()
        && pattern.nodes.values().all(NodePlan::is_id_pinned_only)
        && pattern
            .relations
            .iter()
            .all(|relation| relation.max_hops == 1);
    Ok(Planned {
        plan,
        metadata: PlanMetadata {
            nodes,
            edges,
            stable_order,
            point_lookup,
        },
    })
}
