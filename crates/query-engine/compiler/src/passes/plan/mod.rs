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
pub mod remote;
pub mod rows;
mod stage;
mod traversal;

use orbit_server_config::QueryLimits;
use query_data_model::Catalog;
use query_data_model::implementations::relational::plan::RelationalPlan;
use std::collections::HashMap;

pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::logical::{Kind, Query};
use crate::passes::codegen::{SqlBackend, rejected};
use crate::tp_bounds::TpBounds;

pub use filters::BoundFilter;
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
        .physical_table(table)
        .map(|table| table.sort_key.as_slice())
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
    /// A stage that feeds Combine: its columns are exactly the contract's.
    Stage(Query<'a>, &'a crate::dag::Contract),
    Hydration(&'a HydrationRequest),
}

pub fn plan<B: SqlBackend>(
    request: Request<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &Catalog<B>,
    limits: &QueryLimits,
) -> Result<Planned<RelationalPlan<B>>> {
    let (query, contract) = match request {
        Request::Query(query) => (query, None),
        Request::Stage(query, contract) => (query, Some(contract)),
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
        Kind::Traversal => pattern(query, contract, tp_bounds, model, limits.id_range_span),
        _ if contract.is_some() => Err(QueryError::PipelineInvariant(
            "a stage that feeds Combine plans a traversal".into(),
        )),
        Kind::PathFinding => {
            let path = path_finding::PathFinding::new(query, model, limits.path_anchor_rows)?;
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
        Kind::Aggregation => pattern(query, None, tp_bounds, model, limits.id_range_span),
    }
}

/// Traversal and aggregation: the matched pattern, then the result rows built on it.
fn pattern<B: SqlBackend>(
    query: Query<'_>,
    contract: Option<&crate::dag::Contract>,
    tp_bounds: Option<&TpBounds>,
    model: &Catalog<B>,
    id_range_span: i64,
) -> Result<Planned<RelationalPlan<B>>> {
    let limit = query
        .row_limit()
        .ok_or_else(|| QueryError::PipelineInvariant("queries carry a row limit".into()))?;
    let pattern = pattern::Pattern::new(query, model, contract.is_some())?;
    let mut planned = None;
    let plan = rows::build(model, |q| {
        let matched = pattern.execution(q, tp_bounds, id_range_span)?;
        let nodes = metadata::bindings(&pattern.nodes, query, &matched.bindings)?;
        let (rows, stable_order) = match query.kind() {
            Kind::Aggregation => {
                let result = aggregation::aggregation_plan(query, &pattern.nodes, &matched)?;
                let stable_order = aggregation::emit::stable_order(&result);
                let rows = aggregation::emit::emit(q, matched, &result, limit)?;
                (rows, stable_order)
            }
            _ => {
                let shape = traversal::shape(&pattern, &matched, query, limit)?;
                let stable_order = traversal::stable_order(&pattern, &matched, query, &nodes);
                let matched = match contract {
                    Some(contract) => stage::outputs(matched, &pattern, query, contract)?,
                    None => matched,
                };
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
        .map(|hop| {
            let (source, target) = hop.stored_endpoints();
            let parts = |node: &str| {
                pattern
                    .nodes
                    .get(node)
                    .map_or(1, |node| node.identity.len())
            };
            PlannedEdge {
                variable_length: hop.max_hops > 1,
                column_prefix: hop.column_prefix(),
                rel_types: hop.rel_types.clone(),
                relationship: hop.input_index,
                parts: (parts(source), parts(target)),
            }
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
