//! Planning turns a validated query into a relational plan. Path finding, neighbors, and hydration
//! read logical plans and build through the catalog in one step.
//!
//! Traversal and aggregation still plan from `Input` into a `QueryPlan` that `lower` renders.
//! `query_types/` picks the planner for the query type. They build a `pattern` (nodes, relations,
//! conditions) whose steps assemble a `graph::RelationGraph`; `reuse` decides which relations
//! lowering materializes; `filters` and `predicates` are the shared condition vocabulary.

pub(crate) mod filters;
pub mod graph;
mod hydration;
mod neighbors;
mod node;
mod path_finding;
mod pattern;
pub mod predicates;
pub mod query_types;
pub mod reuse;

use std::collections::HashMap;

use query_data_model::OrbitQueryModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::input::*;
use crate::logical::Query;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::passes::lower::{self, LoweredMetadata, NodeBinding};
use crate::tp_bounds::TpBounds;

pub use filters::{BoundFilter, Test};
pub use pattern::{EdgeRelation, NodePlan};
pub use query_types::{aggregation::Aggregation, traversal::Traversal};

pub struct Plan<T> {
    pub nodes: HashMap<String, NodePlan>,
    pub relations: Vec<EdgeRelation>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub operation: T,
}

pub enum QueryPlan {
    Traversal(Plan<Traversal>),
    Aggregation(Plan<Aggregation>),
}

impl QueryPlan {
    pub fn relations(&self) -> &[EdgeRelation] {
        match self {
            Self::Traversal(plan) => &plan.relations,
            Self::Aggregation(plan) => &plan.relations,
        }
    }

    pub fn is_point_lookup(&self) -> bool {
        let (nodes, relations) = match self {
            Self::Traversal(Plan {
                nodes, relations, ..
            })
            | Self::Aggregation(Plan {
                nodes, relations, ..
            }) => (nodes, relations),
        };
        !nodes.is_empty()
            && nodes.values().all(NodePlan::is_id_pinned_only)
            && relations.iter().all(|relation| relation.max_hops == 1)
    }
}

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
    model: &'m (impl OrbitQueryModel + ?Sized),
    table: &str,
) -> Result<&'m [String]> {
    model
        .table_sort_key(table)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "table '{table}' has no sort key for latest-row resolution"
            ))
        })
}

pub struct Planned<P> {
    pub plan: P,
    pub metadata: LoweredMetadata,
    pub legacy: Option<QueryPlan>,
}

pub enum Request<'a> {
    Query(Query<'a>, &'a Input),
    Hydration(&'a HydrationRequest),
}

pub fn plan<M: RelationalModel>(
    request: Request<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &M,
) -> Result<Planned<PlanOf<M>>> {
    let (query, input) = match request {
        Request::Query(query, input) => (query, input),
        Request::Hydration(hydration) => {
            let arms = hydration::arms(hydration, model)?;
            let plan = model
                .catalog()
                .build(|q| hydration::emit(q, &arms))
                .map_err(rejected)?;
            return Ok(Planned {
                plan,
                metadata: LoweredMetadata::default(),
                legacy: None,
            });
        }
    };
    match query.kind() {
        QueryType::PathFinding => {
            let path = path_finding::PathFinding::new(query, model)?;
            let plan = model
                .catalog()
                .build(|q| path_finding::emit(q, &path))
                .map_err(rejected)?;
            let metadata = LoweredMetadata {
                stable_order: path_finding::stable_order(),
                ..LoweredMetadata::default()
            };
            Ok(Planned {
                plan,
                metadata,
                legacy: None,
            })
        }
        QueryType::Neighbors => {
            let neighbors = neighbors::Neighbors::new(query, model)?;
            let plan = model
                .catalog()
                .build(|q| neighbors::emit(q, &neighbors))
                .map_err(rejected)?;
            let metadata = LoweredMetadata {
                nodes: HashMap::from([(neighbors.center().to_string(), NodeBinding::Projected)]),
                stable_order: neighbors::stable_order(&neighbors),
                ..LoweredMetadata::default()
            };
            Ok(Planned {
                plan,
                metadata,
                legacy: None,
            })
        }
        _ => {
            let legacy = legacy(query, input, model)?;
            let lowered = lower::emit(&legacy, input, tp_bounds, model)?;
            Ok(Planned {
                plan: lowered.plan,
                metadata: lowered.metadata,
                legacy: Some(legacy),
            })
        }
    }
}

fn legacy<M>(query: Query<'_>, input: &Input, model: &M) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    Ok(match input.query_type {
        QueryType::Traversal => {
            QueryPlan::Traversal(query_types::traversal::plan(query, input, model)?)
        }
        QueryType::Aggregation => {
            QueryPlan::Aggregation(query_types::aggregation::plan(query, input, model)?)
        }
        QueryType::Neighbors | QueryType::PathFinding | QueryType::Hydration => {
            return Err(QueryError::PipelineInvariant(
                "neighbors, path finding, and hydration plan from logical plans".into(),
            ));
        }
    })
}
