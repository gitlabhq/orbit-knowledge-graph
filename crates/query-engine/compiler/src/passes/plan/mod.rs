//! Planning turns a validated query into a relational plan. Path finding reads the logical plan and
//! builds through the catalog in one step.
//!
//! The other query types still plan from `Input` into a `QueryPlan` that `lower` renders.
//! `query_types/` picks the planner for the query type. Traversal and aggregation build a `pattern`
//! (nodes, relations, conditions) whose steps assemble a `graph::RelationGraph`; `reuse` decides which
//! relations lowering materializes; `filters` and `predicates` are the shared condition vocabulary.
//! Neighbors and hydration keep their own hand-written shapes.

pub(crate) mod filters;
pub mod graph;
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
use crate::input::*;
use crate::logical::Query;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::passes::lower::{self, LoweredMetadata};
use crate::tp_bounds::TpBounds;

pub use filters::BoundFilter;
pub use pattern::{EdgeRelation, NodePlan};
pub use query_types::{
    EdgeTableConfig, aggregation::Aggregation, hydration::Hydration,
    hydration::HydrationCompileOptions, hydration::HydrationNodePlan, neighbors::Neighbors,
    traversal::Traversal,
};

pub struct Plan<T> {
    pub nodes: HashMap<String, NodePlan>,
    pub relations: Vec<EdgeRelation>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub operation: T,
}

impl<T> Plan<T> {
    fn new(operation: T) -> Self {
        Self {
            nodes: HashMap::new(),
            relations: Vec::new(),
            node_edge_mappings: HashMap::new(),
            denormalized: HashMap::new(),
            operation,
        }
    }
}

pub enum QueryPlan {
    Traversal(Plan<Traversal>),
    Aggregation(Plan<Aggregation>),
    Neighbors(Plan<Neighbors>),
    Hydration(Plan<Hydration>),
}

impl QueryPlan {
    pub fn relations(&self) -> &[EdgeRelation] {
        match self {
            Self::Traversal(plan) => &plan.relations,
            Self::Aggregation(plan) => &plan.relations,
            _ => &[],
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
            _ => return false,
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

pub fn plan<M: RelationalModel>(
    query: Option<Query<'_>>,
    input: &Input,
    tp_bounds: Option<&TpBounds>,
    model: &M,
    hydration_options: HydrationCompileOptions,
) -> Result<Planned<PlanOf<M>>> {
    if let Some(query) = query
        && query.kind() == QueryType::PathFinding
    {
        let path = path_finding::PathFinding::new(query, model)?;
        let plan = model
            .catalog()
            .build(|q| path_finding::emit(q, &path))
            .map_err(rejected)?;
        let metadata = LoweredMetadata {
            stable_order: path_finding::stable_order(),
            ..LoweredMetadata::default()
        };
        return Ok(Planned {
            plan,
            metadata,
            legacy: None,
        });
    }
    let legacy = legacy(input, model, hydration_options)?;
    let lowered = lower::emit(&legacy, input, tp_bounds, model)?;
    Ok(Planned {
        plan: lowered.plan,
        metadata: lowered.metadata,
        legacy: Some(legacy),
    })
}

fn legacy<M>(
    input: &Input,
    model: &M,
    hydration_options: HydrationCompileOptions,
) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    Ok(match input.query_type {
        QueryType::Traversal => QueryPlan::Traversal(query_types::traversal::plan(input, model)?),
        QueryType::Aggregation => {
            QueryPlan::Aggregation(query_types::aggregation::plan(input, model)?)
        }
        QueryType::Neighbors => QueryPlan::Neighbors(query_types::neighbors::plan(input, model)?),
        QueryType::PathFinding => {
            return Err(QueryError::PipelineInvariant(
                "path finding plans from the logical plan".into(),
            ));
        }
        QueryType::Hydration => QueryPlan::Hydration(query_types::hydration::plan(
            input,
            model,
            hydration_options,
        )?),
    })
}
