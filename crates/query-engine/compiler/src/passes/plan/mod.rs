//! Planning turns a validated query into a `QueryPlan`: the backend-neutral reads, joins, and
//! outputs that lowering renders as SQL.
//!
//! `query_types/` picks the planner for the query type. Traversal and aggregation build a `pattern`
//! (nodes, relations, conditions) whose steps assemble a `graph::RelationGraph`; `reuse` decides which
//! relations lowering materializes; `filters` and `predicates` are the shared condition vocabulary.
//! Neighbors, path finding, and hydration keep their own hand-written shapes.

pub(crate) mod filters;
pub mod graph;
mod pattern;
pub mod predicates;
pub mod query_types;
pub mod reuse;

use std::collections::HashMap;

use query_data_model::OrbitQueryModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::input::*;

pub use filters::BoundFilter;
pub use pattern::{EdgeRelation, NodePlan};
pub use query_types::{
    EdgeTableConfig, aggregation::Aggregation, hydration::Hydration,
    hydration::HydrationCompileOptions, hydration::HydrationNodePlan, neighbors::Neighbors,
    pathfinding::PathFinding, traversal::Traversal,
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
    PathFinding(Plan<PathFinding>),
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

pub fn plan_clickhouse(
    input: &Input,
    model: &query_data_model::ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
) -> Result<QueryPlan> {
    plan(input, model, hydration_options)
}

pub fn plan_duckdb(
    input: &Input,
    model: &query_data_model::DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
) -> Result<QueryPlan> {
    plan(input, model, hydration_options)
}

fn plan<M>(
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
            QueryPlan::PathFinding(query_types::pathfinding::plan(input, model)?)
        }
        QueryType::Hydration => QueryPlan::Hydration(query_types::hydration::plan(
            input,
            model,
            hydration_options,
        )?),
    })
}
