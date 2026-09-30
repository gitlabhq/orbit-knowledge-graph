//! One Plan struct with a PlanBody enum. Common fields live on Plan;
//! query-type-specific data lives in the body variant. The Rust enum
//! enforces that emit functions only access their own variant's data.

mod cascade;
pub mod edge_chain;
pub(crate) mod edge_predicates;
pub mod fk;
mod flat;
mod hops;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod physical;

use std::collections::{HashMap, HashSet};

use crate::error::{QueryError, Result};
use crate::input::*;

pub use edge_chain::{
    FkShape, Hop, HopFk, HydrationStrategy, JoinColumns, NodePlan, Selectivity, Strategy,
};
pub use hydration::{HydrationCompileOptions, HydrationNodePlan};
use query_data_model::QueryDataModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

#[derive(Clone)]
pub struct BoundFilter {
    pub filter: InputFilter,
    pub property: Option<query_data_model::PropertyId>,
    pub data_type: Option<ontology::DataType>,
    pub selectivity: ontology::FieldSelectivity,
}

/// Pipeline state compatibility alias (HasQueryPlan, take_query_plan, etc.).
pub type QueryPlan = Plan;

pub struct Plan {
    pub nodes: HashMap<String, NodePlan>,
    pub hops: Vec<Hop>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub body: PlanBody,
}

pub fn denormalized_facts(
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    input
        .nodes
        .iter()
        .filter_map(|node| Some((model.graph().entity_id(node.entity.as_deref()?)?, node)))
        .flat_map(|(entity, node)| {
            [DenormalizedDirection::Source, DenormalizedDirection::Target]
                .into_iter()
                .flat_map(move |direction| {
                    node.filters.keys().filter_map(move |property| {
                        let property = model.graph().property_id(entity, property)?;
                        let key = DenormalizedKey {
                            property,
                            direction,
                        };
                        model
                            .denormalized()
                            .property(key)
                            .cloned()
                            .map(|facts| (key, facts))
                    })
                })
        })
        .collect()
}

pub enum PlanBody {
    Traversal {
        strategy: Strategy,
    },
    Aggregation {
        strategy: Strategy,
        aggregations: Vec<InputAggregationMetric>,
        agg_sort: Option<InputAggSort>,
    },
    Neighbors {
        center: String,
        direction: Direction,
        edge: EdgeTableConfig,
        has_non_denorm: bool,
        /// (tp source table, key column) when the center is a namespace entity;
        /// lets the anchor arm pin to the centers' exact traversal_paths.
        center_tp_lookup: Option<(String, String)>,
    },
    PathFinding(PathFindingBody),
    Hydration {
        nodes: Vec<HydrationNodePlan>,
        options: HydrationCompileOptions,
    },
}

pub struct PathFindingBody {
    pub start: String,
    pub end: String,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub edge: EdgeTableConfig,
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub scoped_by_tp: bool,
}

pub struct EdgeTableConfig {
    pub tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
    /// Valid source entity kinds for the rel_types (union across all types).
    /// Empty when rel_types is unset. Used by pathfinding to add kind
    /// predicates on intermediate hops for granule pruning.
    pub source_kinds: Vec<String>,
    pub target_kinds: Vec<String>,
    /// Physical tables a neighbors arm must scan per direction (a center is the
    /// edge source when outgoing, the target when incoming). Defaults to all
    /// `tables`; `plan_neighbors` narrows them so empty arms aren't scanned.
    pub outgoing_tables: Vec<String>,
    pub incoming_tables: Vec<String>,
}

impl EdgeTableConfig {
    pub fn from_model(model: &(impl QueryDataModel + ?Sized), rel_types: &[String]) -> Self {
        use std::collections::BTreeSet;
        let mut source_kinds = BTreeSet::new();
        let mut target_kinds = BTreeSet::new();
        for rt in rel_types {
            if let Some(route) = model.relationship_route(rt) {
                source_kinds.extend(
                    route
                        .source_entities()
                        .map(|entity| model.graph().entity(entity).name.clone()),
                );
                target_kinds.extend(
                    route
                        .target_entities()
                        .map(|entity| model.graph().entity(entity).name.clone()),
                );
            }
        }
        let tables = model.relationship_tables(rel_types);
        Self {
            rel_type_filter: if rel_types.is_empty() {
                None
            } else {
                Some(rel_types.to_vec())
            },
            source_kinds: source_kinds.into_iter().collect(),
            target_kinds: target_kinds.into_iter().collect(),
            outgoing_tables: tables.clone(),
            incoming_tables: tables.clone(),
            tables,
        }
    }
}

pub fn find_node<'a>(input: &'a Input, alias: &str) -> Result<&'a InputNode> {
    input
        .nodes
        .iter()
        .find(|n| n.id == alias)
        .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
}

pub fn plan_clickhouse(
    input: &Input,
    model: &query_data_model::ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<Plan> {
    plan(input, model, hydration_options, true, table_scans)
}

pub fn plan_duckdb(
    input: &Input,
    model: &query_data_model::DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<Plan> {
    plan(input, model, hydration_options, false, table_scans)
}

fn plan<M>(
    input: &Input,
    model: &M,
    hydration_options: HydrationCompileOptions,
    use_fk_elision: bool,
    table_scans: &HashSet<String>,
) -> Result<Plan>
where
    M: QueryDataModel + ?Sized,
{
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            edge_chain::plan(input, model, use_fk_elision, table_scans)
        }
        QueryType::Neighbors => neighbors::plan_neighbors(input, model),
        QueryType::PathFinding => pathfinding::plan_pathfinding(input, model),
        QueryType::Hydration => hydration::plan_hydration(input, model, hydration_options),
    }
}
