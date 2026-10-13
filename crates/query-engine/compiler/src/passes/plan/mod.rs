pub mod aggregation;
mod context;
pub mod edge_chain;
pub(crate) mod edge_predicates;
pub mod fk;
mod flat;
pub(crate) mod helpers;
mod hops;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod physical;
pub mod requirements;

use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::query::{Direction, Predicate, Query, QueryType};

pub use edge_chain::{Hop, HopFk, HydrationStrategy, JoinColumns, NodePlan, Selectivity};
pub use hydration::{HydrationCompileOptions, HydrationNodePlan};
use query_data_model::OrbitQueryModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

#[derive(Clone)]
pub struct PlannedFilter {
    pub predicate: Predicate,
    pub property: Option<query_data_model::PropertyId>,
    pub data_type: Option<ontology::DataType>,
    pub selectivity: ontology::FieldSelectivity,
    pub in_sort_key: bool,
}

impl From<&crate::query::Filter> for PlannedFilter {
    fn from(filter: &crate::query::Filter) -> Self {
        Self {
            predicate: filter.predicate.clone(),
            property: filter.property,
            data_type: Some(filter.data_type),
            selectivity: filter.selectivity,
            in_sort_key: filter.in_sort_key,
        }
    }
}

pub struct Plan<T> {
    pub nodes: HashMap<String, NodePlan>,
    pub hops: Vec<Hop>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub operation: T,
}

pub enum QueryPlan {
    Traversal(Plan<Traversal>),
    Aggregation(Plan<Aggregation>),
    Neighbors(Plan<Neighbors>),
    PathFinding(Plan<PathFinding>),
    Hydration(Plan<Hydration>),
}

impl QueryPlan {
    pub fn hops(&self) -> &[Hop] {
        match self {
            Self::Traversal(plan) => &plan.hops,
            Self::Aggregation(plan) => &plan.hops,
            _ => &[],
        }
    }
}

pub fn denormalized_facts(
    query: &Query,
    model: &(impl OrbitQueryModel + ?Sized),
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    query
        .nodes
        .iter()
        .flat_map(|node| {
            [DenormalizedDirection::Source, DenormalizedDirection::Target]
                .into_iter()
                .flat_map(move |direction| {
                    node.filters.iter().filter_map(move |filter| {
                        let property = filter.property?;
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

pub struct Traversal {
    pub execution: physical::ExecutionPlan,
}

pub struct Aggregation {
    pub execution: physical::ExecutionPlan,
    pub result: aggregation::AggregationPlan,
}

pub struct Neighbors {
    pub center: String,
    pub direction: Direction,
    pub edge: EdgeTableConfig,
    pub has_non_denorm: bool,
    pub fused_table: Option<String>,
    pub center_tp_lookup: Option<(String, String)>,
}

pub struct Hydration {
    pub nodes: Vec<HydrationNodePlan>,
}

pub struct PathFinding {
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
    pub fn from_model(model: &(impl OrbitQueryModel + ?Sized), rel_types: &[String]) -> Self {
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

pub fn find_node(query: &Query, node: crate::query::NodeId) -> &crate::query::Node {
    query.node(node)
}

pub fn plan_clickhouse(
    query: &Query,
    model: &query_data_model::ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan> {
    plan(query, model, hydration_options, true, table_scans)
}

pub fn plan_duckdb(
    query: &Query,
    model: &query_data_model::DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan> {
    plan(query, model, hydration_options, false, table_scans)
}

fn plan<M>(
    query: &Query,
    model: &M,
    hydration_options: HydrationCompileOptions,
    use_fk_elision: bool,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let context = context::PlanningContext {
        query,
        model,
        nodes: HashMap::new(),
        hops: Vec::new(),
        denormalized: HashMap::new(),
        node_edge_mappings: HashMap::new(),
    };
    match query.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            edge_chain::plan(context, use_fk_elision, table_scans)
        }
        QueryType::Neighbors => neighbors::plan_neighbors(context).map(QueryPlan::Neighbors),
        QueryType::PathFinding => {
            pathfinding::plan_pathfinding(context).map(QueryPlan::PathFinding)
        }
        QueryType::Hydration => {
            hydration::plan_hydration(context, hydration_options).map(QueryPlan::Hydration)
        }
    }
}
