//! One Plan struct with a PlanBody enum. Common fields live on Plan;
//! query-type-specific data lives in the body variant. The Rust enum
//! enforces that emit functions only access their own variant's data.

pub mod edge_chain;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;

use std::collections::{HashMap, HashSet};

use crate::error::{QueryError, Result};
use crate::input::*;

pub use edge_chain::{
    FkShape, Hop, HopFk, HydrationStrategy, JoinColumns, NodePlan, Selectivity, Strategy,
};
pub use hydration::{HydrationCompileOptions, HydrationNodePlan};
use query_data_model::QueryDataModel;

#[derive(Clone)]
pub struct BoundFilter {
    pub filter: InputFilter,
    pub data_type: Option<ontology::DataType>,
    pub selectivity: ontology::FieldSelectivity,
}

/// Pipeline state compatibility alias (HasQueryPlan, take_query_plan, etc.).
pub type QueryPlan = Plan;

pub struct Plan {
    pub nodes: HashMap<String, NodePlan>,
    pub hops: Vec<Hop>,
    pub strategy: Strategy,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    /// Relationship kinds whose edge writes each denorm tag, keyed like
    /// `denorm_columns`.
    pub denorm_rel_kinds: HashMap<(String, String, String), Vec<String>>,
    /// Per-table column sets from the ontology. Used by the lowerer to
    /// push node-level filters (e.g. project_id, branch) down to edge
    /// scans when the edge table has those columns.
    pub table_columns: HashMap<String, HashSet<String>>,
    /// ORDER BY columns per table. Used by the lowerer for LIMIT BY dedup.
    pub table_sort_keys: HashMap<String, Vec<String>>,
    pub scope_requirements: Vec<crate::scope::ScopeProof>,
    pub body: PlanBody,
}

impl Plan {
    pub fn node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        self.node_edge_mappings.clone()
    }
}

pub enum PlanBody {
    Traversal,
    Aggregation {
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
                source_kinds.extend(route.sources.into_iter().map(String::from));
                target_kinds.extend(route.targets.into_iter().map(String::from));
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
    scope_proofs: &HashMap<String, crate::scope::ScopeProof>,
    model: &query_data_model::ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
) -> Result<Plan> {
    plan(input, scope_proofs, model, hydration_options, true)
}

pub fn plan_duckdb(
    input: &Input,
    scope_proofs: &HashMap<String, crate::scope::ScopeProof>,
    model: &query_data_model::DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
) -> Result<Plan> {
    plan(input, scope_proofs, model, hydration_options, false)
}

fn plan<M>(
    input: &Input,
    scope_proofs: &HashMap<String, crate::scope::ScopeProof>,
    model: &M,
    hydration_options: HydrationCompileOptions,
    use_fk_elision: bool,
) -> Result<Plan>
where
    M: QueryDataModel + ?Sized,
{
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            Ok(edge_chain::plan(input, scope_proofs, model, use_fk_elision))
        }
        QueryType::Neighbors => neighbors::plan_neighbors(input, model),
        QueryType::PathFinding => pathfinding::plan_pathfinding(input, model),
        QueryType::Hydration => hydration::plan_hydration(input, model, hydration_options),
    }
}
