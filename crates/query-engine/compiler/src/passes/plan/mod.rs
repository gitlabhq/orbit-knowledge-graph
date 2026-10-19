pub mod aggregation;
pub(crate) mod filters;
pub mod graph;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
mod pattern;
pub mod predicates;
pub mod reuse;

use std::collections::HashMap;

use query_data_model::OrbitQueryModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::input::*;

pub use filters::BoundFilter;
pub use hydration::{HydrationCompileOptions, HydrationNodePlan};
pub use pattern::{EdgeRelation, NodePlan};

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

pub struct Traversal {
    pub execution: graph::ExecutionPlan,
}

pub struct Aggregation {
    pub execution: graph::ExecutionPlan,
    pub result: aggregation::AggregationPlan,
}

pub struct Neighbors {
    pub center: String,
    pub direction: Direction,
    pub edge: EdgeTableConfig,
    pub has_non_denorm: bool,
    pub fused_table: Option<String>,
    pub outgoing_kinds: Vec<String>,
    pub incoming_kinds: Vec<String>,
    pub incoming_scope_preserving: Vec<String>,
    pub incoming_cross_scope: Vec<String>,
    pub outgoing_neighbor_paths: Vec<(String, String)>,
    pub incoming_neighbor_paths: Vec<(String, String)>,
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
    /// Entity kinds the relationship types allow at each end; empty for a wildcard.
    pub source_kinds: Vec<String>,
    pub target_kinds: Vec<String>,
    /// Tables a neighbors arm scans per direction; `neighbors::plan` narrows them so empty arms are skipped.
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

pub fn denormalized_facts(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    input
        .nodes
        .iter()
        .filter_map(|node| Some((model.graph().entity_id(node.entity.as_deref()?)?, node)))
        .flat_map(|(entity, node)| {
            [DenormalizedDirection::Source, DenormalizedDirection::Target]
                .into_iter()
                .flat_map(move |direction| {
                    node.filters
                        .keys()
                        .map(String::as_str)
                        .chain(
                            input
                                .conditions()
                                .filter(|condition| {
                                    condition.target == PredicateTarget::Node(node.id.clone())
                                })
                                .map(|condition| condition.property.as_str()),
                        )
                        .filter_map(move |property| {
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
        QueryType::Traversal => QueryPlan::Traversal(
            pattern::plan(input, model)?.finish(|_, execution| Traversal { execution }),
        ),
        QueryType::Aggregation => {
            QueryPlan::Aggregation(pattern::plan(input, model)?.finish(|pattern, execution| {
                Aggregation {
                    result: aggregation::plan(input, &pattern.nodes, &execution),
                    execution,
                }
            }))
        }
        QueryType::Neighbors => QueryPlan::Neighbors(neighbors::plan(input, model)?),
        QueryType::PathFinding => QueryPlan::PathFinding(pathfinding::plan(input, model)?),
        QueryType::Hydration => {
            QueryPlan::Hydration(hydration::plan(input, model, hydration_options)?)
        }
    })
}
