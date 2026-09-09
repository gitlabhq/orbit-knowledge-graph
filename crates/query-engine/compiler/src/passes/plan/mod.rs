//! One Plan struct with a PlanBody enum. Common fields live on Plan;
//! query-type-specific data lives in the body variant. The Rust enum
//! enforces that emit functions only access their own variant's data.

pub mod edge_chain;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;

use std::collections::{HashMap, HashSet};

use ontology::{DataType, FieldSource, Ontology};

use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::shared::requested_columns;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const TEXT_EXCERPT_BUDGET_BYTES: u64 = WORKHORSE_GRPC_MESSAGE_CAP_BYTES / 4;
const MIN_TEXT_EXCERPT_CHARACTERS: u64 = 256;

pub use edge_chain::{
    FkShape, Hop, HopFk, HydrationStrategy, JoinColumns, NodePlan, Selectivity, Strategy,
};
pub use hydration::HydrationNodePlan;

/// Pipeline state compatibility alias (HasQueryPlan, take_query_plan, etc.).
pub type QueryPlan = Plan;

pub struct Plan {
    pub nodes: HashMap<String, NodePlan>,
    pub hops: Vec<Hop>,
    pub strategy: Strategy,
    pub limit: u32,
    pub order_by: Option<InputOrderBy>,
    pub cursor: Option<InputCursor>,
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
    pub body: PlanBody,
}

impl Plan {
    pub fn node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        self.node_edge_mappings.clone()
    }

    pub(crate) fn resolve_text_excerpts(&mut self, ontology: &Ontology) {
        for node in self.nodes.values_mut() {
            let requested = requested_columns(&node.columns);
            node.text_excerpt.columns =
                requested_text_columns(node.entity.as_deref(), &requested, ontology);
        }
        if let PlanBody::Hydration(nodes) = &mut self.body {
            for node in nodes {
                node.text_excerpt.columns =
                    requested_text_columns(Some(&node.entity), &node.columns, ontology);
            }
        }

        let rows_per_page = u64::from(self.limit.max(1));
        let text_cells_per_row: u64 = self
            .text_excerpts_mut()
            .map(|excerpt| excerpt.columns.len() as u64)
            .sum();
        let max_chars = TEXT_EXCERPT_BUDGET_BYTES / (rows_per_page * text_cells_per_row.max(1));
        for excerpt in self.text_excerpts_mut() {
            excerpt.max_chars = max_chars.max(MIN_TEXT_EXCERPT_CHARACTERS) as u32;
        }
    }

    fn text_excerpts_mut(&mut self) -> impl Iterator<Item = &mut TextExcerpt> {
        let Plan { nodes, body, .. } = self;
        let hydration_nodes = match body {
            PlanBody::Hydration(nodes) => nodes.as_mut_slice(),
            _ => &mut [],
        };
        nodes.values_mut().map(|node| &mut node.text_excerpt).chain(
            hydration_nodes
                .iter_mut()
                .map(|node| &mut node.text_excerpt),
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TextExcerpt {
    pub columns: HashSet<String>,
    pub max_chars: u32,
}

fn requested_text_columns(
    entity: Option<&str>,
    requested: &[String],
    ontology: &Ontology,
) -> HashSet<String> {
    let Some(node) = entity.and_then(|name| ontology.get_node(name)) else {
        return HashSet::new();
    };

    let mut excerpt_columns: HashSet<String> = node
        .fields
        .iter()
        .filter(|field| field.column_name().is_some() && field.data_type == DataType::String)
        .filter(|field| requested.contains(&field.name))
        .map(|field| field.name.clone())
        .collect();

    for field in &node.fields {
        if let FieldSource::Virtual(source) = &field.source {
            for lookup_input in &source.depends_on {
                excerpt_columns.remove(lookup_input);
            }
        }
    }

    excerpt_columns
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
    Hydration(Vec<HydrationNodePlan>),
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
    pub fn from_input(metadata: &CompilerMetadata, rel_types: &[String]) -> Self {
        use std::collections::BTreeSet;
        let mut source_kinds = BTreeSet::new();
        let mut target_kinds = BTreeSet::new();
        for rt in rel_types {
            if let Some(kinds) = metadata.edge_source_kinds.get(rt) {
                source_kinds.extend(kinds.iter().cloned());
            }
            if let Some(kinds) = metadata.edge_target_kinds.get(rt) {
                target_kinds.extend(kinds.iter().cloned());
            }
        }
        let tables = metadata.resolve_edge_tables(rel_types);
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

pub fn plan(input: &mut Input) -> Result<Plan> {
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => Ok(edge_chain::plan(input)),
        QueryType::Neighbors => neighbors::plan_neighbors(input),
        QueryType::PathFinding => pathfinding::plan_pathfinding(input),
        QueryType::Hydration => hydration::plan_hydration(input),
    }
}
