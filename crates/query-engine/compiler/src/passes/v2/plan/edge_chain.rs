//! EdgeChainPlan: the edge-chain query plan and its builder.
//!
//! `EdgeChainPlan::plan()` reads Input, populates the IR.

use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::input::*;

use super::super::shared::{requested_columns, resolve_edge_table};

// ─────────────────────────────────────────────────────────────────────────────
// EdgeChainPlan: the edge-chain query plan
// ─────────────────────────────────────────────────────────────────────────────

pub struct EdgeChainPlan {
    /// Ordered chain of edge hops.
    pub hops: Vec<Hop>,
    /// Per-node planning metadata. Keyed by node alias.
    pub nodes: HashMap<String, NodePlan>,
    /// Execution strategy for the edge chain.
    pub strategy: Strategy,
    /// FK hops that were elided: (target_node, fk_node, fk_column).
    /// Used by emit to populate node_edge_col for the enforce pass.
    pub elided_fks: Vec<(String, String, String)>,
    /// Pre-resolved node_edge_col mappings for the enforce pass.
    /// Computed during plan() from hops + elided_fks.
    pub node_edge_mappings: HashMap<String, (String, String)>,
    /// Whether to synthesize FK edge metadata (traversal only, non-aggregation).
    pub synthesize_fk_edge_metadata: bool,
    /// Pre-computed ORDER BY for the outer query.
    pub order_by: Option<PlanOrderBy>,
    /// Query result limit.
    pub limit: u32,
    /// Aggregation plan (only present for Aggregation query type).
    pub agg: Option<AggPlan>,
}

/// A single edge hop in the plan chain.
pub struct Hop {
    /// Relationship types to match (e.g. ["AUTHORED"]).
    pub rel_types: Vec<String>,
    /// Physical edge table (e.g. "gl_edge", "gl_code_edge").
    pub edge_table: String,
    /// Node alias on the "from" side of this hop.
    pub from_node: String,
    /// Node alias on the "to" side of this hop.
    pub to_node: String,
    /// Edge direction (determines source_id vs target_id mapping).
    pub direction: Direction,
    /// Min hops (1 = include depth-1, 2 = skip depth-1, etc.).
    pub min_hops: u32,
    /// Max hops (1 for single-hop, >1 for variable-length).
    pub max_hops: u32,
    /// FK on a node table that encodes this relationship.
    /// When set, the plan can join node tables directly without the edge table.
    pub fk: Option<HopFk>,
    /// Edge-level filters from the query (e.g. relationship property predicates).
    pub filters: Vec<(String, InputFilter)>,
    /// Pre-resolved join columns for connecting to the previous hop.
    /// None for the first hop (it's the initial FROM).
    pub join_prev: Option<JoinColumns>,
}

/// Pre-resolved join columns for connecting a hop to the previous hop.
pub struct JoinColumns {
    pub prev_alias: String,
    pub prev_col: String,
    pub curr_col: String,
}

/// FK info for a hop — which node has the FK column.
#[derive(Clone, Debug)]
pub struct HopFk {
    /// Node alias that holds the FK (must be one of from_node or to_node).
    pub fk_node: String,
    /// The FK column on that node (e.g. "project_id").
    pub fk_column: String,
    /// The other node's alias (the one the FK points to).
    pub target_node: String,
}

/// Per-node plan: where its ID comes from and what to do with it.
pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub selectivity: Selectivity,
    pub hydration: HydrationStrategy,
    /// Which edge alias + column carries this node's ID in the plan.
    pub id_source: Option<IdSource>,
    /// Node filters from the query input.
    pub filters: Vec<(String, InputFilter)>,
    /// Pinned IDs (node_ids from query input).
    pub node_ids: Vec<i64>,
    /// ID range filter.
    pub id_range: Option<InputIdRange>,
    /// Whether the node table has a traversal_path column (most do; User/Runner don't).
    pub has_traversal_path: bool,
    /// Auth column (usually "id", but e.g. "project_id" for Definition).
    pub redaction_id_column: String,
    /// Columns requested by the user.
    pub columns: Option<ColumnSelection>,
    /// Pre-computed denorm tag predicates for this node.
    /// Only populated for the first edge where the node appears.
    pub denorm_tags: Vec<DenormTag>,
    /// Pre-resolved columns for the dedup subquery.
    pub dedup_columns: Vec<String>,
    /// Whether this node needs IN-narrowing.
    pub use_narrowing: bool,
    /// Whether this node needs elevated-access FilterOnly.
    pub needs_elevated_filter: bool,
    /// Pre-resolved node_edge_col mapping (alias, column).
    pub edge_col_mapping: Option<(String, String)>,
    /// Whether an FK target needs inline JOIN hydration.
    pub fk_needs_join: bool,
    /// Whether this node's columns should appear in SELECT.
    /// False for non-group-by nodes in aggregation queries.
    pub emit_select: bool,
}

/// Pre-computed denorm tag predicate for application on an edge.
pub struct DenormTag {
    pub edge_alias: String,
    pub tag_column: String,
    pub tag_key: String,
    pub tag_value: String,
    pub op: DenormTagOp,
}

/// Operation type for a denorm tag predicate.
pub enum DenormTagOp {
    /// has(edge_column, "key:value")
    Has,
    /// hasAny(edge_column, array("key:v1", "key:v2", ...))
    HasAny(Vec<String>),
}

/// Pre-computed ORDER BY for the outer query (traversal + aggregation).
pub struct PlanOrderBy {
    pub node: String,
    pub property: String,
    pub desc: bool,
}

/// Pre-computed aggregation plan (only for Aggregation queries).
pub struct AggPlan {
    pub specs: Vec<AggSpec>,
    pub sort: Option<AggSortPlan>,
}

/// A single aggregation function specification.
pub struct AggSpec {
    pub function: AggFunction,
    pub target: Option<String>,
    pub property: Option<String>,
    pub alias: String,
    pub group_by: Option<GroupByPlan>,
}

/// Pre-computed GROUP BY columns for an aggregation.
pub struct GroupByPlan {
    pub node_alias: String,
    pub columns: Vec<String>,
}

/// Pre-computed aggregation sort.
pub struct AggSortPlan {
    pub alias: String,
    pub desc: bool,
}

/// Where a node's ID lives in the emitted SQL.
#[derive(Clone)]
pub struct IdSource {
    pub edge_alias: String,
    pub column: String,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Selectivity {
    /// Pinned IDs — most selective.
    Pinned,
    /// Has an ID range.
    IdRange,
    /// Has property filters.
    Filtered,
    /// Auth-scoped only (traversal_path).
    AuthScoped,
    /// Unconstrained.
    Open,
}

impl Selectivity {
    pub fn from_node(node: &InputNode) -> Self {
        if !node.node_ids.is_empty() {
            Self::Pinned
        } else if node.id_range.is_some() {
            Self::IdRange
        } else if !node.filters.is_empty() {
            Self::Filtered
        } else {
            Self::AuthScoped
        }
    }

    pub fn is_selective(self) -> bool {
        matches!(self, Self::Pinned | Self::IdRange | Self::Filtered)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HydrationStrategy {
    /// Full JOIN — needed for GROUP BY, ORDER BY, agg property targets.
    Join,
    /// WHERE IN subquery — non-denormalized filters, no columns in SELECT.
    FilterOnly,
    /// No hydration — edge carries everything needed.
    Skip,
}

pub enum Strategy {
    /// Flat edge chain: e0 JOIN e1 JOIN e2 ... (no CTEs).
    Flat,
    /// Bidirectional: forward arm + backward arm meeting at a hop index.
    Bidirectional { meeting_hop: usize },
    /// Single node, no edges.
    SingleNode,
    /// Star-schema optimization: all hops have FKs on the same center node.
    /// The center node drives a single scan; other nodes JOIN via FK columns.
    /// Zero edge table scans.
    FkStar { center: String },
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeChainPlan::plan()
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeChainPlan {
    /// Build the edge chain plan from query input.
    pub fn plan(input: &mut Input) -> Self {
        let hops = build_hops(input);
        let mut nodes = build_node_plans(input);

        // Partial FK elision: when a hop has an FK column and the far-end
        // node is pinned (concrete node_ids), absorb the FK constraint as a
        // filter on the FK-holding node and drop the hop entirely. This
        // eliminates an edge table scan. E.g. IN_PROJECT with project_id FK
        // becomes `mr.project_id = 278964` on the MR dedup, no edge needed.
        let (mut hops, elided_fks, input) = elide_fk_hops(hops, &mut nodes, input);

        // Reorder chain so the most selective node drives the scan.
        // Also reorder input.relationships to stay in sync — the enforce
        // pass uses relationship index to build EdgeMeta (e0_, e1_, etc.).
        let (reordered_hops, reversed) = reorder_by_selectivity(hops, &nodes);
        hops = reordered_hops;
        if reversed {
            input.relationships.reverse();
        }

        assign_id_sources(&hops, &mut nodes);

        for node_plan in nodes.values_mut() {
            node_plan.hydration = determine_hydration(node_plan, input);
        }

        let strategy = if hops.is_empty() {
            Strategy::SingleNode
        } else if let Some(center) = detect_fk_star(&hops) {
            Strategy::FkStar { center }
        } else {
            Strategy::Flat
        };

        // Pre-resolve join columns for each hop based on shared-node topology.
        resolve_join_columns(&mut hops);

        // Pre-compute denorm tags per node (only on the first edge alias
        // where the node appears).
        resolve_denorm_tags(&hops, &mut nodes, &input.compiler.denormalized_columns);

        // Pre-compute node_edge_col mappings from hops + elided_fks.
        let node_edge_mappings = compute_node_edge_mappings(&hops, &elided_fks, &strategy, &nodes);

        // Pre-compute IN-narrowing decisions.
        resolve_narrowing(&hops, &mut nodes);

        // Pre-resolve elevated-access FilterOnly for Skip nodes.
        resolve_elevated_access(&mut nodes, input);

        // Pre-compute dedup columns for each node.
        resolve_dedup_columns(&mut nodes, input);

        // Pre-resolve FK target join needs.
        resolve_fk_join_needs(&hops, &mut nodes, input);

        // Pre-resolve which nodes emit SELECT columns.
        // In aggregation queries, only group_by nodes emit columns.
        if input.query_type == QueryType::Aggregation {
            let group_by_nodes: HashSet<&str> = input
                .aggregations
                .iter()
                .filter_map(|a| a.group_by.as_deref())
                .collect();
            for np in nodes.values_mut() {
                np.emit_select = group_by_nodes.contains(np.alias.as_str());
            }
        }

        let synthesize_fk_edge_metadata = matches!(strategy, Strategy::FkStar { .. })
            && input.query_type != QueryType::Aggregation;

        let order_by = input.order_by.as_ref().map(|ob| PlanOrderBy {
            node: ob.node.clone(),
            property: ob.property.clone(),
            desc: matches!(ob.direction, OrderDirection::Desc),
        });

        let limit = input.limit;

        let agg = if input.query_type == QueryType::Aggregation {
            Some(build_agg_plan(input, &nodes))
        } else {
            None
        };

        Self {
            hops,
            nodes,
            strategy,
            elided_fks,
            node_edge_mappings,
            synthesize_fk_edge_metadata,
            order_by,
            limit,
            agg,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan builders
// ─────────────────────────────────────────────────────────────────────────────

/// Elide hops that have an FK column when the far-end node is pinned.
/// Converts the FK into a node-level filter and removes the hop + its
/// input.relationships entry so edge alias indices stay in sync.
#[allow(clippy::type_complexity)]
fn elide_fk_hops<'a>(
    hops: Vec<Hop>,
    nodes: &mut HashMap<String, NodePlan>,
    input: &'a mut Input,
) -> (Vec<Hop>, Vec<(String, String, String)>, &'a mut Input) {
    let mut keep_hops = Vec::new();
    let mut keep_rels = Vec::new();
    let mut elided_fks = Vec::new();

    for (i, hop) in hops.into_iter().enumerate() {
        // Only elide if at least one non-FK hop would remain — otherwise
        // the emit loop has no edges to populate node_edge_col from.
        let remaining_non_fk = keep_hops.len();
        let would_be_last = remaining_non_fk == 0;

        let elide_info = hop.fk.as_ref().and_then(|fk| {
            if would_be_last {
                return None;
            }
            let np = nodes.get(&fk.target_node)?;
            if np.selectivity == Selectivity::Pinned
                && !np.node_ids.is_empty()
                && hop.filters.is_empty()
            {
                Some((
                    fk.fk_node.clone(),
                    fk.fk_column.clone(),
                    np.node_ids.clone(),
                ))
            } else {
                None
            }
        });

        let elided = if let Some((fk_node, fk_column, pinned_ids)) = elide_info.clone() {
            if let Some(fk_np) = nodes.get_mut(&fk_node) {
                let filter = if pinned_ids.len() == 1 {
                    InputFilter {
                        op: Some(FilterOp::Eq),
                        value: Some(serde_json::Value::Number(pinned_ids[0].into())),
                        data_type: None,
                    }
                } else {
                    InputFilter {
                        op: Some(FilterOp::In),
                        value: Some(serde_json::Value::Array(
                            pinned_ids
                                .iter()
                                .map(|&id| serde_json::Value::Number(id.into()))
                                .collect(),
                        )),
                        data_type: None,
                    }
                };
                fk_np.filters.push((fk_column, filter));
                if fk_np.selectivity > Selectivity::Filtered {
                    fk_np.selectivity = Selectivity::Filtered;
                }
                true
            } else {
                false
            }
        } else {
            false
        };

        if elided {
            let (fk_node, fk_column, _) = elide_info.unwrap();
            let target_node = hop.fk.as_ref().map(|fk| fk.target_node.clone()).unwrap();
            elided_fks.push((target_node, fk_node, fk_column));
        } else {
            keep_hops.push(hop);
            if i < input.relationships.len() {
                keep_rels.push(input.relationships[i].clone());
            }
        }
    }

    input.relationships = keep_rels;
    (keep_hops, elided_fks, input)
}

fn detect_fk_star(hops: &[Hop]) -> Option<String> {
    let first_center = hops.first()?.fk.as_ref().map(|fk| &fk.fk_node)?;
    for hop in &hops[1..] {
        let center = hop.fk.as_ref().map(|fk| &fk.fk_node)?;
        if center != first_center {
            return None;
        }
    }
    Some(first_center.clone())
}

fn reorder_by_selectivity(
    mut hops: Vec<Hop>,
    nodes: &HashMap<String, NodePlan>,
) -> (Vec<Hop>, bool) {
    if hops.len() <= 1 {
        return (hops, false);
    }
    let start_sel = nodes
        .get(&hops[0].from_node)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);
    let end_sel = nodes
        .get(&hops.last().unwrap().to_node)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);

    if end_sel < start_sel {
        hops.reverse();
        for hop in &mut hops {
            std::mem::swap(&mut hop.from_node, &mut hop.to_node);
            hop.direction = match hop.direction {
                Direction::Outgoing => Direction::Incoming,
                Direction::Incoming => Direction::Outgoing,
                Direction::Both => Direction::Both,
            };
        }
        (hops, true)
    } else {
        (hops, false)
    }
}

fn build_hops(input: &Input) -> Vec<Hop> {
    input
        .relationships
        .iter()
        .map(|rel| {
            let edge_table = resolve_edge_table(input, &rel.types);
            let fk = rel.fk_column.as_ref().and_then(|col| {
                let from_table = input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.from)
                    .and_then(|n| n.table.as_deref())
                    .unwrap_or("");
                let to_table = input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.to)
                    .and_then(|n| n.table.as_deref())
                    .unwrap_or("");

                let from_has = input
                    .compiler
                    .table_columns
                    .get(from_table)
                    .is_some_and(|cols| cols.contains(col));
                let to_has = input
                    .compiler
                    .table_columns
                    .get(to_table)
                    .is_some_and(|cols| cols.contains(col));

                let (fk_node, target_node) = if from_has {
                    (rel.from.clone(), rel.to.clone())
                } else if to_has {
                    (rel.to.clone(), rel.from.clone())
                } else {
                    return None;
                };
                Some(HopFk {
                    fk_node,
                    fk_column: col.clone(),
                    target_node,
                })
            });
            Hop {
                rel_types: rel.types.clone(),
                edge_table,
                from_node: rel.from.clone(),
                to_node: rel.to.clone(),
                direction: rel.direction,
                min_hops: rel.min_hops,
                max_hops: rel.max_hops,
                fk,
                filters: rel.filters.clone().into_iter().collect(),
                join_prev: None,
            }
        })
        .collect()
}

fn build_node_plans(input: &Input) -> HashMap<String, NodePlan> {
    input
        .nodes
        .iter()
        .map(|n| {
            (
                n.id.clone(),
                NodePlan {
                    alias: n.id.clone(),
                    entity: n.entity.clone(),
                    table: n.table.clone(),
                    selectivity: Selectivity::from_node(n),
                    hydration: HydrationStrategy::Skip,
                    id_source: None,
                    has_traversal_path: n.has_traversal_path,
                    redaction_id_column: n.redaction_id_column.clone(),
                    filters: n.filters.clone().into_iter().collect(),
                    node_ids: n.node_ids.clone(),
                    id_range: n.id_range.clone(),
                    columns: n.columns.clone(),
                    denorm_tags: Vec::new(),
                    dedup_columns: Vec::new(),
                    use_narrowing: false,
                    needs_elevated_filter: false,
                    edge_col_mapping: None,
                    fk_needs_join: false,
                    emit_select: true,
                },
            )
        })
        .collect()
}

fn assign_id_sources(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>) {
    for (i, hop) in hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node, col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if let Some(np) = nodes.get_mut(node)
                && np.id_source.is_none()
            {
                np.id_source = Some(IdSource {
                    edge_alias: alias.clone(),
                    column: col.to_string(),
                });
            }
        }
    }
}

fn determine_hydration(node_plan: &NodePlan, input: &Input) -> HydrationStrategy {
    let alias = &node_plan.alias;

    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(alias.as_str()));
    let is_agg_property_target = input.aggregations.iter().any(|a| {
        a.target.as_deref() == Some(alias.as_str())
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });
    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == *alias);

    if is_group_by || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    let has_non_denorm_filters = super::super::shared::has_non_denorm_filters(
        node_plan.entity.as_deref().unwrap_or(""),
        &node_plan.filters,
        &input.compiler.denormalized_columns,
    );

    if has_non_denorm_filters {
        return HydrationStrategy::FilterOnly;
    }

    HydrationStrategy::Skip
}

/// Pre-resolve join columns for each hop based on shared-node topology
/// with the previous hop.
fn resolve_join_columns(hops: &mut [Hop]) {
    for i in 1..hops.len() {
        let prev_hop = &hops[i - 1];
        let prev_alias = format!("e{}", i - 1);
        let (prev_start, prev_end) = prev_hop.direction.edge_columns();

        let curr_hop = &hops[i];
        let (start_col, end_col) = curr_hop.direction.edge_columns();

        let (prev_col, curr_col) = if prev_hop.to_node == curr_hop.from_node {
            (prev_end, start_col)
        } else if prev_hop.to_node == curr_hop.to_node {
            (prev_end, end_col)
        } else if prev_hop.from_node == curr_hop.from_node {
            (prev_start, start_col)
        } else if prev_hop.from_node == curr_hop.to_node {
            (prev_start, end_col)
        } else {
            (prev_end, start_col)
        };

        hops[i].join_prev = Some(JoinColumns {
            prev_alias,
            prev_col: prev_col.to_string(),
            curr_col: curr_col.to_string(),
        });
    }
}

/// Pre-compute denorm tags per node. Only applies tags on the first edge
/// alias where the node appears (later hops already join on filtered IDs).
fn resolve_denorm_tags(
    hops: &[Hop],
    nodes: &mut HashMap<String, NodePlan>,
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) {
    if denorm_map.is_empty() {
        return;
    }
    let mut applied: HashSet<String> = HashSet::new();
    for (i, hop) in hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !applied.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = nodes.get_mut(node_alias) else {
                continue;
            };
            let Some(ref entity) = np.entity else {
                continue;
            };
            let dir = if id_col == SOURCE_ID_COLUMN {
                "source"
            } else {
                "target"
            };
            for (prop, filter) in &np.filters {
                let key = (entity.clone(), prop.clone(), dir.to_string());
                let Some((edge_column, tag_key)) = denorm_map.get(&key) else {
                    continue;
                };
                match filter.op {
                    None | Some(FilterOp::Eq) => {
                        let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                        np.denorm_tags.push(DenormTag {
                            edge_alias: alias.clone(),
                            tag_column: edge_column.clone(),
                            tag_key: tag_key.clone(),
                            tag_value: format!("{tag_key}:{val}"),
                            op: DenormTagOp::Has,
                        });
                    }
                    Some(FilterOp::In) => {
                        if let Some(values) = filter.value.as_ref().and_then(|v| v.as_array()) {
                            let tags: Vec<String> = values
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                                .collect();
                            if tags.len() == 1 {
                                np.denorm_tags.push(DenormTag {
                                    edge_alias: alias.clone(),
                                    tag_column: edge_column.clone(),
                                    tag_key: tag_key.clone(),
                                    tag_value: tags[0].clone(),
                                    op: DenormTagOp::Has,
                                });
                            } else if !tags.is_empty() {
                                np.denorm_tags.push(DenormTag {
                                    edge_alias: alias.clone(),
                                    tag_column: edge_column.clone(),
                                    tag_key: tag_key.clone(),
                                    tag_value: String::new(),
                                    op: DenormTagOp::HasAny(tags),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Pre-compute node_edge_col mappings from hops + elided_fks + strategy.
fn compute_node_edge_mappings(
    hops: &[Hop],
    elided_fks: &[(String, String, String)],
    strategy: &Strategy,
    nodes: &HashMap<String, NodePlan>,
) -> HashMap<String, (String, String)> {
    let mut mappings = HashMap::new();

    match strategy {
        Strategy::FkStar { center } => {
            // Center node maps to itself.
            mappings.insert(
                center.clone(),
                (center.clone(), DEFAULT_PRIMARY_KEY.to_string()),
            );
            // Each hop's target maps via the FK column on the center.
            for hop in hops {
                if let Some(ref fk) = hop.fk {
                    let fk_alias = if fk.fk_node == *center {
                        center.clone()
                    } else {
                        fk.fk_node.clone()
                    };
                    mappings.insert(fk.target_node.clone(), (fk_alias, fk.fk_column.clone()));
                }
            }
        }
        _ => {
            // Flat/Bidirectional: each hop contributes from_node and to_node.
            for (i, hop) in hops.iter().enumerate() {
                let alias = format!("e{i}");
                let (start_col, end_col) = hop.direction.edge_columns();
                mappings
                    .entry(hop.from_node.clone())
                    .or_insert_with(|| (alias.clone(), start_col.to_string()));
                mappings
                    .entry(hop.to_node.clone())
                    .or_insert_with(|| (alias.clone(), end_col.to_string()));
            }
        }
    }

    // Elided FK target nodes.
    for (target_node, fk_node, fk_column) in elided_fks {
        mappings
            .entry(target_node.clone())
            .or_insert_with(|| (fk_node.clone(), fk_column.clone()));
    }

    // Store per-node for convenience.
    let _ = nodes;
    mappings
}

/// Pre-compute IN-narrowing decisions. A node needs narrowing when:
/// - it has Join hydration
/// - it has no user filters, node_ids, or id_range
/// - another node in ANY hop has FilterOnly hydration (i.e. a _filter_ CTE exists)
fn resolve_narrowing(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>) {
    // Check if any node has FilterOnly hydration (will produce a _filter_ CTE).
    let has_filter_only = nodes
        .values()
        .any(|np| np.hydration == HydrationStrategy::FilterOnly);
    if !has_filter_only {
        return;
    }
    // Also check elevated-access nodes that will emit filter CTEs. But we
    // haven't resolved those yet, so check has_elevated_access_level won't
    // work here. Instead, the narrowing decision is based solely on whether
    // _filter_ CTEs exist from FilterOnly nodes. We'll update if
    // needs_elevated_filter also generates CTEs (it does, checked below).
    for hop in hops {
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node_alias, _edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            let Some(np) = nodes.get(node_alias) else {
                continue;
            };
            if np.hydration == HydrationStrategy::Join
                && np.filters.is_empty()
                && np.node_ids.is_empty()
                && np.id_range.is_none()
            {
                // Mark for narrowing. We can't do get_mut while iterating
                // immutably so collect the aliases first.
                let alias = node_alias.clone();
                if let Some(np_mut) = nodes.get_mut(&alias) {
                    np_mut.use_narrowing = true;
                }
            }
        }
    }
}

/// Pre-resolve elevated-access FilterOnly for Skip nodes.
fn resolve_elevated_access(nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    let aliases: Vec<String> = nodes.keys().cloned().collect();
    for alias in aliases {
        let np = &nodes[&alias];
        if np.hydration == HydrationStrategy::Skip
            && np.has_traversal_path
            && np.table.is_some()
            && has_elevated_access_level(np, input)
        {
            nodes.get_mut(&alias).unwrap().needs_elevated_filter = true;
        }
    }
}

/// Whether an entity requires a higher access level than the default (20).
/// Only these entities need a FilterOnly subquery in edge-based queries so
/// the security pass can enforce their stricter min_access_level.
fn has_elevated_access_level(np: &NodePlan, input: &Input) -> bool {
    let Some(ref entity) = np.entity else {
        return false;
    };
    input
        .entity_auth
        .get(entity)
        .is_some_and(|cfg| cfg.required_access_level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
}

/// Pre-compute dedup columns for each node from the query input.
fn resolve_dedup_columns(nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    let aliases: Vec<String> = nodes.keys().cloned().collect();
    for alias in aliases {
        let np = &nodes[&alias];
        let mut seen = HashSet::new();
        let mut cols = Vec::new();

        let mut push = |col: &str| {
            if seen.insert(col.to_string()) {
                cols.push(col.to_string());
            }
        };

        push(DEFAULT_PRIMARY_KEY);
        push(VERSION_COLUMN);
        if np.has_traversal_path {
            push(TRAVERSAL_PATH_COLUMN);
        }

        for col in requested_columns(&np.columns) {
            push(&col);
        }

        for (prop, _) in &np.filters {
            push(prop);
        }

        for agg in &input.aggregations {
            if agg.target.as_deref() == Some(alias.as_str())
                && let Some(ref prop) = agg.property
            {
                push(prop);
            }
        }

        if let Some(ref ob) = input.order_by
            && ob.node == alias
        {
            push(&ob.property);
        }

        if np.redaction_id_column != DEFAULT_PRIMARY_KEY {
            push(&np.redaction_id_column);
        }

        push(DELETED_COLUMN);

        nodes.get_mut(&alias).unwrap().dedup_columns = cols;
    }
}

/// Pre-resolve whether FK target nodes need inline JOIN hydration.
fn resolve_fk_join_needs(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    for hop in hops {
        let Some(ref fk) = hop.fk else { continue };
        let Some(np) = nodes.get(&fk.target_node) else {
            continue;
        };
        let needs = np.hydration == HydrationStrategy::Join
            || (input.query_type != QueryType::Aggregation
                && matches!(&np.columns, Some(ColumnSelection::List(cols)) if !cols.is_empty()));
        if needs && let Some(np_mut) = nodes.get_mut(&fk.target_node) {
            np_mut.fk_needs_join = true;
        }
    }
}

fn build_agg_plan(input: &Input, nodes: &HashMap<String, NodePlan>) -> AggPlan {
    let specs: Vec<AggSpec> = input
        .aggregations
        .iter()
        .map(|a| {
            let group_by = a.group_by.as_ref().and_then(|gb| {
                nodes.get(gb.as_str()).map(|np| GroupByPlan {
                    node_alias: gb.clone(),
                    columns: requested_columns(&np.columns),
                })
            });
            AggSpec {
                function: a.function,
                target: a.target.clone(),
                property: a.property.clone(),
                alias: a.alias.clone().unwrap_or_else(|| "agg_result".to_string()),
                group_by,
            }
        })
        .collect();

    let sort = input.aggregation_sort.as_ref().and_then(|s| {
        specs.get(s.agg_index).map(|spec| AggSortPlan {
            alias: spec.alias.clone(),
            desc: matches!(s.direction, OrderDirection::Desc),
        })
    });

    AggPlan { specs, sort }
}
