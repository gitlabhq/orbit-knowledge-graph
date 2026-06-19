//! Edge-chain query plan builder.
//!
//! `plan()` reads Input, produces a Plan for traversal and aggregation queries.

use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::input::*;

use super::{Plan, PlanBody};
use crate::passes::shared::{requested_columns, resolve_edge_table};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

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
    /// Tight `traversal_path` prefix to confine this hop's edge scan to,
    /// carried over from the originating `InputRelationship`.
    pub scope_prefix: Option<String>,
    /// Whether this hop keeps both endpoints in the same namespace (intrinsic
    /// child). Gates the FK-chain lowering, which is only result-equivalent to
    /// the edge scan for such relationships.
    pub scope_preserving: bool,
    /// Anchor this hop's join column with an IN-subquery over the previous
    /// hop's output ids, so ClickHouse can use the by_source/by_target
    /// projection or bloom filter instead of scanning the full relationship
    /// range. Set by the plan pass for interior single-hop edges in a
    /// multi-edge chain.
    pub cascade_anchor: bool,
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

/// Per-node plan metadata.
pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub selectivity: Selectivity,
    pub hydration: HydrationStrategy,
    pub filters: Vec<(String, InputFilter)>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub has_traversal_path: bool,
    pub redaction_id_column: String,
    pub columns: Option<ColumnSelection>,
    pub dedup_columns: Vec<String>,
    pub use_narrowing: bool,
    pub needs_elevated_filter: bool,
    pub fk_needs_join: bool,
    pub emit_select: bool,
}

impl NodePlan {
    pub fn uses_default_pk(&self) -> bool {
        self.redaction_id_column == DEFAULT_PRIMARY_KEY
    }

    /// Whether this node has point selectivity (node_ids, id_range) or at
    /// least one high-selectivity filter. Used to decide if a narrowing CTE
    /// is worth the cost of a pre-scan.
    pub fn has_selective_filters(&self) -> bool {
        !self.node_ids.is_empty()
            || self.id_range.is_some()
            || self
                .filters
                .iter()
                .any(|(_, f)| f.selectivity == ontology::FieldSelectivity::High)
    }
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
    /// FK-derived traversal answered by joining node tables on their FK
    /// columns, with zero edge-table scans. The [`FkShape`] selects how the
    /// nodes are joined; both shapes share one emit path (`lower::fk`).
    Fk(FkShape),
}

/// Topology of an FK-derived traversal. Single-hop FK is the degenerate
/// one-hop [`FkShape::Star`].
pub enum FkShape {
    /// All hops have FKs on the same center node. The center node drives a
    /// single scan; other nodes JOIN via the center's FK columns.
    Star { center: String },
    /// Every hop is FK-derived and consecutive hops share a node. The node
    /// tables are joined on their FK columns; the edges are a materialization
    /// of those FKs, so the chain skips all edge-table scans.
    Chain,
}

// ─────────────────────────────────────────────────────────────────────────────
// plan()
// ─────────────────────────────────────────────────────────────────────────────

pub fn plan(input: &mut Input) -> Plan {
    let hops = build_hops(input);
    let mut nodes = build_node_plans(input);

    let (mut hops, elided_fks, input) = elide_hops(hops, &mut nodes, input);

    let (reordered_hops, reversed) = reorder_by_selectivity(hops, &nodes);
    hops = reordered_hops;
    if reversed {
        input.relationships.reverse();
    }

    for node_plan in nodes.values_mut() {
        node_plan.hydration = determine_hydration(node_plan, input, &hops);
    }

    let strategy = if hops.is_empty() {
        Strategy::SingleNode
    } else if let Some(shape) = detect_fk(&hops, &nodes) {
        Strategy::Fk(shape)
    } else {
        Strategy::Flat
    };

    resolve_join_columns(&mut hops);
    resolve_cascade_anchors(&mut hops);

    let node_edge_mappings = compute_node_edge_mappings(&hops, &elided_fks, &strategy, &nodes);

    resolve_node_flags(&hops, &mut nodes, input);

    resolve_dedup_columns(&mut nodes, input);

    // In aggregation queries, only node group keys emit entity columns.
    if input.query_type == QueryType::Aggregation {
        let group_by_nodes: HashSet<&str> =
            crate::input::node_group_ids(&input.aggregation.group_by).collect();
        for np in nodes.values_mut() {
            np.emit_select = group_by_nodes.contains(np.alias.as_str());
        }
    }

    let body = if input.query_type == QueryType::Aggregation {
        PlanBody::Aggregation {
            aggregations: input.aggregation.metrics.clone(),
            agg_sort: input.aggregation.sort.clone(),
        }
    } else {
        PlanBody::Traversal
    };

    Plan {
        nodes,
        hops,
        strategy,
        limit: input.limit,
        order_by: input.order_by.clone(),
        cursor: input.cursor,
        node_edge_mappings,
        denorm_columns: input.compiler.denormalized_columns.clone(),
        denorm_rel_kinds: input.compiler.denorm_rel_kinds.clone(),
        table_columns: input.compiler.table_columns.clone(),
        table_sort_keys: input.compiler.table_sort_keys.clone(),
        body,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan builders
// ─────────────────────────────────────────────────────────────────────────────

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
                scope_preserving: rel.scope_preserving,
                filters: rel
                    .filters
                    .iter()
                    .flat_map(|(k, v)| v.iter().map(move |f| (k.clone(), f.clone())))
                    .collect(),
                join_prev: None,
                scope_prefix: rel.scope_prefix.clone(),
                cascade_anchor: false,
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
                    has_traversal_path: n.has_traversal_path,
                    redaction_id_column: n.redaction_id_column.clone(),
                    filters: n
                        .filters
                        .iter()
                        .flat_map(|(k, v)| v.iter().map(move |f| (k.clone(), f.clone())))
                        .collect(),
                    node_ids: n.node_ids.clone(),
                    id_range: n.id_range.clone(),
                    columns: n.columns.clone(),
                    dedup_columns: Vec::new(),
                    use_narrowing: false,
                    needs_elevated_filter: false,
                    fk_needs_join: false,
                    emit_select: true,
                },
            )
        })
        .collect()
}

/// Whether `alias` exists only to pin scope: path-scopable, only a `full_path`/`id`
/// filter, no group-by/agg/order/display role, and touched by exactly one hop.
fn is_pure_scope_anchor(
    alias: &str,
    nodes: &HashMap<String, NodePlan>,
    input: &Input,
    hop_count: &HashMap<String, usize>,
) -> bool {
    let Some(np) = nodes.get(alias) else {
        return false;
    };
    if !np.has_traversal_path || hop_count.get(alias).copied().unwrap_or(0) != 1 {
        return false;
    }
    let Some(input_node) = input.nodes.iter().find(|n| n.id == alias) else {
        return false;
    };
    if !crate::scope::is_scope_only(input_node) {
        return false;
    }

    let in_group_by = input.aggregation.group_by.iter().any(|g| g.node() == alias);
    let is_agg_target = input
        .aggregation
        .metrics
        .iter()
        .any(|m| m.target.as_deref() == Some(alias));
    let is_order_target = input.order_by.as_ref().is_some_and(|ob| ob.node == alias);

    !in_group_by && !is_agg_target && !is_order_target
}

/// Elide hops the node-join path answers without an edge scan, keeping
/// `input.relationships` in sync:
///   - an FK hop whose far end is pinned: push the FK as a node-level filter;
///   - the sole non-FK hop, when it is a scope-implied container (aggregations
///     only): drop it and its orphaned anchor, since the resolved
///     `traversal_path` prefix already encodes the containment and every
///     survivor is then FK-lowerable by `detect_fk`.
#[allow(clippy::type_complexity)]
fn elide_hops<'a>(
    hops: Vec<Hop>,
    nodes: &mut HashMap<String, NodePlan>,
    input: &'a mut Input,
) -> (Vec<Hop>, Vec<(String, String, String)>, &'a mut Input) {
    let mut keep_hops = Vec::new();
    let mut keep_rels = Vec::new();
    let mut elided_fks = Vec::new();

    let mut hop_count: HashMap<String, usize> = HashMap::new();
    for hop in &hops {
        *hop_count.entry(hop.from_node.clone()).or_insert(0) += 1;
        *hop_count.entry(hop.to_node.clone()).or_insert(0) += 1;
    }
    let sole_non_fk = input.query_type == QueryType::Aggregation
        && hops.iter().filter(|h| h.fk.is_none()).count() == 1;

    for (i, hop) in hops.into_iter().enumerate() {
        if sole_non_fk
            && hop.fk.is_none()
            && hop.scope_preserving
            && hop.scope_prefix.is_some()
            && hop.filters.is_empty()
            && let Some(anchor) = [hop.from_node.as_str(), hop.to_node.as_str()]
                .into_iter()
                .find(|a| is_pure_scope_anchor(a, nodes, input, &hop_count))
                .map(str::to_string)
        {
            nodes.remove(&anchor);
            input.nodes.retain(|n| n.id != anchor);
            continue;
        }

        // Only elide if at least one non-FK hop would remain — otherwise
        // the emit loop has no edges to populate node_edge_col from.
        let would_be_last = keep_hops.is_empty();

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
                        ..Default::default()
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
                        ..Default::default()
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

/// Star first (covers single-hop FK), then chain. Chain applies to aggregations
/// too: it joins node tables on FK columns, which is the source of truth for a
/// relationship whose edge rows can lag (e.g. stale `HAS_LATEST_DIFF` edges).
fn detect_fk(hops: &[Hop], nodes: &HashMap<String, NodePlan>) -> Option<FkShape> {
    if let Some(center) = detect_fk_star(hops) {
        return Some(FkShape::Star { center });
    }
    if detect_fk_chain(hops, nodes) {
        return Some(FkShape::Chain);
    }
    None
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

/// Linear FK chain the node-join path answers without edge scans. Each hop must be
/// FK-backed, single fixed-length, edge-filter-free, and not `Both`; gated out of
/// point-selective endpoints (SIP narrowing on the edge scan beats a full leaf-node
/// scan there) and non-emittable shapes.
///
/// A hop must be scope-preserving OR reach a global hub (an endpoint with no
/// `traversal_path`, e.g. User/Runner/Label). The latter covers `prune_to_target`/
/// `prune_to_source` edges: the in-namespace endpoint keeps its scope while the hub
/// is reached only via the FK off that scoped node, so no scope is lost and the
/// authz boundary stays on the scoped side -- same as the edge path.
fn detect_fk_chain(hops: &[Hop], nodes: &HashMap<String, NodePlan>) -> bool {
    let point_selective = |alias: &str| {
        nodes
            .get(alias)
            .is_some_and(|np| matches!(np.selectivity, Selectivity::Pinned | Selectivity::IdRange))
    };
    let reaches_global_hub = |h: &Hop| {
        [h.from_node.as_str(), h.to_node.as_str()]
            .iter()
            .any(|a| nodes.get(*a).is_some_and(|np| !np.has_traversal_path))
    };
    // Authz guard: a global-hub hop is only sound while the chain still has an
    // in-namespace node carrying the traversal_path scope, so the boundary is
    // never lost (a hub is reached only via the FK off a scoped node).
    let has_scope_anchor = hops.iter().any(|h| {
        [h.from_node.as_str(), h.to_node.as_str()]
            .iter()
            .any(|a| nodes.get(*a).is_some_and(|np| np.has_traversal_path))
    });
    hops.len() >= 2
        && has_scope_anchor
        && hops.iter().all(|h| {
            h.fk.is_some()
                && (h.scope_preserving || reaches_global_hub(h))
                && h.max_hops == 1
                && h.filters.is_empty()
                && !matches!(h.direction, Direction::Both)
                && !point_selective(&h.from_node)
                && !point_selective(&h.to_node)
        })
        && is_emittable_fk_chain(hops)
}

/// `emit_chain` joins each hop's not-yet-reached endpoint onto the running FROM,
/// so every hop after the first must attach via exactly one already-reached node
/// (accepts branching trees and either hop orientation; rejects disconnected hops).
fn is_emittable_fk_chain(hops: &[Hop]) -> bool {
    let Some(first) = hops.first() else {
        return false;
    };
    let mut reached: HashSet<&str> =
        HashSet::from([first.from_node.as_str(), first.to_node.as_str()]);
    hops[1..].iter().all(|h| {
        let ok = reached.contains(h.from_node.as_str()) != reached.contains(h.to_node.as_str());
        reached.insert(h.from_node.as_str());
        reached.insert(h.to_node.as_str());
        ok
    })
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

fn determine_hydration(node_plan: &NodePlan, input: &Input, hops: &[Hop]) -> HydrationStrategy {
    let alias = &node_plan.alias;

    let is_group_by_node = crate::input::node_group_ids(&input.aggregation.group_by)
        .any(|node| node == alias.as_str());
    let is_group_by_property = input
        .aggregation
        .group_by
        .iter()
        .any(|group| matches!(group, crate::input::InputGroupByKey::Property { node, .. } if node == alias));
    let is_agg_property_target = input.aggregation.metrics.iter().any(|a| {
        a.target.as_deref() == Some(alias.as_str())
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });
    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == *alias);

    if is_group_by_node || is_group_by_property || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    // Skip the node table only when every filter is carried by a hop's edge
    // tag; an uncovered filter stays on the node table so it isn't dropped.
    let entity = node_plan.entity.as_deref().unwrap_or("");
    let has_uncovered_filter = node_plan.filters.iter().any(|(prop, _)| {
        !filter_covered_by_denorm(entity, prop, alias, hops, &input.compiler.denorm_rel_kinds)
    });

    if has_uncovered_filter {
        return HydrationStrategy::FilterOnly;
    }

    HydrationStrategy::Skip
}

// Mirrors the lowerer's `emit_denorm_tags`: the hydration decision and the tag
// push must agree on which hop carries a denorm.
fn filter_covered_by_denorm(
    entity: &str,
    prop: &str,
    alias: &str,
    hops: &[Hop],
    denorm_rel_kinds: &HashMap<(String, String, String), Vec<String>>,
) -> bool {
    hops.iter().any(|hop| {
        if crate::passes::normalize::is_wildcard(&hop.rel_types) {
            return false;
        }
        let (start_col, end_col) = hop.direction.edge_columns();
        [(&hop.from_node, start_col), (&hop.to_node, end_col)]
            .iter()
            .any(|(node, id_col)| {
                if node.as_str() != alias {
                    return false;
                }
                let dir = if *id_col == SOURCE_ID_COLUMN {
                    "source"
                } else {
                    "target"
                };
                let key = (entity.to_string(), prop.to_string(), dir.to_string());
                denorm_rel_kinds
                    .get(&key)
                    .is_some_and(|kinds| hop.rel_types.iter().any(|t| kinds.iter().any(|k| k == t)))
            })
    })
}

/// Mark interior hops for cascade SIP anchoring. A hop qualifies when it
/// is a non-first, single-hop edge with a resolved `join_prev` in a
/// multi-edge chain. Variable-length hops (max_hops > 1) are excluded
/// because their UNION-ALL arms have their own internal join structure.
fn resolve_cascade_anchors(hops: &mut [Hop]) {
    if hops.len() < 2 {
        return;
    }
    for hop in hops.iter_mut().skip(1) {
        hop.cascade_anchor = hop.join_prev.is_some() && hop.max_hops == 1;
    }
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

/// Pre-compute node_edge_col mappings from hops + elided_fks + strategy.
fn compute_node_edge_mappings(
    hops: &[Hop],
    elided_fks: &[(String, String, String)],
    strategy: &Strategy,
    nodes: &HashMap<String, NodePlan>,
) -> HashMap<String, (String, String)> {
    let mut mappings = HashMap::new();

    match strategy {
        Strategy::Fk(FkShape::Star { center }) => {
            mappings.insert(
                center.clone(),
                (center.clone(), DEFAULT_PRIMARY_KEY.to_string()),
            );
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
        Strategy::Fk(FkShape::Chain) => {
            // Each node is joined as its own table, so it maps to its own PK.
            for hop in hops {
                for node in [&hop.from_node, &hop.to_node] {
                    mappings
                        .entry(node.clone())
                        .or_insert_with(|| (node.clone(), DEFAULT_PRIMARY_KEY.to_string()));
                }
            }
        }
        _ => {
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

fn resolve_node_flags(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    let has_filter_only = nodes
        .values()
        .any(|np| np.hydration == HydrationStrategy::FilterOnly);

    if has_filter_only {
        let mut convergent_targets: HashMap<&str, usize> = HashMap::new();
        for hop in hops {
            *convergent_targets.entry(hop.to_node.as_str()).or_insert(0) += 1;
        }
        let needs: Vec<String> = nodes
            .values()
            .filter(|np| {
                np.hydration == HydrationStrategy::Join
                    && np.filters.is_empty()
                    && np.node_ids.is_empty()
                    && np.id_range.is_none()
                    && convergent_targets
                        .get(np.alias.as_str())
                        .copied()
                        .unwrap_or(0)
                        < 2
            })
            .map(|np| np.alias.clone())
            .collect();
        for alias in needs {
            nodes.get_mut(&alias).unwrap().use_narrowing = true;
        }
    }

    // 2. Elevated access: Skip nodes with elevated auth need FilterOnly
    let elevated: Vec<String> = nodes
        .values()
        .filter(|np| {
            np.hydration == HydrationStrategy::Skip
                && np.has_traversal_path
                && np.table.is_some()
                && has_elevated_access_level(np, input)
        })
        .map(|np| np.alias.clone())
        .collect();
    for alias in elevated {
        nodes.get_mut(&alias).unwrap().needs_elevated_filter = true;
    }

    // 3. FK join needs
    for hop in hops {
        let Some(ref fk) = hop.fk else { continue };
        let Some(np) = nodes.get(&fk.target_node) else {
            continue;
        };
        let needs = np.hydration == HydrationStrategy::Join
            || (input.query_type != QueryType::Aggregation
                && matches!(&np.columns, Some(ColumnSelection::List(cols)) if !cols.is_empty()));
        if needs {
            nodes.get_mut(&fk.target_node).unwrap().fk_needs_join = true;
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

        for agg in &input.aggregation.metrics {
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
