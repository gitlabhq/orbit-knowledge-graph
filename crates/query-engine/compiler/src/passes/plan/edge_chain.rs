use std::collections::HashMap;
use std::collections::HashSet;

use crate::scope::ScopeProof;
use ontology::constants::*;

use crate::input::*;

use super::{BoundFilter, Plan, PlanBody};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

pub struct Hop {
    pub rel_types: Vec<String>,
    pub edge_table: String,
    pub from_node: String,
    pub to_node: String,
    pub direction: Direction,
    /// Min hops (1 = include depth-1, 2 = skip depth-1, etc.).
    pub min_hops: u32,
    /// Max hops (1 for single-hop, >1 for variable-length).
    pub max_hops: u32,
    /// When set, the plan can join node tables directly without the edge table.
    pub fk: Option<HopFk>,
    pub filters: Vec<(String, BoundFilter)>,
    /// None for the first hop (it's the initial FROM).
    pub join_prev: Option<JoinColumns>,
    /// Logical proof that this hop can use the anchored traversal scope.
    pub scope_proof: Option<ScopeProof>,
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

pub struct JoinColumns {
    pub prev_alias: String,
    pub prev_col: String,
    pub curr_col: String,
}

#[derive(Clone, Debug)]
pub struct HopFk {
    /// Node alias that holds the FK (must be one of from_node or to_node).
    pub fk_node: String,
    pub fk_column: String,
    /// The other node's alias (the one the FK points to).
    pub target_node: String,
}

pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub selectivity: Selectivity,
    pub hydration: HydrationStrategy,
    pub filters: Vec<(String, BoundFilter)>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub has_traversal_path: bool,
    pub is_global: bool,
    pub redaction_id_column: String,
    pub columns: Option<ColumnSelection>,
    pub use_narrowing: bool,
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
    Pinned,
    IdRange,
    Filtered,
    /// Auth-scoped only (traversal_path).
    AuthScoped,
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
    SingleNode,
    /// FK-derived traversal answered by joining node tables on their FK
    /// columns, with zero edge-table scans. The [`FkShape`] selects how the
    /// nodes are joined; both shapes share one emit path (`lower::fk`).
    Fk(FkShape),
}

/// Single-hop FK is the degenerate one-hop [`FkShape::Star`].
pub enum FkShape {
    /// All hops have FKs on the same center node. The center node drives a
    /// single scan; other nodes JOIN via the center's FK columns.
    Star { center: String },
    /// Every hop is FK-derived and consecutive hops share a node. The node
    /// tables are joined on their FK columns; the edges are a materialization
    /// of those FKs, so the chain skips all edge-table scans.
    Chain,
}

pub fn plan<M>(
    input: &Input,
    scope_proofs: &HashMap<String, ScopeProof>,
    model: &M,
    use_fk_elision: bool,
) -> Plan
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    let hops = build_hops(input, scope_proofs, model);
    let mut nodes = build_node_plans(input, model);
    let backend = model.query_backend();

    let (mut hops, elided_fks, scope_requirements) = if use_fk_elision {
        elide_hops(hops, &mut nodes, input)
    } else {
        (hops, Vec::new(), Vec::new())
    };

    let (reordered_hops, reversed) = reorder_by_selectivity(hops, &nodes);
    hops = reordered_hops;
    let _ = reversed;
    let denorm_columns = backend.denormalized().columns.clone();
    let denorm_rel_kinds = backend.denormalized().relationships.clone();

    for node_plan in nodes.values_mut() {
        if use_fk_elision {
            node_plan.hydration = determine_hydration(node_plan, input, &hops, &denorm_rel_kinds);
        } else {
            node_plan.hydration = HydrationStrategy::Join;
        }
    }

    let strategy = if hops.is_empty() {
        Strategy::SingleNode
    } else if use_fk_elision && let Some(shape) = detect_fk(&hops, &nodes) {
        Strategy::Fk(shape)
    } else {
        Strategy::Flat
    };

    resolve_join_columns(&mut hops);
    resolve_cascade_anchors(&mut hops);

    let node_edge_mappings = compute_node_edge_mappings(&hops, &elided_fks, &strategy, &nodes);

    resolve_node_flags(&hops, &mut nodes, input);

    if input.query_type == QueryType::Aggregation {
        let group_by_nodes: HashSet<&str> =
            crate::input::node_group_ids(&input.aggregation.group_by).collect();
        for np in nodes.values_mut() {
            np.emit_select = group_by_nodes.contains(np.alias.as_str());
        }
    } else if !use_fk_elision {
        for np in nodes.values_mut() {
            np.emit_select = true;
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

    let table_names: HashSet<String> = input
        .nodes
        .iter()
        .filter_map(|node| {
            node.entity
                .as_deref()
                .and_then(|entity| model.graph().entity_id(entity))
                .and_then(|entity| backend.entity_table(entity))
                .map(String::from)
        })
        .chain(hops.iter().map(|hop| hop.edge_table.clone()))
        .collect();
    let table_columns = table_names
        .iter()
        .filter_map(|table| {
            backend
                .table_columns(table)
                .map(|columns| (table.clone(), columns.clone()))
        })
        .collect();
    let table_sort_keys = table_names
        .iter()
        .filter_map(|table| {
            backend
                .table_sort_key(table)
                .map(|sort_key| (table.clone(), sort_key.to_vec()))
        })
        .collect();
    Plan {
        nodes,
        hops,
        strategy,
        node_edge_mappings,
        scope_requirements,
        denorm_columns,
        denorm_rel_kinds,
        table_columns,
        table_sort_keys,
        body,
    }
}

fn build_hops<M>(input: &Input, scope_proofs: &HashMap<String, ScopeProof>, model: &M) -> Vec<Hop>
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    let entities: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.as_str(), node.entity.as_deref()?)))
        .collect();
    input
        .relationships
        .iter()
        .map(|rel| {
            let relationship_ids: Vec<_> = rel
                .types
                .iter()
                .filter_map(|relationship| model.graph().relationship_id(relationship))
                .collect();
            let edge_table = model
                .query_backend()
                .edge_tables(&relationship_ids)
                .into_iter()
                .next()
                .unwrap_or_else(|| model.query_backend().default_edge_table().to_string());
            let from_entity = input
                .nodes
                .iter()
                .find(|node| node.id == rel.from)
                .and_then(|node| node.entity.as_deref());
            let to_entity = input
                .nodes
                .iter()
                .find(|node| node.id == rel.to)
                .and_then(|node| node.entity.as_deref());
            let fk = from_entity
                .zip(to_entity)
                .and_then(|(source, target)| {
                    let source = model.graph().entity_id(source)?;
                    let target = model.graph().entity_id(target)?;
                    model.query_backend().foreign_key(
                        model.graph(),
                        &relationship_ids,
                        source,
                        target,
                    )
                })
                .and_then(|foreign_key| {
                    let holder = &model.graph().entity(foreign_key.holder).name;
                    let fk_node = if from_entity == Some(holder.as_str()) {
                        rel.from.clone()
                    } else if to_entity == Some(holder.as_str()) {
                        rel.to.clone()
                    } else {
                        return None;
                    };
                    let target_node = if fk_node == rel.from {
                        rel.to.clone()
                    } else {
                        rel.from.clone()
                    };
                    Some(HopFk {
                        fk_node,
                        fk_column: foreign_key.column,
                        target_node,
                    })
                });
            let from_entity = entities.get(rel.from.as_str()).copied().unwrap_or_default();
            let to_entity = entities.get(rel.to.as_str()).copied().unwrap_or_default();
            let scope_preserving = !rel.types.is_empty()
                && rel.types.iter().all(|kind| {
                    model
                        .variant_scope(kind, from_entity, to_entity)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                        || model
                            .variant_scope(kind, to_entity, from_entity)
                            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
            let from_proof = scope_proofs.get(&rel.from);
            let to_proof = scope_proofs.get(&rel.to);
            let scope_proof = if from_proof == to_proof {
                from_proof.cloned()
            } else {
                rel.types.iter().find_map(|kind| {
                    match model.variant_scope(kind, from_entity, to_entity) {
                        Some(ontology::EdgeVariantScope::PruneToSource) => from_proof,
                        Some(ontology::EdgeVariantScope::PruneToTarget) => to_proof,
                        _ => None,
                    }
                    .cloned()
                })
            };
            Hop {
                rel_types: rel.types.clone(),
                edge_table,
                from_node: rel.from.clone(),
                to_node: rel.to.clone(),
                direction: rel.direction,
                min_hops: rel.hops.min,
                max_hops: rel.hops.max,
                fk,
                scope_preserving,
                filters: crate::passes::shared::ordered_filters(&rel.filters, None, model),
                join_prev: None,
                scope_proof,
                cascade_anchor: false,
            }
        })
        .collect()
}

fn build_node_plans<M>(input: &Input, model: &M) -> HashMap<String, NodePlan>
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    input
        .nodes
        .iter()
        .filter_map(|n| {
            let entity = n.entity.as_deref()?;
            let entity_id = model.graph().entity_id(entity)?;
            (
                n.id.clone(),
                NodePlan {
                    alias: n.id.clone(),
                    entity: n.entity.clone(),
                    table: model
                        .query_backend()
                        .entity_table(entity_id)
                        .map(String::from),
                    selectivity: Selectivity::from_node(n),
                    hydration: HydrationStrategy::Skip,
                    has_traversal_path: model.query_backend().entity_has_traversal_path(entity_id),
                    is_global: model.query_backend().entity_is_global(entity_id),
                    redaction_id_column: DEFAULT_PRIMARY_KEY.to_string(),
                    filters: crate::passes::shared::ordered_filters(
                        &n.filters
                            .iter()
                            .filter(|(property, _)| {
                                model
                                    .graph()
                                    .property_id(entity_id, property)
                                    .map(|property| model.graph().property(property))
                                    .is_none_or(|property| {
                                        !matches!(
                                            property.realization,
                                            query_data_model::PropertyRealization::Virtual(_)
                                        )
                                    })
                            })
                            .map(|(property, filters)| (property.clone(), filters.clone()))
                            .collect(),
                        Some(entity_id),
                        model,
                    ),
                    node_ids: n.node_ids.clone(),
                    id_range: n.id_range.clone(),
                    columns: n.columns.as_ref().map(|columns| match columns {
                        ColumnSelection::All => ColumnSelection::All,
                        ColumnSelection::List(columns) => ColumnSelection::List(
                            columns
                                .iter()
                                .filter(|column| {
                                    model
                                        .graph()
                                        .property_id(entity_id, column)
                                        .map(|property| model.graph().property(property))
                                        .is_none_or(|property| {
                                            !matches!(
                                                property.realization,
                                                query_data_model::PropertyRealization::Virtual(_)
                                            )
                                        })
                                })
                                .cloned()
                                .collect(),
                        ),
                    }),
                    use_narrowing: false,
                    fk_needs_join: false,
                    emit_select: true,
                },
            )
                .into()
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
        .any(|m| m.expr.node() == alias);
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
fn elide_hops(
    hops: Vec<Hop>,
    nodes: &mut HashMap<String, NodePlan>,
    input: &Input,
) -> (Vec<Hop>, Vec<(String, String, String)>, Vec<ScopeProof>) {
    let mut keep_hops = Vec::new();
    let mut elided_fks = Vec::new();
    let mut scope_requirements = Vec::new();

    let mut hop_count: HashMap<String, usize> = HashMap::new();
    for hop in &hops {
        *hop_count.entry(hop.from_node.clone()).or_insert(0) += 1;
        *hop_count.entry(hop.to_node.clone()).or_insert(0) += 1;
    }
    let sole_non_fk = input.query_type == QueryType::Aggregation
        && hops.iter().filter(|h| h.fk.is_none()).count() == 1;

    for hop in hops {
        if sole_non_fk
            && hop.fk.is_none()
            && hop.scope_preserving
            && hop.scope_proof.is_some()
            && hop.filters.is_empty()
            && let Some(anchor) = [hop.from_node.as_str(), hop.to_node.as_str()]
                .into_iter()
                .find(|a| is_pure_scope_anchor(a, nodes, input, &hop_count))
                .map(str::to_string)
        {
            scope_requirements.extend(hop.scope_proof.clone());
            nodes.remove(&anchor);
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
                fk_np.filters.push((
                    fk_column,
                    BoundFilter {
                        filter,
                        data_type: Some(ontology::DataType::Int),
                        selectivity: ontology::FieldSelectivity::High,
                    },
                ));
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
        }
    }

    (keep_hops, elided_fks, scope_requirements)
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
/// FK-backed, single fixed-length, edge-filter-free, not `Both`, and either
/// scope-preserving or reaching a global hub; gated out of point-selective endpoints
/// and non-emittable shapes. The chain must keep one scoped node (the authz anchor).
fn detect_fk_chain(hops: &[Hop], nodes: &HashMap<String, NodePlan>) -> bool {
    let point_selective = |alias: &str| {
        nodes
            .get(alias)
            .is_some_and(|np| matches!(np.selectivity, Selectivity::Pinned | Selectivity::IdRange))
    };
    // Global hub (`global: true`): non-namespaced, reached only via FK; safe to elide past.
    let reaches_global_hub = |h: &Hop| {
        [h.from_node.as_str(), h.to_node.as_str()]
            .iter()
            .any(|a| nodes.get(*a).is_some_and(|np| np.is_global))
    };
    // Authz guard: only keep eliding while the chain retains an in-namespace (scoped) node.
    let has_scope_anchor = || {
        hops.iter().any(|h| {
            [h.from_node.as_str(), h.to_node.as_str()]
                .iter()
                .any(|a| nodes.get(*a).is_some_and(|np| np.has_traversal_path))
        })
    };
    hops.len() >= 2
        && has_scope_anchor()
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

fn determine_hydration(
    node_plan: &NodePlan,
    input: &Input,
    hops: &[Hop],
    denorm_rel_kinds: &HashMap<(String, String, String), Vec<String>>,
) -> HydrationStrategy {
    let alias = &node_plan.alias;

    let is_group_by_node = crate::input::node_group_ids(&input.aggregation.group_by)
        .any(|node| node == alias.as_str());
    let is_group_by_property = input
        .aggregation
        .group_by
        .iter()
        .any(|group| matches!(group, crate::input::InputGroupByKey::Property { node, .. } if node == alias));
    let is_agg_property_target = input.aggregation.metrics.iter().any(|a| {
        a.expr.node() == alias.as_str()
            && a.expr.property().is_some()
            && !matches!(a.expr.function(), AggFunction::Count)
    });
    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == *alias);

    if is_group_by_node || is_group_by_property || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    // Skip the node table only when every filter is carried by a hop's edge
    // tag; an uncovered filter stays on the node table so it isn't dropped.
    let entity = node_plan.entity.as_deref().unwrap_or("");
    let has_uncovered_filter = node_plan
        .filters
        .iter()
        .any(|(prop, _)| !filter_covered_by_denorm(entity, prop, alias, hops, denorm_rel_kinds));

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

    for (target_node, fk_node, fk_column) in elided_fks {
        mappings
            .entry(target_node.clone())
            .or_insert_with(|| (fk_node.clone(), fk_column.clone()));
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn node(alias: &str, has_traversal_path: bool, is_global: bool) -> NodePlan {
        NodePlan {
            alias: alias.to_string(),
            entity: None,
            table: None,
            selectivity: Selectivity::Filtered,
            hydration: HydrationStrategy::Skip,
            filters: Vec::new(),
            node_ids: Vec::new(),
            id_range: None,
            has_traversal_path,
            is_global,
            redaction_id_column: DEFAULT_PRIMARY_KEY.to_string(),
            columns: None,
            use_narrowing: false,
            fk_needs_join: false,
            emit_select: true,
        }
    }

    fn fk_hop(from: &str, to: &str, scope_preserving: bool) -> Hop {
        Hop {
            rel_types: vec!["REL".to_string()],
            edge_table: "gl_edge".to_string(),
            from_node: from.to_string(),
            to_node: to.to_string(),
            direction: Direction::Outgoing,
            min_hops: 1,
            max_hops: 1,
            fk: Some(HopFk {
                fk_node: from.to_string(),
                fk_column: "fk_id".to_string(),
                target_node: to.to_string(),
            }),
            filters: Vec::new(),
            join_prev: None,
            scope_proof: None,
            scope_preserving,
            cascade_anchor: false,
        }
    }

    fn node_map(pairs: &[(&str, bool, bool)]) -> HashMap<String, NodePlan> {
        pairs
            .iter()
            .map(|(a, tp, global)| (a.to_string(), node(a, *tp, *global)))
            .collect()
    }

    #[test]
    fn fk_chain_with_global_hub_and_scope_anchor_elides() {
        let hops = [fk_hop("a", "b", true), fk_hop("b", "hub", false)];
        let nodes = node_map(&[("a", true, false), ("b", true, false), ("hub", false, true)]);
        assert!(detect_fk_chain(&hops, &nodes));
    }

    #[test]
    fn fk_chain_all_global_hubs_does_not_elide() {
        // every node is global: no scope anchor, so the chain must stay on the edge path.
        let hops = [fk_hop("h1", "h2", false), fk_hop("h2", "h3", false)];
        let nodes = node_map(&[
            ("h1", false, true),
            ("h2", false, true),
            ("h3", false, true),
        ]);
        assert!(!detect_fk_chain(&hops, &nodes));
    }

    #[test]
    fn fk_chain_non_scope_preserving_between_scoped_nodes_does_not_elide() {
        // non-scope-preserving hop with no global endpoint could cross namespaces, so it must not elide.
        let hops = [fk_hop("a", "b", true), fk_hop("b", "c", false)];
        let nodes = node_map(&[("a", true, false), ("b", true, false), ("c", true, false)]);
        assert!(!detect_fk_chain(&hops, &nodes));
    }
}
