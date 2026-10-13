use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use super::context::PlanningContext;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::query::Query;

use super::{
    Aggregation, BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    QueryPlan, Traversal,
};
use query_data_model::OrbitQueryModel;

pub struct Hop {
    pub input_index: usize,
    pub rel_types: Vec<String>,
    pub relationships: Vec<query_data_model::RelationshipId>,
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
    pub prev_index: usize,
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
    pub referenced_column: String,
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
    pub(super) fn from_query(node: &crate::query::Node, strip_virtuals: bool) -> Self {
        let columns = node
            .columns
            .iter()
            .filter(|property| !strip_virtuals || property.is_stored())
            .filter_map(|property| property.column.clone())
            .collect();
        let filters = node
            .filters
            .iter()
            .filter(|filter| !strip_virtuals || filter.stored)
            .map(|filter| (filter.name.clone(), super::BoundFilter::from(filter)))
            .collect();
        Self {
            alias: node.alias.clone(),
            entity: Some(node.entity_name.clone()),
            table: Some(node.table.clone()),
            selectivity: Selectivity::from_query_node(node),
            hydration: HydrationStrategy::Skip,
            filters,
            node_ids: node.node_ids.clone(),
            id_range: node.id_range.clone(),
            has_traversal_path: node.has_traversal_path,
            is_global: node.is_global,
            redaction_id_column: node.redaction_id_column.clone(),
            columns: Some(ColumnSelection::List(columns)),
            use_narrowing: false,
            fk_needs_join: false,
            emit_select: true,
        }
    }

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
    pub fn from_query_node(node: &crate::query::Node) -> Self {
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

pub(super) fn plan<M>(
    mut context: PlanningContext<'_, M>,
    use_fk_elision: bool,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let input = context.input;
    let model = context.model;
    let hops = build_hops(input, model);
    let mut nodes: HashMap<_, _> = input
        .nodes
        .iter()
        .map(|node| (node.alias.clone(), NodePlan::from_query(node, true)))
        .collect();

    let (mut hops, elided_fks) = if use_fk_elision {
        elide_hops(hops, &mut nodes, input, model)
    } else {
        (hops, Vec::new())
    };

    let (reordered_hops, reversed) = reorder_by_selectivity(hops, &nodes);
    hops = reordered_hops;
    let _ = reversed;
    let denormalized = super::denormalized_facts(input, model);

    for node_plan in nodes.values_mut() {
        if use_fk_elision {
            node_plan.hydration = determine_hydration(
                node_plan,
                input,
                &hops,
                &denormalized,
                table_scans.contains(&node_plan.alias),
            );
        } else {
            node_plan.hydration = HydrationStrategy::Join;
        }
    }

    resolve_join_columns(&mut hops)?;
    resolve_cascade_anchors(&mut hops);

    resolve_node_flags(&hops, &mut nodes, input);

    if input.query_type == QueryType::Aggregation {
        let group_by_nodes: HashSet<&str> = input
            .aggregation
            .groups
            .iter()
            .filter(|&group| matches!(group, crate::query::Group::Node { .. }))
            .map(|group| input.node(group.node()).alias.as_str())
            .collect();
        for np in nodes.values_mut() {
            np.emit_select = group_by_nodes.contains(np.alias.as_str());
        }
    } else if !use_fk_elision {
        for np in nodes.values_mut() {
            np.emit_select = true;
        }
    }

    context.hops = hops;
    context.nodes = nodes;
    context.denormalized = denormalized;
    let execution = if context.hops.is_empty() {
        context.single_node()?
    } else if use_fk_elision && let Some(center) = detect_fk_star(&context.hops) {
        super::fk::star(&context, &center)?
    } else if use_fk_elision && detect_fk_chain(&context.hops, &context.nodes) {
        super::fk::chain(&context)?
    } else {
        super::flat::plan(&context)?
    };
    if !context.hops.is_empty() {
        context.node_edge_mappings = execution
            .bindings
            .iter()
            .map(|binding| {
                (
                    binding.node.clone(),
                    (binding.alias.clone(), binding.column.clone()),
                )
            })
            .collect();
    }
    for (target, holder, column) in elided_fks {
        context
            .node_edge_mappings
            .entry(target)
            .or_insert((holder, column));
    }
    Ok(if input.query_type == QueryType::Aggregation {
        let result = context.aggregation(&execution);
        QueryPlan::Aggregation(context.finish(Aggregation { execution, result }))
    } else {
        QueryPlan::Traversal(context.finish(Traversal { execution }))
    })
}

fn build_hops<M>(input: &Query, model: &M) -> Vec<Hop>
where
    M: OrbitQueryModel + ?Sized,
{
    input
        .relationships
        .iter()
        .enumerate()
        .map(|(input_index, rel)| {
            let edge_table = if crate::query::is_wildcard(&rel.names) {
                model.default_edge_table().to_string()
            } else {
                rel.tables
                    .first()
                    .cloned()
                    .unwrap_or_else(|| model.default_edge_table().to_string())
            };
            let from = input.node(rel.from);
            let to = input.node(rel.to);
            let fk = match rel.direction {
                Direction::Outgoing => {
                    model.foreign_key(&rel.names, &from.entity_name, &to.entity_name)
                }
                Direction::Incoming => {
                    model.foreign_key(&rel.names, &to.entity_name, &from.entity_name)
                }
                Direction::Both => None,
            }
            .and_then(|foreign_key| {
                let fk_column = model.property_column(foreign_key.property)?.to_string();
                let referenced_column = model
                    .property_column(foreign_key.referenced_key)?
                    .to_string();
                let holder_is_from = matches!(
                    (rel.direction, foreign_key.holder),
                    (Direction::Outgoing, query_data_model::Endpoint::Source)
                        | (Direction::Incoming, query_data_model::Endpoint::Target)
                );
                let (fk_node, target_node) = if holder_is_from {
                    (from.alias.clone(), to.alias.clone())
                } else {
                    (to.alias.clone(), from.alias.clone())
                };
                Some(HopFk {
                    fk_node,
                    fk_column,
                    target_node,
                    referenced_column,
                })
            });
            let scope_preserving = !rel.names.is_empty()
                && rel.names.iter().all(|kind| {
                    model
                        .variant_scope(kind, &from.entity_name, &to.entity_name)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                        || model
                            .variant_scope(kind, &to.entity_name, &from.entity_name)
                            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
            let filters = rel
                .filters
                .iter()
                .map(|filter| (filter.name.clone(), super::BoundFilter::from(filter)))
                .collect();
            Hop {
                input_index,
                rel_types: rel.names.clone(),
                relationships: rel.ids.clone(),
                edge_table,
                from_node: from.alias.clone(),
                to_node: to.alias.clone(),
                direction: rel.direction,
                min_hops: rel.hops.min,
                max_hops: rel.hops.max,
                fk,
                scope_preserving,
                filters,
                join_prev: None,
                cascade_anchor: false,
            }
        })
        .collect()
}

#[allow(clippy::type_complexity)]
fn elide_hops(
    hops: Vec<Hop>,
    nodes: &mut HashMap<String, NodePlan>,
    input: &Query,
    model: &(impl OrbitQueryModel + ?Sized),
) -> (Vec<Hop>, Vec<(String, String, String)>) {
    let mut keep_hops = Vec::new();
    let mut elided_fks = Vec::new();
    for hop in hops {
        // Only elide if at least one non-FK hop would remain — otherwise
        // the emit loop has no edges to populate node_edge_col from.
        let would_be_last = keep_hops.is_empty();

        let elide_info = hop.fk.as_ref().and_then(|fk| {
            if would_be_last
                || input
                    .relationships
                    .iter()
                    .filter(|relationship| {
                        input.node(relationship.from).alias == fk.target_node
                            || input.node(relationship.to).alias == fk.target_node
                    })
                    .count()
                    > 1
                || input.comparisons.iter().any(|predicate| {
                    [
                        &input.node(predicate.left.node).alias,
                        &input.node(predicate.right.node).alias,
                    ]
                    .into_iter()
                    .any(|node| node == &hop.from_node || node == &hop.to_node)
                })
            {
                return None;
            }
            let np = nodes.get(&fk.target_node)?;
            let needs_role_scan = np.entity.as_deref().is_some_and(|entity| {
                model
                    .entity_minimum_access_level(entity)
                    .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
            });
            let needs_target_values = input.query_type != QueryType::Aggregation
                || input
                    .aggregation
                    .groups
                    .iter()
                    .any(|group| input.node(group.node()).alias == fk.target_node)
                || input.aggregation.metrics.iter().any(|metric| {
                    input.node(metric.target).alias == fk.target_node && metric.property.is_some()
                })
                || input
                    .order
                    .as_ref()
                    .is_some_and(|order| input.node(order.node).alias == fk.target_node);
            if needs_role_scan || (np.node_ids.len() > 1 && needs_target_values) {
                return None;
            }
            if np.selectivity == Selectivity::Pinned
                && fk.referenced_column == DEFAULT_PRIMARY_KEY
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
                        property: None,
                        data_type: Some(ontology::DataType::Int),
                        selectivity: ontology::FieldSelectivity::High,
                        in_sort_key: false,
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

    (keep_hops, elided_fks)
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
    input: &Query,
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    requires_table_scan: bool,
) -> HydrationStrategy {
    let alias = &node_plan.alias;

    let is_group_by_node = input.aggregation.groups.iter().any(|group| {
        matches!(group, crate::query::Group::Node { .. })
            && input.node(group.node()).alias == *alias
    });
    let is_group_by_property = input
        .aggregation
        .groups
        .iter()
        .any(|group| matches!(group, crate::query::Group::Property { node, .. } if input.node(*node).alias == *alias));
    let is_agg_property_target = input.aggregation.metrics.iter().any(|a| {
        input.node(a.target).alias == *alias
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });
    let is_order_by_target = input
        .order
        .as_ref()
        .is_some_and(|order| input.node(order.node).alias == *alias);
    let is_join_predicate_target = input.comparisons.iter().any(|predicate| {
        [
            (
                &input.node(predicate.left.node).alias,
                &predicate.left.property.name,
            ),
            (
                &input.node(predicate.right.node).alias,
                &predicate.right.property.name,
            ),
        ]
        .into_iter()
        .any(|(node, property)| node == alias && property != DEFAULT_PRIMARY_KEY)
    });

    if is_group_by_node
        || is_group_by_property
        || is_agg_property_target
        || is_order_by_target
        || is_join_predicate_target
    {
        return HydrationStrategy::Join;
    }

    // Skip the node table only when every filter is carried by a hop's edge
    // tag; an uncovered filter stays on the node table so it isn't dropped.
    let has_uncovered_filter = node_plan
        .filters
        .iter()
        .any(|(_, filter)| !filter_covered_by_denorm(filter, alias, hops, denormalized));

    if has_uncovered_filter {
        HydrationStrategy::FilterOnly
    } else if requires_table_scan {
        HydrationStrategy::Join
    } else {
        HydrationStrategy::Skip
    }
}

// Mirrors the lowerer's `emit_denorm_tags`: the hydration decision and the tag
// push must agree on which hop carries a denorm.
fn filter_covered_by_denorm(
    filter: &BoundFilter,
    alias: &str,
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> bool {
    hops.iter().any(|hop| {
        if crate::query::is_wildcard(&hop.rel_types) {
            return false;
        }
        let (start_col, end_col) = hop.direction.edge_columns();
        [(&hop.from_node, start_col), (&hop.to_node, end_col)]
            .iter()
            .any(|(node, id_col)| {
                if node.as_str() != alias {
                    return false;
                }
                let direction = if *id_col == SOURCE_ID_COLUMN {
                    DenormalizedDirection::Source
                } else {
                    DenormalizedDirection::Target
                };
                super::helpers::lowerable_denorm(filter, direction, denormalized).is_some_and(
                    |facts| {
                        hop.relationships
                            .iter()
                            .any(|relationship| facts.relationships.contains(relationship))
                    },
                )
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

fn resolve_join_columns(hops: &mut [Hop]) -> Result<()> {
    let Some(first) = hops.first() else {
        return Ok(());
    };
    let mut reached = HashSet::from([first.from_node.clone(), first.to_node.clone()]);
    for i in 1..hops.len() {
        let next = (i..hops.len())
            .find(|&index| {
                reached.contains(&hops[index].from_node) != reached.contains(&hops[index].to_node)
            })
            .ok_or_else(|| {
                QueryError::Validation("relationships must form a connected tree".into())
            })?;
        hops[i..=next].rotate_right(1);
        let curr_hop = &hops[i];
        let (start_col, end_col) = curr_hop.direction.edge_columns();
        let join = hops[..i]
            .iter()
            .enumerate()
            .rev()
            .find_map(|(prev_index, previous)| {
                let (prev_start, prev_end) = previous.direction.edge_columns();
                let (prev_col, curr_col) = if previous.to_node == curr_hop.from_node {
                    (prev_end, start_col)
                } else if previous.to_node == curr_hop.to_node {
                    (prev_end, end_col)
                } else if previous.from_node == curr_hop.from_node {
                    (prev_start, start_col)
                } else if previous.from_node == curr_hop.to_node {
                    (prev_start, end_col)
                } else {
                    return None;
                };
                Some(JoinColumns {
                    prev_index,
                    prev_col: prev_col.into(),
                    curr_col: curr_col.into(),
                })
            });
        reached.insert(curr_hop.from_node.clone());
        reached.insert(curr_hop.to_node.clone());
        hops[i].join_prev = join;
    }
    Ok(())
}

fn resolve_node_flags(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>, input: &Query) {
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
        let needs = fk.referenced_column != DEFAULT_PRIMARY_KEY
            || np.hydration == HydrationStrategy::Join
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
            relationships: Vec::new(),
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
                referenced_column: DEFAULT_PRIMARY_KEY.to_string(),
            }),
            filters: Vec::new(),
            join_prev: None,
            input_index: 0,
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
