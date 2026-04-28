//! AST optimization pass.
//!
//! Runs after lowering — before enforce, security, and check.
//! Rewrites the AST for better ClickHouse performance without changing
//! query semantics.
//!
//! ## `fold_filters_into_aggregates`
//!
//! Converts WHERE-filtered aggregations into ClickHouse `-If` combinators:
//!
//! ```sql
//! -- before
//! SELECT p.name, COUNT(mr.id) FROM ... WHERE mr.state = 'merged' GROUP BY p.name
//!
//! -- after
//! SELECT p.name, countIf(mr.id, mr.state = 'merged') FROM ... GROUP BY p.name
//! ```
//!
//! This avoids materializing per-filter hash tables in the aggregation engine.
//! Each `-If` aggregate maintains one counter per group regardless of data volume.
//! See: <https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if>

use std::collections::{HashMap, HashSet};

use crate::ast::{ChType, Cte, Expr, Node, Query, SelectExpr, TableRef};
use crate::constants::{
    BACKWARD_CTE, CASCADE_EDGE_ALIAS, END_ID_COLUMN, FORWARD_CTE, HOP_EDGE_ALIAS, START_ID_COLUMN,
    cascade_cte, node_filter_cte, skip_security_filter_tables,
};
use crate::input::{Direction, Input, InputNode, QueryType};

use ontology::constants::{
    DEFAULT_PRIMARY_KEY, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};

const ROOT_SIP_CTE: &str = "_root_ids";

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node, input: &mut Input) {
    match node {
        Node::Insert(_) => {}
        Node::Query(q) => {
            inject_entity_kind_filters(q, input);
            push_kind_literals_into_variable_length_arms(q, input);
            if input.query_type == QueryType::Aggregation {
                inject_agg_group_by_kind_filters(q, input);
            }
            apply_sip_prefilter(q, input);
            apply_nonroot_node_ids_to_edges(q, input);
            apply_edge_led_reorder(q, input);
            if input.query_type == QueryType::Traversal && input.relationships.len() > 1 {
                cascade_node_filter_ctes(q, input);
            }
            if input.query_type == QueryType::Traversal {
                narrow_joined_nodes_via_pinned_neighbors(q, input);
            }
            if input.query_type == QueryType::Aggregation {
                apply_target_sip_prefilter(q, input);
                fold_filters_into_aggregates(q, input);
                prune_unreferenced_node_joins(q, input);
            }
            if matches!(
                input.query_type,
                QueryType::Traversal | QueryType::Aggregation
            ) {
                apply_traversal_hop_frontiers(q, input);
            }
            if input.query_type == QueryType::PathFinding {
                apply_path_hop_frontiers(q, input);
            }
        }
    }
}

/// Drop node-table joins whose alias has no role in the result: not in any
/// `aggregations.{target, group_by}`, no filters, no `node_ids`, and not the
/// query root. Edge joins to the pruned node stay as existence semi-joins,
/// so row counts are unchanged.
fn prune_unreferenced_node_joins(q: &mut Query, input: &Input) {
    if input.query_type != QueryType::Aggregation || input.relationships.is_empty() {
        return;
    }

    let mut referenced: HashSet<String> = HashSet::new();
    for agg in &input.aggregations {
        if let Some(t) = &agg.target {
            referenced.insert(t.clone());
        }
        if let Some(g) = &agg.group_by {
            referenced.insert(g.clone());
        }
    }
    for n in &input.nodes {
        if !n.node_ids.is_empty() || !n.filters.is_empty() {
            referenced.insert(n.id.clone());
        }
    }
    let root_alias = input
        .relationships
        .first()
        .map(|r| r.from.clone())
        .or_else(|| input.nodes.first().map(|n| n.id.clone()));
    if let Some(root) = root_alias {
        referenced.insert(root);
    }

    // Count how many relationships touch each node alias. Only leaf nodes
    // (degree ≤ 1) are safe to prune — pruning an intermediate node would
    // leave the adjacent edge JOINs dangling on the now-undefined alias.
    // Example: `User -- AUTHORED --> MR -- HAS_NOTE --> Note` with MR
    // unreferenced in the aggregation. e1's `ON mr.id = e1.source_id` would
    // reference a missing `mr` alias after pruning.
    let mut degree: HashMap<&str, usize> = HashMap::new();
    for rel in &input.relationships {
        *degree.entry(rel.from.as_str()).or_default() += 1;
        *degree.entry(rel.to.as_str()).or_default() += 1;
    }

    let prune: HashSet<String> = input
        .nodes
        .iter()
        .filter(|n| {
            !referenced.contains(&n.id) && degree.get(n.id.as_str()).copied().unwrap_or(0) <= 1
        })
        .map(|n| n.id.clone())
        .collect();
    if prune.is_empty() {
        return;
    }

    prune_table_joins(&mut q.from, &prune);

    if let Some(w) = q.where_clause.take() {
        let kept: Vec<Expr> = w
            .flatten_and()
            .into_iter()
            .filter(|c| !c.column_aliases().iter().any(|a| prune.contains(a)))
            .collect();
        q.where_clause = Expr::conjoin(kept);
    }

    q.ctes.retain(|c| {
        !prune.iter().any(|alias| {
            c.name == node_filter_cte(alias)
                || c.name == format!("_cascade_{alias}")
                || c.name == format!("_target_{alias}_ids")
        })
    });
}

/// Walk the FROM tree and replace `Join { right: TableRef::Subquery|Scan { alias ∈ prune } }`
/// with the left side. Recurses into left subtree first to handle nested joins.
fn prune_table_joins(table: &mut TableRef, prune: &HashSet<String>) {
    loop {
        match table {
            TableRef::Join { left, right, .. } => {
                prune_table_joins(left, prune);
                let right_alias = match right.as_ref() {
                    TableRef::Scan { alias, .. } => Some(alias.clone()),
                    TableRef::Subquery { alias, .. } => Some(alias.clone()),
                    _ => None,
                };
                let should_prune = right_alias.is_some_and(|a| prune.contains(&a));
                if !should_prune {
                    return;
                }
                let mut placeholder = TableRef::Scan {
                    table: String::new(),
                    alias: String::new(),
                    final_: false,
                };
                std::mem::swap(left.as_mut(), &mut placeholder);
                *table = placeholder;
            }
            _ => return,
        }
    }
}

/// Inject `source_kind`/`target_kind` filters for each node with a known
/// entity type. Gives ClickHouse an extra predicate for granule pruning on
/// the `by_source`/`by_target` projections whose PK includes the kind column.
///
/// Iterates relationships first so that a node shared between multiple
/// relationships gets a kind filter on every edge it touches. Without this,
/// a query like `Project ↔ MR ↔ User` only constrains MR via `e0` and the
/// `e1` join can match `User AUTHORED <any entity with that ID>` rows,
/// producing edges with the wrong `target_kind` in the result and missing
/// kind-PK pruning on the second-hop edge.
fn inject_entity_kind_filters(q: &mut Query, input: &Input) {
    let node_edge_col = &input.compiler.node_edge_col;
    let entity_for: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|n| n.entity.as_deref().map(|e| (n.id.as_str(), e)))
        .collect();

    let mut kind_filters: Vec<Expr> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut push_filter = |alias: &str, kind_col: &'static str, entity: &str| {
        if seen.insert(format!("{alias}.{kind_col}={entity}")) {
            kind_filters.push(Expr::eq(
                Expr::col(alias, kind_col),
                Expr::param(ChType::String, entity.to_string()),
            ));
        }
    };

    // Edge endpoints: for each relationship's edge alias, constrain BOTH
    // sides if the corresponding node has a known entity type.
    for (i, rel) in input.relationships.iter().enumerate() {
        let edge_alias = if rel.max_hops > 1 {
            format!("hop_e{i}")
        } else {
            format!("e{i}")
        };
        let (start_col, end_col) = if rel.max_hops > 1 {
            rel.direction.union_columns()
        } else {
            rel.direction.edge_columns()
        };
        if let (Some(entity), Some(kind_col)) = (
            entity_for.get(rel.from.as_str()),
            edge_kind_column(start_col),
        ) {
            push_filter(&edge_alias, kind_col, entity);
        }
        if let (Some(entity), Some(kind_col)) =
            (entity_for.get(rel.to.as_str()), edge_kind_column(end_col))
        {
            push_filter(&edge_alias, kind_col, entity);
        }
    }

    // Single-edge query types (Search/Neighbors) don't have relationships
    // but still rely on `node_edge_col` for kind injection.
    for node in &input.nodes {
        if let Some(entity) = &node.entity
            && let Some((alias, edge_col)) = node_edge_col.get(&node.id)
            && let Some(kind_col) = edge_kind_column(edge_col)
        {
            push_filter(alias, kind_col, entity);
        }
    }

    if !kind_filters.is_empty() {
        let mut parts: Vec<Expr> = q.where_clause.take().into_iter().collect();
        parts.extend(kind_filters);
        q.where_clause = Expr::conjoin(parts);
    }
}

/// Push static `source_kind`/`target_kind = '<entity>'` literals into each
/// arm of a variable-length traversal's UNION ALL.
///
/// Lowering already constrains kinds at the OUTER alias (`hop_e{i}`), but
/// ClickHouse will not propagate those into the arm's per-edge scans. The
/// literals are static, so each inner edge scan can use the kind-led PK
/// projection (`by_rel_source_kind` / `by_rel_target_kind`) for granule
/// pruning. Dynamic IN-subqueries cannot — they force per-row hash probes.
fn push_kind_literals_into_variable_length_arms(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    if input.relationships.is_empty() {
        return;
    }

    for (i, rel) in input.relationships.iter().enumerate() {
        if rel.max_hops <= 1 {
            continue;
        }

        let from_entity = input
            .nodes
            .iter()
            .find(|n| n.id == rel.from)
            .and_then(|n| n.entity.as_deref());
        let to_entity = input
            .nodes
            .iter()
            .find(|n| n.id == rel.to)
            .and_then(|n| n.entity.as_deref());
        if from_entity.is_none() && to_entity.is_none() {
            continue;
        }

        // For Outgoing/Both: e1.source_id = rel.from, e<depth>.target_id = rel.to.
        // For Incoming: e1.target_id = rel.from, e<depth>.source_id = rel.to.
        let (from_kind_col, to_kind_col) = match rel.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
        };

        let alias = format!("hop_e{i}");
        let union_ref = match find_union_mut(&mut q.from, &alias) {
            Some(u) => u,
            None => continue,
        };
        let TableRef::Union { queries, .. } = union_ref else {
            continue;
        };

        let start = rel.min_hops.max(1);
        for (arm_idx, arm) in queries.iter_mut().enumerate() {
            let depth = start + arm_idx as u32;
            let mut filters: Vec<Expr> = Vec::new();
            if let Some(ent) = from_entity {
                filters.push(Expr::eq(
                    Expr::col("e1", from_kind_col),
                    Expr::param(ChType::String, ent.to_string()),
                ));
            }
            if let Some(ent) = to_entity {
                let last = format!("e{depth}");
                filters.push(Expr::eq(
                    Expr::col(&last, to_kind_col),
                    Expr::param(ChType::String, ent.to_string()),
                ));
            }
            if filters.is_empty() {
                continue;
            }
            let mut parts: Vec<Expr> = arm.where_clause.take().into_iter().collect();
            parts.extend(filters);
            arm.where_clause = Expr::conjoin(parts);
        }
    }
}

/// Walk a `TableRef` tree to find a `Union` with a given alias.
fn find_union_mut<'a>(table_ref: &'a mut TableRef, alias: &str) -> Option<&'a mut TableRef> {
    let is_match = matches!(
        table_ref,
        TableRef::Union { alias: a, .. } if a == alias
    );
    if is_match {
        return Some(table_ref);
    }
    match table_ref {
        TableRef::Join { left, right, .. } => {
            if let Some(found) = find_union_mut(left, alias) {
                Some(found)
            } else {
                find_union_mut(right, alias)
            }
        }
        _ => None,
    }
}

/// Map an edge ID column to its corresponding entity kind column.
/// Works for both single-hop columns (source_id/target_id) and
/// multi-hop union columns (start_id/end_id).
fn edge_kind_column(edge_col: &str) -> Option<&'static str> {
    match edge_col {
        SOURCE_ID_COLUMN | START_ID_COLUMN => Some(SOURCE_KIND_COLUMN),
        TARGET_ID_COLUMN | END_ID_COLUMN => Some(TARGET_KIND_COLUMN),
        _ => None,
    }
}

/// Eliminate unnecessary node table joins from aggregation queries.
///
/// When an aggregation target node has no filters, no pinned `node_ids`, and
/// only appears in property-less aggregates (e.g. `COUNT`), its table scan
/// can be removed from the FROM tree. The aggregate expression is rewritten
/// to reference the edge column instead (e.g. `COUNT(mr.id)` → `COUNT(e0.source_id)`),
/// and a `source_kind`/`target_kind` filter is added to ensure only edges for
/// the correct entity type are counted.
///
/// Constraints: single relationship, single-hop only.
/// Inject entity kind filters for aggregation group-by nodes.
///
/// Their table JOINs are kept (for property access), but adding the kind
/// predicate to the edge lets ClickHouse prune edges that don't connect
/// to the expected entity before the JOIN.
fn inject_agg_group_by_kind_filters(q: &mut Query, input: &Input) {
    if input.relationships.len() != 1 {
        return;
    }
    let rel = &input.relationships[0];
    if rel.max_hops > 1 {
        return;
    }

    let group_by_ids: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|a| a.group_by.as_deref())
        .collect();

    let edge_alias = "e0";
    let mut gb_kind_filters: Vec<Expr> = Vec::new();
    for gb_id in &group_by_ids {
        let node = match input.nodes.iter().find(|n| n.id == *gb_id) {
            Some(n) => n,
            None => continue,
        };
        let entity = match node.entity.as_deref() {
            Some(e) => e,
            None => continue,
        };
        let (start_col, end_col) = rel.direction.edge_columns();
        let id_col = if *gb_id == rel.from {
            start_col
        } else {
            end_col
        };
        let kind_col = match edge_kind_column(id_col) {
            Some(k) => k,
            None => continue,
        };
        gb_kind_filters.push(Expr::eq(
            Expr::col(edge_alias, kind_col),
            Expr::param(ChType::String, entity.to_string()),
        ));
    }
    if !gb_kind_filters.is_empty() {
        let mut parts: Vec<Expr> = q.where_clause.take().into_iter().collect();
        parts.extend(gb_kind_filters);
        q.where_clause = Expr::conjoin(parts);
    }
}

/// Choose the SIP root: the node with pinned `node_ids` (fewest wins).
/// Falls back to the `from` node of the first relationship.
///
/// For aggregation queries, keep the default from-node when it already has
/// selectivity (filters or node_ids) — the target-SIP pass handles the
/// aggregation target separately and changing the root can produce worse
/// plans. But when the default has no selectivity at all, allow a pinned
/// node to take over.
fn choose_sip_root(input: &Input) -> Option<&InputNode> {
    let first_from = input.relationships.first().map(|r| r.from.as_str())?;
    let default_node = input.nodes.iter().find(|n| n.id == first_from);

    let pinned = input
        .nodes
        .iter()
        .filter(|n| !n.node_ids.is_empty())
        .min_by_key(|n| n.node_ids.len());

    if input.query_type == QueryType::Aggregation {
        let default_has_selectivity =
            default_node.is_some_and(|n| !n.node_ids.is_empty() || !n.filters.is_empty());
        if default_has_selectivity {
            return default_node;
        }
    }

    pinned.or(default_node)
}

/// SIP (Sideways Information Passing) pre-filter.
///
/// Materializes the root node's matching IDs in a CTE and pushes them into
/// the edge table scan via IN subquery. Combined with the namespace-first
/// edge PK `(traversal_path, source_id, relationship_kind)`, the IN filter
/// and startsWith filter work together for precise granule pruning.
///
/// When source_id IN (...) is present without startsWith, ClickHouse selects
/// the `by_source` projection instead. When both are present, the base table
/// PK handles both predicates via prefix matching.
fn apply_sip_prefilter(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    if input.relationships.is_empty() {
        return;
    }
    // Edge-centric traversals handle node filtering via IN subqueries in
    // lower.rs. SIP's root CTE can't extract edge-centric conditions
    // (they reference edge aliases, not node aliases).
    if input.query_type == QueryType::Traversal {
        return;
    }

    let root_node = match choose_sip_root(input) {
        Some(n) => n,
        None => return,
    };

    let has_filters = !root_node.filters.is_empty();
    let has_node_ids = !root_node.node_ids.is_empty();
    let has_id_range = root_node.id_range.is_some();
    let has_explicit_selectivity = has_filters || has_node_ids || has_id_range;

    // When the root node's table was eliminated from the FROM (edge-only
    // aggregation), SIP is only worthwhile if the node has explicit selectivity
    // (filters/node_ids). A traversal_path-only SIP on a large table (e.g. 8M
    // jobs) scans more rows than it saves. For small tables the source_kind
    // filter on the edge already narrows sufficiently.
    let node_is_edge_only = input.compiler.node_edge_col.contains_key(&root_node.id);
    if node_is_edge_only && !has_explicit_selectivity {
        return;
    }

    // Apply SIP when root node has explicit filters OR when its table will
    // get a security filter (startsWith on traversal_path). Tables in
    // skip_security_filter_for_tables won't get security filters,
    // so an unfiltered SIP CTE would push all IDs — skip those.
    let skip = skip_security_filter_tables();
    let root_table_has_security_filter = root_node
        .table
        .as_deref()
        .is_some_and(|t| !skip.iter().any(|s| s == t));

    if !has_explicit_selectivity && !root_table_has_security_filter {
        return;
    }

    let root_alias = &root_node.id;
    let root_table = match &root_node.table {
        Some(t) => t.clone(),
        None => return,
    };

    // Build the CTE: SELECT id FROM root_table WHERE <root-only filters>
    // Extract only WHERE conjuncts that reference the root node alias.
    // The security pass will inject startsWith(traversal_path, ...) automatically.
    let root_only_conds = q
        .where_clause
        .as_ref()
        .map(|w| {
            let conjuncts = w.clone().flatten_and();
            conjuncts
                .into_iter()
                .filter(|c| {
                    let aliases = c.column_aliases();
                    !aliases.is_empty() && aliases.iter().all(|a| a == root_alias)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let cte_where = Expr::and_all(root_only_conds.into_iter().map(Some));

    // Reuse an existing _nf_* CTE for the root node if one was already
    // created by the lowerer. ClickHouse inlines CTEs, so duplicating
    // the same query under two names doubles the scan.
    let existing_nf = node_filter_cte(root_alias);
    let sip_cte_name = if q.ctes.iter().any(|c| c.name == existing_nf) {
        existing_nf
    } else {
        let cte_query = Query {
            select: vec![SelectExpr::new(
                Expr::col(root_alias, DEFAULT_PRIMARY_KEY),
                DEFAULT_PRIMARY_KEY,
            )],
            from: TableRef::scan(&root_table, root_alias),
            where_clause: cte_where,
            ..Default::default()
        };
        q.ctes.push(Cte::new(ROOT_SIP_CTE, cte_query));
        ROOT_SIP_CTE.to_string()
    };

    // Inject root SIP into edges adjacent to the root node.
    let mut node_ctes: HashMap<String, String> = HashMap::new();
    node_ctes.insert(root_alias.clone(), sip_cte_name.clone());

    for (i, rel) in input.relationships.iter().enumerate() {
        let from_cte = node_ctes.get(&rel.from).cloned();
        let to_cte = node_ctes.get(&rel.to).cloned();
        let (start_col, end_col) = rel.direction.edge_columns();
        let edge_alias = if rel.max_hops > 1 {
            format!("hop_e{i}")
        } else {
            format!("e{i}")
        };
        let aliases = HashSet::from([edge_alias]);

        if let Some(ref cte) = from_cte {
            inject_sip_for_aliases(
                &mut q.from,
                &mut q.where_clause,
                start_col,
                cte,
                &aliases,
                input,
                ArmAnchor::First,
            );
        }
        if let Some(ref cte) = to_cte {
            // For variable-length arms whose endpoints both carry static kind
            // literals (from `push_kind_literals_into_variable_length_arms`),
            // the per-arm IN-subquery is redundant: the outer to-side node
            // table is JOIN'd with the same cascade CTE, so the arm rows are
            // already filtered. Skipping the per-row hash probe inside every
            // arm lets the kind-led PK projection do its work.
            let arms_have_kind_literals = rel.max_hops > 1
                && input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.from)
                    .is_some_and(|n| n.entity.is_some())
                && input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.to)
                    .is_some_and(|n| n.entity.is_some());

            if !arms_have_kind_literals {
                inject_sip_for_aliases(
                    &mut q.from,
                    &mut q.where_clause,
                    end_col,
                    cte,
                    &aliases,
                    input,
                    ArmAnchor::Last,
                );
            }
        }

        // Cascading SIP: when the root is selective (node_ids, filters, etc.),
        // chain CTEs through relationships so every edge AND node table scan
        // gets narrowed. Skip cascades for broad roots (e.g. "all MRs") where
        // the cascade CTE itself would scan as many edge rows as the main query.
        if !has_explicit_selectivity {
            continue;
        }

        // Skip cascade for edge-only nodes: their table is absent from FROM,
        // so the cascade would be built but never referenced.
        let to_edge_only = input.compiler.node_edge_col.contains_key(&rel.to);
        let from_edge_only = input.compiler.node_edge_col.contains_key(&rel.from);

        // Use multi-hop cascade for max_hops > 1, single-hop for max_hops == 1.
        let build_cascade =
            |node_alias: &str, select_col: &str, filter_col: &str, parent: &str| -> Option<Query> {
                if rel.max_hops > 1 {
                    build_multihop_cascade_for_node(
                        input,
                        node_alias,
                        select_col,
                        filter_col,
                        parent,
                        &rel.types,
                        rel.max_hops,
                    )
                } else {
                    build_cascade_for_node(
                        input, node_alias, select_col, filter_col, parent, &rel.types,
                    )
                }
            };

        if from_cte.is_some()
            && to_cte.is_none()
            && !to_edge_only
            && let Some(cte) =
                build_cascade(&rel.to, end_col, start_col, from_cte.as_ref().unwrap())
        {
            let name = cascade_cte(&rel.to);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.to.clone(), name);
        }
        if to_cte.is_some()
            && from_cte.is_none()
            && !from_edge_only
            && let Some(cte) =
                build_cascade(&rel.from, start_col, end_col, to_cte.as_ref().unwrap())
        {
            let name = cascade_cte(&rel.from);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.from.clone(), name);
        }
    }

    // Inject cascade CTE filters into node table scans. Each non-root node
    // with a cascade CTE gets `node.id IN (SELECT id FROM cascade_cte)`,
    // allowing ClickHouse to prewhere-filter large node tables (e.g. gl_job).
    // Edge-only nodes have no table in FROM, so referencing `node.id` would
    // emit a bare identifier ClickHouse interprets as a database name.
    for (alias, cte_name) in &node_ctes {
        if *cte_name == sip_cte_name {
            continue;
        }
        if input.compiler.node_edge_col.contains_key(alias) {
            continue;
        }
        let node_filter = Expr::InSubquery {
            expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
            cte_name: cte_name.clone(),
            column: DEFAULT_PRIMARY_KEY.to_string(),
        };
        q.where_clause = Expr::and_all([q.where_clause.take(), Some(node_filter)]);
    }
}

/// Build a cascade CTE that selects reachable node IDs by following edges
/// from a parent CTE.
///
/// Generates: `SELECT {select_col} AS id FROM gl_edge WHERE {filter_col} IN (parent) AND relationship_kind = ...`
///
/// Safe for all tables including gl_user: cascade CTEs are only created when
/// the root has explicit selectivity, so the parent CTE produces few IDs.
fn build_cascade_for_node(
    input: &Input,
    node_alias: &str,
    select_col: &str,
    filter_col: &str,
    parent_cte: &str,
    rel_types: &[String],
) -> Option<Query> {
    let node = input.nodes.iter().find(|n| n.id == node_alias)?;
    node.table.as_deref()?;

    let alias = CASCADE_EDGE_ALIAS;
    let parent_filter = Expr::InSubquery {
        expr: Box::new(Expr::col(alias, filter_col)),
        cte_name: parent_cte.to_string(),
        column: DEFAULT_PRIMARY_KEY.to_string(),
    };
    let rel_filter = if rel_types.len() == 1 {
        Expr::eq(
            Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
            Expr::param(ChType::String, rel_types[0].clone()),
        )
    } else {
        Expr::col_in(
            alias,
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            rel_types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, true))
    };

    // Filter by entity kind on the selected side (source_kind / target_kind)
    // so the cascade only picks up IDs of the correct type.
    let kind_col = if select_col == SOURCE_ID_COLUMN {
        SOURCE_KIND_COLUMN
    } else {
        TARGET_KIND_COLUMN
    };
    let kind_filter = node.entity.as_ref().map(|entity| {
        Expr::eq(
            Expr::col(alias, kind_col),
            Expr::param(ChType::String, entity.clone()),
        )
    });

    let tables = input.compiler.resolve_edge_tables(rel_types);
    let from = if tables.len() == 1 {
        TableRef::scan(&tables[0], alias)
    } else {
        let queries = tables
            .iter()
            .map(|t| Query {
                select: vec![SelectExpr::star()],
                from: TableRef::scan(t, alias),
                ..Default::default()
            })
            .collect();
        TableRef::union_all(queries, alias)
    };

    Some(Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, select_col),
            DEFAULT_PRIMARY_KEY,
        )],
        from,
        where_clause: Expr::and_all([Some(parent_filter), Some(rel_filter), kind_filter]),
        ..Default::default()
    })
}

/// Build a multi-hop cascade CTE: UNION ALL of edge chains from depth 1
/// to `max_hops`. Each arm is a self-join chain anchored on `parent_cte`.
///
/// For `max_hops=2`, `select_col=target_id`, `filter_col=source_id`:
/// ```sql
/// SELECT ce.target_id AS id FROM gl_edge ce
///   WHERE ce.source_id IN (parent) AND rel_kind = 'T' AND ce.target_kind = 'E'
/// UNION ALL
/// SELECT e2.target_id AS id FROM gl_edge e1 JOIN gl_edge e2 ON e1.target_id = e2.source_id
///   WHERE e1.source_id IN (parent) AND e1.rel_kind = 'T' AND e2.rel_kind = 'T' AND e2.target_kind = 'E'
/// ```
fn build_multihop_cascade_for_node(
    input: &Input,
    node_alias: &str,
    select_col: &str,
    filter_col: &str,
    parent_cte: &str,
    rel_types: &[String],
    max_hops: u32,
) -> Option<Query> {
    let node = input.nodes.iter().find(|n| n.id == node_alias)?;
    node.table.as_deref()?;

    let tables = input.compiler.resolve_edge_tables(rel_types);
    let rel_filter_expr = |alias: &str| -> Expr {
        if rel_types.len() == 1 {
            Expr::eq(
                Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
                Expr::param(ChType::String, rel_types[0].clone()),
            )
        } else {
            Expr::col_in(
                alias,
                RELATIONSHIP_KIND_COLUMN,
                ChType::String,
                rel_types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            )
            .unwrap_or_else(|| Expr::param(ChType::Bool, true))
        }
    };

    let kind_col = if select_col == SOURCE_ID_COLUMN {
        SOURCE_KIND_COLUMN
    } else {
        TARGET_KIND_COLUMN
    };
    let kind_filter = |alias: &str| -> Option<Expr> {
        node.entity.as_ref().map(|entity| {
            Expr::eq(
                Expr::col(alias, kind_col),
                Expr::param(ChType::String, entity.clone()),
            )
        })
    };

    let edge_scan = |alias: &str| -> TableRef {
        if tables.len() == 1 {
            TableRef::scan(&tables[0], alias)
        } else {
            let queries = tables
                .iter()
                .map(|t| Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(t, alias),
                    ..Default::default()
                })
                .collect();
            TableRef::union_all(queries, alias)
        }
    };

    // The join column that chains consecutive edges. For outgoing
    // (filter_col=source_id, select_col=target_id): chain on target→source.
    // For incoming (filter_col=target_id, select_col=source_id): chain on source→target.
    let (chain_next, chain_anchor) = if filter_col == SOURCE_ID_COLUMN {
        (TARGET_ID_COLUMN, SOURCE_ID_COLUMN)
    } else {
        (SOURCE_ID_COLUMN, TARGET_ID_COLUMN)
    };

    let mut arms: Vec<Query> = Vec::new();
    for depth in 1..=max_hops {
        let first_alias = if depth == 1 {
            CASCADE_EDGE_ALIAS.to_string()
        } else {
            "e1".to_string()
        };
        let last = if depth == 1 {
            CASCADE_EDGE_ALIAS.to_string()
        } else {
            format!("e{depth}")
        };

        // Build join chain: e1 JOIN e2 ON ... JOIN e3 ON ...
        let mut from = edge_scan(&first_alias);
        for i in 2..=depth {
            let prev = format!("e{}", i - 1);
            let curr = format!("e{i}");
            let join_cond = Expr::eq(Expr::col(&prev, chain_next), Expr::col(&curr, chain_anchor));
            from = TableRef::join(
                crate::ast::JoinType::Inner,
                from,
                edge_scan(&curr),
                join_cond,
            );
        }

        // WHERE: anchor filter on first edge + rel_type on all edges + kind on last
        let mut conds: Vec<Expr> = Vec::new();
        conds.push(Expr::InSubquery {
            expr: Box::new(Expr::col(&first_alias, filter_col)),
            cte_name: parent_cte.to_string(),
            column: DEFAULT_PRIMARY_KEY.to_string(),
        });
        for i in 1..=depth {
            let alias = if depth == 1 {
                CASCADE_EDGE_ALIAS.to_string()
            } else {
                format!("e{i}")
            };
            conds.push(rel_filter_expr(&alias));
        }
        if let Some(kf) = kind_filter(&last) {
            conds.push(kf);
        }

        arms.push(Query {
            select: vec![SelectExpr::new(
                Expr::col(&last, select_col),
                DEFAULT_PRIMARY_KEY,
            )],
            from,
            where_clause: Expr::conjoin(conds),
            ..Default::default()
        });
    }

    if arms.len() == 1 {
        Some(arms.into_iter().next().unwrap())
    } else {
        let mut first = arms.into_iter();
        let base = first.next().unwrap();
        Some(Query {
            union_all: first.collect(),
            ..base
        })
    }
}

/// Target-side SIP for aggregation queries.
///
/// When an aggregation target node has filters, materializes the matching
/// target IDs in a CTE and pushes them into the edge scan from the target
/// side. This narrows the edge scan by the selectivity of the target filters,
/// which is the common case for aggregations (e.g. "count merged MRs per project"
/// where the target MR has `state = 'merged'`).
///
/// Target conditions are intentionally kept in the main WHERE clause so that
/// `fold_filters_into_aggregates` can still convert aggregates to `-If`
/// combinators (e.g. `countIf`). The two optimizations serve different layers:
/// SIP narrows the edge scan (I/O), while `-If` gives ClickHouse bounded
/// aggregation memory per group regardless of data volume.
fn apply_target_sip_prefilter(q: &mut Query, input: &Input) {
    if input.relationships.is_empty() {
        return;
    }

    let target_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.target.as_deref())
        .collect();

    let mut injected: HashSet<String> = HashSet::new();

    for (i, rel) in input.relationships.iter().enumerate() {
        let target_node = match input
            .nodes
            .iter()
            .find(|n| n.id == rel.to && target_aliases.contains(n.id.as_str()))
        {
            Some(n) => n,
            None => continue,
        };

        let target_table = match &target_node.table {
            Some(t) => t.clone(),
            None => continue,
        };

        let target_alias = &target_node.id;

        if !injected.insert(target_alias.clone()) {
            continue;
        }

        // Clone target-only conjuncts into the CTE; leave originals in WHERE
        // so fold_filters can still convert them to -If combinators.
        // Skip `InSubquery` conjuncts: those are structural filters injected by
        // the cascade pass (e.g. `mr.id IN _cascade_mr`). Materializing them
        // again as `_target_<alias>_ids` produces a no-op CTE that re-derives
        // the same id set already in `_cascade_<alias>`.
        let target_only_conds: Vec<Expr> = q
            .where_clause
            .as_ref()
            .map(|w| {
                let conjuncts = w.clone().flatten_and();
                conjuncts
                    .into_iter()
                    .filter(|c| {
                        if matches!(c, Expr::InSubquery { .. }) {
                            return false;
                        }
                        let aliases = c.column_aliases();
                        !aliases.is_empty() && aliases.iter().all(|a| a == target_alias)
                    })
                    .collect()
            })
            .unwrap_or_default();

        if target_only_conds.is_empty() {
            continue;
        }

        let cte_name = format!("_target_{target_alias}_ids");
        let cte_where = Expr::and_all(target_only_conds.into_iter().map(Some));

        let cte_query = Query {
            select: vec![SelectExpr::new(
                Expr::col(target_alias, DEFAULT_PRIMARY_KEY),
                DEFAULT_PRIMARY_KEY,
            )],
            from: TableRef::scan(&target_table, target_alias),
            where_clause: cte_where,
            ..Default::default()
        };
        q.ctes.push(Cte::new(&cte_name, cte_query));

        let (_, end_col) = rel.direction.edge_columns();
        let edge_alias = if rel.max_hops > 1 {
            format!("hop_e{i}")
        } else {
            format!("e{i}")
        };
        let aliases = HashSet::from([edge_alias]);
        inject_sip_for_aliases(
            &mut q.from,
            &mut q.where_clause,
            end_col,
            &cte_name,
            &aliases,
            input,
            ArmAnchor::Last,
        );
    }
}

/// Which edge in a multi-hop UNION arm's left-deep join chain a SIP filter
/// should target. The chain's start-facing column lives on `e1` (leftmost);
/// the chain's end-facing column lives on `e<depth>` (rightmost).
#[derive(Copy, Clone)]
enum ArmAnchor {
    First,
    Last,
}

/// Walk the FROM tree and inject `{edge_alias}.{edge_col} IN (SELECT <id_col> FROM <cte>)`
/// into edge table scans whose alias is in `target_aliases`.
///
/// For Union arms (multi-hop), the `anchor` controls which edge in the arm's
/// join chain receives the filter:
/// - `ArmAnchor::First` for from-side SIP (`edge_col` is the chain's start column).
/// - `ArmAnchor::Last` for to-side SIP (`edge_col` is the chain's end column).
fn inject_sip_for_aliases(
    table_ref: &mut TableRef,
    outer_where: &mut Option<Expr>,
    edge_col: &str,
    cte_name: &str,
    target_aliases: &HashSet<String>,
    input: &Input,
    anchor: ArmAnchor,
) {
    match table_ref {
        TableRef::Scan { table, alias, .. }
            if is_edge_table(table, input) && target_aliases.contains(alias.as_str()) =>
        {
            let sip_filter = make_sip_filter(alias, edge_col, cte_name);
            *outer_where = Expr::and_all([outer_where.take(), Some(sip_filter)]);
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            inject_sip_for_aliases(
                left,
                outer_where,
                edge_col,
                cte_name,
                target_aliases,
                input,
                anchor,
            );
            inject_sip_for_aliases(
                right,
                outer_where,
                edge_col,
                cte_name,
                target_aliases,
                input,
                anchor,
            );
        }
        TableRef::Union { alias, queries, .. } if target_aliases.contains(alias.as_str()) => {
            for arm in queries {
                inject_sip_at_anchor(
                    &mut arm.from,
                    &mut arm.where_clause,
                    edge_col,
                    cte_name,
                    input,
                    anchor,
                );
            }
        }
        TableRef::Union { .. } => {}
        TableRef::Subquery { query, .. } => {
            inject_sip_for_aliases(
                &mut query.from,
                &mut query.where_clause,
                edge_col,
                cte_name,
                target_aliases,
                input,
                anchor,
            );
        }
    }
}

/// Inject SIP into the first (leftmost) or last (rightmost) edge scan in an
/// arm's left-deep join chain.
fn inject_sip_at_anchor(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    edge_col: &str,
    cte_name: &str,
    input: &Input,
    anchor: ArmAnchor,
) {
    match from {
        TableRef::Scan { table, alias, .. } if is_edge_table(table, input) => {
            let sip_filter = make_sip_filter(alias, edge_col, cte_name);
            *where_clause = Expr::and_all([where_clause.take(), Some(sip_filter)]);
        }
        TableRef::Join { left, right, .. } => match anchor {
            ArmAnchor::First => {
                inject_sip_at_anchor(left, where_clause, edge_col, cte_name, input, anchor);
            }
            ArmAnchor::Last => {
                inject_sip_at_anchor(right, where_clause, edge_col, cte_name, input, anchor);
            }
        },
        _ => {}
    }
}

fn is_edge_table(table: &str, input: &Input) -> bool {
    input.compiler.edge_tables.contains(table)
}

fn make_sip_filter(alias: &str, edge_col: &str, cte_name: &str) -> Expr {
    Expr::InSubquery {
        expr: Box::new(Expr::col(alias, edge_col)),
        cte_name: cte_name.to_string(),
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }
}

/// Rewrite `AGG(arg) ... WHERE <target_conds> AND <other_conds>`
/// into `AGGIf(arg, <target_conds>) ... WHERE <other_conds>`.
///
/// A WHERE conjunct is "foldable" into an aggregate if it references
/// only columns from the aggregate's target table (i.e. the table alias
/// of the aggregate's first argument). Structural predicates (JOINs,
/// group-by node filters) stay in WHERE.
fn fold_filters_into_aggregates(q: &mut Query, input: &Input) {
    let where_clause = match q.where_clause.take() {
        Some(w) => w,
        None => return,
    };

    let conjuncts = where_clause.flatten_and();

    // Build target alias set from Input aggregations (node ID = table alias after lowering).
    // Exclude edge-only targets — their filters are already in _nf_* CTEs.
    let target_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.target.as_deref())
        .filter(|t| !input.compiler.node_edge_col.contains_key(*t))
        .collect();

    // Count aggregations per target alias. When a single aggregation targets
    // an alias, folded conjuncts can be retained in WHERE so DeduplicatePass
    // can hoist sort-key (structural) ones into the LIMIT 1 BY subquery for
    // granule pruning. With multiple aggregations targeting the same alias
    // (e.g. countIf(state='opened') + countIf(state='closed')), per-If
    // filters disagree and a retained outer WHERE would corrupt the counts.
    let mut aggs_per_alias: HashMap<&str, usize> = HashMap::new();
    for agg in &input.aggregations {
        if let Some(t) = agg.target.as_deref() {
            *aggs_per_alias.entry(t).or_default() += 1;
        }
    }

    // Build group-by alias set to avoid folding their filters.
    let group_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.as_deref())
        .collect();

    // Build mapping from SQL function name -> -If name using AggFunction.
    let if_names: HashMap<&str, &str> = input
        .aggregations
        .iter()
        .filter_map(|agg| {
            agg.function
                .as_sql_if()
                .map(|if_n| (agg.function.as_sql(), if_n))
        })
        .collect();

    let mut folded_by_alias: HashMap<String, Vec<Expr>> = HashMap::new();
    let mut remaining: Vec<Expr> = Vec::new();

    for conjunct in conjuncts {
        let aliases = conjunct.column_aliases();

        // Keep in WHERE if:
        //   - references no columns (constant expression)
        //   - references multiple aliases (cross-table predicate)
        //   - references a group_by alias (group node filter must stay)
        //   - references an alias that isn't an aggregation target
        //   - is a SIP/cascade subquery filter (structural optimization, not a
        //     user property filter — folding it into countIf would reference a
        //     node alias that only exists as a CTE, not in the FROM clause)
        let should_keep = aliases.is_empty()
            || aliases.len() > 1
            || aliases.iter().any(|a| group_aliases.contains(a.as_str()))
            || aliases.iter().any(|a| !target_aliases.contains(a.as_str()))
            || matches!(&conjunct, Expr::InSubquery { .. });

        if should_keep {
            remaining.push(conjunct);
        } else if let Some(alias) = aliases.into_iter().next() {
            // Retain in WHERE when this alias has exactly one aggregation
            // target. DeduplicatePass.partition_filters will hoist sort-key
            // columns (id, project_id, traversal_path, branch) into the
            // dedup subquery's WHERE, enabling granule pruning. Mutable
            // columns stay in the outer WHERE, where they correctly
            // evaluate against the deduped row. The countIf(_, conjunct)
            // becomes redundant in this case but the cost is negligible.
            let single_target = aggs_per_alias.get(alias.as_str()).copied().unwrap_or(0) <= 1;
            if single_target {
                remaining.push(conjunct.clone());
            }
            folded_by_alias.entry(alias).or_default().push(conjunct);
        }
    }

    if folded_by_alias.is_empty() {
        q.where_clause = Expr::conjoin(remaining);
        return;
    }

    // Rewrite each aggregate in SELECT: AGG(arg) → AGGIf(arg, folded_conds).
    for sel in &mut q.select {
        if let Some((_alias, conds)) = extract_and_match(&sel.expr, &folded_by_alias) {
            sel.expr = rewrite_agg_to_if(&sel.expr, &if_names, conds);
        }
    }

    // Also rewrite ORDER BY expressions that reference the same aggregates.
    for ord in &mut q.order_by {
        if let Some((_alias, conds)) = extract_and_match(&ord.expr, &folded_by_alias) {
            ord.expr = rewrite_agg_to_if(&ord.expr, &if_names, conds);
        }
    }

    q.where_clause = Expr::conjoin(remaining);
}

/// Check if an expression is an aggregate targeting an alias with folded conditions.
fn extract_and_match<'a>(
    expr: &Expr,
    folded: &'a HashMap<String, Vec<Expr>>,
) -> Option<(String, &'a [Expr])> {
    let alias = extract_agg_target_alias(expr)?;
    let conds = folded.get(&alias)?;
    if conds.is_empty() {
        return None;
    }
    Some((alias, conds))
}

/// Rewrite `AGG(arg)` to `AGGIf(arg, cond1 AND cond2 AND ...)`.
fn rewrite_agg_to_if(expr: &Expr, if_names: &HashMap<&str, &str>, conditions: &[Expr]) -> Expr {
    match expr {
        Expr::FuncCall { name, args } => {
            let if_name = match if_names.get(name.as_str()) {
                Some(n) => *n,
                None => return expr.clone(),
            };
            let condition = match Expr::conjoin(conditions.to_vec()) {
                Some(c) => c,
                None => return expr.clone(),
            };

            let mut new_args = args.clone();
            new_args.push(condition);
            Expr::FuncCall {
                name: if_name.to_string(),
                args: new_args,
            }
        }
        _ => expr.clone(),
    }
}

/// Extract the table alias from the first argument of a FuncCall.
fn extract_agg_target_alias(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FuncCall { args, .. } => args.first().and_then(|arg| match arg {
            Expr::Column { table, .. } => Some(table.clone()),
            _ => None,
        }),
        _ => None,
    }
}

/// Cascade node filter CTEs through relationships for edge-centric traversals.
///
/// The lowerer creates `_nf_{node}` CTEs that filter each node independently.
/// For multi-rel queries, this misses the relationship chain: e.g. "merged MRs"
/// could be narrowed to "merged MRs authored by users 1,3,5" by intersecting
/// with the AUTHORED edge.
///
/// This pass finds `_nf_*` CTEs and adds edge-based intersection conditions
/// when a connected node has a narrower CTE (fewer IDs, typically node_ids).
fn cascade_node_filter_ctes(q: &mut Query, input: &Input) {
    // Track which nodes have been narrowed (have a usable CTE as cascade source).
    // Start with pinned nodes, then propagate through relationships.
    let mut narrowed: HashSet<String> = input
        .nodes
        .iter()
        .filter(|n| !n.node_ids.is_empty())
        .map(|n| n.id.clone())
        .collect();

    if narrowed.is_empty() {
        return;
    }

    // Iterate until no more cascades are possible. Each pass may narrow new
    // nodes, enabling further cascades in the next pass.
    let mut changed = true;
    while changed {
        changed = false;
        for rel in &input.relationships {
            let (start_col, end_col) = rel.direction.edge_columns();

            // narrowed → not-yet-narrowed
            let (source_id, target_id, edge_filter_col, edge_select_col) =
                if narrowed.contains(&rel.from) && !narrowed.contains(&rel.to) {
                    (&rel.from, &rel.to, start_col, end_col)
                } else if narrowed.contains(&rel.to) && !narrowed.contains(&rel.from) {
                    (&rel.to, &rel.from, end_col, start_col)
                } else {
                    continue;
                };

            // Source CTE: either _nf_{source} or _cascade_{source} from a previous pass
            let nf_name = node_filter_cte(source_id);
            let cascade_source = cascade_cte(source_id);
            let source_cte = if q.ctes.iter().any(|c| c.name == nf_name) {
                nf_name
            } else if q.ctes.iter().any(|c| c.name == cascade_source) {
                cascade_source
            } else {
                continue;
            };

            let cascade_name = cascade_cte(target_id);
            if q.ctes.iter().any(|c| c.name == cascade_name) {
                continue; // already cascaded
            }

            let cte_query = if rel.max_hops > 1 {
                match build_multihop_cascade_for_node(
                    input,
                    target_id,
                    edge_select_col,
                    edge_filter_col,
                    &source_cte,
                    &rel.types,
                    rel.max_hops,
                ) {
                    Some(q) => q,
                    None => continue,
                }
            } else {
                match build_cascade_for_node(
                    input,
                    target_id,
                    edge_select_col,
                    edge_filter_col,
                    &source_cte,
                    &rel.types,
                ) {
                    Some(q) => q,
                    None => continue,
                }
            };
            q.ctes.push(Cte::new(&cascade_name, cte_query));

            let target_nf = node_filter_cte(target_id);
            if let Some(cte) = q.ctes.iter_mut().find(|c| c.name == target_nf) {
                let filter = Expr::InSubquery {
                    expr: Box::new(Expr::col(target_id.as_str(), DEFAULT_PRIMARY_KEY)),
                    cte_name: cascade_name,
                    column: DEFAULT_PRIMARY_KEY.to_string(),
                };
                cte.query.where_clause =
                    Expr::and_all([cte.query.where_clause.take(), Some(filter)]);
            }

            narrowed.insert(target_id.clone());
            changed = true;
        }
    }
}

/// For traversal queries, derive `_nf_{neighbor}` CTEs for un-pinned nodes
/// that are reachable via a single hop from a pinned node.
///
/// Without this, a query like `File[node_ids: [X]] --DEFINES--> Definition`
/// builds `_nf_f` for the pinned File but leaves the joined-side Definition
/// table unrestricted. DeduplicatePass.wrap_join_scans then dedups the full
/// authorized Definition table before the JOIN, which on production data
/// scans tens of millions of rows for a single file's ~30 definitions.
///
/// We materialize the neighbor's reachable ids in `_nf_{neighbor}` once.
/// `wrap_join_scans` (deduplicate.rs) already injects the standard
/// `neighbor.id IN (SELECT id FROM _nf_{neighbor})` filter into the
/// neighbor's dedup subquery whenever such a CTE exists.
fn narrow_joined_nodes_via_pinned_neighbors(q: &mut Query, input: &Input) {
    if input.relationships.is_empty() {
        return;
    }

    // Seed the narrowed set with directly pinned nodes; extend it as we build
    // `_nf_*` cascade CTEs so that downstream relationships can chain off them.
    // Without the fixed-point loop, `Project[pinned] → File → Definition` only
    // narrows File and leaves Definition unrestricted, which forces dedup to
    // scan the whole authorized Definition table before the join.
    let mut narrowed: HashSet<String> = input
        .nodes
        .iter()
        .filter(|n| !n.node_ids.is_empty())
        .map(|n| n.id.clone())
        .collect();
    if narrowed.is_empty() {
        return;
    }

    let mut changed = true;
    while changed {
        changed = false;
        for rel in &input.relationships {
            let (start_col, end_col) = rel.direction.edge_columns();

            let (source_id, target_id, edge_filter_col, edge_select_col) =
                if narrowed.contains(&rel.from) && !narrowed.contains(&rel.to) {
                    (&rel.from, &rel.to, start_col, end_col)
                } else if narrowed.contains(&rel.to) && !narrowed.contains(&rel.from) {
                    (&rel.to, &rel.from, end_col, start_col)
                } else {
                    continue;
                };

            let source_nf = node_filter_cte(source_id);
            let target_nf = node_filter_cte(target_id);

            if !q.ctes.iter().any(|c| c.name == source_nf) {
                continue;
            }
            if q.ctes.iter().any(|c| c.name == target_nf) {
                continue;
            }

            if let Some(cte_query) = build_cascade_for_node(
                input,
                target_id,
                edge_select_col,
                edge_filter_col,
                &source_nf,
                &rel.types,
            ) {
                q.ctes.push(Cte::new(&target_nf, cte_query));
                narrowed.insert(target_id.clone());
                changed = true;
            }
        }
    }
}

/// Traversal hop frontier optimization.
///
/// For multi-hop traversal relationships (`max_hops > 1`), materializes the
/// reachable IDs at each hop depth in CTEs and injects SIP filters into the
/// deeper UNION ALL arms of the multi-hop `TableRef::Union`.
///
/// Without this, only `e1` (the first edge in each arm) gets a SIP filter from
/// the root node's IDs. Intermediate edges (`e2`, `e3`) rely solely on their
/// join conditions with the previous edge, which forces ClickHouse to do full
/// edge scans on those tables.
///
/// For a relationship at index `i` with `max_hops=3` and a pinned `from` node:
/// - `_thop{i}_1`: reachable IDs at depth 1 (via edge scan from root)
/// - `_thop{i}_2`: reachable IDs at depth 2 (via edge scan from `_thop{i}_1`)
/// - Arm depth=2: `e2.{start_col} IN (SELECT id FROM _thop{i}_1)`
/// - Arm depth=3: `e3.{start_col} IN (SELECT id FROM _thop{i}_2)`
fn apply_traversal_hop_frontiers(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }

    for (i, rel) in input.relationships.iter().enumerate() {
        if rel.max_hops <= 1 {
            continue;
        }

        // Skip when neither endpoint has a lowerer-created `_nf_*` CTE or
        // pinned `node_ids`. Cascade-derived CTEs (from
        // `narrow_joined_nodes_via_pinned_neighbors`) contain IDs reachable
        // from another node, not the user's filter result set — forward-
        // chaining from them produces descendants, not intermediate hops.
        let from_selective = input
            .nodes
            .iter()
            .find(|n| n.id == rel.from)
            .is_some_and(|n| !n.node_ids.is_empty())
            || input
                .compiler
                .lowerer_nf_ctes
                .contains(&node_filter_cte(&rel.from));
        let to_selective = input
            .nodes
            .iter()
            .find(|n| n.id == rel.to)
            .is_some_and(|n| !n.node_ids.is_empty())
            || input
                .compiler
                .lowerer_nf_ctes
                .contains(&node_filter_cte(&rel.to));
        if !from_selective && !to_selective {
            continue;
        }

        let (start_col, end_col) = rel.direction.edge_columns();

        // Find the source CTE that provides the selective root IDs.
        // The lowerer creates `_nf_{from}` when the from-node has conditions.
        // Fall back to `_nf_{to}` if the to-node is the selective side.
        let (source_cte, anchor_col, next_col) = {
            let nf_from = node_filter_cte(&rel.from);
            let nf_to = node_filter_cte(&rel.to);
            if q.ctes.iter().any(|c| c.name == nf_from) {
                (nf_from, start_col, end_col)
            } else if q.ctes.iter().any(|c| c.name == nf_to) {
                (nf_to, end_col, start_col)
            } else {
                // No selective source — frontier CTEs would scan the full
                // edge table, providing no benefit.
                continue;
            }
        };

        let edge_tables = input.compiler.resolve_edge_tables(&rel.types);
        let type_filter = if rel.types.is_empty() || (rel.types.len() == 1 && rel.types[0] == "*") {
            None
        } else {
            Some(&rel.types)
        };
        let prefix = format!("_thop{i}_");

        // Build frontier CTEs: _thop{i}_1 chains from source CTE,
        // _thop{i}_2 chains from _thop{i}_1, etc. We need depth-1 CTEs
        // (depth 1 arm doesn't need a frontier — it already has the root SIP).
        let mut new_ctes = Vec::new();
        for hop in 1..rel.max_hops {
            let hop_name = format!("{prefix}{hop}");
            let alias = HOP_EDGE_ALIAS;

            // Anchor filter: for hop 1, filter from source CTE;
            // for hop 2+, chain from previous frontier CTE.
            let parent_cte = if hop == 1 {
                source_cte.clone()
            } else {
                format!("{prefix}{}", hop - 1)
            };
            let anchor_filter = Expr::InSubquery {
                expr: Box::new(Expr::col(alias, anchor_col)),
                cte_name: parent_cte,
                column: DEFAULT_PRIMARY_KEY.to_string(),
            };

            // Relationship type filter.
            let rel_filter = type_filter.and_then(|types| {
                Expr::col_in(
                    alias,
                    RELATIONSHIP_KIND_COLUMN,
                    ChType::String,
                    types
                        .iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                )
            });

            let from = if edge_tables.len() == 1 {
                TableRef::scan(&edge_tables[0], alias)
            } else {
                let queries = edge_tables
                    .iter()
                    .map(|t| Query {
                        select: vec![SelectExpr::star()],
                        from: TableRef::scan(t, alias),
                        ..Default::default()
                    })
                    .collect();
                TableRef::union_all(queries, alias)
            };

            new_ctes.push(Cte::new(
                &hop_name,
                Query {
                    select: vec![SelectExpr::new(
                        Expr::col(alias, next_col),
                        DEFAULT_PRIMARY_KEY,
                    )],
                    from,
                    where_clause: Expr::and_all([Some(anchor_filter), rel_filter]),
                    ..Default::default()
                },
            ));
        }

        // Prepend hop CTEs before existing CTEs so they're available.
        new_ctes.append(&mut q.ctes);
        q.ctes = new_ctes;

        // Inject SIP filters into the UNION ALL arms inside the FROM tree.
        let hop_alias = format!("hop_e{i}");
        inject_traversal_hop_sip(&mut q.from, &hop_alias, &prefix, anchor_col, rel.min_hops);
    }
}

/// Walk the FROM tree to find the `TableRef::Union` with the given alias
/// and inject hop frontier SIP filters into its arms.
fn inject_traversal_hop_sip(
    table_ref: &mut TableRef,
    target_alias: &str,
    cte_prefix: &str,
    anchor_col: &str,
    min_hops: u32,
) {
    match table_ref {
        TableRef::Union { alias, queries, .. } if alias == target_alias => {
            // Arms are indexed from min_hops..=max_hops.
            // Arm 0 = min_hops depth, Arm 1 = min_hops+1, etc.
            // Only arms at depth >= 2 get a frontier filter (depth 1 already
            // has the root SIP from inject_sip_first_edge).
            for (arm_idx, arm) in queries.iter_mut().enumerate() {
                let depth = min_hops + arm_idx as u32;
                if depth < 2 {
                    continue;
                }
                // The frontier CTE at hop N-1 materializes IDs reachable at
                // depth N-1, so arm at depth N filters e{N}.anchor_col against it.
                let hop_cte_name = format!("{cte_prefix}{}", depth - 1);
                let last_edge = format!("e{depth}");
                let sip_filter = Expr::InSubquery {
                    expr: Box::new(Expr::col(&last_edge, anchor_col)),
                    cte_name: hop_cte_name,
                    column: DEFAULT_PRIMARY_KEY.to_string(),
                };
                arm.where_clause = Expr::and_all([arm.where_clause.take(), Some(sip_filter)]);
            }
        }
        TableRef::Join { left, right, .. } => {
            inject_traversal_hop_sip(left, target_alias, cte_prefix, anchor_col, min_hops);
            inject_traversal_hop_sip(right, target_alias, cte_prefix, anchor_col, min_hops);
        }
        _ => {}
    }
}

/// Path hop frontier optimization.
///
/// For path-finding queries with max_depth > 2, materializes the reachable
/// IDs at each hop depth in CTEs (`_fwd_hop1`, `_bwd_hop1`, etc.) and injects
/// SIP filters into the deeper UNION ALL arms of the forward/backward CTEs.
/// This narrows edge scans at each depth instead of doing full self-joins.
fn apply_path_hop_frontiers(q: &mut Query, input: &Input) {
    let path = match &input.path {
        Some(p) => p,
        None => return,
    };

    let start = input.nodes.iter().find(|n| n.id == path.from);
    let end = input.nodes.iter().find(|n| n.id == path.to);
    let (start_ids, end_ids) = match (start, end) {
        (Some(s), Some(e)) => (&s.node_ids, &e.node_ids),
        _ => return,
    };

    let max_depth = path.max_depth;
    let forward_depth = max_depth.div_ceil(2);
    let backward_depth = max_depth / 2;

    // Build hop frontier CTEs and inject SIP into frontier arms.
    let edge_tables = input.compiler.resolve_edge_tables(&path.rel_types);
    let mut new_ctes = Vec::new();
    inject_hop_frontiers(
        q,
        FORWARD_CTE,
        start_ids,
        forward_depth,
        true,
        &mut new_ctes,
        &edge_tables,
    );
    if backward_depth > 0 {
        inject_hop_frontiers(
            q,
            BACKWARD_CTE,
            end_ids,
            backward_depth,
            false,
            &mut new_ctes,
            &edge_tables,
        );
    }

    // Prepend hop CTEs before the forward/backward CTEs so they're available.
    new_ctes.append(&mut q.ctes);
    q.ctes = new_ctes;
}

/// Build hop frontier CTEs for one direction and inject SIP filters into
/// the corresponding frontier CTE's UNION ALL arms.
fn inject_hop_frontiers(
    q: &mut Query,
    cte_name: &str,
    anchor_ids: &[i64],
    max_depth: u32,
    is_forward: bool,
    new_ctes: &mut Vec<Cte>,
    edge_tables: &[String],
) {
    let prefix = if is_forward { "_fwd_hop" } else { "_bwd_hop" };
    let anchor_col = if is_forward {
        SOURCE_ID_COLUMN
    } else {
        TARGET_ID_COLUMN
    };
    let next_col = if is_forward {
        TARGET_ID_COLUMN
    } else {
        SOURCE_ID_COLUMN
    };

    // Build hop frontier CTEs: _fwd_hop1 chains from anchor IDs,
    // _fwd_hop2 chains from _fwd_hop1, etc.
    for hop in 1..max_depth {
        let hop_name = format!("{prefix}{hop}");
        let parent = if hop == 1 {
            None
        } else {
            Some(format!("{prefix}{}", hop - 1))
        };
        let alias = HOP_EDGE_ALIAS;

        let anchor_filter = if let Some(parent) = parent {
            Some(Expr::InSubquery {
                expr: Box::new(Expr::col(alias, anchor_col)),
                cte_name: parent,
                column: DEFAULT_PRIMARY_KEY.to_string(),
            })
        } else {
            Expr::col_in(
                alias,
                anchor_col,
                ChType::Int64,
                anchor_ids
                    .iter()
                    .map(|id| serde_json::Value::from(*id))
                    .collect(),
            )
        };

        let from = if edge_tables.len() == 1 {
            TableRef::scan(&edge_tables[0], alias)
        } else {
            let queries = edge_tables
                .iter()
                .map(|t| Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(t, alias),
                    ..Default::default()
                })
                .collect();
            TableRef::union_all(queries, alias)
        };
        new_ctes.push(Cte::new(
            &hop_name,
            Query {
                select: vec![SelectExpr::new(
                    Expr::col(alias, next_col),
                    DEFAULT_PRIMARY_KEY,
                )],
                from,
                where_clause: anchor_filter,
                ..Default::default()
            },
        ));
    }

    // Inject SIP filters into the UNION ALL arms of the frontier CTE.
    // Arms at depth >= 2 get: e{depth}.anchor_col IN (SELECT id FROM hop{depth-1})
    let frontier_cte = match q.ctes.iter_mut().find(|c| c.name == cte_name) {
        Some(c) => c,
        None => return,
    };

    // The frontier CTE is either a single query (depth=1) or has union_all arms.
    // Arm 0 is the base query (depth=1), arms 1+ are in union_all (depth=2+).
    // Only depth >= 2 gets a SIP filter, so we only touch union_all entries.
    for (i, arm) in frontier_cte.query.union_all.iter_mut().enumerate() {
        let depth = (i + 2) as u32; // union_all[0] = depth 2
        if depth > max_depth {
            continue;
        }
        let hop_cte_name = format!("{prefix}{}", depth - 1);
        let last_edge = format!("e{depth}");
        let sip_filter = Expr::InSubquery {
            expr: Box::new(Expr::col(&last_edge, anchor_col)),
            cte_name: hop_cte_name,
            column: DEFAULT_PRIMARY_KEY.to_string(),
        };
        arm.where_clause = Expr::and_all([arm.where_clause.take(), Some(sip_filter)]);
    }
}

/// For non-root nodes with pinned `node_ids`, inject literal IN filters
/// directly on the edge columns. This avoids a CTE round-trip for small
/// literal sets that ClickHouse can push into PREWHERE immediately.
fn apply_nonroot_node_ids_to_edges(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    let root_alias = input
        .relationships
        .first()
        .map(|r| &r.from)
        .or_else(|| input.nodes.first().map(|n| &n.id));

    for (i, rel) in input.relationships.iter().enumerate() {
        if rel.max_hops > 1 {
            continue;
        }
        let (start_col, end_col) = rel.direction.edge_columns();
        let edge_alias = format!("e{i}");

        if let Some(node) = input.nodes.iter().find(|n| n.id == rel.to)
            && !node.node_ids.is_empty()
            && root_alias != Some(&node.id)
            && let Some(filter) = Expr::col_in(
                &edge_alias,
                end_col,
                ChType::Int64,
                node.node_ids
                    .iter()
                    .map(|&id| serde_json::Value::Number(id.into()))
                    .collect(),
            )
        {
            q.where_clause = Expr::and_all([q.where_clause.take(), Some(filter)]);
        }

        if let Some(node) = input.nodes.iter().find(|n| n.id == rel.from)
            && !node.node_ids.is_empty()
            && root_alias != Some(&node.id)
            && let Some(filter) = Expr::col_in(
                &edge_alias,
                start_col,
                ChType::Int64,
                node.node_ids
                    .iter()
                    .map(|&id| serde_json::Value::Number(id.into()))
                    .collect(),
            )
        {
            q.where_clause = Expr::and_all([q.where_clause.take(), Some(filter)]);
        }
    }
}

/// Swap the innermost node-edge JOIN pair so edge becomes the driving table.
/// This enables LIMIT pushdown: each edge row is checked against the node
/// hash table and IN subquery PREWHERE filter in one pass.
fn apply_edge_led_reorder(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    let root_id = input
        .relationships
        .first()
        .map(|r| &r.from)
        .or_else(|| input.nodes.first().map(|n| &n.id));
    let has_selective = input.relationships.iter().any(|rel| {
        let to_sel = input
            .nodes
            .iter()
            .find(|n| n.id == rel.to)
            .is_some_and(|n| !n.node_ids.is_empty() && root_id != Some(&n.id));
        let from_sel = input
            .nodes
            .iter()
            .find(|n| n.id == rel.from)
            .is_some_and(|n| !n.node_ids.is_empty() && root_id != Some(&n.id));
        (to_sel || from_sel) && rel.max_hops == 1
    });
    if !has_selective {
        return;
    }
    let mut current = &mut q.from;
    loop {
        match current {
            TableRef::Join { left, right, .. } => {
                let r_edge = matches!(right.as_ref(), TableRef::Scan { table, .. } if is_edge_table(table, input));
                let l_node = matches!(left.as_ref(), TableRef::Scan { table, .. } if !is_edge_table(table, input));
                if r_edge && l_node {
                    std::mem::swap(left, right);
                    return;
                }
                current = left.as_mut();
            }
            _ => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Op, OrderExpr, SelectExpr, TableRef};
    use crate::input::{AggFunction, InputAggregation};

    fn count_expr(table: &str, col: &str) -> Expr {
        Expr::func("COUNT", vec![Expr::col(table, col)])
    }

    fn sum_expr(table: &str, col: &str) -> Expr {
        Expr::func("SUM", vec![Expr::col(table, col)])
    }

    fn eq_filter(table: &str, col: &str, val: &str) -> Expr {
        Expr::eq(
            Expr::col(table, col),
            Expr::Param {
                data_type: crate::ast::ChType::String,
                value: serde_json::Value::String(val.to_string()),
            },
        )
    }

    fn has_in_subquery(expr: &Expr, expected_cte: &str) -> bool {
        match expr {
            Expr::InSubquery { cte_name, .. } => cte_name == expected_cte,
            Expr::BinaryOp { left, right, .. } => {
                has_in_subquery(left, expected_cte) || has_in_subquery(right, expected_cte)
            }
            _ => false,
        }
    }

    fn agg_input(aggs: Vec<InputAggregation>) -> Input {
        Input {
            query_type: QueryType::Aggregation,
            aggregations: aggs,
            ..Default::default()
        }
    }

    fn count_agg(target: &str, group_by: Option<&str>) -> InputAggregation {
        InputAggregation {
            function: AggFunction::Count,
            target: Some(target.to_string()),
            group_by: group_by.map(str::to_string),
            property: None,
            alias: Some("count".to_string()),
        }
    }

    fn sum_agg(target: &str, property: &str, group_by: Option<&str>) -> InputAggregation {
        InputAggregation {
            function: AggFunction::Sum,
            target: Some(target.to_string()),
            group_by: group_by.map(str::to_string),
            property: Some(property.to_string()),
            alias: Some("total".to_string()),
        }
    }

    #[test]
    fn folds_target_filter_into_count_if() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                eq_filter("p", "name", "my-project"),
                eq_filter("mr", "state", "merged"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }

        // Group-by node filter stays in WHERE; target's filter is retained
        // alongside the countIf so DeduplicatePass can hoist sort-key
        // columns into the LIMIT 1 BY subquery for granule pruning.
        let where_aliases = q.where_clause.as_ref().unwrap().column_aliases();
        assert!(where_aliases.contains("p"));
        assert!(where_aliases.contains("mr"));
    }

    #[test]
    fn keeps_group_by_node_filters_in_where() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                eq_filter("p", "name", "my-project"),
                eq_filter("mr", "state", "merged"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf, got {other:?}"),
        }

        let where_aliases = q.where_clause.as_ref().unwrap().column_aliases();
        assert!(where_aliases.contains("p"));
    }

    #[test]
    fn no_group_by_still_folds() {
        let input = agg_input(vec![count_agg("mr", None)]);
        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("mr", "id"), "total")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[0].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected countIf, got {other:?}"),
        }
        // Single-aggregate target: filter retained in WHERE for granule pruning.
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn non_aggregate_query_skips_optimization() {
        let mut input = Input {
            query_type: QueryType::Traversal,
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "mr_id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        }));

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let original = q.where_clause.clone();
        optimize(&mut node, &mut input);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert_eq!(q.where_clause, original);
    }

    #[test]
    fn folds_multiple_conditions() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                eq_filter("mr", "state", "merged"),
                eq_filter("mr", "draft", "false"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
                match &args[1] {
                    Expr::BinaryOp { op: Op::And, .. } => {}
                    other => panic!("expected AND condition, got {other:?}"),
                }
            }
            other => panic!("expected countIf, got {other:?}"),
        }

        // Single-aggregate target: both conjuncts retained in WHERE alongside
        // the per-If filters, so DeduplicatePass can hoist them.
        let where_aliases = q.where_clause.as_ref().unwrap().column_aliases();
        assert!(where_aliases.contains("mr"));
    }

    #[test]
    fn multi_aggregate_does_not_retain_conjuncts() {
        // Two aggregations target the same alias with conflicting per-If
        // filters. Retaining either filter in outer WHERE would corrupt the
        // other count, so fold must REMOVE conjuncts from WHERE in this case.
        let input = agg_input(vec![
            InputAggregation {
                function: AggFunction::Count,
                target: Some("mr".to_string()),
                group_by: None,
                property: None,
                alias: Some("opened".to_string()),
            },
            InputAggregation {
                function: AggFunction::Count,
                target: Some("mr".to_string()),
                group_by: None,
                property: None,
                alias: Some("merged".to_string()),
            },
        ]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(count_expr("mr", "id"), "opened"),
                SelectExpr::new(count_expr("mr", "id"), "merged"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "opened")),
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        // Filter must NOT be retained — would corrupt the other countIf.
        assert!(q.where_clause.is_none());
    }

    #[test]
    fn rewrites_order_by_to_match() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            order_by: vec![OrderExpr {
                expr: count_expr("mr", "id"),
                desc: true,
            }],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.order_by[0].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn folds_sum_if() {
        let input = agg_input(vec![sum_agg("mr", "additions", Some("p"))]);
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(sum_expr("mr", "additions"), "total_additions"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "sumIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected sumIf, got {other:?}"),
        }
    }

    #[test]
    fn no_foldable_conditions_is_noop() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let cross_table = Expr::eq(Expr::col("mr", "author_id"), Expr::col("p", "creator_id"));
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(cross_table),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "COUNT"),
            other => panic!("expected COUNT, got {other:?}"),
        }
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn pinned_traversal_creates_neighbor_nf_cte() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // Source node is pinned via node_ids; target is unpinned.
        // The pass must create _nf_<target> by deriving target ids from the
        // edge filtered by _nf_<source>, so DeduplicatePass can narrow the
        // target's dedup subquery.
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "f".into(),
                    entity: Some("File".into()),
                    table: Some("gl_file".into()),
                    node_ids: vec![42i64],
                    ..Default::default()
                },
                InputNode {
                    id: "d".into(),
                    entity: Some("Definition".into()),
                    table: Some("gl_definition".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["DEFINES".into()],
                from: "f".into(),
                to: "d".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        // Simulate the lowerer having created _nf_f for the pinned File.
        let mut q = Query {
            select: vec![SelectExpr::new(Expr::col("d", "name"), "name")],
            from: TableRef::scan("gl_definition", "d"),
            ctes: vec![Cte::new(
                "_nf_f",
                Query {
                    select: vec![SelectExpr::new(Expr::col("f", "id"), "id")],
                    from: TableRef::scan("gl_file", "f"),
                    where_clause: Some(Expr::eq(Expr::col("f", "id"), Expr::lit(42))),
                    ..Default::default()
                },
            )],
            ..Default::default()
        };

        narrow_joined_nodes_via_pinned_neighbors(&mut q, &input);

        // _nf_d should now exist alongside _nf_f.
        assert!(
            q.ctes.iter().any(|c| c.name == "_nf_d"),
            "expected _nf_d CTE to be derived from edge filtered by _nf_f"
        );
    }

    #[test]
    fn pinned_traversal_cascades_across_multiple_relationships() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // Project[pinned] -- IN_PROJECT --> File -- DEFINES --> Definition
        // The pass must cascade: pinned p -> _nf_f via IN_PROJECT, then
        // _nf_f -> _nf_d via DEFINES. Without the fixed-point loop, only
        // _nf_f gets built and Definition's dedup scans the full table.
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![278964i64],
                    ..Default::default()
                },
                InputNode {
                    id: "f".into(),
                    entity: Some("File".into()),
                    table: Some("gl_file".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "d".into(),
                    entity: Some("Definition".into()),
                    table: Some("gl_definition".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![
                InputRelationship {
                    types: vec!["IN_PROJECT".into()],
                    from: "f".into(),
                    to: "p".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
                InputRelationship {
                    types: vec!["DEFINES".into()],
                    from: "f".into(),
                    to: "d".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
            ],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![SelectExpr::new(Expr::col("d", "name"), "name")],
            from: TableRef::scan("gl_definition", "d"),
            ctes: vec![Cte::new(
                "_nf_p",
                Query {
                    select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
                    from: TableRef::scan("gl_project", "p"),
                    where_clause: Some(Expr::eq(Expr::col("p", "id"), Expr::lit(278964))),
                    ..Default::default()
                },
            )],
            ..Default::default()
        };

        narrow_joined_nodes_via_pinned_neighbors(&mut q, &input);

        assert!(
            q.ctes.iter().any(|c| c.name == "_nf_f"),
            "expected _nf_f CTE derived from _nf_p via IN_PROJECT"
        );
        assert!(
            q.ctes.iter().any(|c| c.name == "_nf_d"),
            "expected _nf_d CTE cascaded from _nf_f via DEFINES"
        );
    }

    #[test]
    fn pinned_traversal_skips_when_both_sides_pinned() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // Both pinned: nothing to derive.
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "f".into(),
                    entity: Some("File".into()),
                    table: Some("gl_file".into()),
                    node_ids: vec![1i64],
                    ..Default::default()
                },
                InputNode {
                    id: "d".into(),
                    entity: Some("Definition".into()),
                    table: Some("gl_definition".into()),
                    node_ids: vec![2i64],
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["DEFINES".into()],
                from: "f".into(),
                to: "d".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![SelectExpr::new(Expr::col("d", "name"), "name")],
            from: TableRef::scan("gl_definition", "d"),
            ..Default::default()
        };

        narrow_joined_nodes_via_pinned_neighbors(&mut q, &input);

        assert!(
            q.ctes.is_empty(),
            "no CTEs should be created when both sides are pinned"
        );
    }

    #[test]
    fn multihop_cascade_builds_union_all_of_edge_chains() {
        use crate::input::InputNode;

        let input = Input {
            nodes: vec![InputNode {
                id: "f".into(),
                entity: Some("File".into()),
                table: Some("gl_file".into()),
                ..Default::default()
            }],
            ..Default::default()
        };

        let q = build_multihop_cascade_for_node(
            &input,
            "f",
            TARGET_ID_COLUMN,
            SOURCE_ID_COLUMN,
            "_root_ids",
            &["CONTAINS".to_string()],
            2,
        );
        let q = q.expect("should build cascade");

        // max_hops=2: base query is depth-1, union_all has depth-2.
        assert_eq!(
            q.union_all.len(),
            1,
            "expected 1 union_all arm (depth 2), got: {}",
            q.union_all.len()
        );

        // Depth-2 arm has a JOIN (e1 JOIN e2).
        let depth2 = &q.union_all[0];
        assert!(
            matches!(depth2.from, TableRef::Join { .. }),
            "depth-2 arm should be a JOIN, got: {:?}",
            std::mem::discriminant(&depth2.from)
        );

        // max_hops=3 produces 2 union_all arms.
        let q3 = build_multihop_cascade_for_node(
            &input,
            "f",
            TARGET_ID_COLUMN,
            SOURCE_ID_COLUMN,
            "_root_ids",
            &["CONTAINS".to_string()],
            3,
        )
        .unwrap();
        assert_eq!(q3.union_all.len(), 2, "max_hops=3 should have 2 union arms");
    }

    #[test]
    fn target_sip_injects_cte_for_aggregation_target_with_filters() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "p".into(),
                to: "mr".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_edge", "e0"),
                    TableRef::scan("gl_merge_request", "mr"),
                    Expr::eq(Expr::col("e0", "target_id"), Expr::col("mr", "id")),
                ),
                Expr::eq(Expr::col("p", "id"), Expr::col("e0", "source_id")),
            ),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        apply_target_sip_prefilter(&mut q, &input);

        // Should have created a _target_mr_ids CTE
        assert_eq!(q.ctes.len(), 1, "expected one CTE for target SIP");
        assert_eq!(q.ctes[0].name, "_target_mr_ids");

        // The WHERE should now include an IN subquery referencing the CTE.
        assert!(
            has_in_subquery(q.where_clause.as_ref().unwrap(), "_target_mr_ids"),
            "WHERE should contain InSubquery referencing _target_mr_ids"
        );
    }

    #[test]
    fn target_sip_deduplicates_same_alias_across_relationships() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![
                InputRelationship {
                    types: vec!["CONTAINS".into()],
                    from: "p".into(),
                    to: "mr".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
                InputRelationship {
                    types: vec!["MANAGES".into()],
                    from: "p".into(),
                    to: "mr".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
            ],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_edge", "e0"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        apply_target_sip_prefilter(&mut q, &input);

        assert_eq!(
            q.ctes.len(),
            1,
            "should create exactly one CTE despite two relationships targeting 'mr'"
        );
        assert_eq!(q.ctes[0].name, "_target_mr_ids");
    }

    #[test]
    fn target_sip_skips_when_no_target_filters() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    // No filters on the target
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "p".into(),
                to: "mr".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_edge", "e0"),
            ..Default::default()
        };

        apply_target_sip_prefilter(&mut q, &input);

        assert!(
            q.ctes.is_empty(),
            "no CTE should be created without target filters"
        );
    }

    fn has_kind_filter(expr: &Expr, edge_alias: &str, kind_col: &str, entity: &str) -> bool {
        match expr {
            Expr::BinaryOp {
                op: Op::Eq,
                left,
                right,
            } => {
                matches!(
                    (left.as_ref(), right.as_ref()),
                    (
                        Expr::Column { table, column },
                        Expr::Param { value: serde_json::Value::String(val), .. }
                    ) if table == edge_alias && column == kind_col && val == entity
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                has_kind_filter(left, edge_alias, kind_col, entity)
                    || has_kind_filter(right, edge_alias, kind_col, entity)
            }
            _ => false,
        }
    }

    #[test]
    fn group_by_kind_filter_injected() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "p".into(),
                to: "mr".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_edge", "e0"),
                    TableRef::scan("gl_merge_request", "mr"),
                    Expr::eq(Expr::col("e0", "target_id"), Expr::col("mr", "id")),
                ),
                Expr::eq(Expr::col("p", "id"), Expr::col("e0", "source_id")),
            ),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        inject_agg_group_by_kind_filters(&mut q, &input);

        let w = q.where_clause.as_ref().expect("WHERE should exist");
        assert!(
            has_kind_filter(w, "e0", "source_kind", "Project"),
            "WHERE should contain e0.source_kind = 'Project'"
        );
    }

    #[test]
    fn group_by_kind_filter_with_target_filters() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // mr has a filter, but the group-by node (p = Project) should
        // still get its kind filter.
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    filters: [(
                        "state".into(),
                        crate::input::InputFilter {
                            op: None,
                            value: Some(serde_json::json!("merged")),
                            ..Default::default()
                        },
                    )]
                    .into(),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "p".into(),
                to: "mr".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_edge", "e0"),
                    TableRef::scan("gl_merge_request", "mr"),
                    Expr::eq(Expr::col("e0", "target_id"), Expr::col("mr", "id")),
                ),
                Expr::eq(Expr::col("p", "id"), Expr::col("e0", "source_id")),
            ),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        inject_agg_group_by_kind_filters(&mut q, &input);

        let w = q.where_clause.as_ref().expect("WHERE should exist");
        assert!(
            has_kind_filter(w, "e0", "source_kind", "Project"),
            "group-by kind filter should be injected even when target has filters"
        );
    }

    #[test]
    fn group_by_kind_filter_incoming_direction() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // Incoming: rel.from = "mr", rel.to = "p".
        // edge_columns() for Incoming returns (target_id, source_id),
        // so mr (from) maps to target_id → target_kind,
        // and p (to) maps to source_id → source_kind.
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["IN_PROJECT".into()],
                from: "mr".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Incoming,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("mr", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_edge", "e0"),
                    TableRef::scan("gl_merge_request", "mr"),
                    Expr::eq(Expr::col("e0", "target_id"), Expr::col("mr", "id")),
                ),
                Expr::eq(Expr::col("p", "id"), Expr::col("e0", "source_id")),
            ),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        inject_agg_group_by_kind_filters(&mut q, &input);

        let w = q.where_clause.as_ref().expect("WHERE should exist");
        assert!(
            has_kind_filter(w, "e0", "source_kind", "Project"),
            "group-by node p should get source_kind for Incoming direction"
        );
    }

    #[test]
    fn traversal_hop_frontiers_creates_ctes_and_injects_sip() {
        use crate::input::{Direction, InputNode, InputRelationship};

        // Build a multi-hop UNION ALL with 3 arms (depth 1, 2, 3).
        let arm1 = Query {
            select: vec![SelectExpr::new(Expr::col("e1", "target_id"), "end_id")],
            from: TableRef::scan("gl_edge", "e1"),
            ..Default::default()
        };
        let arm2 = Query {
            select: vec![SelectExpr::new(Expr::col("e2", "target_id"), "end_id")],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_edge", "e1"),
                TableRef::scan("gl_edge", "e2"),
                Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
            ),
            ..Default::default()
        };
        let arm3 = Query {
            select: vec![SelectExpr::new(Expr::col("e3", "target_id"), "end_id")],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_edge", "e1"),
                    TableRef::scan("gl_edge", "e2"),
                    Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
                ),
                TableRef::scan("gl_edge", "e3"),
                Expr::eq(Expr::col("e2", "target_id"), Expr::col("e3", "source_id")),
            ),
            ..Default::default()
        };

        let nf_cte = Cte::new(
            "_nf_u",
            Query {
                select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
                from: TableRef::scan("gl_user", "u"),
                where_clause: Expr::col_in(
                    "u",
                    "id",
                    ChType::Int64,
                    vec![serde_json::Value::from(1)],
                ),
                ..Default::default()
            },
        );

        let mut q = Query {
            ctes: vec![nf_cte],
            select: vec![SelectExpr::star()],
            from: TableRef::union_all(vec![arm1, arm2, arm3], "hop_e0"),
            ..Default::default()
        };

        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["MEMBER_OF".into()],
                from: "u".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 3,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        apply_traversal_hop_frontiers(&mut q, &input);

        // Should have 3 CTEs: _nf_u (existing) + _thop0_1 + _thop0_2
        let cte_names: Vec<&str> = q.ctes.iter().map(|c| c.name.as_str()).collect();
        assert!(
            cte_names.contains(&"_thop0_1"),
            "expected _thop0_1 CTE, got: {cte_names:?}"
        );
        assert!(
            cte_names.contains(&"_thop0_2"),
            "expected _thop0_2 CTE, got: {cte_names:?}"
        );

        // _thop0_1 should reference _nf_u (the source CTE)
        let thop1 = q.ctes.iter().find(|c| c.name == "_thop0_1").unwrap();
        let thop1_where = thop1
            .query
            .where_clause
            .as_ref()
            .expect("_thop0_1 must have WHERE");
        assert!(
            has_in_subquery(thop1_where, "_nf_u"),
            "_thop0_1 must reference _nf_u, got: {thop1_where:?}"
        );

        // _thop0_2 should chain from _thop0_1
        let thop2 = q.ctes.iter().find(|c| c.name == "_thop0_2").unwrap();
        let thop2_where = thop2
            .query
            .where_clause
            .as_ref()
            .expect("_thop0_2 must have WHERE");
        assert!(
            has_in_subquery(thop2_where, "_thop0_1"),
            "_thop0_2 must reference _thop0_1, got: {thop2_where:?}"
        );

        // Arm at depth 1 should NOT have a frontier SIP
        let TableRef::Union { queries, .. } = &q.from else {
            panic!("expected Union FROM");
        };
        assert!(
            queries[0].where_clause.is_none(),
            "depth-1 arm should not have frontier SIP"
        );

        // Arm at depth 2 should have SIP referencing _thop0_1
        let arm2_where = queries[1]
            .where_clause
            .as_ref()
            .expect("depth-2 arm must have WHERE");
        assert!(
            has_in_subquery(arm2_where, "_thop0_1"),
            "depth-2 arm must reference _thop0_1, got: {arm2_where:?}"
        );

        // Arm at depth 3 should have SIP referencing _thop0_2
        let arm3_where = queries[2]
            .where_clause
            .as_ref()
            .expect("depth-3 arm must have WHERE");
        assert!(
            has_in_subquery(arm3_where, "_thop0_2"),
            "depth-3 arm must reference _thop0_2, got: {arm3_where:?}"
        );
    }

    #[test]
    fn traversal_hop_frontiers_skips_without_nf_cte() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let arm1 = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan("gl_edge", "e1"),
            ..Default::default()
        };
        let arm2 = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_edge", "e1"),
                TableRef::scan("gl_edge", "e2"),
                Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
            ),
            ..Default::default()
        };

        // No _nf_ CTEs — frontier optimization should be skipped.
        let mut q = Query {
            ctes: vec![],
            select: vec![SelectExpr::star()],
            from: TableRef::union_all(vec![arm1, arm2], "hop_e0"),
            ..Default::default()
        };

        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["MEMBER_OF".into()],
                from: "u".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 2,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        apply_traversal_hop_frontiers(&mut q, &input);

        // No frontier CTEs should be created.
        assert!(
            q.ctes.is_empty(),
            "no frontier CTEs without _nf_ source, got: {:?}",
            q.ctes.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn traversal_hop_frontiers_fires_for_lowerer_filter_cte() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let arm1 = Query {
            select: vec![SelectExpr::new(Expr::col("e1", "target_id"), "end_id")],
            from: TableRef::scan("gl_edge", "e1"),
            ..Default::default()
        };
        let arm2 = Query {
            select: vec![SelectExpr::new(Expr::col("e2", "target_id"), "end_id")],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_edge", "e1"),
                TableRef::scan("gl_edge", "e2"),
                Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
            ),
            ..Default::default()
        };

        let nf_cte = Cte::new(
            "_nf_mr",
            Query {
                select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                from: TableRef::scan("gl_merge_request", "mr"),
                where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
                ..Default::default()
            },
        );

        let mut q = Query {
            ctes: vec![nf_cte],
            select: vec![SelectExpr::star()],
            from: TableRef::union_all(vec![arm1, arm2], "hop_e0"),
            ..Default::default()
        };

        let mut input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["IN_PROJECT".into()],
                from: "mr".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 2,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };
        input.compiler.lowerer_nf_ctes.insert("_nf_mr".to_string());

        apply_traversal_hop_frontiers(&mut q, &input);

        let cte_names: Vec<&str> = q.ctes.iter().map(|c| c.name.as_str()).collect();
        assert!(
            cte_names.contains(&"_thop0_1"),
            "hop frontier should fire for lowerer filter CTE, got: {cte_names:?}"
        );
    }

    #[test]
    fn traversal_hop_frontiers_skips_cascade_derived_cte() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let arm1 = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan("gl_edge", "e1"),
            ..Default::default()
        };
        let arm2 = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_edge", "e1"),
                TableRef::scan("gl_edge", "e2"),
                Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
            ),
            ..Default::default()
        };

        let nf_cte = Cte::new(
            "_nf_mr",
            Query {
                select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                from: TableRef::scan("gl_merge_request", "mr"),
                ..Default::default()
            },
        );

        let mut q = Query {
            ctes: vec![nf_cte],
            select: vec![SelectExpr::star()],
            from: TableRef::union_all(vec![arm1, arm2], "hop_e0"),
            ..Default::default()
        };

        // lowerer_nf_ctes is empty — _nf_mr was cascade-derived
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    table: Some("gl_merge_request".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["IN_PROJECT".into()],
                from: "mr".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 2,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        apply_traversal_hop_frontiers(&mut q, &input);

        assert!(
            !q.ctes.iter().any(|c| c.name.starts_with("_thop")),
            "cascade-derived CTE should not trigger hop frontiers"
        );
    }

    #[test]
    fn variable_length_arms_get_static_kind_literals() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "g".into(),
                    entity: Some("Group".into()),
                    table: Some("gl_group".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "g".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 3,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        let arm = |depth: u32| {
            let mut from = TableRef::scan("gl_edge", "e1");
            for i in 2..=depth {
                let prev = format!("e{}", i - 1);
                let curr = format!("e{i}");
                from = TableRef::join(
                    crate::ast::JoinType::Inner,
                    from,
                    TableRef::scan("gl_edge", &curr),
                    Expr::eq(Expr::col(&prev, "target_id"), Expr::col(&curr, "source_id")),
                );
            }
            Query {
                select: vec![SelectExpr::star()],
                from,
                ..Default::default()
            }
        };
        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("hop_e0", "end_id"), "n")],
            from: TableRef::union_all(vec![arm(1), arm(2), arm(3)], "hop_e0"),
            ..Default::default()
        };

        push_kind_literals_into_variable_length_arms(&mut q, &input);

        let TableRef::Union { queries, .. } = &q.from else {
            panic!("expected hop_e0 union");
        };
        assert_eq!(queries.len(), 3);

        let w1 = queries[0].where_clause.as_ref().expect("arm1 WHERE");
        assert!(has_kind_filter(w1, "e1", "source_kind", "Group"));
        assert!(has_kind_filter(w1, "e1", "target_kind", "Project"));

        let w2 = queries[1].where_clause.as_ref().expect("arm2 WHERE");
        assert!(has_kind_filter(w2, "e1", "source_kind", "Group"));
        assert!(has_kind_filter(w2, "e2", "target_kind", "Project"));
        assert!(!has_kind_filter(w2, "e1", "target_kind", "Project"));

        let w3 = queries[2].where_clause.as_ref().expect("arm3 WHERE");
        assert!(has_kind_filter(w3, "e1", "source_kind", "Group"));
        assert!(has_kind_filter(w3, "e3", "target_kind", "Project"));
    }

    #[test]
    fn variable_length_arms_skip_when_no_entity_pinned() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "g".into(),
                    entity: None,
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: None,
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "g".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 2,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("hop_e0", "end_id"), "n")],
            from: TableRef::union_all(
                vec![
                    Query {
                        select: vec![SelectExpr::star()],
                        from: TableRef::scan("gl_edge", "e1"),
                        ..Default::default()
                    },
                    Query {
                        select: vec![SelectExpr::star()],
                        from: TableRef::join(
                            crate::ast::JoinType::Inner,
                            TableRef::scan("gl_edge", "e1"),
                            TableRef::scan("gl_edge", "e2"),
                            Expr::eq(Expr::col("e1", "target_id"), Expr::col("e2", "source_id")),
                        ),
                        ..Default::default()
                    },
                ],
                "hop_e0",
            ),
            ..Default::default()
        };

        push_kind_literals_into_variable_length_arms(&mut q, &input);

        let TableRef::Union { queries, .. } = &q.from else {
            panic!("expected union");
        };
        for arm in queries {
            assert!(arm.where_clause.is_none(), "no kinds → no injection");
        }
    }

    #[test]
    fn variable_length_arms_incoming_swaps_kind_columns() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "g".into(),
                    entity: Some("Group".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["CONTAINS".into()],
                from: "p".into(),
                to: "g".into(),
                min_hops: 1,
                max_hops: 2,
                direction: Direction::Incoming,
                filters: Default::default(),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("hop_e0", "end_id"), "n")],
            from: TableRef::union_all(
                vec![
                    Query {
                        select: vec![SelectExpr::star()],
                        from: TableRef::scan("gl_edge", "e1"),
                        ..Default::default()
                    },
                    Query {
                        select: vec![SelectExpr::star()],
                        from: TableRef::join(
                            crate::ast::JoinType::Inner,
                            TableRef::scan("gl_edge", "e1"),
                            TableRef::scan("gl_edge", "e2"),
                            Expr::eq(Expr::col("e1", "source_id"), Expr::col("e2", "target_id")),
                        ),
                        ..Default::default()
                    },
                ],
                "hop_e0",
            ),
            ..Default::default()
        };

        push_kind_literals_into_variable_length_arms(&mut q, &input);

        let TableRef::Union { queries, .. } = &q.from else {
            panic!("expected union");
        };
        let w1 = queries[0].where_clause.as_ref().expect("arm1 WHERE");
        assert!(has_kind_filter(w1, "e1", "target_kind", "Project"));
        assert!(has_kind_filter(w1, "e1", "source_kind", "Group"));

        let w2 = queries[1].where_clause.as_ref().expect("arm2 WHERE");
        assert!(has_kind_filter(w2, "e1", "target_kind", "Project"));
        assert!(has_kind_filter(w2, "e2", "source_kind", "Group"));
    }
}
