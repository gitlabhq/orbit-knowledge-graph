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

use crate::ast::{ChType, Cte, Expr, Node, Op, Query, SelectExpr, TableRef};
use crate::constants::SKIP_SECURITY_FILTER_TABLES;
use crate::input::{Input, QueryType};
use crate::security::SecurityContext;
use ontology::constants::{DEFAULT_PRIMARY_KEY, EDGE_TABLE, TRAVERSAL_PATH_COLUMN};

const ROOT_SIP_CTE: &str = "_root_ids";

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node, input: &Input, ctx: &SecurityContext) {
    match node {
        Node::Query(q) => {
            apply_keyset_pagination(q, input, ctx);
            apply_sip_prefilter(q, input, ctx);
            apply_reverse_sip(q, input);
            if input.query_type == QueryType::Aggregation {
                apply_target_sip_prefilter(q, input);
                fold_filters_into_aggregates(q, input);
            }
        }
    }
}

/// Keyset pagination and OFFSET elimination.
///
/// When a cursor is present, decomposes it into a PK predicate:
///   (traversal_path > :tp) OR (traversal_path = :tp AND id > :cursor_id)
/// for each traversal path in the security context. This lets ClickHouse
/// seek directly via the primary key instead of scanning + skipping via OFFSET.
///
/// When node_ids are present (with or without a cursor), OFFSET is also
/// removed -- the result set is already bounded by explicit IDs, so
/// positional skipping is redundant.
fn apply_keyset_pagination(q: &mut Query, input: &Input, ctx: &SecurityContext) {
    let root_node = match input.nodes.first() {
        Some(n) => n,
        None => return,
    };

    let has_node_ids = !root_node.node_ids.is_empty();

    if let Some(cursor) = &input.cursor {
        let root_alias = &root_node.id;
        let keyset_predicate = if ctx.traversal_paths.len() == 1 {
            build_keyset_expr(root_alias, &ctx.traversal_paths[0], cursor.id)
        } else {
            Expr::or_all(
                ctx.traversal_paths
                    .iter()
                    .map(|tp| Some(build_keyset_expr(root_alias, tp, cursor.id))),
            )
            .unwrap_or_else(|| Expr::param(ChType::Bool, false))
        };

        q.where_clause = Expr::and_all([q.where_clause.take(), Some(keyset_predicate)]);
        q.offset = None;
    } else if has_node_ids {
        q.offset = None;
    }
}

/// SIP (Sideways Information Passing) pre-filter.
///
/// Materializes the root node's matching IDs in a CTE and pushes them into
/// the edge table scan via IN subquery. This triggers ClickHouse's `by_source`
/// projection on the edge table, reducing rows scanned by up to 63%.
///
/// Applied when either:
/// - The root node has explicit selectivity (filters, node_ids, cursor, id_range)
/// - The root node's table has a traversal_path security filter (the security
///   pass will inject startsWith into the CTE, giving the IN subquery enough
///   selectivity to trigger projection-based edge scans)
fn apply_sip_prefilter(q: &mut Query, input: &Input, ctx: &SecurityContext) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    if input.relationships.is_empty() {
        return;
    }

    // The SIP root must be the `from` node of the first relationship — that's
    // the node whose IDs map to the edge table's start column (source_id for
    // outgoing, target_id for incoming).
    let first_rel = match input.relationships.first() {
        Some(r) => r,
        None => return,
    };
    let root_node = match input.nodes.iter().find(|n| n.id == first_rel.from) {
        Some(n) => n,
        None => return,
    };

    let has_cursor = input.cursor.is_some();
    let has_filters = !root_node.filters.is_empty();
    let has_node_ids = !root_node.node_ids.is_empty();
    let has_id_range = root_node.id_range.is_some();
    let has_explicit_selectivity = has_cursor || has_filters || has_node_ids || has_id_range;

    // Apply SIP when root node has explicit filters OR when its table will
    // get a security filter (startsWith on traversal_path). Tables in
    // SKIP_SECURITY_FILTER_TABLES (e.g. gl_user) won't get security filters,
    // so an unfiltered SIP CTE would push all IDs — skip those.
    let root_table_has_security_filter = root_node
        .table
        .as_deref()
        .is_some_and(|t| !SKIP_SECURITY_FILTER_TABLES.contains(&t));

    if !has_explicit_selectivity && !root_table_has_security_filter {
        return;
    }

    let root_alias = &root_node.id;
    let root_table = match &root_node.table {
        Some(t) => t.clone(),
        None => return,
    };

    // Build optional keyset predicate for the CTE (narrows the materialized set)
    let keyset_predicate = input.cursor.as_ref().map(|cursor| {
        if ctx.traversal_paths.len() == 1 {
            build_keyset_expr(root_alias, &ctx.traversal_paths[0], cursor.id)
        } else {
            Expr::or_all(
                ctx.traversal_paths
                    .iter()
                    .map(|tp| Some(build_keyset_expr(root_alias, tp, cursor.id))),
            )
            .unwrap_or_else(|| Expr::param(ChType::Bool, false))
        }
    });

    // Build the CTE: SELECT id FROM root_table WHERE <root-only filters>
    // Extract only WHERE conjuncts that reference the root node alias.
    // The security pass will inject startsWith(traversal_path, ...) automatically.
    let root_only_conds = q
        .where_clause
        .as_ref()
        .map(|w| {
            let conjuncts = flatten_and(w.clone());
            conjuncts
                .into_iter()
                .filter(|c| {
                    let aliases = collect_column_aliases(c);
                    !aliases.is_empty() && aliases.iter().all(|a| a == root_alias)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let cte_where = Expr::and_all(
        root_only_conds
            .into_iter()
            .map(Some)
            .chain(std::iter::once(keyset_predicate)),
    );

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

    // Inject root SIP into edges adjacent to the root node.
    let mut node_ctes: HashMap<String, String> = HashMap::new();
    node_ctes.insert(root_alias.clone(), ROOT_SIP_CTE.to_string());

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
            inject_sip_for_aliases(&mut q.from, &mut q.where_clause, start_col, cte, &aliases);
        }
        if let Some(ref cte) = to_cte {
            inject_sip_for_aliases(&mut q.from, &mut q.where_clause, end_col, cte, &aliases);
        }

        // Cascading SIP: when the root is selective (node_ids, filters, etc.),
        // chain CTEs through relationships so every edge AND node table scan
        // gets narrowed. Skip cascades for broad roots (e.g. "all MRs") where
        // the cascade CTE itself would scan as many edge rows as the main query.
        if !has_explicit_selectivity || rel.max_hops > 1 {
            continue;
        }

        if let (Some(parent), None) = (&from_cte, &to_cte)
            && let Some(cte) =
                build_cascade_for_node(input, &rel.to, end_col, start_col, parent, &rel.types)
        {
            let name = format!("_cascade_{}", rel.to);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.to.clone(), name);
        }
        if let (None, Some(parent)) = (&from_cte, &to_cte)
            && let Some(cte) =
                build_cascade_for_node(input, &rel.from, start_col, end_col, parent, &rel.types)
        {
            let name = format!("_cascade_{}", rel.from);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.from.clone(), name);
        }
    }

    // Inject cascade CTE filters into node table scans. Each non-root node
    // with a cascade CTE gets `node.id IN (SELECT id FROM cascade_cte)`,
    // allowing ClickHouse to prewhere-filter large node tables (e.g. gl_job).
    for (alias, cte_name) in &node_ctes {
        if cte_name == ROOT_SIP_CTE {
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

    let alias = "_ce";
    let parent_filter = Expr::InSubquery {
        expr: Box::new(Expr::col(alias, filter_col)),
        cte_name: parent_cte.to_string(),
        column: DEFAULT_PRIMARY_KEY.to_string(),
    };
    let rel_filter = if rel_types.len() == 1 {
        Expr::eq(
            Expr::col(alias, "relationship_kind"),
            Expr::param(ChType::String, rel_types[0].clone()),
        )
    } else {
        Expr::col_in(
            alias,
            "relationship_kind",
            ChType::String,
            rel_types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, true))
    };

    Some(Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, select_col),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(EDGE_TABLE, alias),
        where_clause: Some(Expr::and(parent_filter, rel_filter)),
        ..Default::default()
    })
}

/// Reverse SIP: when a non-root node has explicit node_ids, trace edges
/// backwards to narrow the node connected to it, then cascade forward.
///
/// Example: traversal Project(id=X) → MR(state=merged) → Note
///   Root = MR (first_rel.from), but Project has node_ids.
///   Reverse CTE: SELECT source_id FROM gl_edge WHERE target_id = X AND rel = 'IN_PROJECT'
///   This narrows MR scans from all merged MRs to only those in project X,
///   and cascades forward to narrow Note scans as well.
fn apply_reverse_sip(q: &mut Query, input: &Input) {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return;
    }
    if input.relationships.is_empty() {
        return;
    }
    let root_alias = match input.relationships.first() {
        Some(r) => &r.from,
        None => return,
    };

    // narrowed_alias → reverse CTE name
    let mut narrowed: HashMap<String, String> = HashMap::new();

    for node in &input.nodes {
        if node.id == *root_alias || node.node_ids.is_empty() {
            continue;
        }

        for rel in &input.relationships {
            if rel.max_hops > 1 {
                continue;
            }
            let is_to = rel.to == node.id;
            let is_from = rel.from == node.id;
            if !is_to && !is_from {
                continue;
            }

            let (start_col, end_col) = rel.direction.edge_columns();
            let (select_col, filter_col) = if is_to {
                (start_col, end_col)
            } else {
                (end_col, start_col)
            };

            let cte_name = format!("_reverse_{}", node.id);
            if q.ctes.iter().any(|c| c.name == cte_name) {
                break;
            }

            let re = "_re";
            let id_cond = Expr::col_in(
                re,
                filter_col,
                ChType::Int64,
                node.node_ids
                    .iter()
                    .map(|&id| serde_json::Value::from(id))
                    .collect(),
            );
            let rel_cond = if rel.types.len() == 1 {
                Expr::eq(
                    Expr::col(re, "relationship_kind"),
                    Expr::param(ChType::String, rel.types[0].clone()),
                )
            } else {
                Expr::col_in(
                    re,
                    "relationship_kind",
                    ChType::String,
                    rel.types
                        .iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                )
                .unwrap_or_else(|| Expr::param(ChType::Bool, true))
            };

            q.ctes.push(Cte::new(
                &cte_name,
                Query {
                    select: vec![SelectExpr::new(
                        Expr::col(re, select_col),
                        DEFAULT_PRIMARY_KEY,
                    )],
                    from: TableRef::scan(EDGE_TABLE, re),
                    where_clause: Expr::and_all([id_cond, Some(rel_cond)]),
                    ..Default::default()
                },
            ));

            let other_alias = if is_to { &rel.from } else { &rel.to };
            narrowed.insert(other_alias.clone(), cte_name);
            break;
        }
    }

    if narrowed.is_empty() {
        return;
    }

    // Inject reverse CTEs into edge scans
    for (i, rel) in input.relationships.iter().enumerate() {
        if rel.max_hops > 1 {
            continue;
        }
        let (start_col, end_col) = rel.direction.edge_columns();
        let edge_alias = format!("e{i}");
        let aliases = HashSet::from([edge_alias]);

        if let Some(cte) = narrowed.get(&rel.from) {
            inject_sip_for_aliases(&mut q.from, &mut q.where_clause, start_col, cte, &aliases);
        }
        if let Some(cte) = narrowed.get(&rel.to) {
            inject_sip_for_aliases(&mut q.from, &mut q.where_clause, end_col, cte, &aliases);
        }
    }

    // Inject into narrowed node table scans
    for (alias, cte_name) in &narrowed {
        let filter = Expr::InSubquery {
            expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
            cte_name: cte_name.clone(),
            column: DEFAULT_PRIMARY_KEY.to_string(),
        };
        q.where_clause = Expr::and_all([q.where_clause.take(), Some(filter)]);
    }

    // Narrow the root CTE if the reverse SIP covers the root alias.
    // Move the reverse CTE before _root_ids so it can be referenced, then
    // add `root.id IN (SELECT id FROM _reverse_X)` to the root CTE.
    // This narrows all downstream cascades automatically.
    if let Some(cte_name) = narrowed.get(root_alias) {
        if let Some(idx) = q.ctes.iter().position(|c| c.name == *cte_name) {
            let cte = q.ctes.remove(idx);
            q.ctes.insert(0, cte);
        }
        if let Some(root_cte) = q.ctes.iter_mut().find(|c| c.name == ROOT_SIP_CTE) {
            let intersection = Expr::InSubquery {
                expr: Box::new(Expr::col(root_alias, DEFAULT_PRIMARY_KEY)),
                cte_name: cte_name.clone(),
                column: DEFAULT_PRIMARY_KEY.to_string(),
            };
            root_cte.query.where_clause =
                Expr::and_all([root_cte.query.where_clause.take(), Some(intersection)]);
        }
    }

    // Forward-cascade from narrowed nodes to other connected nodes
    for (narrowed_alias, reverse_cte) in &narrowed {
        for rel in &input.relationships {
            if rel.max_hops > 1 {
                continue;
            }
            let (start_col, end_col) = rel.direction.edge_columns();
            let (target_alias, sel_col, filt_col) = if rel.from == *narrowed_alias {
                (&rel.to, end_col, start_col)
            } else if rel.to == *narrowed_alias {
                (&rel.from, start_col, end_col)
            } else {
                continue;
            };

            if narrowed.contains_key(target_alias) {
                continue;
            }
            if input
                .nodes
                .iter()
                .any(|n| n.id == *target_alias && !n.node_ids.is_empty())
            {
                continue;
            }

            let cascade_name = format!("_rev_cascade_{}", target_alias);
            if q.ctes.iter().any(|c| c.name == cascade_name) {
                continue;
            }

            if let Some(cte) = build_cascade_for_node(
                input,
                target_alias,
                sel_col,
                filt_col,
                reverse_cte,
                &rel.types,
            ) {
                q.ctes.push(Cte::new(&cascade_name, cte));
                let filter = Expr::InSubquery {
                    expr: Box::new(Expr::col(target_alias, DEFAULT_PRIMARY_KEY)),
                    cte_name: cascade_name,
                    column: DEFAULT_PRIMARY_KEY.to_string(),
                };
                q.where_clause = Expr::and_all([q.where_clause.take(), Some(filter)]);
            }
        }
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
        let target_only_conds: Vec<Expr> = q
            .where_clause
            .as_ref()
            .map(|w| {
                let conjuncts = flatten_and(w.clone());
                conjuncts
                    .into_iter()
                    .filter(|c| {
                        let aliases = collect_column_aliases(c);
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
        );
    }
}

/// Build a decomposed keyset predicate for one traversal path:
///   (traversal_path > :tp) OR (traversal_path = :tp AND id > :cursor_id)
fn build_keyset_expr(alias: &str, tp: &str, cursor_id: i64) -> Expr {
    let tp_gt = Expr::binary(
        Op::Gt,
        Expr::col(alias, TRAVERSAL_PATH_COLUMN),
        Expr::param(ChType::String, tp.to_string()),
    );
    let tp_eq_and_id_gt = Expr::and(
        Expr::eq(
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            Expr::param(ChType::String, tp.to_string()),
        ),
        Expr::binary(
            Op::Gt,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::param(ChType::Int64, cursor_id),
        ),
    );
    Expr::or(tp_gt, tp_eq_and_id_gt)
}

/// Walk the FROM tree and inject `{edge_alias}.{edge_col} IN (SELECT <id_col> FROM <cte>)`
/// into edge table scans whose alias is in `target_aliases`.
///
/// This ensures SIP only pushes IDs into edges that connect to the correct node.
/// For Union arms (multi-hop), injects into the first (leftmost) edge scan
/// in each arm — intermediate edge scans connect to hop results, not to root IDs.
fn inject_sip_for_aliases(
    table_ref: &mut TableRef,
    outer_where: &mut Option<Expr>,
    edge_col: &str,
    cte_name: &str,
    target_aliases: &HashSet<String>,
) {
    match table_ref {
        TableRef::Scan { table, alias, .. }
            if is_edge_table(table) && target_aliases.contains(alias.as_str()) =>
        {
            let sip_filter = make_sip_filter(alias, edge_col, cte_name);
            *outer_where = Expr::and_all([outer_where.take(), Some(sip_filter)]);
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            inject_sip_for_aliases(left, outer_where, edge_col, cte_name, target_aliases);
            inject_sip_for_aliases(right, outer_where, edge_col, cte_name, target_aliases);
        }
        TableRef::Union { alias, queries, .. } if target_aliases.contains(alias.as_str()) => {
            for arm in queries {
                inject_sip_first_edge(&mut arm.from, &mut arm.where_clause, edge_col, cte_name);
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
            );
        }
    }
}

/// Inject SIP into only the first (leftmost) edge scan in a FROM tree.
/// Used for multi-hop UNION ALL arms where only `e1` connects to root node IDs.
fn inject_sip_first_edge(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    edge_col: &str,
    cte_name: &str,
) {
    match from {
        TableRef::Scan { table, alias, .. } if is_edge_table(table) => {
            let sip_filter = make_sip_filter(alias, edge_col, cte_name);
            *where_clause = Expr::and_all([where_clause.take(), Some(sip_filter)]);
        }
        TableRef::Join { left, .. } => {
            inject_sip_first_edge(left, where_clause, edge_col, cte_name);
        }
        _ => {}
    }
}

fn is_edge_table(table: &str) -> bool {
    table == "gl_edge" || table.starts_with("gl_edge")
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

    let conjuncts = flatten_and(where_clause);

    // Build target alias set from Input aggregations (node ID = table alias after lowering).
    let target_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.target.as_deref())
        .collect();

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
        let aliases = collect_column_aliases(&conjunct);

        // Keep in WHERE if:
        //   - references no columns (constant expression)
        //   - references multiple aliases (cross-table predicate)
        //   - references a group_by alias (group node filter must stay)
        //   - references an alias that isn't an aggregation target
        let should_keep = aliases.is_empty()
            || aliases.len() > 1
            || aliases.iter().any(|a| group_aliases.contains(a.as_str()))
            || aliases.iter().any(|a| !target_aliases.contains(a.as_str()));

        if should_keep {
            remaining.push(conjunct);
        } else if let Some(alias) = aliases.into_iter().next() {
            folded_by_alias.entry(alias).or_default().push(conjunct);
        }
    }

    if folded_by_alias.is_empty() {
        q.where_clause = rebuild_and(remaining);
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

    q.where_clause = rebuild_and(remaining);
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
            let condition = match conditions.iter().cloned().reduce(Expr::and) {
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

/// Flatten nested AND expressions into a flat list of conjuncts.
fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            let mut out = flatten_and(*left);
            out.extend(flatten_and(*right));
            out
        }
        other => vec![other],
    }
}

/// Rebuild an AND chain from conjuncts. Returns None if empty.
fn rebuild_and(mut conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let first = conjuncts.remove(0);
    Some(conjuncts.into_iter().fold(first, Expr::and))
}

/// Collect all unique table aliases referenced by column expressions.
fn collect_column_aliases(expr: &Expr) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_aliases_inner(expr, &mut aliases);
    aliases
}

fn collect_aliases_inner(expr: &Expr, aliases: &mut HashSet<String>) {
    match expr {
        Expr::Column { table, .. } => {
            aliases.insert(table.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aliases_inner(left, aliases);
            collect_aliases_inner(right, aliases);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_aliases_inner(inner, aliases);
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_aliases_inner(arg, aliases);
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_aliases_inner(expr, aliases);
        }
        Expr::Literal(_) | Expr::Param { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{OrderExpr, SelectExpr, TableRef};
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

        // Group-by node filter stays in WHERE.
        let where_aliases = collect_column_aliases(q.where_clause.as_ref().unwrap());
        assert!(where_aliases.contains("p"));
        assert!(!where_aliases.contains("mr"));
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

        let where_aliases = collect_column_aliases(q.where_clause.as_ref().unwrap());
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
        assert!(q.where_clause.is_none());
    }

    #[test]
    fn non_aggregate_query_skips_optimization() {
        let input = Input {
            query_type: QueryType::Traversal,
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "mr_id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        }));

        let original = match &node {
            Node::Query(q) => q.where_clause.clone(),
        };
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        optimize(&mut node, &input, &ctx);

        match &node {
            Node::Query(q) => assert_eq!(q.where_clause, original),
        }
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
}
