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
use crate::input::{Input, InputNode, QueryType};
use crate::security::SecurityContext;
use ontology::constants::{DEFAULT_PRIMARY_KEY, EDGE_TABLE, TRAVERSAL_PATH_COLUMN};

const ROOT_SIP_CTE: &str = "_root_ids";

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node, input: &Input, ctx: &SecurityContext) {
    match node {
        Node::Query(q) => {
            apply_keyset_pagination(q, input, ctx);
            apply_sip_prefilter(q, input, ctx);
            apply_nonroot_node_ids_to_edges(q, input);
            apply_edge_led_reorder(q, input);
            if input.query_type == QueryType::Traversal && input.relationships.len() > 1 {
                cascade_node_filter_ctes(q, input);
            }
            if input.query_type == QueryType::Aggregation {
                apply_target_sip_prefilter(q, input);
                fold_filters_into_aggregates(q, input);
            }
            if input.query_type == QueryType::PathFinding {
                apply_path_hop_frontiers(q, input);
            }
            // NOTE: CTE LIMIT propagation was evaluated and rejected.
            // Applying LIMIT to cascade or _nf_ CTEs is unsafe: the first N
            // IDs by physical order may not survive downstream JOINs (e.g.,
            // first 500 failed pipelines have zero failed jobs). Any LIMIT
            // before the final JOIN can produce incomplete results.
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
        // For aggregation, prefer the default node (first_rel.from) when it
        // has selectivity. Its filters (e.g. state='merged') are included in
        // the root CTE, producing a tighter cascade than using a pinned node
        // whose cascade through edges would lose the filter context.
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
    // Edge-centric traversals handle node filtering via IN subqueries in
    // lower.rs. SIP's root CTE can't extract edge-centric conditions
    // (they reference edge aliases, not node aliases).
    if input.query_type == QueryType::Traversal {
        return;
    }

    // Pick the most selective node as SIP root so the cascade starts narrow.
    let root_node = match choose_sip_root(input) {
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

        if from_cte.is_some()
            && to_cte.is_none()
            && let Some(cte) = build_cascade_for_node(
                input,
                &rel.to,
                end_col,
                start_col,
                from_cte.as_ref().unwrap(),
                &rel.types,
            )
        {
            let name = format!("_cascade_{}", rel.to);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.to.clone(), name);
        }
        if to_cte.is_some()
            && from_cte.is_none()
            && let Some(cte) = build_cascade_for_node(
                input,
                &rel.from,
                start_col,
                end_col,
                to_cte.as_ref().unwrap(),
                &rel.types,
            )
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

    // Filter by entity kind on the selected side (source_kind / target_kind)
    // so the cascade only picks up IDs of the correct type.
    let kind_col = if select_col == "source_id" {
        "source_kind"
    } else {
        "target_kind"
    };
    let kind_filter = node.entity.as_ref().map(|entity| {
        Expr::eq(
            Expr::col(alias, kind_col),
            Expr::param(ChType::String, entity.clone()),
        )
    });

    Some(Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, select_col),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(EDGE_TABLE, alias),
        where_clause: Expr::and_all([Some(parent_filter), Some(rel_filter), kind_filter]),
        ..Default::default()
    })
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
            let source_cte = if q.ctes.iter().any(|c| c.name == format!("_nf_{source_id}")) {
                format!("_nf_{source_id}")
            } else if q
                .ctes
                .iter()
                .any(|c| c.name == format!("_cascade_{source_id}"))
            {
                format!("_cascade_{source_id}")
            } else {
                continue;
            };

            let cascade_name = format!("_cascade_{target_id}");
            if q.ctes.iter().any(|c| c.name == cascade_name) {
                continue; // already cascaded
            }

            let target = input.nodes.iter().find(|n| n.id == *target_id);
            let alias = "_ce";

            let edge_filter = Expr::InSubquery {
                expr: Box::new(Expr::col(alias, edge_filter_col)),
                cte_name: source_cte.clone(),
                column: DEFAULT_PRIMARY_KEY.to_string(),
            };
            let rel_filter = if rel.types.len() == 1 {
                Expr::eq(
                    Expr::col(alias, "relationship_kind"),
                    Expr::param(ChType::String, rel.types[0].clone()),
                )
            } else {
                Expr::col_in(
                    alias,
                    "relationship_kind",
                    ChType::String,
                    rel.types
                        .iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                )
                .unwrap_or_else(|| Expr::param(ChType::Bool, true))
            };
            let kind_filter = target.and_then(|n| n.entity.as_ref()).map(|entity| {
                let kind_col = if edge_select_col == "source_id" {
                    "source_kind"
                } else {
                    "target_kind"
                };
                Expr::eq(
                    Expr::col(alias, kind_col),
                    Expr::param(ChType::String, entity.clone()),
                )
            });

            q.ctes.push(Cte::new(
                &cascade_name,
                Query {
                    select: vec![SelectExpr::new(
                        Expr::col(alias, edge_select_col),
                        DEFAULT_PRIMARY_KEY,
                    )],
                    from: TableRef::scan(EDGE_TABLE, alias),
                    where_clause: Expr::and_all([Some(edge_filter), Some(rel_filter), kind_filter]),
                    ..Default::default()
                },
            ));

            let target_nf = format!("_nf_{target_id}");
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
    let mut new_ctes = Vec::new();
    inject_hop_frontiers(q, "forward", start_ids, forward_depth, true, &mut new_ctes);
    if backward_depth > 0 {
        inject_hop_frontiers(q, "backward", end_ids, backward_depth, false, &mut new_ctes);
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
) {
    let prefix = if is_forward { "_fwd_hop" } else { "_bwd_hop" };
    let anchor_col = if is_forward { "source_id" } else { "target_id" };
    let next_col = if is_forward { "target_id" } else { "source_id" };

    // Build hop frontier CTEs: _fwd_hop1 chains from anchor IDs,
    // _fwd_hop2 chains from _fwd_hop1, etc.
    for hop in 1..max_depth {
        let hop_name = format!("{prefix}{hop}");
        let parent = if hop == 1 {
            None
        } else {
            Some(format!("{prefix}{}", hop - 1))
        };
        let alias = "_he";

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

        new_ctes.push(Cte::new(
            &hop_name,
            Query {
                select: vec![SelectExpr::new(
                    Expr::col(alias, next_col),
                    DEFAULT_PRIMARY_KEY,
                )],
                from: TableRef::scan(EDGE_TABLE, alias),
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
                let r_edge =
                    matches!(right.as_ref(), TableRef::Scan { table, .. } if is_edge_table(table));
                let l_node =
                    matches!(left.as_ref(), TableRef::Scan { table, .. } if !is_edge_table(table));
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
