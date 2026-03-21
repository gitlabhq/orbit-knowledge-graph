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

use crate::ast::{ChType, Cte, Expr, JoinType, Node, Op, Query, SelectExpr, TableRef};
use crate::constants::SKIP_SECURITY_FILTER_TABLES;
use crate::input::{Input, QueryType};
use crate::security::SecurityContext;
use ontology::constants::{DEFAULT_PRIMARY_KEY, EDGE_TABLE, TRAVERSAL_PATH_COLUMN};

const ROOT_SIP_CTE: &str = "_root_ids";
const ROOT_NARROWED_CTE: &str = "_root_narrowed";
const LIMIT_PUSHDOWN_MULTIPLIER: u32 = 3;

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node, input: &Input, ctx: &SecurityContext) {
    match node {
        Node::Query(q) => {
            apply_keyset_pagination(q, input, ctx);
            apply_sip_prefilter(q, input, ctx);
            apply_reverse_sip(q, input);
            apply_filtered_node_sip(q, input);
            if input.query_type == QueryType::Aggregation {
                apply_edge_only_aggregation(q, input);
                apply_target_only_count_elimination(q, input);
                fold_filters_into_aggregates(q, input);
            }
            apply_join_to_in_setting(q, input);
        }
    }
}

/// Append `SETTINGS query_plan_convert_join_to_in = 1` for queries with
/// relationships. This tells ClickHouse to auto-convert JOINs to IN
/// subqueries when the right side is small, improving edge scan performance.
fn apply_join_to_in_setting(q: &mut Query, input: &Input) {
    if !input.relationships.is_empty() && !q.ctes.is_empty() {
        q.settings
            .push(("query_plan_convert_join_to_in".into(), "1".into()));
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

    // LIMIT pushdown: for traversal queries with ORDER BY on the root node,
    // push the ORDER BY and a padded LIMIT into the SIP CTE. This narrows the
    // materialized ID set from all matching roots to top-N candidates.
    // Only for Traversal — aggregation needs all rows for correct counts.
    let (cte_order_by, cte_limit) =
        if input.query_type == QueryType::Traversal && has_explicit_selectivity {
            if let (Some(ob), Some(limit)) = (&input.order_by, q.limit) {
                if ob.node == *root_alias {
                    let order = vec![crate::ast::OrderExpr {
                        expr: Expr::col(root_alias, &ob.property),
                        desc: ob.direction == crate::input::OrderDirection::Desc,
                    }];
                    (order, Some(limit * LIMIT_PUSHDOWN_MULTIPLIER))
                } else {
                    (vec![], None)
                }
            } else {
                (vec![], None)
            }
        } else {
            (vec![], None)
        };

    let cte_query = Query {
        select: vec![SelectExpr::new(
            Expr::col(root_alias, DEFAULT_PRIMARY_KEY),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(&root_table, root_alias),
        where_clause: cte_where,
        order_by: cte_order_by,
        limit: cte_limit,
        ..Default::default()
    };
    q.ctes.push(Cte::new(ROOT_SIP_CTE, cte_query));

    // Multi-relationship root narrowing: when multiple single-hop relationships
    // share the same root and the root isn't in GROUP BY, narrow the root SIP
    // using the first relationship's edges. This filters subsequent edge scans
    // to only root IDs that participate in the first relationship.
    let effective_root = if should_narrow_root(input, root_alias) {
        let (start_col, _) = first_rel.direction.edge_columns();
        let narrowed = build_narrowed_root_cte(ROOT_SIP_CTE, start_col, &first_rel.types);
        q.ctes.push(Cte::new(ROOT_NARROWED_CTE, narrowed));
        ROOT_NARROWED_CTE
    } else {
        ROOT_SIP_CTE
    };

    // Inject root SIP into edges adjacent to the root node.
    let mut node_ctes: HashMap<String, String> = HashMap::new();
    node_ctes.insert(root_alias.clone(), effective_root.to_string());

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

        // Cascading SIP: chain CTEs through relationships to narrow downstream
        // edge and node table scans. Cascade when:
        //   - Root has explicit selectivity (node_ids, filters, etc.)
        //   - OR the target node is a SKIP_SECURITY_FILTER table (e.g. gl_user,
        //     1.5M rows) that would otherwise be read fully
        // Skip cascades for multi-hop relationships.
        if rel.max_hops > 1 {
            continue;
        }

        let is_skip_security_table = |node_alias: &str| {
            input
                .nodes
                .iter()
                .find(|n| n.id == node_alias)
                .and_then(|n| n.table.as_deref())
                .is_some_and(|t| SKIP_SECURITY_FILTER_TABLES.contains(&t))
        };

        if let (Some(parent), None) = (&from_cte, &to_cte)
            && (has_explicit_selectivity || is_skip_security_table(&rel.to))
            && let Some(cte) =
                build_cascade_for_node(input, &rel.to, end_col, start_col, parent, &rel.types)
        {
            let name = format!("_cascade_{}", rel.to);
            q.ctes.push(Cte::new(&name, cte));
            node_ctes.insert(rel.to.clone(), name);
        }
        if let (None, Some(parent)) = (&from_cte, &to_cte)
            && (has_explicit_selectivity || is_skip_security_table(&rel.from))
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

/// Check if root narrowing should be applied: multi-relationship aggregation
/// from the same root where root is not in GROUP BY.
fn should_narrow_root(input: &Input, root_alias: &str) -> bool {
    if input.query_type != QueryType::Aggregation {
        return false;
    }
    if input.relationships.len() < 2 {
        return false;
    }
    if !input
        .relationships
        .iter()
        .all(|r| r.from == root_alias && r.max_hops <= 1)
    {
        return false;
    }
    !input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(root_alias))
}

/// Build a CTE that narrows the root SIP to only IDs that participate in
/// a specific relationship: `SELECT start_col AS id FROM gl_edge WHERE
/// start_col IN (parent) AND relationship_kind = ...`
fn build_narrowed_root_cte(parent_cte: &str, start_col: &str, rel_types: &[String]) -> Query {
    let alias = "_ne";
    let parent_filter = Expr::InSubquery {
        expr: Box::new(Expr::col(alias, start_col)),
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

    Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, start_col),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(EDGE_TABLE, alias),
        where_clause: Some(Expr::and(parent_filter, rel_filter)),
        ..Default::default()
    }
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

/// Filtered-node SIP for non-root nodes with WHERE filters.
///
/// Any non-root `to` node with filters gets materialized in a CTE and pushed
/// into the adjacent edge scan from the target side. This triggers the
/// `by_target` projection for dramatic granule reduction.
///
/// For aggregation queries, conditions are cloned (not moved) into the CTE
/// so `fold_filters_into_aggregates` can still convert them to `-If`.
fn apply_filtered_node_sip(q: &mut Query, input: &Input) {
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

    let mut injected: HashSet<String> = HashSet::new();

    for (i, rel) in input.relationships.iter().enumerate() {
        let target_node = match input.nodes.iter().find(|n| n.id == rel.to) {
            Some(n) => n,
            None => continue,
        };

        if target_node.id == *root_alias {
            continue;
        }

        let target_table = match &target_node.table {
            Some(t) => t.clone(),
            None => continue,
        };

        let target_alias = &target_node.id;

        if !injected.insert(target_alias.clone()) {
            continue;
        }

        // Skip when no user-specified filters AND a cascade CTE already covers
        // this node. When there's no cascade, security-injected conditions
        // (startsWith) are still worth materializing since they narrow edge
        // scans that would otherwise be full table scans.
        if target_node.filters.is_empty() && target_node.node_ids.is_empty() {
            let has_cascade = q
                .ctes
                .iter()
                .any(|c| c.name == format!("_cascade_{target_alias}"));
            if has_cascade {
                continue;
            }
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

        let (start_col, end_col) = rel.direction.edge_columns();
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

        // Reverse cascade: from the filtered-node CTE, trace edges backward
        // to narrow the `from` side (e.g. gl_user). This avoids scanning
        // large SKIP_SECURITY tables that have no other narrowing.
        if rel.max_hops == 1 {
            let from_node = match input.nodes.iter().find(|n| n.id == rel.from) {
                Some(n) => n,
                None => continue,
            };
            let is_from_skip = from_node
                .table
                .as_deref()
                .is_some_and(|t| SKIP_SECURITY_FILTER_TABLES.contains(&t));
            if is_from_skip && from_node.node_ids.is_empty() && from_node.filters.is_empty() {
                if let Some(cascade) = build_cascade_for_node(
                    input, &rel.from, start_col, end_col, &cte_name, &rel.types,
                ) {
                    let cascade_name = format!("_cascade_{}", rel.from);
                    q.ctes.push(Cte::new(&cascade_name, cascade));
                    let filter = Expr::InSubquery {
                        expr: Box::new(Expr::col(&rel.from, DEFAULT_PRIMARY_KEY)),
                        cte_name: cascade_name,
                        column: DEFAULT_PRIMARY_KEY.to_string(),
                    };
                    q.where_clause = Expr::and_all([q.where_clause.take(), Some(filter)]);
                }
            }
        }
    }
}

/// Edge-only aggregation: eliminate the root node table from the main query
/// when it's only used as a COUNT target.
///
/// Preconditions:
///   - Aggregation query with a single relationship (single-hop)
///   - All aggregations are COUNT on the root node (the `from` side)
///   - The root node is NOT in any aggregation's group_by
///   - A SIP CTE exists for the root (_root_ids)
///
/// Rewrites COUNT(root.id) → COUNT(edge.start_col) and removes the root
/// table from the JOIN tree, halving the scan for large root tables.
fn apply_edge_only_aggregation(q: &mut Query, input: &Input) {
    if input.relationships.len() != 1 {
        return;
    }
    let rel = &input.relationships[0];
    if rel.max_hops > 1 {
        return;
    }

    let root_alias = &rel.from;

    // Root must not be in any GROUP BY
    let group_by_nodes: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|a| a.group_by.as_deref())
        .collect();
    if group_by_nodes.contains(root_alias.as_str()) {
        return;
    }

    // All aggregations must be COUNT targeting the root node
    if !input.aggregations.iter().all(|a| {
        a.function == crate::input::AggFunction::Count
            && a.target.as_deref() == Some(root_alias.as_str())
            && a.property.is_none()
    }) {
        return;
    }

    // SIP CTE must exist
    if !q.ctes.iter().any(|c| c.name == ROOT_SIP_CTE) {
        return;
    }

    let (start_col, _) = rel.direction.edge_columns();
    let edge_alias = "e0";

    // Rewrite COUNT(root.id) → COUNT(edge.start_col) in SELECT and ORDER BY
    for sel in &mut q.select {
        rewrite_count_target(&mut sel.expr, root_alias, edge_alias, start_col);
    }
    for ord in &mut q.order_by {
        rewrite_count_target(&mut ord.expr, root_alias, edge_alias, start_col);
    }

    // Remove root table from FROM tree, extracting edge-only JOIN conditions
    let extracted_conds = eliminate_root_scan(&mut q.from, root_alias);

    // Remove root-only WHERE conjuncts (already enforced by SIP CTE)
    if let Some(where_clause) = q.where_clause.take() {
        let conjuncts = flatten_and(where_clause);
        let kept: Vec<Expr> = conjuncts
            .into_iter()
            .filter(|c| {
                let aliases = collect_column_aliases(c);
                // Keep if it references non-root aliases (or has no aliases)
                aliases.is_empty() || aliases.iter().any(|a| a != root_alias)
            })
            .collect();
        q.where_clause = Expr::and_all(kept.into_iter().map(Some));
    }

    // Add extracted non-root conditions (source_kind, relationship_kind) to WHERE
    for cond in extracted_conds {
        q.where_clause = Expr::and_all([q.where_clause.take(), Some(cond)]);
    }

    // Remove the cascade CTE for the GROUP BY node — it's now redundant since
    // the edge SIP (e0.source_id IN _root_ids) already narrows reachable targets.
    let cascade_name = format!("_cascade_{}", rel.to);
    q.ctes.retain(|c| c.name != cascade_name);

    // Remove cascade InSubquery from WHERE
    if let Some(where_clause) = q.where_clause.take() {
        let conjuncts = flatten_and(where_clause);
        let kept: Vec<Expr> = conjuncts
            .into_iter()
            .filter(
                |c| !matches!(c, Expr::InSubquery { cte_name, .. } if *cte_name == cascade_name),
            )
            .collect();
        q.where_clause = Expr::and_all(kept.into_iter().map(Some));
    }
}

/// Eliminate the target node table when it's only used as a COUNT target with
/// no user filters. Rewrites COUNT(target.id) → COUNT(edge.end_col) and removes
/// the target table JOIN. Works for single or multi-relationship queries where
/// one relationship's `to` node is the COUNT target.
fn apply_target_only_count_elimination(q: &mut Query, input: &Input) {
    if input.relationships.is_empty() {
        return;
    }

    // Find all COUNT targets
    let count_targets: HashSet<&str> = input
        .aggregations
        .iter()
        .filter(|a| a.function == crate::input::AggFunction::Count && a.property.is_none())
        .filter_map(|a| a.target.as_deref())
        .collect();
    if count_targets.len() != 1 {
        return;
    }
    let target_alias = *count_targets.iter().next().unwrap();

    // All aggregations must be COUNT targeting the same node
    if !input.aggregations.iter().all(|a| {
        a.function == crate::input::AggFunction::Count
            && a.target.as_deref() == Some(target_alias)
            && a.property.is_none()
    }) {
        return;
    }

    // Target must not be in GROUP BY
    let group_by_nodes: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|a| a.group_by.as_deref())
        .collect();
    if group_by_nodes.contains(target_alias) {
        return;
    }

    // Find the relationship whose `to` node is the COUNT target
    let (rel_idx, rel) = match input
        .relationships
        .iter()
        .enumerate()
        .find(|(_, r)| r.to == target_alias && r.max_hops <= 1)
    {
        Some(found) => found,
        None => return,
    };

    // Target node must have no user-specified filters
    let target_node = match input.nodes.iter().find(|n| n.id == target_alias) {
        Some(n) => n,
        None => return,
    };
    if !target_node.filters.is_empty() || !target_node.node_ids.is_empty() {
        return;
    }

    let (_, end_col) = rel.direction.edge_columns();
    let edge_alias = format!("e{rel_idx}");

    // Rewrite COUNT(target.id) → COUNT(edge.end_col) in SELECT and ORDER BY
    for sel in &mut q.select {
        rewrite_count_target(&mut sel.expr, target_alias, &edge_alias, end_col);
    }
    for ord in &mut q.order_by {
        rewrite_count_target(&mut ord.expr, target_alias, &edge_alias, end_col);
    }

    // Remove target table from FROM tree, preserve edge-side conditions (e.g. target_kind)
    let extracted_conds = eliminate_target_scan(&mut q.from, target_alias);

    // Remove target-only WHERE conjuncts
    if let Some(where_clause) = q.where_clause.take() {
        let conjuncts = flatten_and(where_clause);
        let kept: Vec<Expr> = conjuncts
            .into_iter()
            .filter(|c| {
                let aliases = collect_column_aliases(c);
                aliases.is_empty() || aliases.iter().any(|a| a != target_alias)
            })
            .collect();
        q.where_clause = Expr::and_all(kept.into_iter().map(Some));
    }

    // Add extracted edge-side conditions (e.g., target_kind) to WHERE
    for cond in extracted_conds {
        q.where_clause = Expr::and_all([q.where_clause.take(), Some(cond)]);
    }

    // Remove cascade CTE for the target node
    let cascade_name = format!("_cascade_{}", target_alias);
    q.ctes.retain(|c| c.name != cascade_name);

    // Remove cascade InSubquery from WHERE
    if let Some(where_clause) = q.where_clause.take() {
        let conjuncts = flatten_and(where_clause);
        let kept: Vec<Expr> = conjuncts
            .into_iter()
            .filter(
                |c| !matches!(c, Expr::InSubquery { cte_name, .. } if *cte_name == cascade_name),
            )
            .collect();
        q.where_clause = Expr::and_all(kept.into_iter().map(Some));
    }
}

/// Remove a table scan from the FROM tree by alias.
/// Searches the JOIN tree for `Join(..., Scan(target_alias), on)` and removes it,
/// returning edge-only conditions from the ON clause for WHERE injection.
fn eliminate_target_scan(from: &mut TableRef, target_alias: &str) -> Vec<Expr> {
    // Check if the right side of this join is the target
    let right_is_target = matches!(
        from,
        TableRef::Join { right, .. }
        if matches!(right.as_ref(), TableRef::Scan { alias, .. } if alias == target_alias)
    );

    if right_is_target {
        let old = std::mem::replace(
            from,
            TableRef::Scan {
                table: String::new(),
                alias: String::new(),
            },
        );
        let TableRef::Join {
            left: inner, on, ..
        } = old
        else {
            unreachable!()
        };
        let extracted: Vec<Expr> = flatten_and(on)
            .into_iter()
            .filter(|c| {
                let aliases = collect_column_aliases(c);
                !aliases.iter().any(|a| a == target_alias)
            })
            .collect();
        *from = *inner;
        return extracted;
    }

    // Recurse into left subtree
    if let TableRef::Join { left, .. } = from {
        eliminate_target_scan(left, target_alias)
    } else {
        Vec::new()
    }
}

/// Rewrite COUNT(old_alias.id) → COUNT(new_alias.new_col) in an expression tree.
fn rewrite_count_target(expr: &mut Expr, old_alias: &str, new_alias: &str, new_col: &str) {
    match expr {
        Expr::FuncCall { name, args } if name == "COUNT" => {
            for arg in args.iter_mut() {
                if let Expr::Column { table, column } = arg {
                    if table == old_alias && column == DEFAULT_PRIMARY_KEY {
                        *table = new_alias.to_string();
                        *column = new_col.to_string();
                    }
                }
            }
        }
        _ => {}
    }
}

/// Remove the root table scan from the FROM tree.
/// Returns non-root conditions from the removed JOIN's ON clause.
///
/// Expected pattern: Join(Join(Scan(root), Scan(edge), on_inner), Scan(target), on_outer)
/// Result:           Join(Scan(edge), Scan(target), on_outer)
fn eliminate_root_scan(from: &mut TableRef, root_alias: &str) -> Vec<Expr> {
    // Check the pattern without moving: Join(Join(Scan(root), edge, _), target, _)
    let matches = matches!(
        from,
        TableRef::Join { left, .. }
        if matches!(
            left.as_ref(),
            TableRef::Join { left: inner_left, .. }
            if matches!(inner_left.as_ref(), TableRef::Scan { alias, .. } if alias == root_alias)
        )
    );
    if !matches {
        return Vec::new();
    }

    // Take ownership and destructure
    let old = std::mem::replace(
        from,
        TableRef::Scan {
            table: String::new(),
            alias: String::new(),
        },
    );
    let TableRef::Join {
        left: inner_join,
        right: outer_right,
        on: outer_on,
        ..
    } = old
    else {
        unreachable!()
    };
    let TableRef::Join {
        right: edge,
        on: inner_on,
        ..
    } = *inner_join
    else {
        unreachable!()
    };

    let mut extracted = Vec::new();
    for c in flatten_and(inner_on) {
        let aliases = collect_column_aliases(&c);
        if !aliases.iter().any(|a| a == root_alias) {
            extracted.push(c);
        }
    }
    *from = TableRef::Join {
        join_type: JoinType::Inner,
        left: edge,
        right: outer_right,
        on: outer_on,
    };
    extracted
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

        let is_cascade_sip = matches!(
            &conjunct,
            Expr::InSubquery { cte_name, .. } if cte_name.starts_with("_cascade_")
        );

        // Keep in WHERE if:
        //   - references no columns (constant expression)
        //   - references multiple aliases (cross-table predicate)
        //   - references a group_by alias (group node filter must stay)
        //   - references an alias that isn't an aggregation target
        //   - is a cascade CTE filter (must stay for table scan narrowing)
        let should_keep = aliases.is_empty()
            || aliases.len() > 1
            || aliases.iter().any(|a| group_aliases.contains(a.as_str()))
            || aliases.iter().any(|a| !target_aliases.contains(a.as_str()))
            || is_cascade_sip;

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

        apply_filtered_node_sip(&mut q, &input);

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

        apply_filtered_node_sip(&mut q, &input);

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

        apply_filtered_node_sip(&mut q, &input);

        assert!(
            q.ctes.is_empty(),
            "no CTE should be created without target filters or WHERE conjuncts"
        );
    }

    #[test]
    fn target_sip_skips_when_cascade_already_covers_node() {
        use crate::input::{Direction, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "pipe".into(),
                    entity: Some("Pipeline".into()),
                    table: Some("gl_pipeline".into()),
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
                from: "pipe".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("pipe", Some("p"))],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("pipe", "id"), "pipe_count"),
            ],
            from: TableRef::scan("gl_edge", "e0"),
            where_clause: Some(eq_filter("p", "traversal_path", "1/")),
            ctes: vec![Cte::new("_cascade_p", Query::default())],
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        apply_filtered_node_sip(&mut q, &input);

        assert_eq!(
            q.ctes.len(),
            1,
            "should not add target SIP when cascade already covers the node"
        );
        assert_eq!(q.ctes[0].name, "_cascade_p");
    }

    #[test]
    fn cascade_cte_filter_stays_in_where_not_folded_into_aggregate() {
        use crate::input::{Direction, InputNode, InputRelationship};

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
                    id: "reviewer".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["REVIEWER".into()],
                from: "mr".into(),
                to: "reviewer".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![count_agg("reviewer", Some("mr"))],
            ..Default::default()
        };

        // Simulate a query with a cascade CTE filter on gl_user
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("mr", "title"), "mr_title"),
                SelectExpr::new(count_expr("reviewer", "id"), "reviewer_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::InSubquery {
                expr: Box::new(Expr::col("reviewer", "id")),
                cte_name: "_cascade_reviewer".into(),
                column: "id".into(),
            }),
            group_by: vec![Expr::col("mr", "title")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        // Cascade filter must stay in WHERE (not folded into countIf)
        assert!(
            has_in_subquery(q.where_clause.as_ref().unwrap(), "_cascade_reviewer"),
            "cascade CTE filter should remain in WHERE"
        );

        // Aggregate should still be COUNT, not countIf
        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "COUNT"),
            other => panic!("expected COUNT, got {other:?}"),
        }
    }

    #[test]
    fn settings_added_for_queries_with_relationships() {
        use crate::input::{Direction, InputNode, InputRelationship};

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
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            limit: 10,
            ..Default::default()
        };

        let mut q = Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "mr_id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ctes: vec![Cte::new("_root_ids", Query::default())],
            ..Default::default()
        };

        apply_join_to_in_setting(&mut q, &input);

        assert_eq!(q.settings.len(), 1);
        assert_eq!(q.settings[0].0, "query_plan_convert_join_to_in");
        assert_eq!(q.settings[0].1, "1");
    }

    #[test]
    fn settings_not_added_without_relationships() {
        let input = Input {
            query_type: QueryType::Search,
            ..Default::default()
        };

        let mut q = Query::default();
        apply_join_to_in_setting(&mut q, &input);

        assert!(q.settings.is_empty());
    }

    #[test]
    fn edge_only_aggregation_eliminates_root_table() {
        use crate::input::{
            AggFunction, Direction, InputAggregation, InputNode, InputRelationship,
        };

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "pipe".into(),
                    entity: Some("Pipeline".into()),
                    table: Some("gl_pipeline".into()),
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
                from: "pipe".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("pipe".into()),
                group_by: Some("p".into()),
                property: None,
                alias: Some("total".into()),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(Expr::func("COUNT", vec![Expr::col("pipe", "id")]), "total"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_pipeline", "pipe"),
                    TableRef::scan("gl_edge", "e0"),
                    Expr::and(
                        Expr::eq(Expr::col("pipe", "id"), Expr::col("e0", "source_id")),
                        Expr::eq(Expr::col("e0", "source_kind"), Expr::string("Pipeline")),
                    ),
                ),
                TableRef::scan("gl_project", "p"),
                Expr::eq(Expr::col("e0", "target_id"), Expr::col("p", "id")),
            ),
            where_clause: Some(eq_filter("pipe", "status", "failed")),
            group_by: vec![Expr::col("p", "name")],
            ctes: vec![Cte::new("_root_ids", Query::default())],
            ..Default::default()
        };

        apply_edge_only_aggregation(&mut q, &input);

        // COUNT target should be rewritten to edge column
        let count_sel = &q.select[1];
        if let Expr::FuncCall { args, .. } = &count_sel.expr {
            assert_eq!(
                args[0],
                Expr::col("e0", "source_id"),
                "COUNT should target edge column"
            );
        } else {
            panic!("expected FuncCall");
        }

        // FROM should not contain gl_pipeline scan
        fn has_scan(t: &TableRef, alias: &str) -> bool {
            match t {
                TableRef::Scan { alias: a, .. } => a == alias,
                TableRef::Join { left, right, .. } => {
                    has_scan(left, alias) || has_scan(right, alias)
                }
                _ => false,
            }
        }
        assert!(
            !has_scan(&q.from, "pipe"),
            "root table should be eliminated from FROM"
        );
        assert!(has_scan(&q.from, "e0"), "edge table should remain in FROM");
        assert!(has_scan(&q.from, "p"), "target table should remain in FROM");
    }

    #[test]
    fn edge_only_aggregation_skips_non_count() {
        use crate::input::{
            AggFunction, Direction, InputAggregation, InputNode, InputRelationship,
        };

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "pipe".into(),
                    entity: Some("Pipeline".into()),
                    table: Some("gl_pipeline".into()),
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
                from: "pipe".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![InputAggregation {
                function: AggFunction::Sum,
                target: Some("pipe".into()),
                group_by: Some("p".into()),
                property: Some("duration".into()),
                alias: Some("total_dur".into()),
            }],
            ..Default::default()
        };

        let mut q = Query {
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_pipeline", "pipe"),
                    TableRef::scan("gl_edge", "e0"),
                    Expr::lit(true),
                ),
                TableRef::scan("gl_project", "p"),
                Expr::lit(true),
            ),
            ctes: vec![Cte::new("_root_ids", Query::default())],
            ..Default::default()
        };

        apply_edge_only_aggregation(&mut q, &input);

        fn has_scan(t: &TableRef, alias: &str) -> bool {
            match t {
                TableRef::Scan { alias: a, .. } => a == alias,
                TableRef::Join { left, right, .. } => {
                    has_scan(left, alias) || has_scan(right, alias)
                }
                _ => false,
            }
        }
        assert!(
            has_scan(&q.from, "pipe"),
            "root table should NOT be eliminated for non-COUNT aggregation"
        );
    }

    #[test]
    fn edge_only_aggregation_rewrites_order_by() {
        use crate::input::{Direction, InputAggregation, InputNode, InputRelationship};

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
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("mr".into()),
                group_by: Some("p".into()),
                property: None,
                alias: Some("mr_count".into()),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_merge_request", "mr"),
                    TableRef::scan("gl_edge", "e0"),
                    Expr::eq(Expr::col("mr", "id"), Expr::col("e0", "source_id")),
                ),
                TableRef::scan("gl_project", "p"),
                Expr::eq(Expr::col("e0", "target_id"), Expr::col("p", "id")),
            ),
            order_by: vec![OrderExpr {
                expr: count_expr("mr", "id"),
                desc: true,
            }],
            group_by: vec![Expr::col("p", "name")],
            ctes: vec![Cte::new("_root_ids", Query::default())],
            ..Default::default()
        };

        apply_edge_only_aggregation(&mut q, &input);

        match &q.order_by[0].expr {
            Expr::FuncCall { args, .. } => {
                assert_eq!(
                    args[0],
                    Expr::col("e0", "source_id"),
                    "ORDER BY COUNT should target edge column after root elimination"
                );
            }
            other => panic!("expected FuncCall in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn multi_rel_root_narrowing_creates_cte() {
        use crate::input::{Direction, InputAggregation, InputNode, InputRelationship};

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
                    id: "reviewer".into(),
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
            relationships: vec![
                InputRelationship {
                    types: vec!["REVIEWER".into()],
                    from: "mr".into(),
                    to: "reviewer".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
                InputRelationship {
                    types: vec!["IN_PROJECT".into()],
                    from: "mr".into(),
                    to: "p".into(),
                    min_hops: 1,
                    max_hops: 1,
                    direction: Direction::Outgoing,
                    filters: Default::default(),
                },
            ],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("reviewer".into()),
                group_by: Some("p".into()),
                property: None,
                alias: Some("reviewer_count".into()),
            }],
            ..Default::default()
        };

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let mut node = crate::lower::lower(&input).unwrap();
        optimize(&mut node, &input, &ctx);

        let q = match &node {
            Node::Query(q) => q,
        };

        assert!(
            q.ctes.iter().any(|c| c.name == "_root_narrowed"),
            "should create _root_narrowed CTE for multi-rel aggregation"
        );
        assert!(
            q.ctes.iter().any(|c| c.name == "_root_ids"),
            "_root_ids should still exist as parent"
        );

        // WHERE should reference _root_narrowed for edge SIPs
        let where_str = format!("{:?}", q.where_clause);
        assert!(
            where_str.contains("_root_narrowed"),
            "edge SIPs should use _root_narrowed"
        );
    }

    #[test]
    fn multi_rel_root_narrowing_skips_when_root_in_group_by() {
        assert!(
            !should_narrow_root(
                &Input {
                    query_type: QueryType::Aggregation,
                    relationships: vec![
                        crate::input::InputRelationship {
                            types: vec!["R1".into()],
                            from: "mr".into(),
                            to: "a".into(),
                            min_hops: 1,
                            max_hops: 1,
                            direction: crate::input::Direction::Outgoing,
                            filters: Default::default(),
                        },
                        crate::input::InputRelationship {
                            types: vec!["R2".into()],
                            from: "mr".into(),
                            to: "b".into(),
                            min_hops: 1,
                            max_hops: 1,
                            direction: crate::input::Direction::Outgoing,
                            filters: Default::default(),
                        },
                    ],
                    aggregations: vec![InputAggregation {
                        function: AggFunction::Count,
                        target: Some("a".into()),
                        group_by: Some("mr".into()),
                        property: None,
                        alias: Some("cnt".into()),
                    }],
                    ..Default::default()
                },
                "mr"
            ),
            "should not narrow when root is in group_by"
        );
    }

    #[test]
    fn target_only_count_elimination_removes_target_join() {
        use crate::input::{Direction, InputAggregation, InputNode, InputRelationship};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "author".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    ..Default::default()
                },
                InputNode {
                    id: "note".into(),
                    entity: Some("Note".into()),
                    table: Some("gl_note".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "author".into(),
                to: "note".into(),
                min_hops: 1,
                max_hops: 1,
                direction: Direction::Outgoing,
                filters: Default::default(),
            }],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("note".into()),
                group_by: Some("author".into()),
                property: None,
                alias: Some("note_count".into()),
            }],
            ..Default::default()
        };

        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("author", "id"), "author_id"),
                SelectExpr::new(count_expr("note", "id"), "note_count"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    TableRef::scan("gl_user", "author"),
                    TableRef::scan("gl_edge", "e0"),
                    Expr::and(
                        Expr::eq(Expr::col("author", "id"), Expr::col("e0", "source_id")),
                        Expr::eq(Expr::col("e0", "source_kind"), Expr::string("User")),
                    ),
                ),
                TableRef::scan("gl_note", "note"),
                Expr::and(
                    Expr::eq(Expr::col("e0", "target_id"), Expr::col("note", "id")),
                    Expr::eq(Expr::col("e0", "target_kind"), Expr::string("Note")),
                ),
            ),
            order_by: vec![OrderExpr {
                expr: count_expr("note", "id"),
                desc: true,
            }],
            group_by: vec![Expr::col("author", "id")],
            where_clause: Some(Expr::func(
                "startsWith",
                vec![Expr::col("e0", "traversal_path"), Expr::string("1/")],
            )),
            ..Default::default()
        };

        apply_target_only_count_elimination(&mut q, &input);

        // COUNT target should be rewritten to edge column
        match &q.select[1].expr {
            Expr::FuncCall { args, .. } => {
                assert_eq!(
                    args[0],
                    Expr::col("e0", "target_id"),
                    "COUNT should target edge.target_id"
                );
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }

        // gl_note should be eliminated from FROM tree
        let from_str = format!("{:?}", q.from);
        assert!(
            !from_str.contains("gl_note"),
            "gl_note should be eliminated from FROM"
        );

        // target_kind condition should be preserved in WHERE
        let where_str = format!("{:?}", q.where_clause);
        assert!(
            where_str.contains("target_kind"),
            "target_kind condition should be preserved"
        );
    }
}
