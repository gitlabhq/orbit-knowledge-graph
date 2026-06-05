//! Emit: flat edge chain.

use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::EmitOutput;
use super::helpers::{
    NarrowSource, build_multi_hop_union, dedup_edge_scan, emit_denorm_tags, emit_filter_narrowing,
    emit_filter_subquery, emit_node_ids_on_edge, emit_node_join_with_narrowing, limit_by_scan,
    node_id_pin_predicates, push_edge_predicates,
};
use crate::passes::plan::*;
use crate::passes::shared::filter_to_expr;

/// `startsWith(<alias>.traversal_path, '<prefix>')` for a hop confined to a
/// project/group scope, or `None` when the hop carries no resolved prefix.
/// Emitted alongside the broad authorization filter so ClickHouse can seek the
/// edge PK to the project's contiguous range instead of the whole org.
fn edge_scope_predicate(hop: &Hop, alias: &str) -> Option<Expr> {
    hop.scope_prefix.as_deref().map(|prefix| {
        Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::string(prefix),
            ],
        )
    })
}

/// Collect all edge predicates for a hop into a target vec.
#[allow(clippy::too_many_arguments)]
fn collect_edge_predicates(
    target: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    plan: &Plan,
    start_col: &str,
    end_col: &str,
    ctes: &mut Vec<Cte>,
    tagged_nodes: &mut HashSet<(String, String)>,
    narrowed_nodes: &mut HashSet<String>,
) {
    push_edge_predicates(target, alias, hop, &plan.nodes, &plan.table_columns, false);
    for (prop, filter) in &hop.filters {
        target.push(filter_to_expr(alias, prop, filter));
    }
    emit_denorm_tags(target, plan, hop, alias, start_col, end_col, tagged_nodes);
    emit_node_ids_on_edge(target, alias, hop, &plan.nodes, start_col, end_col);
    emit_filter_narrowing(
        target,
        hop,
        &plan.nodes,
        alias,
        start_col,
        end_col,
        ctes,
        narrowed_nodes,
    );
}

pub(super) fn emit_flat_chain(plan: &Plan) -> Result<EmitOutput> {
    let is_aggregation = matches!(plan.body, PlanBody::Aggregation { .. });
    let dedup_edges = plan.hops.len() >= 2;

    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();
    let mut from: Option<TableRef> = None;
    let mut tagged_nodes: HashSet<(String, String)> = HashSet::new();
    let mut narrowed_nodes: HashSet<String> = HashSet::new();
    let mut filter_only_done: HashSet<String> = HashSet::new();
    let mut edge_if_predicates: Option<Expr> = None;

    for (i, hop) in plan.hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        let is_multi_hop = hop.max_hops > 1;

        // Single-hop aggregation: use LIMIT BY dedup with -If combinators
        // instead of FINAL.
        let use_limit_by = is_aggregation && !dedup_edges && !is_multi_hop;

        if use_limit_by {
            let Some(sort_key) = plan.table_sort_keys.get(&hop.edge_table) else {
                return Err(QueryError::Lowering(format!(
                    "no sort key for edge table '{}'; cannot emit LIMIT BY dedup",
                    hop.edge_table
                )));
            };
            if sort_key.is_empty() {
                return Err(QueryError::Lowering(format!(
                    "sort key for edge table '{}' is empty; cannot emit LIMIT BY dedup",
                    hop.edge_table
                )));
            }
            let mut inner_preds = Vec::new();
            collect_edge_predicates(
                &mut inner_preds,
                &alias,
                hop,
                plan,
                start_col,
                end_col,
                &mut ctes,
                &mut tagged_nodes,
                &mut narrowed_nodes,
            );

            inner_preds.extend(edge_scope_predicate(hop, &alias));

            edge_if_predicates = Expr::conjoin(inner_preds.clone());

            from = Some(limit_by_scan(
                &hop.edge_table,
                &alias,
                vec![SelectExpr::star()],
                sort_key,
                inner_preds,
            ));
        } else {
            let mut narrow_in: Vec<Expr> = Vec::new();
            emit_filter_narrowing(
                &mut narrow_in,
                hop,
                &plan.nodes,
                &alias,
                start_col,
                end_col,
                &mut ctes,
                &mut narrowed_nodes,
            );
            // For multi-hop dedup queries, FilterOnly nodes still use
            // CTEs so their IN-subqueries can be pushed inside the edge
            // dedup scan for PK pruning. Single-hop queries handle
            // FilterOnly via JOIN in the node processing loop below.
            if dedup_edges {
                for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)]
                {
                    let Some(np) = plan.nodes.get(node_alias) else {
                        continue;
                    };
                    let elevated_skip =
                        matches!(np.hydration, HydrationStrategy::Skip) && np.needs_elevated_filter;
                    let is_filter_only = matches!(np.hydration, HydrationStrategy::FilterOnly);
                    if (is_filter_only || elevated_skip)
                        && filter_only_done.insert(node_alias.clone())
                    {
                        narrow_in.extend(emit_filter_subquery(np, &alias, edge_col, &mut ctes)?);
                    }
                }
            }

            let edge_pk_leading: Vec<&str> = plan
                .table_sort_keys
                .get(&hop.edge_table)
                .map(|k| k.iter().take(4).map(String::as_str).collect())
                .unwrap_or_default();
            let push_narrow_inner =
                edge_pk_leading.contains(&start_col) || edge_pk_leading.contains(&end_col);

            let edge_source = if is_multi_hop {
                let (union, union_wheres) = build_multi_hop_union(hop, &alias, &plan.nodes);
                where_parts.extend(union_wheres);
                where_parts.extend(narrow_in);
                union
            } else if dedup_edges {
                let mut inner = node_id_pin_predicates(&alias, hop, &plan.nodes);
                inner.extend(edge_scope_predicate(hop, &alias));
                if push_narrow_inner {
                    inner.extend(narrow_in);
                } else {
                    where_parts.extend(narrow_in);
                }
                dedup_edge_scan(&hop.edge_table, &alias, &plan.table_columns, inner)
            } else {
                where_parts.extend(narrow_in);
                where_parts.extend(edge_scope_predicate(hop, &alias));
                TableRef::scan(&hop.edge_table, &alias)
            };

            if let Some(prev_from) = from.take() {
                let jc = hop
                    .join_prev
                    .as_ref()
                    .expect("non-first hop must have join_prev");
                from = Some(TableRef::join(
                    JoinType::Inner,
                    prev_from,
                    edge_source,
                    Expr::eq(
                        Expr::col(&jc.prev_alias, &jc.prev_col),
                        Expr::col(&alias, &jc.curr_col),
                    ),
                ));
            } else {
                from = Some(edge_source);
            }

            if !is_multi_hop {
                push_edge_predicates(
                    &mut where_parts,
                    &alias,
                    hop,
                    &plan.nodes,
                    &plan.table_columns,
                    dedup_edges,
                );
            }

            for (prop, filter) in &hop.filters {
                where_parts.push(filter_to_expr(&alias, prop, filter));
            }

            emit_denorm_tags(
                &mut where_parts,
                plan,
                hop,
                &alias,
                start_col,
                end_col,
                &mut tagged_nodes,
            );
            let used_dedup = dedup_edges && !is_multi_hop;
            if !used_dedup {
                emit_node_ids_on_edge(
                    &mut where_parts,
                    &alias,
                    hop,
                    &plan.nodes,
                    start_col,
                    end_col,
                );
            }
        }

        edge_aliases.push(alias);
    }

    let mut from = from.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?;
    let mut selects = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();

    for (i, hop) in plan.hops.iter().enumerate() {
        let edge_alias = &edge_aliases[i];
        let (start_col, end_col) = hop.direction.edge_columns();

        for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !hydrated.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = plan.nodes.get(node_alias) else {
                continue;
            };
            match np.hydration {
                HydrationStrategy::Join => {
                    let narrow_source = if np.use_narrowing {
                        let narrow_alias = format!("{edge_alias}n");
                        let narrow_query = Query {
                            select: vec![SelectExpr::new(
                                Expr::col(&narrow_alias, edge_col),
                                DEFAULT_PRIMARY_KEY,
                            )],
                            distinct: true,
                            from: TableRef::scan(&hop.edge_table, &narrow_alias),
                            where_clause: {
                                let mut nw = Vec::new();
                                push_edge_predicates(
                                    &mut nw,
                                    &format!("{edge_alias}n"),
                                    hop,
                                    &plan.nodes,
                                    &plan.table_columns,
                                    false,
                                );
                                Expr::conjoin(nw)
                            },
                            ..Default::default()
                        };
                        let narrow_name = format!("_narrow_{}", np.alias);
                        ctes.push(Cte::new(&narrow_name, narrow_query));
                        Some(NarrowSource::Cte(narrow_name))
                    } else {
                        None
                    };

                    let table = np.table.as_deref().ok_or_else(|| {
                        QueryError::Lowering(format!("node '{}' has no table", np.alias))
                    })?;
                    let node_sort_key = plan.table_sort_keys.get(table).ok_or_else(|| {
                        QueryError::Lowering(format!("no sort key for node table '{table}'"))
                    })?;
                    let (new_from, ns, nw) = emit_node_join_with_narrowing(
                        from,
                        np,
                        edge_alias,
                        edge_col,
                        false,
                        narrow_source,
                        node_sort_key,
                    )?;
                    from = new_from;
                    selects.extend(ns);
                    where_parts.extend(nw);
                }
                HydrationStrategy::FilterOnly => {
                    if filter_only_done.insert(node_alias.clone()) {
                        let table = np.table.as_deref().ok_or_else(|| {
                            QueryError::Lowering(format!("node '{}' has no table", np.alias))
                        })?;
                        let node_sort_key = plan.table_sort_keys.get(table).ok_or_else(|| {
                            QueryError::Lowering(format!("no sort key for node table '{table}'"))
                        })?;
                        let (new_from, _selects, nw) = emit_node_join_with_narrowing(
                            from,
                            np,
                            edge_alias,
                            edge_col,
                            false,
                            None,
                            node_sort_key,
                        )?;
                        from = new_from;
                        where_parts.extend(nw);
                    }
                }
                HydrationStrategy::Skip => {
                    if np.needs_elevated_filter && filter_only_done.insert(node_alias.clone()) {
                        let table = np.table.as_deref().ok_or_else(|| {
                            QueryError::Lowering(format!("node '{}' has no table", np.alias))
                        })?;
                        let node_sort_key = plan.table_sort_keys.get(table).ok_or_else(|| {
                            QueryError::Lowering(format!("no sort key for node table '{table}'"))
                        })?;
                        let (new_from, _selects, nw) = emit_node_join_with_narrowing(
                            from,
                            np,
                            edge_alias,
                            edge_col,
                            false,
                            None,
                            node_sort_key,
                        )?;
                        from = new_from;
                        where_parts.extend(nw);
                    }
                }
            }
        }
    }

    Ok(EmitOutput {
        from,
        edge_aliases,
        where_parts,
        select: selects,
        ctes,
        edge_if_predicates,
    })
}
