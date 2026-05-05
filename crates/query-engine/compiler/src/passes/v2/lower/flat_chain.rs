//! Emit: flat edge chain.

use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::super::plan::*;
use super::super::shared::filter_to_expr;
use super::EmitOutput;
use super::helpers::{
    build_multi_hop_union, emit_denorm_tags, emit_filter_narrowing, emit_filter_subquery,
    emit_node_ids_on_edge, emit_node_join_with_narrowing, push_edge_predicates,
};

pub(super) fn emit_flat_chain(plan: &Plan) -> Result<EmitOutput> {
    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();
    let mut from: Option<TableRef> = None;
    let mut tagged_nodes: HashSet<String> = HashSet::new();
    let mut narrowed_nodes: HashSet<String> = HashSet::new();

    for (i, hop) in plan.hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        let is_multi_hop = hop.max_hops > 1;

        // Build edge source: UNION ALL for multi-hop, plain scan for single.
        let edge_source = if is_multi_hop {
            let (union, union_wheres) = build_multi_hop_union(hop, &alias, &plan.nodes);
            where_parts.extend(union_wheres);
            union
        } else {
            TableRef::scan(&hop.edge_table, &alias)
        };

        // JOIN to previous hop (or set as initial FROM) using pre-resolved
        // join columns.
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
                start_col,
                end_col,
            );
        }

        // Relationship-level filters (edge property predicates from the query).
        for (prop, filter) in &hop.filters {
            where_parts.push(filter_to_expr(&alias, prop, filter));
        }

        // Compute denorm tags from plan.denorm_columns.
        emit_denorm_tags(
            &mut where_parts,
            plan,
            hop,
            &alias,
            start_col,
            end_col,
            &mut tagged_nodes,
        );
        emit_node_ids_on_edge(
            &mut where_parts,
            &alias,
            hop,
            &plan.nodes,
            start_col,
            end_col,
        );
        emit_filter_narrowing(
            &mut where_parts,
            hop,
            &plan.nodes,
            &alias,
            start_col,
            end_col,
            &mut ctes,
            &mut narrowed_nodes,
        );

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
                    // Use pre-resolved narrowing decision from plan().
                    // IMPORTANT NOTE: Only emit _narrow_* when NO _filter_* CTEs exist yet —
                    // otherwise the _narrow_ CTE's edge predicates would
                    // reference _filter_* via IN, creating a correlated
                    // subquery that ClickHouse rejects for parameterized queries.
                    let narrow_cte = if np.use_narrowing
                        && !ctes.iter().any(|c: &Cte| c.name.starts_with("_filter_"))
                    {
                        let narrow_name = format!("_narrow_{}", np.alias);
                        let narrow_query = Query {
                            select: vec![SelectExpr::new(
                                Expr::col(edge_alias, edge_col),
                                DEFAULT_PRIMARY_KEY,
                            )],
                            from: TableRef::scan(&hop.edge_table, format!("{edge_alias}n")),
                            where_clause: {
                                let mut nw = Vec::new();
                                push_edge_predicates(
                                    &mut nw,
                                    &format!("{edge_alias}n"),
                                    hop,
                                    &plan.nodes,
                                    start_col,
                                    end_col,
                                );
                                Expr::conjoin(nw)
                            },
                            ..Default::default()
                        };
                        ctes.push(Cte::new(&narrow_name, narrow_query));
                        Some(narrow_name)
                    } else {
                        None
                    };

                    let (new_from, ns, nw) = emit_node_join_with_narrowing(
                        from,
                        np,
                        edge_alias,
                        edge_col,
                        false,
                        narrow_cte.as_deref(),
                    )?;
                    from = new_from;
                    selects.extend(ns);
                    where_parts.extend(nw);
                }
                HydrationStrategy::FilterOnly => {
                    where_parts.extend(emit_filter_subquery(np, edge_alias, edge_col, &mut ctes)?);
                }
                HydrationStrategy::Skip => {
                    // Elevated-access nodes always need a FilterOnly CTE so
                    // the security pass can enforce the stricter
                    // min_access_level. Without the CTE, SecurityPass never
                    // sees the node's table and can't inject the role-gated
                    // startsWith filter.
                    if np.needs_elevated_filter {
                        where_parts
                            .extend(emit_filter_subquery(np, edge_alias, edge_col, &mut ctes)?);
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
    })
}
