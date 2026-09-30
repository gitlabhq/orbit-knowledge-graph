use std::collections::{HashMap, HashSet};

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::helpers::{
    NarrowSource, emit_filter_narrowing, emit_filter_subquery, emit_node_join_with_narrowing,
};
use super::{EmitOutput, NodeBinding};
use crate::passes::plan::edge_predicates::{
    node_id_pin_predicates, push_denorm_tags, push_edge_predicates, push_filtered_edge_predicates,
};
use crate::passes::plan::physical::{EdgeRead, FlatPlan, PhysicalSource};
use crate::passes::plan::*;
use crate::passes::shared::deleted_false;
use crate::passes::shared::filter_to_expr;

pub(super) fn emit_flat_chain(plan: &Plan, physical: &FlatPlan) -> Result<EmitOutput> {
    let reads = &physical.reads;
    if reads.len() != plan.hops.len() {
        return Err(QueryError::Lowering(
            "each flat-chain hop requires a planned read".into(),
        ));
    }
    let dedup_edges = plan.hops.len() >= 2;

    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();
    let mut from: Option<TableRef> = None;
    let mut tagged_nodes: HashSet<(String, String)> = HashSet::new();
    let mut narrowed_nodes: HashSet<String> = HashSet::new();
    let mut filter_only_done: HashSet<String> = HashSet::new();
    let mut edge_if_predicates: Option<Expr> = None;

    for (i, (hop, read)) in plan.hops.iter().zip(reads).enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        let is_multi_hop = matches!(read, EdgeRead::MultiHop(_));
        let scan = |final_| PhysicalSource::Scan {
            relationship: Some(hop.input_index),
            table: hop.edge_table.clone(),
            alias: alias.clone(),
            final_,
        };

        if let EdgeRead::Latest { sort_key } = read {
            let mut inner_preds = Vec::new();
            push_filtered_edge_predicates(
                &mut inner_preds,
                &alias,
                hop,
                &plan.nodes,
                &plan.table_columns,
                &plan.denormalized,
                &mut tagged_nodes,
            );
            emit_filter_narrowing(
                &mut inner_preds,
                hop,
                physical,
                &alias,
                start_col,
                end_col,
                &mut ctes,
                &mut narrowed_nodes,
            );

            edge_if_predicates = Expr::conjoin(inner_preds.clone());

            let source = PhysicalSource::Latest {
                sort_key: sort_key.clone(),
                alias: alias.clone(),
                input: Box::new(PhysicalSource::Filter {
                    predicate: Expr::conjoin(inner_preds).expect("edge predicates"),
                    input: Box::new(scan(false)),
                }),
            };
            from = Some(super::physical::emit_source(&source).from);
        } else {
            let mut narrow_in: Vec<Expr> = Vec::new();
            emit_filter_narrowing(
                &mut narrow_in,
                hop,
                physical,
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
                    let is_filter_only = matches!(np.hydration, HydrationStrategy::FilterOnly);
                    if is_filter_only && filter_only_done.insert(node_alias.clone()) {
                        narrow_in.extend(emit_filter_subquery(
                            np,
                            &alias,
                            edge_col,
                            DEFAULT_PRIMARY_KEY,
                            &mut ctes,
                        )?);
                    }
                }
            }

            if let Some(anchor) = &physical.cascades[i] {
                let jc = hop
                    .join_prev
                    .as_ref()
                    .expect("cascade-anchored hop must have join_prev");
                narrow_in.push(Expr::InSelect {
                    expr: Box::new(Expr::col(&alias, &jc.curr_col)),
                    query: Box::new(super::physical::query(anchor)),
                });
            }

            let edge_source = if let EdgeRead::MultiHop(source) = read {
                let output = super::physical::emit_source(source);
                where_parts.extend(output.where_parts);
                where_parts.extend(narrow_in);
                output.from
            } else if let EdgeRead::Final { narrow_inside } = read {
                let mut inner = node_id_pin_predicates(&alias, hop, &plan.nodes);
                if *narrow_inside {
                    inner.extend(narrow_in);
                } else {
                    where_parts.extend(narrow_in);
                }
                inner.push(deleted_false(&alias));
                let source = PhysicalSource::Scope {
                    alias: alias.clone(),
                    input: Box::new(PhysicalSource::Filter {
                        predicate: Expr::conjoin(inner).expect("deletion predicate"),
                        input: Box::new(scan(true)),
                    }),
                };
                super::physical::emit_source(&source).from
            } else {
                where_parts.extend(narrow_in);
                super::physical::emit_source(&scan(false)).from
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

            push_denorm_tags(
                &mut where_parts,
                &plan.nodes,
                &plan.denormalized,
                hop,
                &alias,
                &mut tagged_nodes,
            );
            let used_dedup = dedup_edges && !is_multi_hop;
            if !used_dedup {
                where_parts.extend(node_id_pin_predicates(&alias, hop, &plan.nodes));
            }
        }

        edge_aliases.push(alias);
    }

    let mut from = from.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?;
    let mut selects = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();
    let mut nodes = HashMap::new();

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
            let binding = nodes
                .entry(node_alias.clone())
                .or_insert_with(|| NodeBinding::source(edge_alias, edge_col, None));
            if np.hydration == HydrationStrategy::Skip
                || (np.hydration == HydrationStrategy::FilterOnly
                    && !filter_only_done.insert(node_alias.clone()))
            {
                continue;
            }
            let narrow_source = if np.hydration == HydrationStrategy::Join && np.use_narrowing {
                let narrow_alias = format!("{edge_alias}n");
                let mut nw = Vec::new();
                push_edge_predicates(
                    &mut nw,
                    &narrow_alias,
                    hop,
                    &plan.nodes,
                    &plan.table_columns,
                    false,
                );
                nw.extend(node_id_pin_predicates(&narrow_alias, hop, &plan.nodes));
                if let Some(anchor) = &physical.cascades[i] {
                    let jc = hop
                        .join_prev
                        .as_ref()
                        .expect("cascade-anchored hop must have join_prev");
                    nw.push(Expr::InSelect {
                        expr: Box::new(Expr::col(&narrow_alias, &jc.curr_col)),
                        query: Box::new(super::physical::query(anchor)),
                    });
                }
                let narrow_query = Query {
                    select: vec![SelectExpr::new(
                        Expr::col(&narrow_alias, edge_col),
                        DEFAULT_PRIMARY_KEY,
                    )],
                    from: TableRef::scan(&hop.edge_table, &narrow_alias)
                        .with_relationship(hop.input_index),
                    where_clause: Expr::conjoin(nw),
                    ..Default::default()
                };
                let narrow_name = format!("_narrow_{}", np.alias);
                ctes.push(Cte::new(&narrow_name, narrow_query));
                Some(NarrowSource::Cte(narrow_name))
            } else {
                None
            };

            let table = np
                .table
                .as_deref()
                .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
            let node_sort_key = plan.table_sort_keys.get(table).ok_or_else(|| {
                QueryError::Lowering(format!("no sort key for node table '{table}'"))
            })?;
            let (new_from, ns, nw) = emit_node_join_with_narrowing(
                from,
                np,
                edge_alias,
                edge_col,
                DEFAULT_PRIMARY_KEY,
                narrow_source,
                node_sort_key,
            )?;
            from = new_from;
            let NodeBinding::Values { table_alias, .. } = binding else {
                unreachable!()
            };
            *table_alias = Some(node_alias.clone());
            selects.extend(ns);
            where_parts.extend(nw);
        }
    }

    Ok(EmitOutput {
        nodes,
        from,
        edge_aliases,
        where_parts,
        select: selects,
        ctes,
        edge_if_predicates,
    })
}
