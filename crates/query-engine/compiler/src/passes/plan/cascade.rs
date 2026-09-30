use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::edge_predicates::push_filtered_edge_predicates;
use super::physical::{PhysicalPlan, PhysicalSource};
use super::{DenormalizedKey, DenormalizedProperty, Hop, HydrationStrategy, NodePlan};
use crate::ast::{Expr, SelectExpr};

pub(super) fn plan(
    hops: &[Hop],
    nodes: &HashMap<String, NodePlan>,
    table_columns: &HashMap<String, HashSet<String>>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    narrowing: &HashMap<String, PhysicalPlan>,
) -> Vec<Option<PhysicalPlan>> {
    let filtered = |alias: &String| {
        narrowing.contains_key(alias)
            || (hops.len() >= 2
                && nodes
                    .get(alias)
                    .is_some_and(|node| node.hydration == HydrationStrategy::FilterOnly))
    };
    let mut cascades: Vec<Option<PhysicalPlan>> = Vec::with_capacity(hops.len());
    for (index, hop) in hops.iter().enumerate() {
        let Some(join) = hop
            .join_prev
            .as_ref()
            .filter(|_| hop.cascade_anchor && index > 0)
        else {
            cascades.push(None);
            continue;
        };
        let previous = &hops[index - 1];
        let upstream = &cascades[index - 1];
        let selective = [&previous.from_node, &previous.to_node]
            .into_iter()
            .any(|alias| {
                filtered(alias)
                    || nodes
                        .get(alias)
                        .is_some_and(|node| !node.node_ids.is_empty() || node.id_range.is_some())
            });
        if !selective && upstream.is_none() {
            cascades.push(None);
            continue;
        }
        let alias = format!("{}p", join.prev_alias);
        let mut predicates = Vec::new();
        push_filtered_edge_predicates(
            &mut predicates,
            &alias,
            previous,
            nodes,
            table_columns,
            denormalized,
            &mut HashSet::new(),
        );
        let (start, end) = previous.direction.edge_columns();
        for (node, column) in [(&previous.from_node, start), (&previous.to_node, end)] {
            if filtered(node) {
                predicates.push(Expr::InSubquery {
                    expr: Box::new(Expr::col(&alias, column)),
                    cte_name: format!("_filter_{node}"),
                    column: DEFAULT_PRIMARY_KEY.into(),
                });
            }
        }
        let mut source = PhysicalSource::Filter {
            predicate: Expr::conjoin(predicates).expect("edge predicates"),
            input: Box::new(PhysicalSource::Scan {
                table: previous.edge_table.clone(),
                alias: alias.clone(),
                final_: false,
                relationship: Some(previous.input_index),
            }),
        };
        if let Some(upstream) = upstream {
            source = PhysicalSource::KeyFilter {
                value: Expr::col(
                    &alias,
                    &previous.join_prev.as_ref().expect("cascade join").curr_col,
                ),
                keys: Box::new(upstream.clone()),
                input: Box::new(source),
            };
        }
        cascades.push(Some(PhysicalPlan {
            source,
            outputs: vec![SelectExpr::col(&alias, &join.prev_col)],
        }));
    }
    cascades
}
