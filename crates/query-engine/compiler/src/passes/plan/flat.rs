use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, JoinType};
use crate::error::{QueryError, Result};
use crate::passes::shared::{deleted_false, filter_to_expr};

use super::edge_predicates::{
    node_id_pin_predicates, push_denorm_tags, push_edge_predicates, push_filtered_edge_predicates,
};
use super::physical::{HopFilters, PhysicalPlan, PhysicalSource};
use super::{DenormalizedKey, DenormalizedProperty, Hop, NodePlan};

#[allow(clippy::too_many_arguments)]
pub(super) fn edge_source(
    hops: &[Hop],
    aggregate: bool,
    sort_keys: &HashMap<String, Vec<String>>,
    nodes: &HashMap<String, NodePlan>,
    table_columns: &HashMap<String, HashSet<String>>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    filters: &[HopFilters],
    cascades: &[Option<PhysicalPlan>],
) -> Result<(PhysicalSource, Option<Expr>)> {
    let mut source = None;
    let mut tagged = HashSet::new();
    let dedup = hops.len() >= 2;
    for (index, hop) in hops.iter().enumerate() {
        let alias = format!("e{index}");
        let scan = |final_| PhysicalSource::Scan {
            table: hop.edge_table.clone(),
            alias: alias.clone(),
            final_,
            relationship: Some(hop.input_index),
        };
        let multi_hop = hop.max_hops > 1;
        let membership = &filters[index].predicates;
        let cascade = cascades[index].as_ref();
        if !multi_hop && !dedup && aggregate {
            let sort_key = sort_keys
                .get(&hop.edge_table)
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    QueryError::Lowering(format!(
                        "no sort key for edge table '{}'; cannot plan latest rows",
                        hop.edge_table
                    ))
                })?;
            let mut predicates = Vec::new();
            push_filtered_edge_predicates(
                &mut predicates,
                &alias,
                hop,
                nodes,
                table_columns,
                denormalized,
                &mut tagged,
            );
            predicates.extend(membership.clone());
            let condition = Expr::conjoin(predicates.clone());
            return Ok((
                PhysicalSource::Latest {
                    sort_key: sort_key.clone(),
                    alias: alias.clone(),
                    input: Box::new(scan(false).filter(predicates)),
                },
                condition,
            ));
        }
        let edge = if multi_hop {
            super::hops::multi_hop(hop, &alias, nodes)
                .filter(membership.clone())
                .cascade(hop, &alias, cascade)
        } else if dedup {
            let (start, end) = hop.direction.edge_columns();
            let narrow_inside = sort_keys
                .get(&hop.edge_table)
                .is_some_and(|keys| keys.iter().take(4).any(|key| key == start || key == end));
            let mut input = scan(true).filter(node_id_pin_predicates(&alias, hop, nodes));
            if narrow_inside {
                input = input
                    .filter(membership.clone())
                    .cascade(hop, &alias, cascade);
            }
            let scoped = PhysicalSource::Scope {
                alias: alias.clone(),
                input: Box::new(input.filter(vec![deleted_false(&alias)])),
            };
            if narrow_inside {
                scoped
            } else {
                scoped
                    .filter(membership.clone())
                    .cascade(hop, &alias, cascade)
            }
        } else {
            scan(false)
                .filter(membership.clone())
                .cascade(hop, &alias, cascade)
        };
        let mut predicates = Vec::new();
        if !multi_hop {
            push_edge_predicates(&mut predicates, &alias, hop, nodes, table_columns, dedup);
        }
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| filter_to_expr(&alias, property, filter)),
        );
        push_denorm_tags(
            &mut predicates,
            nodes,
            denormalized,
            hop,
            &alias,
            &mut tagged,
        );
        if !dedup || multi_hop {
            predicates.extend(node_id_pin_predicates(&alias, hop, nodes));
        }
        let edge = edge.filter(predicates);
        source = Some(match source {
            Some(previous) => {
                let join = hop
                    .join_prev
                    .as_ref()
                    .expect("non-first hop must have join_prev");
                PhysicalSource::Join {
                    kind: JoinType::Inner,
                    condition: Expr::eq(
                        Expr::col(&join.prev_alias, &join.prev_col),
                        Expr::col(&alias, &join.curr_col),
                    ),
                    left: Box::new(previous),
                    right: Box::new(edge),
                }
            }
            None => edge,
        });
    }
    Ok((
        source.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?,
        None,
    ))
}
