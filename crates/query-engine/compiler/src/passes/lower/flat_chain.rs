use std::collections::{HashMap, HashSet};

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::helpers::{NarrowSource, emit_node_join_with_narrowing};
use super::{EmitOutput, NodeBinding};
use crate::passes::plan::physical::FlatPlan;
use crate::passes::plan::*;

pub(super) fn emit_flat_chain(plan: &Plan, physical: &FlatPlan) -> Result<EmitOutput> {
    let mut output = super::physical::emit_source(&physical.source);
    output.nodes = HashMap::new();
    output.edge_if_predicates = physical.edge_if_predicates.clone();
    output.edge_aliases = (0..plan.hops.len())
        .map(|index| format!("e{index}"))
        .collect();
    output.ctes = physical
        .filters
        .iter()
        .flat_map(|step| &step.definitions)
        .map(|alias| {
            Cte::new(
                format!("_filter_{alias}"),
                super::physical::query(&physical.narrowing[alias]),
            )
        })
        .collect();
    let mut hydrated = HashSet::new();

    for (i, hop) in plan.hops.iter().enumerate() {
        let edge_alias = &output.edge_aliases[i];
        let (start_col, end_col) = hop.direction.edge_columns();

        for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !hydrated.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = plan.nodes.get(node_alias) else {
                continue;
            };
            let binding = output
                .nodes
                .entry(node_alias.clone())
                .or_insert_with(|| NodeBinding::source(edge_alias, edge_col, None));
            if np.hydration == HydrationStrategy::Skip
                || (np.hydration == HydrationStrategy::FilterOnly
                    && physical.narrowing.contains_key(node_alias))
            {
                continue;
            }
            let narrow_source = physical.node_narrowing.get(node_alias).map(|keys| {
                let narrow_name = format!("_narrow_{}", np.alias);
                output
                    .ctes
                    .push(Cte::new(&narrow_name, super::physical::query(keys)));
                NarrowSource::Cte(narrow_name)
            });

            let table = np
                .table
                .as_deref()
                .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
            let node_sort_key = plan.table_sort_keys.get(table).ok_or_else(|| {
                QueryError::Lowering(format!("no sort key for node table '{table}'"))
            })?;
            let (from, selects, predicates) = emit_node_join_with_narrowing(
                output.from,
                np,
                edge_alias,
                edge_col,
                DEFAULT_PRIMARY_KEY,
                narrow_source,
                node_sort_key,
            )?;
            output.from = from;
            let NodeBinding::Values { table_alias, .. } = binding else {
                unreachable!()
            };
            *table_alias = Some(node_alias.clone());
            output.select.extend(selects);
            output.where_parts.extend(predicates);
        }
    }

    Ok(output)
}
