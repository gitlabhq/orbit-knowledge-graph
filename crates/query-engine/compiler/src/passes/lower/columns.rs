//! Columns a materialized relation must select: ClickHouse stores every column an `AS MATERIALIZED`
//! body selects, so the body keeps only what the relation's consumers reference.

use std::collections::{BTreeSet, HashMap, HashSet};

use ontology::constants::VERSION_COLUMN;

use crate::passes::plan::graph::{ExecutionPlan, RelationId, RelationNode};
use crate::passes::plan::predicates::{Column, Predicate};

type Consumer = (Option<RelationId>, Option<String>);

/// An empty set means the relation must keep every column.
pub(super) fn required_columns(
    plan: &ExecutionPlan,
    materialized: &HashSet<RelationId>,
    root_columns: &[Column],
) -> HashMap<RelationId, BTreeSet<String>> {
    let graph = &plan.graph;
    let mut consumers: HashMap<RelationId, Vec<Consumer>> = HashMap::new();
    let mut key_needs: HashMap<RelationId, BTreeSet<String>> = HashMap::new();
    consumers.entry(plan.root).or_default().push((None, None));
    let mut seen = HashSet::new();
    let mut stack = vec![plan.root];
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = graph.get(id);
        for input in node.inputs() {
            consumers
                .entry(input.id)
                .or_default()
                .push((Some(id), input.alias.clone()));
            stack.push(input.id);
        }
        for leaf in node.predicates().iter().flat_map(Predicate::leaves) {
            if let Predicate::Membership { relation, keys, .. } = leaf {
                key_needs
                    .entry(*relation)
                    .or_default()
                    .extend(keys.iter().cloned());
                stack.push(*relation);
            }
        }
    }

    let mut output_columns = root_columns.to_vec();
    for projection in &plan.outputs {
        projection.value.columns(&mut output_columns);
    }
    for binding in &plan.bindings {
        output_columns.push(Column::new(&binding.alias, &binding.column));
        output_columns.extend(binding.traversal_path.clone());
    }

    materialized
        .iter()
        .map(|&id| {
            let mut aliases: HashSet<String> =
                super::graph::exposed_alias(id, graph).into_iter().collect();
            let mut referenced = Vec::new();
            let mut visited = HashSet::new();
            let mut queue = vec![id];
            while let Some(current) = queue.pop() {
                if !visited.insert(current) {
                    continue;
                }
                for (parent, alias) in consumers.get(&current).into_iter().flatten() {
                    aliases.extend(alias.clone());
                    let Some(parent) = parent else {
                        referenced.extend(output_columns.iter().cloned());
                        continue;
                    };
                    let node = graph.get(*parent);
                    if matches!(node, RelationNode::UnionAll { .. }) {
                        return (id, BTreeSet::new());
                    }
                    aliases.extend(super::graph::exposed_alias(*parent, graph));
                    node_columns(node, &mut referenced);
                    if !matches!(node, RelationNode::Project { .. }) {
                        queue.push(*parent);
                    }
                }
            }
            let mut columns: BTreeSet<String> = referenced
                .into_iter()
                .filter(|column| aliases.contains(&column.source))
                .map(|column| column.name)
                .collect();
            columns.extend(key_needs.get(&id).into_iter().flatten().cloned());
            (id, columns)
        })
        .collect()
}

fn node_columns(node: &RelationNode, out: &mut Vec<Column>) {
    for predicate in node.predicates() {
        predicate.columns(out);
    }
    match node {
        RelationNode::Join { endpoints, .. } => {
            out.push(endpoints.0.clone());
            out.push(endpoints.1.clone());
        }
        RelationNode::Latest {
            alias, sort_key, ..
        } => {
            out.extend(sort_key.iter().map(|key| Column::new(alias, key)));
            out.push(Column::new(alias, VERSION_COLUMN));
        }
        RelationNode::Project { outputs, .. } => {
            for projection in outputs {
                projection.value.columns(out);
            }
        }
        _ => {}
    }
}
