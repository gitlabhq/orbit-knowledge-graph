//! Columns a materialized relation must select: ClickHouse stores every column an `AS MATERIALIZED`
//! body selects, so the body keeps only what the relation's consumers reference.

use std::collections::{BTreeSet, HashMap, HashSet};

use ontology::constants::{DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, VERSION_COLUMN};

use super::graph::exposed_alias;
use crate::passes::plan::graph::{ExecutionPlan, RelationId, RelationNode};
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection};
use crate::passes::plan::reuse::RelationUsage;

type Consumer = (Option<RelationId>, Option<String>);

/// An empty set means the relation must keep every column.
pub(super) fn required_columns(
    plan: &ExecutionPlan,
    usage: &RelationUsage,
    root: &[Column],
) -> HashMap<RelationId, BTreeSet<String>> {
    let graph = &plan.graph;
    let mut consumers: HashMap<RelationId, Vec<Consumer>> = HashMap::new();
    let mut keys: HashMap<RelationId, BTreeSet<String>> = HashMap::new();
    consumers.entry(plan.root).or_default().push((None, None));
    for (id, node) in graph.iter().filter(|(id, _)| usage.references(*id) > 0) {
        for input in node.inputs() {
            consumers
                .entry(input.id)
                .or_default()
                .push((Some(id), input.alias.clone()));
        }
        for leaf in node.predicates().iter().flat_map(Predicate::leaves) {
            if let Predicate::Membership {
                relation,
                keys: wanted,
                ..
            } = leaf
            {
                keys.entry(*relation)
                    .or_default()
                    .extend(wanted.iter().cloned());
            }
        }
    }
    let mut root_columns = root.to_vec();
    projected(&plan.outputs, &mut root_columns);
    for binding in &plan.bindings {
        root_columns.push(Column::new(&binding.alias, &binding.column));
        root_columns.extend(binding.traversal_path.clone());
    }
    usage
        .materialized()
        .map(|id| {
            let mut aliases: HashSet<String> = exposed_alias(id, graph).into_iter().collect();
            let mut columns = Vec::new();
            let mut visited = HashSet::new();
            let mut queue = vec![id];
            while let Some(current) = queue.pop() {
                if !visited.insert(current) {
                    continue;
                }
                for (parent, alias) in consumers.get(&current).into_iter().flatten() {
                    aliases.extend(alias.clone());
                    let Some(parent) = parent else {
                        columns.extend(root_columns.iter().cloned());
                        continue;
                    };
                    let node = graph.get(*parent);
                    if matches!(node, RelationNode::UnionAll { .. }) {
                        return (id, BTreeSet::new());
                    }
                    aliases.extend(exposed_alias(*parent, graph));
                    node_columns(node, &mut columns);
                    if !matches!(node, RelationNode::Project { .. }) {
                        queue.push(*parent);
                    }
                }
            }
            let mut needed: BTreeSet<String> = columns
                .into_iter()
                .filter(|column| aliases.contains(&column.source))
                .map(|column| column.name)
                .collect();
            needed.extend(keys.get(&id).into_iter().flatten().cloned());
            (id, needed)
        })
        .collect()
}

pub(super) fn referenced(predicate: &Predicate, out: &mut Vec<Column>) {
    match predicate {
        Predicate::Boolean(expression) => {
            for leaf in expression.leaves() {
                referenced(leaf, out);
            }
        }
        Predicate::Property { column, test, .. } => {
            out.push(column.clone());
            if let Some(rhs) = test.rhs() {
                out.push(rhs.clone());
            }
        }
        Predicate::Ids { column, .. }
        | Predicate::IdRange { column, .. }
        | Predicate::EntityKind { column, .. }
        | Predicate::Tags { column, .. } => out.push(column.clone()),
        Predicate::Live { alias } => out.push(Column::new(alias, DELETED_COLUMN)),
        Predicate::ColumnEq { left, right } => out.extend([left.clone(), right.clone()]),
        Predicate::RelationshipKinds { alias, kinds } => {
            if !crate::input::is_wildcard(kinds) {
                out.push(Column::new(alias, RELATIONSHIP_KIND_COLUMN));
            }
        }
        Predicate::Membership { columns, .. } => out.extend(columns.iter().cloned()),
    }
}

pub(super) fn value_columns(value: &OutputValue, out: &mut Vec<Column>) {
    match value {
        OutputValue::Column(column) | OutputValue::EpochAsNull(column, _) => {
            out.push(column.clone())
        }
        OutputValue::Text(_) | OutputValue::Depth(_) => {}
        OutputValue::Path(steps) => {
            for (id, kind) in steps {
                out.extend([id.clone(), kind.clone()]);
            }
        }
        OutputValue::PathAppend { path, id, kind }
        | OutputValue::PathPrepend { path, id, kind } => {
            out.extend([path.clone(), id.clone(), kind.clone()]);
        }
    }
}

fn projected(outputs: &[Projection], out: &mut Vec<Column>) {
    for output in outputs {
        value_columns(&output.value, out);
    }
}

fn node_columns(node: &RelationNode, out: &mut Vec<Column>) {
    for predicate in node.predicates() {
        referenced(predicate, out);
    }
    match node {
        RelationNode::Join { endpoints, .. } => {
            out.extend([endpoints.0.clone(), endpoints.1.clone()]);
        }
        RelationNode::Latest {
            alias, sort_key, ..
        } => {
            out.extend(sort_key.iter().map(|key| Column::new(alias, key)));
            out.push(Column::new(alias, VERSION_COLUMN));
            out.extend(sort_key.iter().map(|key| Column::new(alias, key)));
        }
        RelationNode::Project { outputs, .. } => projected(outputs, out),
        _ => {}
    }
}
