//! Assembles the relation graph: the shared chain state (root, bindings, id sets), node table reads,
//! and the steps in `edge`, `fk`, and `multi_hop` that each add one relation to the chain.

mod edge;
mod fk;
mod multi_hop;

use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, NodePlan, Pattern};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::passes::plan::graph::{BindingSource, ExecutionPlan, RelationGraph, RelationId};
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection, membership};

type IdSet = (RelationId, String);

pub(super) struct RelationBuilder<'a, M: OrbitQueryModel + ?Sized> {
    pattern: &'a Pattern<'a, M>,
    root: Option<RelationId>,
    outputs: Vec<Projection>,
    nodes: HashMap<String, PlannedNode>,
    graph: RelationGraph,
    tagged: HashSet<(String, String, usize)>,
}

#[derive(Default)]
struct PlannedNode {
    binding: Option<BindingSource>,
    ids: Option<IdSet>,
    bounded_read: Option<RelationId>,
    read: Option<RelationId>,
}

impl<'a, M: OrbitQueryModel + ?Sized> RelationBuilder<'a, M> {
    pub fn new(pattern: &'a Pattern<'a, M>) -> Self {
        Self {
            pattern,
            root: None,
            outputs: Vec::new(),
            nodes: HashMap::new(),
            graph: RelationGraph::new(pattern.model.supports_selective_latest_rows()),
            tagged: HashSet::new(),
        }
    }

    pub fn build(mut self) -> Result<ExecutionPlan> {
        let pattern = self.pattern;
        if pattern.relations.is_empty() {
            return self.single_node();
        }
        for (index, relation) in pattern.relations.iter().enumerate() {
            match &relation.fk {
                Some(key) => self.add_fk(relation, key)?,
                None => self.add_edge(index, relation)?,
            }
        }
        let joined: Vec<&str> = pattern
            .input
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .filter(|alias| self.nodes.contains_key(*alias) && pattern.nodes[*alias].joined)
            .collect();
        for alias in joined {
            self.ensure_node(alias)?;
        }
        let bindings = pattern
            .input
            .nodes
            .iter()
            .filter_map(|node| self.nodes.get(&node.id)?.binding.clone())
            .collect();
        Ok(ExecutionPlan {
            root: self
                .root
                .ok_or_else(|| QueryError::Lowering("traversal has no physical source".into()))?,
            graph: self.graph,
            outputs: self.outputs,
            bindings,
        })
    }

    fn single_node(mut self) -> Result<ExecutionPlan> {
        let node = self
            .pattern
            .nodes
            .values()
            .next()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let root = self.pattern.node_read(&mut self.graph, node)?;
        Ok(ExecutionPlan {
            graph: self.graph,
            root,
            outputs: node.outputs(),
            bindings: vec![BindingSource::table(&node.alias)],
        })
    }

    fn binding(&self, node: &str) -> Option<&BindingSource> {
        self.nodes.get(node)?.binding.as_ref()
    }

    fn has_table(&self, node: &str) -> bool {
        self.binding(node)
            .is_some_and(|binding| binding.property_source.is_some())
    }

    fn ids(&self, node: &str) -> Option<&IdSet> {
        self.nodes.get(node)?.ids.as_ref()
    }

    /// Ids a non-selective node received from the edges around it; a selective node reads its own.
    fn reached_ids(&self, node: &str) -> Option<IdSet> {
        if self.pattern.endpoint_is_selective(node) {
            return None;
        }
        self.ids(node).cloned()
    }

    fn set_ids(&mut self, node: &str, ids: IdSet) {
        self.nodes.entry(node.to_string()).or_default().ids = Some(ids);
    }

    fn set_binding(
        &mut self,
        node: &str,
        alias: &str,
        column: &str,
        property_source: Option<&str>,
        traversal_path: Option<Column>,
    ) {
        self.nodes.entry(node.to_string()).or_default().binding = Some(BindingSource {
            node: node.to_string(),
            alias: alias.to_string(),
            column: column.to_string(),
            property_source: property_source.map(str::to_string),
            traversal_path,
        });
    }

    fn bind_table(&mut self, node: &NodePlan) {
        let traversal_path = node
            .has_traversal_path
            .then(|| Column::new(&node.alias, TRAVERSAL_PATH_COLUMN));
        self.set_binding(
            &node.alias,
            &node.alias,
            DEFAULT_PRIMARY_KEY,
            Some(&node.alias),
            traversal_path,
        );
    }

    fn join_root(
        &mut self,
        scan: RelationId,
        alias: &str,
        on: impl FnOnce(&Self) -> Result<(Column, Column)>,
    ) -> Result<()> {
        let Some(root) = self.root.take() else {
            self.root = Some(scan);
            return Ok(());
        };
        let on = on(self)?;
        self.root = Some(self.graph.join(root, scan.alias(alias), on, vec![]));
        Ok(())
    }

    fn ensure_node(&mut self, alias: &str) -> Result<()> {
        if self.has_table(alias) {
            return Ok(());
        }
        let mut node = self.pattern.uncovered_node(alias);
        if let Some(ids) = self.reached_ids(alias) {
            node.predicates
                .push(membership(Column::new(alias, DEFAULT_PRIMARY_KEY), ids));
        }
        self.attach_table(&node, |builder| {
            let binding = builder.binding(alias).ok_or_else(|| {
                QueryError::Lowering(format!(
                    "node '{alias}' is not connected to the planned relations"
                ))
            })?;
            Ok((
                Column::new(alias, DEFAULT_PRIMARY_KEY),
                Column::new(&binding.alias, &binding.column),
            ))
        })
    }

    /// Reads a node's table into the chain, joined on the columns `on` picks, and binds the node to it.
    fn attach_table(
        &mut self,
        node: &NodePlan,
        on: impl FnOnce(&Self) -> Result<(Column, Column)>,
    ) -> Result<()> {
        let scan = self.node_read(node)?;
        self.join_root(scan, &node.alias, on)?;
        self.outputs.extend(node.outputs());
        self.bind_table(node);
        Ok(())
    }

    fn push_relation_outputs(&mut self, relation: &EdgeRelation, values: [OutputValue; 5]) {
        if self.pattern.aggregate() {
            return;
        }
        let prefix = relation.column_prefix();
        let suffixes = [
            EDGE_TYPE_SUFFIX,
            EDGE_SRC_SUFFIX,
            EDGE_SRC_TYPE_SUFFIX,
            EDGE_DST_SUFFIX,
            EDGE_DST_TYPE_SUFFIX,
        ];
        self.outputs.extend(
            values
                .into_iter()
                .zip(suffixes)
                .map(|(value, suffix)| Projection::new(value, format!("{prefix}{suffix}"))),
        );
    }

    fn node_read(&mut self, node: &NodePlan) -> Result<RelationId> {
        let read = self.pattern.node_read(&mut self.graph, node)?;
        let state = self.nodes.entry(node.alias.clone()).or_default();
        state.read = Some(read);
        if self
            .pattern
            .node_conditions(node)
            .iter()
            .any(Predicate::is_selective)
        {
            state.bounded_read = Some(read);
        }
        Ok(read)
    }

    fn node_ids(&mut self, alias: &str) -> Result<IdSet> {
        if let Some(ids) = self.ids(alias) {
            return Ok(ids.clone());
        }
        let node = self.pattern.nodes[alias].clone();
        self.node_ids_from(&node)
    }

    fn node_ids_from(&mut self, node: &NodePlan) -> Result<IdSet> {
        if let Some(ids) = self.ids(&node.alias) {
            return Ok(ids.clone());
        }
        let source = self.node_read(node)?;
        let id = self.graph.project(
            source,
            vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, DEFAULT_PRIMARY_KEY)),
                DEFAULT_PRIMARY_KEY,
            )],
        );
        let ids = (id, DEFAULT_PRIMARY_KEY.to_string());
        self.set_ids(&node.alias, ids.clone());
        Ok(ids)
    }

    /// Keeps the ids an edge read produces for an endpoint when a later step can narrow by them.
    fn bind_relation_ids(&mut self, index: usize, relation: &EdgeRelation, source: RelationId) {
        let pattern = self.pattern;
        let (start, end) = relation.direction.edge_columns();
        let bounded = [&relation.from_node, &relation.to_node]
            .into_iter()
            .any(|node| self.ids(node).is_some() || pattern.endpoint_is_selective(node));
        for (node, column) in [(&relation.from_node, start), (&relation.to_node, end)] {
            if self.ids(node).is_some() {
                continue;
            }
            let other = relation.other_end(node);
            let used_later = bounded
                && pattern.relations[index + 1..]
                    .iter()
                    .any(|next| next.endpoint_column(node).is_some())
                || pattern.model.supports_selective_latest_rows()
                    && pattern.nodes[node].joined
                    && (pattern.endpoint_is_selective(other) || self.ids(other).is_some());
            if used_later {
                let ids = self.ids_reached_by(source, column);
                self.set_ids(node, ids);
            }
        }
    }

    fn endpoint_ids(&mut self, node: &str) -> Option<IdSet> {
        if let Some(ids) = self.ids(node) {
            return Some(ids.clone());
        }
        let ids = self.ids_through_holder(node, DEFAULT_PRIMARY_KEY)?;
        self.set_ids(node, ids.clone());
        Some(ids)
    }

    fn ids_through_holder(&mut self, holder: &str, column: &str) -> Option<IdSet> {
        if !self.pattern.model.supports_selective_latest_rows() {
            return None;
        }
        let read = self.nodes.get(holder)?.bounded_read?;
        Some(self.ids_reached_by(read, column))
    }

    fn ids_reached_by(&mut self, source: RelationId, column: &str) -> IdSet {
        let bound = self.graph.bind(source, "_id_source");
        let id = self.graph.project(
            bound,
            vec![Projection::new(
                OutputValue::Column(Column::new("_id_source", column)),
                DEFAULT_PRIMARY_KEY,
            )],
        );
        (id, DEFAULT_PRIMARY_KEY.to_string())
    }
}
