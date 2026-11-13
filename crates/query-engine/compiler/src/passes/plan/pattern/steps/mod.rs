//! Builds the pattern's relations: the shared chain state (root, bindings, id sets), node table
//! reads, and the steps in `edge`, `fk`, and `multi_hop` that each add one relation to the chain.

mod edge;
mod fk;
mod multi_hop;

use crate::passes::codegen::SqlBackend;
use crate::passes::plan::build::stored;
use std::collections::{HashMap, HashSet};

use query_data_model::constants::*;
use query_data_model::implementations::relational::plan::Relation;

use super::{EdgeRelation, NodePlan, Pattern};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection, membership};
use crate::passes::plan::rows::{BindingSource, Reads, Rel};
use query_data_model::implementations::relational::Target;

type IdSet = (Rel, String);

/// The pattern's joined rows and what the result reads from them.
pub struct Matched {
    pub rows: Relation,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
    pub condition: Vec<Predicate>,
}

impl Matched {
    pub fn property(&self, node: &str, property: &str) -> Column {
        self.bindings
            .iter()
            .find(|binding| binding.node == node)
            .and_then(|binding| binding.column(property))
            .unwrap_or_else(|| Column::new(node, property))
    }
}

pub(super) struct RelationBuilder<'a, 'q, 'c, B: SqlBackend> {
    pattern: &'a Pattern<'a, B>,
    reads: Reads<'q, 'c, B>,
    root: Option<Relation>,
    filters: Vec<Predicate>,
    condition: Vec<Predicate>,
    outputs: Vec<Projection>,
    nodes: HashMap<String, PlannedNode>,
    tagged: HashSet<(String, Target, usize)>,
}

#[derive(Default)]
struct PlannedNode {
    binding: Option<BindingSource>,
    ids: Option<IdSet>,
    bounded_read: Option<Rel>,
    read: Option<Rel>,
}

impl<'a, 'q, 'c, B> RelationBuilder<'a, 'q, 'c, B>
where
    B: SqlBackend,
{
    pub fn new(pattern: &'a Pattern<'a, B>, reads: Reads<'q, 'c, B>) -> Self {
        Self {
            pattern,
            reads,
            root: None,
            filters: Vec::new(),
            condition: Vec::new(),
            outputs: Vec::new(),
            nodes: HashMap::new(),
            tagged: HashSet::new(),
        }
    }

    /// The joined rows with the pattern's filters and the `conditions` bound to the result bindings.
    pub fn build(
        mut self,
        conditions: impl FnOnce(&[BindingSource]) -> Result<Vec<Predicate>>,
    ) -> Result<Matched> {
        let pattern = self.pattern;
        let bindings = if pattern.relations.is_empty() {
            self.single_node()?
        } else {
            for (index, relation) in pattern.relations.iter().enumerate() {
                match &relation.fk {
                    Some(key) => self.add_fk(relation, key)?,
                    None => self.add_edge(index, relation)?,
                }
            }
            let joined: Vec<&str> = pattern
                .query
                .bindings()
                .into_iter()
                .map(|binding| binding.name)
                .filter(|alias| self.nodes.contains_key(*alias) && pattern.nodes[*alias].joined)
                .collect();
            for alias in joined {
                self.ensure_node(alias)?;
            }
            pattern
                .query
                .bindings()
                .into_iter()
                .filter_map(|binding| self.nodes.get(binding.name)?.binding.clone())
                .collect()
        };
        let root = self
            .root
            .take()
            .ok_or_else(|| QueryError::Lowering("traversal has no physical source".into()))?;
        self.filters.extend(conditions(&bindings)?);
        let rows = self.reads.filter(root, &self.filters)?;
        Ok(Matched {
            rows,
            outputs: self.outputs,
            bindings,
            condition: self.condition,
        })
    }

    fn single_node(&mut self) -> Result<Vec<BindingSource>> {
        let node = self
            .pattern
            .nodes
            .values()
            .next()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let read = self.pattern.node_read(&mut self.reads, node)?;
        self.root = Some(self.reads.read(&read, None)?);
        self.outputs = node.outputs();
        Ok(vec![BindingSource::table(&node.alias, &node.id_column)])
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
            identity: self.pattern.nodes[node].id_column.clone(),
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
            &node.id_column,
            Some(&node.alias),
            traversal_path,
        );
    }

    fn join_root(
        &mut self,
        scan: &Rel,
        alias: &str,
        on: impl FnOnce(&Self) -> Result<(Column, Column)>,
    ) -> Result<()> {
        let Some(root) = self.root.take() else {
            self.root = Some(self.reads.read(scan, None)?);
            return Ok(());
        };
        let on = on(self)?;
        let right = self.reads.read(scan, Some(alias))?;
        self.root = Some(self.reads.join(root, right, on, &[])?);
        Ok(())
    }

    fn ensure_node(&mut self, alias: &str) -> Result<()> {
        if self.has_table(alias) {
            return Ok(());
        }
        let mut node = self.pattern.uncovered_node(alias);
        if let Some(ids) = self.reached_ids(alias) {
            node.predicates
                .push(membership(Column::new(alias, &node.id_column), ids));
        }
        let id_column = node.id_column.clone();
        self.attach_table(&node, |builder| {
            let binding = builder.binding(alias).ok_or_else(|| {
                QueryError::Lowering(format!(
                    "node '{alias}' is not connected to the planned relations"
                ))
            })?;
            Ok((
                Column::new(alias, &id_column),
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
        self.join_root(&scan, &node.alias, on)?;
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
            EDGE_KIND_SUFFIX,
            EDGE_SRC_SUFFIX,
            EDGE_SRC_KIND_SUFFIX,
            EDGE_DST_SUFFIX,
            EDGE_DST_KIND_SUFFIX,
        ];
        self.outputs.extend(
            values
                .into_iter()
                .zip(suffixes)
                .map(|(value, suffix)| Projection::new(value, format!("{prefix}{suffix}"))),
        );
    }

    fn node_read(&mut self, node: &NodePlan) -> Result<Rel> {
        let read = self.pattern.node_read(&mut self.reads, node)?;
        let span = self.reads.id_range_span;
        let selective = self
            .pattern
            .node_conditions(node)
            .iter()
            .any(|predicate| predicate.is_selective(span));
        let state = self.nodes.entry(node.alias.clone()).or_default();
        state.read = Some(read.clone());
        if selective {
            state.bounded_read = Some(read.clone());
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
        let id = Projection::new(
            OutputValue::Column(Column::new(&node.alias, &node.id_column)),
            &node.id_column,
        );
        let ids = (
            self.reads.project(&source, None, &[id])?,
            node.id_column.clone(),
        );
        self.set_ids(&node.alias, ids.clone());
        Ok(ids)
    }

    /// Keeps the ids an edge read produces for an endpoint when a later step can narrow by them.
    fn bind_relation_ids(
        &mut self,
        index: usize,
        relation: &EdgeRelation,
        source: &Rel,
    ) -> Result<()> {
        let pattern = self.pattern;
        let (start, end) = relation.id_columns()?;
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
                    .any(|next| next.endpoint_field(node).is_some())
                || !stored::<B>()
                    && pattern.nodes[node].joined
                    && (pattern.endpoint_is_selective(other) || self.ids(other).is_some());
            if used_later {
                let ids = self.ids_reached_by(source, column)?;
                self.set_ids(node, ids);
            }
        }
        Ok(())
    }

    fn endpoint_ids(&mut self, node: &str) -> Result<Option<IdSet>> {
        if let Some(ids) = self.ids(node) {
            return Ok(Some(ids.clone()));
        }
        let id_column = self.pattern.nodes[node].id_column.clone();
        let Some(ids) = self.ids_through_holder(node, &id_column)? else {
            return Ok(None);
        };
        self.set_ids(node, ids.clone());
        Ok(Some(ids))
    }

    fn ids_through_holder(&mut self, holder: &str, column: &str) -> Result<Option<IdSet>> {
        if stored::<B>() {
            return Ok(None);
        }
        let Some(read) = self
            .nodes
            .get(holder)
            .and_then(|state| state.bounded_read.clone())
        else {
            return Ok(None);
        };
        self.ids_reached_by(&read, column).map(Some)
    }

    fn ids_reached_by(&mut self, source: &Rel, column: &str) -> Result<IdSet> {
        let id = Projection::new(
            OutputValue::Column(Column::new(id_source_alias(), column)),
            id_set_column(),
        );
        let ids = self.reads.project(source, Some(id_source_alias()), &[id])?;
        Ok((ids, id_set_column().to_string()))
    }
}
