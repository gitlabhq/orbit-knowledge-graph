use std::collections::HashMap;

use super::requirements::{Column, Predicate, Projection};
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub struct ExecutionPlan {
    pub graph: RelationGraph,
    pub root: RelationId,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

impl ExecutionPlan {
    pub fn property(&self, node: &str, property: &str) -> Column {
        self.bindings
            .iter()
            .find(|binding| binding.node == node)
            .and_then(|binding| binding.column(property))
            .unwrap_or_else(|| Column::new(node, property))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationId(pub usize);

impl RelationId {
    pub fn alias(self, alias: impl Into<String>) -> RelationRef {
        RelationRef::from(self).alias(alias)
    }
}

#[derive(Clone)]
pub struct RelationRef {
    pub id: RelationId,
    pub alias: Option<String>,
}

impl RelationRef {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
}

impl From<RelationId> for RelationRef {
    fn from(id: RelationId) -> Self {
        Self { id, alias: None }
    }
}

pub struct Relation<'a> {
    graph: &'a mut RelationGraph,
    id: RelationId,
}

impl Relation<'_> {
    pub fn alias(self, alias: impl Into<String>) -> RelationRef {
        RelationRef::from(self.id).alias(alias)
    }

    pub fn filter(self, predicates: Vec<Predicate>) -> Self {
        let id = self.graph.filter(self.id, predicates);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn join(
        self,
        right: impl Into<RelationRef>,
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
    ) -> Self {
        let id = self.graph.join(self.id, right, endpoints, predicates);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn project(self, outputs: Vec<Projection>) -> Self {
        let id = self.graph.project(self.id, outputs);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn distinct(self) -> Self {
        let id = self.graph.distinct(self.id);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn semi_join(self, column: Column, keys: RelationId, key: impl Into<String>) -> Self {
        let id = self.graph.semi_join(self.id, column, keys, key);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn bind(self, alias: impl Into<String>) -> Self {
        let id = self.graph.bind(self.id, alias);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn latest(
        self,
        alias: impl Into<String>,
        sort_key: Vec<String>,
        condition: Vec<Predicate>,
    ) -> Self {
        let id = self.graph.latest(self.id, alias, sort_key, condition);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn sort(self, keys: Vec<(Column, bool)>) -> Self {
        let id = self.graph.sort(self.id, keys);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn limit(self, rows: u32) -> Self {
        let id = self.graph.limit(self.id, rows);
        Self {
            graph: self.graph,
            id,
        }
    }

    pub fn finish(self) -> RelationId {
        self.id
    }
}

#[derive(Default)]
pub struct RelationGraph {
    nodes: Vec<RelationNode>,
    reads: HashMap<ReadKey, RelationId>,
}

#[derive(Hash, PartialEq, Eq)]
struct ReadKey {
    table: String,
    occurrence: Option<String>,
    final_: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ReadOwner {
    Node(String),
    Relationship(usize),
}

impl RelationGraph {
    pub fn from(&mut self, id: RelationId) -> Relation<'_> {
        Relation { graph: self, id }
    }

    pub fn add(&mut self, node: RelationNode) -> RelationId {
        let id = RelationId(self.nodes.len());
        self.nodes.push(node);
        id
    }

    pub fn get(&self, id: RelationId) -> &RelationNode {
        &self.nodes[id.0]
    }

    pub fn iter(&self) -> impl Iterator<Item = (RelationId, &RelationNode)> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (RelationId(index), node))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (RelationId, &mut RelationNode)> {
        self.nodes
            .iter_mut()
            .enumerate()
            .map(|(index, node)| (RelationId(index), node))
    }

    pub fn usage(&self, root: RelationId) -> RelationUsage {
        let mut usage = RelationUsage::default();
        usage.visit(root, self);
        usage
    }

    pub fn filter_uses(
        &mut self,
        root: &mut RelationId,
        target: RelationId,
        predicates: Vec<Predicate>,
    ) {
        let replacement = self.filter(target, predicates);
        let existing = replacement.0;
        for node in &mut self.nodes[..existing] {
            node.replace_input(target, replacement);
        }
        if *root == target {
            *root = replacement;
        }
    }

    pub(crate) fn read(
        &mut self,
        table: impl Into<String>,
        alias: impl Into<String>,
        final_: bool,
        _relationship: Option<usize>,
    ) -> RelationId {
        self.read_with_occurrence(table, alias, final_, true)
    }

    pub(crate) fn shared_read(
        &mut self,
        table: impl Into<String>,
        alias: impl Into<String>,
        final_: bool,
    ) -> RelationId {
        self.read_with_occurrence(table, alias, final_, false)
    }

    fn read_with_occurrence(
        &mut self,
        table: impl Into<String>,
        alias: impl Into<String>,
        final_: bool,
        occurrence_is_identity: bool,
    ) -> RelationId {
        let table = table.into();
        let alias = alias.into();
        let key = ReadKey {
            table: table.clone(),
            occurrence: occurrence_is_identity.then(|| alias.clone()),
            final_,
        };
        if let Some(id) = self.reads.get(&key) {
            return *id;
        }
        let id = self.add(RelationNode::Read {
            table,
            alias,
            final_,
        });
        self.reads.insert(key, id);
        id
    }

    pub(crate) fn owned(
        &mut self,
        input: RelationId,
        owner: ReadOwner,
        alias: impl Into<String>,
    ) -> RelationId {
        self.add(RelationNode::Owned {
            input: input.into(),
            owner,
            alias: alias.into(),
        })
    }

    pub(crate) fn filter(
        &mut self,
        input: impl Into<RelationRef>,
        predicates: Vec<Predicate>,
    ) -> RelationId {
        let input = input.into();
        if predicates.is_empty() {
            return input.id;
        }
        self.add(RelationNode::Filter { input, predicates })
    }

    pub(crate) fn project(
        &mut self,
        input: impl Into<RelationRef>,
        outputs: Vec<Projection>,
    ) -> RelationId {
        self.add(RelationNode::Project {
            input: input.into(),
            outputs,
        })
    }

    pub(crate) fn distinct(&mut self, input: impl Into<RelationRef>) -> RelationId {
        self.add(RelationNode::Distinct {
            input: input.into(),
        })
    }

    pub fn union_all(&mut self, inputs: Vec<RelationRef>, alias: impl Into<String>) -> RelationId {
        self.add(RelationNode::UnionAll {
            inputs,
            alias: alias.into(),
        })
    }

    pub(crate) fn join(
        &mut self,
        left: impl Into<RelationRef>,
        right: impl Into<RelationRef>,
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
    ) -> RelationId {
        self.add(RelationNode::Join {
            left: left.into(),
            right: right.into(),
            endpoints,
            predicates,
        })
    }

    pub(crate) fn semi_join(
        &mut self,
        input: impl Into<RelationRef>,
        column: Column,
        keys: RelationId,
        key: impl Into<String>,
    ) -> RelationId {
        self.filter(
            input,
            vec![Predicate::Membership {
                column,
                relation: keys,
                key: key.into(),
            }],
        )
    }

    pub(crate) fn bind(
        &mut self,
        input: impl Into<RelationRef>,
        alias: impl Into<String>,
    ) -> RelationId {
        self.add(RelationNode::Bind {
            input: input.into(),
            alias: alias.into(),
        })
    }

    pub(crate) fn latest(
        &mut self,
        input: impl Into<RelationRef>,
        alias: impl Into<String>,
        sort_key: Vec<String>,
        condition: Vec<Predicate>,
    ) -> RelationId {
        self.add(RelationNode::Latest {
            input: input.into(),
            alias: alias.into(),
            sort_key,
            condition,
        })
    }

    fn sort(&mut self, input: impl Into<RelationRef>, keys: Vec<(Column, bool)>) -> RelationId {
        self.add(RelationNode::Sort {
            input: input.into(),
            keys,
        })
    }

    fn limit(&mut self, input: impl Into<RelationRef>, rows: u32) -> RelationId {
        self.add(RelationNode::Limit {
            input: input.into(),
            rows,
        })
    }
}

#[derive(Default)]
pub struct RelationUsage {
    references: HashMap<RelationId, usize>,
    materialized: Vec<RelationId>,
}

impl RelationUsage {
    pub fn references(&self, id: RelationId) -> usize {
        self.references.get(&id).copied().unwrap_or_default()
    }

    pub fn materialized(&self) -> impl Iterator<Item = RelationId> + '_ {
        self.materialized.iter().copied()
    }

    pub fn is_materialized(&self, id: RelationId) -> bool {
        self.materialized.contains(&id)
    }

    fn visit(&mut self, id: RelationId, graph: &RelationGraph) {
        *self.references.entry(id).or_default() += 1;
        if self.references[&id] > 1 {
            self.materialize(id);
            return;
        }
        match graph.get(id) {
            RelationNode::Read { .. } => {}
            RelationNode::Owned { input, .. }
            | RelationNode::Project { input, .. }
            | RelationNode::Distinct { input, .. }
            | RelationNode::Bind { input, .. }
            | RelationNode::Sort { input, .. }
            | RelationNode::Limit { input, .. } => self.visit(input.id, graph),
            RelationNode::Filter { input, predicates } => {
                self.visit(input.id, graph);
                self.visit_predicates(predicates, graph);
            }
            RelationNode::Latest {
                input, condition, ..
            } => {
                self.visit(input.id, graph);
                self.visit_predicates(condition, graph);
            }
            RelationNode::UnionAll { inputs, .. } => {
                for input in inputs {
                    self.visit(input.id, graph);
                }
            }
            RelationNode::Join {
                left,
                right,
                predicates,
                ..
            } => {
                self.visit(left.id, graph);
                self.visit(right.id, graph);
                self.visit_predicates(predicates, graph);
            }
        }
    }

    fn visit_predicates(&mut self, predicates: &[Predicate], graph: &RelationGraph) {
        for predicate in predicates {
            match predicate {
                Predicate::Boolean(expression) => {
                    for predicate in expression.leaves() {
                        self.visit_predicates(std::slice::from_ref(predicate), graph);
                    }
                }
                Predicate::Membership { relation, .. } => {
                    self.materialize(*relation);
                    self.visit(*relation, graph);
                }
                _ => {}
            }
        }
    }

    fn materialize(&mut self, id: RelationId) {
        if !self.materialized.contains(&id) {
            self.materialized.push(id);
            self.materialized.sort_by_key(|id| id.0);
        }
    }
}

#[derive(Clone)]
pub enum RelationNode {
    Read {
        table: String,
        alias: String,
        final_: bool,
    },
    Owned {
        input: RelationRef,
        owner: ReadOwner,
        alias: String,
    },
    Filter {
        input: RelationRef,
        predicates: Vec<Predicate>,
    },
    Project {
        input: RelationRef,
        outputs: Vec<Projection>,
    },
    Distinct {
        input: RelationRef,
    },
    UnionAll {
        inputs: Vec<RelationRef>,
        alias: String,
    },
    Join {
        left: RelationRef,
        right: RelationRef,
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
    },
    Bind {
        input: RelationRef,
        alias: String,
    },
    Latest {
        input: RelationRef,
        alias: String,
        sort_key: Vec<String>,
        condition: Vec<Predicate>,
    },
    Sort {
        input: RelationRef,
        keys: Vec<(Column, bool)>,
    },
    Limit {
        input: RelationRef,
        rows: u32,
    },
}

impl RelationNode {
    fn replace_input(&mut self, target: RelationId, replacement: RelationId) {
        let replace = |reference: &mut RelationRef| {
            if reference.id == target {
                reference.id = replacement;
            }
        };
        match self {
            Self::Read { .. } => {}
            Self::Owned { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Distinct { input, .. }
            | Self::Bind { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. } => replace(input),
            Self::UnionAll { inputs, .. } => {
                for input in inputs {
                    replace(input);
                }
            }
            Self::Join { left, right, .. } => {
                replace(left);
                replace(right);
            }
        }
    }
}

#[derive(Clone)]
pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub property_source: Option<String>,
    pub traversal_path: Option<Column>,
}

impl BindingSource {
    pub(super) fn column(&self, property: &str) -> Option<Column> {
        if property == ontology::constants::TRAVERSAL_PATH_COLUMN {
            return self.traversal_path.clone();
        }
        if property == DEFAULT_PRIMARY_KEY {
            Some(Column::new(&self.alias, &self.column))
        } else {
            self.property_source
                .as_ref()
                .map(|source| Column::new(source, property))
        }
    }

    pub(super) fn table(alias: &str) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            column: DEFAULT_PRIMARY_KEY.into(),
            property_source: Some(alias.into()),
            traversal_path: Some(Column::new(
                alias,
                ontology::constants::TRAVERSAL_PATH_COLUMN,
            )),
        }
    }
}
