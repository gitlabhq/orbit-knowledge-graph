use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};

use super::predicates::{Column, Predicate, Projection};
use super::reuse::RelationUsage;

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
        if property == TRAVERSAL_PATH_COLUMN {
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
            traversal_path: Some(Column::new(alias, TRAVERSAL_PATH_COLUMN)),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationId(pub usize);

impl RelationId {
    pub fn alias(self, alias: impl Into<String>) -> RelationRef {
        RelationRef {
            id: self,
            alias: Some(alias.into()),
        }
    }
}

#[derive(Clone)]
pub struct RelationRef {
    pub id: RelationId,
    pub alias: Option<String>,
}

impl From<RelationId> for RelationRef {
    fn from(id: RelationId) -> Self {
        Self { id, alias: None }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ReadOwner {
    Node(String),
    Relationship(usize),
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
            | Self::Bind { input, .. }
            | Self::Latest { input, .. } => replace(input),
            Self::UnionAll { inputs, .. } => inputs.iter_mut().for_each(replace),
            Self::Join { left, right, .. } => {
                replace(left);
                replace(right);
            }
        }
    }
}

#[derive(PartialEq)]
struct ReadKey {
    table: String,
    alias: String,
    owner: ReadOwner,
    key: Vec<String>,
    predicates: Vec<Predicate>,
    selective_latest: bool,
}

#[derive(Default)]
pub struct RelationGraph {
    nodes: Vec<RelationNode>,
    reads: Vec<(ReadKey, RelationId)>,
}

impl RelationGraph {
    fn add(&mut self, node: RelationNode) -> RelationId {
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

    pub fn usage(&self, root: RelationId) -> RelationUsage {
        RelationUsage::of(root, self)
    }

    /// Replaces `target` with the relation `transform` builds on top of it, in every earlier node and in `root`.
    pub fn transform(
        &mut self,
        root: &mut RelationId,
        target: RelationId,
        transform: impl FnOnce(&mut Self, RelationId) -> RelationId,
    ) {
        let replacement = transform(self, target);
        for node in &mut self.nodes[..replacement.0] {
            node.replace_input(target, replacement);
        }
        if *root == target {
            *root = replacement;
        }
    }

    pub fn read(
        &mut self,
        table: impl Into<String>,
        alias: impl Into<String>,
        final_: bool,
    ) -> RelationId {
        self.add(RelationNode::Read {
            table: table.into(),
            alias: alias.into(),
            final_,
        })
    }

    pub fn owned(
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

    /// The current row per sort key, planned once per distinct read so repeated reads share one relation.
    pub fn current_rows(
        &mut self,
        table: &str,
        alias: &str,
        owner: ReadOwner,
        key: &[String],
        predicates: Vec<Predicate>,
        selective_latest: bool,
    ) -> RelationId {
        let read = ReadKey {
            table: table.to_string(),
            alias: alias.to_string(),
            owner: owner.clone(),
            key: key.to_vec(),
            predicates: predicates.clone(),
            selective_latest,
        };
        if let Some((_, id)) = self.reads.iter().find(|(planned, _)| *planned == read) {
            return *id;
        }
        let id = self.plan_current_rows(table, alias, owner, key, predicates, selective_latest);
        self.reads.push((read, id));
        id
    }

    fn plan_current_rows(
        &mut self,
        table: &str,
        alias: &str,
        owner: ReadOwner,
        key: &[String],
        predicates: Vec<Predicate>,
        selective_latest: bool,
    ) -> RelationId {
        if !selective_latest || !predicates.iter().any(Predicate::is_selective) {
            let read = self.read(table, alias, true);
            let owned = self.owned(read, owner, alias);
            return self.filter(owned, predicates);
        }
        let (identity, rest): (Vec<_>, Vec<_>) = predicates
            .iter()
            .cloned()
            .partition(|predicate| predicate.filters_sort_key(key));
        let read = self.read(table, alias, false);
        let current = self.owned(read, owner.clone(), alias);
        let mut current = self.filter(current, identity);
        if rest.iter().any(Predicate::is_selective) {
            let candidate_read = self.read(table, alias, false);
            let candidate = self.owned(candidate_read, owner, alias);
            let candidate = self.filter(candidate, predicates);
            let candidate = self.project(
                candidate,
                key.iter()
                    .map(|column| Projection::col(alias, column))
                    .collect(),
            );
            current = self.filter(
                current,
                vec![Predicate::Membership {
                    columns: key
                        .iter()
                        .map(|column| Column::new(alias, column))
                        .collect(),
                    relation: candidate,
                    keys: key.to_vec(),
                }],
            );
        }
        let latest = self.latest(current, alias, key.to_vec(), vec![]);
        self.filter(latest, rest)
    }

    pub fn filter(
        &mut self,
        input: impl Into<RelationRef>,
        predicates: Vec<Predicate>,
    ) -> RelationId {
        let input = input.into();
        if predicates.is_empty() {
            return input.id;
        }
        if input.alias.is_none()
            && let RelationNode::Project {
                input: projected,
                outputs,
            } = self.get(input.id).clone()
        {
            let filtered = self.add(RelationNode::Filter {
                input: projected,
                predicates,
            });
            return self.project(filtered, outputs);
        }
        self.add(RelationNode::Filter { input, predicates })
    }

    pub fn project(
        &mut self,
        input: impl Into<RelationRef>,
        outputs: Vec<Projection>,
    ) -> RelationId {
        self.add(RelationNode::Project {
            input: input.into(),
            outputs,
        })
    }

    pub fn union_all(&mut self, inputs: Vec<RelationRef>, alias: impl Into<String>) -> RelationId {
        self.add(RelationNode::UnionAll {
            inputs,
            alias: alias.into(),
        })
    }

    pub fn join(
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

    pub fn bind(&mut self, input: impl Into<RelationRef>, alias: impl Into<String>) -> RelationId {
        self.add(RelationNode::Bind {
            input: input.into(),
            alias: alias.into(),
        })
    }

    pub fn latest(
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
}
