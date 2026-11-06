use std::collections::{HashMap, HashSet};

use ontology::constants::VERSION_COLUMN;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, JoinKind, Order, Relation, RelationalBuilder,
};

use super::NodeBinding;
use super::build::predicates::{Lowering, Shared};
use super::build::{Built, and, column, computed, eq, filtered, scan, stored};
use crate::constants::internal_column_prefix;
use crate::passes::plan::graph::{
    ExecutionPlan, ReadOwner, RelationGraph, RelationId, RelationNode, RelationRef,
};
use crate::passes::plan::predicates::{Column, Predicate, Projection};
use crate::tp_bounds::{TpBound, TpBounds};

pub(super) struct Reads<'a> {
    graph: &'a RelationGraph,
    materialized: HashSet<RelationId>,
    tp_bounds: Option<&'a TpBounds>,
    pub(super) shared: Shared,
}

pub(super) enum Pending<'a> {
    Predicate(&'a Predicate),
    Bound(&'a TpBound, String),
}

pub(super) struct Source<'a> {
    pub(super) rows: Relation,
    pub(super) predicates: Vec<Pending<'a>>,
}

pub(super) struct Executed<'a> {
    pub(super) source: Source<'a>,
    pub(super) reads: Reads<'a>,
}

#[derive(PartialEq)]
enum Shape {
    Shared(String),
    Scan(String),
    Other,
}

pub(super) fn execute<'a, B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &'a ExecutionPlan,
    root_columns: &[Column],
    tp_bounds: Option<&'a TpBounds>,
) -> Built<Executed<'a>> {
    let usage = plan.graph.usage(plan.root);
    let mut reads = Reads {
        graph: &plan.graph,
        materialized: usage.materialized().collect(),
        tp_bounds,
        shared: Shared::default(),
    };
    let required = super::columns::required_columns(plan, &usage, root_columns);
    for id in usage.materialized() {
        let columns = match (required.get(&id), exposed_alias(id, &plan.graph)) {
            (Some(columns), Some(alias)) if !columns.is_empty() => Some((alias, columns)),
            _ => None,
        };
        let materialized = usage.references(id) > 1 && !stored::<B>();
        let body = q.cte(&relation_name(id), materialized, |q| {
            let source = reads.node(q, id, Some(id))?;
            let items = match columns {
                Some((alias, columns)) => Items::Columns(
                    columns
                        .iter()
                        .filter(|column| !stored::<B>() || !super::build::versioning(column))
                        .map(|column| (alias.clone(), column.clone()))
                        .collect(),
                ),
                None => Items::Star,
            };
            reads.query(q, source, items)
        })?;
        reads.shared.insert(id, body);
    }
    let source = reads.reference(q, &RelationRef::from(plan.root), None)?;
    Ok(Executed { source, reads })
}

pub(super) fn bindings(plan: &ExecutionPlan) -> HashMap<String, NodeBinding> {
    plan.bindings
        .iter()
        .map(|binding| {
            let source = NodeBinding::source(
                &binding.alias,
                &binding.column,
                binding.property_source.clone(),
                binding.traversal_path.clone(),
            );
            (binding.node.clone(), source)
        })
        .collect()
}

pub(crate) fn relation_name(id: RelationId) -> String {
    format!("{}relation_{}", internal_column_prefix(), id.0)
}

/// A materialized reference must expose the alias its inline form would, or predicates and join conditions dangle.
pub(super) fn exposed_alias(id: RelationId, graph: &RelationGraph) -> Option<String> {
    match graph.get(id) {
        RelationNode::Read { alias, .. }
        | RelationNode::Owned { alias, .. }
        | RelationNode::UnionAll { alias, .. }
        | RelationNode::Bind { alias, .. }
        | RelationNode::Latest { alias, .. } => Some(alias.clone()),
        RelationNode::Filter { input, .. } => input
            .alias
            .clone()
            .or_else(|| exposed_alias(input.id, graph)),
        RelationNode::Project { input, .. } => {
            Some(input.alias.clone().unwrap_or_else(|| relation_name(id)))
        }
        RelationNode::Join { .. } => None,
    }
}

impl<'a> Reads<'a> {
    fn shared(&self, relation: &RelationRef, current: Option<RelationId>) -> Option<String> {
        (self.materialized.contains(&relation.id) && current != Some(relation.id)).then(|| {
            relation
                .alias
                .clone()
                .or_else(|| exposed_alias(relation.id, self.graph))
                .unwrap_or_else(|| relation_name(relation.id))
        })
    }

    fn inner(&self, relation: &RelationRef, current: Option<RelationId>) -> Shape {
        if let Some(alias) = self.shared(relation, current) {
            return Shape::Shared(alias);
        }
        match self.graph.get(relation.id) {
            RelationNode::Read {
                alias,
                final_: false,
                ..
            } => Shape::Scan(alias.clone()),
            RelationNode::Owned { input, alias, .. } => {
                let mut reference = input.clone();
                reference.alias = Some(alias.clone());
                self.shape(&reference, current)
            }
            RelationNode::Filter { input, .. } => self.shape(input, current),
            _ => Shape::Other,
        }
    }

    fn wraps(&self, relation: &RelationRef, current: Option<RelationId>) -> bool {
        let inner = self.inner(relation, current);
        matches!(&relation.alias, Some(alias)
            if inner != Shape::Shared(alias.clone()) && inner != Shape::Scan(alias.clone()))
    }

    fn shape(&self, relation: &RelationRef, current: Option<RelationId>) -> Shape {
        match self.wraps(relation, current) {
            true => Shape::Other,
            false => self.inner(relation, current),
        }
    }

    fn reference<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        relation: &RelationRef,
        current: Option<RelationId>,
    ) -> Built<Source<'a>> {
        let inline = |this: &mut Self, q: &mut RelationalBuilder<'_, B>| match this
            .shared(relation, current)
        {
            Some(alias) => {
                let body = this.shared.get(relation.id)?.clone();
                let rows = q.read(&body, Some(&alias))?;
                Ok(Source {
                    rows,
                    predicates: Vec::new(),
                })
            }
            None => this.node(q, relation.id, current),
        };
        match (&relation.alias, self.wraps(relation, current)) {
            (Some(alias), true) => self.wrapped(q, alias, Items::Star, inline),
            _ => inline(self, q),
        }
    }
}

impl<'a> Reads<'a> {
    fn node<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        id: RelationId,
        current: Option<RelationId>,
    ) -> Built<Source<'a>> {
        let graph = self.graph;
        Ok(match graph.get(id) {
            RelationNode::Read {
                table,
                alias,
                final_,
            } => Source {
                rows: scan(q, table, alias, *final_)?,
                predicates: Vec::new(),
            },
            RelationNode::Owned {
                input,
                owner,
                alias,
            } => {
                let mut reference = input.clone();
                reference.alias = Some(alias.clone());
                let mut source = self.reference(q, &reference, current)?;
                let bound = self.tp_bounds.and_then(|bounds| match owner {
                    ReadOwner::Node(node) => bounds.node(node),
                    ReadOwner::Relationship(index) => bounds.relationship(*index),
                });
                if let Some(bound) = bound {
                    source
                        .predicates
                        .insert(0, Pending::Bound(bound, alias.clone()));
                }
                source
            }
            RelationNode::Filter { input, predicates } => {
                let mut source = self.reference(q, input, current)?;
                source
                    .predicates
                    .extend(predicates.iter().map(Pending::Predicate));
                source
            }
            RelationNode::Project { input, outputs } => {
                let alias = input.alias.clone().unwrap_or_else(|| relation_name(id));
                let items = Items::Projections(outputs);
                self.wrapped(q, &alias, items, |this, q| {
                    this.reference(q, input, current)
                })?
            }
            RelationNode::UnionAll { inputs, alias } => {
                let body = q.subquery(alias, |q| {
                    let mut arms = Vec::new();
                    for input in inputs {
                        let source = self.reference(q, input, current)?;
                        arms.push(self.query(q, source, Items::Star)?);
                    }
                    q.union(arms)
                })?;
                Source {
                    rows: q.read(&body, Some(alias))?,
                    predicates: Vec::new(),
                }
            }
            RelationNode::Join {
                left,
                right,
                endpoints,
                predicates,
            } => {
                let mut left = self.reference(q, left, current)?;
                let right = self.reference(q, right, current)?;
                let lowering = Lowering {
                    scope: &[&left.rows, &right.rows],
                    shared: &self.shared,
                };
                let mut condition = eq(
                    column(lowering.scope, &endpoints.0)?,
                    column(lowering.scope, &endpoints.1)?,
                );
                for predicate in predicates {
                    condition = and(condition, lowering.predicate(q, predicate)?);
                }
                let rows = q.join(left.rows, right.rows, JoinKind::Inner(condition))?;
                left.predicates.extend(right.predicates);
                Source {
                    rows,
                    predicates: left.predicates,
                }
            }
            RelationNode::Bind { input, alias } => {
                self.wrapped(q, alias, Items::Star, |this, q| {
                    this.reference(q, input, current)
                })?
            }
            RelationNode::Latest {
                input,
                alias,
                sort_key,
                condition,
            } => {
                let body = q.subquery(alias, |q| {
                    let source = self.reference(q, input, current)?;
                    let rows = self.filter(q, source)?;
                    let keys = sort_key
                        .iter()
                        .map(|key| rows.qualified(alias, key))
                        .collect::<Built<Vec<_>>>()?;
                    let rows = match stored::<B>() {
                        true => {
                            let orders = keys.into_iter().map(|key| Order {
                                value: key.expr(),
                                descending: false,
                            });
                            q.order(rows, orders.collect())?
                        }
                        false => {
                            let version = rows.qualified(alias, VERSION_COLUMN)?;
                            q.latest(rows, keys, version)?
                        }
                    };
                    q.select_all(rows)
                })?;
                Source {
                    rows: q.read(&body, Some(alias))?,
                    predicates: condition.iter().map(Pending::Predicate).collect(),
                }
            }
        })
    }

    fn wrapped<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        alias: &str,
        items: Items<'_>,
        build: impl FnOnce(&mut Self, &mut RelationalBuilder<'_, B>) -> Built<Source<'a>>,
    ) -> Built<Source<'a>> {
        let body = q.subquery(alias, |q| {
            let source = build(self, q)?;
            self.query(q, source, items)
        })?;
        Ok(Source {
            rows: q.read(&body, Some(alias))?,
            predicates: Vec::new(),
        })
    }

    pub(super) fn filter<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        source: Source<'_>,
    ) -> Built<Relation> {
        let predicates = self.resolve(q, &source)?;
        filtered(q, source.rows, predicates)
    }

    pub(super) fn resolve<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        source: &Source<'_>,
    ) -> Built<Vec<Option<Expr>>> {
        let lowering = Lowering {
            scope: &[&source.rows],
            shared: &self.shared,
        };
        let mut resolved = Vec::new();
        for pending in &source.predicates {
            resolved.push(match pending {
                Pending::Predicate(Predicate::Live { .. }) if stored::<B>() => None,
                Pending::Predicate(predicate) => Some(lowering.predicate(q, predicate)?),
                Pending::Bound(bound, alias) => {
                    Some(crate::tp_bounds::bound(q, lowering.scope, bound, alias)?)
                }
            });
        }
        Ok(resolved)
    }

    fn query<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        source: Source<'_>,
        items: Items<'_>,
    ) -> Built<Relation> {
        let rows = self.filter(q, source)?;
        let items = match items {
            Items::Star => return q.select_all(rows),
            Items::Columns(columns) => columns
                .into_iter()
                .map(|(alias, name)| {
                    Ok((name.clone(), super::build::col(&[&rows], &alias, &name)?))
                })
                .collect::<Built<Vec<_>>>()?,
            Items::Projections(outputs) => Lowering {
                scope: &[&rows],
                shared: &self.shared,
            }
            .projections::<B>(outputs)?,
        };
        let (rows, columns) = computed(q, rows, items)?;
        q.select(rows, columns)
    }
}

pub(super) enum Items<'p> {
    Star,
    Columns(Vec<(String, String)>),
    Projections(&'p [Projection]),
}
