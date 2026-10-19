use std::collections::HashMap;

use super::graph::{RelationGraph, RelationId, RelationNode};
use super::predicates::Predicate;

#[derive(Default)]
pub struct RelationUsage {
    references: HashMap<RelationId, usize>,
    materialized: Vec<RelationId>,
}

impl RelationUsage {
    pub(super) fn of(root: RelationId, graph: &RelationGraph) -> Self {
        let mut usage = Self::default();
        usage.visit(root, graph);
        usage
    }

    pub fn references(&self, id: RelationId) -> usize {
        self.references.get(&id).copied().unwrap_or_default()
    }

    pub fn materialized(&self) -> impl Iterator<Item = RelationId> + '_ {
        self.materialized.iter().copied()
    }

    fn visit(&mut self, id: RelationId, graph: &RelationGraph) {
        *self.references.entry(id).or_default() += 1;
        if self.references[&id] > 1 {
            if Self::reusable(id, graph) {
                self.materialize(id);
            }
            return;
        }
        match graph.get(id) {
            RelationNode::Read { .. } => {}
            RelationNode::Owned { input, .. }
            | RelationNode::Project { input, .. }
            | RelationNode::Bind { input, .. } => self.visit(input.id, graph),
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

    fn reusable(id: RelationId, graph: &RelationGraph) -> bool {
        match graph.get(id) {
            RelationNode::Read { .. } | RelationNode::Owned { .. } => false,
            RelationNode::Filter { input, predicates } => {
                let selective: Vec<_> = predicates.iter().filter(|p| p.is_selective()).collect();
                if !selective.is_empty() && selective.iter().all(|p| p.is_point_lookup()) {
                    return false;
                }
                !selective.is_empty() || Self::reusable(input.id, graph)
            }
            RelationNode::Project { input, .. } | RelationNode::Bind { input, .. } => {
                Self::reusable(input.id, graph)
            }
            RelationNode::UnionAll { .. }
            | RelationNode::Join { .. }
            | RelationNode::Latest { .. } => true,
        }
    }

    fn materialize(&mut self, id: RelationId) {
        if !self.materialized.contains(&id) {
            self.materialized.push(id);
            // Kept in relation order because the lowerer emits CTEs in this order.
            self.materialized.sort_by_key(|id| id.0);
        }
    }
}
