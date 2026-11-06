//! Read access to a logical plan for the compiler phases: bindings, steps,
//! conjuncts, and the result operations, all by declaration id.

mod conditions;

pub use conditions::{Leaf, Literals};

use query_data_model::plan::logical::{
    Expr, Hops, LogicalOperation, LogicalPlan, LogicalType, Measure, Predicate, Projection,
};
use query_data_model::plan::tracking::{NodeId, Ref};
use query_data_model::{Direction, EntityId, Hop, Model, RelationshipId};

use crate::input::QueryType;

#[derive(Clone, Copy)]
pub struct Query<'p> {
    pub plan: &'p LogicalPlan,
    pub model: &'p Model,
}

#[derive(Debug, Clone, Copy)]
pub struct Binding<'p> {
    pub output: Ref,
    pub entity: EntityId,
    pub name: &'p str,
}

#[derive(Debug, Clone, Copy)]
pub struct Step<'p> {
    pub node: NodeId,
    pub from: Ref,
    pub edge: Ref,
    pub to: Ref,
    pub relationships: &'p [RelationshipId],
    pub direction: Direction,
    pub range: Hops,
    pub hops: &'p [Hop],
}

impl<'p> Query<'p> {
    pub fn new(plan: &'p LogicalPlan, model: &'p Model) -> Self {
        Self { plan, model }
    }

    pub fn operations(self) -> impl Iterator<Item = (NodeId, &'p LogicalOperation)> {
        self.plan
            .nodes()
            .map(move |node| (node, self.plan.operation(node)))
    }

    pub fn kind(self) -> QueryType {
        let mut kind = QueryType::Traversal;
        for (_, operation) in self.operations() {
            kind = match operation {
                LogicalOperation::Neighbors { .. } => QueryType::Neighbors,
                LogicalOperation::Shortest { .. } => QueryType::PathFinding,
                LogicalOperation::Aggregate { .. } => QueryType::Aggregation,
                _ => continue,
            };
        }
        kind
    }

    pub fn name(self, output: Ref) -> &'p str {
        self.plan.name(output).unwrap_or_default()
    }

    pub fn entity(self, output: Ref) -> Option<EntityId> {
        match self.plan.value(output) {
            LogicalType::Entity(entity) => Some(*entity),
            _ => None,
        }
    }

    pub fn entity_name(self, output: Ref) -> Option<&'p str> {
        self.entity(output)
            .map(|entity| self.model.graph().entity(entity).name.as_str())
    }

    pub fn bindings(self) -> Vec<Binding<'p>> {
        let declared: Vec<Ref> = self.plan.declaration_order().collect();
        let mut bindings: Vec<Binding<'p>> = self
            .operations()
            .filter(|(_, operation)| {
                matches!(
                    operation,
                    LogicalOperation::Entities(_) | LogicalOperation::Follow { .. }
                )
            })
            .filter_map(|(node, _)| {
                let output = self.plan.outputs(node).last()?;
                Some(Binding {
                    output,
                    entity: self.entity(output)?,
                    name: self.name(output),
                })
            })
            .collect();
        bindings.sort_by_key(|binding| {
            declared
                .iter()
                .position(|output| *output == binding.output)
                .unwrap_or(declared.len())
        });
        bindings
    }

    pub fn binding(self, name: &str) -> Option<Binding<'p>> {
        self.bindings()
            .into_iter()
            .find(|binding| binding.name == name)
    }

    pub fn steps(self) -> Vec<Step<'p>> {
        self.operations()
            .filter_map(|(node, operation)| match operation {
                LogicalOperation::Follow {
                    from,
                    relationships,
                    direction,
                    range,
                    hops,
                } => {
                    let outputs: Vec<Ref> = self.plan.outputs(node).collect();
                    let [.., edge, to] = outputs[..] else {
                        return None;
                    };
                    Some(Step {
                        node,
                        from: *from,
                        edge,
                        to,
                        relationships,
                        direction: *direction,
                        range: *range,
                        hops,
                    })
                }
                _ => None,
            })
            .collect()
    }

    pub fn conjuncts(self) -> Vec<&'p Predicate> {
        fn flatten<'a>(predicate: &'a Predicate, into: &mut Vec<&'a Predicate>) {
            match predicate {
                Predicate::And(all) => all.iter().for_each(|inner| flatten(inner, into)),
                leaf => into.push(leaf),
            }
        }
        let mut conjuncts = Vec::new();
        for (_, operation) in self.operations() {
            if let LogicalOperation::Filter(predicate) = operation {
                flatten(predicate, &mut conjuncts);
            }
        }
        conjuncts
    }

    pub fn selected(self) -> Vec<&'p Projection> {
        self.operations()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Select(items) => Some(items.iter()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    pub fn ordered(self) -> Vec<Expr> {
        self.operations()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Order(keys) => Some(keys.iter().map(|(key, _)| *key)),
                _ => None,
            })
            .flatten()
            .collect()
    }

    pub fn aggregate(self) -> Option<(&'p [Expr], &'p [Measure])> {
        self.operations()
            .find_map(|(_, operation)| match operation {
                LogicalOperation::Aggregate { groups, measures } => {
                    Some((&groups[..], &measures[..]))
                }
                _ => None,
            })
    }
}
