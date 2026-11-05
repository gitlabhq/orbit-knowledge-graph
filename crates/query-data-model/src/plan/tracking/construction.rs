use std::collections::HashSet;

use super::contract::{Item, Output, Outputs, Spec};
use super::plan::{Arena, Node, OutputInfo};
use super::rewrite::Pending;
use super::{Declared, Language, NodeId, OutputId, Plan, PlanId, Ref, Rejected, Rows, ScopeId};
use crate::catalog::ModelId;

pub struct Construction<L: Language> {
    pub(super) arena: Arena<L>,
    pub(super) scope: ScopeId,
    pub(super) failed: bool,
    pub(super) pending: Option<Pending<L>>,
}

#[derive(Debug)]
pub struct Connected {
    pub rows: Rows,
    pub declared: Vec<Ref>,
}

impl<L: Language> Construction<L> {
    pub fn new(snapshot: ModelId) -> Self {
        Self {
            arena: Arena {
                plan: PlanId::fresh(),
                snapshot,
                nodes: Vec::new(),
                outputs: Vec::new(),
                parents: vec![None],
                bodies: Vec::new(),
            },
            scope: ScopeId(0),
            failed: false,
            pending: None,
        }
    }

    pub fn snapshot(&self) -> ModelId {
        self.arena.snapshot
    }

    pub fn attempt<T, E: From<Rejected>>(
        &mut self,
        step: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        if self.failed {
            return Err(Rejected::Failed.into());
        }
        let result = step(self);
        self.failed |= result.is_err();
        result
    }

    pub fn declared<T: Copy>(&self, declared: Declared<T>) -> Result<T, Rejected> {
        if declared.snapshot() != self.arena.snapshot {
            return Err(Rejected::ForeignSnapshot);
        }
        Ok(declared.id)
    }

    pub fn operation(&self, rows: &Rows) -> Result<&L::Operation, Rejected> {
        self.node(rows)
            .map(|node| &self.arena.nodes[node.0].operation)
    }

    pub fn inputs(&self, rows: &Rows) -> Result<&[NodeId], Rejected> {
        self.node(rows)
            .map(|node| self.arena.nodes[node.0].inputs.as_slice())
    }

    pub fn outputs(&self, rows: &Rows) -> Result<Vec<Ref>, Rejected> {
        let node = self.node(rows)?;
        let plan = self.arena.plan;
        let contract = &self.arena.nodes[node.0].contract;
        Ok(contract
            .iter()
            .map(|output| Ref {
                plan,
                output: *output,
            })
            .collect())
    }

    pub fn value(&self, output: Ref) -> Result<(Option<&str>, &L::Value), Rejected> {
        if output.plan != self.arena.plan {
            return Err(Rejected::ForeignPlan);
        }
        let info = &self.arena.outputs[output.output.0];
        Ok((info.name.as_deref(), &info.value))
    }

    pub fn connect(
        &mut self,
        operation: L::Operation,
        inputs: Vec<Rows>,
        outputs: Outputs<L::Value>,
    ) -> Result<Connected, Rejected> {
        self.attempt(|this| this.connect_checked(operation, inputs, outputs))
    }

    fn connect_checked(
        &mut self,
        operation: L::Operation,
        inputs: Vec<Rows>,
        outputs: Outputs<L::Value>,
    ) -> Result<Connected, Rejected> {
        let base = self.arena.outputs.len();
        let mut fresh = Vec::new();
        let mut own = |declare: super::Declare<L::Value>| {
            fresh.push(OutputInfo {
                name: declare.name,
                value: declare.value,
            });
            OutputId(base + fresh.len() - 1)
        };
        let spec = match outputs {
            Outputs::Preserve(declared) => {
                Spec::Preserve(declared.into_iter().map(&mut own).collect())
            }
            Outputs::Replace(items) => Spec::Replace(
                items
                    .into_iter()
                    .map(|item| match item {
                        Output::Keep(output) if output.plan == self.arena.plan => {
                            Ok(Item::Keep(output.output))
                        }
                        Output::Keep(_) => Err(Rejected::ForeignPlan),
                        Output::Declare(declare) => Ok(Item::Own(own(declare))),
                    })
                    .collect::<Result<_, _>>()?,
            ),
        };
        let declared = (base..base + fresh.len())
            .map(|index| Ref {
                plan: self.arena.plan,
                output: OutputId(index),
            })
            .collect();
        let nodes = inputs
            .iter()
            .map(|rows| self.node(rows))
            .collect::<Result<Vec<_>, _>>()?;
        let node = self.attach(operation, nodes, spec, fresh)?;
        Ok(Connected {
            rows: self.rows(node),
            declared,
        })
    }

    pub(super) fn node(&self, rows: &Rows) -> Result<NodeId, Rejected> {
        if rows.plan != self.arena.plan {
            return Err(Rejected::ForeignPlan);
        }
        let node = self
            .arena
            .nodes
            .get(rows.node.0)
            .ok_or(Rejected::ForeignPlan)?;
        if node.scope != self.scope {
            return Err(Rejected::OutOfScope);
        }
        if node.consumed {
            return Err(Rejected::Consumed);
        }
        Ok(rows.node)
    }

    pub(super) fn rows(&self, node: NodeId) -> Rows {
        Rows {
            plan: self.arena.plan,
            node,
        }
    }

    pub(super) fn attach(
        &mut self,
        mut operation: L::Operation,
        inputs: Vec<NodeId>,
        spec: Spec,
        fresh: Vec<OutputInfo<L::Value>>,
    ) -> Result<NodeId, Rejected> {
        let arena = &self.arena;
        let visible: HashSet<OutputId> = inputs
            .iter()
            .flat_map(|input| arena.nodes[input.0].contract.iter().copied())
            .collect();
        let mut invalid = None;
        L::references(&mut operation, &mut |output| {
            if output.plan != arena.plan {
                invalid.get_or_insert(Rejected::ForeignPlan);
            } else if !visible.contains(&output.output) {
                invalid.get_or_insert(Rejected::NotVisible(arena.describe(output.output)));
            }
        });
        if let Some(rejected) = invalid {
            return Err(rejected);
        }
        let contract = match &spec {
            Spec::Preserve(own) => inputs
                .iter()
                .flat_map(|input| arena.nodes[input.0].contract.iter().copied())
                .chain(own.iter().copied())
                .collect(),
            Spec::Replace(items) => items
                .iter()
                .map(|item| match item {
                    Item::Keep(output) if !visible.contains(output) => {
                        Err(Rejected::NotVisible(arena.describe(*output)))
                    }
                    Item::Keep(output) | Item::Own(output) => Ok(*output),
                })
                .collect::<Result<_, _>>()?,
        };
        for input in &inputs {
            self.arena.nodes[input.0].consumed = true;
        }
        self.arena.outputs.extend(fresh);
        self.arena.nodes.push(Node {
            operation,
            inputs,
            spec,
            contract,
            scope: self.scope,
            consumed: false,
        });
        Ok(NodeId(self.arena.nodes.len() - 1))
    }

    pub fn finish(mut self, root: Rows) -> Result<Plan<L>, Rejected> {
        let root = self.attempt(|this| {
            if this.pending.as_ref().is_some_and(|pending| !pending.done()) {
                return Err(Rejected::Failed);
            }
            let root = this.node(&root)?;
            this.arena.nodes[root.0].consumed = true;
            let unused = this
                .arena
                .nodes
                .iter()
                .filter(|node| !node.consumed)
                .count();
            if unused > 0 {
                return Err(Rejected::Unused(unused));
            }
            Ok(root)
        })?;
        Ok(Plan {
            arena: self.arena,
            root,
        })
    }
}
