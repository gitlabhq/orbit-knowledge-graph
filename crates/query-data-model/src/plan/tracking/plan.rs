use super::contract::Spec;
use super::{BodyId, Language, NodeId, OutputId, PlanId, Ref, ScopeId};
use crate::catalog::ModelId;

#[derive(Debug)]
pub(super) struct Node<L: Language> {
    pub(super) operation: L::Operation,
    pub(super) inputs: Vec<NodeId>,
    pub(super) spec: Spec,
    pub(super) contract: Vec<OutputId>,
    pub(super) scope: ScopeId,
    pub(super) consumed: bool,
}

#[derive(Debug)]
pub(super) struct OutputInfo<V> {
    pub(super) name: Option<String>,
    pub(super) value: V,
}

#[derive(Debug)]
pub(super) struct BodyInfo {
    pub(super) name: String,
    pub(super) scope: ScopeId,
    pub(super) root: NodeId,
}

#[derive(Debug)]
pub(super) struct Arena<L: Language> {
    pub(super) plan: PlanId,
    pub(super) snapshot: ModelId,
    pub(super) nodes: Vec<Node<L>>,
    pub(super) outputs: Vec<OutputInfo<L::Value>>,
    pub(super) parents: Vec<Option<ScopeId>>,
    pub(super) bodies: Vec<BodyInfo>,
    pub(super) order: Vec<OutputId>,
}

impl<L: Language> Arena<L> {
    pub(super) fn sees(&self, scope: ScopeId, ancestor: ScopeId) -> bool {
        let mut current = Some(scope);
        while let Some(scope) = current {
            if scope == ancestor {
                return true;
            }
            current = self.parents[scope.0];
        }
        false
    }

    pub(super) fn describe(&self, output: OutputId) -> String {
        self.outputs[output.0]
            .name
            .clone()
            .unwrap_or_else(|| format!("#{}", output.0))
    }
}

#[derive(Debug)]
pub struct Plan<L: Language> {
    pub(super) arena: Arena<L>,
    pub(super) root: NodeId,
}

impl<L: Language> Plan<L> {
    pub fn snapshot(&self) -> ModelId {
        self.arena.snapshot
    }

    pub fn root(&self) -> NodeId {
        self.root
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeId> {
        (0..self.arena.nodes.len()).map(NodeId)
    }

    pub fn operation(&self, node: NodeId) -> &L::Operation {
        &self.arena.nodes[node.0].operation
    }

    pub fn inputs(&self, node: NodeId) -> &[NodeId] {
        &self.arena.nodes[node.0].inputs
    }

    pub fn scope(&self, node: NodeId) -> ScopeId {
        self.arena.nodes[node.0].scope
    }

    pub fn outputs(&self, node: NodeId) -> impl Iterator<Item = Ref> + '_ {
        let plan = self.arena.plan;
        self.arena.nodes[node.0]
            .contract
            .iter()
            .map(move |output| Ref {
                plan,
                output: *output,
            })
    }

    pub fn name(&self, output: Ref) -> Option<&str> {
        self.arena.outputs[output.output.0].name.as_deref()
    }

    pub fn value(&self, output: Ref) -> &L::Value {
        &self.arena.outputs[output.output.0].value
    }

    pub fn declaration_order(&self) -> impl Iterator<Item = Ref> + '_ {
        let plan = self.arena.plan;
        self.arena.order.iter().map(move |output| Ref {
            plan,
            output: *output,
        })
    }

    pub fn bodies(&self) -> impl Iterator<Item = BodyId> {
        (0..self.arena.bodies.len()).map(BodyId)
    }

    pub fn body(&self, body: BodyId) -> (&str, NodeId) {
        let info = &self.arena.bodies[body.0];
        (&info.name, info.root)
    }

    /// The bodies defined directly in the scope of `body`, or at the top when `body` is `None`.
    pub fn defines(&self, body: Option<BodyId>) -> Vec<BodyId> {
        let scope = body.map_or(ScopeId(0), |body| self.arena.bodies[body.0].scope);
        self.bodies()
            .filter(|inner| self.arena.parents[self.arena.bodies[inner.0].scope.0] == Some(scope))
            .collect()
    }

    pub(super) fn handle(&self, output: OutputId) -> Ref {
        Ref {
            plan: self.arena.plan,
            output,
        }
    }
}
