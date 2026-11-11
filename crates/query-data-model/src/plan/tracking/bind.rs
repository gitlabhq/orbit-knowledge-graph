use super::contract::{Item, Spec};
use super::rebuild::{Hook, Op, Rebuild, Rebuilder};
use super::{BodyId, Declare, NodeId, Output, OutputId, Outputs, Ref, Rejected};

impl<Q: Rebuilder> Rebuild<'_, Q> {
    /// Connects `operation` for `node` and points the node's own outputs at the new ones, in order.
    pub fn replay(
        &mut self,
        q: &mut Q,
        node: NodeId,
        operation: Op<Q>,
        inputs: Vec<Q::Rows>,
    ) -> Result<Q::Rows, Q::Error> {
        let plan = self.plan();
        let spec = &plan.arena.nodes[node.0].spec;
        let declare = |output: &OutputId| {
            let info = &plan.arena.outputs[output.0];
            Declare {
                name: info.name.clone(),
                value: info.value.clone(),
            }
        };
        let outputs = match spec {
            Spec::Preserve(own) => Outputs::Preserve(own.iter().map(declare).collect()),
            Spec::Replace(items) => Outputs::Replace(
                items
                    .iter()
                    .map(|item| match item {
                        Item::Keep(output) => self.output(plan.handle(*output)).map(Output::Keep),
                        Item::Own(output) => Ok(Output::Declare(declare(output))),
                    })
                    .collect::<Result<_, _>>()?,
            ),
        };
        let rows = q.apply(operation, inputs, outputs)?;
        let fresh = q.construction().own(Q::rows(&rows))?;
        for (old, new) in spec.own().into_iter().zip(fresh) {
            self.refs.insert(plan.handle(old), new);
        }
        Ok(rows)
    }

    pub fn keep<H: Hook<Q>>(
        &mut self,
        hook: &mut H,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Q::Rows, Q::Error> {
        let inputs = self.inputs(hook, q, node)?;
        let operation = self.operation(hook, q, node)?;
        self.replay(q, node, operation, inputs)
    }

    pub fn output(&self, old: Ref) -> Result<Ref, Rejected> {
        self.refs
            .get(&old)
            .copied()
            .ok_or_else(|| Rejected::NotVisible(self.describe(old)))
    }

    pub fn reconnect(&mut self, old: Ref, new: Ref) {
        if let Some(previous) = self.refs.insert(old, new) {
            self.released.insert(previous);
        }
    }

    pub fn define(&mut self, old: BodyId, new: BodyId) {
        self.bodies.insert(old, new);
    }

    pub fn defined(&self, old: BodyId) -> Result<BodyId, Rejected> {
        self.bodies
            .get(&old)
            .copied()
            .ok_or_else(|| Rejected::BodyNotVisible(self.plan().body(old).0.to_string()))
    }
}
