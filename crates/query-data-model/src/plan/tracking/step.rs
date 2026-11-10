use std::collections::HashSet;

use super::rebuild::{Hook, Op, Rebuild, Rebuilder};
use super::{NodeId, Ref, Rejected};

/// One operation of the old plan, with its rebuilt inputs and its outputs mapped into the new plan.
pub struct Step<'r, 'p, Q: Rebuilder> {
    rebuild: &'r mut Rebuild<'p, Q>,
    node: NodeId,
    pub operation: Op<Q>,
    pub inputs: Vec<Q::Rows>,
}

impl<Q: Rebuilder> Step<'_, '_, Q> {
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The outputs of the old operation.
    pub fn outputs(&self) -> Vec<Ref> {
        self.rebuild.plan().outputs(self.node).collect()
    }

    pub fn output(&self, old: Ref) -> Result<Ref, Rejected> {
        self.rebuild.output(old)
    }

    /// Replays the operation on the inputs, both as the hook left them.
    pub fn keep(&mut self, q: &mut Q) -> Result<Q::Rows, Q::Error> {
        let inputs = std::mem::take(&mut self.inputs);
        let operation = self.operation.clone();
        self.rebuild.replay(q, self.node, operation, inputs)
    }

    /// Points the consumers of an old output of this operation at `new`.
    pub fn reconnect(&mut self, old: Ref, new: Ref) -> Result<(), Rejected> {
        match self.outputs().contains(&old) {
            true => Ok(self.rebuild.reconnect(old, new)),
            false => Err(Rejected::BadReconnection),
        }
    }

    /// Lets consumers see `new`, an output the old operation did not have.
    pub fn expose(&mut self, new: Ref) {
        self.rebuild.exposed.insert(new);
    }
}

pub struct Each<F>(pub F);

impl<Q, F> Hook<Q> for Each<F>
where
    Q: Rebuilder,
    F: FnMut(&mut Q, &mut Step<'_, '_, Q>) -> Result<Q::Rows, Q::Error>,
{
    fn node(
        &mut self,
        rebuild: &mut Rebuild<'_, Q>,
        q: &mut Q,
        node: NodeId,
    ) -> Result<Q::Rows, Q::Error> {
        let inputs = rebuild.inputs(self, q, node)?;
        let operation = rebuild.operation(self, q, node)?;
        let mut step = Step {
            rebuild,
            node,
            operation,
            inputs,
        };
        let rows = (self.0)(q, &mut step)?;
        rebuild.check(q, node, &rows)?;
        Ok(rows)
    }
}

impl<Q: Rebuilder> Rebuild<'_, Q> {
    /// Every old output maps into the new outputs, and every other new output was exposed or
    /// reconnected away from; a body's root keeps exactly its outputs.
    fn check(&self, q: &mut Q, node: NodeId, rows: &Q::Rows) -> Result<(), Rejected> {
        let plan = self.plan();
        if node == plan.root() {
            return Ok(());
        }
        let body = plan.bodies().any(|body| plan.body(body).1 == node);
        let construction = q.construction();
        let fresh = construction.outputs(Q::rows(rows))?;
        let mut images = HashSet::new();
        let mut changed = Vec::new();
        for old in plan.outputs(node) {
            match self.refs.get(&old) {
                Some(new) if fresh.contains(new) => {
                    images.insert(*new);
                }
                _ => changed.push(self.describe(old)),
            }
        }
        for new in &fresh {
            let allowed = self.exposed.contains(new) || self.released.contains(new);
            if !images.contains(new) && (body || !allowed) {
                changed.push(construction.describe(*new));
            }
        }
        match changed.is_empty() {
            true => Ok(()),
            false => Err(Rejected::ChangedOutputs(changed.join(", "))),
        }
    }
}
