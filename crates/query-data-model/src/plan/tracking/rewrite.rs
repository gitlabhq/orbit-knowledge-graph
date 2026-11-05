use std::collections::HashMap;

use super::plan::{Arena, Node};
use super::{BodyId, Construction, Language, NodeId, OutputId, Plan, Ref, Rejected, Rows};
use crate::catalog::ModelId;

pub(super) struct Pending<L: Language> {
    old: Vec<Option<Node<L>>>,
    root: NodeId,
    next: usize,
    mapped: Vec<NodeId>,
    remap: HashMap<OutputId, OutputId>,
    bodies: HashMap<NodeId, BodyId>,
    visiting: Option<Visit>,
}

struct Visit {
    old: NodeId,
    new: NodeId,
    contract: Vec<OutputId>,
    reconnect: Vec<(OutputId, OutputId)>,
}

impl<L: Language> Pending<L> {
    pub(super) fn done(&self) -> bool {
        self.next == self.old.len() && self.visiting.is_none()
    }
}

pub fn rewrite<L: Language, B, E: From<Rejected>>(
    builder: &mut B,
    access: fn(&mut B) -> &mut Construction<L>,
    mut visit: impl FnMut(&mut B, Rows) -> Result<Rows, E>,
) -> Result<Rows, E> {
    while let Some(rows) = access(builder).advance()? {
        let replacement = visit(builder, rows)?;
        access(builder).replace(replacement)?;
    }
    Ok(access(builder).rewritten()?)
}

impl<L: Language> Construction<L> {
    pub fn rewrite(plan: Plan<L>, snapshot: ModelId) -> Result<Self, Rejected> {
        if plan.arena.snapshot != snapshot {
            return Err(Rejected::ForeignSnapshot);
        }
        let Arena {
            plan: id,
            snapshot,
            nodes,
            outputs,
            parents,
            bodies,
        } = plan.arena;
        let pending_bodies = bodies
            .iter()
            .enumerate()
            .map(|(index, body)| (body.root, BodyId(index)))
            .collect();
        let mut construction = Self::new(snapshot);
        construction.arena = Arena {
            plan: id,
            snapshot,
            nodes: Vec::with_capacity(nodes.len()),
            outputs,
            parents,
            bodies,
        };
        construction.pending = Some(Pending {
            old: nodes.into_iter().map(Some).collect(),
            root: plan.root,
            next: 0,
            mapped: Vec::new(),
            remap: HashMap::new(),
            bodies: pending_bodies,
            visiting: None,
        });
        Ok(construction)
    }

    pub fn reconnect(&mut self, from: Ref, to: Ref) -> Result<(), Rejected> {
        self.attempt(|this| {
            let plan = this.arena.plan;
            let visit = this
                .pending
                .as_mut()
                .and_then(|pending| pending.visiting.as_mut());
            match visit {
                Some(visit)
                    if from.plan == plan
                        && to.plan == plan
                        && visit.contract.contains(&from.output) =>
                {
                    visit.reconnect.push((from.output, to.output));
                    Ok(())
                }
                _ => Err(Rejected::BadReconnection),
            }
        })
    }

    pub fn visiting(&self) -> Option<NodeId> {
        let pending = self.pending.as_ref()?;
        pending.visiting.as_ref().map(|visit| visit.old)
    }

    pub fn detach(&mut self, rows: Rows) -> Result<(L::Operation, Vec<Rows>), Rejected> {
        self.attempt(|this| {
            let node = this.node(&rows)?;
            let visited = this
                .pending
                .as_ref()
                .and_then(|pending| pending.visiting.as_ref())
                .is_some_and(|visit| visit.new == node);
            if !visited || node.0 + 1 != this.arena.nodes.len() {
                return Err(Rejected::NotDetachable);
            }
            let detached = this.arena.nodes.pop().ok_or(Rejected::Failed)?;
            for input in &detached.inputs {
                this.arena.nodes[input.0].consumed = false;
            }
            let inputs = detached.inputs.iter().map(|input| this.rows(*input));
            Ok((detached.operation, inputs.collect()))
        })
    }

    fn advance(&mut self) -> Result<Option<Rows>, Rejected> {
        self.attempt(|this| {
            let Some(pending) = this.pending.as_mut() else {
                return Err(Rejected::BadReconnection);
            };
            let Some(slot) = pending.old.get_mut(pending.next) else {
                return Ok(None);
            };
            let mut node = slot.take().ok_or(Rejected::Failed)?;
            let old = NodeId(pending.next);
            pending.next += 1;
            let remap = &pending.remap;
            let map = |output: OutputId| remap.get(&output).copied().unwrap_or(output);
            L::references(&mut node.operation, &mut |output| {
                output.output = map(output.output)
            });
            node.spec.remap(map);
            let inputs = node
                .inputs
                .iter()
                .map(|input| pending.mapped[input.0])
                .collect();
            this.scope = node.scope;
            let new = this.attach(node.operation, inputs, node.spec, Vec::new())?;
            let pending = this.pending.as_mut().ok_or(Rejected::Failed)?;
            pending.visiting = Some(Visit {
                old,
                new,
                contract: this.arena.nodes[new.0].contract.clone(),
                reconnect: Vec::new(),
            });
            Ok(Some(this.rows(new)))
        })
    }

    fn replace(&mut self, replacement: Rows) -> Result<(), Rejected> {
        self.attempt(|this| {
            let new = this.node(&replacement)?;
            let pending = this.pending.as_mut().ok_or(Rejected::Failed)?;
            let visit = pending.visiting.take().ok_or(Rejected::Failed)?;
            let body = pending.bodies.remove(&visit.old);
            if visit.old != pending.root {
                let exposed = &this.arena.nodes[new.0].contract;
                check_boundary(&this.arena, &visit, exposed, body.is_some())?;
            }
            pending.remap.extend(visit.reconnect);
            pending.mapped.push(new);
            if let Some(body) = body {
                this.arena.bodies[body.0].root = new;
                this.arena.nodes[new.0].consumed = true;
            }
            Ok(())
        })
    }

    fn rewritten(&mut self) -> Result<Rows, Rejected> {
        let pending = self.pending.as_ref().ok_or(Rejected::Failed)?;
        Ok(self.rows(pending.mapped[pending.root.0]))
    }

    pub(super) fn rebuilt(&self, body: BodyId) -> bool {
        self.pending
            .as_ref()
            .is_none_or(|pending| !pending.bodies.values().any(|pending| *pending == body))
    }
}

fn check_boundary<L: Language>(
    arena: &Arena<L>,
    visit: &Visit,
    exposed: &[OutputId],
    body: bool,
) -> Result<(), Rejected> {
    let from = |output: &OutputId| visit.reconnect.iter().any(|(old, _)| old == output);
    let to = |output: &OutputId| visit.reconnect.iter().any(|(_, new)| new == output);
    if visit
        .reconnect
        .iter()
        .any(|(_, new)| !exposed.contains(new))
    {
        return Err(Rejected::BadReconnection);
    }
    let missing = visit
        .contract
        .iter()
        .filter(|output| !exposed.contains(output) && !from(output));
    let extra = exposed
        .iter()
        .filter(|output| !visit.contract.contains(output) && !to(output));
    let changed: Vec<String> = missing
        .chain(extra)
        .chain(visit.reconnect.iter().filter(|_| body).map(|(old, _)| old))
        .map(|output| arena.describe(*output))
        .collect();
    if changed.is_empty() {
        Ok(())
    } else {
        Err(Rejected::ChangedOutputs(changed.join(", ")))
    }
}
