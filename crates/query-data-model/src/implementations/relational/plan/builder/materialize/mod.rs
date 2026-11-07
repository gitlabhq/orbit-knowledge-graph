use std::collections::{HashMap, HashSet};

use super::{RelationalBuilder, Subquery};
use crate::catalog::Catalog;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{
    BinaryOp, Expr, Operation, Relation, RelationalError, RelationalPlan, Sharing,
};
use crate::plan::tracking::{BodyId, NodeId, Ref};

mod copy;

/// How `Catalog::materialize` turns shared relations into SQL bodies.
#[derive(Debug, Clone)]
pub struct SharePolicy {
    pub materialized: bool,
    pub prefix: String,
}

impl SharePolicy {
    pub fn of<B: RelationalBackend>(prefix: impl Into<String>) -> Self {
        Self {
            materialized: B::CAPABILITIES.materialized_bodies,
            prefix: prefix.into(),
        }
    }
}

impl<B: RelationalBackend> Catalog<B> {
    /// Shares read more than once become bodies with only the columns their readers use; a share read
    /// once is inlined into its reader, its filters joining the reader's WHERE.
    pub fn materialize(
        &self,
        plan: RelationalPlan<B>,
        policy: &SharePolicy,
    ) -> Result<RelationalPlan<B>, RelationalError> {
        if !plan
            .bodies
            .iter()
            .any(|body| body.sharing == Sharing::Pending)
        {
            return Ok(plan);
        }
        let mut copier = Copier::new(&plan, policy);
        self.build(|q| {
            if !plan.settings.is_empty() {
                q.settings(plan.settings.clone())?;
            }
            copier.run(q)
        })
    }
}

struct Copier<'p, B> {
    old: &'p RelationalPlan<B>,
    policy: &'p SharePolicy,
    readers: Vec<usize>,
    wanted: HashSet<(BodyId, usize)>,
    refs: HashMap<Ref, Ref>,
    bodies: HashMap<BodyId, Subquery>,
    kept: HashMap<BodyId, Vec<usize>>,
    named: usize,
}

struct Source {
    rows: Relation,
    pending: Vec<Expr>,
}

impl<'p, B: RelationalBackend> Copier<'p, B> {
    fn new(old: &'p RelationalPlan<B>, policy: &'p SharePolicy) -> Self {
        let plan = &old.plan;
        let mut readers = vec![0; old.bodies.len()];
        let mut live: HashSet<Ref> = plan.outputs(plan.root()).collect();
        for node in plan.nodes() {
            let mut operation = plan.operation(node).clone();
            operation.visit(&mut |output| {
                live.insert(*output);
            });
            match plan.operation(node) {
                Operation::Read { body, .. } => readers[body.index()] += 1,
                Operation::Union => {
                    for arm in plan.inputs(node) {
                        live.extend(plan.outputs(*arm));
                    }
                }
                _ => {}
            }
        }
        let mut wanted: HashSet<(BodyId, usize)> = HashSet::new();
        loop {
            let before = wanted.len();
            for node in plan.nodes() {
                let Operation::Read { body, .. } = plan.operation(node) else {
                    continue;
                };
                let (_, root) = plan.body(*body);
                for (index, (read, fed)) in plan.outputs(node).zip(plan.outputs(root)).enumerate() {
                    if live.contains(&read) && wanted.insert((*body, index)) {
                        live.insert(fed);
                    }
                }
            }
            if wanted.len() == before {
                break;
            }
        }
        Self {
            old,
            policy,
            readers,
            wanted,
            refs: HashMap::new(),
            bodies: HashMap::new(),
            kept: HashMap::new(),
            named: 0,
        }
    }

    fn run(&mut self, q: &mut RelationalBuilder<'_, B>) -> Result<Relation, RelationalError> {
        let mut top: Vec<BodyId> = self.old.with.clone();
        top.extend(self.old.plan.bodies().filter(|body| self.shared(*body)));
        top.sort();
        for body in top {
            self.define(q, body)?;
        }
        self.complete(q, self.old.plan.root())
    }

    fn shared(&self, body: BodyId) -> bool {
        self.old.body(body).sharing == Sharing::Pending && self.readers[body.index()] > 1
    }

    fn name(&mut self) -> String {
        self.named += 1;
        format!("{}{}", self.policy.prefix, self.named - 1)
    }

    fn bind(
        &mut self,
        q: &RelationalBuilder<'_, B>,
        node: NodeId,
        rows: &Relation,
    ) -> Result<(), RelationalError> {
        let plan = &self.old.plan;
        let fresh = q.construction.outputs(&rows.rows)?;
        let old: Vec<Ref> = plan.outputs(node).collect();
        let inherited: HashSet<Ref> = plan
            .inputs(node)
            .iter()
            .flat_map(|input| plan.outputs(*input))
            .collect();
        let shift = fresh.len() as isize - old.len() as isize;
        for (position, output) in old.iter().enumerate() {
            if inherited.contains(output) {
                continue;
            }
            let index = usize::try_from(position as isize + shift)
                .map_err(|_| RelationalError::Incomplete("a copied output"))?;
            self.refs.insert(*output, fresh[index]);
        }
        Ok(())
    }

    fn remap(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        expr: &Expr,
    ) -> Result<Expr, RelationalError> {
        let mut expr = expr.clone();
        let mut bodies = Vec::new();
        expr.bodies(&mut |body| bodies.push(*body));
        for body in bodies {
            if !self.bodies.contains_key(&body) {
                let (name, _) = self.old.plan.body(body);
                let name = name.to_string();
                let built = q.subquery(&name, |q| self.statement(q, body))?;
                self.bodies.insert(body, built);
            }
        }
        expr.bodies(&mut |body| *body = self.bodies[body].id());
        let mut missing = false;
        expr.visit(&mut |output| match self.refs.get(output) {
            Some(mapped) => *output = *mapped,
            None => missing = true,
        });
        match missing {
            true => Err(RelationalError::Incomplete("a copied column")),
            false => Ok(expr),
        }
    }
}

fn conjunction(mut parts: Vec<Expr>) -> Option<Expr> {
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        length => {
            let right = parts.split_off(length / 2);
            Some(Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(conjunction(parts)?),
                right: Box::new(conjunction(right)?),
            })
        }
    }
}
