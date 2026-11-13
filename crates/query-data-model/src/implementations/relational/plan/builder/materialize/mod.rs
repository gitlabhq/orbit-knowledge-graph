use std::collections::{HashMap, HashSet};

use super::{RelationalBuilder, Subquery};
use crate::catalog::Catalog;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{
    BinaryOp, Expr, Operation, Relation, RelationalError, RelationalPlan, Sharing,
};
use crate::plan::tracking::{BodyId, NodeId, Ref};

mod hook;
mod read;

/// How `Catalog::materialize` turns shared relations into SQL bodies.
#[derive(Debug, Clone)]
pub struct SharePolicy {
    pub materialized: bool,
    pub inline_lookups: bool,
    pub prefix: String,
}

impl SharePolicy {
    pub fn of<B: RelationalBackend>(prefix: impl Into<String>) -> Self {
        Self {
            materialized: B::SQL.materialized_bodies,
            inline_lookups: true,
            prefix: prefix.into(),
        }
    }
}

impl<B: RelationalBackend> Catalog<B> {
    /// Shares read more than once become bodies with only the columns their readers use; a share read
    /// once, or a point lookup the policy recomputes, is inlined into each reader, its filters joining
    /// the reader's WHERE.
    pub fn materialize(
        &self,
        plan: RelationalPlan<B>,
        policy: &SharePolicy,
    ) -> Result<RelationalPlan<B>, RelationalError> {
        if !plan
            .bodies
            .iter()
            .any(|body| matches!(body.sharing, Sharing::Pending { .. }))
        {
            return Ok(plan);
        }
        self.rebuild_with(&plan, &mut Share::new(&plan, policy))
    }
}

struct Share<'p, B> {
    old: &'p RelationalPlan<B>,
    policy: &'p SharePolicy,
    readers: Vec<usize>,
    wanted: HashSet<(BodyId, usize)>,
    bodies: HashMap<BodyId, Subquery>,
    roots: HashMap<BodyId, Vec<Ref>>,
    pending: HashMap<NodeId, Vec<Expr>>,
    named: usize,
}

impl<'p, B: RelationalBackend> Share<'p, B> {
    fn new(old: &'p RelationalPlan<B>, policy: &'p SharePolicy) -> Self {
        let plan = &old.plan;
        let mut readers = vec![0; old.bodies.len()];
        for node in old.reachable() {
            if let Operation::Read { body, .. } = plan.operation(node) {
                readers[body.index()] += 1;
            }
        }
        let mut live: HashSet<Ref> = plan.outputs(plan.root()).collect();
        for node in plan.nodes() {
            let mut operation = plan.operation(node).clone();
            operation.visit(&mut |output| {
                live.insert(*output);
            });
            if *plan.operation(node) == Operation::Union {
                for arm in plan.inputs(node) {
                    live.extend(plan.outputs(*arm));
                }
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
            bodies: HashMap::new(),
            roots: HashMap::new(),
            pending: HashMap::new(),
            named: 0,
        }
    }

    fn shared(&self, body: BodyId) -> bool {
        match self.old.body(body).sharing {
            Sharing::Pending { lookup } => {
                self.readers[body.index()] > 1 && !(lookup && self.policy.inline_lookups)
            }
            _ => false,
        }
    }

    fn name(&mut self) -> String {
        self.named += 1;
        format!("{}{}", self.policy.prefix, self.named - 1)
    }

    fn defer(&mut self, rows: &Relation, predicates: impl IntoIterator<Item = Expr>) {
        let pending = self.pending.entry(rows.rows.node()).or_default();
        pending.extend(predicates);
    }

    /// Joins the filters deferred onto `rows` into one WHERE.
    fn flush(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        rows: Relation,
    ) -> Result<Relation, RelationalError> {
        let pending = self.pending.remove(&rows.rows.node()).unwrap_or_default();
        match conjunction(pending) {
            Some(predicate) => q.filter(rows, predicate),
            None => Ok(rows),
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
