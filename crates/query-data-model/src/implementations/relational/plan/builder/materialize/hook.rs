use super::Share;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::builder::RelationalBuilder;
use crate::implementations::relational::plan::{
    Operation, Relation, RelationalError, Selection, Sharing,
};
use crate::plan::tracking::{BodyId, Hook, NodeId, Rebuild};

type Built<T> = Result<T, RelationalError>;
type Q<'q, 'c, B> = &'q mut RelationalBuilder<'c, B>;
type R<'r, 'p, 'c, B> = &'r mut Rebuild<'p, RelationalBuilder<'c, B>>;

impl<'c, B: RelationalBackend> Hook<RelationalBuilder<'c, B>> for Share<'_, B> {
    fn node(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        node: NodeId,
    ) -> Built<Relation> {
        let old = self.old;
        let plan = &old.plan;
        let inputs = plan.inputs(node);
        match plan.operation(node) {
            Operation::Read { body, alias } => self.read(rebuild, q, node, *body, alias, false),
            Operation::Filter(_) => {
                let rows = rebuild.visit(self, q, inputs[0])?;
                if let Operation::Filter(predicate) = rebuild.operation(self, q, node)? {
                    self.defer(&rows, [predicate]);
                }
                Ok(rows)
            }
            Operation::Join(_) => {
                let left = rebuild.visit(self, q, inputs[0])?;
                let right = match plan.operation(inputs[1]) {
                    Operation::Read { body, alias } => {
                        self.read(rebuild, q, inputs[1], *body, alias, true)?
                    }
                    _ => rebuild.visit(self, q, inputs[1])?,
                };
                let Operation::Join(kind) = rebuild.operation(self, q, node)? else {
                    return Err(RelationalError::Incomplete("a join"));
                };
                let mut pending = self.pending.remove(&left.rows.node()).unwrap_or_default();
                pending.extend(self.pending.remove(&right.rows.node()).unwrap_or_default());
                let rows = q.join(left, right, kind)?;
                self.defer(&rows, pending);
                Ok(rows)
            }
            Operation::Scan { .. } => rebuild.keep(self, q, node),
            Operation::Union => {
                let mut arms = Vec::new();
                for arm in inputs {
                    arms.push(self.complete(rebuild, q, *arm)?);
                }
                rebuild.replay(q, node, Operation::Union, arms)
            }
            _ => {
                let input = self.complete(rebuild, q, inputs[0])?;
                let operation = rebuild.operation(self, q, node)?;
                rebuild.replay(q, node, operation, vec![input])
            }
        }
    }

    fn body(&mut self, rebuild: R<'_, '_, 'c, B>, q: Q<'_, 'c, B>, body: BodyId) -> Built<BodyId> {
        let name = self.old.plan.body(body).0.to_string();
        Ok(q.subquery(&name, |q| self.statement(rebuild, q, body))?
            .id())
    }

    fn scope(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        body: Option<BodyId>,
    ) -> Built<()> {
        let bodies = match body {
            Some(body) => self.old.body(body).with.clone(),
            None => {
                let mut top = self.old.with.clone();
                top.extend(self.old.plan.bodies().filter(|body| self.shared(*body)));
                top.sort();
                top
            }
        };
        for body in bodies {
            self.define(rebuild, q, body)?;
        }
        Ok(())
    }
}

impl<'c, B: RelationalBackend> Share<'_, B> {
    fn define(&mut self, rebuild: R<'_, '_, 'c, B>, q: Q<'_, 'c, B>, body: BodyId) -> Built<()> {
        let built = match self.old.body(body).sharing {
            Sharing::Shared { materialized } => {
                let (name, _) = self.old.plan.body(body);
                q.cte(name, materialized, |q| self.statement(rebuild, q, body))?
            }
            _ => {
                let name = self.name();
                let materialized = self.policy.materialized;
                q.cte(&name, materialized, |q| self.pruned(rebuild, q, body))?
            }
        };
        self.bodies.insert(body, built);
        Ok(())
    }

    pub(super) fn statement(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        body: BodyId,
    ) -> Built<Relation> {
        self.scope(rebuild, q, Some(body))?;
        let (_, root) = self.old.plan.body(body);
        let rows = self.complete(rebuild, q, root)?;
        self.roots.insert(body, q.construction.outputs(&rows.rows)?);
        Ok(rows)
    }

    pub(super) fn complete(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        node: NodeId,
    ) -> Built<Relation> {
        let rows = rebuild.visit(self, q, node)?;
        self.flush(q, rows)
    }

    fn pruned(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        body: BodyId,
    ) -> Built<Relation> {
        let old = self.old;
        let plan = &old.plan;
        let (_, root) = plan.body(body);
        let outputs: Vec<_> = plan.outputs(root).collect();
        let wanted = |index: &usize| self.wanted.contains(&(body, *index));
        if *plan.operation(root) != Operation::Select(Selection::All)
            || (0..outputs.len()).all(|index| wanted(&index))
        {
            return self.statement(rebuild, q, body);
        }
        let rows = self.complete(rebuild, q, plan.inputs(root)[0])?;
        let mut kept: Vec<usize> = (0..outputs.len())
            .filter(|index| {
                self.wanted.contains(&(body, *index))
                    || rows.computed.iter().any(|(computed, _)| {
                        rebuild.output(outputs[*index]).ok().as_ref() == Some(computed)
                    })
            })
            .collect();
        kept.sort_by_key(|index| plan.name(outputs[*index]).unwrap_or_default());
        let columns = kept
            .iter()
            .map(|index| q.column(rebuild.output(outputs[*index])?))
            .collect::<Built<Vec<_>>>()?;
        let rows = q.select(rows, columns)?;
        self.roots.insert(body, q.construction.outputs(&rows.rows)?);
        Ok(rows)
    }
}
