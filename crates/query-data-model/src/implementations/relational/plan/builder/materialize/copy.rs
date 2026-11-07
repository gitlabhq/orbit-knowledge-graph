use super::{Copier, Source, conjunction};
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::builder::RelationalBuilder;
use crate::implementations::relational::plan::{
    JoinKind, Measure, Operation, Order, Relation, RelationalError, Selection, Sharing,
};
use crate::plan::tracking::{BodyId, Declare, Declared, NodeId};

type Built<T> = Result<T, RelationalError>;

impl<B: RelationalBackend> Copier<'_, B> {
    pub(super) fn define(&mut self, q: &mut RelationalBuilder<'_, B>, body: BodyId) -> Built<()> {
        let built = match self.old.body(body).sharing {
            Sharing::Shared { materialized } => {
                let (name, _) = self.old.plan.body(body);
                let name = name.to_string();
                q.cte(&name, materialized, |q| self.statement(q, body))?
            }
            _ => {
                let name = self.name();
                let materialized = self.policy.materialized;
                q.cte(&name, materialized, |q| self.pruned(q, body))?
            }
        };
        self.bodies.insert(body, built);
        Ok(())
    }

    pub(super) fn statement(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        body: BodyId,
    ) -> Built<Relation> {
        for inner in self.old.body(body).with.clone() {
            self.define(q, inner)?;
        }
        let (_, root) = self.old.plan.body(body);
        let rows = self.complete(q, root)?;
        self.root(q, body, &rows)?;
        Ok(rows)
    }

    fn root(&mut self, q: &RelationalBuilder<'_, B>, body: BodyId, rows: &Relation) -> Built<()> {
        let outputs = q.construction.outputs(&rows.rows)?;
        self.roots.insert(body, outputs);
        Ok(())
    }

    fn pruned(&mut self, q: &mut RelationalBuilder<'_, B>, body: BodyId) -> Built<Relation> {
        let old = self.old;
        let plan = &old.plan;
        let (_, root) = plan.body(body);
        let outputs: Vec<_> = plan.outputs(root).collect();
        let wanted = |index: &usize| self.wanted.contains(&(body, *index));
        if *plan.operation(root) != Operation::Select(Selection::All)
            || (0..outputs.len()).all(|index| wanted(&index))
        {
            return self.statement(q, body);
        }
        let input = plan.inputs(root)[0];
        let source = self.source(q, input)?;
        let rows = self.flush(q, source)?;
        let mut kept: Vec<usize> = (0..outputs.len())
            .filter(|index| {
                self.wanted.contains(&(body, *index))
                    || rows
                        .computed
                        .iter()
                        .any(|(computed, _)| self.refs.get(&outputs[*index]) == Some(computed))
            })
            .collect();
        kept.sort_by_key(|index| plan.name(outputs[*index]).unwrap_or_default());
        let columns = kept
            .iter()
            .map(|index| q.column(self.refs[&outputs[*index]]))
            .collect::<Built<Vec<_>>>()?;
        let rows = q.select(rows, columns)?;
        self.root(q, body, &rows)?;
        Ok(rows)
    }

    pub(super) fn complete(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        node: NodeId,
    ) -> Built<Relation> {
        let source = self.source(q, node)?;
        self.flush(q, source)
    }

    fn flush(&mut self, q: &mut RelationalBuilder<'_, B>, source: Source) -> Built<Relation> {
        match conjunction(source.pending) {
            Some(predicate) => q.filter(source.rows, predicate),
            None => Ok(source.rows),
        }
    }

    fn source(&mut self, q: &mut RelationalBuilder<'_, B>, node: NodeId) -> Built<Source> {
        let old = self.old;
        let plan = &old.plan;
        let operation = plan.operation(node).clone();
        let inputs = plan.inputs(node).to_vec();
        let rows = match operation {
            Operation::Read { body, alias } => return self.read(q, node, body, alias, false),
            Operation::Filter(predicate) => {
                let mut source = self.source(q, inputs[0])?;
                source.pending.push(self.remap(q, &predicate)?);
                return Ok(source);
            }
            Operation::Join(kind) => {
                let left = self.source(q, inputs[0])?;
                let right = match plan.operation(inputs[1]).clone() {
                    Operation::Read { body, alias } => {
                        self.read(q, inputs[1], body, alias, true)?
                    }
                    _ => self.source(q, inputs[1])?,
                };
                let kind = match kind {
                    JoinKind::Inner(on) => JoinKind::Inner(self.remap(q, &on)?),
                    JoinKind::Left(on) => JoinKind::Left(self.remap(q, &on)?),
                    JoinKind::Cross => JoinKind::Cross,
                };
                let rows = q.join(left.rows, right.rows, kind)?;
                self.bind(q, node, &rows)?;
                let mut pending = left.pending;
                pending.extend(right.pending);
                return Ok(Source { rows, pending });
            }
            Operation::Scan { table, read, alias } => {
                let table = Declared::new(q.catalog().model().id(), table);
                q.scan(table, read, &alias)?
            }
            Operation::Union => {
                let mut arms = Vec::new();
                for arm in inputs {
                    arms.push(self.complete(q, arm)?);
                }
                q.union(arms)?
            }
            operation => {
                let rows = self.complete(q, inputs[0])?;
                self.apply(q, node, operation, rows)?
            }
        };
        self.bind(q, node, &rows)?;
        Ok(Source {
            rows,
            pending: Vec::new(),
        })
    }

    fn read(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        node: NodeId,
        body: BodyId,
        alias: Option<String>,
        joined: bool,
    ) -> Built<Source> {
        let old = self.old;
        let plan = &old.plan;
        let (name, root) = plan.body(body);
        let sharing = old.body(body).sharing;
        if sharing == Sharing::Pending && !self.shared(body) {
            if let Some(source) = self.splice(q, root, alias.as_deref(), joined)? {
                let fed: Vec<_> = plan
                    .outputs(node)
                    .zip(plan.outputs(root))
                    .filter_map(|(read, fed)| Some((read, *self.refs.get(&fed)?)))
                    .collect();
                self.refs.extend(fed);
                return Ok(source);
            }
            let name = alias.clone().unwrap_or_else(|| self.name());
            let inline = q.subquery(&name, |q| self.statement(q, body))?;
            let rows = q.read(&inline, Some(&name))?;
            self.through(q, node, body, &rows)?;
            return Ok(Source {
                rows,
                pending: Vec::new(),
            });
        }
        let built = match sharing {
            Sharing::Inline => {
                let name = name.to_string();
                q.subquery(&name, |q| self.statement(q, body))?
            }
            _ => self.bodies[&body].clone(),
        };
        let rows = q.read(&built, alias.as_deref())?;
        self.through(q, node, body, &rows)?;
        Ok(Source {
            rows,
            pending: Vec::new(),
        })
    }

    /// Maps a read's outputs through its body's root, whose copy may keep fewer columns.
    fn through(
        &mut self,
        q: &RelationalBuilder<'_, B>,
        node: NodeId,
        body: BodyId,
        rows: &Relation,
    ) -> Built<()> {
        let plan = &self.old.plan;
        let (_, root) = plan.body(body);
        let fresh = q.construction.outputs(&rows.rows)?;
        let copied = &self.roots[&body];
        let mapped: Vec<_> = plan
            .outputs(node)
            .zip(plan.outputs(root))
            .filter_map(|(read, fed)| {
                let new = self.refs.get(&fed)?;
                let position = copied.iter().position(|copy| copy == new)?;
                Some((read, fresh[position]))
            })
            .collect();
        self.refs.extend(mapped);
        Ok(())
    }

    /// A body that only filters a source under the reader's alias reads that source in place, except
    /// that a join input keeps a derived source and its filters in their own block.
    fn splice(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        root: NodeId,
        alias: Option<&str>,
        joined: bool,
    ) -> Built<Option<Source>> {
        let old = self.old;
        let plan = &old.plan;
        if *plan.operation(root) != Operation::Select(Selection::All) {
            return Ok(None);
        }
        let mut filters = Vec::new();
        let mut base = plan.inputs(root)[0];
        while let Operation::Filter(predicate) = plan.operation(base) {
            filters.push(predicate.clone());
            base = plan.inputs(base)[0];
        }
        let named = match plan.operation(base) {
            Operation::Scan { alias, .. } => Some(alias.as_str()),
            Operation::Read { alias, .. } if !joined => alias.as_deref(),
            _ => None,
        };
        if alias.is_none() || named != alias {
            return Ok(None);
        }
        let mut source = self.source(q, base)?;
        for predicate in filters.iter().rev() {
            source.pending.push(self.remap(q, predicate)?);
        }
        Ok(Some(source))
    }

    fn apply(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        node: NodeId,
        operation: Operation,
        rows: Relation,
    ) -> Built<Relation> {
        let old = self.old;
        let plan = &old.plan;
        Ok(match operation {
            Operation::Aggregate { groups, measures } => {
                let groups = groups
                    .iter()
                    .map(|group| self.remap(q, group))
                    .collect::<Built<_>>()?;
                let mut copied = Vec::new();
                for measure in measures {
                    copied.push(Measure {
                        args: measure
                            .args
                            .iter()
                            .map(|arg| self.remap(q, arg))
                            .collect::<Built<_>>()?,
                        condition: measure
                            .condition
                            .as_ref()
                            .map(|c| self.remap(q, c))
                            .transpose()?,
                        ..measure
                    });
                }
                q.aggregate(rows, groups, copied)?.rows
            }
            Operation::Compute(values) => {
                let outputs: Vec<_> = plan.outputs(node).collect();
                let names = &outputs[outputs.len() - values.len()..];
                let mut items = Vec::new();
                for (value, output) in values.iter().zip(names) {
                    items.push(Declare {
                        name: plan.name(*output).map(str::to_string),
                        value: self.remap(q, value)?,
                    });
                }
                q.compute(rows, items)?.0
            }
            Operation::Order(orders) => {
                let orders = orders
                    .into_iter()
                    .map(|order| {
                        Ok(Order {
                            value: self.remap(q, &order.value)?,
                            ..order
                        })
                    })
                    .collect::<Built<_>>()?;
                q.order(rows, orders)?
            }
            Operation::Latest { key, version } => {
                let key = key
                    .iter()
                    .map(|key| q.column(self.refs[key]))
                    .collect::<Built<_>>()?;
                let version = q.column(self.refs[&version])?;
                q.latest(rows, key, version)?
            }
            Operation::LimitBy { count, by } => {
                let by = by
                    .iter()
                    .map(|value| self.remap(q, value))
                    .collect::<Built<_>>()?;
                q.limit_by(rows, count, by)?
            }
            Operation::Limit { count, offset } => q.limit(rows, count, offset)?,
            Operation::Select(Selection::All) => q.select_all(rows)?,
            Operation::Select(Selection::Columns(columns)) => {
                let columns = columns
                    .iter()
                    .map(|c| q.column(self.refs[c]))
                    .collect::<Built<_>>()?;
                q.select(rows, columns)?
            }
            operation => unreachable!("{operation:?} is copied by its source"),
        })
    }
}
