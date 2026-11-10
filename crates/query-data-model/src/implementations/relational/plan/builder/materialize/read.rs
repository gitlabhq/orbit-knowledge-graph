use super::Share;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::builder::RelationalBuilder;
use crate::implementations::relational::plan::{
    Operation, Relation, RelationalError, Selection, Sharing,
};
use crate::plan::tracking::{BodyId, NodeId, Rebuild};

type Built<T> = Result<T, RelationalError>;
type Q<'q, 'c, B> = &'q mut RelationalBuilder<'c, B>;
type R<'r, 'p, 'c, B> = &'r mut Rebuild<'p, RelationalBuilder<'c, B>>;

impl<'c, B: RelationalBackend> Share<'_, B> {
    pub(super) fn read(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        node: NodeId,
        body: BodyId,
        alias: &Option<String>,
        joined: bool,
    ) -> Built<Relation> {
        let old = self.old;
        let plan = &old.plan;
        let (name, root) = plan.body(body);
        let sharing = old.body(body).sharing;
        if matches!(sharing, Sharing::Pending { .. }) && !self.shared(body) {
            if let Some(rows) = self.splice(rebuild, q, root, alias.as_deref(), joined)? {
                for (read, fed) in plan.outputs(node).zip(plan.outputs(root)) {
                    if let Ok(mapped) = rebuild.output(fed) {
                        rebuild.reconnect(read, mapped);
                    }
                }
                return Ok(rows);
            }
            let name = alias.clone().unwrap_or_else(|| self.name());
            let inline = q.subquery(&name, |q| self.statement(rebuild, q, body))?;
            let rows = q.read(&inline, Some(&name))?;
            self.through(rebuild, q, node, body, &rows)?;
            return Ok(rows);
        }
        let built = match sharing {
            Sharing::Inline => q.subquery(name, |q| self.statement(rebuild, q, body))?,
            _ => self.bodies[&body].clone(),
        };
        let rows = q.read(&built, alias.as_deref())?;
        self.through(rebuild, q, node, body, &rows)?;
        Ok(rows)
    }

    /// Maps a read's outputs through its body's root, whose copy may keep fewer columns.
    fn through(
        &self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        node: NodeId,
        body: BodyId,
        rows: &Relation,
    ) -> Built<()> {
        let plan = &self.old.plan;
        let (_, root) = plan.body(body);
        let fresh = q.construction.outputs(&rows.rows)?;
        let copied = &self.roots[&body];
        for (read, fed) in plan.outputs(node).zip(plan.outputs(root)) {
            let Ok(new) = rebuild.output(fed) else {
                continue;
            };
            if let Some(position) = copied.iter().position(|copy| *copy == new) {
                rebuild.reconnect(read, fresh[position]);
            }
        }
        Ok(())
    }

    /// A body that only filters a source under the reader's alias reads that source in place, except
    /// that a join input keeps a derived source and its filters in their own block.
    fn splice(
        &mut self,
        rebuild: R<'_, '_, 'c, B>,
        q: Q<'_, 'c, B>,
        root: NodeId,
        alias: Option<&str>,
        joined: bool,
    ) -> Built<Option<Relation>> {
        let old = self.old;
        let plan = &old.plan;
        if *plan.operation(root) != Operation::Select(Selection::All) {
            return Ok(None);
        }
        let mut filters = Vec::new();
        let mut base = plan.inputs(root)[0];
        while let Operation::Filter(_) = plan.operation(base) {
            filters.push(base);
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
        let rows = rebuild.visit(self, q, base)?;
        for filter in filters.into_iter().rev() {
            if let Operation::Filter(predicate) = rebuild.operation(self, q, filter)? {
                self.defer(&rows, [predicate]);
            }
        }
        Ok(Some(rows))
    }
}
