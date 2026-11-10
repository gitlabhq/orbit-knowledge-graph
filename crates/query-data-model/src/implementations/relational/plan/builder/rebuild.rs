use super::{RelationalBuilder, Subquery};
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{Expr, RelationalError, Sharing};
use crate::plan::tracking::{BodyId, Hook, Rebuild, Rejected, Step};

impl<'c, B: RelationalBackend> RelationalBuilder<'c, B> {
    /// Rebuilds the bodies a scope defines, in the order the old plan defined them.
    pub(super) fn define_scope<H: Hook<Self>>(
        &mut self,
        rebuild: &mut Rebuild<'_, Self>,
        hook: &mut H,
        body: Option<BodyId>,
    ) -> Result<(), RelationalError> {
        let old = rebuild.old();
        for inner in old.plan.defines(body) {
            let (name, root) = old.plan.body(inner);
            let mut statement = |q: &mut Self| {
                hook.scope(rebuild, q, Some(inner))?;
                rebuild.visit(hook, q, root)
            };
            let built = match old.body(inner).sharing {
                Sharing::Shared { materialized } => self.cte(name, materialized, statement)?,
                Sharing::Inline => self.subquery(name, statement)?,
                Sharing::Pending { lookup: false } => {
                    let rows = statement(self)?;
                    self.share(rows)?
                }
                Sharing::Pending { lookup: true } => {
                    let rows = statement(self)?;
                    self.share_lookup(rows)?
                }
            };
            rebuild.define(inner, built.id());
        }
        Ok(())
    }

    pub(super) fn subquery_of(&self, body: BodyId) -> Result<Subquery, RelationalError> {
        let name = self.names.get(body.index()).ok_or(Rejected::ForeignPlan)?;
        Ok(Subquery {
            body: self.construction.body(body)?,
            name: name.clone(),
        })
    }
}

impl<B: RelationalBackend> Step<'_, '_, RelationalBuilder<'_, B>> {
    /// `expr`, written against the old plan, with its columns pointing into the rebuilt plan.
    pub fn expr(&self, mut expr: Expr) -> Result<Expr, RelationalError> {
        let mut missing = None;
        expr.visit(&mut |output| match self.output(*output) {
            Ok(mapped) => *output = mapped,
            Err(rejected) => missing = Some(rejected),
        });
        match missing {
            Some(rejected) => Err(rejected.into()),
            None => Ok(expr),
        }
    }
}
