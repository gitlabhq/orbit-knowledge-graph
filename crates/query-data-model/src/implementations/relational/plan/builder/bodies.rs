use super::{RelationalBuilder, complete, supported};
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{BodyUse, Expr, Relation, RelationalError, Sharing};
use crate::plan::tracking::{Body, BodyId};

const SHARED: &str = "shared";

#[derive(Debug, Clone, PartialEq)]
pub struct Subquery {
    pub(super) body: Body,
    pub(super) name: String,
}

impl Subquery {
    pub fn id(&self) -> BodyId {
        self.body.id()
    }

    pub fn contains(&self, value: Expr) -> Expr {
        Expr::In {
            value: Box::new(value),
            query: self.id(),
        }
    }

    pub fn scalar(&self) -> Expr {
        Expr::Scalar(self.id())
    }
}

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn cte(
        &mut self,
        name: &str,
        materialized: bool,
        build: impl FnOnce(&mut Self) -> Result<Relation, RelationalError>,
    ) -> Result<Subquery, RelationalError> {
        self.guard(match materialized {
            true => supported(B::SQL.materialized_bodies, "materialized bodies"),
            false => Ok(()),
        })?;
        self.define(name, Sharing::Shared { materialized }, build)
    }

    /// Rows that every read of the returned handle sees; `Catalog::materialize` decides how each is shared.
    pub fn share(&mut self, rows: Relation) -> Result<Subquery, RelationalError> {
        self.shared(rows, false)
    }

    /// Shared rows found by a point lookup, which a share policy may recompute at each read.
    pub fn share_lookup(&mut self, rows: Relation) -> Result<Subquery, RelationalError> {
        self.shared(rows, true)
    }

    fn shared(&mut self, rows: Relation, lookup: bool) -> Result<Subquery, RelationalError> {
        let rows = self.guard(complete(rows, "a shared relation"))?;
        let width = self.construction.outputs(&rows.rows)?.len();
        let body = self.construction.share(SHARED, rows.rows)?;
        self.bodies.push(BodyUse {
            sharing: Sharing::Pending { lookup },
            with: Vec::new(),
        });
        self.names.push(SHARED.to_string());
        self.widths.push(width);
        Ok(Subquery {
            body,
            name: SHARED.to_string(),
        })
    }

    pub fn subquery(
        &mut self,
        name: &str,
        build: impl FnOnce(&mut Self) -> Result<Relation, RelationalError>,
    ) -> Result<Subquery, RelationalError> {
        self.define(name, Sharing::Inline, build)
    }

    fn define(
        &mut self,
        name: &str,
        sharing: Sharing,
        build: impl FnOnce(&mut Self) -> Result<Relation, RelationalError>,
    ) -> Result<Subquery, RelationalError> {
        let opening = self.construction.open(name)?;
        self.frames.push(Vec::new());
        let root = build(self);
        let with = self.frames.pop().unwrap_or_default();
        let root = self.guard(complete(root?, "a subquery"))?;
        let width = self.construction.outputs(&root.rows)?.len();
        let body = self.construction.close(opening, root.rows)?;
        self.bodies.push(BodyUse { sharing, with });
        self.names.push(name.to_string());
        self.widths.push(width);
        if sharing != Sharing::Inline
            && let Some(frame) = self.frames.last_mut()
        {
            frame.push(body.id());
        }
        Ok(Subquery {
            body,
            name: name.to_string(),
        })
    }

    pub(super) fn body(&self, subquery: &Subquery) -> Result<&BodyUse, RelationalError> {
        self.bodies
            .get(subquery.id().index())
            .ok_or(RelationalError::Rejected(
                crate::plan::tracking::Rejected::ForeignPlan,
            ))
    }
}
