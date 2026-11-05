use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{JoinKind, Relation, RelationalBuilder};

use super::query::{Imported, Importer};
use super::scope::Scope;
use crate::ast::{self, JoinType, TableRef};

impl Importer {
    pub(super) fn from<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        table: &TableRef,
    ) -> Imported<Relation> {
        match table {
            TableRef::Scan {
                table,
                alias,
                final_,
                ..
            } => {
                let declared = q.catalog().table(table)?;
                q.scan(declared, self.read_mode(*final_), alias)
            }
            TableRef::Cte { name, alias } => {
                let body = self.cte(name)?.clone();
                q.read(&body, Some(alias))
            }
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let left = self.from(q, left)?;
                let right = self.from(q, right)?;
                let kind = match join_type {
                    JoinType::Cross => JoinKind::Cross,
                    join_type => {
                        let mut scope = Scope::new(vec![&left, &right]);
                        let on = self.expr(q, on, &mut scope)?;
                        match join_type {
                            JoinType::Left => JoinKind::Left(on),
                            _ => JoinKind::Inner(on),
                        }
                    }
                };
                q.join(left, right, kind)
            }
            TableRef::Union { queries, alias } => {
                let body = q.subquery(alias, |q| {
                    let arms = queries
                        .iter()
                        .map(|query| self.statement(q, query))
                        .collect::<Imported<Vec<_>>>()?;
                    q.union(arms)
                })?;
                q.read(&body, Some(alias))
            }
            TableRef::Subquery { query, alias } => {
                let body = q.subquery(alias, |q| self.statement(q, query))?;
                q.read(&body, Some(alias))
            }
        }
    }

    pub(super) fn subquery<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
    ) -> Imported<query_data_model::implementations::relational::plan::Subquery> {
        q.subquery("subquery", |q| self.statement(q, query))
    }
}
