use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    ReadMode, Relation, RelationalBuilder, RelationalError, Subquery,
};
use query_data_model::plan::tracking::Declare;

use super::scope::{Binding, Scope};
use crate::ast;
use crate::passes::codegen::SqlDialect;

pub(super) type Imported<T> = Result<T, RelationalError>;

pub(super) struct Importer {
    pub(super) dialect: SqlDialect,
    ctes: Vec<(String, Subquery)>,
}

impl Importer {
    pub(super) fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            ctes: Vec::new(),
        }
    }

    pub(super) fn statement<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
    ) -> Imported<Relation> {
        let defined = self.ctes.len();
        let result = self.shared(q, query).and_then(|()| self.query(q, query));
        self.ctes.truncate(defined);
        result
    }

    fn shared<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
    ) -> Imported<()> {
        for cte in &query.ctes {
            if cte.recursive {
                return Err(RelationalError::Unsupported("recursive bodies"));
            }
            let materialized = cte.materialized && self.dialect == SqlDialect::ClickHouse;
            let body = q.cte(&cte.name, materialized, |q| self.statement(q, &cte.query))?;
            self.ctes.push((cte.name.clone(), body));
        }
        Ok(())
    }

    pub(super) fn cte(&self, name: &str) -> Imported<&Subquery> {
        self.ctes
            .iter()
            .rev()
            .find(|(cte, _)| cte == name)
            .map(|(_, body)| body)
            .ok_or_else(|| RelationalError::UnknownTable(name.to_string()))
    }

    fn query<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
    ) -> Imported<Relation> {
        if query.distinct {
            return Err(RelationalError::Unsupported("distinct selects"));
        }
        if query.union_all.is_empty() {
            return self.block(q, query, true);
        }
        let mut arms = vec![self.block(q, query, false)?];
        for arm in &query.union_all {
            if !arm.ctes.is_empty() {
                return Err(RelationalError::Unsupported(
                    "union arms with their own bodies",
                ));
            }
            arms.push(self.query(q, arm)?);
        }
        let rows = q.union(arms)?;
        self.tail(q, query, rows, &[], &[])
    }

    fn block<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
        tail: bool,
    ) -> Imported<Relation> {
        let rows = self.from(q, &query.from)?;
        let rows = match self.predicate(query.where_clause.as_ref()) {
            Some(predicate) => {
                let predicate = self.expr(q, &predicate, &mut Scope::new(vec![&rows]))?;
                q.filter(rows, predicate)?
            }
            None => rows,
        };
        let grouped = self.group(q, query, rows)?;
        let (groups, measures) = (grouped.groups, grouped.measures);
        let mut rows = grouped.rows;
        if let Some(having) = &query.having {
            let mut scope = Scope::aggregated(&rows, &groups, &measures);
            let predicate = self.expr(q, having, &mut scope)?;
            rows = q.filter(rows, predicate)?;
        }
        let items: Vec<_> = query
            .select
            .iter()
            .filter(|item| {
                self.dialect == SqlDialect::ClickHouse || !super::tail::versioning(&item.expr)
            })
            .collect();
        if let [
            ast::SelectExpr {
                expr: ast::Expr::Star,
                ..
            },
        ] = items.as_slice()
        {
            let rows = self.finish(q, query, rows, tail, &groups, &measures)?;
            return q.select_all(rows);
        }
        let mut computed = Vec::new();
        let mut scope = Scope::aggregated(&rows, &groups, &measures);
        for item in items.iter().filter(|item| computes(item)) {
            let value = self.expr(q, &item.expr, &mut scope)?;
            computed.push(Declare {
                name: item.alias.clone(),
                value,
            });
        }
        let (rows, outputs) = match computed.is_empty() {
            true => (rows, Vec::new()),
            false => q.compute(rows, computed)?,
        };
        let mut outputs = outputs.into_iter();
        let mut columns = Vec::new();
        let scope = Scope::aggregated(&rows, &groups, &measures);
        for item in items {
            columns.push(match (&item.expr, computes(item)) {
                (ast::Expr::Column { table, column }, false) => scope.qualified(table, column)?,
                _ => outputs.next().expect("one output per computed select item"),
            });
        }
        let rows = self.finish(q, query, rows, tail, &groups, &measures)?;
        q.select(rows, columns)
    }

    fn finish<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
        rows: Relation,
        tail: bool,
        groups: &[Binding],
        measures: &[Binding],
    ) -> Imported<Relation> {
        match tail {
            true => self.tail(q, query, rows, groups, measures),
            false => Ok(rows),
        }
    }

    pub(super) fn read_mode(&self, final_: bool) -> ReadMode {
        match (self.dialect, final_) {
            (SqlDialect::ClickHouse, false) => ReadMode::Raw,
            _ => ReadMode::Current,
        }
    }
}

fn computes(item: &ast::SelectExpr) -> bool {
    item.alias.is_some() || !matches!(item.expr, ast::Expr::Column { .. })
}
