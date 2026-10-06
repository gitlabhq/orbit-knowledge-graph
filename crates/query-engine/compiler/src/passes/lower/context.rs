use crate::ast::{Cte, Expr, Query, SelectExpr, TableRef};
use crate::config::BindingNames;
use crate::error::{QueryError, Result};
use query_data_model::bindings::{
    ColumnRef, DefinitionId, QueryBindings, RelationId, RelationSource, ScopeId,
};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

pub(crate) struct LoweringContext<'a, M: QueryDataModel + ?Sized> {
    pub model: &'a M,
    pub bindings: &'a mut QueryBindings,
    pub names: &'a mut BindingNames,
}

impl<M: QueryDataModel + ?Sized> LoweringContext<'_, M> {
    pub fn scope(&mut self, parent: ScopeId) -> Result<ScopeId> {
        self.bindings
            .scope(parent)
            .map_err(|error| QueryError::Lowering(error.to_string()))
    }

    pub fn scan(
        &mut self,
        scope: ScopeId,
        table: &str,
        hint: &str,
        final_: bool,
    ) -> Result<(TableRef, RelationId)> {
        let storage = self.model.query_backend().storage();
        let table = storage
            .resolve_table(table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let relation = self
            .bindings
            .scan(storage, scope, table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names
            .tables
            .insert(table, storage.table(table).name().into());
        self.names.relation_name(self.bindings, relation, hint)?;
        for (export, column) in self
            .bindings
            .exports(relation)
            .expect("scan exports")
            .iter()
            .zip(storage.table(table).columns())
        {
            self.names.exports.insert(*export, column.name().into());
        }
        Ok((
            TableRef::Scan {
                relation,
                final_,
                relationship: None,
            },
            relation,
        ))
    }

    pub fn column(&self, scope: ScopeId, relation: RelationId, name: &str) -> Result<ColumnRef> {
        stored_column(self.model, self.bindings, scope, relation, name)
    }

    pub fn select(&mut self, scope: ScopeId, expression: Expr, name: &str) -> Result<SelectExpr> {
        select(self.bindings, self.names, scope, expression, name)
    }

    pub fn project(
        &mut self,
        query: &mut Query,
        relation: RelationId,
        column: &str,
        name: &str,
    ) -> Result<()> {
        let column = self.column(query.scope, relation, column)?;
        query
            .select
            .push(self.select(query.scope, Expr::Column(column), name)?);
        Ok(())
    }

    pub fn derived(
        &mut self,
        parent: ScopeId,
        query: Query,
        hint: &str,
    ) -> Result<(TableRef, RelationId)> {
        let relation = self
            .bindings
            .derived(parent, query.scope)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(self.bindings, relation, hint)?;
        Ok((TableRef::subquery(query, relation), relation))
    }

    pub fn union(
        &mut self,
        parent: ScopeId,
        queries: Vec<Query>,
        hint: &str,
    ) -> Result<(TableRef, RelationId)> {
        let scopes = queries.iter().map(|query| query.scope).collect::<Vec<_>>();
        let relation = self
            .bindings
            .union(parent, &scopes)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(self.bindings, relation, hint)?;
        Ok((TableRef::union_all(queries, relation), relation))
    }

    pub fn define(&mut self, parent: ScopeId, query: Query, hint: &str) -> Result<Cte> {
        let definition = self
            .bindings
            .define(parent, query.scope)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.definition_name(definition, hint);
        Ok(Cte::new(&definition, query))
    }

    pub fn reference(
        &mut self,
        scope: ScopeId,
        definition: DefinitionId,
        hint: &str,
    ) -> Result<(TableRef, RelationId)> {
        let relation = self
            .bindings
            .reference(scope, definition)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(self.bindings, relation, hint)?;
        Ok((TableRef::Cte { relation }, relation))
    }

    pub fn membership(
        &mut self,
        scope: ScopeId,
        column: ColumnRef,
        definition: DefinitionId,
    ) -> Result<Expr> {
        let relation = self
            .bindings
            .reference(scope, definition)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let export = *self
            .bindings
            .exports(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
            .first()
            .ok_or_else(|| QueryError::Lowering("membership definition has no output".into()))?;
        Ok(Expr::InSubquery {
            expr: Box::new(Expr::Column(column)),
            cte_name: definition,
            column: export,
        })
    }

    pub fn deletion(&self, scope: ScopeId, relation: RelationId) -> Result<Option<Expr>> {
        let RelationSource::Scan(table) = self
            .bindings
            .source(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
        else {
            return Err(QueryError::Lowering(
                "deletion metadata requires a stored scan".into(),
            ));
        };
        let query_data_model::storage::RowSemantics::Versioned {
            deletion: Some(deletion),
            ..
        } = self
            .model
            .query_backend()
            .storage()
            .table(*table)
            .row_semantics()
        else {
            return Ok(None);
        };
        let column = self
            .bindings
            .stored_column(
                scope,
                relation,
                query_data_model::storage::StoredColumnRef {
                    table: *table,
                    column: deletion.column,
                },
            )
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        Ok(Some(super::sql::deleted_false(column)))
    }

    pub fn edge_scan(
        &mut self,
        scope: ScopeId,
        tables: &[String],
        hint: &str,
    ) -> Result<(TableRef, RelationId)> {
        if let [table] = tables {
            return self.scan(scope, table, hint, false);
        }
        let mut arms = Vec::new();
        for table in tables {
            let body = self.scope(scope)?;
            let (from, relation) = self.scan(body, table, hint, false)?;
            let mut query = Query::new(body, from);
            for column in ontology::EDGE_RESERVED_COLUMNS {
                self.project(&mut query, relation, column, column)?;
            }
            query.where_clause = self.deletion(body, relation)?;
            arms.push(query);
        }
        self.union(scope, arms, hint)
    }
}

pub(crate) fn stored_column(
    model: &(impl QueryDataModel + ?Sized),
    bindings: &QueryBindings,
    scope: ScopeId,
    relation: RelationId,
    name: &str,
) -> Result<ColumnRef> {
    let storage = model.query_backend().storage();
    let exports = bindings
        .exports(relation)
        .map_err(|error| QueryError::Lowering(error.to_string()))?;
    let mut matches = exports.iter().filter(|export| {
        matches!(bindings.origin(**export), Ok(query_data_model::bindings::ExportOrigin::Stored(column)) if storage.column(column).name() == name)
    });
    let export = *matches
        .next()
        .ok_or_else(|| QueryError::Lowering(format!("relation has no stored column '{name}'")))?;
    if matches.next().is_some() {
        return Err(QueryError::Lowering(format!(
            "ambiguous stored column '{name}'"
        )));
    }
    bindings
        .column(scope, relation, export)
        .map_err(|error| QueryError::Lowering(error.to_string()))
}

pub(crate) fn select(
    bindings: &mut QueryBindings,
    names: &mut BindingNames,
    scope: ScopeId,
    expression: Expr,
    name: impl Into<String>,
) -> Result<SelectExpr> {
    let export = match expression {
        Expr::Column(column) => bindings.project_column(scope, column),
        _ => bindings.project(scope),
    }
    .map_err(|error| QueryError::Lowering(error.to_string()))?;
    names.exports.insert(export, name.into());
    Ok(SelectExpr::exporting(expression, &export))
}
