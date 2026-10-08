use super::{PathScopeId, QueryScope, ScopeSource};
use crate::error::Result;
use crate::input::Input;
use crate::query_graph::api::{
    Aggregate, Expr, Function, LoweredGraph, OperationKind, QueryId, Read, lit,
};
use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;

pub fn apply_graph<'a, M: QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    scope: &QueryScope,
    input: &Input,
) -> Result<LoweredGraph<'a, M>> {
    graph.rewrite(root, |q, rows| {
        let OperationKind::Scan {
            table,
            label: Some(label),
            ..
        } = rows.kind()
        else {
            return Ok(rows);
        };
        let proof = if input.nodes.iter().any(|node| node.id == *label)
            && q.catalog().table_path_scopable(table.name())
        {
            scope.nodes.get(label)
        } else {
            label
                .strip_prefix('e')
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| scope.relationships.get(index))
                .and_then(Option::as_ref)
        };
        let Some(proof) = proof else {
            return Ok(rows);
        };
        let column = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        let required = scope
            .requirements
            .iter()
            .any(|requirement| requirement.sources == proof.sources);
        let mut predicates = Vec::new();
        for source in &proof.sources {
            let path = match source {
                ScopeSource::Literal(path) => lit(path.clone()),
                ScopeSource::Lookup {
                    source_table,
                    key_column,
                    value,
                } => lookup_path(q, source_table, key_column, value)?,
            };
            let mut predicate = column.starts_with(path.clone());
            if let Some((min, max)) = proof.depth {
                predicate = if min == 0 && max == 0 {
                    column.eq(path.clone())
                } else {
                    let depth = Expr::call(Function::PathDepth, [column.expr()]);
                    let base = Expr::call(Function::PathDepth, [path.clone()]);
                    predicate
                        .and(depth.clone().ge(base.clone().add(i64::from(min))))
                        .and(depth.le(base.add(i64::from(max))))
                };
            }
            predicates.push(if required {
                predicate.and(path.ne(super::UNRESOLVED_PATH))
            } else {
                predicate.or(path.eq(super::UNRESOLVED_PATH))
            });
        }
        Ok(q.filter(
            rows,
            predicates
                .into_iter()
                .reduce(Expr::or)
                .unwrap_or_else(|| lit(false)),
        )?)
    })
}

fn lookup_path<'a, M: QueryDataModel + ?Sized>(
    q: &mut crate::query_graph::QueryScope<'_, 'a, M>,
    table: &str,
    key: &str,
    value: &PathScopeId,
) -> crate::query_graph::Result<Expr> {
    let lookup = q.subquery(|q| {
        let (read, value) = match value {
            PathScopeId::Numeric(value) => (Read::Raw, lit(*value)),
            PathScopeId::Text(value) => (Read::Current, lit(value.clone())),
        };
        let rows = q.scan(table, read)?;
        let predicate = rows.column(key)?.eq(value);
        let path = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        let deleted = rows.column(ontology::DELETED_COLUMN)?;
        let version = rows.column(ontology::VERSION_COLUMN)?;
        let rows = q.filter(rows, predicate)?;
        let rows = q.aggregate(
            rows,
            [],
            [
                Expr::aggregate(Aggregate::ArgMax, [path.expr(), version.expr()]).named("path"),
                Expr::aggregate(Aggregate::ArgMax, [deleted.expr(), version.expr()])
                    .named("deleted"),
            ],
        )?;
        let value = Expr::call(
            Function::If,
            [
                rows.column("deleted")?.expr(),
                Expr::literal(SqlType::String, serde_json::Value::Null),
                rows.column("path")?.expr(),
            ],
        );
        let value = Expr::call(Function::Coalesce, [value, lit(super::UNRESOLVED_PATH)]);
        q.select(rows, [value.named("path")])
    })?;
    q.scalar(lookup, "path")
}
