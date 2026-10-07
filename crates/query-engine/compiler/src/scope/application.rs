use query_data_model::QueryDataModel;
use std::convert::Infallible;

use super::{PathScopeId, QueryScope, ScopeSource};
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::query_graph::{
    BlockId, Expression, OperationKind, QueryGraph, ReadMode, ScanInput, Source,
};

pub fn apply_graph<'a, M: QueryDataModel + ?Sized>(
    graph: QueryGraph<'a, M, Infallible>,
    root: BlockId,
    scope: &QueryScope,
    input: &Input,
) -> Result<QueryGraph<'a, M, Infallible>> {
    graph.rewrite_operations(root, |graph, operation| {
        let OperationKind::Source { relation, .. } = operation.kind() else {
            return Ok(operation);
        };
        let relation = *relation;
        let declaration = graph.relation(relation)?;
        let Source::Stored(table) = declaration.source else {
            return Ok(operation);
        };
        let proof = match declaration.input {
            Some(ScanInput::Node(index)) if graph.catalog().table_path_scopable(table.name()) => {
                scope.nodes.get(&input.nodes[index].id)
            }
            Some(ScanInput::Relationship(index)) => {
                scope.relationships.get(index).and_then(Option::as_ref)
            }
            _ => None,
        };
        let Some(proof) = proof else {
            return Ok(operation);
        };
        let column = graph.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?;
        let required = scope
            .requirements
            .iter()
            .any(|requirement| requirement.sources == proof.sources);
        let mut predicates = Vec::new();
        for source in &proof.sources {
            let path = match source {
                ScopeSource::Literal(path) => Expression::Text(path.clone()),
                ScopeSource::Lookup {
                    source_table,
                    key_column,
                    value,
                } => lookup_path(graph, operation.block(), source_table, key_column, value)?,
            };
            let mut predicate = Expression::StartsWith(
                Box::new(Expression::Column(column)),
                Box::new(path.clone()),
            );
            if let Some((min, max)) = proof.depth {
                predicate = if min == 0 && max == 0 {
                    Expression::equal(Expression::Column(column), path.clone())
                } else {
                    let depth = Expression::PathDepth(Box::new(Expression::Column(column)));
                    let bound = |hops| {
                        Expression::Add(
                            Box::new(Expression::PathDepth(Box::new(path.clone()))),
                            Box::new(Expression::Integer(i64::from(hops))),
                        )
                    };
                    Expression::And(
                        Box::new(predicate),
                        Box::new(Expression::And(
                            Box::new(Expression::GreaterEqual(
                                Box::new(depth.clone()),
                                Box::new(bound(min)),
                            )),
                            Box::new(Expression::LessEqual(Box::new(depth), Box::new(bound(max)))),
                        )),
                    )
                };
            }
            let unresolved = Expression::Text(super::UNRESOLVED_PATH.into());
            predicates.push(if required {
                Expression::And(
                    Box::new(predicate),
                    Box::new(Expression::Predicate {
                        operator: crate::input::FilterOp::Ne,
                        value: Box::new(path),
                        argument: Some(Box::new(unresolved)),
                        fold_case: false,
                    }),
                )
            } else {
                Expression::Or(
                    Box::new(predicate),
                    Box::new(Expression::equal(path, unresolved)),
                )
            });
        }
        let predicate = predicates
            .into_iter()
            .reduce(|left, right| Expression::Or(Box::new(left), Box::new(right)))
            .unwrap_or(Expression::Boolean(false));
        Ok(graph.filter_relation(operation, predicate)?)
    })
}

fn lookup_path<'a, M: QueryDataModel + ?Sized>(
    graph: &mut QueryGraph<'a, M, Infallible>,
    parent: BlockId,
    table: &str,
    key: &str,
    value: &PathScopeId,
) -> Result<Expression<'a>> {
    let table = graph
        .catalog()
        .stored_table(table)
        .ok_or_else(|| QueryError::Lowering(format!("unknown scope table {table}")))?;
    let lookup = graph.query_in(parent)?;
    let scan = graph.scan_stored(lookup, table, super::LOOKUP_ALIAS)?;
    let (read, value) = match value {
        PathScopeId::Numeric(value) => (ReadMode::Raw, Expression::Integer(*value)),
        PathScopeId::Text(value) => (ReadMode::Current, Expression::Text(value.clone())),
    };
    let operation = graph.filter_relation(
        graph.read_relation(scan, read)?,
        Expression::equal(Expression::Column(graph.stored_column(scan, key)?), value),
    )?;
    let operation = graph.aggregate_relation(operation, vec![])?;
    let projection = graph.project_values(
        operation,
        [(
            ontology::TRAVERSAL_PATH_COLUMN.into(),
            Expression::LatestPath {
                path: graph.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?,
                version: graph.stored_column(scan, ontology::VERSION_COLUMN)?,
                deletion: graph.stored_column(scan, ontology::DELETED_COLUMN)?,
            },
        )],
    )?;
    graph.finish_query(projection)?;
    let output = graph
        .outputs(lookup)?
        .next()
        .ok_or(crate::query_graph::GraphError::EmptyProjection)?;
    Ok(graph.scalar_query(parent, output, super::LOOKUP_ALIAS)?)
}
