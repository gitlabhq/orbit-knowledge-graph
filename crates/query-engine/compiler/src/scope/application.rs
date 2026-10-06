use query_data_model::QueryDataModel;

use super::{QueryScope, resolved_scope_guard, scope_predicate};
use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Node, TableRef};
use crate::error::Result;

pub fn apply(
    node: &mut Node,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        apply_scan_predicates(&query.from, &mut query.where_clause, scope, model);
        Ok(())
    })?;
    for requirement in &scope.requirements {
        append_predicate(&mut query.where_clause, resolved_scope_guard(requirement));
    }
    Ok(())
}

pub fn apply_graph<'a, M: QueryDataModel + ?Sized>(
    graph: &mut crate::query_graph::QueryGraph<
        'a,
        M,
        crate::query_graph::Expression<'a>,
        crate::query_graph::LoweredOperation<'a>,
    >,
    scope: &QueryScope,
    input: &crate::input::Input,
) -> Result<()> {
    use crate::query_graph::{Expression as E, LoweredOperation as Operation, ScanInput, Source};
    if !scope.requirements.is_empty() {
        return Err(crate::error::QueryError::Lowering(
            "graph scope lookup requirements are not implemented".into(),
        ));
    }
    let blocks = graph.blocks().collect::<Vec<_>>();
    for block in blocks {
        let Ok(relations) = graph.relations(block) else {
            continue;
        };
        let relations = relations.collect::<Vec<_>>();
        for relation in relations {
            let declaration = graph.relation(relation)?;
            let Source::Stored(table) = declaration.source else {
                continue;
            };
            if !graph.catalog().table_path_scopable(table.name()) {
                continue;
            }
            let proof = match declaration.input {
                Some(ScanInput::Node(index)) => scope.nodes.get(&input.nodes[index].id),
                Some(ScanInput::Relationship(index)) => {
                    scope.relationships.get(index).and_then(Option::as_ref)
                }
                None => None,
            };
            let Some(proof) = proof else { continue };
            if proof.depth.is_some() {
                return Err(crate::error::QueryError::Lowering(
                    "graph scope depth is not implemented".into(),
                ));
            }
            let column = graph.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?;
            let mut predicates = Vec::new();
            let mut lookups = Vec::new();
            for source in &proof.sources {
                match source {
                    super::ScopeSource::Literal(path) => predicates.push(E::StartsWith(
                        Box::new(E::Column(column)),
                        Box::new(E::Text(path.clone())),
                    )),
                    super::ScopeSource::Lookup {
                        source_table,
                        key_column,
                        value,
                    } => {
                        let model = graph.catalog();
                        let table = model
                            .ontology()
                            .nodes()
                            .find_map(|entity| {
                                model
                                    .entity_table(&entity.name)
                                    .filter(|table| *table == source_table)
                            })
                            .ok_or_else(|| {
                                crate::error::QueryError::Lowering(format!(
                                    "unknown scope table {source_table}"
                                ))
                            })?;
                        let lookup = graph.select(Operation::One);
                        let scan = graph.scan(lookup, table, super::LOOKUP_ALIAS)?;
                        let key = graph.stored_column(scan, key_column)?;
                        let version =
                            graph.stored_column(scan, ontology::constants::VERSION_COLUMN)?;
                        let deleted =
                            graph.stored_column(scan, ontology::constants::DELETED_COLUMN)?;
                        let (operation, value) = match value {
                            super::PathScopeId::Numeric(value) => {
                                (Operation::source(scan), E::Integer(*value))
                            }
                            super::PathScopeId::Text(value) => {
                                (Operation::current(scan), E::Text(value.clone()))
                            }
                        };
                        *graph.operation_mut(lookup)? = operation
                            .filter(E::equal(E::Column(key), value))
                            .aggregate(vec![]);
                        graph.project(
                            lookup,
                            ontology::TRAVERSAL_PATH_COLUMN,
                            E::LatestPath {
                                path: graph.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?,
                                version,
                                deletion: deleted,
                            },
                        )?;
                        let output = graph.outputs(lookup)?.next().expect("scope path output");
                        let scope_relation = graph.derive(block, lookup, super::LOOKUP_ALIAS)?;
                        let path = graph.output_column(scope_relation, output)?;
                        predicates.push(E::Or(
                            Box::new(E::StartsWith(
                                Box::new(E::Column(column)),
                                Box::new(E::Column(path)),
                            )),
                            Box::new(E::equal(
                                E::Column(path),
                                E::Text(super::UNRESOLVED_PATH.into()),
                            )),
                        ));
                        lookups.push(scope_relation);
                    }
                }
            }
            let predicate = predicates
                .into_iter()
                .reduce(|a, b| E::Or(Box::new(a), Box::new(b)))
                .unwrap_or(E::Boolean(false));
            let scan = graph
                .operation_mut(block)?
                .source_mut(relation)
                .ok_or_else(|| {
                    crate::error::QueryError::Lowering("scope scan missing from operation".into())
                })?;
            let mut operation = std::mem::replace(scan, Operation::One);
            for lookup in lookups {
                operation = Operation::Join {
                    left: Box::new(operation),
                    right: Box::new(Operation::source(lookup)),
                    kind: crate::query_graph::JoinKind::Cross,
                    condition: E::Boolean(true),
                };
            }
            *scan = operation.filter(predicate);
        }
    }
    Ok(())
}

fn apply_scan_predicates(
    table: &TableRef,
    target: &mut Option<Expr>,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
) {
    visit_relations(table, &mut |relation| {
        if let TableRef::Scan {
            table,
            alias,
            relationship,
            ..
        } = relation
        {
            let proof = match relationship {
                Some(index) => scope.relationships.get(*index).and_then(Option::as_ref),
                None if model.table_path_scopable(table) => scope.nodes.get(alias),
                None => None,
            };
            if let Some(proof) = proof {
                append_predicate(target, scope_predicate(proof, alias));
            }
        }
    });
}

fn append_predicate(target: &mut Option<Expr>, predicate: Expr) {
    *target = Some(match target.take() {
        Some(existing) => Expr::and(existing, predicate),
        None => predicate,
    });
}
