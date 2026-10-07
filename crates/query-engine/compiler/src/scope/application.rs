use query_data_model::QueryDataModel;

use super::QueryScope;
use crate::error::Result;

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
            let proof = match declaration.input {
                Some(ScanInput::Node(index))
                    if graph.catalog().table_path_scopable(table.name()) =>
                {
                    scope.nodes.get(&input.nodes[index].id)
                }
                Some(ScanInput::Relationship(index)) => {
                    scope.relationships.get(index).and_then(Option::as_ref)
                }
                _ => None,
            };
            let Some(proof) = proof else { continue };
            let column = graph.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?;
            let mut predicates = Vec::new();
            let required = scope
                .requirements
                .iter()
                .any(|requirement| requirement.sources == proof.sources);
            for source in &proof.sources {
                let path = match source {
                    super::ScopeSource::Literal(path) => E::Text(path.clone()),
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
                        E::ScalarQuery(graph.output_column(scope_relation, output)?)
                    }
                };
                let mut predicate =
                    E::StartsWith(Box::new(E::Column(column)), Box::new(path.clone()));
                if let Some((min, max)) = proof.depth {
                    if min == 0 && max == 0 {
                        predicate = E::equal(E::Column(column), path.clone());
                    } else {
                        let depth = E::PathDepth(Box::new(E::Column(column)));
                        let bound = |hops| {
                            E::Add(
                                Box::new(E::PathDepth(Box::new(path.clone()))),
                                Box::new(E::Integer(i64::from(hops))),
                            )
                        };
                        predicate = E::And(
                            Box::new(predicate),
                            Box::new(E::And(
                                Box::new(E::GreaterEqual(
                                    Box::new(depth.clone()),
                                    Box::new(bound(min)),
                                )),
                                Box::new(E::LessEqual(Box::new(depth), Box::new(bound(max)))),
                            )),
                        );
                    }
                }
                let unresolved = E::Text(super::UNRESOLVED_PATH.into());
                predicates.push(if required {
                    E::And(
                        Box::new(predicate),
                        Box::new(E::Predicate {
                            operator: crate::input::FilterOp::Ne,
                            value: Box::new(path),
                            argument: Some(Box::new(unresolved)),
                            fold_case: false,
                        }),
                    )
                } else {
                    E::Or(Box::new(predicate), Box::new(E::equal(path, unresolved)))
                });
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
            let operation = std::mem::replace(scan, Operation::One);
            *scan = operation.filter(predicate);
        }
    }
    Ok(())
}
