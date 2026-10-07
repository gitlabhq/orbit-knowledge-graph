use super::*;
use std::convert::Infallible;

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub fn lower_operations(self) -> QueryGraph<'a, M, Infallible> {
        let blocks = self
            .blocks
            .into_iter()
            .map(|block| Block {
                owner: block.owner,
                visible: block.visible,
                required: block.required,
                definitions: block.definitions,
                relations: block.relations,
                operation: block.operation.map(|operation| QueryOperation {
                    block: operation.block,
                    outputs: operation.outputs,
                    kind: match operation.kind {
                        QueryKind::Project(input) => QueryKind::Project(lower(input)),
                        QueryKind::UnionAll(arms) => QueryKind::UnionAll(arms),
                    },
                }),
            })
            .collect();
        QueryGraph {
            catalog: self.catalog,
            owner: self.owner,
            blocks,
        }
    }
}

fn lower(operation: PhysicalOperation<'_>) -> LoweredOperation<'_> {
    use OperationKind::*;
    let Relational {
        block,
        kind,
        columns,
        occurrences,
        expanded,
    } = operation;
    let kind = match kind {
        One => One,
        Source { relation, read } => Source { relation, read },
        Filter { input, predicate } => Filter {
            input: Box::new(lower(*input)),
            predicate,
        },
        Join {
            left,
            right,
            kind,
            condition,
        } => Join {
            left: Box::new(lower(*left)),
            right: Box::new(lower(*right)),
            kind,
            condition,
        },
        Aggregate { input, groups } => Aggregate {
            input: Box::new(lower(*input)),
            groups,
        },
        Expand { input, column } => Expand {
            input: Box::new(lower(*input)),
            column,
        },
        Materialize { input, relation } => Materialize {
            input: Box::new(lower(*input)),
            relation,
        },
        Sort { input, keys } => Sort {
            input: Box::new(lower(*input)),
            keys,
        },
        FirstBy { input, keys } => FirstBy {
            input: Box::new(lower(*input)),
            keys,
        },
        Limit { input, count } => Limit {
            input: Box::new(lower(*input)),
            count,
        },
        Latest {
            input,
            requirement:
                LatestRows {
                    version,
                    keys,
                    deletion,
                },
        } => {
            let order = keys
                .iter()
                .map(|column| (*column, false))
                .chain([(version, true)])
                .collect();
            let sorted = lower(*input).wrap(|input| Sort { input, keys: order });
            let latest = sorted.wrap(|input| FirstBy { input, keys });
            return if let Some(deleted) = deletion {
                latest.wrap(|input| Filter {
                    input,
                    predicate: Expression::equal(
                        Expression::Column(deleted),
                        Expression::Boolean(false),
                    ),
                })
            } else {
                latest
            };
        }
    };
    Relational {
        block,
        kind,
        columns,
        occurrences,
        expanded,
    }
}
