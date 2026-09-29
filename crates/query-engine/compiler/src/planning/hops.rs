use query_data_model::QueryDataModel;

use super::bind::{Plan, bind_filter, call, dynamic_edges};
use super::generic::{Assignment, Expr, JoinKind, Node, Op, Schema, ValueType, Values};
use super::physical::Scalar;
use crate::error::{QueryError, Result};
use crate::input::InputRelationship;

pub fn bind(
    relationship: &InputRelationship,
    index: usize,
    endpoints: (&str, &str),
    model: &impl QueryDataModel,
    values: &mut Values,
) -> Result<(Plan, Schema)> {
    if relationship.hops.min == 0
        || relationship.hops.max > 3
        || relationship.hops.min > relationship.hops.max
    {
        return Err(QueryError::Validation(
            "traversal hops must be within 1..3".into(),
        ));
    }
    let mut inputs = Vec::new();
    let mut arms = Vec::new();
    for depth in relationship.hops.min..=relationship.hops.max {
        let mut chain = None;
        let mut first = None;
        let mut previous = None;
        let mut path = Vec::new();
        for hop in 0..depth {
            let (mut source, edge) = dynamic_edges(&relationship.types, model, values)?;
            if let super::bind::Source::Edge { relationship, .. } = &mut source {
                *relationship = index;
            }
            let read = Node {
                op: Op::Read(source),
                inputs: vec![],
            };
            let root = if let (Some(left), Some((id, kind))) = (chain.take(), previous) {
                Node {
                    op: Op::Join {
                        kind: JoinKind::Inner,
                        condition: call(
                            Scalar::And,
                            vec![
                                call(Scalar::Equal, vec![Expr::Value(id), Expr::Value(edge[0])]),
                                call(Scalar::Equal, vec![Expr::Value(kind), Expr::Value(edge[2])]),
                            ],
                        ),
                    },
                    inputs: vec![left, read],
                }
            } else {
                first = Some(edge);
                path.push(call(
                    Scalar::Record,
                    vec![Expr::Value(edge[0]), Expr::Value(edge[2])],
                ));
                read
            };
            let mut predicates = Vec::new();
            if hop == 0 {
                predicates.push(call(
                    Scalar::Equal,
                    vec![Expr::Value(edge[2]), Expr::String(endpoints.0.into())],
                ));
            }
            if hop + 1 == depth {
                predicates.push(call(
                    Scalar::Equal,
                    vec![Expr::Value(edge[3]), Expr::String(endpoints.1.into())],
                ));
            }
            let mut filters: Vec<_> = relationship.filters.iter().collect();
            filters.sort_unstable_by_key(|(name, _)| *name);
            for (name, filters) in filters {
                let position = [
                    "source_id",
                    "target_id",
                    "source_kind",
                    "target_kind",
                    "relationship_kind",
                ]
                .iter()
                .position(|field| field == name)
                .ok_or_else(|| {
                    QueryError::ReferenceError(format!("unsupported hop property {name}"))
                })?;
                for filter in filters {
                    predicates.push(bind_filter(edge[position], filter, values)?);
                }
            }
            chain = Some(
                match predicates
                    .into_iter()
                    .reduce(|left, right| call(Scalar::And, vec![left, right]))
                {
                    Some(predicate) => Node {
                        op: Op::Filter(predicate),
                        inputs: vec![root],
                    },
                    None => root,
                },
            );
            path.push(call(
                Scalar::Record,
                vec![Expr::Value(edge[1]), Expr::Value(edge[3])],
            ));
            previous = Some((edge[1], edge[3]));
        }
        let first = first.expect("positive hop depth");
        let (last_id, last_kind) = previous.expect("positive hop depth");
        let expressions = vec![
            Expr::Value(first[0]),
            Expr::Value(last_id),
            Expr::Value(first[2]),
            Expr::Value(last_kind),
            Expr::Value(first[4]),
            call(Scalar::List, path),
            Expr::Int64(i64::from(depth)),
        ];
        let root = chain.expect("positive hop depth");
        let schema = root.output(values)?;
        let assignments = expressions
            .into_iter()
            .map(|expression| {
                Ok(Assignment {
                    output: values.allocate(expression.data_type(&schema, values)?),
                    expression,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        arms.push(
            assignments
                .iter()
                .map(|assignment| assignment.output)
                .collect(),
        );
        inputs.push(Node {
            op: Op::Project(assignments),
            inputs: vec![root],
        });
    }
    let outputs = [
        ValueType::Int64,
        ValueType::Int64,
        ValueType::String,
        ValueType::String,
        ValueType::String,
        ValueType::List(Box::new(ValueType::Record(vec![
            ValueType::Int64,
            ValueType::String,
        ]))),
        ValueType::Int64,
    ]
    .map(|kind| values.allocate(kind))
    .to_vec();
    Ok((
        Node {
            op: Op::Union {
                outputs: outputs.clone(),
                arms,
            },
            inputs,
        },
        outputs,
    ))
}
