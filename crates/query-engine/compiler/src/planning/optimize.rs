use crate::error::Result;

use super::generic::{Expr, JoinKind, Node, Op, Operation, Program, SubplanId, Values};
use super::physical::Scalar;

pub struct Candidate<S, E> {
    pub program: Program<S, Scalar, E>,
    pub values: Values,
}

pub fn join_candidates<S: Operation + Clone, E: Operation + Clone>(
    root: Node<S, Scalar, E>,
    values: Values,
) -> Result<Vec<Candidate<S, E>>> {
    root.output(&values)?;
    let mut candidates = Vec::new();

    if let Op::Join {
        kind: JoinKind::Inner,
        condition:
            Expr::Call {
                function: Scalar::Equal,
                arguments,
            },
    } = &root.op
        && let [Expr::Value(first), Expr::Value(second)] = arguments.as_slice()
    {
        let left = root.inputs[0].output(&values)?;
        let right = root.inputs[1].output(&values)?;
        let keys = if left.contains(first) && right.contains(second) {
            Some([*first, *second])
        } else if left.contains(second) && right.contains(first) {
            Some([*second, *first])
        } else {
            None
        };

        if let Some(keys) = keys {
            for producer in 0..2 {
                let consumer = 1 - producer;
                let schema = root.inputs[producer].output(&values)?;
                let mut candidate_values = values.clone();
                let key = candidate_values.allocate(values.data_type(keys[producer])?.clone());
                let reference = |exports| Node {
                    op: Op::Reference {
                        subplan: SubplanId(0),
                        exports,
                    },
                    inputs: vec![],
                };
                let mut inputs = root.inputs.clone();
                inputs[producer] =
                    reference(schema.into_iter().map(|value| (value, value)).collect());
                inputs[consumer] = Node {
                    op: Op::Join {
                        kind: JoinKind::Semi,
                        condition: Expr::Call {
                            function: Scalar::Equal,
                            arguments: vec![Expr::Value(keys[consumer]), Expr::Value(key)],
                        },
                    },
                    inputs: vec![
                        inputs[consumer].clone(),
                        reference(vec![(keys[producer], key)]),
                    ],
                };

                let program = Program {
                    subplans: vec![root.inputs[producer].clone()],
                    root: Node {
                        op: root.op.clone(),
                        inputs,
                    },
                };
                program.output(&candidate_values)?;
                candidates.push(Candidate {
                    program,
                    values: candidate_values,
                });
            }
        }
    }

    candidates.insert(
        0,
        Candidate {
            program: Program {
                subplans: vec![],
                root,
            },
            values,
        },
    );
    Ok(candidates)
}

pub fn select<S: Operation, E: Operation, Cost: Ord>(
    candidates: Vec<Candidate<S, E>>,
    mut cost: impl FnMut(&Program<S, Scalar, E>) -> Cost,
) -> Result<Option<Candidate<S, E>>> {
    let mut best = None;

    for candidate in candidates {
        candidate.program.output(&candidate.values)?;
        let score = cost(&candidate.program);

        if best.as_ref().is_none_or(|(_, previous)| score < *previous) {
            best = Some((candidate, score));
        }
    }

    Ok(best.map(|(candidate, _)| candidate))
}
