use crate::error::Result;

use super::generic::{Expr, JoinKind, Node, Op, Operation, Program, SubplanId, Values};
use super::physical::Scalar;

#[derive(Clone)]
pub struct Candidate<S, E> {
    pub program: Program<S, Scalar, E>,
    pub values: Values,
}

pub fn join_candidates<S: Operation + Clone, E: Operation + Clone>(
    root: Node<S, Scalar, E>,
    values: Values,
) -> Result<Vec<Candidate<S, E>>> {
    root.output(&values)?;
    enumerate(root, values, Vec::new())
}

fn enumerate<S: Operation + Clone, E: Operation + Clone>(
    root: Node<S, Scalar, E>,
    values: Values,
    subplans: Vec<Node<S, Scalar, E>>,
) -> Result<Vec<Candidate<S, E>>> {
    let mut combinations = vec![(Vec::new(), values, subplans)];

    for input in root.inputs {
        let mut next = Vec::new();

        for (inputs, values, subplans) in combinations {
            for candidate in enumerate(input.clone(), values, subplans)? {
                let mut inputs = inputs.clone();
                inputs.push(candidate.program.root);
                next.push((inputs, candidate.values, candidate.program.subplans));
            }
        }

        combinations = next;
    }

    let mut result = Vec::new();
    for (inputs, values, subplans) in combinations {
        result.extend(alternatives(
            Node {
                op: root.op.clone(),
                inputs,
            },
            values,
            subplans,
        )?);
    }

    Ok(result)
}

fn alternatives<S: Operation + Clone, E: Operation + Clone>(
    root: Node<S, Scalar, E>,
    values: Values,
    subplans: Vec<Node<S, Scalar, E>>,
) -> Result<Vec<Candidate<S, E>>> {
    let mut candidates = Vec::new();
    let mut schemas = Vec::new();

    for subplan in &subplans {
        schemas.push(subplan.output_with(&values, &schemas)?);
    }

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
        let left = root.inputs[0].output_with(&values, &schemas)?;
        let right = root.inputs[1].output_with(&values, &schemas)?;
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
                let schema = root.inputs[producer].output_with(&values, &schemas)?;
                let mut candidate_values = values.clone();
                let key = candidate_values.allocate(values.data_type(keys[producer])?.clone());
                let reference = |exports| Node {
                    op: Op::Reference {
                        subplan: SubplanId(subplans.len()),
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

                let mut definitions = subplans.clone();
                definitions.push(root.inputs[producer].clone());
                let program = Program {
                    subplans: definitions,
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
            program: Program { subplans, root },
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

pub fn estimated_work<S, E>(
    program: &Program<S, Scalar, E>,
    source_work: impl Fn(&S) -> u64,
) -> (u64, usize) {
    fn work<S, E>(
        node: &Node<S, Scalar, E>,
        subplans: &[u64],
        source_work: &impl Fn(&S) -> u64,
    ) -> u64 {
        let own = match &node.op {
            Op::Read(source) => source_work(source),
            Op::Reference { subplan, .. } => subplans[subplan.0],
            _ => 1,
        };

        node.inputs.iter().fold(own, |total, input| {
            total.saturating_add(work(input, subplans, source_work))
        })
    }

    let mut subplans = Vec::new();
    for subplan in &program.subplans {
        subplans.push(work(subplan, &subplans, &source_work));
    }

    (
        work(&program.root, &subplans, &source_work),
        program.subplans.len(),
    )
}
