use super::generic::{Expr, JoinKind, Node, Op, Program, ValueId};
use super::physical::Scalar;

#[derive(Clone, Copy, Debug)]
pub struct Estimate {
    pub rows: u64,
    pub work: u64,
}

pub fn estimate<S, E>(
    program: &Program<S, Scalar, E>,
    source: impl Fn(&S) -> Estimate,
    distinct_values: impl Fn(&S, ValueId) -> Option<u64>,
) -> Estimate {
    fn visit<S, E>(
        node: &Node<S, Scalar, E>,
        subplans: &[Estimate],
        source: &impl Fn(&S) -> Estimate,
        distinct_values: &impl Fn(&S, ValueId) -> Option<u64>,
    ) -> Estimate {
        let inputs = node
            .inputs
            .iter()
            .map(|input| visit(input, subplans, source, distinct_values))
            .collect::<Vec<_>>();
        let work = inputs
            .iter()
            .fold(0u64, |work, input| work.saturating_add(input.work));

        match &node.op {
            Op::Read(read) => source(read),
            Op::Reference { subplan, .. } => subplans[subplan.0],
            Op::Filter(predicate) => {
                let rows = match predicate {
                    Expr::Bool(false) => 0,
                    Expr::Call {
                        function: Scalar::Equal,
                        arguments,
                    } => match (&node.inputs[0].op, arguments.as_slice()) {
                        (
                            Op::Read(read),
                            [Expr::Value(value), literal] | [literal, Expr::Value(value)],
                        ) if matches!(
                            literal,
                            Expr::Bool(_)
                                | Expr::Int64(_)
                                | Expr::UInt64(_)
                                | Expr::Float64(_)
                                | Expr::String(_)
                        ) =>
                        {
                            inputs[0]
                                .rows
                                .div_ceil(distinct_values(read, *value).unwrap_or(1).max(1))
                        }
                        _ => inputs[0].rows,
                    },
                    _ => inputs[0].rows,
                };
                Estimate {
                    rows,
                    work: work.saturating_add(inputs[0].rows),
                }
            }
            Op::Join { kind, .. } => {
                let left = inputs[0].rows;
                let right = inputs[1].rows;
                let rows = match kind {
                    JoinKind::Semi => left.min(right),
                    JoinKind::Anti => left,
                    JoinKind::Inner => left.max(right).min(left.saturating_mul(right)),
                };
                Estimate {
                    rows,
                    work: work
                        .saturating_add(left)
                        .saturating_add(right.saturating_mul(2))
                        .saturating_add(rows),
                }
            }
            Op::Aggregate { groups, .. } => Estimate {
                rows: if groups.is_empty() { 1 } else { inputs[0].rows },
                work: work.saturating_add(inputs[0].rows),
            },
            Op::Union { .. } => Estimate {
                rows: inputs
                    .iter()
                    .fold(0u64, |rows, input| rows.saturating_add(input.rows)),
                work,
            },
            Op::Limit(limit) => Estimate {
                rows: inputs[0].rows.min(u64::from(*limit)),
                work,
            },
            Op::Project(_) | Op::Sort(_) => Estimate {
                rows: inputs[0].rows,
                work: work.saturating_add(inputs[0].rows),
            },
            Op::Extension(_) => Estimate {
                rows: u64::MAX,
                work: u64::MAX,
            },
        }
    }

    let mut subplans = Vec::new();
    for subplan in &program.subplans {
        subplans.push(visit(subplan, &subplans, &source, &distinct_values));
    }

    visit(&program.root, &subplans, &source, &distinct_values)
}
