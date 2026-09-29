use std::convert::Infallible;

use super::Scan;
use crate::error::Result;
use crate::planning::generic::{Assignment, Expr, JoinKind, Node, Op, ValueId};
use crate::planning::optimize::Candidate;
use crate::planning::physical::Scalar;

type Plan = Node<Scan, Scalar, Infallible>;

struct Input<'a> {
    scan: &'a Scan,
    exports: Vec<Assignment<Scalar>>,
    predicates: Vec<Expr<Scalar>>,
}

fn substitute(expression: &Expr<Scalar>, bindings: &[Assignment<Scalar>]) -> Expr<Scalar> {
    let mut result = expression.clone();
    result.visit_mut(&mut |expression| {
        if let Expr::Value(value) = expression {
            *expression = bindings
                .iter()
                .find(|assignment| assignment.output == *value)
                .expect("verified input value")
                .expression
                .clone();
        }
    });
    result
}

fn input(node: &Plan) -> Option<Input<'_>> {
    match &node.op {
        Op::Read(scan) => Some(Input {
            scan,
            exports: scan
                .read
                .columns
                .iter()
                .map(|(value, _)| Assignment {
                    output: *value,
                    expression: Expr::Value(*value),
                })
                .collect(),
            predicates: vec![],
        }),
        Op::Project(assignments) => {
            let mut input = input(&node.inputs[0])?;
            input.exports = assignments
                .iter()
                .map(|assignment| Assignment {
                    output: assignment.output,
                    expression: substitute(&assignment.expression, &input.exports),
                })
                .collect();
            Some(input)
        }
        Op::Filter(predicate) => {
            let mut input = input(&node.inputs[0])?;
            input.predicates.push(substitute(predicate, &input.exports));
            Some(input)
        }
        _ => None,
    }
}

pub fn fuse_holder(
    candidate: &Candidate<Scan, Scalar, Infallible>,
) -> Result<Vec<Candidate<Scan, Scalar, Infallible>>> {
    let root = &candidate.program.root;
    let Op::Join {
        kind: JoinKind::Inner,
        condition,
    } = &root.op
    else {
        return Ok(vec![]);
    };
    let (Some(left), Some(right)) = (input(&root.inputs[0]), input(&root.inputs[1])) else {
        return Ok(vec![]);
    };
    let a = left.scan;
    let b = right.scan;

    if a.read.table != b.read.table
        || a.read.current_rows != b.read.current_rows
        || a.layout != b.layout
        || a.layout.current_row_key().is_empty()
        || (a.binding.is_some() && b.binding.is_some() && a.binding != b.binding)
        || (a.relationship.is_some()
            && b.relationship.is_some()
            && a.relationship != b.relationship)
        || a.foreign_key.is_some()
        || b.foreign_key.is_some()
    {
        return Ok(vec![]);
    }

    let exports: Vec<_> = left.exports.into_iter().chain(right.exports).collect();
    let condition = substitute(condition, &exports);
    let mut equalities = Vec::new();
    fn conjuncts(expression: &Expr<Scalar>, equalities: &mut Vec<(Expr<Scalar>, Expr<Scalar>)>) {
        match expression {
            Expr::Call {
                function: Scalar::And,
                arguments,
            } => {
                for argument in arguments {
                    conjuncts(argument, equalities);
                }
            }
            Expr::Call {
                function: Scalar::Equal,
                arguments,
            } => {
                let term = |expression: &Expr<Scalar>| match expression {
                    Expr::Value(_)
                    | Expr::Bool(_)
                    | Expr::Int64(_)
                    | Expr::UInt64(_)
                    | Expr::String(_) => true,
                    Expr::Float64(value) => value.is_finite(),
                    _ => false,
                };
                if let [left, right] = arguments.as_slice()
                    && term(left)
                    && term(right)
                {
                    equalities.push((left.clone(), right.clone()));
                }
            }
            _ => {}
        }
    }
    for predicate in left
        .predicates
        .iter()
        .chain(&right.predicates)
        .chain(std::iter::once(&condition))
    {
        conjuncts(predicate, &mut equalities);
    }

    let equivalent = |left: ValueId, right: ValueId| {
        let mut connected = vec![Expr::Value(left)];
        let mut next = 0;

        while next < connected.len() {
            for (first, second) in &equalities {
                for (from, to) in [(first, second), (second, first)] {
                    if *from == connected[next] && !connected.contains(to) {
                        connected.push(to.clone());
                    }
                }
            }
            next += 1;
        }

        connected.contains(&Expr::Value(right))
    };

    let key_proven = a.layout.current_row_key().iter().all(|column| {
        a.read
            .columns
            .iter()
            .filter(|(_, name)| name == column)
            .any(|(left, _)| {
                b.read
                    .columns
                    .iter()
                    .filter(|(_, name)| name == column)
                    .any(|(right, _)| equivalent(*left, *right))
            })
    });
    if !key_proven {
        return Ok(vec![]);
    }

    let mut scan = a.clone();
    scan.binding = a.binding.clone().or_else(|| b.binding.clone());
    scan.relationship = a.relationship.or(b.relationship);
    scan.read.columns.extend(b.read.columns.iter().cloned());

    let predicate = left
        .predicates
        .into_iter()
        .chain(right.predicates)
        .chain(std::iter::once(condition))
        .reduce(|left, right| Expr::Call {
            function: Scalar::And,
            arguments: vec![left, right],
        })
        .unwrap();
    let mut rewritten = candidate.clone();
    rewritten.program.root = Node {
        op: Op::Project(exports),
        inputs: vec![Node {
            op: Op::Filter(predicate),
            inputs: vec![Node {
                op: Op::Read(scan),
                inputs: vec![],
            }],
        }],
    };
    Ok(vec![rewritten])
}
