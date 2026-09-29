use std::collections::HashMap;
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
        || a.deletion_column != b.deletion_column
        || a.replacement_key.is_empty()
        || a.replacement_key != b.replacement_key
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
    fn conjuncts(expression: &Expr<Scalar>, equalities: &mut Vec<(ValueId, ValueId)>) {
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
                if let [Expr::Value(left), Expr::Value(right)] = arguments.as_slice() {
                    equalities.push((*left, *right));
                }
            }
            _ => {}
        }
    }
    conjuncts(&condition, &mut equalities);

    let key_proven = a.replacement_key.iter().all(|column| {
        equalities.iter().any(|(left, right)| {
            let matches = |left, right| {
                a.read.columns.contains(&(left, column.clone()))
                    && b.read.columns.contains(&(right, column.clone()))
            };
            matches(*left, *right) || matches(*right, *left)
        })
    });
    if !key_proven {
        return Ok(vec![]);
    }

    let mut scan = a.clone();
    scan.binding = a.binding.clone().or_else(|| b.binding.clone());
    scan.relationship = a.relationship.or(b.relationship);
    let mut mapping = HashMap::new();
    for (value, column) in &b.read.columns {
        let selected = scan
            .read
            .columns
            .iter()
            .find(|(_, name)| name == column)
            .map(|(value, _)| *value);
        match selected {
            Some(selected)
                if candidate.values.data_type(selected)?
                    == candidate.values.data_type(*value)? =>
            {
                mapping.insert(*value, selected);
            }
            _ => scan.read.columns.push((*value, column.clone())),
        }
    }

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
    let mut predicate = predicate;
    predicate.map_values(&mut |value| *value = mapping.get(value).copied().unwrap_or(*value));
    let assignments = exports
        .into_iter()
        .map(|mut assignment| {
            assignment
                .expression
                .map_values(&mut |value| *value = mapping.get(value).copied().unwrap_or(*value));
            assignment
        })
        .collect();

    let mut rewritten = candidate.clone();
    rewritten.program.root = Node {
        op: Op::Project(assignments),
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
