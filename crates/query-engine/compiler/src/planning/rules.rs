use crate::error::Result;

use super::generic::facts::unique_keys;
use super::generic::{Expr, JoinKind, Node, Op, Operation, Schema, SubplanId};
use super::optimize::Candidate;
use super::physical::Scalar;

pub fn projections<S: Operation + Clone, E: Operation + Clone>(
    candidate: &Candidate<S, Scalar, E>,
) -> Result<Vec<Candidate<S, Scalar, E>>> {
    let Op::Project(_) = &candidate.program.root.op else {
        return Ok(vec![]);
    };
    let mut rewritten = candidate.clone();
    if compose_projection(&mut rewritten.program.root) {
        Ok(vec![rewritten])
    } else {
        Ok(vec![])
    }
}

fn compose_projection<S: Clone, E: Clone>(node: &mut Node<S, Scalar, E>) -> bool {
    let Op::Project(outputs) = &mut node.op else {
        return false;
    };
    let child = &mut node.inputs[0];
    match &child.op {
        Op::Project(inner)
            if inner
                .iter()
                .all(|assignment| matches!(assignment.expression, Expr::Value(_))) =>
        {
            for output in outputs {
                output.expression.map_values(&mut |value| {
                    let assignment = inner
                        .iter()
                        .find(|assignment| assignment.output == *value)
                        .expect("verified projection input");
                    let Expr::Value(source) = assignment.expression else {
                        unreachable!()
                    };
                    *value = source;
                });
            }
            node.inputs = std::mem::take(&mut child.inputs);
        }
        _ => return false,
    }

    true
}

pub fn registered<S: Operation + Clone, E: Operation + Clone>()
-> [super::optimize::Rule<S, Scalar, E>; 2] {
    [unread_unique_join, sip]
}

pub fn normalize<S: Operation + Clone, E: Operation + Clone>(
    candidate: &mut Candidate<S, Scalar, E>,
) -> Result<()> {
    let mut schemas = Vec::new();
    for root in candidate
        .program
        .subplans
        .iter_mut()
        .chain(std::iter::once(&mut candidate.program.root))
    {
        let output = root.output_with(&candidate.values, &schemas)?;
        loop {
            let mut changed = prune(root, output.clone());
            root.visit_mut(&mut |node| {
                changed |= compose_projection(node);
            });
            if !changed {
                break;
            }
        }
        schemas.push(output);
    }
    Ok(())
}

pub fn prune_columns<S: Operation + Clone, E: Operation + Clone>(
    candidate: &Candidate<S, Scalar, E>,
) -> Result<Vec<Candidate<S, Scalar, E>>> {
    let output = candidate.program.output(&candidate.values)?;
    let mut rewritten = candidate.clone();

    if prune(&mut rewritten.program.root, output) {
        Ok(vec![rewritten])
    } else {
        Ok(vec![])
    }
}

fn dependencies(expression: &Expr<Scalar>, required: &mut Schema) {
    let mut expression = expression.clone();
    expression.map_values(&mut |value| {
        if !required.contains(value) {
            required.push(*value);
        }
    });
}

fn prune<S: Operation, E: Operation>(node: &mut Node<S, Scalar, E>, mut required: Schema) -> bool {
    match &mut node.op {
        Op::Read(source) => source.retain_outputs(&required),
        Op::Reference { exports, .. } => {
            if !exports.iter().any(|(_, value)| required.contains(value)) {
                return false;
            }
            let before = exports.len();
            exports.retain(|(_, output)| required.contains(output));
            before != exports.len()
        }
        Op::Project(assignments) => {
            if !assignments
                .iter()
                .any(|assignment| required.contains(&assignment.output))
            {
                return false;
            }
            let before = assignments.len();
            assignments.retain(|assignment| required.contains(&assignment.output));
            required.clear();
            for assignment in assignments.iter() {
                dependencies(&assignment.expression, &mut required);
            }

            prune(&mut node.inputs[0], required) | (before != assignments.len())
        }
        Op::Filter(predicate) => {
            dependencies(predicate, &mut required);
            prune(&mut node.inputs[0], required)
        }
        Op::Sort(keys) => {
            for key in keys {
                if !required.contains(&key.value) {
                    required.push(key.value);
                }
            }
            prune(&mut node.inputs[0], required)
        }
        Op::Limit(_) => prune(&mut node.inputs[0], required),
        Op::Join { condition, .. } => {
            dependencies(condition, &mut required);
            let left = prune(&mut node.inputs[0], required.clone());
            prune(&mut node.inputs[1], required) | left
        }
        Op::Aggregate { groups, measures } => {
            required.clear();
            for group in groups {
                dependencies(&group.expression, &mut required);
            }
            for measure in measures {
                for expression in measure.argument.iter().chain(measure.filter.iter()) {
                    dependencies(expression, &mut required);
                }
            }
            prune(&mut node.inputs[0], required)
        }
        Op::Union { outputs, arms } => {
            let positions: Vec<_> = outputs
                .iter()
                .enumerate()
                .filter_map(|(index, value)| required.contains(value).then_some(index))
                .collect();
            if positions.is_empty() {
                return false;
            }
            let mut changed = positions.len() != outputs.len();
            *outputs = positions.iter().map(|index| outputs[*index]).collect();

            for (arm, input) in arms.iter_mut().zip(&mut node.inputs) {
                *arm = positions.iter().map(|index| arm[*index]).collect();
                changed |= prune(input, arm.clone());
            }
            changed
        }
        Op::Extension(_) => false,
    }
}

pub fn unread_unique_join<S: Operation + Clone, E: Operation + Clone>(
    candidate: &Candidate<S, Scalar, E>,
) -> Result<Vec<Candidate<S, Scalar, E>>> {
    let root = &candidate.program.root;
    let Op::Project(assignments) = &root.op else {
        return Ok(vec![]);
    };
    let join = &root.inputs[0];
    let Op::Join {
        kind: JoinKind::Inner,
        condition:
            Expr::Call {
                function: Scalar::Equal,
                arguments,
            },
    } = &join.op
    else {
        return Ok(vec![]);
    };
    let [Expr::Value(left_key), Expr::Value(right_key)] = arguments.as_slice() else {
        return Ok(vec![]);
    };
    let mut schemas = Vec::new();
    for subplan in &candidate.program.subplans {
        schemas.push(subplan.output_with(&candidate.values, &schemas)?);
    }
    let left = join.inputs[0].output_with(&candidate.values, &schemas)?;
    if !left.contains(left_key)
        || !unique_keys(&join.inputs[1], &candidate.program.subplans)?
            .iter()
            .any(|key| key == &vec![*right_key])
        || assignments.iter().any(|assignment| {
            assignment
                .expression
                .data_type(&left, &candidate.values)
                .is_err()
        })
    {
        return Ok(vec![]);
    }

    let mut rewritten = candidate.clone();
    if let Op::Join { kind, .. } = &mut rewritten.program.root.inputs[0].op {
        *kind = JoinKind::Semi;
    }
    Ok(vec![rewritten])
}

pub fn sip<S: Operation + Clone, E: Operation + Clone>(
    candidate: &Candidate<S, Scalar, E>,
) -> Result<Vec<Candidate<S, Scalar, E>>> {
    let root = &candidate.program.root;
    let Op::Join {
        kind: JoinKind::Inner,
        condition:
            Expr::Call {
                function: Scalar::Equal,
                arguments,
            },
    } = &root.op
    else {
        return Ok(vec![]);
    };
    let [Expr::Value(first), Expr::Value(second)] = arguments.as_slice() else {
        return Ok(vec![]);
    };

    let mut schemas = Vec::new();
    for subplan in &candidate.program.subplans {
        schemas.push(subplan.output_with(&candidate.values, &schemas)?);
    }

    let left = root.inputs[0].output_with(&candidate.values, &schemas)?;
    let right = root.inputs[1].output_with(&candidate.values, &schemas)?;
    let keys = if left.contains(first) && right.contains(second) {
        [*first, *second]
    } else if left.contains(second) && right.contains(first) {
        [*second, *first]
    } else {
        return Ok(vec![]);
    };

    for producer in 0..2 {
        let consumer = 1 - producer;
        let Op::Reference { subplan, exports } = &root.inputs[producer].op else {
            continue;
        };
        let membership = &root.inputs[consumer];
        let Op::Join {
            kind: JoinKind::Semi,
            condition:
                Expr::Call {
                    function: Scalar::Equal,
                    arguments,
                },
        } = &membership.op
        else {
            continue;
        };
        let [Expr::Value(consumer_key), Expr::Value(producer_key)] = arguments.as_slice() else {
            continue;
        };
        let Op::Reference {
            subplan: key_source,
            exports: key_exports,
        } = &membership.inputs[1].op
        else {
            continue;
        };

        if *consumer_key == keys[consumer]
            && key_source == subplan
            && exports.iter().any(|(source, target)| {
                *target == keys[producer] && key_exports.contains(&(*source, *producer_key))
            })
        {
            return Ok(vec![]);
        }
    }

    let mut alternatives = Vec::new();
    for producer in 0..2 {
        let consumer = 1 - producer;
        let mut rewritten = candidate.clone();
        let key = rewritten
            .values
            .allocate(candidate.values.data_type(keys[producer])?.clone());
        let reference = |exports| Node {
            op: Op::Reference {
                subplan: SubplanId(candidate.program.subplans.len()),
                exports,
            },
            inputs: vec![],
        };
        let schema = if producer == 0 { &left } else { &right };
        rewritten
            .program
            .subplans
            .push(root.inputs[producer].clone());
        rewritten.program.root.inputs[producer] =
            reference(schema.iter().map(|value| (*value, *value)).collect());
        rewritten.program.root.inputs[consumer] = Node {
            op: Op::Join {
                kind: JoinKind::Semi,
                condition: Expr::Call {
                    function: Scalar::Equal,
                    arguments: vec![Expr::Value(keys[consumer]), Expr::Value(key)],
                },
            },
            inputs: vec![
                root.inputs[consumer].clone(),
                reference(vec![(keys[producer], key)]),
            ],
        };

        alternatives.push(rewritten);
    }

    Ok(alternatives)
}
