use crate::error::Result;

use super::generic::facts::unique_keys;
use super::generic::{Expr, JoinKind, Node, Op, Operation, SubplanId};
use super::optimize::Candidate;
use super::physical::Scalar;

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
