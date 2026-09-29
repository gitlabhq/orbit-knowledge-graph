use crate::error::Result;

use super::{Expr, Function, JoinKind, Node, Op, Operation, Schema};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyCoverage {
    Exact,
    Superset,
    Unknown,
}

pub fn unique_keys<S: Operation, F: Function, E: Operation>(
    node: &Node<S, F, E>,
    subplans: &[Node<S, F, E>],
) -> Result<Vec<Schema>> {
    Ok(match &node.op {
        Op::Read(source) if source.key_coverage() == KeyCoverage::Exact => source.unique_keys(),
        Op::Extension(extension) if extension.key_coverage() == KeyCoverage::Exact => extension.unique_keys(),
        Op::Read(_) | Op::Extension(_) => Vec::new(),
        Op::Reference { subplan, exports } => unique_keys(&subplans[subplan.0], &subplans[..subplan.0])?
            .into_iter().filter_map(|key| key.into_iter().map(|source| {
                exports.iter().find(|(value, _)| *value == source).map(|(_, output)| *output)
            }).collect()).collect(),
        Op::Filter(_) | Op::Sort(_) | Op::Limit(_) | Op::Join { kind: JoinKind::Semi | JoinKind::Anti, .. } => {
            unique_keys(&node.inputs[0], subplans)?
        }
        Op::Project(assignments) => unique_keys(&node.inputs[0], subplans)?
            .into_iter().filter_map(|key| key.into_iter().map(|source| {
                assignments.iter().find(|assignment| matches!(assignment.expression, Expr::Value(value) if value == source))
                    .map(|assignment| assignment.output)
            }).collect()).collect(),
        Op::Aggregate { groups, .. } => vec![groups.iter().map(|group| group.output).collect()],
        Op::Join { kind: JoinKind::Inner, .. } => {
            let left = unique_keys(&node.inputs[0], subplans)?;
            let right = unique_keys(&node.inputs[1], subplans)?;
            left.into_iter().flat_map(|left| right.iter().map(move |right| {
                left.iter().chain(right).copied().collect()
            })).collect()
        }
        Op::Union { .. } => Vec::new(),
    })
}
