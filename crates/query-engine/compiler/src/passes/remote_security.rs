//! The security phase for code and live plans: each read's guard becomes the request's own
//! form of a path filter, then a check proves every read carries it.

use std::collections::BTreeMap;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::Guard;
use query_data_model::plan::code::CodeOperation;
use query_data_model::plan::live::LiveOperation;
use query_data_model::plan::tracking::{Language, NodeId, Plan};

use crate::error::{QueryError, Result};
use crate::passes::plan::remote::{RemotePlan, Target};
use crate::types::SecurityContext;

/// Where each repository and workspace sits in the namespace tree, resolved on the graph
/// connection before the request goes out.
#[derive(Debug, Clone, Default)]
pub struct Namespaces {
    pub repositories: BTreeMap<i64, TraversalPath>,
    pub workspaces: BTreeMap<String, TraversalPath>,
}

pub fn secure(
    target: &Target,
    plan: RemotePlan,
    ctx: &SecurityContext,
    namespaces: &Namespaces,
) -> Result<RemotePlan> {
    match (target, plan) {
        (Target::Code(code), RemotePlan::Code(plan)) => {
            let index = plan
                .nodes()
                .find_map(|node| match plan.operation(node) {
                    CodeOperation::Repositories { index, .. } => Some(*index),
                    _ => None,
                })
                .ok_or_else(|| QueryError::Security("the code plan reads no index".into()))?;
            let readable = readable(code.guard(index), ctx)?;
            let plan = code.rewrite(plan, |q, files| {
                let CodeOperation::Repositories { projects, .. } = q.operation(&files)? else {
                    return Ok(files);
                };
                let allowed: Vec<i64> = projects
                    .iter()
                    .copied()
                    .filter(|project| {
                        namespaces
                            .repositories
                            .get(project)
                            .is_some_and(|path| path.is_within_scope(&readable))
                    })
                    .collect();
                q.restrict(files, allowed)
            })?;
            Ok(RemotePlan::Code(plan))
        }
        (Target::Live(live), RemotePlan::Live(plan)) => {
            let operation = plan
                .nodes()
                .find_map(|node| match plan.operation(node) {
                    LiveOperation::Call(operation) => Some(*operation),
                    _ => None,
                })
                .ok_or_else(|| QueryError::Security("the live plan calls no operation".into()))?;
            let readable = readable(live.guard(operation), ctx)?;
            let workspaces: Vec<String> = namespaces
                .workspaces
                .iter()
                .filter(|(_, path)| path.is_within_scope(&readable))
                .map(|(workspace, _)| workspace.clone())
                .collect();
            let plan = live.rewrite(plan, |q, calls| match q.operation(&calls)? {
                LiveOperation::Call(_) => q.workspaces(calls, workspaces.clone()),
                _ => Ok(calls),
            })?;
            Ok(RemotePlan::Live(plan))
        }
        _ => Err(QueryError::PipelineInvariant(
            "the plan does not match its target".into(),
        )),
    }
}

fn readable<'c>(guard: Option<&Guard>, ctx: &'c SecurityContext) -> Result<Vec<&'c TraversalPath>> {
    let guard = guard.ok_or_else(|| QueryError::Security("the read has no guard".into()))?;
    Ok(ctx.paths_at_least(guard.min_role.unwrap_or_default()))
}

/// Every repository read feeds a restriction, and every call a workspace precondition.
pub fn check(plan: &RemotePlan) -> Result<()> {
    match plan {
        RemotePlan::Code(plan) => guarded(
            plan,
            |operation| matches!(operation, CodeOperation::Repositories { .. }),
            |operation| matches!(operation, CodeOperation::Restrict(_)),
            "repository read",
        ),
        RemotePlan::Live(plan) => guarded(
            plan,
            |operation| matches!(operation, LiveOperation::Call(_)),
            |operation| matches!(operation, LiveOperation::Workspaces(_)),
            "live call",
        ),
    }
}

fn guarded<L: Language>(
    plan: &Plan<L>,
    read: impl Fn(&L::Operation) -> bool,
    guard: impl Fn(&L::Operation) -> bool,
    what: &str,
) -> Result<()> {
    let consumer = |node: NodeId| {
        plan.nodes()
            .find(|consumer| plan.inputs(*consumer).contains(&node))
    };
    for node in plan.nodes().filter(|node| read(plan.operation(*node))) {
        if !consumer(node).is_some_and(|consumer| guard(plan.operation(consumer))) {
            return Err(QueryError::Security(format!("a {what} carries no guard")));
        }
    }
    Ok(())
}
