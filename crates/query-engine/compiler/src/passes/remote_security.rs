//! The security phase for code and live plans: each read's guard becomes the request's own
//! form of its scope restriction, then a check proves every read carries it.

use std::collections::{BTreeMap, HashMap};

use orbit_authorization::{Grants, Key, ScopeSet};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Model;
use query_data_model::catalog::{Catalog, Guard, Rule};
use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::{CodeOperation, CodePlan};
use query_data_model::plan::live::{LiveOperation, LivePlan};
use query_data_model::plan::tracking::{Language, NodeId, Plan};

use super::guards::scope_set;
use crate::error::{QueryError, Result};

/// The scope key of each repository and workspace a plan reads, resolved on the graph
/// connection through the read's `via` chain before the request goes out.
#[derive(Debug, Clone, Default)]
pub struct Namespaces {
    pub repositories: BTreeMap<i64, TraversalPath>,
    pub workspaces: BTreeMap<String, TraversalPath>,
}

/// Each repository read keeps the repositories whose scope key the caller can read.
pub fn secure_code(
    code: &Catalog<OrbitCode>,
    plan: CodePlan,
    grants: &Grants,
    namespaces: &Namespaces,
) -> Result<CodePlan> {
    let mut allowed: HashMap<NodeId, Vec<i64>> = HashMap::new();
    for node in plan.nodes() {
        let CodeOperation::Repositories { index, projects } = plan.operation(node) else {
            continue;
        };
        let set = admitted(code.model(), code.index_guard(*index), grants)?;
        let keep = |project: &i64| match set {
            None => true,
            Some(set) => namespaces
                .repositories
                .get(project)
                .is_some_and(|path| holds(set, path)),
        };
        allowed.insert(node, projects.iter().copied().filter(keep).collect());
    }
    Ok(code.rebuild(plan, |q, step| {
        let files = step.keep(q)?;
        match allowed.remove(&step.node()) {
            Some(projects) => q.restrict(files, projects),
            None => Ok(files),
        }
    })?)
}

/// Each call keeps the workspaces whose scope key the caller can read.
pub fn secure_live(
    live: &Catalog<Live>,
    plan: LivePlan,
    grants: &Grants,
    namespaces: &Namespaces,
) -> Result<LivePlan> {
    let mut allowed: HashMap<NodeId, Vec<String>> = HashMap::new();
    for node in plan.nodes() {
        let LiveOperation::Call(operation) = plan.operation(node) else {
            continue;
        };
        let set = admitted(live.model(), live.operation_guard(*operation), grants)?;
        let workspaces = namespaces.workspaces.iter().filter(|(_, path)| match set {
            None => true,
            Some(set) => holds(set, path),
        });
        allowed.insert(node, workspaces.map(|(name, _)| name.clone()).collect());
    }
    Ok(live.rebuild(plan, |q, step| {
        let calls = step.keep(q)?;
        match allowed.remove(&step.node()) {
            Some(workspaces) => q.workspaces(calls, workspaces),
            None => Ok(calls),
        }
    })?)
}

/// The caller's scope set for a read's `via` policy; none for a read only its checks restrict.
fn admitted<'g>(
    model: &Model,
    guard: Option<&Guard>,
    grants: &'g Grants,
) -> Result<Option<&'g ScopeSet>> {
    let guard = guard.ok_or_else(|| QueryError::Security("the read has no guard".into()))?;
    match guard.rule {
        Rule::Via(_) => scope_set(grants, model, guard.scope, guard.permission).map(Some),
        Rule::Checked => Ok(None),
        _ => Err(QueryError::Security(
            "a remote read is restricted through a via chain or its checks".into(),
        )),
    }
}

fn holds(set: &ScopeSet, path: &TraversalPath) -> bool {
    match set {
        ScopeSet::All => true,
        ScopeSet::Subtrees(roots) => {
            let roots: Vec<TraversalPath> = roots.iter().map(|root| root.as_str().into()).collect();
            path.is_within_scope(&roots.iter().collect::<Vec<_>>())
        }
        ScopeSet::Keys(keys) => keys.contains(&Key::Text(path.as_str().to_string())),
    }
}

/// Every repository read feeds a restriction.
pub fn check_code(plan: &CodePlan) -> Result<()> {
    guarded(
        plan,
        |operation| matches!(operation, CodeOperation::Repositories { .. }),
        |operation| matches!(operation, CodeOperation::Restrict(_)),
        "repository read",
    )
}

/// Every call feeds a workspace precondition.
pub fn check_live(plan: &LivePlan) -> Result<()> {
    guarded(
        plan,
        |operation| matches!(operation, LiveOperation::Call(_)),
        |operation| matches!(operation, LiveOperation::Workspaces(_)),
        "live call",
    )
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
