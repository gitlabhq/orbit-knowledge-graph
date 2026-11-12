//! The security phase for code and live plans: what the policies of a stage's reads admit
//! becomes the request's own form of its scope restriction, then a check proves every read
//! carries it.

use std::collections::{BTreeMap, BTreeSet};

use orbit_authorization::{Grants, Key, ScopeSet};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::{Catalog, Rule};
use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::{CodeOperation, CodePlan};
use query_data_model::plan::live::{LiveOperation, LivePlan, Workspaces};
use query_data_model::plan::logical::{LogicalOperation, LogicalPlan, LogicalType};
use query_data_model::plan::tracking::{Language, NodeId, Plan};
use query_data_model::{EntityId, Model};

use super::guards::scope_set;
use crate::error::{QueryError, Result};
use crate::passes::plan::remote::Target;

/// The scope key of each repository or workspace a stage reads, by the entity whose `via` chain
/// resolved it on a relational connection before the request went out.
#[derive(Debug, Clone, Default)]
pub struct Namespaces(pub BTreeMap<EntityId, BTreeMap<Key, TraversalPath>>);

/// What the policies of a stage's reads admit: every repository or workspace the request names,
/// when only checks restrict its rows, or only these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    Named,
    Only(BTreeSet<Key>),
}

impl Admitted {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Only(keys) if keys.is_empty())
    }

    fn intersect(self, keys: BTreeSet<Key>) -> Self {
        match self {
            Self::Named => Self::Only(keys),
            Self::Only(kept) => Self::Only(kept.intersection(&keys).cloned().collect()),
        }
    }
}

/// Every entity a stage's logical plan reads; one it cannot name fails closed.
pub fn read_entities(logical: &LogicalPlan) -> Result<Vec<EntityId>> {
    let unnamed = || QueryError::Security("a remote stage reads nodes it cannot name".into());
    let mut entities = Vec::new();
    for node in logical.nodes() {
        let entity = match logical.operation(node) {
            LogicalOperation::Entities(entity) => *entity,
            LogicalOperation::Follow { .. } => {
                match logical.outputs(node).last().map(|to| logical.value(to)) {
                    Some(LogicalType::Entity(entity)) => *entity,
                    _ => return Err(unnamed()),
                }
            }
            LogicalOperation::Neighbors { .. }
            | LogicalOperation::Shortest { .. }
            | LogicalOperation::Read(_)
            | LogicalOperation::Union => return Err(unnamed()),
            _ => continue,
        };
        if !entities.contains(&entity) {
            entities.push(entity);
        }
    }
    Ok(entities)
}

/// What every read of the stage admits: each `via` read keeps the keys whose scope the caller
/// holds, a global read needs the permission somewhere, and a checked or trusted read leaves the
/// rows to its checks.
pub fn admitted(
    target: &Target,
    logical: &LogicalPlan,
    grants: &Grants,
    namespaces: &Namespaces,
) -> Result<Admitted> {
    let model = target.model();
    let mut admitted = Admitted::Named;
    for entity in read_entities(logical)? {
        let guard = target
            .guard(entity)
            .ok_or_else(|| QueryError::Security("the read has no guard".into()))?;
        let set = || scope_set(grants, model, guard.scope, guard.permission);
        match &guard.rule {
            Rule::Via(_) => {
                let set = set()?;
                let resolved = namespaces.0.get(&entity).ok_or_else(|| {
                    QueryError::Security(format!(
                        "no scope lookup resolved {}'s via chain",
                        model.graph().entity(entity).name
                    ))
                })?;
                let held = resolved
                    .iter()
                    .filter(|(_, path)| holds(set, path))
                    .map(|(key, _)| key.clone())
                    .collect();
                admitted = admitted.intersect(held);
            }
            Rule::Global if set()?.is_empty() => admitted = Admitted::Only(BTreeSet::new()),
            Rule::Global | Rule::Checked | Rule::Trusted => {}
            Rule::Column(_) | Rule::Owner(_) => {
                return Err(QueryError::Security(
                    "a remote read is restricted through a via chain, its permission or its checks"
                        .into(),
                ));
            }
        }
    }
    Ok(admitted)
}

/// Each repository read keeps the repositories its policies admit.
pub fn secure_code(
    code: &Catalog<OrbitCode>,
    plan: CodePlan,
    admitted: &Admitted,
) -> Result<CodePlan> {
    Ok(code.rebuild(plan, |q, step| {
        let readable = match &step.operation {
            CodeOperation::Repositories { projects, .. } => Some(
                projects
                    .iter()
                    .copied()
                    .filter(|project| match admitted {
                        Admitted::Named => true,
                        Admitted::Only(keys) => keys.contains(&Key::Int(*project)),
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        };
        let files = step.keep(q)?;
        match readable {
            Some(projects) => q.restrict(files, projects),
            None => Ok(files),
        }
    })?)
}

/// Each call reads the workspaces its policies admit.
pub fn secure_live(live: &Catalog<Live>, plan: LivePlan, admitted: &Admitted) -> Result<LivePlan> {
    let workspaces = match admitted {
        Admitted::Named => Workspaces::Any,
        Admitted::Only(keys) => Workspaces::Only(keys.iter().map(ToString::to_string).collect()),
    };
    Ok(live.rebuild(plan, |q, step| {
        let call = matches!(step.operation, LiveOperation::Call(_));
        let calls = step.keep(q)?;
        match call {
            true => q.workspaces(calls, workspaces.clone()),
            false => Ok(calls),
        }
    })?)
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

impl Target {
    pub fn model(&self) -> &Model {
        match self {
            Self::Code(code) => code.model(),
            Self::Live(live) => live.model(),
        }
    }
}
