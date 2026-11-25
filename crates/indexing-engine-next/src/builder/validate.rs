use std::collections::{BTreeMap, BTreeSet};

use crate::builder::EngineBuilder;
use crate::lane::LaneId;
use crate::trigger::Trigger;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("the {kind} name {name:?} must not be empty, and must not hold a slash or a space")]
    InvalidName { kind: &'static str, name: String },
    #[error("two of the registered {kind}s have the name {name}")]
    DuplicateName { kind: &'static str, name: String },
    #[error("the config lane {lane} belongs to no registered worker")]
    UnknownLaneWorker { lane: LaneId },
    #[error(
        "the config lane {lane} moves timed out tasks to {on_timeout}, which is not a lane of the same worker"
    )]
    UnknownTimeoutLane { lane: LaneId, on_timeout: LaneId },
    #[error("the config sizes the pool {pool}, but no module registered it")]
    UnregisteredPool { pool: String },
    #[error("the planner {planner} waits for the worker {worker}, but no module registered it")]
    UnregisteredDoneSource {
        planner: &'static str,
        worker: &'static str,
    },
    #[error("the reconcilers {first} and {second} both write {table}")]
    SharedWrite {
        table: &'static str,
        first: &'static str,
        second: &'static str,
    },
}

pub(super) fn reject_invalid_or_duplicate_names(builder: &EngineBuilder) -> Result<(), BuildError> {
    reject_invalid_or_duplicate("worker", builder.workers.iter().map(|entry| entry.name))?;
    reject_invalid_or_duplicate("planner", builder.planners.iter().map(|entry| entry.name))?;
    reject_invalid_or_duplicate(
        "reconciler",
        builder.reconcilers.iter().map(|entry| entry.name),
    )?;
    reject_invalid_or_duplicate("pool", builder.pools.iter().copied())
}

pub(super) fn reject_unknown_lanes(builder: &EngineBuilder) -> Result<(), BuildError> {
    let workers: BTreeSet<&str> = builder.workers.iter().map(|entry| entry.name).collect();
    for (lane, config) in &builder.config.lanes {
        if !workers.contains(lane.worker()) {
            return Err(BuildError::UnknownLaneWorker { lane: lane.clone() });
        }
        if let Some(on_timeout) = &config.on_timeout
            && on_timeout.worker() != lane.worker()
        {
            return Err(BuildError::UnknownTimeoutLane {
                lane: lane.clone(),
                on_timeout: on_timeout.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn reject_unregistered_pools(builder: &EngineBuilder) -> Result<(), BuildError> {
    for pool in builder.config.pools.keys() {
        if !builder.pools.contains(&pool.as_str()) {
            return Err(BuildError::UnregisteredPool { pool: pool.clone() });
        }
    }
    Ok(())
}

pub(super) fn reject_done_triggers_on_unknown_workers(
    builder: &EngineBuilder,
) -> Result<(), BuildError> {
    let workers: BTreeSet<&str> = builder.workers.iter().map(|entry| entry.name).collect();
    for planner in &builder.planners {
        for trigger in &planner.triggers {
            if let Trigger::Done(done) = trigger
                && !workers.contains(done.worker())
            {
                return Err(BuildError::UnregisteredDoneSource {
                    planner: planner.name,
                    worker: done.worker(),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn reject_tables_with_two_writers(builder: &EngineBuilder) -> Result<(), BuildError> {
    let mut writers: BTreeMap<&'static str, &'static str> = BTreeMap::new();
    for reconciler in &builder.reconcilers {
        for table in reconciler.writes {
            if let Some(first) = writers.insert(table, reconciler.name) {
                return Err(BuildError::SharedWrite {
                    table,
                    first,
                    second: reconciler.name,
                });
            }
        }
    }
    Ok(())
}

fn reject_invalid_or_duplicate(
    kind: &'static str,
    names: impl Iterator<Item = &'static str>,
) -> Result<(), BuildError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if name.is_empty() || name.contains(['/', ' ']) {
            return Err(BuildError::InvalidName {
                kind,
                name: name.to_owned(),
            });
        }
        if !seen.insert(name) {
            return Err(BuildError::DuplicateName {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(())
}
