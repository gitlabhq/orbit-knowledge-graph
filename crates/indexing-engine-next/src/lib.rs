#![deny(clippy::allow_attributes, clippy::allow_attributes_without_reason)]

mod builder;
pub mod clock;
mod config;
mod disposition;
mod event;
mod lane;
pub mod lease;
mod ledger;
mod payload;
mod pending;
mod planner;
pub mod ports;
pub mod prelude;
mod reconciler;
mod runner;
mod submit;
pub mod task;
pub mod testkit;
mod trigger;
pub mod versioned;
mod worker;

pub use builder::{BuildError, Engine, EngineBuilder};
pub use config::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};
pub use event::{
    EngineEvent, Failure, LaneEvent, LaneStep, Observer, TaskEvent, TaskIdentity, TaskStep,
};
pub use lane::{InvalidLaneId, Lane, LaneId};
pub use ledger::JobSummary;
pub use payload::{Key, Payload, Tenant};
pub use planner::{PlanContext, PlanError, Planner, Submit};
pub use reconciler::{ReconcileContext, ReconcileError, Reconciler};
pub use trigger::{DoneTrigger, EventBatch, Fired, Trigger};
pub use worker::{Outcome, Permit, PoolRequest, TaskContext, TaskError, Worker};
