#![deny(clippy::allow_attributes, clippy::allow_attributes_without_reason)]

mod builder;
mod config;
mod disposition;
mod lane;
mod ledger;
mod payload;
mod planner;
mod port;
pub mod prelude;
pub mod protocol;
mod reconciler;
mod serve;
pub mod testkit;
mod trigger;
mod worker;

pub use builder::{BuildError, Engine, EngineBuilder};
pub use config::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};
pub use lane::Lane;
pub use ledger::JobSummary;
pub use payload::{Key, Payload, Tenant};
pub use planner::{PlanContext, PlanError, Planner, Submit};
pub use reconciler::{ReconcileContext, ReconcileError, Reconciler};
pub use trigger::{DoneTrigger, EventBatch, Fired, Trigger};
pub use worker::{Outcome, Permit, PoolRequest, TaskContext, TaskError, Worker};
