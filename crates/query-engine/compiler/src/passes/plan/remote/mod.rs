//! Planning for the connections that are not SQL: an Orbit Next code plan for structural matches
//! and a live plan for entities fetched at query time.

mod code;
mod live;

pub use code::{named_repositories, plan as plan_code};
pub use live::plan as plan_live;

use std::sync::Arc;

use query_data_model::Catalog;
use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::CodePlan;
use query_data_model::plan::live::LivePlan;

use crate::error::QueryError;

/// A code or live connection the deployment serves.
#[derive(Debug)]
pub enum Target {
    Code(Arc<Catalog<OrbitCode>>),
    Live(Arc<Catalog<Live>>),
}

#[derive(Debug)]
pub enum RemotePlan {
    Code(CodePlan),
    Live(LivePlan),
}

fn unsupported(backend: &str, what: &str) -> QueryError {
    QueryError::Validation(format!("the {backend} backend cannot {what}"))
}
