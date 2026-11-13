//! Planning for the connections that are not SQL: an Orbit Next code plan for structural matches
//! and a live plan for entities fetched at query time.

mod code;
mod live;

pub use code::{named_repositories, plan as plan_code};
pub use live::plan as plan_live;

use std::sync::Arc;

use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::CodePlan;
use query_data_model::plan::live::LivePlan;
use query_data_model::{Catalog, EntityId};

use crate::error::QueryError;

/// A code or live connection the deployment serves.
#[derive(Debug, Clone)]
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

/// The store placement put `entity` on, among a stage's placed stores.
pub(crate) fn placed_store(
    placed: &[(EntityId, query_data_model::StoreId)],
    entity: EntityId,
) -> crate::error::Result<query_data_model::StoreId> {
    placed
        .iter()
        .find(|(held, _)| *held == entity)
        .map(|(_, store)| *store)
        .ok_or_else(|| {
            QueryError::PipelineInvariant("a stage reads an entity it did not place".into())
        })
}

impl Target {
    pub fn connection(&self) -> query_data_model::ConnectionId {
        match self {
            Self::Code(code) => code.connection(),
            Self::Live(live) => live.connection(),
        }
    }

    pub fn backend(&self) -> crate::dag::Backend {
        match self {
            Self::Code(_) => crate::dag::Backend::OrbitCode,
            Self::Live(_) => crate::dag::Backend::Live,
        }
    }

    pub fn guard(&self, entity: EntityId) -> Option<&query_data_model::catalog::Guard> {
        match self {
            Self::Code(code) => code.guard(entity),
            Self::Live(live) => live.guard(entity),
        }
    }
}
