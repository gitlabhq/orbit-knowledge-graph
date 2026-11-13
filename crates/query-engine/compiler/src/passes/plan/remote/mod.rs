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
use query_data_model::{Catalog, EntityId, PropertyId};

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

impl Target {
    pub fn connection(&self) -> &str {
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

    /// Whether the connection's rows carry `identity`, the one identity property of `entity`: a
    /// live call returns the columns its store holds, a code search returns matches.
    pub fn returns_identity(&self, entity: EntityId, identity: PropertyId) -> bool {
        match self {
            Self::Code(_) => false,
            Self::Live(live) => live
                .home(entity)
                .is_some_and(|(store, _)| store.column(identity).is_some()),
        }
    }
}
