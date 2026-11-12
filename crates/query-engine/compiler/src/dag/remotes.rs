use std::sync::Arc;

use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::{EntityId, Model, PropertyId, StorageBackend};

use super::Backend;
use crate::passes::plan::remote::Target;

/// The code and live connections a deployment serves next to its relational one: the
/// connections it registers an executor for.
#[derive(Debug, Clone, Default)]
pub struct Remotes {
    targets: Arc<Vec<Target>>,
    connections: Arc<Vec<String>>,
}

impl Remotes {
    /// Every code and live connection the model declares, for deployments that execute them all.
    pub fn all(model: &Arc<Model>) -> Self {
        let mut connections: Vec<&str> = model
            .stores()
            .iter()
            .map(|store| store.connection.as_str())
            .collect();
        connections.sort_unstable();
        connections.dedup();
        let targets = connections
            .into_iter()
            .filter_map(|connection| {
                if let Ok(code) = model.physical::<OrbitCode>(connection) {
                    return Some(Target::Code(Arc::new(code)));
                }
                model
                    .physical::<Live>(connection)
                    .ok()
                    .map(|live| Target::Live(Arc::new(live)))
            })
            .collect();
        Self::new(targets)
    }

    pub fn new(targets: Vec<Target>) -> Self {
        let connections = targets
            .iter()
            .map(|target| target.connection().to_string())
            .collect();
        Self {
            targets: Arc::new(targets),
            connections: Arc::new(connections),
        }
    }

    pub fn connections(&self) -> &[String] {
        &self.connections
    }

    pub fn targets(&self) -> &[Target] {
        &self.targets
    }

    pub fn target(&self, connection: &str) -> Option<&Target> {
        self.targets
            .iter()
            .find(|target| target.connection() == connection)
    }

    /// The code connection that runs structural matches over `property` of `entity`.
    pub fn searching(&self, entity: EntityId, property: PropertyId) -> Option<&Target> {
        self.targets.iter().find(|target| match target {
            Target::Code(code) => code
                .home(entity)
                .is_some_and(|(_, facts)| facts.structural(entity) == Some(property)),
            Target::Live(_) => false,
        })
    }

    /// The connection that holds `entity`: a live endpoint, else a code index.
    pub fn holding(&self, entity: EntityId) -> Option<&Target> {
        let live = self.targets.iter().find(|target| match target {
            Target::Live(live) => live.endpoint(entity).is_some(),
            Target::Code(_) => false,
        });
        live.or_else(|| {
            self.targets.iter().find(|target| match target {
                Target::Code(code) => code.home(entity).is_some(),
                Target::Live(_) => false,
            })
        })
    }
}

impl Target {
    pub fn connection(&self) -> &str {
        match self {
            Self::Code(code) => code.connection(),
            Self::Live(live) => live.connection(),
        }
    }

    pub fn backend(&self) -> Backend {
        match self {
            Self::Code(_) => Backend::OrbitCode,
            Self::Live(_) => Backend::Live,
        }
    }

    pub fn guard(&self, entity: EntityId) -> Option<&query_data_model::catalog::Guard> {
        match self {
            Self::Code(code) => code.guard(entity),
            Self::Live(live) => live.guard(entity),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Code(_) => OrbitCode::KIND,
            Self::Live(_) => Live::KIND,
        }
    }
}
