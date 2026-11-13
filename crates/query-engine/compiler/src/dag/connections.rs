//! The connections a deployment serves, each a peer that a stage of a query can run on.

use std::sync::Arc;

use query_data_model::catalog::Guard;
use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::{
    Catalog, ClickHouse, DataModelError, DuckDb, EntityId, Model, PropertyId, StorageBackend,
};

use super::Backend;
use crate::passes::plan::remote::Target;

/// One served connection, as the catalog its stages plan against. A new relational kind is one
/// more arm here and its stage pipeline in `compile`.
#[derive(Debug, Clone)]
pub enum Peer {
    ClickHouse(Arc<Catalog<ClickHouse>>),
    DuckDb(Arc<Catalog<DuckDb>>),
    Remote(Target),
}

impl Peer {
    /// The connection `connection` of `model`, as the peer of the kind its stores declare.
    pub fn of(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        let kind = model
            .stores()
            .iter()
            .find(|store| store.connection == connection)
            .map(|store| store.kind.as_str())
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "connection",
                name: connection.to_string(),
            })?;
        Ok(match kind {
            ClickHouse::KIND => Self::ClickHouse(Arc::new(model.physical(connection)?)),
            DuckDb::KIND => Self::DuckDb(Arc::new(model.physical(connection)?)),
            OrbitCode::KIND => Self::Remote(Target::Code(Arc::new(model.physical(connection)?))),
            _ => Self::Remote(Target::Live(Arc::new(model.physical::<Live>(connection)?))),
        })
    }

    pub fn connection(&self) -> &str {
        match self {
            Self::ClickHouse(catalog) => catalog.connection(),
            Self::DuckDb(catalog) => catalog.connection(),
            Self::Remote(target) => target.connection(),
        }
    }

    pub fn backend(&self) -> Backend {
        match self {
            Self::ClickHouse(_) => Backend::ClickHouse,
            Self::DuckDb(_) => Backend::DuckDb,
            Self::Remote(target) => target.backend(),
        }
    }

    pub fn remote(&self) -> Option<&Target> {
        match self {
            Self::Remote(target) => Some(target),
            Self::ClickHouse(_) | Self::DuckDb(_) => None,
        }
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        match self {
            Self::ClickHouse(catalog) => catalog.guard(entity),
            Self::DuckDb(catalog) => catalog.guard(entity),
            Self::Remote(target) => target.guard(entity),
        }
    }

    /// Whether a stage on this connection reads `entity`: a relational table, a live endpoint
    /// or a code index.
    pub fn holds(&self, entity: EntityId) -> bool {
        match self {
            Self::ClickHouse(catalog) => catalog.table_of(entity).is_some(),
            Self::DuckDb(catalog) => catalog.table_of(entity).is_some(),
            Self::Remote(Target::Live(live)) => live.endpoint(entity).is_some(),
            Self::Remote(Target::Code(code)) => code.home(entity).is_some(),
        }
    }

    fn model(&self) -> &Model {
        match self {
            Self::ClickHouse(catalog) => catalog.model(),
            Self::DuckDb(catalog) => catalog.model(),
            Self::Remote(target) => target.model(),
        }
    }
}

/// Why a binding has no connection to run on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unplaced {
    /// No store of the deployment marks the entity complete.
    Unstored,
    /// The entity's complete store sits on a connection the deployment does not serve.
    Unserved(String),
}

/// Every connection a deployment serves.
#[derive(Debug, Clone)]
pub struct Connections {
    peers: Arc<Vec<Peer>>,
    names: Arc<Vec<String>>,
}

impl Connections {
    pub fn new(peers: Vec<Peer>) -> Self {
        let names = peers
            .iter()
            .map(|peer| peer.connection().to_string())
            .collect();
        Self {
            peers: Arc::new(peers),
            names: Arc::new(names),
        }
    }

    /// The one connection `catalog` serves.
    pub fn single<B: crate::passes::codegen::SqlBackend>(catalog: &Catalog<B>) -> Self {
        let model = catalog.shared_model();
        let catalog = model
            .physical::<B>(catalog.connection())
            .expect("a catalog's own connection resolves");
        Self::new(vec![B::peer(Arc::new(catalog))])
    }

    /// Every connection of a deployment's model, each as the peer of its kind.
    pub fn of(model: &Arc<Model>) -> Result<Self, DataModelError> {
        let mut connections: Vec<&str> = model
            .stores()
            .iter()
            .map(|store| store.connection.as_str())
            .collect();
        connections.sort_unstable();
        connections.dedup();
        let peers = connections
            .into_iter()
            .map(|connection| Peer::of(model, connection))
            .collect::<Result<_, _>>()?;
        Ok(Self::new(peers))
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn get(&self, connection: &str) -> Option<&Peer> {
        self.peers
            .iter()
            .find(|peer| peer.connection() == connection)
    }

    pub fn target(&self, connection: &str) -> Option<&Target> {
        self.get(connection)?.remote()
    }

    /// The code connection that runs structural matches over `property` of `entity`.
    pub fn searching(&self, entity: EntityId, property: PropertyId) -> Option<usize> {
        self.peers.iter().position(|peer| match peer {
            Peer::Remote(Target::Code(code)) => code
                .home(entity)
                .is_some_and(|(_, facts)| facts.structural(entity) == Some(property)),
            _ => false,
        })
    }

    /// The connection a read of `entity` runs on: the one its `complete: true` store sits on.
    pub fn holding(&self, entity: EntityId) -> Result<usize, Unplaced> {
        let model = self
            .peers
            .first()
            .map(Peer::model)
            .ok_or(Unplaced::Unstored)?;
        let store = model
            .stores()
            .iter()
            .find(|store| store.complete(entity))
            .ok_or(Unplaced::Unstored)?;
        self.peers
            .iter()
            .position(|peer| peer.connection() == store.connection)
            .ok_or_else(|| Unplaced::Unserved(store.connection.clone()))
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.peers[self.holding(entity).ok()?].guard(entity)
    }
}
