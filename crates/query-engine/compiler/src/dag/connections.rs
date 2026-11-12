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

    pub fn shared_model(&self) -> &Arc<Model> {
        match self {
            Self::ClickHouse(catalog) => catalog.shared_model(),
            Self::DuckDb(catalog) => catalog.shared_model(),
            Self::Remote(Target::Code(code)) => code.shared_model(),
            Self::Remote(Target::Live(live)) => live.shared_model(),
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

    /// Whether the connection's home store of `entity` marks it complete.
    fn complete(&self, entity: EntityId) -> bool {
        let complete = |source: Option<(_, &query_data_model::catalog::Source)>| {
            source.is_some_and(|(_, source)| source.complete)
        };
        match self {
            Self::ClickHouse(catalog) => complete(catalog.source(entity)),
            Self::DuckDb(catalog) => complete(catalog.source(entity)),
            Self::Remote(Target::Code(code)) => complete(code.source(entity)),
            Self::Remote(Target::Live(live)) => complete(live.source(entity)),
        }
    }

    /// Whether the connection's rows carry `identity`, the one identity property of `entity`: a
    /// relational stage and a live call return the columns their store holds, a code search
    /// returns matches.
    pub fn returns_identity(&self, entity: EntityId, identity: PropertyId) -> bool {
        match self {
            Self::ClickHouse(_) | Self::DuckDb(_) => true,
            Self::Remote(target) => target.returns_identity(entity, identity),
        }
    }
}

/// Every connection a deployment serves, in the order it prefers them when several hold an
/// entity and none marks it complete.
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

    /// The connections of `model` named in `served`, each as the peer of its kind.
    pub fn serving(model: &Arc<Model>, served: &[&str]) -> Result<Self, DataModelError> {
        let peers = served
            .iter()
            .map(|connection| Peer::of(model, connection))
            .collect::<Result<_, _>>()?;
        Ok(Self::new(peers))
    }

    /// These connections, then `others`.
    #[must_use]
    pub fn with(&self, others: &Connections) -> Self {
        Self::new(self.peers.iter().chain(others.peers()).cloned().collect())
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

    /// The connection a read of `entity` runs on: the one whose store marks it complete, else
    /// the first that holds it.
    pub fn holding(&self, entity: EntityId) -> Option<usize> {
        let holders = || {
            self.peers
                .iter()
                .enumerate()
                .filter(|(_, peer)| peer.holds(entity))
        };
        holders()
            .find(|(_, peer)| peer.complete(entity))
            .or_else(|| holders().next())
            .map(|(index, _)| index)
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.peers[self.holding(entity)?].guard(entity)
    }
}
