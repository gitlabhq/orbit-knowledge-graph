//! The connections a deployment serves, each a peer that a stage of a query can run on, and the
//! placement of each read on one store of one of them.

use std::sync::Arc;

use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::{
    Catalog, ClickHouse, ConnectionId, DataModelError, DuckDb, Model, StorageBackend, StoreId,
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
    /// The connection `connection` of `model`, as the catalog of the kind its stores declare.
    pub fn of(model: &Arc<Model>, connection: ConnectionId) -> Result<Self, DataModelError> {
        Ok(match model.connection(connection).kind() {
            ClickHouse::KIND => Self::ClickHouse(Arc::new(model.physical(connection)?)),
            DuckDb::KIND => Self::DuckDb(Arc::new(model.physical(connection)?)),
            OrbitCode::KIND => Self::Remote(Target::Code(Arc::new(model.physical(connection)?))),
            Live::KIND => Self::Remote(Target::Live(Arc::new(model.physical(connection)?))),
            kind => {
                return Err(DataModelError::UnknownReference {
                    kind: "backend",
                    name: kind.to_string(),
                });
            }
        })
    }

    pub fn connection(&self) -> ConnectionId {
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

    /// Whether the connection evaluates scope filters and relationship steps in its own query.
    pub fn joins(&self) -> bool {
        let model = self.model();
        model.connection(self.connection()).capabilities().joins
    }

    /// The connection's name, for messages.
    pub fn name(&self) -> &str {
        self.model().connection(self.connection()).name()
    }

    pub fn model(&self) -> &Model {
        match self {
            Self::ClickHouse(catalog) => catalog.model(),
            Self::DuckDb(catalog) => catalog.model(),
            Self::Remote(target) => target.model(),
        }
    }
}

/// The store a read runs on, and the connection that store sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Place {
    pub connection: ConnectionId,
    pub store: StoreId,
}

/// Every connection a deployment serves.
#[derive(Debug, Clone)]
pub struct Connections {
    peers: Arc<Vec<Peer>>,
    ids: Arc<Vec<ConnectionId>>,
}

impl Connections {
    pub fn new(peers: Vec<Peer>) -> Self {
        let ids = peers.iter().map(Peer::connection).collect();
        Self {
            peers: Arc::new(peers),
            ids: Arc::new(ids),
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

    /// Every connection of a deployment's model, in the model's order, each as the peer of its
    /// kind.
    pub fn of(model: &Arc<Model>) -> Result<Self, DataModelError> {
        let peers = model
            .connections()
            .map(|connection| Peer::of(model, connection.id()))
            .collect::<Result<_, _>>()?;
        Ok(Self::new(peers))
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    /// The served connections, which logical plans across them read.
    pub fn ids(&self) -> &[ConnectionId] {
        &self.ids
    }

    pub fn get(&self, connection: ConnectionId) -> Option<&Peer> {
        self.peers
            .iter()
            .find(|peer| peer.connection() == connection)
    }

    pub fn target(&self, connection: ConnectionId) -> Option<&Target> {
        self.get(connection)?.remote()
    }
}
