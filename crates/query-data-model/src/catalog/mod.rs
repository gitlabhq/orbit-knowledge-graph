mod access;
mod backend;
mod diagnostics;
mod frontend;
mod lookups;
mod store;

pub use access::{
    Access, EntityAccess, Guard, PathHop, PropertyAccess, Redaction, RowFilter, Scope,
};
pub use backend::{Catalog, RouteInput, SourceInput, StorageBackend, StoreInput};
pub use diagnostics::{Diagnostics, SpecError};
pub use frontend::Unresolved;
pub use lookups::{Lookup, Lookups};
pub use store::{DeclaredRoute, Source, Store};

use std::collections::HashSet;

use sha2::{Digest as _, Sha256};

use crate::spec::Fragment;
use crate::{DataModelError, GraphCatalog, PropertyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelId([u8; 32]);

#[derive(Debug)]
pub struct Model {
    id: ModelId,
    graph: GraphCatalog,
    access: Access,
    lookups: Lookups,
    stores: Vec<Store>,
}

impl Model {
    pub fn from_yaml<'a>(
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, DataModelError> {
        Self::load(&Fragment::parse_all(sources)?)
    }

    pub fn load(fragments: &[Fragment]) -> Result<Self, DataModelError> {
        let mut diagnostics = Diagnostics::default();
        let (mut graphs, mut authorizations, mut storages, mut services) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for fragment in fragments {
            match fragment {
                Fragment::Graph(graph) => graphs.push(graph.clone()),
                Fragment::Authorization(access) => authorizations.push(access),
                Fragment::Storage(storage) => storages.push(storage.as_ref()),
                Fragment::Service(service) => services.push(service),
            }
        }

        let graph = GraphCatalog::from_spec(graphs, &mut diagnostics);
        let access = Access::resolve(&graph, &authorizations, &mut diagnostics);
        let lookups = Lookups::resolve(&graph, &services, &mut diagnostics);
        let served: HashSet<PropertyId> = lookups.served().collect();
        let mut stores = store::resolve(&graph, &storages, &served, &mut diagnostics);
        access.resolve_guards(&graph, &mut stores, &mut diagnostics);
        diagnostics.finish()?;
        Ok(Self {
            id: identity(fragments)?,
            graph,
            access,
            lookups,
            stores,
        })
    }

    pub fn id(&self) -> ModelId {
        self.id
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn access(&self) -> &Access {
        &self.access
    }

    pub fn lookups(&self) -> &Lookups {
        &self.lookups
    }

    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        self.lookups.get(property)
    }

    pub fn stores(&self) -> &[Store] {
        &self.stores
    }
}

fn identity(fragments: &[Fragment]) -> Result<ModelId, DataModelError> {
    struct Digest<'a>(&'a mut Sha256);
    impl std::io::Write for Digest<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hasher = Sha256::new();
    serde_json::to_writer(Digest(&mut hasher), fragments)
        .map_err(|error| DataModelError::Invalid(error.to_string()))?;
    Ok(ModelId(hasher.finalize().into()))
}
