mod access;
mod diagnostics;
mod frontend;
mod store;

pub use access::{Access, EntityAccess, Guard, PropertyAccess, Redaction, RowFilter, Scope};
pub(crate) use diagnostics::Diagnostics;
pub use diagnostics::SpecError;
pub use frontend::Unresolved;
pub use store::{
    Column, EdgeTable, Engine, IndexKind, Lookup, Realization, Source, Store, Versioning,
};

use sha2::{Digest, Sha256};

use crate::spec::Fragment;
use crate::{DataModelError, GraphCatalog, PropertyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogId([u8; 32]);

#[derive(Debug)]
pub struct Catalog {
    id: CatalogId,
    graph: GraphCatalog,
    access: Access,
    stores: Vec<Store>,
}

impl Catalog {
    pub fn from_yaml<'a>(
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, DataModelError> {
        let mut diagnostics = Diagnostics::default();
        let fragments = sources
            .into_iter()
            .filter_map(|(name, yaml)| {
                Fragment::parse(yaml)
                    .map_err(|error| diagnostics.error(name, error.to_string()))
                    .ok()
            })
            .collect::<Vec<_>>();
        diagnostics.finish()?;
        Self::load(&fragments)
    }

    pub fn load(fragments: &[Fragment]) -> Result<Self, DataModelError> {
        let mut diagnostics = Diagnostics::default();
        let (mut graphs, mut authorizations, mut storages) = (Vec::new(), Vec::new(), Vec::new());
        for fragment in fragments {
            match fragment {
                Fragment::Graph(graph) => graphs.push(graph.clone()),
                Fragment::Authorization(access) => authorizations.push(access),
                Fragment::Storage(storage) => storages.push(storage.as_ref()),
            }
        }

        let graph = GraphCatalog::from_spec(graphs, &mut diagnostics);
        let access = Access::resolve(&graph, &authorizations, &mut diagnostics);
        let mut stores = store::resolve(&graph, &storages, &mut diagnostics);
        for store in &mut stores {
            access.resolve_guards(&graph, store, &mut diagnostics);
        }
        diagnostics.finish()?;
        Ok(Self {
            id: identity(fragments)?,
            graph,
            access,
            stores,
        })
    }

    pub fn id(&self) -> CatalogId {
        self.id
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn access(&self) -> &Access {
        &self.access
    }

    pub fn stores(&self) -> &[Store] {
        &self.stores
    }

    pub fn store(&self, name: &str) -> Option<&Store> {
        self.stores.iter().find(|store| store.name == name)
    }

    pub fn realizations(
        &self,
        property: PropertyId,
    ) -> impl Iterator<Item = (&Store, &Realization)> {
        self.stores
            .iter()
            .filter_map(move |store| Some((store, store.realization(property)?)))
    }
}

fn identity(fragments: &[Fragment]) -> Result<CatalogId, DataModelError> {
    let mut hasher = Sha256::new();
    for fragment in fragments {
        let yaml = fragment
            .to_yaml()
            .map_err(|error| DataModelError::Invalid(error.to_string()))?;
        hasher.update(yaml.as_bytes());
    }
    Ok(CatalogId(hasher.finalize().into()))
}
