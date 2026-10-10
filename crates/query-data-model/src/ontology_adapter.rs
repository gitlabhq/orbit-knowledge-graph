use std::sync::Arc;

use crate::implementations::{clickhouse, duckdb};
use crate::storage::relational::Schema;
use crate::{
    DataModel, DataModelError, GitLabAuthzCatalog, GitLabPolicy, GraphCatalog, OrbitQueryModel,
    Relational, RelationalBackend, RelationalMapping, Storage, TrustedLocalCatalog,
};

pub trait FromOntology: Sized {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError>;
}

pub struct OntologyModel<B: RelationalBackend, A> {
    ontology: Arc<ontology::Ontology>,
    model: DataModel<Relational<B>, A>,
}

impl<B: RelationalBackend, A> OntologyModel<B, A> {
    pub fn ontology(&self) -> &Arc<ontology::Ontology> {
        &self.ontology
    }

    pub fn graph(&self) -> &GraphCatalog {
        self.model.graph()
    }

    pub fn backend(&self) -> &B::Mapping {
        self.model.backend()
    }

    pub fn storage(&self) -> &Storage<Relational<B>> {
        self.model.storage()
    }

    pub fn authorization(&self) -> &A {
        self.model.authorization()
    }
}

impl<B, A> OntologyModel<B, A>
where
    B: RelationalBackend,
    Storage<Relational<B>>: FromOntology,
    A: FromOntology,
{
    pub fn derive(ontology: Arc<ontology::Ontology>) -> Result<Self, DataModelError> {
        let graph = GraphCatalog::derive(&ontology)?;
        let storage = Storage::from_ontology(&ontology, &graph)?;
        let authorization = A::from_ontology(&ontology, &graph)?;
        Ok(Self {
            ontology,
            model: DataModel::new(graph, storage, authorization),
        })
    }
}

impl<B, A> OrbitQueryModel for OntologyModel<B, A>
where
    B: RelationalBackend,
    B::Mapping: RelationalMapping,
    A: GitLabPolicy,
{
    type BackendCatalog = B::Mapping;
    type AuthorizationCatalog = A;

    fn ontology(&self) -> &ontology::Ontology {
        &self.ontology
    }
    fn graph(&self) -> &GraphCatalog {
        self.model.graph()
    }
    fn query_backend(&self) -> &B::Mapping {
        self.model.backend()
    }
    fn query_authorization(&self) -> &A {
        self.model.authorization()
    }
}

impl FromOntology for Storage<Relational<clickhouse::storage::ClickHouse>> {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let schema = Schema::<clickhouse::storage::ClickHouse>::derive(ontology)?;
        let mapping = clickhouse::mapping::derive(ontology, graph, &schema)?;
        Ok(Self::new(schema, mapping))
    }
}

impl FromOntology for Storage<Relational<duckdb::storage::DuckDb>> {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let schema = Schema::<duckdb::storage::DuckDb>::derive(ontology);
        let mapping = duckdb::mapping::derive(ontology, graph, &schema)?;
        Ok(Self::new(schema, mapping))
    }
}

impl FromOntology for GitLabAuthzCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        GitLabAuthzCatalog::from_ontology(ontology, graph)
    }
}

impl FromOntology for TrustedLocalCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        TrustedLocalCatalog::from_ontology(ontology, graph)
    }
}
