use std::sync::Arc;

use named_queries::NamedQueries;
use ontology::Ontology;
use orbit_migrations::schema::GraphSchema;
use tracing::warn;

use crate::pipeline::PathResolver;

#[derive(Clone)]
pub(crate) struct ServingSchema {
    pub ontology: Arc<Ontology>,
    pub migration_version: u32,
    pub expected_table_names: Vec<String>,
    pub named_queries: Arc<NamedQueries>,
    pub path_resolver: Option<Arc<PathResolver>>,
}

impl ServingSchema {
    pub fn new(
        migration_version: u32,
        ontology: Arc<Ontology>,
        path_resolver: Arc<PathResolver>,
    ) -> anyhow::Result<Self> {
        let mut named_queries = NamedQueries::load_embedded()?;
        for rejected in named_queries.retain_compilable(&ontology) {
            warn!(
                migration_version,
                %rejected,
                "named query unavailable in serving schema"
            );
        }
        let expected_table_names = GraphSchema::from_ontology(&ontology)
            .tables
            .into_iter()
            .map(|table| table.name)
            .collect();
        Ok(Self {
            ontology,
            migration_version,
            expected_table_names,
            named_queries: Arc::new(named_queries),
            path_resolver: Some(path_resolver),
        })
    }
}
