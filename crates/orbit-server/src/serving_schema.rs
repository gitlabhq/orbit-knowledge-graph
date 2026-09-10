use std::sync::Arc;

use named_queries::NamedQueries;
use ontology::Ontology;
use orbit_migrations::version::SCHEMA_VERSION;

use crate::pipeline::PathResolver;

#[derive(Clone)]
pub(crate) struct ServingSchema {
    pub ontology: Arc<Ontology>,
    pub migration_version: u32,
    pub named_queries: Arc<NamedQueries>,
    pub path_resolver: Option<Arc<PathResolver>>,
}

impl ServingSchema {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        Self {
            ontology,
            migration_version: *SCHEMA_VERSION,
            named_queries: Arc::new(
                NamedQueries::load_embedded()
                    .expect("embedded named queries are validated by the build script"),
            ),
            path_resolver: None,
        }
    }
}
