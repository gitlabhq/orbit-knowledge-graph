use std::sync::Arc;

use named_queries::{NamedQueries, NamedQuery};
use ontology::Ontology;
use orbit_migrations::schema::GraphSchema;
use query_engine::compiler::validate_normalize;
use tracing::warn;

use crate::pipeline::PathResolver;

#[derive(Clone)]
pub(crate) struct ServingSchema {
    pub ontology: Arc<Ontology>,
    pub migration_version: u32,
    pub expected_table_names: Vec<String>,
    pub named_queries: Arc<NamedQueries>,
    pub path_resolver: Arc<PathResolver>,
}

impl ServingSchema {
    pub fn new(
        migration_version: u32,
        ontology: Arc<Ontology>,
        path_resolver: Arc<PathResolver>,
    ) -> anyhow::Result<Self> {
        let mut named_queries = NamedQueries::load_embedded()?;
        named_queries.retain(|query| match fits_ontology(query, &ontology) {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    migration_version,
                    query = %query.name,
                    error,
                    "named query unavailable in serving schema"
                );
                false
            }
        });
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
            path_resolver,
        })
    }
}

fn fits_ontology(query: &NamedQuery, ontology: &Ontology) -> Result<(), String> {
    let rendered = query.render_example().map_err(|error| error.to_string())?;
    validate_normalize(&rendered, ontology).map_err(|error| error.to_string())?;
    Ok(())
}
