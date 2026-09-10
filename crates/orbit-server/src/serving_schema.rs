use std::sync::Arc;

use named_queries::{BindingValues, NamedQueries};
use ontology::Ontology;
use orbit_migrations::schema::GraphSchema;
use query_engine::compiler::{Frontend, SecurityContext, compile};
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
        let bindings = BindingValues { current_user_id: 1 };
        let security = SecurityContext::new(1, vec!["1/".into()])?;
        named_queries.retain(|query| {
            let result = query
                .render(&bindings, &query.example_parameters())
                .map_err(anyhow::Error::from)
                .and_then(|rendered| {
                    compile(&rendered, Frontend::JsonDsl, &ontology, &security).map_err(Into::into)
                });
            if let Err(error) = &result {
                warn!(
                    migration_version,
                    query = %query.name,
                    %error,
                    "named query unavailable in serving schema"
                );
            }
            result.is_ok()
        });
        Ok(Self {
            expected_table_names: GraphSchema::from_ontology(&ontology)
                .tables
                .into_iter()
                .map(|table| table.name)
                .collect(),
            ontology,
            migration_version,
            named_queries: Arc::new(named_queries),
            path_resolver: Some(path_resolver),
        })
    }
}
