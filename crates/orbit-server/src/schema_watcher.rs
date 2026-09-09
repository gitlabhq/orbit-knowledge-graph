use std::sync::{Arc, RwLock};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use named_queries::{BindingValues, NamedQueries};
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use opentelemetry::KeyValue;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::{read_active_version, table_prefix};
use orbit_server_config::{AppConfig, PathResolverConfig};
use query_engine::compiler::{Frontend, SecurityContext, compile};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

use crate::pipeline::PathResolver;

pub(crate) struct ServingSchema {
    pub migration_version: u32,
    pub ontology: Arc<Ontology>,
    pub named_queries: NamedQueries,
    pub path_resolver: Arc<PathResolver>,
}

impl ServingSchema {
    fn new(
        migration_version: u32,
        ontology: Arc<Ontology>,
        path_resolver: Arc<PathResolver>,
    ) -> anyhow::Result<Self> {
        let mut named_queries = NamedQueries::load_embedded()?;
        let bindings = BindingValues { current_user_id: 1 };
        let security = SecurityContext::new(1, vec!["1/".into()])?;
        named_queries.retain(|query| {
            let result = query.render(&bindings, &query.example_parameters())
                .map_err(anyhow::Error::from)
                .and_then(|rendered| compile(&rendered, Frontend::JsonDsl, &ontology, &security).map_err(Into::into));
            if let Err(error) = &result {
                warn!(migration_version, query = %query.name, %error, "named query unavailable in serving schema");
            }
            result.is_ok()
        });
        Ok(Self {
            migration_version,
            ontology,
            named_queries,
            path_resolver,
        })
    }
}

#[derive(Default)]
pub struct SchemaWatcher {
    current: RwLock<Option<Arc<ServingSchema>>>,
}

impl SchemaWatcher {
    pub fn spawn(
        graph: Arc<ArrowClickHouseClient>,
        embedded: OntologyArchive,
        catalog: OntologyCatalog,
        config: &AppConfig,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let watcher = Arc::new(Self::default());
        register_state_gauge(&watcher);
        let target = watcher.clone();
        let path_config = config.path_resolver.clone();
        let poll_interval = Duration::from_secs(config.schema.version_poll_interval_secs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = interval.tick() => {}
                }
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    result = target.refresh(&graph, &catalog, &embedded, &path_config) => {
                        if let Err(error) = result {
                            warn!(%error, "serving schema refresh failed; retrying");
                        }
                    }
                }
            }
        });
        watcher
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<ServingSchema>, Status> {
        self.current
            .read()
            .expect("serving schema lock poisoned")
            .clone()
            .ok_or_else(|| Status::unavailable("Active schema is unavailable"))
    }

    async fn refresh(
        &self,
        graph: &Arc<ArrowClickHouseClient>,
        catalog: &OntologyCatalog,
        embedded: &OntologyArchive,
        path_config: &PathResolverConfig,
    ) -> anyhow::Result<()> {
        let Some(active_version) = read_active_version(graph).await? else {
            *self.current.write().expect("serving schema lock poisoned") = None;
            return Ok(());
        };
        if self
            .snapshot()
            .is_ok_and(|schema| schema.migration_version == active_version)
        {
            return Ok(());
        }

        let candidate = async {
            let stored;
            let archive = if active_version == embedded.schema_version() {
                embedded
            } else {
                stored = catalog.load(active_version).await?;
                &stored
            };
            let ontology = Arc::new(
                archive
                    .load_ontology()?
                    .with_schema_version_prefix(&table_prefix(active_version)),
            );
            let resolver = Arc::new(PathResolver::new(graph.clone(), &ontology, path_config).await);
            ServingSchema::new(active_version, ontology, resolver)
        }
        .await;

        let confirmed_version = read_active_version(graph).await?;
        if confirmed_version != Some(active_version) {
            if confirmed_version.is_none() {
                *self.current.write().expect("serving schema lock poisoned") = None;
            }
            return Ok(());
        }
        let schema = match candidate {
            Ok(schema) => schema,
            Err(error) => {
                *self.current.write().expect("serving schema lock poisoned") = None;
                return Err(error.context(format!("active ontology v{active_version} unavailable")));
            }
        };
        *self.current.write().expect("serving schema lock poisoned") = Some(Arc::new(schema));
        info!(
            migration_version = active_version,
            "serving schema installed"
        );
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn fixed(ontology: Arc<Ontology>) -> Arc<Self> {
        use clickhouse_client::ClickHouseConfigurationExt;
        let config = AppConfig::embedded_defaults();
        let resolver = PathResolver::without_dictionaries(
            Arc::new(config.graph.build_client()),
            &ontology,
            &config.path_resolver,
        );
        let schema = ServingSchema::new(
            *orbit_migrations::version::SCHEMA_VERSION,
            ontology,
            Arc::new(resolver),
        )
        .expect("test serving schema must load");
        Arc::new(Self {
            current: RwLock::new(Some(Arc::new(schema))),
        })
    }
}

fn register_state_gauge(watcher: &Arc<SchemaWatcher>) {
    use orbit_observability::server::schema_watcher as spec;
    let watcher = Arc::downgrade(watcher);
    spec::STATE.build_observable_gauge_i64(&orbit_observability::meter(), move |observer| {
        let Some(watcher) = watcher.upgrade() else {
            return;
        };
        let ready = watcher.snapshot().is_ok();
        for (state, active) in [("ready", ready), ("pending", !ready)] {
            observer.observe(
                i64::from(active),
                &[KeyValue::new(spec::labels::STATE, state)],
            );
        }
    });
}
