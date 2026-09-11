use std::sync::{Arc, RwLock};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use opentelemetry::KeyValue;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::{read_active_version, table_prefix, version_tables_complete};
use orbit_server_config::{AppConfig, PathResolverConfig};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

use crate::pipeline::PathResolver;
use crate::serving_schema::ServingSchema;

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
        let loader = ServingSchemaLoader {
            graph,
            embedded,
            catalog,
            path_config: config.path_resolver.clone(),
        };
        let poll_interval = Duration::from_secs(config.schema.version_poll_interval_secs);
        tokio::spawn(watch_loop(watcher.clone(), loader, poll_interval, shutdown));
        watcher
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<ServingSchema>, Status> {
        self.current()
            .ok_or_else(|| Status::unavailable("Active schema is unavailable"))
    }

    fn current(&self) -> Option<Arc<ServingSchema>> {
        self.current
            .read()
            .expect("serving schema lock poisoned")
            .clone()
    }

    fn install(&self, schema: Option<Arc<ServingSchema>>) {
        let installed_version = schema.as_ref().map(|schema| schema.migration_version);
        let mut slot = self.current.write().expect("serving schema lock poisoned");
        let previous_version = slot.as_ref().map(|schema| schema.migration_version);
        *slot = schema;
        if installed_version != previous_version {
            info!(
                migration_version = installed_version,
                "serving schema changed"
            );
        }
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

struct ServingSchemaLoader {
    graph: Arc<ArrowClickHouseClient>,
    embedded: OntologyArchive,
    catalog: OntologyCatalog,
    path_config: PathResolverConfig,
}

impl ServingSchemaLoader {
    async fn load(&self, version: u32) -> anyhow::Result<Arc<ServingSchema>> {
        let ontology = Arc::new(
            self.load_ontology(version)
                .await?
                .with_schema_version_prefix(&table_prefix(version)),
        );
        let resolver =
            Arc::new(PathResolver::new(self.graph.clone(), &ontology, &self.path_config).await);
        ServingSchema::new(version, ontology, resolver).map(Arc::new)
    }

    async fn load_ontology(&self, version: u32) -> anyhow::Result<Ontology> {
        if version == self.embedded.schema_version() {
            return Ok(self.embedded.load_ontology()?);
        }
        Ok(self.catalog.load(version).await?.load_ontology()?)
    }
}

async fn watch_loop(
    watcher: Arc<SchemaWatcher>,
    loader: ServingSchemaLoader,
    poll_interval: Duration,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let poll = async {
            interval.tick().await;
            refresh(&watcher, &loader).await
        };
        tokio::select! {
            _ = shutdown.cancelled() => return,
            result = poll => {
                if let Err(error) = result {
                    warn!(%error, "serving schema refresh failed; retrying");
                }
            }
        }
    }
}

async fn refresh(watcher: &SchemaWatcher, loader: &ServingSchemaLoader) -> anyhow::Result<()> {
    let Some(active_version) = read_active_version(&loader.graph).await? else {
        watcher.install(None);
        return Ok(());
    };

    let candidate = match watcher
        .current()
        .filter(|schema| schema.migration_version == active_version)
    {
        Some(schema) => Ok(schema),
        None => loader.load(active_version).await,
    };
    let tables_complete = match &candidate {
        Ok(schema) => {
            version_tables_complete(&loader.graph, active_version, &schema.expected_table_names)
                .await?
        }
        Err(_) => false,
    };

    // A failure only clears the slot once the failed version is confirmed still
    // active. Otherwise a promotion mid-poll would clear a usable snapshot.
    if read_active_version(&loader.graph).await? != Some(active_version) {
        return Ok(());
    }
    let schema = match candidate {
        Ok(schema) => schema,
        Err(error) => {
            watcher.install(None);
            return Err(error.context(format!("active ontology v{active_version} unavailable")));
        }
    };
    watcher.install(tables_complete.then_some(schema));
    Ok(())
}

fn register_state_gauge(watcher: &Arc<SchemaWatcher>) {
    use orbit_observability::server::schema_watcher as spec;
    let watcher = Arc::downgrade(watcher);
    spec::STATE.build_observable_gauge_i64(&orbit_observability::meter(), move |observer| {
        let Some(watcher) = watcher.upgrade() else {
            return;
        };
        let ready = watcher.current().is_some();
        for (state, active) in [("ready", ready), ("pending", !ready)] {
            observer.observe(
                i64::from(active),
                &[KeyValue::new(spec::labels::STATE, state)],
            );
        }
    });
}
