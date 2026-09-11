use std::sync::{Arc, RwLock};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use named_queries::{NamedQueries, NamedQuery};
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use opentelemetry::KeyValue;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::schema::GraphSchema;
use orbit_migrations::version::{read_active_version, table_prefix, version_tables_complete};
use orbit_server_config::{AppConfig, PathResolverConfig};
use query_engine::compiler::validate_normalize;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

use crate::pipeline::PathResolver;

#[derive(Default)]
pub struct ActiveSchema {
    installed: RwLock<Option<Arc<SchemaSnapshot>>>,
}

impl ActiveSchema {
    pub fn spawn(
        graph: Arc<ArrowClickHouseClient>,
        embedded: OntologyArchive,
        catalog: OntologyCatalog,
        config: &AppConfig,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let active = Arc::new(Self::default());
        register_state_gauge(&active);
        let loader = SnapshotLoader {
            graph,
            embedded,
            catalog,
            path_resolver_config: config.path_resolver.clone(),
        };
        let poll_interval = Duration::from_secs(config.schema.version_poll_interval_secs);
        tokio::spawn(active.clone().follow(loader, poll_interval, shutdown));
        active
    }

    pub fn snapshot(&self) -> Result<Arc<SchemaSnapshot>, Status> {
        self.installed()
            .ok_or_else(|| Status::unavailable("Active schema is unavailable"))
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn pinned(ontology: Arc<Ontology>) -> Arc<Self> {
        use clickhouse_client::ClickHouseConfigurationExt;
        let config = AppConfig::embedded_defaults();
        let path_resolver = PathResolver::without_dictionaries(
            Arc::new(config.graph.build_client()),
            &ontology,
            &config.path_resolver,
        );
        let snapshot = SchemaSnapshot::new(
            *orbit_migrations::version::SCHEMA_VERSION,
            ontology,
            Arc::new(path_resolver),
        )
        .expect("pinned schema snapshot must load");
        Arc::new(Self {
            installed: RwLock::new(Some(Arc::new(snapshot))),
        })
    }

    async fn follow(
        self: Arc<Self>,
        loader: SnapshotLoader,
        poll_interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticks = tokio::time::interval(poll_interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let poll = async {
                ticks.tick().await;
                self.refresh(&loader).await
            };
            tokio::select! {
                _ = shutdown.cancelled() => return,
                result = poll => {
                    if let Err(error) = result {
                        warn!(%error, "active schema refresh failed; retrying");
                    }
                }
            }
        }
    }

    async fn refresh(&self, loader: &SnapshotLoader) -> anyhow::Result<()> {
        let Some(active_version) = read_active_version(&loader.graph).await? else {
            self.install(None);
            return Ok(());
        };

        let snapshot = match self
            .installed()
            .filter(|snapshot| snapshot.migration_version == active_version)
        {
            Some(current) => Ok(current),
            None => loader.load(active_version).await,
        };
        let tables_complete = match &snapshot {
            Ok(snapshot) => {
                let expected: Vec<String> = GraphSchema::from_ontology(&snapshot.ontology)
                    .table_names()
                    .into_iter()
                    .map(String::from)
                    .collect();
                version_tables_complete(&loader.graph, active_version, &expected).await?
            }
            Err(_) => false,
        };

        // A failure only clears the slot once the failed version is confirmed still
        // active. Otherwise a promotion mid-poll would clear a usable snapshot.
        if read_active_version(&loader.graph).await? != Some(active_version) {
            return Ok(());
        }
        match snapshot {
            Ok(snapshot) => {
                self.install(tables_complete.then_some(snapshot));
                Ok(())
            }
            Err(error) => {
                self.install(None);
                Err(error.context(format!("active ontology v{active_version} unavailable")))
            }
        }
    }

    fn installed(&self) -> Option<Arc<SchemaSnapshot>> {
        self.installed
            .read()
            .expect("active schema lock poisoned")
            .clone()
    }

    fn install(&self, snapshot: Option<Arc<SchemaSnapshot>>) {
        let next_version = snapshot.as_ref().map(|snapshot| snapshot.migration_version);
        let mut slot = self.installed.write().expect("active schema lock poisoned");
        let previous_version = slot.as_ref().map(|snapshot| snapshot.migration_version);
        *slot = snapshot;
        if next_version != previous_version {
            info!(migration_version = next_version, "active schema changed");
        }
    }
}

pub struct SchemaSnapshot {
    pub migration_version: u32,
    pub ontology: Arc<Ontology>,
    pub named_queries: Arc<NamedQueries>,
    pub path_resolver: Arc<PathResolver>,
}

impl SchemaSnapshot {
    fn new(
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
                    "named query unavailable in active schema"
                );
                false
            }
        });
        Ok(Self {
            migration_version,
            ontology,
            named_queries: Arc::new(named_queries),
            path_resolver,
        })
    }
}

struct SnapshotLoader {
    graph: Arc<ArrowClickHouseClient>,
    embedded: OntologyArchive,
    catalog: OntologyCatalog,
    path_resolver_config: PathResolverConfig,
}

impl SnapshotLoader {
    async fn load(&self, version: u32) -> anyhow::Result<Arc<SchemaSnapshot>> {
        let ontology = if version == self.embedded.schema_version() {
            self.embedded.load_ontology()?
        } else {
            self.catalog.load(version).await?.load_ontology()?
        };
        let ontology = Arc::new(ontology.with_schema_version_prefix(&table_prefix(version)));
        let path_resolver =
            PathResolver::new(self.graph.clone(), &ontology, &self.path_resolver_config).await;
        SchemaSnapshot::new(version, ontology, Arc::new(path_resolver)).map(Arc::new)
    }
}

fn fits_ontology(query: &NamedQuery, ontology: &Ontology) -> Result<(), String> {
    let rendered = query.render_example().map_err(|error| error.to_string())?;
    validate_normalize(&rendered, ontology).map_err(|error| error.to_string())?;
    Ok(())
}

fn register_state_gauge(active: &Arc<ActiveSchema>) {
    use orbit_observability::server::schema_watcher as spec;
    let active = Arc::downgrade(active);
    spec::STATE.build_observable_gauge_i64(&orbit_observability::meter(), move |observer| {
        let Some(active) = active.upgrade() else {
            return;
        };
        let ready = active.installed().is_some();
        for (state, holds) in [("ready", ready), ("pending", !ready)] {
            observer.observe(
                i64::from(holds),
                &[KeyValue::new(spec::labels::STATE, state)],
            );
        }
    });
}
