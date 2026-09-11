use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::anyhow;
use clickhouse_client::ArrowClickHouseClient;
use futures::TryStreamExt;
use named_queries::{NamedQueries, NamedQuery};
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use opentelemetry::KeyValue;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::schema::GraphSchema;
use orbit_migrations::version::{read_active_version, table_prefix, version_tables_complete};
use orbit_server_config::{AppConfig, PathResolverConfig};
use query_engine::compiler::validate_normalize;
use tokio::time::sleep;
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
        let retry_backoff = Duration::from_secs(config.schema.version_poll_interval_secs);
        tokio::spawn(active.clone().follow(loader, retry_backoff, shutdown));
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
        retry_backoff: Duration,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                Err(error) = self.follow_active_version(&loader, retry_backoff) => {
                    warn!(%error, "active version watch lost; reopening");
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = sleep(retry_backoff) => {}
            }
        }
    }

    async fn follow_active_version(
        &self,
        loader: &SnapshotLoader,
        retry_backoff: Duration,
    ) -> anyhow::Result<Infallible> {
        let mut changes = loader.catalog.active_version_changes().await?;
        let mut target = match loader.active_version().await {
            Ok(version) => version,
            Err(error) => {
                warn!(%error, "active version unknown until the key is written");
                None
            }
        };
        loop {
            let next_change = match self.install_version(loader, target).await {
                Ok(()) => changes.try_next().await?,
                Err(error) => {
                    warn!(%error, "active schema unavailable; retrying");
                    tokio::select! {
                        change = changes.try_next() => change?,
                        _ = sleep(retry_backoff) => continue,
                    }
                }
            };
            target = next_change.ok_or_else(|| anyhow!("active version watch closed"))?;
        }
    }

    async fn install_version(
        &self,
        loader: &SnapshotLoader,
        version: Option<u32>,
    ) -> anyhow::Result<()> {
        let Some(version) = version else {
            self.install(None);
            return Ok(());
        };
        if self
            .installed()
            .is_some_and(|snapshot| snapshot.migration_version == version)
        {
            return Ok(());
        }
        match loader.load(version).await {
            Ok(snapshot) => {
                self.install(Some(snapshot));
                Ok(())
            }
            Err(error) => {
                self.install(None);
                Err(error.context(format!("active ontology v{version} unavailable")))
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
    /// The key is a cache of `gkg_schema_version`; before the first dispatcher
    /// that writes it runs, ClickHouse is the only place the answer exists.
    async fn active_version(&self) -> anyhow::Result<Option<u32>> {
        match self.catalog.active_version().await? {
            Some(version) => Ok(Some(version)),
            None => Ok(read_active_version(&self.graph).await?),
        }
    }

    async fn load(&self, version: u32) -> anyhow::Result<Arc<SchemaSnapshot>> {
        let ontology = if version == self.embedded.schema_version() {
            self.embedded.load_ontology()?
        } else {
            self.catalog.load(version).await?.load_ontology()?
        };
        let ontology = Arc::new(ontology.with_schema_version_prefix(&table_prefix(version)));
        let expected_tables: Vec<String> = GraphSchema::from_ontology(&ontology)
            .table_names()
            .into_iter()
            .map(String::from)
            .collect();
        if !version_tables_complete(&self.graph, version, &expected_tables).await? {
            return Err(anyhow!("v{version} tables are incomplete"));
        }
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
