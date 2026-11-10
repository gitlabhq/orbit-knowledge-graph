use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::anyhow;
use clickhouse_client::ArrowClickHouseClient;
use futures::TryStreamExt;
use named_queries::{Language, NamedQueries, NamedQuery};
use opentelemetry::KeyValue;
use orbit_log_fields::{info, warn};
use orbit_migrations::archive::ModelArchive;
use orbit_migrations::catalog::ModelCatalog;
use orbit_migrations::schema::GraphSchema;
use orbit_migrations::version::{read_active_version, table_prefix, version_tables_complete};
use orbit_server_config::AppConfig;
use query_data_model::introspection::{IntrospectionScope, build_relationship_patterns};
use query_data_model::{Catalog, ClickHouse};
use query_engine::compiler::{Frontend, check};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::Status;

#[derive(Default)]
pub struct ActiveSchema {
    installed: RwLock<Option<Arc<SchemaSnapshot>>>,
}

impl ActiveSchema {
    pub fn spawn(
        active: &Arc<Self>,
        graph: Arc<ArrowClickHouseClient>,
        embedded: ModelArchive,
        catalog: ModelCatalog,
        config: &AppConfig,
        shutdown: CancellationToken,
    ) {
        register_state_gauge(active);
        let loader = SnapshotLoader {
            graph,
            embedded,
            catalog,
        };
        let retry_backoff = Duration::from_secs(config.schema.version_poll_interval_secs);
        tokio::spawn(active.clone().follow(loader, retry_backoff, shutdown));
    }

    pub fn snapshot(&self) -> Result<Arc<SchemaSnapshot>, Status> {
        self.installed()
            .ok_or_else(|| Status::unavailable("Active schema is unavailable"))
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn pinned(data_model: Arc<Catalog<ClickHouse>>) -> Arc<Self> {
        let snapshot = SchemaSnapshot::new(*orbit_migrations::version::SCHEMA_VERSION, data_model)
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
                    warn!(
                        safe(ERROR_TYPE) = crate::expanded_logs::anyhow_error_type(&error),
                        expanded(ERROR_MESSAGE) = %error,
                        "active version watch lost; reopening"
                    );
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
                warn!(
                    safe(ERROR_TYPE) = "active_version_read",
                    expanded(ERROR_MESSAGE) = %error,
                    "active version unknown; keeping the last installed snapshot"
                );
                self.installed().map(|snapshot| snapshot.migration_version)
            }
        };
        loop {
            let next_change = match self.install_version(loader, target).await {
                Ok(()) => changes.try_next().await?,
                Err(error) => {
                    warn!(
                        safe(ERROR_TYPE) = "schema_install",
                        expanded(ERROR_MESSAGE) = %format_args!("{error:#}"),
                        "active schema unavailable; retrying"
                    );
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
                Err(error.context(format!("active schema v{version} unavailable")))
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
            info!(
                safe(MIGRATION_VERSION) = next_version,
                "active schema changed"
            );
        }
    }
}

pub struct SchemaSnapshot {
    pub migration_version: u32,
    pub data_model: Arc<Catalog<ClickHouse>>,
    pub named_queries: Arc<NamedQueries>,
    pub relationship_patterns: String,
}

impl SchemaSnapshot {
    fn new(migration_version: u32, data_model: Arc<Catalog<ClickHouse>>) -> anyhow::Result<Self> {
        let mut named_queries = NamedQueries::load_embedded()?;
        named_queries.retain(|query| match fits_data_model(query, &data_model) {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    safe(MIGRATION_VERSION) = migration_version,
                    safe(QUERY_NAME) = query.name.as_str(),
                    safe(ERROR_TYPE) = "query_validation",
                    expanded(ERROR_MESSAGE) = %error,
                    "named query unavailable in active schema"
                );
                false
            }
        });
        let relationship_patterns =
            build_relationship_patterns(data_model.model(), IntrospectionScope::All).join("\n");
        Ok(Self {
            migration_version,
            data_model,
            named_queries: Arc::new(named_queries),
            relationship_patterns,
        })
    }
}

struct SnapshotLoader {
    graph: Arc<ArrowClickHouseClient>,
    embedded: ModelArchive,
    catalog: ModelCatalog,
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
        let catalogued;
        let archive = if version == self.embedded.schema_version() {
            &self.embedded
        } else {
            catalogued = self.catalog.load(version).await?;
            &catalogued
        };
        let prefix = table_prefix(version);
        let data_model = Arc::new(Catalog::<ClickHouse>::versioned(
            &archive.packages(),
            &prefix,
        )?);
        let physical = data_model.schema()?.clone();
        let expected_tables: Vec<String> = GraphSchema::new(physical, false)
            .table_names()
            .into_iter()
            .map(String::from)
            .collect();
        if !version_tables_complete(&self.graph, version, &expected_tables).await? {
            return Err(anyhow!("v{version} tables are incomplete"));
        }
        SchemaSnapshot::new(version, data_model).map(Arc::new)
    }
}

fn fits_data_model(
    query: &NamedQuery,
    data_model: &Arc<Catalog<ClickHouse>>,
) -> Result<(), String> {
    let json = query
        .render_example_language(Language::Json)
        .map_err(|error| error.to_string())?;
    check(&json, Frontend::JsonDsl, data_model).map_err(|error| error.to_string())?;
    let gql = query
        .render_example_language(Language::Gql)
        .map_err(|error| error.to_string())?;
    check(&gql, Frontend::Gql, data_model).map_err(|error| error.to_string())?;
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
