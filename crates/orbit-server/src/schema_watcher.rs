use std::sync::{Arc, RwLock};
use std::time::Duration;

use clickhouse_client::ClickHouseConfigurationExt;
use ontology::archive::OntologyArchive;
use opentelemetry::KeyValue;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::read_active_version;
use orbit_server_config::AppConfig;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{info, warn};

use crate::grpc::OrbitServiceImpl;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    Pending,
    Ready,
}

impl SchemaState {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
        }
    }
}

pub struct SchemaWatcher {
    service: RwLock<Option<Arc<OrbitServiceImpl>>>,
}

impl SchemaWatcher {
    pub fn spawn(
        service: OrbitServiceImpl,
        embedded: OntologyArchive,
        catalog: OntologyCatalog,
        config: &AppConfig,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let watcher = Self::pending();
        register_state_gauge(&watcher);

        let graph = Arc::new(config.graph.build_client());
        let path_config = config.path_resolver.clone();
        let poll_interval = Duration::from_secs(config.schema.version_poll_interval_secs);
        let embedded_version = embedded.schema_version();
        let embedded_bytes = embedded.bytes().to_vec();
        let target = watcher.clone();

        tokio::spawn(async move {
            let mut service = Arc::new(service);
            let mut serving_version = None;
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = interval.tick() => {}
                }

                let version = match read_active_version(&graph).await {
                    Ok(Some(version)) => version,
                    Ok(None) => {
                        *target.service.write().expect("serving schema lock") = None;
                        serving_version = None;
                        continue;
                    }
                    Err(error) => {
                        warn!(%error, "cannot read active schema; keeping serving snapshot");
                        continue;
                    }
                };
                if serving_version == Some(version) {
                    continue;
                }

                let candidate = async {
                    let archive = if version == embedded_version {
                        OntologyArchive::from_bytes(version, &embedded_bytes)?
                    } else {
                        catalog.load(version).await?
                    };
                    service
                        .for_schema(&archive, graph.clone(), &path_config)
                        .await
                };
                let candidate = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    candidate = candidate => candidate,
                };

                match candidate {
                    Ok(candidate) => {
                        if !matches!(read_active_version(&graph).await, Ok(Some(active)) if active == version)
                        {
                            continue;
                        }
                        service = Arc::new(candidate);
                        serving_version = Some(version);
                        *target.service.write().expect("serving schema lock") =
                            Some(service.clone());
                        info!(version, "serving schema installed");
                    }
                    Err(error) => {
                        *target.service.write().expect("serving schema lock") = None;
                        serving_version = None;
                        warn!(version, %error, "active ontology unavailable; refusing queries");
                    }
                }
            }
        });

        watcher
    }

    pub fn pending() -> Arc<Self> {
        Arc::new(Self {
            service: RwLock::new(None),
        })
    }

    pub fn fixed(service: OrbitServiceImpl) -> Arc<Self> {
        Arc::new(Self {
            service: RwLock::new(Some(Arc::new(service))),
        })
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn for_state(state: SchemaState) -> Arc<Self> {
        if state == SchemaState::Pending {
            return Self::pending();
        }
        Self::fixed(OrbitServiceImpl::new(
            Arc::new(
                crate::auth::JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0)
                    .unwrap(),
            ),
            Arc::new(ontology::Ontology::load_embedded().unwrap()),
            &orbit_server_config::ClickHouseConfiguration::default(),
            crate::cluster_health::ClusterHealthChecker::default().into_arc(),
            orbit_server_config::GrpcConfig::default().stream_timeout_secs,
            Arc::new(orbit_server_config::AnalyticsConfig::default()),
        ))
    }

    pub fn current(&self) -> SchemaState {
        if self.service.read().expect("serving schema lock").is_some() {
            SchemaState::Ready
        } else {
            SchemaState::Pending
        }
    }

    pub fn serving(&self) -> Result<Arc<OrbitServiceImpl>, Status> {
        self.service
            .read()
            .expect("serving schema lock")
            .clone()
            .ok_or_else(|| Status::unavailable("active ontology archive is not ready"))
    }
}

fn register_state_gauge(watcher: &Arc<SchemaWatcher>) {
    use orbit_observability::server::schema_watcher as spec;
    let watcher = Arc::downgrade(watcher);
    spec::STATE.build_observable_gauge_i64(&orbit_observability::meter(), move |observer| {
        let Some(watcher) = watcher.upgrade() else {
            return;
        };
        for state in [SchemaState::Pending, SchemaState::Ready] {
            observer.observe(
                i64::from(watcher.current() == state),
                &[KeyValue::new(spec::labels::STATE, state.as_label())],
            );
        }
    });
}
