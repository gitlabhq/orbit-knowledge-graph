//! The executors the server registers with the DAG runner: ClickHouse on its graph connection,
//! and an executor opened at startup for every other connection its deployment shape serves.

use std::sync::{Arc, Mutex};

use clickhouse_client::ArrowClickHouseClient;
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, DuckDbError};
use orbit_server_config::ConnectionsConfig;
use query_data_model::{Catalog, ClickHouse, ConnectionId, DataModelError, Model};
use query_engine::compiler::dag::{Connections, Env, Frontend, Peer};
use query_engine::compiler::orbit_authorization::Checks;
use query_engine::compiler::{self, HydrationRequest};
use query_engine::pipeline::{PipelineError, QueryPipelineContext};
use query_engine::shared::dag::{DagError, Executed, Executor, Executors, Runner, Silent};

use super::execution::ClickHouseExecutor;
use crate::pipeline::correlation;

/// ClickHouse on the graph connection, under `label` and `log_comment`.
pub fn graph_executors(
    client: Arc<ArrowClickHouseClient>,
    data_model: &Catalog<ClickHouse>,
    label: &str,
    log_comment: String,
    settings: Vec<(String, String)>,
) -> Executors {
    let executor = ClickHouseExecutor {
        client,
        label: label.to_string(),
        log_comment,
        settings,
    };
    Executors::default().with(data_model.connection(), Arc::new(executor))
}

/// Why the server cannot serve a deployment shape.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error(transparent)]
    Model(#[from] DataModelError),
    #[error("connection {connection} is {backend}, and the server has no executor for it")]
    Unserved {
        connection: String,
        backend: &'static str,
    },
    #[error("connection {connection} is {backend}, and connections.{backend} has no entry for it")]
    Unconfigured {
        connection: String,
        backend: &'static str,
    },
    #[error(transparent)]
    Open(#[from] DuckDbError),
}

/// The executors of every connection a deployment shape serves next to its graph connection,
/// opened once at startup.
#[derive(Clone, Default)]
pub struct PeerExecutors(Vec<(ConnectionId, Arc<dyn Executor>)>);

impl PeerExecutors {
    /// Opens every connection of `model` but its graph connection, each from its entry in `config`.
    pub fn open(model: &Arc<Model>, config: &ConnectionsConfig) -> Result<Self, ServeError> {
        let graph = Catalog::<ClickHouse>::from_model(model)?;
        let mut peers = Vec::new();
        for peer in Connections::of(model)?.peers() {
            let connection = peer.connection();
            let name = model.connection(connection).name();
            let backend: &'static str = peer.backend().into();
            match peer {
                Peer::ClickHouse(_) if connection == graph.connection() => {}
                Peer::DuckDb(catalog) => {
                    let settings =
                        config
                            .duckdb
                            .get(name)
                            .ok_or_else(|| ServeError::Unconfigured {
                                connection: name.to_string(),
                                backend,
                            })?;
                    let client = DuckDbClient::open_s3(name, settings, catalog)?;
                    let executor: Arc<dyn Executor> = Arc::new(DuckDbExecutor {
                        client: Arc::new(Mutex::new(client)),
                    });
                    peers.push((connection, executor));
                }
                Peer::ClickHouse(_) | Peer::Remote(_) => {
                    return Err(ServeError::Unserved {
                        connection: name.to_string(),
                        backend,
                    });
                }
            }
        }
        Ok(Self(peers))
    }

    /// `graph`, with every peer executor next to it.
    pub fn with_graph(&self, graph: Executors) -> Executors {
        self.0
            .iter()
            .fold(graph, |executors, (connection, executor)| {
                executors.with(*connection, Arc::clone(executor))
            })
    }
}

/// The hydration stage of the query in `ctx`, on the rows its checks kept.
pub(crate) async fn hydrate(
    ctx: &QueryPipelineContext,
    request: HydrationRequest,
    kind: &str,
) -> Result<(compiler::ParameterizedQuery, Executed), PipelineError> {
    let data_model = ctx
        .server_extensions
        .get::<Arc<Catalog<ClickHouse>>>()
        .ok_or_else(|| PipelineError::custom("query data model missing"))?;
    let client = ctx
        .server_extensions
        .get::<Arc<ArrowClickHouseClient>>()
        .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
    let label = format!("hydration:{kind}");
    let log_comment = correlation::log_comment(Some(&label));
    let executors = graph_executors(
        Arc::clone(client),
        data_model,
        &label,
        log_comment,
        Vec::new(),
    );
    let frontend = Frontend::Server(Arc::clone(data_model));
    let connections = compiler::graph_only(data_model);
    let env = Env {
        frontend: &frontend,
        connections: &connections,
        grants: ctx.grants()?,
        limits: ctx.limits()?,
        trace: false,
    };
    let mut silent = Silent;
    let checks = Checks::default();
    let mut runner = Runner::new(env, &executors, &checks, &mut silent);
    runner.hydrate(request).await.map_err(|error| match error {
        DagError::Compile(error) => PipelineError::Compile {
            client_safe: false,
            reason: error.rejection_reason(),
            message: error.to_string(),
        },
        DagError::Execute(error) => PipelineError::Execution(error.0),
    })
}

#[cfg(test)]
mod tests {
    use clickhouse_client::ClickHouseConfigurationExt;
    use orbit_server_config::{
        AppConfig, DuckDbConnection, DuckDbLocation, S3Credentials, S3Endpoint, S3UrlStyle,
    };
    use query_data_model::Packages;
    use query_data_model::testkit::{deployments, with_demo};

    use super::*;

    fn jira() -> Arc<Model> {
        with_demo().deployed(&deployments(), "jira").unwrap()
    }

    fn lake(extension_directory: &std::path::Path) -> DuckDbConnection {
        DuckDbConnection {
            extension_directory: extension_directory.to_path_buf(),
            s3: S3Endpoint {
                endpoint: "127.0.0.1:9".into(),
                region: "us-east-1".into(),
                url_style: S3UrlStyle::Path,
                use_ssl: false,
            },
            credentials: S3Credentials {
                access_key_id: "test-key".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
            },
            location: DuckDbLocation::Parquet {
                bucket: "orbit-lake".into(),
                prefix: "jira/".into(),
            },
        }
    }

    fn refused(model: &Arc<Model>, config: &ConnectionsConfig) -> String {
        match PeerExecutors::open(model, config) {
            Ok(_) => panic!("the shape opened"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn the_server_shape_runs_on_its_graph_connection_alone() {
        let model = Packages::embedded().deployment("server").unwrap();
        let peers = PeerExecutors::open(&model, &ConnectionsConfig::default()).unwrap();
        assert!(peers.0.is_empty());
        let graph = Catalog::<ClickHouse>::from_model(&model).unwrap();
        let client = Arc::new(AppConfig::embedded_defaults().graph.build_client("graph"));
        let executors = peers.with_graph(graph_executors(
            client,
            &graph,
            "base",
            String::new(),
            Vec::new(),
        ));
        let connections = Connections::of(&model).unwrap();
        assert_eq!(connections.ids(), [graph.connection()]);
        executors.get(graph.connection()).unwrap();
    }

    #[test]
    fn a_code_or_live_connection_fails_startup() {
        let model = with_demo().deployment("demo").unwrap();
        assert_eq!(
            refused(&model, &ConnectionsConfig::default()),
            "connection code_indexing is orbit_code, and the server has no executor for it"
        );
    }

    #[test]
    fn a_duckdb_connection_without_its_config_entry_fails_startup() {
        assert_eq!(
            refused(&jira(), &ConnectionsConfig::default()),
            "connection jira_lake is duckdb, and connections.duckdb has no entry for it"
        );
    }

    #[test]
    fn a_duckdb_connection_opens_on_s3_from_its_config_entry() {
        let empty = tempfile::tempdir().unwrap();
        let config = ConnectionsConfig {
            duckdb: [("jira_lake".to_string(), lake(empty.path()))].into(),
        };
        let error = refused(&jira(), &config);
        assert!(
            error.starts_with(
                "connection jira_lake cannot load the httpfs and ducklake extensions: "
            ),
            "{error}"
        );
    }
}
