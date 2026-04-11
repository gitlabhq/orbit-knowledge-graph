use std::sync::Arc;

use clap::Parser;
use clickhouse_client::ClickHouseConfigurationExt;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::{Args, Mode};
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::content;
use gkg_server::grpc::GrpcServer;
use gkg_server::health_check as health_check_mode;
use gkg_server::shutdown;
use gkg_server::webserver::Server as HttpServer;
use gkg_server_config::AppConfig;
use indexer::schema;
use indexer::{DispatcherConfig, IndexerConfig};
use query_engine::compiler::input::QueryType;
use strum::VariantNames;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls CryptoProvider");

    let args = Args::parse();
    let config = AppConfig::load()?;

    let invalid_keys = config.query.validate_keys(QueryType::VARIANTS);
    anyhow::ensure!(
        invalid_keys.is_empty(),
        "unknown query type(s) in config: {invalid_keys:?} (valid: {:?})",
        QueryType::VARIANTS,
    );
    gkg_server_config::query::init(config.query.clone());

    let mut builder = labkit::Builder::new(args.mode.service_name())
        .propagate_correlation(true)
        .echo_response_header(true);
    if let Some(level) = config
        .metrics
        .log_level
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        builder = builder.log_level(level);
    }
    if config.metrics.otel.enabled && !config.metrics.otel.endpoint.is_empty() {
        builder = builder.otel_grpc_endpoint(&config.metrics.otel.endpoint);
    }
    if config.metrics.prometheus.enabled {
        builder = builder.prometheus_metrics_port(config.metrics.prometheus.port);
    }
    let _guard = builder.init().expect("labkit init");

    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    ontology::constants::validate_ontology_constants(&ontology);

    info!(mode = ?args.mode, "starting");

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result = match args.mode {
        Mode::DispatchIndexing => {
            config.schema.validate()?;
            let graph = config.graph.build_client();
            info!("initializing schema version table");
            schema::version::init(&graph).await?;

            let dispatcher_config = DispatcherConfig {
                nats: config.nats.clone(),
                graph: config.graph.clone(),
                datalake: config.datalake.clone(),
                schedule: config.schedule.clone(),
                health_bind_address: config.dispatcher_health_bind_address,
            };
            indexer::run_dispatcher(&dispatcher_config, &ontology, shutdown)
                .await
                .map_err(Into::into)
        }
        Mode::HealthCheck => health_check_mode::run(&config).await.map_err(Into::into),
        Mode::Indexer => {
            let indexer_config = IndexerConfig {
                nats: config.nats.clone(),
                graph: config.graph.clone(),
                datalake: config.datalake.clone(),
                engine: config.engine.clone(),
                gitlab: config.gitlab_client_config(),
                schedule: config.schedule.clone(),
                health_bind_address: config.indexer_health_bind_address,
                schema: config.schema.clone(),
            };
            indexer::run(&indexer_config, ontology, shutdown)
                .await
                .map_err(Into::into)
        }
        Mode::Webserver => {
            config.schema.validate()?;
            let graph = config.graph.build_client();
            info!("initializing schema version table");
            schema::version::init(&graph).await?;
            run_webserver(&config, ontology).await
        }
    };

    signal_task.abort();

    result
}

async fn run_webserver(
    config: &AppConfig,
    ontology: Arc<ontology::Ontology>,
) -> anyhow::Result<()> {
    let validator = Arc::new(JwtValidator::new(
        config.jwt_secret()?,
        config.jwt_clock_skew_secs,
    )?);

    let cluster_health = ClusterHealthChecker::new(config.health_check_url.clone()).into_arc();

    let gitlab_client_config = config.gitlab_client_config().ok_or_else(|| {
        anyhow::anyhow!(
            "GitLab client config is required: set gitlab.base_url and provide \
             the JWT signing key (via config or /etc/secrets/gitlab/jwt/signing_key)"
        )
    })?;
    let gitlab_client = Arc::new(
        gitlab_client::GitlabClient::new(gitlab_client_config)
            .map_err(|e| anyhow::anyhow!("failed to create GitlabClient: {e}"))?,
    );

    let mut registry = query_engine::shared::content::ColumnResolverRegistry::new();
    registry.register(
        "gitaly",
        Arc::new(content::gitaly::GitalyContentService::new(
            gitlab_client.clone(),
        )),
    );
    let resolver_registry = Some(Arc::new(registry));
    info!("Content resolution enabled (GitlabClient configured)");

    let graph_client = config.graph.build_client();
    let http_server =
        HttpServer::bind(config.bind_address, graph_client, Some(gitlab_client)).await?;
    info!(addr = %config.bind_address, "HTTP server bound");

    let tls_config = gkg_server::tls::load_tls_config(&config.tls).await?;

    let grpc_server = GrpcServer::new(
        config.grpc_bind_address,
        validator,
        ontology,
        &config.graph,
        cluster_health,
        tls_config,
        resolver_registry,
        config.grpc.clone(),
    );
    info!(addr = %config.grpc_bind_address, "gRPC server starting");

    tokio::select! {
        res = http_server.run() => res.map_err(Into::into),
        res = grpc_server.run() => res.map_err(Into::into),
    }
}
