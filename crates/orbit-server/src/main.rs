#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use clickhouse_client::ClickHouseConfigurationExt;
use indexer::schema;
use indexer::{DispatcherConfig, IndexerConfig};
use orbit_billing::{QuotaService, SnowplowBillingTracker};
use orbit_log_fields::{info, warn};
use orbit_migrations::version::SCHEMA_VERSION;
use orbit_server::active_schema::ActiveSchema;
use orbit_server::analytics::{ConsentGate, GitlabInstanceWatch, SnowplowAnalyticsTracker};
use orbit_server::auth::JwtValidator;
use orbit_server::cli::{Args, Mode};
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::content;
use orbit_server::grpc::GrpcServer;
use orbit_server::health_check as health_check_mode;
use orbit_server::probes;
use orbit_server::shutdown;
use orbit_server_config::{AppConfig, IndexerModule};
use query_engine::compiler::input::QueryType;
use strum::VariantNames;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let aws_lc_version = orbit_server::fips::install_crypto_provider()?;

    let args = Args::parse();
    let config = AppConfig::load(args.config.as_deref())?;

    // Force-parse schema/format versions at boot so malformed version files
    // fail fast instead of per-request.
    let _ = orbit_server::tools::ToolService::build_query_dsl_version();
    std::sync::LazyLock::force(&query_engine::formatters::RAW_OUTPUT_FORMAT_VERSION);

    let invalid_keys = config.query.validate_keys(QueryType::VARIANTS);
    anyhow::ensure!(
        invalid_keys.is_empty(),
        "unknown query type(s) in config: {invalid_keys:?} (valid: {:?})",
        QueryType::VARIANTS,
    );
    orbit_server_config::query::init(config.query.clone());
    orbit_server_config::features::init(config.features.clone());

    let mut builder = labkit::Builder::new(args.mode.service_name())
        .propagate_correlation(true)
        .expanded_logs(orbit_server::expanded_logs::policy(
            &config.metrics.expanded_logs,
        ))
        .log_level(orbit_server::expanded_logs::log_directive(
            config.metrics.log_level.as_deref(),
        ));
    if config.metrics.otel.enabled && !config.metrics.otel.endpoint.is_empty() {
        builder = builder.otel_grpc_endpoint(&config.metrics.otel.endpoint);
    }
    let internal_tls = orbit_server::tls::load_internal(&config.tls)?;
    let probe_bind_address = config.probe_server_bind_address()?;
    builder = builder.health_bind(probe_bind_address);
    if config.metrics.prometheus.enabled {
        builder = builder.prometheus_metrics(probe_bind_address);
    }
    let active_schema = Arc::new(ActiveSchema::default());
    let serving = Arc::new(AtomicBool::new(false));
    for (name, check) in probes::readiness_checks(args.mode, &active_schema, &serving) {
        builder = builder.add_readiness_check(name, check);
    }
    builder = builder.probe_tls(internal_tls.clone());
    let mut guard = builder.init().expect("labkit init");
    orbit_server::expanded_logs::install_panic_hook();

    if config.metrics.prometheus.port.is_some() {
        warn!("metrics.prometheus.port is deprecated, use probe_server.bind_address");
    }

    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    ontology::constants::validate_ontology_constants(&ontology);

    info!(
        safe(SERVER_MODE) = args.mode.service_name(),
        safe(AWS_LC_VERSION) = aws_lc_version,
        "starting"
    );

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result: anyhow::Result<()> = async {
        match args.mode {
            Mode::ClickhouseSetup => orbit_server::clickhouse_setup::run(&config).await,
            Mode::DispatchIndexing => {
                config.schema.validate()?;
                let archive = ontology::archive::OntologyArchive::from_bytes(
                    *schema::version::SCHEMA_VERSION,
                    include_bytes!(env!("ONTOLOGY_ARCHIVE_PATH")),
                )?;

                let graph = config.graph.build_client("graph");
                info!("initializing schema version table");
                schema::version::init(&graph).await?;

                let dispatcher_config = DispatcherConfig::from(&config);
                indexer::run_dispatcher(&dispatcher_config, &archive, serving, shutdown)
                    .await
                    .map_err(Into::into)
            }
            Mode::HealthCheck => health_check_mode::run(&config, internal_tls)
                .await
                .map_err(Into::into),
            Mode::Indexer => {
                let indexer_config = IndexerConfig::from(&config);
                // The indexer runs no migration, so it indexes with the merged
                // model directly. With no providers this is the core ontology, so
                // default behavior is unchanged.
                let indexer_ontology = indexer_ontology(&config, ontology).await?;
                indexer::run(
                    &indexer_config,
                    indexer_ontology,
                    serving,
                    shutdown,
                    indexer_duckdb::sql_transform()?,
                )
                .await
                .map_err(Into::into)
            }
            Mode::Webserver => {
                config.schema.validate()?;
                config.grpc.validate()?;
                run_webserver(&config, active_schema, serving, shutdown.clone()).await
            }
        }
    }
    .await;

    signal_task.abort();
    if let Err(error) = &result {
        orbit_log_fields::error!(
            safe(ERROR_TYPE) = orbit_server::expanded_logs::anyhow_error_type(error),
            expanded(ERROR_MESSAGE) = %format_args!("{error:#}"),
            "server stopped with an error"
        );
    }
    guard.shutdown().await;
    if result.is_err() {
        drop(guard);
        std::process::exit(1);
    }
    Ok(())
}

/// The ontology the indexer registers handlers with. With configured providers
/// this is the core model merged with their packages, so extension entities are
/// indexed; with none it is the core model handed in, unchanged.
async fn indexer_ontology(
    config: &AppConfig,
    core: Arc<ontology::Ontology>,
) -> anyhow::Result<Arc<ontology::Ontology>> {
    if !config.extensions.is_enabled() {
        return Ok(core);
    }
    let model = extensions::bootstrap::load_provider_model(
        &extension_provider_configs(config),
        *SCHEMA_VERSION,
    )
    .await?;
    Ok(Arc::new(model.ontology))
}

fn extension_provider_configs(config: &AppConfig) -> Vec<extensions::provider::ProviderConfig> {
    config
        .extensions
        .providers
        .iter()
        .map(|provider| extensions::provider::ProviderConfig {
            name: provider.name.clone(),
            url: provider.url.clone(),
            jwt_key_file: provider.jwt_key_file.clone(),
            refresh_secs: provider.refresh_secs,
        })
        .collect()
}

async fn run_webserver(
    config: &AppConfig,
    active_schema: Arc<ActiveSchema>,
    serving: Arc<AtomicBool>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    orbit_billing::enforcement::validate(&config.billing)?;
    info!(
        safe(ENABLED) = orbit_billing::enforcement::ENFORCED,
        "billing enforcement"
    );

    let validator = Arc::new(JwtValidator::new(
        config.jwt_secret()?,
        config.jwt_clock_skew_secs,
    )?);

    // The webserver needs GitLab only to serve code or SDLC content. With neither
    // module enabled it runs with no GitLab client, no Gitaly resolver, and no
    // GitLab health overlay, so the demo has no GitLab dependency at all.
    let gitlab_required = config.engine.is_module_enabled(IndexerModule::Sdlc)
        || config.engine.is_module_enabled(IndexerModule::Code);
    let gitlab_client = match config.gitlab_client_config() {
        Some(client_config) => Some(Arc::new(
            gitlab_client::GitlabClient::new(client_config)
                .map_err(|e| anyhow::anyhow!("failed to create GitlabClient: {e}"))?,
        )),
        None if gitlab_required => {
            return Err(anyhow::anyhow!(
                "GitLab client config is required when the sdlc or code module is enabled: \
                 set gitlab.base_url and provide the JWT signing key"
            ));
        }
        None => None,
    };

    let graph = config.graph.build_client("graph");
    let cluster_health = ClusterHealthChecker::new(
        config.health_check_url.clone(),
        Some(graph.clone()),
        gitlab_client.clone(),
    )
    .into_arc();

    let mut resolver_registry = query_engine::shared::content::ColumnResolverRegistry::new();
    if let Some(client) = &gitlab_client {
        resolver_registry.register(
            "gitaly",
            Arc::new(content::gitaly::GitalyService::new(client.clone())),
        );
        info!("Content resolution enabled (GitlabClient configured)");
    } else {
        info!("Content resolution disabled: no GitLab module enabled");
    }

    info!("initializing NATS connection");
    let nats = Arc::new(
        nats_client::NatsClient::connect(&config.nats)
            .await
            .map_err(|e| anyhow::anyhow!("NATS connection failed: {e}"))?,
    );
    let archive = ontology::archive::OntologyArchive::from_bytes(
        *SCHEMA_VERSION,
        include_bytes!(env!("ONTOLOGY_ARCHIVE_PATH")),
    )?;
    let catalog = orbit_migrations::catalog::OntologyCatalog::open(nats).await?;
    ActiveSchema::spawn(
        &active_schema,
        Arc::new(graph.clone()),
        archive,
        catalog,
        config,
        shutdown.clone(),
    );

    let grpc_listener = tokio::net::TcpListener::bind(config.grpc_bind_address).await?;
    info!(
        // ast-grep-ignore: log-safe-field-is-plain -- a socket address from the Orbit config
        safe(TCP_ADDRESS) = config.grpc_bind_address.to_string(),
        "gRPC server bound"
    );

    let tls_config = orbit_server::tls::load_tls_config(&config.tls).await?;

    let mut grpc_server = GrpcServer::new(
        validator,
        active_schema,
        graph,
        cluster_health,
        tls_config,
        config.grpc.clone(),
        Arc::new(config.analytics.clone()),
    )
    .with_resolver_registry(Arc::new(resolver_registry));

    orbit_billing::register_metrics();
    orbit_billing::register_quota_metrics();
    let mut billing_tracker = None;
    if config.billing.enabled {
        if config.billing.collector_url.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "billing.enabled=true but billing.collector_url is empty"
            ));
        }
        info!(
            expanded(URL) = %config.billing.collector_url,
            safe(SNOWPLOW_APP_ID) = orbit_billing::constants::APP_ID,
            "initializing billing event tracker"
        );
        let billing_gitlab_client = gitlab_client.clone().ok_or_else(|| {
            anyhow::anyhow!("billing.enabled=true requires a GitLab client (set gitlab.base_url)")
        })?;
        let cc_token_cache = Some(Arc::new(gitlab_client::CloudConnectorTokenCache::new(
            billing_gitlab_client,
        )));
        let tracker = Arc::new(
            SnowplowBillingTracker::from_config(&config.billing, cc_token_cache)
                .map_err(|e| anyhow::anyhow!("billing tracker initialization failed: {e}"))?,
        );
        grpc_server = grpc_server.with_billing(tracker.clone());
        billing_tracker = Some(tracker);
    } else {
        info!("billing tracker disabled (billing.enabled=false): no events will be emitted");
    }

    if config.billing.quota.enabled {
        config.billing.quota.validate()?;
        info!(
            expanded(URL) = %config.billing.quota.customers_dot_url,
            safe(AUTH_MODE) = <&str>::from(config.billing.quota.auth_mode),
            "initializing usage quota gate"
        );
        let quota = QuotaService::from_config(&config.billing)
            .map_err(|e| anyhow::anyhow!("quota service initialization failed: {e}"))?;
        grpc_server = grpc_server.with_quota(Arc::new(quota));
    }

    let mut analytics_tracker = None;
    let collector_url_missing = config.analytics.collector_url.trim().is_empty();
    if config.analytics.enabled == Some(true) && collector_url_missing {
        return Err(anyhow::anyhow!(
            "analytics.enabled=true but analytics.collector_url is empty"
        ));
    }
    if config.analytics.may_send_events() && !collector_url_missing {
        info!(
            expanded(URL) = %config.analytics.collector_url,
            safe(DEPLOYMENT_TYPE) = <&str>::from(config.analytics.deployment.kind),
            safe(DEPLOYMENT_ENVIRONMENT) = <&str>::from(config.analytics.deployment.environment),
            "initializing analytics event tracker"
        );
        let tracker = SnowplowAnalyticsTracker::from_config(&config.analytics)
            .map_err(|e| anyhow::anyhow!("analytics tracker initialization failed: {e}"))?;
        let instance = GitlabInstanceWatch::default();
        if config.analytics.follows_gitlab_setting() {
            let analytics_gitlab_client = gitlab_client.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "analytics follows the GitLab setting but no GitLab client is configured"
                )
            })?;
            instance.spawn_refresh(analytics_gitlab_client);
        }
        grpc_server = grpc_server.with_analytics(Arc::new(ConsentGate::new(
            Arc::new(tracker.clone()),
            &config.analytics,
            &instance,
        )));
        analytics_tracker = Some(tracker);
    }

    info!(
        // ast-grep-ignore: log-safe-field-is-plain -- a socket address from the Orbit config
        safe(TCP_ADDRESS) = config.grpc_bind_address.to_string(),
        "gRPC server starting"
    );
    serving.store(true, Ordering::Relaxed);

    let result = tokio::select! {
        res = grpc_server.run(grpc_listener) => res.map_err(Into::into),
        _ = shutdown.cancelled() => Ok(()),
    };

    tokio::join!(
        async {
            if let Some(tracker) = analytics_tracker {
                tracker.shutdown().await;
            }
        },
        async {
            if let Some(tracker) = billing_tracker {
                tracker.shutdown().await;
            }
        }
    );

    result
}
