//! Building the runtime configuration types from the generated schema types.

use std::collections::HashMap;
use std::net::{AddrParseError, SocketAddr};

use crate::analytics::{AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind};
use crate::app::AppConfig;
use crate::billing::{BillingAuthMode, BillingConfig, QuotaAuthMode, QuotaConfig};
use crate::clickhouse::{ClickHouseConfiguration, ProfilingConfig};
use crate::clickhouse_setup::ClickHouseSetupConfig;
use crate::engine::{
    CodeBackfillSweepConfig, CodeIndexingPipelineConfig, CodeIndexingTaskHandlerConfig,
    CronSchedule, DatalakeRetryConfig, EngineConfiguration, EntityHandlerConfig,
    GlobalDispatcherConfig, HandlersConfiguration, IndexerModule, MigrationCompletionConfig,
    NamespaceDeletionSchedulerConfig, NamespaceDispatcherConfig, ScheduleConfig,
    ScheduleConfiguration, ScheduledTasksConfiguration, SiphonRouterConfig,
    StaleEdgeReconciliationConfig, SubscriptionConfig, TableCleanupConfig,
};
use crate::features::{Feature, FeatureScope, FeaturesConfig};
use crate::gitlab::{GitlabConfig, JwtConfig};
use crate::grpc::GrpcConfig;
use crate::health_check::{HealthCheckConfig, NamespaceTarget};
use crate::metrics::{ExpandedLogsConfig, MetricsConfig, OtelConfig, PrometheusConfig};
use crate::nats::NatsConfiguration;
use crate::object_storage::{ObjectStorageAuth, ObjectStorageConfig, ObjectStorageProvider};
use crate::probe_server::ProbeServerConfig;
use crate::proto;
use crate::query::{QueryConfig, QuerySettings};
use crate::schema::SchemaConfig;
use crate::tls::{ListenerTlsConfig, TlsConfig};

/// A value the schema cannot constrain but the runtime types require.
///
/// Protobuf has no socket address, cron expression or closed feature-flag
/// name, so those are still parsed here and can still fail.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("{field} is not a socket address: {source}")]
    BindAddress {
        field: &'static str,
        #[source]
        source: AddrParseError,
    },
    #[error("schedule.tasks.{task}.cron is not a cron expression: {source}")]
    Cron {
        task: &'static str,
        #[source]
        source: croner::errors::CronError,
    },
    #[error("{field} is {value}, which is not one of its declared values")]
    UnknownEnum { field: &'static str, value: i32 },
    #[error("features.{0} is not a feature this release knows")]
    UnknownFeature(String),
    #[error("metrics.prometheus.port is {0}, which is not a port number")]
    PortRange(u32),
}

type Result<T> = std::result::Result<T, ConvertError>;

/// The schema marks every section required and the loader validates before
/// conversion, so a missing one means the two have drifted apart.
fn present<T>(value: Option<T>) -> T {
    value.expect("a section the schema marks required is set")
}

fn address(value: &str, field: &'static str) -> Result<SocketAddr> {
    value
        .parse()
        .map_err(|source| ConvertError::BindAddress { field, source })
}

fn cron(expression: String, task: &'static str) -> Result<ScheduleConfiguration> {
    CronSchedule::try_from(expression)
        .map(|cron| ScheduleConfiguration { cron })
        .map_err(|source| ConvertError::Cron { task, source })
}

impl TryFrom<proto::Config> for AppConfig {
    type Error = ConvertError;

    fn try_from(config: proto::Config) -> Result<Self> {
        Ok(Self {
            grpc_bind_address: address(&config.grpc_bind_address, "grpc_bind_address")?,
            jwt_clock_skew_secs: config.jwt_clock_skew_secs,
            health_check_url: config.health_check_url,
            nats: nats(present(config.nats))?,
            datalake: clickhouse(present(config.datalake)),
            graph: clickhouse(present(config.graph)),
            clickhouse_setup: clickhouse_setup(present(config.clickhouse_setup)),
            engine: engine(present(config.engine))?,
            gitlab: gitlab(present(config.gitlab)),
            schedule: schedule_config(present(config.schedule))?,
            health_check: health_check(present(config.health_check))?,
            metrics: metrics(present(config.metrics))?,
            probe_server: probe_server(present(config.probe_server))?,
            tls: tls(present(config.tls)),
            query: query(present(config.query)),
            grpc: grpc(present(config.grpc))?,
            schema: schema(present(config.schema)),
            analytics: analytics(present(config.analytics))?,
            billing: billing(present(config.billing))?,
            features: features(config.features)?,
            object_storage: object_storage(present(config.object_storage))?,
        })
    }
}

fn nats(value: proto::Nats) -> Result<NatsConfiguration> {
    Ok(NatsConfiguration {
        url: value.url,
        username: value.username,
        password: value.password,
        tls_ca_cert_path: value.tls_ca_cert_path,
        tls_cert_path: value.tls_cert_path,
        tls_key_path: value.tls_key_path,
        connection_timeout_secs: value.connection_timeout_secs,
        request_timeout_secs: value.request_timeout_secs,
        ack_wait_secs: value.ack_wait_secs,
        max_deliver: value.max_deliver,
        subscription_buffer_size: value.subscription_buffer_size as usize,
        consumer_name: value.consumer_name,
        batch_size: value.batch_size as usize,
        auto_create_streams: value.auto_create_streams,
        stream_replicas: value.stream_replicas as usize,
        stream_max_age_secs: value.stream_max_age_secs,
        stream_max_bytes: value.stream_max_bytes,
        stream_max_messages: value.stream_max_messages,
        fetch_expires_secs: value.fetch_expires_secs,
        consumer_inactive_threshold_secs: value.consumer_inactive_threshold_secs,
        release_gc_idle_threshold_secs: value.release_gc_idle_threshold_secs,
    })
}

fn clickhouse(value: proto::ClickHouse) -> ClickHouseConfiguration {
    let profiling = present(value.profiling);
    ClickHouseConfiguration {
        database: value.database,
        url: value.url,
        username: value.username,
        password: value.password,
        session_settings: value.session_settings,
        replicated: value.replicated,
        insert_settings: value.insert_settings,
        profiling: ProfilingConfig {
            enabled: profiling.enabled,
            explain: profiling.explain,
            query_log: profiling.query_log,
            processors: profiling.processors,
            instance_health: profiling.instance_health,
        },
    }
}

fn clickhouse_setup(value: proto::ClickHouseSetup) -> ClickHouseSetupConfig {
    ClickHouseSetupConfig {
        admin_username: value.admin_username,
        admin_password: value.admin_password,
        writer_password: value.writer_password,
        reader_password: value.reader_password,
        siphon_reader_password: value.siphon_reader_password,
    }
}

fn gitlab(value: proto::Gitlab) -> GitlabConfig {
    let jwt = present(value.jwt);
    GitlabConfig {
        base_url: value.base_url,
        jwt: JwtConfig {
            signing_key: jwt.signing_key,
            verifying_key: jwt.verifying_key,
        },
        resolve_host: value.resolve_host,
    }
}

fn tls(value: proto::Tls) -> TlsConfig {
    let internal = present(value.internal);
    TlsConfig {
        cert_path: value.cert_path,
        key_path: value.key_path,
        internal: ListenerTlsConfig {
            enabled: internal.enabled,
            cert_path: internal.cert_path,
            key_path: internal.key_path,
        },
    }
}

fn engine(value: proto::Engine) -> Result<EngineConfiguration> {
    let handlers = present(value.handlers);
    let entity = present(handlers.entity_handler);
    let code = present(handlers.code_indexing_task);
    let pipeline = present(code.pipeline);
    let retry = present(value.datalake_retry);

    Ok(EngineConfiguration {
        max_concurrent_workers: value.max_concurrent_workers.map(|workers| workers as usize),
        concurrency_groups: value
            .concurrency_groups
            .into_iter()
            .map(|(group, limit)| (group, limit as usize))
            .collect(),
        topics: value
            .topics
            .into_iter()
            .map(|(topic, config)| {
                (
                    topic,
                    SubscriptionConfig {
                        concurrency_group: config.concurrency_group,
                        max_attempts: config.max_attempts,
                        retry_interval_secs: config.retry_interval_secs,
                        dead_letter_on_exhaustion: config.dead_letter_on_exhaustion,
                        max_ack_pending: config.max_ack_pending,
                    },
                )
            })
            .collect(),
        handlers: HandlersConfiguration {
            entity_handler: EntityHandlerConfig {
                datalake_batch_size: entity.datalake_batch_size,
                batch_size_overrides: entity.batch_size_overrides,
                system_notes_resolve_lookup_batch_size: entity
                    .system_notes_resolve_lookup_batch_size
                    as usize,
            },
            code_indexing_task: CodeIndexingTaskHandlerConfig {
                pipeline: code_indexing_pipeline(pipeline),
            },
        },
        datalake_retry: DatalakeRetryConfig {
            halving_initial_block_size: retry.halving_initial_block_size,
            halving_min_block_size: retry.halving_min_block_size,
        },
        modules: value
            .modules
            .into_iter()
            .map(indexer_module)
            .collect::<Result<_>>()?,
    })
}

fn indexer_module(value: i32) -> Result<IndexerModule> {
    match proto::IndexerModule::try_from(value) {
        Ok(proto::IndexerModule::Sdlc) => Ok(IndexerModule::Sdlc),
        Ok(proto::IndexerModule::Code) => Ok(IndexerModule::Code),
        Ok(proto::IndexerModule::NamespaceDeletion) => Ok(IndexerModule::NamespaceDeletion),
        Err(_) => Err(ConvertError::UnknownEnum {
            field: "engine.modules",
            value,
        }),
    }
}

fn code_indexing_pipeline(value: proto::CodeIndexingPipeline) -> CodeIndexingPipelineConfig {
    CodeIndexingPipelineConfig {
        max_file_size_bytes: value.max_file_size_bytes,
        max_files: value.max_files as usize,
        max_total_bytes: value.max_total_bytes,
        max_repository_files: value.max_repository_files as usize,
        source_memory_budget_bytes: value.source_memory_budget_bytes,
        source_compress_spill: value.source_compress_spill,
        worker_threads: value.worker_threads as usize,
        max_concurrent_languages: value.max_concurrent_languages as usize,
        per_file_timeout_ms: value.per_file_timeout_ms,
        per_file_parse_timeout_ms: value.per_file_parse_timeout_ms,
        per_file_walk_timeout_ms: value.per_file_walk_timeout_ms,
        per_file_ssa_timeout_ms: value.per_file_ssa_timeout_ms,
        cross_file_resolve_timeout_ms: value.cross_file_resolve_timeout_ms,
        max_tree_depth: value.max_tree_depth as usize,
        max_cross_file_names: value.max_cross_file_names as usize,
        max_manifest_bytes: value.max_manifest_bytes,
        max_file_edges: value.max_file_edges as usize,
        job_timeout_secs: value.job_timeout_secs,
        fetch_concurrency: value.fetch_concurrency as usize,
        archive_storage_budget: value.archive_storage_budget as usize,
        archive_tenant_budget: value.archive_tenant_budget as usize,
        archive_lease_ttl_secs: value.archive_lease_ttl_secs,
        archive_lease_max_wait_secs: value.archive_lease_max_wait_secs,
        write_slice_rows: value.write_slice_rows as usize,
        write_max_concurrent: value.write_max_concurrent as usize,
        small_repo_max_files: value.small_repo_max_files as usize,
        small_indexing_slots: value.small_indexing_slots.map(|slots| slots as usize),
        big_indexing_slots: value.big_indexing_slots.map(|slots| slots as usize),
    }
}

fn schedule_config(value: proto::Schedule) -> Result<ScheduleConfig> {
    let tasks = present(value.tasks);
    let namespace = present(tasks.namespace);
    let siphon = present(tasks.siphon);
    let code_backfill = present(tasks.code_backfill);
    let stale_edge = present(tasks.stale_edge_reconciliation);

    Ok(ScheduleConfig {
        tasks: ScheduledTasksConfiguration {
            global: GlobalDispatcherConfig {
                schedule: cron(present(tasks.global).cron, "global")?,
            },
            namespace: NamespaceDispatcherConfig {
                schedule: cron(namespace.cron, "namespace")?,
                sweep_interval_secs: namespace.sweep_interval_secs,
            },
            siphon: SiphonRouterConfig {
                events_stream_name: siphon.events_stream_name,
                batch_size: siphon.batch_size as usize,
            },
            code_backfill: CodeBackfillSweepConfig {
                schedule: cron(code_backfill.cron, "code-backfill")?,
                publish_window: code_backfill.publish_window as usize,
                stale_sweeps_per_tick: code_backfill.stale_sweeps_per_tick as usize,
            },
            table_cleanup: TableCleanupConfig {
                schedule: cron(present(tasks.table_cleanup).cron, "table-cleanup")?,
            },
            namespace_deletion: NamespaceDeletionSchedulerConfig {
                schedule: cron(present(tasks.namespace_deletion).cron, "namespace-deletion")?,
            },
            migration_completion: MigrationCompletionConfig {
                schedule: cron(
                    present(tasks.migration_completion).cron,
                    "migration-completion",
                )?,
            },
            stale_edge_reconciliation: StaleEdgeReconciliationConfig {
                schedule: cron(stale_edge.cron, "stale-edge-reconciliation")?,
                lookback_secs: stale_edge.lookback_secs,
            },
        },
    })
}

fn health_check(value: proto::HealthCheck) -> Result<HealthCheckConfig> {
    Ok(HealthCheckConfig {
        bind_address: address(&value.bind_address, "health_check.bind_address")?,
        targets: value
            .targets
            .into_iter()
            .map(|target| NamespaceTarget {
                namespace: target.namespace,
                deployments: target.deployments,
                stateful_sets: target.stateful_sets,
            })
            .collect(),
    })
}

fn metrics(value: proto::Metrics) -> Result<MetricsConfig> {
    let expanded = present(value.expanded_logs);
    let otel = present(value.otel);
    let prometheus = present(value.prometheus);

    Ok(MetricsConfig {
        log_level: value.log_level,
        expanded_logs: ExpandedLogsConfig {
            global_override: expanded.global_override,
            root_namespace_ids: expanded.root_namespace_ids,
        },
        otel: OtelConfig {
            enabled: otel.enabled,
            endpoint: otel.endpoint,
        },
        prometheus: PrometheusConfig {
            enabled: prometheus.enabled,
            port: prometheus
                .port
                .map(|port| u16::try_from(port).map_err(|_| ConvertError::PortRange(port)))
                .transpose()?,
        },
    })
}

fn probe_server(value: proto::ProbeServer) -> Result<ProbeServerConfig> {
    Ok(ProbeServerConfig {
        bind_address: value
            .bind_address
            .map(|bind| address(&bind, "probe_server.bind_address"))
            .transpose()?,
    })
}

fn query(value: proto::QuerySettings) -> QuerySettings {
    QuerySettings {
        default: query_config(present(value.default)),
        overrides: value
            .overrides
            .into_iter()
            .map(|(name, config)| (name, query_config(config)))
            .collect(),
    }
}

fn query_config(value: proto::QueryConfig) -> QueryConfig {
    QueryConfig {
        max_execution_time: value.max_execution_time,
        max_memory_usage: value.max_memory_usage,
        max_bytes_to_read: value.max_bytes_to_read,
        max_rows_to_read: value.max_rows_to_read,
        max_rows_in_set: value.max_rows_in_set,
        max_ast_elements: value.max_ast_elements,
        max_expanded_ast_elements: value.max_expanded_ast_elements,
        max_ast_depth: value.max_ast_depth,
        use_query_cache: value.use_query_cache,
        query_cache_ttl: value.query_cache_ttl,
        compiler_derived: Default::default(),
    }
}

fn grpc(value: proto::Grpc) -> Result<GrpcConfig> {
    Ok(GrpcConfig {
        keepalive_interval_secs: value.keepalive_interval_secs,
        keepalive_timeout_secs: value.keepalive_timeout_secs,
        tcp_keepalive_secs: value.tcp_keepalive_secs,
        connection_window_size: value.connection_window_size,
        stream_window_size: value.stream_window_size,
        concurrency_limit: value.concurrency_limit as usize,
        max_connection_age_secs: value.max_connection_age_secs,
        max_connection_age_grace_secs: value.max_connection_age_grace_secs,
        stream_timeout_secs: value.stream_timeout_secs,
        max_header_list_size_bytes: value.max_header_list_size_bytes,
        result_chunk_bytes: value.result_chunk_bytes as usize,
        max_result_bytes: value.max_result_bytes as usize,
    })
}

fn schema(value: proto::Schema) -> SchemaConfig {
    SchemaConfig {
        max_retained_versions: value.max_retained_versions,
        version_poll_interval_secs: value.version_poll_interval_secs,
        indexer_schema_wait_timeout_secs: value.indexer_schema_wait_timeout_secs,
    }
}

fn analytics(value: proto::Analytics) -> Result<AnalyticsConfig> {
    let deployment = present(value.deployment);
    Ok(AnalyticsConfig {
        enabled: value.enabled,
        collector_url: value.collector_url,
        deployment: DeploymentConfig {
            kind: match proto::DeploymentKind::try_from(deployment.r#type) {
                Ok(proto::DeploymentKind::SelfManaged) => DeploymentKind::SelfManaged,
                Ok(proto::DeploymentKind::Com) => DeploymentKind::Com,
                Ok(proto::DeploymentKind::Dedicated) => DeploymentKind::Dedicated,
                Err(_) => {
                    return Err(ConvertError::UnknownEnum {
                        field: "analytics.deployment.type",
                        value: deployment.r#type,
                    });
                }
            },
            environment: match proto::DeploymentEnvironment::try_from(deployment.environment) {
                Ok(proto::DeploymentEnvironment::Development) => DeploymentEnvironment::Development,
                Ok(proto::DeploymentEnvironment::Staging) => DeploymentEnvironment::Staging,
                Ok(proto::DeploymentEnvironment::Production) => DeploymentEnvironment::Production,
                Err(_) => {
                    return Err(ConvertError::UnknownEnum {
                        field: "analytics.deployment.environment",
                        value: deployment.environment,
                    });
                }
            },
        },
    })
}

fn billing(value: proto::Billing) -> Result<BillingConfig> {
    let quota = present(value.quota);
    Ok(BillingConfig {
        enabled: value.enabled,
        collector_url: value.collector_url,
        auth_mode: match proto::BillingAuthMode::try_from(value.auth_mode) {
            Ok(proto::BillingAuthMode::Oidc) => BillingAuthMode::Oidc,
            Ok(proto::BillingAuthMode::CloudConnector) => BillingAuthMode::CloudConnector,
            Err(_) => {
                return Err(ConvertError::UnknownEnum {
                    field: "billing.auth_mode",
                    value: value.auth_mode,
                });
            }
        },
        quota: QuotaConfig {
            enabled: quota.enabled,
            customers_dot_url: quota.customers_dot_url,
            auth_mode: match proto::QuotaAuthMode::try_from(quota.auth_mode) {
                Ok(proto::QuotaAuthMode::AdminToken) => QuotaAuthMode::AdminToken,
                Ok(proto::QuotaAuthMode::LicenseChecksum) => QuotaAuthMode::LicenseChecksum,
                Err(_) => {
                    return Err(ConvertError::UnknownEnum {
                        field: "billing.quota.auth_mode",
                        value: quota.auth_mode,
                    });
                }
            },
            api_user: quota.api_user,
            api_token: quota.api_token,
            request_timeout_ms: quota.request_timeout_ms,
            fallback_cache_ttl_secs: quota.fallback_cache_ttl_secs,
        },
    })
}

fn features(value: HashMap<String, proto::FeatureScope>) -> Result<FeaturesConfig> {
    value
        .into_iter()
        .map(|(name, scope)| {
            let feature: Feature = serde_json::from_value(serde_json::Value::String(name.clone()))
                .map_err(|_| ConvertError::UnknownFeature(name))?;
            Ok((
                feature,
                FeatureScope {
                    enabled: scope.enabled,
                    namespaces: scope.namespaces,
                },
            ))
        })
        .collect()
}

fn object_storage(value: proto::ObjectStorage) -> Result<ObjectStorageConfig> {
    Ok(ObjectStorageConfig {
        enabled: value.enabled,
        provider: match proto::ObjectStorageProvider::try_from(value.provider) {
            Ok(proto::ObjectStorageProvider::S3) => ObjectStorageProvider::S3,
            Ok(proto::ObjectStorageProvider::Gcs) => ObjectStorageProvider::Gcs,
            Ok(proto::ObjectStorageProvider::Local) => ObjectStorageProvider::Local,
            Err(_) => {
                return Err(ConvertError::UnknownEnum {
                    field: "object_storage.provider",
                    value: value.provider,
                });
            }
        },
        bucket: value.bucket,
        prefix: value.prefix,
        auth: match proto::ObjectStorageAuth::try_from(value.auth) {
            Ok(proto::ObjectStorageAuth::Identity) => ObjectStorageAuth::Identity,
            Ok(proto::ObjectStorageAuth::Static) => ObjectStorageAuth::Static,
            Err(_) => {
                return Err(ConvertError::UnknownEnum {
                    field: "object_storage.auth",
                    value: value.auth,
                });
            }
        },
        region: value.region,
        endpoint: value.endpoint,
        path_style: value.path_style,
        allow_http: value.allow_http,
        ca_cert_path: value.ca_cert_path,
        access_key_id: value.access_key_id,
        secret_access_key: value.secret_access_key,
        session_token: value.session_token,
        service_account_key: value.service_account_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use labkit_config::{Format, Loader};

    fn loader() -> Loader {
        Loader::new()
            .with_descriptors(proto::FILE_DESCRIPTOR_SET)
            .expect("the generated descriptor set registers")
    }

    fn defaults_as_json() -> Vec<u8> {
        let document: serde_json::Value = orbit_utils::yaml::from_str(crate::EMBEDDED_DEFAULTS)
            .expect("config/default.yaml parses");
        serde_json::to_vec(&document).expect("a parsed document re-serialises")
    }

    /// The schema covers `config/default.yaml` exactly: strict mode rejects any
    /// key the schema does not declare, so a field missing from the proto fails
    /// here rather than being dropped at startup.
    #[test]
    fn the_schema_declares_every_key_in_the_embedded_defaults() {
        loader()
            .with_strict_mode()
            .load_bytes::<proto::Config>(&defaults_as_json(), Format::Json)
            .expect("the embedded defaults load under strict mode");
    }

    /// The conversion reproduces what the current loader builds from the same
    /// file, field for field.
    #[test]
    fn converting_the_embedded_defaults_matches_the_current_loader() {
        let loaded = loader()
            .load_bytes::<proto::Config>(&defaults_as_json(), Format::Json)
            .expect("the embedded defaults load");

        let converted = AppConfig::try_from(loaded).expect("the embedded defaults convert");

        assert_eq!(
            serde_json::to_value(&converted).unwrap(),
            serde_json::to_value(AppConfig::embedded_defaults()).unwrap(),
        );
    }

    #[test]
    fn an_unparseable_bind_address_is_reported_with_its_field() {
        let error = address("not-an-address", "grpc_bind_address").unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("grpc_bind_address is not a socket address"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_feature_flag_is_rejected() {
        let error = features(HashMap::from([(
            "not_a_real_flag".to_owned(),
            proto::FeatureScope::default(),
        )]))
        .unwrap_err();

        assert!(matches!(error, ConvertError::UnknownFeature(name) if name == "not_a_real_flag"));
    }
}
