//! Shared configuration types for the GKG service.
//!
//! All config struct definitions live here so that any crate in the workspace
//! can depend on this lightweight crate without pulling in heavy runtime
//! dependencies. Runtime methods that need external crates (e.g.
//! `ClickHouseConfiguration::build_client()` or `TlsConfig::load_tls_config()`)
//! stay in the crates that own the runtime logic.

pub mod analytics;
pub mod app;
pub mod billing;
pub mod clickhouse;
pub mod engine;
pub mod features;
pub mod gitlab;
pub mod grpc;
pub mod health_check;
pub mod metrics;
pub mod nats;
pub mod query;
pub mod resources;
pub mod schema;
pub mod secret_file_source;
pub mod tls;

pub use analytics::{AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind};
pub use app::{AppConfig, ConfigError, SECRET_FILE_DIR, SharedAppConfig};
pub use billing::{BillingConfig, QuotaConfig};
pub use clickhouse::{ClickHouseConfiguration, ConfigurationError, ProfilingConfig};
pub use engine::{
    CodeBackfillSweepConfig, CodeIndexingPipelineConfig, CodeIndexingTaskHandlerConfig,
    DatalakeRetryConfig, EngineConfigError, EngineConfiguration, EntityHandlerConfig,
    GlobalDispatcherConfig, HandlersConfiguration, IndexerModule, MigrationCompletionConfig,
    NamespaceDeletionSchedulerConfig, NamespaceDispatcherConfig, RuntimeDefaultsReport,
    ScheduleConfig, ScheduleConfiguration, ScheduledTasksConfiguration, SiphonRouterConfig,
    StaleEdgeReconciliationConfig, SubscriptionConfig, TableCleanupConfig,
    derive_concurrency_groups, derive_max_concurrent_workers, derive_stream_block_size,
};
pub use features::{Feature, FeatureScope, FeaturesConfig};
pub use gitlab::{GitlabClientConfiguration, GitlabConfig, JwtConfig};
pub use grpc::GrpcConfig;
pub use health_check::{HealthCheckConfig, NamespaceTarget};
pub use metrics::{MetricsConfig, OtelConfig, PrometheusConfig};
pub use nats::NatsConfiguration;
pub use query::{CompilerDerivedSettings, PathResolverConfig, QueryConfig, QuerySettings};
pub use resources::ContainerResources;
pub use schema::{SchemaConfig, SchemaConfigError};
pub use tls::TlsConfig;
