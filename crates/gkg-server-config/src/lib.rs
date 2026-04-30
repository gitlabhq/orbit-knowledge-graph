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
pub mod circuit_breaker;
pub mod clickhouse;
pub mod engine;

pub mod gitlab;
pub mod grpc;
pub mod health_check;
pub mod metrics;
pub mod nats;
pub mod query;
pub mod schema;
pub mod secret_file_source;
pub mod tls;

pub use analytics::{AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind};
pub use app::{AppConfig, ConfigError, SECRET_FILE_DIR, SharedAppConfig};
pub use billing::BillingConfig;
pub use circuit_breaker::{CircuitBreakerConfig, ServiceCircuitBreakerConfig};
pub use clickhouse::{ClickHouseConfiguration, ConfigurationError, ProfilingConfig};
pub use engine::{
    CodeIndexingPipelineConfig, CodeIndexingTaskHandlerConfig, DatalakeRetryConfig,
    EngineConfigError, EngineConfiguration, GlobalDispatcherConfig, GlobalHandlerConfig,
    HandlerConfiguration, HandlersConfiguration, IndexerModule, MigrationCompletionConfig,
    NamespaceCodeBackfillDispatcherConfig, NamespaceDeletionHandlerConfig,
    NamespaceDeletionSchedulerConfig, NamespaceDispatcherConfig, NamespaceHandlerConfig,
    ScheduleConfig, ScheduleConfiguration, ScheduledTasksConfiguration,
    SiphonCodeIndexingTaskDispatcherConfig, TableCleanupConfig,
};
pub use gitlab::{GitlabClientConfiguration, GitlabConfig, JwtConfig};
pub use grpc::GrpcConfig;
pub use health_check::{HealthCheckConfig, NamespaceTarget};
pub use metrics::{MetricsConfig, OtelConfig, PrometheusConfig};
pub use nats::NatsConfiguration;
pub use query::{QueryConfig, QuerySettings};
pub use schema::{SchemaConfig, SchemaConfigError};
pub use tls::TlsConfig;
