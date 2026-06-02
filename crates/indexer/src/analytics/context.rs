//! Builders that turn accumulated indexing stats into typed Snowplow
//! contexts. Mirrors the webserver's `analytics::context` but sources its
//! dimensions from the indexer config and dispatch rather than JWT claims.

use gkg_analytics::{
    OrbitCodeIndexingContext, OrbitCommonContext, OrbitSdlcIndexingContext, orbit_code_indexing,
    orbit_common, orbit_sdlc_indexing,
};
use gkg_server_config::{AnalyticsConfig, DeploymentKind};
use labkit_events::Error as LabkitError;

use crate::observer::IndexingMode;

/// What triggered an indexing run. The indexer cannot distinguish a
/// scheduled backfill from a manual one, so only `Push` and `Scheduled`
/// are derived today (a campaign-correlated dispatch is treated as
/// scheduled; everything else as push).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerType {
    Push,
    Scheduled,
}

fn validation<E: std::fmt::Display>(field: &'static str) -> impl FnOnce(E) -> LabkitError {
    move |e| LabkitError::Validation {
        field,
        message: e.to_string(),
    }
}

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    root_namespace_id: Option<i64>,
) -> Result<OrbitCommonContext, LabkitError> {
    let environment: &'static str = config.deployment.environment.into();

    Ok(OrbitCommonContext::new(orbit_common::OrbitCommon {
        deployment_type: deployment_type(config.deployment.kind),
        environment: environment
            .parse::<orbit_common::OrbitCommonEnvironment>()
            .map_err(validation("environment"))?,
        correlation_id: None,
        instance_id: None,
        unique_instance_id: None,
        host_name: None,
        organization_id: None,
        root_namespace_ids: root_namespace_id.map(|id| vec![id]),
        schema_version: None,
    }))
}

pub(crate) struct SdlcInputs {
    pub namespace_id: Option<i64>,
    pub root_namespace_id: Option<i64>,
    pub entity_type: String,
    pub indexing_mode: IndexingMode,
    pub dispatch_id: String,
    pub campaign_id: Option<String>,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
}

pub(crate) fn build_sdlc(inputs: SdlcInputs) -> Result<OrbitSdlcIndexingContext, LabkitError> {
    use orbit_sdlc_indexing::OrbitSdlcIndexingIndexingMode as Mode;

    Ok(OrbitSdlcIndexingContext::new(
        orbit_sdlc_indexing::OrbitSdlcIndexing {
            namespace_id: inputs.namespace_id,
            root_namespace_id: inputs.root_namespace_id,
            entity_type: inputs
                .entity_type
                .parse()
                .map_err(validation("entity_type"))?,
            indexing_mode: match inputs.indexing_mode {
                IndexingMode::Full => Mode::Full,
                IndexingMode::Incremental => Mode::Incremental,
            },
            dispatch_id: inputs
                .dispatch_id
                .parse()
                .map_err(validation("dispatch_id"))?,
            campaign_id: inputs
                .campaign_id
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(validation("campaign_id"))?,
            read_rows: inputs.read_rows as i64,
            read_bytes: inputs.read_bytes as i64,
            written_rows: inputs.written_rows as i64,
            written_bytes: inputs.written_bytes as i64,
            duration_ms: inputs.duration_ms as i64,
        },
    ))
}

pub(crate) struct CodeInputs {
    pub project_id: i64,
    pub namespace_id: Option<i64>,
    pub root_namespace_id: Option<i64>,
    pub branch: Option<String>,
    pub commit_sha: Option<String>,
    pub trigger_type: TriggerType,
    pub indexing_mode: IndexingMode,
    pub dispatch_id: String,
    pub campaign_id: Option<String>,
    pub files_discovered: u64,
    pub files_parsed: u64,
    pub files_skipped: u64,
    pub bytes_discovered: u64,
    pub directories_indexed: u64,
    pub definitions_indexed: u64,
    pub imported_symbols_indexed: u64,
    pub edges_indexed: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
}

pub(crate) fn build_code(inputs: CodeInputs) -> Result<OrbitCodeIndexingContext, LabkitError> {
    use orbit_code_indexing::OrbitCodeIndexingIndexingMode as Mode;
    use orbit_code_indexing::OrbitCodeIndexingTriggerType as Trigger;

    Ok(OrbitCodeIndexingContext::new(
        orbit_code_indexing::OrbitCodeIndexing {
            project_id: inputs.project_id,
            namespace_id: inputs.namespace_id,
            root_namespace_id: inputs.root_namespace_id,
            branch: inputs
                .branch
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(validation("branch"))?,
            commit_sha: inputs
                .commit_sha
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(validation("commit_sha"))?,
            trigger_type: match inputs.trigger_type {
                TriggerType::Push => Trigger::Push,
                TriggerType::Scheduled => Trigger::Scheduled,
            },
            indexing_mode: match inputs.indexing_mode {
                IndexingMode::Full => Mode::Full,
                IndexingMode::Incremental => Mode::Incremental,
            },
            dispatch_id: inputs
                .dispatch_id
                .parse()
                .map_err(validation("dispatch_id"))?,
            campaign_id: inputs
                .campaign_id
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(validation("campaign_id"))?,
            files_discovered: inputs.files_discovered as i64,
            files_parsed: inputs.files_parsed as i64,
            files_skipped: inputs.files_skipped as i64,
            bytes_discovered: inputs.bytes_discovered as i64,
            directories_indexed: inputs.directories_indexed as i64,
            definitions_indexed: inputs.definitions_indexed as i64,
            imported_symbols_indexed: inputs.imported_symbols_indexed as i64,
            edges_indexed: inputs.edges_indexed as i64,
            written_rows: inputs.written_rows as i64,
            written_bytes: inputs.written_bytes as i64,
            duration_ms: inputs.duration_ms as i64,
        },
    ))
}

fn deployment_type(kind: DeploymentKind) -> orbit_common::OrbitCommonDeploymentType {
    use orbit_common::OrbitCommonDeploymentType as DeploymentType;
    match kind {
        DeploymentKind::Com => DeploymentType::Com,
        DeploymentKind::Dedicated => DeploymentType::Dedicated,
        DeploymentKind::SelfManaged => DeploymentType::SelfManaged,
    }
}
