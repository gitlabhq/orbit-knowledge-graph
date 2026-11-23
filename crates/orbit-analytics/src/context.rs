//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! Data types are codegen'd from `config/schemas/iglu/<name>/<version>.json`
//! by `build.rs` via [`typify`] and included here at compile time. Each
//! generated module ([`orbit_common`], [`orbit_query`]) exposes the
//! typed struct (e.g. `OrbitCommon`) plus three `&'static str` consts:
//! `VERSION`, `SCHEMA_URI`, `SCHEMA_JSON`.
//!
//! Wrap a generated type with [`OrbitCommonContext`] or
//! [`OrbitQueryContext`] to attach it to a `StructuredEvent` — these
//! wrappers `impl labkit_events::SnowplowContext`.

use labkit_events::SnowplowContext;
use orbit_server_config::DeploymentKind;

include!(concat!(env!("OUT_DIR"), "/iglu_schemas.rs"));

fn cut_to_max_lengths(
    mut data: serde_json::Value,
    max_lengths: &[(&str, usize)],
) -> serde_json::Value {
    for (field, max_length) in max_lengths {
        match data.get_mut(*field) {
            Some(serde_json::Value::String(value)) => cut_to_length(value, *max_length),
            Some(serde_json::Value::Array(items)) => {
                for item in items {
                    if let serde_json::Value::String(value) = item {
                        cut_to_length(value, *max_length);
                    }
                }
            }
            _ => {}
        }
    }
    data
}

fn cut_to_length(value: &mut String, max_length: usize) {
    if let Some((cut, _)) = value.char_indices().nth(max_length) {
        value.truncate(cut);
    }
}

impl From<DeploymentKind> for orbit_common::OrbitCommonDeploymentType {
    fn from(kind: DeploymentKind) -> Self {
        match kind {
            DeploymentKind::Com => Self::Com,
            DeploymentKind::Dedicated => Self::Dedicated,
            DeploymentKind::SelfManaged => Self::SelfManaged,
        }
    }
}

pub fn validation<E: std::fmt::Display>(
    field: &'static str,
) -> impl FnOnce(E) -> labkit_events::Error {
    move |e| labkit_events::Error::Validation {
        field,
        message: e.to_string(),
    }
}

#[derive(Debug, Clone)]
pub struct OrbitCommonContext {
    pub data: orbit_common::OrbitCommon,
}

impl OrbitCommonContext {
    pub fn new(data: orbit_common::OrbitCommon) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCommonContext {
    fn schema(&self) -> &str {
        orbit_common::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data).expect("generated OrbitCommon is always serializable"),
            orbit_common::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitQueryContext {
    pub data: orbit_query::OrbitQuery,
}

impl OrbitQueryContext {
    pub fn new(data: orbit_query::OrbitQuery) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitQueryContext {
    fn schema(&self) -> &str {
        orbit_query::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data).expect("generated OrbitQuery is always serializable"),
            orbit_query::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitSdlcIndexingContext {
    pub data: orbit_sdlc_indexing::OrbitSdlcIndexing,
}

impl OrbitSdlcIndexingContext {
    pub fn new(data: orbit_sdlc_indexing::OrbitSdlcIndexing) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitSdlcIndexingContext {
    fn schema(&self) -> &str {
        orbit_sdlc_indexing::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitSdlcIndexing is always serializable"),
            orbit_sdlc_indexing::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitCodeIndexingContext {
    pub data: orbit_code_indexing::OrbitCodeIndexing,
}

impl OrbitCodeIndexingContext {
    pub fn new(data: orbit_code_indexing::OrbitCodeIndexing) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCodeIndexingContext {
    fn schema(&self) -> &str {
        orbit_code_indexing::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitCodeIndexing is always serializable"),
            orbit_code_indexing::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitCliContext {
    pub data: orbit_cli::OrbitCli,
}

impl OrbitCliContext {
    pub fn new(data: orbit_cli::OrbitCli) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCliContext {
    fn schema(&self) -> &str {
        orbit_cli::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data).expect("generated OrbitCli is always serializable"),
            orbit_cli::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitCliCommandContext {
    pub data: orbit_cli_command::OrbitCliCommand,
}

impl OrbitCliCommandContext {
    pub fn new(data: orbit_cli_command::OrbitCliCommand) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCliCommandContext {
    fn schema(&self) -> &str {
        orbit_cli_command::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitCliCommand is always serializable"),
            orbit_cli_command::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitCliSetupContext {
    pub data: orbit_cli_setup::OrbitCliSetup,
}

impl OrbitCliSetupContext {
    pub fn new(data: orbit_cli_setup::OrbitCliSetup) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCliSetupContext {
    fn schema(&self) -> &str {
        orbit_cli_setup::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitCliSetup is always serializable"),
            orbit_cli_setup::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitGraphStatsContext {
    pub data: orbit_graph_stats::OrbitGraphStats,
}

impl OrbitGraphStatsContext {
    pub fn new(data: orbit_graph_stats::OrbitGraphStats) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitGraphStatsContext {
    fn schema(&self) -> &str {
        orbit_graph_stats::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitGraphStats is always serializable"),
            orbit_graph_stats::MAX_LENGTHS,
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrbitNamespaceGraphStatsContext {
    pub data: orbit_namespace_graph_stats::OrbitNamespaceGraphStats,
}

impl OrbitNamespaceGraphStatsContext {
    pub fn new(data: orbit_namespace_graph_stats::OrbitNamespaceGraphStats) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitNamespaceGraphStatsContext {
    fn schema(&self) -> &str {
        orbit_namespace_graph_stats::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        cut_to_max_lengths(
            serde_json::to_value(&self.data)
                .expect("generated OrbitNamespaceGraphStats is always serializable"),
            orbit_namespace_graph_stats::MAX_LENGTHS,
        )
    }
}

// Re-exported for callers that need the bare URI string, e.g. assertions in
// observer and integration tests.
pub const ORBIT_COMMON_SCHEMA: &str = orbit_common::SCHEMA_URI;
pub const ORBIT_QUERY_SCHEMA: &str = orbit_query::SCHEMA_URI;
pub const ORBIT_SDLC_INDEXING_SCHEMA: &str = orbit_sdlc_indexing::SCHEMA_URI;
pub const ORBIT_CODE_INDEXING_SCHEMA: &str = orbit_code_indexing::SCHEMA_URI;
pub const ORBIT_CLI_SCHEMA: &str = orbit_cli::SCHEMA_URI;
pub const ORBIT_CLI_COMMAND_SCHEMA: &str = orbit_cli_command::SCHEMA_URI;
pub const ORBIT_CLI_SETUP_SCHEMA: &str = orbit_cli_setup::SCHEMA_URI;
pub const ORBIT_GRAPH_STATS_SCHEMA: &str = orbit_graph_stats::SCHEMA_URI;
pub const ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA: &str = orbit_namespace_graph_stats::SCHEMA_URI;

#[cfg(any(test, feature = "testkit"))]
const IGLU_SELF_DESCRIBING_BASE_DIALECT: &str = "http://json-schema.org/draft-04/schema#";

/// Return the inlined schema JSON for `name` at its pinned version.
///
/// Used by tests in this crate's consumers to compile an Iglu validator
/// against the same JSON the runtime emits contexts for.
#[cfg(any(test, feature = "testkit"))]
pub fn load_schema_json(name: &str) -> serde_json::Value {
    let raw = match name {
        "orbit_common" => orbit_common::SCHEMA_JSON,
        "orbit_query" => orbit_query::SCHEMA_JSON,
        "orbit_sdlc_indexing" => orbit_sdlc_indexing::SCHEMA_JSON,
        "orbit_code_indexing" => orbit_code_indexing::SCHEMA_JSON,
        "orbit_cli" => orbit_cli::SCHEMA_JSON,
        "orbit_cli_command" => orbit_cli_command::SCHEMA_JSON,
        "orbit_cli_setup" => orbit_cli_setup::SCHEMA_JSON,
        "orbit_graph_stats" => orbit_graph_stats::SCHEMA_JSON,
        "orbit_namespace_graph_stats" => orbit_namespace_graph_stats::SCHEMA_JSON,
        other => panic!("unknown iglu schema {other:?}"),
    };
    let mut schema: serde_json::Value =
        serde_json::from_str(raw).expect("vendored Iglu schema is valid JSON");
    schema["$schema"] = IGLU_SELF_DESCRIBING_BASE_DIALECT.into();
    schema
}

#[cfg(test)]
mod tests {
    use labkit_events::SnowplowContext;

    use super::{OrbitQueryContext, OrbitSdlcIndexingContext, orbit_query, orbit_sdlc_indexing};

    const OVER_LONG_ENTITY_TYPE: &str =
        "HAS_IDENTIFIER_siphon_vulnerability_occurrence_identifiers_VulnerabilityIdentifier";

    fn sdlc_context(entity_type: &str) -> OrbitSdlcIndexingContext {
        OrbitSdlcIndexingContext::new(orbit_sdlc_indexing::OrbitSdlcIndexing {
            namespace_id: None,
            root_namespace_id: None,
            entity_type: entity_type.to_owned(),
            indexing_mode: orbit_sdlc_indexing::OrbitSdlcIndexingIndexingMode::Full,
            dispatch_id: "00000000-0000-0000-0000-000000000000".to_owned(),
            campaign_id: None,
            read_rows: 0,
            read_bytes: 0,
            scanned_rows: 0,
            scanned_bytes: 0,
            written_rows: 0,
            written_bytes: 0,
            duration_ms: 0,
        })
    }

    #[test]
    fn over_long_value_is_cut_to_its_schema_limit() {
        let data = sdlc_context(OVER_LONG_ENTITY_TYPE).data();

        assert_eq!(data["entity_type"], OVER_LONG_ENTITY_TYPE[..64]);
    }

    #[test]
    fn value_within_its_schema_limit_is_unchanged() {
        let data = sdlc_context("VulnerabilityIdentifier").data();

        assert_eq!(data["entity_type"], "VulnerabilityIdentifier");
    }

    #[test]
    fn over_long_array_items_are_cut_to_their_schema_limit() {
        let query = OrbitQueryContext::new(orbit_query::OrbitQuery {
            entity_types: Some(vec!["x".repeat(100), "Project".to_owned()]),
            ..Default::default()
        });

        let data = query.data();

        assert_eq!(data["entity_types"][0], "x".repeat(64));
        assert_eq!(data["entity_types"][1], "Project");
    }

    #[test]
    fn multibyte_value_is_cut_on_a_character_boundary() {
        let data = sdlc_context(&"é".repeat(70)).data();

        assert_eq!(data["entity_type"], "é".repeat(64));
    }
}
