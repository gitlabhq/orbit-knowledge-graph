use std::time::Duration;

use gkg_analytics::{OrbitCommonContext, OrbitQueryContext, orbit_common, orbit_query};
use gkg_server_config::{AnalyticsConfig, DeploymentKind};
use labkit_events::Error as LabkitError;
use query_engine::compiler::ExecMetrics;

use crate::auth::{Claims, SourceType};

/// Map any `Display`-able conversion error to [`LabkitError::Validation`]
/// tagged with the schema field that produced it. Used by the typify-generated
/// bounded newtype `TryFrom` impls below.
fn validation<E: std::fmt::Display>(field: &'static str) -> impl FnOnce(E) -> LabkitError {
    move |e| LabkitError::Validation {
        field,
        message: e.to_string(),
    }
}

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    claims: &Claims,
    schema_version: &str,
) -> Result<OrbitCommonContext, LabkitError> {
    let environment: &'static str = config.deployment.environment.into();

    Ok(OrbitCommonContext::new(orbit_common::OrbitCommon {
        deployment_type: deployment_type(config.deployment.kind),
        environment: environment
            .parse::<orbit_common::OrbitCommonEnvironment>()
            .map_err(validation("environment"))?,
        correlation_id: labkit::correlation::current()
            .as_deref()
            .map(str::parse::<orbit_common::OrbitCommonCorrelationId>)
            .transpose()
            .map_err(validation("correlation_id"))?,
        instance_id: parse_opt(&claims.instance_id, "instance_id")?,
        unique_instance_id: parse_opt(&claims.unique_instance_id, "unique_instance_id")?,
        host_name: parse_opt(&claims.host_name, "host_name")?,
        organization_id: claims.organization_id.map(|id| id as i64),
        root_namespace_ids: claims.root_namespace_id.map(|ns| vec![ns]),
        schema_version: Some(
            schema_version
                .parse::<orbit_common::OrbitCommonSchemaVersion>()
                .map_err(validation("schema_version"))?,
        ),
    }))
}

pub(crate) fn build_query(
    claims: &Claims,
    tool_name: &str,
    coding_agent: Option<&str>,
    metrics: &ExecMetrics,
    row_count: usize,
    redacted_count: usize,
    total_elapsed: Duration,
) -> Result<OrbitQueryContext, LabkitError> {
    let queried = leaf_namespace_ids(claims);
    let info = metrics.query_info.as_ref();

    Ok(OrbitQueryContext::new(orbit_query::OrbitQuery {
        source_type: source_type(claims.source_type),
        tool_name: Some(
            tool_name
                .parse::<orbit_query::OrbitQueryToolName>()
                .map_err(validation("tool_name"))?,
        ),
        coding_agent: coding_agent
            .and_then(|a| a.parse::<orbit_query::OrbitQueryCodingAgent>().ok()),
        queried_namespace_ids: if queried.is_empty() {
            None
        } else {
            Some(queried)
        },
        root_namespace_id: claims.root_namespace_id,
        global_user_id: parse_opt_query(&claims.global_user_id, "global_user_id")?,
        session_id: claims
            .ai_session_id
            .as_deref()
            .map(str::parse::<orbit_query::OrbitQuerySessionId>)
            .transpose()
            .map_err(validation("session_id"))?,
        user_type: None,
        plan: None,
        is_gitlab_team_member: claims.is_gitlab_team_member,

        // QueryInfo dimensions
        query_type: info.and_then(|i| i.query_type.parse().ok()),
        node_count: info.map(|i| i.node_count as u64),
        relationship_count: info.map(|i| i.relationship_count as u64),
        entity_types: info.map(|i| i.entity_types.iter().filter_map(|s| s.parse().ok()).collect()),
        relationship_types: info.map(|i| i.relationship_types.iter().filter_map(|s| s.parse().ok()).collect()),
        filter_count: info.map(|i| i.filter_count as u64),
        filter_fields: info.map(|i| i.filter_fields.iter().filter_map(|s| s.parse().ok()).collect()),
        filter_ops: info.map(|i| i.filter_ops.iter().filter_map(|s| s.parse().ok()).collect()),
        agg_functions: info.map(|i| i.agg_functions.iter().filter_map(|s| s.parse().ok()).collect()),
        is_search: info.map(|i| i.is_search),
        has_cursor: info.map(|i| i.has_cursor),
        has_order_by: info.map(|i| i.has_order_by),
        limit: info.map(|i| i.limit as u64),
        max_hops: info.map(|i| i.max_hops as u64),
        group_by_count: info.map(|i| i.group_by_count as u64),
        hydration_plan: info.and_then(|i| i.hydration_plan.parse().ok()),
        dynamic_columns: info.and_then(|i| i.dynamic_columns.parse().ok()),
        path_max_depth: info.and_then(|i| i.path_max_depth.map(|d| d as u64)),
        has_variable_hops: info.map(|i| i.has_variable_hops),
        has_virtual_columns: info.map(|i| i.has_virtual_columns),

        // ExecMetrics
        duration_ms: Some(ExecMetrics::ms(total_elapsed)),
        compile_ms: metrics.compile_ms,
        execute_ms: metrics.execute_ms,
        authorization_ms: metrics.authorization_ms,
        hydration_ms: metrics.hydration_ms,
        row_count: Some(row_count as u64),
        redacted_count: Some(redacted_count as u64),
        ch_read_rows: Some(metrics.ch_read_rows),
        ch_read_bytes: Some(metrics.ch_read_bytes),
        ch_memory_usage: Some(metrics.ch_memory_usage),
    }))
}

/// Parse an optional `Claims` string into one of the orbit_common bounded
/// newtypes. The bounds are 255 chars for instance/host fields; if the
/// claim ever exceeds that, surface as a typed validation error rather
/// than truncate silently.
fn parse_opt<T>(value: &Option<String>, field: &'static str) -> Result<Option<T>, LabkitError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .as_deref()
        .map(str::parse::<T>)
        .transpose()
        .map_err(validation(field))
}

/// Same as [`parse_opt`] but for orbit_query newtypes (maxLength 64).
fn parse_opt_query(
    value: &Option<String>,
    field: &'static str,
) -> Result<Option<orbit_query::OrbitQueryGlobalUserId>, LabkitError> {
    parse_opt(value, field)
}

fn deployment_type(kind: DeploymentKind) -> orbit_common::OrbitCommonDeploymentType {
    use orbit_common::OrbitCommonDeploymentType as DT;
    match kind {
        DeploymentKind::Com => DT::Com,
        DeploymentKind::Dedicated => DT::Dedicated,
        DeploymentKind::SelfManaged => DT::SelfManaged,
    }
}

fn source_type(source: SourceType) -> orbit_query::OrbitQuerySourceType {
    use orbit_query::OrbitQuerySourceType as ST;
    match source {
        SourceType::Frontend => ST::Frontend,
        SourceType::Dws => ST::Dws,
        SourceType::Mcp => ST::Mcp,
        SourceType::Core => ST::Core,
        SourceType::Rest => ST::Rest,
        SourceType::CodeIntelligence => ST::CodeIntelligence,
    }
}

fn leaf_namespace_ids(claims: &Claims) -> Vec<i64> {
    claims
        .group_traversal_ids
        .iter()
        .filter_map(|tp| gkg_utils::traversal_path::leaf_id(&tp.path))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TraversalPathClaim;
    use labkit_events::StructuredEvent;
    use query_engine::compiler::QueryInfo;

    fn claims_with_paths(paths: Vec<&str>) -> Claims {
        Claims {
            sub: "u".into(),
            iss: "gitlab".into(),
            aud: "gkg".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "t".into(),
            admin: false,
            organization_id: Some(1),
            min_access_level: None,
            group_traversal_ids: paths
                .into_iter()
                .map(|p| TraversalPathClaim {
                    path: p.to_string(),
                    access_levels: vec![20],
                })
                .collect(),
            source_type: crate::auth::SourceType::Mcp,
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
            is_gitlab_team_member: None,
        }
    }

    fn query_data(claims: &Claims, tool: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, "33").unwrap();
        let query = build_query(claims, tool, None, &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[1].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version).unwrap();
        let query = build_query(claims, "query_graph", None, &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[0].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version).unwrap();
        let query = build_query(claims, "query_graph", None, &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[0].data.clone()
    }

    #[test]
    fn leaf_namespace_ids_extracts_last_segment() {
        let claims = claims_with_paths(vec!["1/22/", "1/33/", "2000271/122276018/"]);
        assert_eq!(leaf_namespace_ids(&claims), vec![22, 33, 122276018]);
    }

    #[test]
    fn leaf_namespace_ids_skips_unparseable_and_empty() {
        let claims = claims_with_paths(vec!["", "abc/", "1/22/", "/"]);
        assert_eq!(leaf_namespace_ids(&claims), vec![22]);
    }

    #[test]
    fn build_query_sets_queried_namespace_ids_when_paths_present() {
        let claims = claims_with_paths(vec!["1/22/", "1/33/"]);
        let data = query_data(&claims, "query_graph");
        let ids = data["queried_namespace_ids"].as_array().unwrap();
        assert_eq!(ids[0], 22);
        assert_eq!(ids[1], 33);
    }

    #[test]
    fn build_query_omits_queried_namespace_ids_when_empty() {
        let claims = claims_with_paths(vec![]);
        let data = query_data(&claims, "query_graph");
        assert!(data.get("queried_namespace_ids").is_none());
    }

    #[test]
    fn build_query_passes_through_tool_name() {
        let claims = claims_with_paths(vec![]);
        let data = query_data(&claims, "get_graph_schema");
        assert_eq!(data["tool_name"], "get_graph_schema");
    }

    #[test]
    fn build_query_passes_through_coding_agent() {
        let claims = claims_with_paths(vec![]);
        let common = build_common(&AnalyticsConfig::default(), &claims, "33").unwrap();
        let query = build_query(&claims, "query_graph", Some("claude-code"), &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        let data = event.contexts()[1].data.clone();
        assert_eq!(data["coding_agent"], "claude-code");
    }

    #[test]
    fn build_query_omits_coding_agent_when_none() {
        let claims = claims_with_paths(vec![]);
        let data = query_data(&claims, "query_graph");
        assert!(data.get("coding_agent").is_none());
    }

    #[test]
    fn build_query_drops_oversized_coding_agent() {
        let claims = claims_with_paths(vec![]);
        let query = build_query(&claims, "query_graph", Some(&"x".repeat(65)), &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
        assert!(query.data().get("coding_agent").is_none());
    }

    #[test]
    fn build_common_sets_schema_version() {
        let claims = claims_with_paths(vec![]);
        let data = common_data(&claims, "33");
        assert_eq!(data["schema_version"], "33");
    }

    #[test]
    fn build_common_rejects_oversized_instance_id() {
        // The Iglu maxLength=255 bound is enforced by the typify-generated
        // newtype, surfaced as labkit_events::Error::Validation.
        let mut claims = claims_with_paths(vec![]);
        claims.instance_id = Some("x".repeat(256));
        let err = build_common(&AnalyticsConfig::default(), &claims, "33").unwrap_err();
        assert!(
            matches!(
                err,
                LabkitError::Validation {
                    field: "instance_id",
                    ..
                }
            ),
            "expected Validation(instance_id), got: {err:?}"
        );
    }

    // ── Iglu schema validation ──────────────────────────────────────────

    mod iglu {
        use super::*;
        use std::sync::LazyLock;

        static ORBIT_COMMON_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
            let schema = gkg_analytics::load_schema_json("orbit_common");
            jsonschema::validator_for(&schema).expect("orbit_common schema compiles")
        });

        static ORBIT_QUERY_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
            let schema = gkg_analytics::load_schema_json("orbit_query");
            jsonschema::validator_for(&schema).expect("orbit_query schema compiles")
        });

        fn assert_valid(validator: &jsonschema::Validator, data: &serde_json::Value, label: &str) {
            let errors: Vec<_> = validator
                .iter_errors(data)
                .map(|e| format!("  - {e}"))
                .collect();
            if !errors.is_empty() {
                panic!(
                    "{label} failed Iglu schema validation:\n{}",
                    errors.join("\n")
                );
            }
        }

        #[test]
        fn common_context_validates_against_iglu_schema() {
            let claims = claims_with_paths(vec!["1/22/"]);
            let common = build_common(&AnalyticsConfig::default(), &claims, "33").unwrap();
            assert_valid(&ORBIT_COMMON_VALIDATOR, &common.data(), "orbit_common");
        }

        #[test]
        fn common_context_minimal_validates() {
            let claims = claims_with_paths(vec![]);
            let common = build_common(&AnalyticsConfig::default(), &claims, "33").unwrap();
            assert_valid(
                &ORBIT_COMMON_VALIDATOR,
                &common.data(),
                "orbit_common (minimal)",
            );
        }

        #[test]
        fn query_context_validates_against_iglu_schema() {
            let claims = claims_with_paths(vec!["1/22/"]);
        let query = build_query(&claims, "query_graph", Some("claude-code"), &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
            assert_valid(&ORBIT_QUERY_VALIDATOR, &query.data(), "orbit_query");
        }

        #[test]
        fn query_context_minimal_validates() {
            let claims = claims_with_paths(vec![]);
            let query = build_query(&claims, "query_graph", None, &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
            assert_valid(
                &ORBIT_QUERY_VALIDATOR,
                &query.data(),
                "orbit_query (minimal)",
            );
        }

        #[test]
        fn code_intelligence_validates_against_iglu_schema() {
            let mut claims = claims_with_paths(vec!["1/22/"]);
            claims.source_type = crate::auth::SourceType::CodeIntelligence;
            let query = build_query(&claims, "query_graph", None, &ExecMetrics::default(), 0, 0, Duration::ZERO).unwrap();
            assert_eq!(query.data()["source_type"], "code_intelligence");
            assert_valid(
                &ORBIT_QUERY_VALIDATOR,
                &query.data(),
                "orbit_query (code_intelligence)",
            );
        }
    }

    #[test]
    fn query_info_fields_merged_into_context() {
        let claims = claims_with_paths(vec!["1/22/"]);
        let info = QueryInfo {
            query_type: "traversal", node_count: 2, relationship_count: 1,
            entity_types: vec!["MergeRequest".into(), "User".into()],
            relationship_types: vec!["AUTHORED".into()],
            filter_count: 1, filter_fields: vec!["state".into()], filter_ops: vec!["eq".into()],
            is_search: false, has_cursor: false, has_order_by: false,
            limit: 10, max_hops: 1, agg_functions: vec![], group_by_count: 0,
            hydration_plan: "static", dynamic_columns: "default",
            path_max_depth: None, has_variable_hops: false, has_virtual_columns: false,
        };
        let metrics = ExecMetrics { query_info: Some(info), ..Default::default() };
        let query = build_query(&claims, "query_graph", None, &metrics, 0, 0, Duration::ZERO).unwrap();
        let data = query.data();

        assert_eq!(data["source_type"], "mcp");
        assert_eq!(data["tool_name"], "query_graph");
        assert_eq!(data["queried_namespace_ids"][0], 22);
        assert_eq!(data["query_type"], "traversal");
        assert_eq!(data["node_count"], 2);
        assert_eq!(data["entity_types"][0], "MergeRequest");
        assert_eq!(data["filter_ops"][0], "eq");
        assert_eq!(data["is_search"], false);
        assert_eq!(data["hydration_plan"], "static");
    }
}
