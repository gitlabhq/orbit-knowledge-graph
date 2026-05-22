use gkg_server_config::AnalyticsConfig;
use labkit_events::SnowplowContext;
use query_engine::compiler::QueryInfo;
use serde::Serialize;

use crate::auth::Claims;

const ORBIT_COMMON_SCHEMA: &str = "iglu:com.gitlab/orbit_common/jsonschema/1-0-0";
const ORBIT_QUERY_SCHEMA: &str = "iglu:com.gitlab/orbit_query/jsonschema/2-0-2";

// ─────────────────────────────────────────────────────────────────────────────
// orbit_common
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct OrbitCommonContext {
    data: serde_json::Value,
}

impl SnowplowContext for OrbitCommonContext {
    fn schema(&self) -> &str {
        ORBIT_COMMON_SCHEMA
    }

    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
}

#[derive(Serialize)]
struct OrbitCommonData<'a> {
    deployment_type: &'a str,
    environment: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique_instance_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_namespace_ids: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_version: Option<&'a str>,
}

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    claims: &Claims,
    schema_version: &str,
) -> OrbitCommonContext {
    let correlation_id = labkit::correlation::current();

    let data = OrbitCommonData {
        deployment_type: gkg_analytics::deployment_type(config.deployment.kind),
        environment: gkg_analytics::deployment_env(config),
        correlation_id: correlation_id.as_ref().map(|id| id.as_str()),
        instance_id: claims.instance_id.as_deref(),
        unique_instance_id: claims.unique_instance_id.as_deref(),
        host_name: claims.host_name.as_deref(),
        organization_id: claims.organization_id.map(|id| id as i64),
        root_namespace_ids: claims.root_namespace_id.map(|ns| vec![ns]),
        schema_version: Some(schema_version),
    };

    OrbitCommonContext {
        data: serde_json::to_value(data).unwrap_or_default(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// orbit_query
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct OrbitQueryContext {
    data: serde_json::Value,
}

impl SnowplowContext for OrbitQueryContext {
    fn schema(&self) -> &str {
        ORBIT_QUERY_SCHEMA
    }

    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
}

#[derive(Serialize)]
struct OrbitQueryData<'a> {
    source_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coding_agent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queried_namespace_ids: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_namespace_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    global_user_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a str>,
}

pub(crate) fn build_query(
    claims: &Claims,
    tool_name: &str,
    coding_agent: Option<&str>,
    query_info: Option<&QueryInfo>,
) -> OrbitQueryContext {
    let queried = leaf_namespace_ids(claims);

    let base = OrbitQueryData {
        source_type: map_source(&claims.source_type),
        tool_name: Some(tool_name),
        coding_agent,
        queried_namespace_ids: if queried.is_empty() {
            None
        } else {
            Some(queried)
        },
        root_namespace_id: claims.root_namespace_id,
        global_user_id: claims.global_user_id.as_deref(),
        session_id: claims.ai_session_id.as_deref(),
    };

    let mut data = serde_json::to_value(base).unwrap_or_default();

    if let Some(info) = query_info
        && let Ok(serde_json::Value::Object(info_map)) = serde_json::to_value(info)
        && let serde_json::Value::Object(map) = &mut data
    {
        map.extend(info_map);
    }

    OrbitQueryContext { data }
}

fn leaf_namespace_ids(claims: &Claims) -> Vec<i64> {
    claims
        .group_traversal_ids
        .iter()
        .filter_map(|tp| gkg_utils::traversal_path::leaf_id(&tp.path))
        .collect()
}

fn map_source(s: &str) -> &'static str {
    match s {
        "frontend" => "frontend",
        "dws" => "dws",
        "mcp" => "mcp",
        "core" => "core",
        _ => "rest",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TraversalPathClaim;
    use gkg_server_config::AnalyticsConfig;
    use labkit_events::StructuredEvent;

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
            source_type: "mcp".into(),
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
        }
    }

    fn query_data(claims: &Claims, tool: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, "33");
        let query = build_query(claims, tool, None, None);
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[1].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version);
        let query = build_query(claims, "query_graph", None, None);
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
        let common = build_common(&AnalyticsConfig::default(), &claims, "33");
        let query = build_query(&claims, "query_graph", Some("claude-code"), None);
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
    fn map_source_recognises_all_jwt_values() {
        let cases = [
            ("frontend", "frontend"),
            ("dws", "dws"),
            ("mcp", "mcp"),
            ("core", "core"),
            ("rest", "rest"),
            ("anything-else", "rest"),
        ];
        for (input, expected) in cases {
            assert_eq!(map_source(input), expected, "for input {input}");
        }
    }

    #[test]
    fn build_common_sets_schema_version() {
        let claims = claims_with_paths(vec![]);
        let data = common_data(&claims, "33");
        assert_eq!(data["schema_version"], "33");
    }

    #[test]
    fn common_schema_is_1_0_0() {
        let claims = claims_with_paths(vec![]);
        let common = build_common(&AnalyticsConfig::default(), &claims, "33");
        assert_eq!(common.schema(), ORBIT_COMMON_SCHEMA);
    }

    #[test]
    fn query_info_fields_merged_into_context() {
        let claims = claims_with_paths(vec!["1/22/"]);
        let info = QueryInfo {
            query_type: "traversal",
            node_count: 2,
            relationship_count: 1,
            entity_types: vec!["MergeRequest".into(), "User".into()],
            relationship_types: vec!["AUTHORED".into()],
            filter_count: 1,
            filter_fields: vec!["state".into()],
            filter_ops: vec!["eq".into()],
            is_search: false,
            has_cursor: false,
            has_order_by: false,
            limit: 10,
            max_hops: 1,
            agg_functions: vec![],
            group_by_count: 0,
            hydration_plan: "static",
            dynamic_columns: "default",
            path_max_depth: None,
            has_variable_hops: false,
            has_virtual_columns: false,
        };
        let query = build_query(&claims, "query_graph", None, Some(&info));
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

    #[test]
    fn query_schema_is_2_0_2() {
        let claims = claims_with_paths(vec![]);
        let query = build_query(&claims, "query_graph", None, None);
        assert_eq!(query.schema(), ORBIT_QUERY_SCHEMA);
    }
}
