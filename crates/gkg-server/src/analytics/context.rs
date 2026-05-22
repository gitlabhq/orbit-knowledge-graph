use gkg_server_config::AnalyticsConfig;
use labkit_events::SnowplowContext;
use labkit_events::orbit::OrbitCommonContext;
use query_engine::compiler::QueryInfo;
use serde::Serialize;
use serde_json::json;

use crate::auth::Claims;

/// Iglu schema for the extended orbit_query context (2-0-2).
const ORBIT_QUERY_SCHEMA: &str = "iglu:com.gitlab/orbit_query/jsonschema/2-0-2";

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    claims: &Claims,
    schema_version: &str,
) -> Option<OrbitCommonContext> {
    let mut b = gkg_analytics::common_builder(config).schema_version(schema_version);
    if let Some(id) = labkit::correlation::current() {
        b = b.correlation_id(id.as_str());
    }
    if let Some(ref id) = claims.instance_id {
        b = b.instance_id(id);
    }
    if let Some(ref id) = claims.unique_instance_id {
        b = b.unique_instance_id(id);
    }
    if let Some(ref h) = claims.host_name {
        b = b.host_name(h);
    }
    if let Some(org) = claims.organization_id {
        b = b.organization_id(org as i64);
    }
    if let Some(ns) = claims.root_namespace_id {
        b = b.root_namespace_ids(vec![ns]);
    }
    b.build()
        .map_err(|e| tracing::warn!(error = %e, "drop analytics event: orbit_common build failed"))
        .ok()
}

/// Consumer-owned orbit_query context (schema 2-0-2).
///
/// Replaces the deprecated `labkit_events::orbit::OrbitQueryContext`. Includes
/// both the original auth/identity fields and the QueryInfo structural
/// dimensions added in 2-0-2.
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

/// Serialized subset -- only the fields we actually populate.
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
        let common = build_common(&AnalyticsConfig::default(), claims, "33").unwrap();
        let query = build_query(claims, tool, None, None);
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[1].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version).unwrap();
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
        let common = build_common(&AnalyticsConfig::default(), &claims, "33").unwrap();
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

        // Original auth fields still present.
        assert_eq!(data["source_type"], "mcp");
        assert_eq!(data["tool_name"], "query_graph");
        assert_eq!(data["queried_namespace_ids"][0], 22);

        // QueryInfo fields merged in.
        assert_eq!(data["query_type"], "traversal");
        assert_eq!(data["node_count"], 2);
        assert_eq!(data["entity_types"][0], "MergeRequest");
        assert_eq!(data["filter_ops"][0], "eq");
        assert_eq!(data["is_search"], false);
        assert_eq!(data["hydration_plan"], "static");
    }

    #[test]
    fn schema_is_2_0_2() {
        let claims = claims_with_paths(vec![]);
        let query = build_query(&claims, "query_graph", None, None);
        assert_eq!(
            query.schema(),
            "iglu:com.gitlab/orbit_query/jsonschema/2-0-2"
        );
    }
}
