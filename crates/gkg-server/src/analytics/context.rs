use gkg_server_config::AnalyticsConfig;
use labkit_events::orbit::{OrbitCommonContext, OrbitQueryContext, SourceType, ToolName};

use crate::auth::Claims;

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

pub(crate) fn build_query(claims: &Claims, tool_name: ToolName) -> Option<OrbitQueryContext> {
    let mut b = OrbitQueryContext::builder(map_source(&claims.source_type)).tool_name(tool_name);
    if let Some(ref id) = claims.global_user_id {
        b = b.global_user_id(id);
    }
    if let Some(ref s) = claims.ai_session_id {
        b = b.session_id(s);
    }
    if let Some(ns) = claims.root_namespace_id {
        b = b.root_namespace_id(ns);
    }
    let queried = leaf_namespace_ids(claims);
    if !queried.is_empty() {
        b = b.queried_namespace_ids(queried);
    }
    b.build()
        .map_err(|e| tracing::warn!(error = %e, "drop analytics event: orbit_query build failed"))
        .ok()
}

/// Leaf namespace ID per scoped traversal path. A path like `"1/22/"` resolves
/// to `22`. Empty paths and unparseable segments are skipped; duplicates are
/// preserved in input order.
fn leaf_namespace_ids(claims: &Claims) -> Vec<i64> {
    claims
        .group_traversal_ids
        .iter()
        .filter_map(|tp| {
            tp.path
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<i64>().ok())
        })
        .collect()
}

fn map_source(s: &str) -> SourceType {
    match s {
        "dap" => SourceType::Dap,
        "mcp" => SourceType::Mcp,
        "cli" => SourceType::Cli,
        _ => SourceType::RestApi,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TraversalPathClaim;

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

    /// Inspect the JSON `data` of a context built via `build_*`. The
    /// `to_self_describing_json` method is `pub(crate)` in labkit_events, so
    /// we round-trip through `GkgEvent` and read its public `contexts()`.
    fn query_data(claims: &Claims, tool: ToolName) -> serde_json::Value {
        use labkit_events::gkg::GkgEvent;
        let common = build_common(&AnalyticsConfig::default(), claims, "33").unwrap();
        let query = build_query(claims, tool).unwrap();
        let event = GkgEvent::query_executed(common, query);
        event.contexts()[1].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        use labkit_events::gkg::GkgEvent;
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version).unwrap();
        let query = build_query(claims, ToolName::QueryGraph).unwrap();
        let event = GkgEvent::query_executed(common, query);
        event.contexts()[0].data.clone()
    }

    #[test]
    fn build_query_sets_queried_namespace_ids_when_paths_present() {
        let claims = claims_with_paths(vec!["1/22/", "1/33/"]);
        let data = query_data(&claims, ToolName::QueryGraph);
        let ids = data["queried_namespace_ids"].as_array().unwrap();
        assert_eq!(ids[0], 22);
        assert_eq!(ids[1], 33);
    }

    #[test]
    fn build_query_omits_queried_namespace_ids_when_empty() {
        let claims = claims_with_paths(vec![]);
        let data = query_data(&claims, ToolName::QueryGraph);
        assert!(data.get("queried_namespace_ids").is_none());
    }

    #[test]
    fn build_query_passes_through_tool_name() {
        let claims = claims_with_paths(vec![]);
        let data = query_data(&claims, ToolName::GetGraphSchema);
        assert_eq!(data["tool_name"], "get_graph_schema");
    }

    #[test]
    fn build_common_sets_schema_version() {
        let claims = claims_with_paths(vec![]);
        let data = common_data(&claims, "33");
        assert_eq!(data["schema_version"], "33");
    }
}
