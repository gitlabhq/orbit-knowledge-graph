use gkg_analytics::{OrbitCommonContext, OrbitCommonData, OrbitQueryContext, OrbitQueryData};
use gkg_server_config::AnalyticsConfig;

use crate::auth::Claims;

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    claims: &Claims,
    schema_version: &str,
) -> OrbitCommonContext {
    let correlation_id = labkit::correlation::current();

    OrbitCommonContext::new(OrbitCommonData {
        deployment_type: config.deployment.kind.into(),
        environment: config.deployment.environment.into(),
        correlation_id: correlation_id.as_deref(),
        instance_id: claims.instance_id.as_deref(),
        unique_instance_id: claims.unique_instance_id.as_deref(),
        host_name: claims.host_name.as_deref(),
        organization_id: claims.organization_id.map(|id| id as i64),
        root_namespace_ids: claims.root_namespace_id.map(|ns| vec![ns]),
        schema_version: Some(schema_version),
    })
}

pub(crate) fn build_query(
    claims: &Claims,
    tool_name: &str,
    coding_agent: Option<&str>,
) -> OrbitQueryContext {
    let queried = leaf_namespace_ids(claims);

    OrbitQueryContext::new(OrbitQueryData {
        source_type: claims.source_type.into(),
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
    })
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
    use gkg_analytics::{ORBIT_COMMON_SCHEMA, ORBIT_QUERY_SCHEMA};
    use labkit_events::{SnowplowContext, StructuredEvent};

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
        let common = build_common(&AnalyticsConfig::default(), claims, "33");
        let query = build_query(claims, tool, None);
        let event = StructuredEvent::builder("gkg", "gkg_query_executed")
            .context(common)
            .context(query)
            .build()
            .unwrap();
        event.contexts()[1].data.clone()
    }

    fn common_data(claims: &Claims, schema_version: &str) -> serde_json::Value {
        let common = build_common(&AnalyticsConfig::default(), claims, schema_version);
        let query = build_query(claims, "query_graph", None);
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
        let query = build_query(&claims, "query_graph", Some("claude-code"));
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
    fn build_common_sets_schema_version() {
        let claims = claims_with_paths(vec![]);
        let data = common_data(&claims, "33");
        assert_eq!(data["schema_version"], "33");
    }

    #[test]
    fn common_schema_is_1_0_0() {
        let claims = claims_with_paths(vec![]);
        let common = build_common(&AnalyticsConfig::default(), &claims, "33");
        assert_eq!(common.schema(), *ORBIT_COMMON_SCHEMA);
    }

    #[test]
    fn query_schema_is_2_0_1() {
        let claims = claims_with_paths(vec![]);
        let query = build_query(&claims, "query_graph", None);
        assert_eq!(query.schema(), *ORBIT_QUERY_SCHEMA);
    }

    // ── Iglu schema validation ──────────────────────────────────────────

    mod iglu {
        use super::*;
        use std::sync::LazyLock;

        static ORBIT_COMMON_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
            let schema: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../config/schemas/iglu/orbit_common.1-0-0.json"
            ))
            .expect("orbit_common schema is valid JSON");
            jsonschema::validator_for(&schema).expect("orbit_common schema compiles")
        });

        static ORBIT_QUERY_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
            let schema: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../config/schemas/iglu/orbit_query.2-0-1.json"
            ))
            .expect("orbit_query schema is valid JSON");
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
            let common = build_common(&AnalyticsConfig::default(), &claims, "33");
            assert_valid(&ORBIT_COMMON_VALIDATOR, &common.data(), "orbit_common");
        }

        #[test]
        fn common_context_minimal_validates() {
            let claims = claims_with_paths(vec![]);
            let common = build_common(&AnalyticsConfig::default(), &claims, "33");
            assert_valid(
                &ORBIT_COMMON_VALIDATOR,
                &common.data(),
                "orbit_common (minimal)",
            );
        }

        #[test]
        fn query_context_validates_against_iglu_schema() {
            let claims = claims_with_paths(vec!["1/22/"]);
            let query = build_query(&claims, "query_graph", Some("claude-code"));
            assert_valid(&ORBIT_QUERY_VALIDATOR, &query.data(), "orbit_query");
        }

        #[test]
        fn query_context_minimal_validates() {
            let claims = claims_with_paths(vec![]);
            let query = build_query(&claims, "query_graph", None);
            assert_valid(
                &ORBIT_QUERY_VALIDATOR,
                &query.data(),
                "orbit_query (minimal)",
            );
        }
    }
}
