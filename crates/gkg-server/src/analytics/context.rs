use gkg_server_config::AnalyticsConfig;
use labkit_events::orbit::{OrbitCommonContext, OrbitQueryContext, SourceType, ToolName};

use crate::auth::Claims;

pub(crate) fn build_common(
    config: &AnalyticsConfig,
    claims: &Claims,
) -> Option<OrbitCommonContext> {
    let mut b = gkg_analytics::common_builder(config);
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

pub(crate) fn build_query(claims: &Claims) -> Option<OrbitQueryContext> {
    let mut b =
        OrbitQueryContext::builder(map_source(&claims.source_type)).tool_name(ToolName::QueryGraph);
    if let Some(ref id) = claims.global_user_id {
        b = b.global_user_id(id);
    }
    if let Some(ref s) = claims.ai_session_id {
        b = b.session_id(s);
    }
    if let Some(ns) = claims.root_namespace_id {
        b = b.root_namespace_id(ns);
    }
    b.build()
        .map_err(|e| tracing::warn!(error = %e, "drop analytics event: orbit_query build failed"))
        .ok()
}

fn map_source(s: &str) -> SourceType {
    match s {
        "dap" => SourceType::Dap,
        "mcp" => SourceType::Mcp,
        "cli" => SourceType::Cli,
        _ => SourceType::RestApi,
    }
}
