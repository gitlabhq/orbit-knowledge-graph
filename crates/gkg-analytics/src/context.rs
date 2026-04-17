use std::future::Future;

use bon::Builder;
use serde::Serialize;
use strum::Display;

tokio::task_local! {
    pub(crate) static ORBIT_CONTEXT: OrbitContext;
}

/// Product-analytics context attached to every GKG event.
///
/// Mirrors `iglu:com.gitlab/orbit/jsonschema/1-0-0`. `source_type` and
/// `deployment_type` are required at compile time via the typestate builder.
#[derive(Builder, Clone, Debug, Serialize)]
pub struct OrbitContext {
    pub source_type: SourceType,
    pub deployment_type: DeploymentType,

    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_namespace_id: Option<String>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_type: Option<UserType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_gitlab_team_member: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<ToolName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<Tier>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

pub fn current() -> Option<OrbitContext> {
    ORBIT_CONTEXT.try_with(Clone::clone).ok()
}

pub async fn with_context<F, T>(ctx: OrbitContext, fut: F) -> T
where
    F: Future<Output = T>,
{
    ORBIT_CONTEXT.scope(ctx, fut).await
}

#[derive(Clone, Copy, Debug, Serialize, Display, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SourceType {
    Dap,
    Mcp,
    RestApi,
    Cli,
}

#[derive(Clone, Copy, Debug, Serialize, Display, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeploymentType {
    Com,
    Dedicated,
    SelfManaged,
}

#[derive(Clone, Copy, Debug, Serialize, Display, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum UserType {
    Human,
    ServiceAccount,
    Bot,
}

#[derive(Clone, Copy, Debug, Serialize, Display, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Tier {
    Premium,
    Ultimate,
}

#[derive(Clone, Copy, Debug, Serialize, Display, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ToolName {
    QueryGraph,
    GetGraphSchema,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_is_none_outside_scope() {
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn with_context_propagates_to_current() {
        let ctx = OrbitContext::builder()
            .source_type(SourceType::Mcp)
            .deployment_type(DeploymentType::Com)
            .namespace_id("42")
            .build();
        with_context(ctx, async {
            let got = current().expect("context should be in scope");
            assert_eq!(got.source_type, SourceType::Mcp);
            assert_eq!(got.namespace_id.as_deref(), Some("42"));
        })
        .await;
        assert!(current().is_none(), "context must not leak past scope");
    }

    #[test]
    fn serializes_required_only() {
        let ctx = OrbitContext::builder()
            .source_type(SourceType::Cli)
            .deployment_type(DeploymentType::SelfManaged)
            .build();
        let v = serde_json::to_value(&ctx).unwrap();
        assert_eq!(v["source_type"], "cli");
        assert_eq!(v["deployment_type"], "self_managed");
        assert!(v.get("correlation_id").is_none());
        assert!(v.get("user_type").is_none());
    }
}
