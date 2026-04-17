//! Analytics context types attached to events.
//!
//! A context mirrors one iglu context schema. Every event attaches
//! [`OrbitCommon`] plus exactly one path-specific context (query / SDLC
//! indexing / code indexing). Contexts are propagated via `task_local!`
//! so emission sites deep in the call stack read the scoped values.
//!
//! Each concrete context:
//! - implements [`AnalyticsContext`] (sealed)
//! - has a typestate builder (via [`bon`]) enforcing required fields
//! - exposes `::current()` and `.scope(fut).await` for propagation
//!
//! Per epic gitlab-org&21189#note_3259533173 — see the epic thread for
//! the iglu schema split rationale.

use bon::Builder;
use serde::Serialize;
use strum::Display;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// A Snowplow-style context attached to events. Each concrete type
/// corresponds to one iglu context schema.
pub trait AnalyticsContext: sealed::Sealed + Clone + Serialize + Send + 'static {
    /// Iglu schema URI — e.g. `iglu:com.gitlab/orbit_common/jsonschema/1-0-0`.
    const SCHEMA_URI: &'static str;

    /// Read the context currently in scope, if any.
    fn current() -> Option<Self>;
}

// ---------- enums shared across contexts ----------

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
pub enum SourceType {
    Dap,
    Mcp,
    RestApi,
    Cli,
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

// ---------- macro: declare a context type ----------

/// Generate the `AnalyticsContext` impl + task_local + `scope` helper for
/// a context struct. Keeps each context's wiring to one macro call.
macro_rules! declare_context {
    ($ty:ty, schema = $schema:literal, key = $key:ident) => {
        tokio::task_local! { static $key: $ty; }

        impl $crate::context::sealed::Sealed for $ty {}

        impl $crate::context::AnalyticsContext for $ty {
            const SCHEMA_URI: &'static str = $schema;
            fn current() -> Option<Self> {
                $key.try_with(Clone::clone).ok()
            }
        }

        impl $ty {
            /// Run `fut` with `self` as the current scoped context.
            pub async fn scope<F, T>(self, fut: F) -> T
            where
                F: ::std::future::Future<Output = T>,
            {
                $key.scope(self, fut).await
            }
        }
    };
}

// ---------- OrbitCommon — attached to every GKG event ----------

#[derive(Builder, Clone, Debug, Serialize)]
pub struct OrbitCommon {
    pub deployment_type: DeploymentType,

    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_gitlab_team_member: Option<bool>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<String>,
}
declare_context!(
    OrbitCommon,
    schema = "iglu:com.gitlab/orbit_common/jsonschema/1-0-0",
    key = ORBIT_COMMON
);

// ---------- QueryContext — attached to query + schema-introspection events ----------

#[derive(Builder, Clone, Debug, Serialize)]
pub struct QueryContext {
    pub source_type: SourceType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<ToolName>,
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
    pub tier: Option<Tier>,
    #[builder(into)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}
declare_context!(
    QueryContext,
    schema = "iglu:com.gitlab/orbit_query/jsonschema/1-0-0",
    key = ORBIT_QUERY
);

// SDLC + Code indexing contexts follow the same recipe and land with
// their surface-area MRs under epic &21189.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_is_none_outside_scope() {
        assert!(OrbitCommon::current().is_none());
        assert!(QueryContext::current().is_none());
    }

    #[tokio::test]
    async fn scope_propagates_to_current() {
        let common = OrbitCommon::builder()
            .deployment_type(DeploymentType::Com)
            .build();
        let query = QueryContext::builder()
            .source_type(SourceType::Mcp)
            .namespace_id("42")
            .build();
        common
            .scope(query.scope(async {
                assert_eq!(
                    OrbitCommon::current().unwrap().deployment_type,
                    DeploymentType::Com
                );
                let q = QueryContext::current().unwrap();
                assert_eq!(q.source_type, SourceType::Mcp);
                assert_eq!(q.namespace_id.as_deref(), Some("42"));
            }))
            .await;
        assert!(OrbitCommon::current().is_none());
        assert!(QueryContext::current().is_none());
    }

    #[test]
    fn serializes_required_only() {
        let common = OrbitCommon::builder()
            .deployment_type(DeploymentType::SelfManaged)
            .build();
        let v = serde_json::to_value(&common).unwrap();
        assert_eq!(v["deployment_type"], "self_managed");
        assert!(v.get("correlation_id").is_none());

        let query = QueryContext::builder().source_type(SourceType::Cli).build();
        let v = serde_json::to_value(&query).unwrap();
        assert_eq!(v["source_type"], "cli");
        assert!(v.get("tool_name").is_none());
    }
}
