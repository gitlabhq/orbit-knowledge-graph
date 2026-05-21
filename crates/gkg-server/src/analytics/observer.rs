use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::AnalyticsConfig;
use labkit_events::{SnowplowContext, StructuredEvent};
use query_engine::compiler::QueryInfo;
use query_engine::pipeline::{PipelineError, PipelineObserver};

use crate::auth::Claims;

use gkg_analytics::AnalyticsTracker;

use super::context::{build_common, build_query};

const GKG_CATEGORY: &str = "gkg";
const ACTION_QUERY_EXECUTED: &str = "gkg_query_executed";

/// Iglu schema URI for the query info context.
/// The schema must be registered in the GitLab Iglu repo before events
/// carrying this context will pass Snowplow validation.
pub const ORBIT_QUERY_INFO_SCHEMA: &str = "iglu:com.gitlab/orbit_query_info/jsonschema/1-0-0";

/// Snowplow context wrapper for [`QueryInfo`].
struct QueryInfoContext(QueryInfo);

impl SnowplowContext for QueryInfoContext {
    fn schema(&self) -> &str {
        ORBIT_QUERY_INFO_SCHEMA
    }

    fn data(&self) -> serde_json::Value {
        serde_json::to_value(&self.0).unwrap_or_default()
    }
}

pub(crate) struct AnalyticsObserver {
    tracker: Option<Arc<dyn AnalyticsTracker>>,
    config: Arc<AnalyticsConfig>,
    claims: Claims,
    tool_name: String,
    coding_agent: Option<String>,
    schema_version: String,
    errored: Cell<bool>,
    query_info: Option<QueryInfo>,
}

impl AnalyticsObserver {
    pub(crate) fn new(
        tracker: Option<Arc<dyn AnalyticsTracker>>,
        config: Arc<AnalyticsConfig>,
        claims: Claims,
        tool_name: impl Into<String>,
        coding_agent: Option<String>,
        schema_version: String,
    ) -> Self {
        Self {
            tracker,
            config,
            claims,
            tool_name: tool_name.into(),
            coding_agent,
            schema_version,
            errored: Cell::new(false),
            query_info: None,
        }
    }
}

impl PipelineObserver for AnalyticsObserver {
    fn set_query_type(&mut self, _query_type: &'static str) {}

    fn set_query_dimensions(&mut self, info: QueryInfo) {
        self.query_info = Some(info);
    }

    fn compiled(&mut self, _elapsed: Duration) {}
    fn executed(&mut self, _elapsed: Duration, _batch_count: usize) {}
    fn authorized(&mut self, _elapsed: Duration) {}
    fn hydrated(&mut self, _elapsed: Duration) {}
    fn query_executed(&mut self, _label: &str, _read_rows: u64, _read_bytes: u64, _memory: i64) {}

    fn record_error(&self, _error: &PipelineError) {
        self.errored.set(true);
    }

    fn finish(&self, _row_count: usize, _redacted_count: usize) {
        if self.errored.get() {
            return;
        }
        let Some(tracker) = self.tracker.as_ref() else {
            return;
        };
        let Some(common) = build_common(&self.config, &self.claims, &self.schema_version) else {
            return;
        };
        let Some(query) = build_query(&self.claims, &self.tool_name, self.coding_agent.as_deref())
        else {
            return;
        };

        let mut builder = StructuredEvent::builder(GKG_CATEGORY, ACTION_QUERY_EXECUTED)
            .context(common)
            .context(query);

        if let Some(ref info) = self.query_info {
            builder = builder.context(QueryInfoContext(info.clone()));
        }

        match builder.build() {
            Ok(event) => tracker.track(event),
            Err(e) => tracing::warn!(error = %e, "failed to build analytics event"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gkg_server_config::AnalyticsConfig;
    use query_engine::pipeline::{PipelineError, PipelineObserver};

    use gkg_analytics::InMemoryAnalyticsTracker;

    use super::*;

    fn test_claims() -> Claims {
        Claims {
            sub: "u:1".into(),
            iss: "gitlab".into(),
            aud: "gkg".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "t".into(),
            admin: false,
            organization_id: Some(42),
            min_access_level: None,
            group_traversal_ids: vec![],
            source_type: "mcp".into(),
            ai_session_id: Some("sess".into()),
            instance_id: Some("inst".into()),
            unique_instance_id: Some("uniq".into()),
            instance_version: None,
            global_user_id: Some("guser".into()),
            host_name: Some("gitlab.com".into()),
            root_namespace_id: Some(99i64),
            deployment_type: Some(".com".into()),
            realm: Some("SaaS".into()),
        }
    }

    #[test]
    fn emits_one_event_on_finish() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.finish(10, 0);
        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn event_has_three_contexts_when_query_info_present() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.set_query_dimensions(QueryInfo::from(&compiler::CompiledQueryContext {
            query_type: compiler::input::QueryType::Traversal,
            base: compiler::passes::codegen::ParameterizedQuery {
                sql: String::new(),
                params: Default::default(),
                result_context: compiler::passes::enforce::ResultContext::new(
                    compiler::input::QueryType::Traversal,
                ),
                query_config: Default::default(),
                dialect: compiler::passes::codegen::SqlDialect::ClickHouse,
            },
            hydration: compiler::HydrationPlan::None,
            input: compiler::Input {
                query_type: compiler::input::QueryType::Traversal,
                nodes: vec![compiler::InputNode {
                    entity: Some("User".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
        }));
        obs.finish(10, 0);

        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].contexts().len(), 3);
        assert_eq!(events[0].contexts()[2].schema, ORBIT_QUERY_INFO_SCHEMA);
        assert_eq!(events[0].contexts()[2].data["query_type"], "traversal");
        assert_eq!(events[0].contexts()[2].data["is_search"], true);
    }

    #[test]
    fn skips_on_error() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.record_error(&PipelineError::Execution("x".into()));
        obs.finish(0, 0);
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn skips_when_tracker_absent() {
        let obs = AnalyticsObserver::new(
            None,
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.finish(1, 0);
    }
}
