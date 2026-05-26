use std::cell::Cell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gkg_server_config::AnalyticsConfig;
use labkit_events::StructuredEvent;
use query_engine::compiler::QueryInfo;
use query_engine::pipeline::{PipelineError, PipelineObserver};

use crate::auth::Claims;

use gkg_analytics::AnalyticsTracker;

use super::context::{QueryExecMetrics, build_common, build_query};

const GKG_CATEGORY: &str = "gkg";
const ACTION_QUERY_EXECUTED: &str = "gkg_query_executed";

pub(crate) struct AnalyticsObserver {
    tracker: Option<Arc<dyn AnalyticsTracker>>,
    config: Arc<AnalyticsConfig>,
    claims: Claims,
    tool_name: String,
    coding_agent: Option<String>,
    schema_version: String,
    errored: Cell<bool>,
    query_info: Option<QueryInfo>,
    start: Instant,
    exec_metrics: QueryExecMetrics,
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
            start: Instant::now(),
            exec_metrics: QueryExecMetrics::default(),
        }
    }
}

fn ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

impl PipelineObserver for AnalyticsObserver {
    fn set_query_type(&mut self, _query_type: &'static str) {}

    fn set_query_info(&mut self, info: QueryInfo) {
        self.query_info = Some(info);
    }

    fn compiled(&mut self, elapsed: Duration) {
        self.exec_metrics.compile_ms = Some(ms(elapsed));
    }

    fn executed(&mut self, elapsed: Duration, _batch_count: usize) {
        self.exec_metrics.execute_ms = Some(ms(elapsed));
    }

    fn authorized(&mut self, elapsed: Duration) {
        self.exec_metrics.authorization_ms = Some(ms(elapsed));
    }

    fn hydrated(&mut self, elapsed: Duration) {
        self.exec_metrics.hydration_ms = Some(ms(elapsed));
    }

    fn query_executed(&mut self, _label: &str, read_rows: u64, read_bytes: u64, memory: i64) {
        *self.exec_metrics.ch_read_rows.get_or_insert(0) += read_rows;
        *self.exec_metrics.ch_read_bytes.get_or_insert(0) += read_bytes;
        if memory > 0 {
            let mem = &mut self.exec_metrics.ch_memory_usage;
            *mem = Some(mem.unwrap_or(0).max(memory as u64));
        }
    }

    fn record_error(&self, _error: &PipelineError) {
        self.errored.set(true);
    }

    fn finish(&self, row_count: usize, redacted_count: usize) {
        if self.errored.get() {
            return;
        }
        let Some(tracker) = self.tracker.as_ref() else {
            return;
        };

        let mut metrics = self.exec_metrics.clone();
        metrics.duration_ms = Some(ms(self.start.elapsed()));
        metrics.row_count = Some(row_count as u64);
        metrics.redacted_count = Some(redacted_count as u64);

        let common = build_common(&self.config, &self.claims, &self.schema_version);
        let query = build_query(
            &self.claims,
            &self.tool_name,
            self.coding_agent.as_deref(),
            self.query_info.as_ref(),
            Some(&metrics),
        );

        match StructuredEvent::builder(GKG_CATEGORY, ACTION_QUERY_EXECUTED)
            .context(common)
            .context(query)
            .build()
        {
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
    fn exec_metrics_and_query_info_merged() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.set_query_info(QueryInfo::from(&compiler::CompiledQueryContext {
            query_type: compiler::input::QueryType::Traversal,
            base: compiler::passes::codegen::ParameterizedQuery {
                sql: String::new(),
                params: Default::default(),
                result_context: compiler::passes::enforce::ResultContext::new(),
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
        obs.compiled(Duration::from_millis(5));
        obs.executed(Duration::from_millis(50), 2);
        obs.authorized(Duration::from_millis(10));
        obs.query_executed("base", 1000, 50000, 8_000_000);
        obs.finish(42, 3);

        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        let data = &events[0].contexts()[1].data;
        // QueryInfo fields.
        assert_eq!(data["query_type"], "traversal");
        assert_eq!(data["is_search"], true);
        // Exec metrics.
        assert_eq!(data["compile_ms"], 5);
        assert_eq!(data["execute_ms"], 50);
        assert_eq!(data["authorization_ms"], 10);
        assert_eq!(data["row_count"], 42);
        assert_eq!(data["redacted_count"], 3);
        assert_eq!(data["ch_read_rows"], 1000);
        assert_eq!(data["ch_read_bytes"], 50000);
        assert_eq!(data["ch_memory_usage"], 8_000_000);
        assert!(data["duration_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn ch_stats_accumulate_across_queries() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let mut obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
            "query_graph",
            None,
            "33".to_string(),
        );
        obs.query_executed("base", 500, 10000, 4_000_000);
        obs.query_executed("hydration:static", 300, 6000, 2_000_000);
        obs.finish(10, 0);

        let data = &tracker.drain()[0].contexts()[1].data;
        assert_eq!(data["ch_read_rows"], 800);
        assert_eq!(data["ch_read_bytes"], 16000);
        // Peak memory -- max, not sum.
        assert_eq!(data["ch_memory_usage"], 4_000_000);
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
