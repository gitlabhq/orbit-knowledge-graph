use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::AnalyticsConfig;
use labkit_events::gkg::GkgEvent;
use query_engine::pipeline::{PipelineError, PipelineObserver};

use crate::auth::Claims;

use super::context::{build_common, build_query};
use super::tracker::AnalyticsTracker;

pub(crate) struct AnalyticsObserver {
    tracker: Option<Arc<dyn AnalyticsTracker>>,
    config: Arc<AnalyticsConfig>,
    claims: Claims,
    errored: Cell<bool>,
}

impl AnalyticsObserver {
    pub(crate) fn new(
        tracker: Option<Arc<dyn AnalyticsTracker>>,
        config: Arc<AnalyticsConfig>,
        claims: Claims,
    ) -> Self {
        Self {
            tracker,
            config,
            claims,
            errored: Cell::new(false),
        }
    }
}

impl PipelineObserver for AnalyticsObserver {
    fn set_query_type(&mut self, _query_type: &'static str) {}
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
        let Some(common) = build_common(&self.config, &self.claims) else {
            return;
        };
        let Some(query) = build_query(&self.claims) else {
            return;
        };
        tracker.track(GkgEvent::query_executed(common, query));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gkg_server_config::AnalyticsConfig;
    use query_engine::pipeline::{PipelineError, PipelineObserver};

    use super::*;
    use crate::analytics::tracker::InMemoryAnalyticsTracker;

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
        );
        obs.finish(10, 0);
        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn skips_on_error() {
        let tracker = Arc::new(InMemoryAnalyticsTracker::new());
        let obs = AnalyticsObserver::new(
            Some(tracker.clone()),
            Arc::new(AnalyticsConfig::default()),
            test_claims(),
        );
        obs.record_error(&PipelineError::Execution("x".into()));
        obs.finish(0, 0);
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn skips_when_tracker_absent() {
        let obs = AnalyticsObserver::new(None, Arc::new(AnalyticsConfig::default()), test_claims());
        obs.finish(1, 0);
    }
}
