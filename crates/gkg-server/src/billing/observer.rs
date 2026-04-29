use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use gkg_observability::billing::events as spec;
use labkit_events::BillingEvent;
use opentelemetry::KeyValue;
use query_engine::pipeline::{PipelineError, PipelineObserver};
use serde_json::json;

use crate::auth::Claims;

use super::constants::{CATEGORY, EVENT_TYPE, UNIT_OF_MEASURE, normalize_realm};
use super::metrics::METRICS;
use super::tracker::BillingTracker;
use super::{REASON_EVENT_BUILD_FAILED, REASON_REALM_MISSING, REASON_REALM_UNRECOGNIZED};

fn record_dropped(reason: &'static str) {
    METRICS
        .dropped
        .add(1, &[KeyValue::new(spec::labels::REASON, reason)]);
}

fn correlation_id_string() -> String {
    labkit::correlation::current()
        .map(|id| id.as_str().to_string())
        .unwrap_or_default()
}

pub(crate) struct BillingObserver {
    tracker: Option<Arc<dyn BillingTracker>>,
    claims: Claims,
    query_type: &'static str,
    errored: Cell<bool>,
}

impl BillingObserver {
    pub(crate) fn new(tracker: Option<Arc<dyn BillingTracker>>, claims: Claims) -> Self {
        Self {
            tracker,
            claims,
            query_type: "unknown",
            errored: Cell::new(false),
        }
    }

    fn build_event(&self) -> Option<BillingEvent> {
        let correlation_id = correlation_id_string();
        let Some(raw_realm) = self.claims.realm.as_deref() else {
            tracing::warn!(
                user_id = self.claims.user_id,
                correlation_id = %correlation_id,
                "billing event skipped: realm missing from JWT claims"
            );
            record_dropped(REASON_REALM_MISSING);
            return None;
        };
        let Some(realm) = normalize_realm(raw_realm) else {
            tracing::warn!(
                user_id = self.claims.user_id,
                raw_realm = raw_realm,
                correlation_id = %correlation_id,
                "billing event skipped: unrecognized realm value"
            );
            record_dropped(REASON_REALM_UNRECOGNIZED);
            return None;
        };

        let mut builder = BillingEvent::builder(CATEGORY, EVENT_TYPE, realm, UNIT_OF_MEASURE, 1.0);

        if let Some(org_id) = self.claims.organization_id {
            builder = builder.organization_id(org_id as i64);
        }

        builder = builder.subject(self.claims.user_id.to_string());

        if let Some(ref id) = labkit::correlation::current() {
            builder = builder.correlation_id(id.as_str());
        }

        if let Some(ref id) = self.claims.instance_id {
            builder = builder.instance_id(id.as_str());
        }
        if let Some(ref id) = self.claims.unique_instance_id {
            builder = builder.unique_instance_id(id.as_str());
        }
        if let Some(ref v) = self.claims.instance_version {
            builder = builder.instance_version(v.as_str());
        }
        if let Some(ref id) = self.claims.global_user_id {
            builder = builder.global_user_id(id.as_str());
        }
        if let Some(ref h) = self.claims.host_name {
            builder = builder.host_name(h.as_str());
        }
        if let Some(ns_id) = self.claims.root_namespace_id {
            builder = builder.root_namespace_id(ns_id);
        }
        if let Some(ref dt) = self.claims.deployment_type {
            builder = builder.deployment_type(dt.as_str());
        }

        builder = builder.metadata(json!({
            "query_type": self.query_type,
            "feature_qualified_name": format!("orbit-{}", self.claims.source_type),
        }));

        match builder.build() {
            Ok(event) => Some(event),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    user_id = self.claims.user_id,
                    correlation_id = %correlation_id,
                    "failed to build billing event"
                );
                record_dropped(REASON_EVENT_BUILD_FAILED);
                None
            }
        }
    }
}

impl PipelineObserver for BillingObserver {
    fn set_query_type(&mut self, query_type: &'static str) {
        self.query_type = query_type;
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
        if let Some(ref tracker) = self.tracker
            && let Some(event) = self.build_event()
        {
            let _span = tracing::info_span!("billing.track", query_type = self.query_type)
                .entered();
            tracker.track(event);
            METRICS.emitted.add(1, &[]);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use query_engine::pipeline::{PipelineError, PipelineObserver};

    use super::*;
    use crate::billing::tracker::InMemoryBillingTracker;

    fn test_claims() -> Claims {
        Claims {
            sub: "u:123".into(),
            iss: "gitlab".into(),
            aud: "gitlab-knowledge-graph".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 123,
            username: "testuser".into(),
            admin: false,
            organization_id: Some(42),
            min_access_level: None,
            group_traversal_ids: vec![],
            source_type: "mcp".into(),
            ai_session_id: None,
            instance_id: Some("inst-abc".into()),
            unique_instance_id: Some("uid-abc".into()),
            instance_version: Some("18.0.0".into()),
            global_user_id: Some("guser-456".into()),
            host_name: Some("gitlab.com".into()),
            root_namespace_id: Some(9970),
            deployment_type: Some(".com".into()),
            realm: Some("SaaS".into()),
        }
    }

    #[test]
    fn billing_observer_emits_on_finish() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let mut obs = BillingObserver::new(Some(tracker.clone()), test_claims());
        obs.set_query_type("traversal");
        obs.finish(42, 3);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_skips_on_error() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let mut obs = BillingObserver::new(Some(tracker.clone()), test_claims());
        obs.set_query_type("traversal");
        obs.record_error(&PipelineError::Execution("test error".into()));
        obs.finish(42, 3);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_emits_with_lowercase_realm_alias() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let claims = Claims {
            realm: Some("saas".into()),
            ..test_claims()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), claims);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_emits_with_self_managed_realm_alias() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let claims = Claims {
            realm: Some("self-managed".into()),
            ..test_claims()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), claims);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_skips_when_realm_absent() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let claims = Claims {
            realm: None,
            ..test_claims()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), claims);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_skips_when_realm_unrecognized() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let claims = Claims {
            realm: Some("bogus".into()),
            ..test_claims()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), claims);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_emits_when_optional_fields_absent() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let claims = Claims {
            organization_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            ..test_claims()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), claims);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    // Smoke test: verifies no panic when billing is disabled (tracker is None).
    // No assertion — the observable behaviour is that finish() silently skips.
    #[test]
    fn billing_observer_skips_when_tracker_none() {
        let mut obs = BillingObserver::new(None, test_claims());
        obs.set_query_type("traversal");
        obs.finish(1, 0);
    }
}
