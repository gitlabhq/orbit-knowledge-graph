use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use gkg_observability::billing::events as spec;
use labkit_events::BillingEvent;
use opentelemetry::KeyValue;
use query_engine::compiler::{ExecMetrics, QueryInfo};
use query_engine::pipeline::{PipelineError, PipelineObserver};
use serde_json::json;

use crate::constants::{
    CATEGORY, EVENT_TYPE, UNIT_OF_MEASURE, feature_qualified_name, normalize_realm,
};
use crate::inputs::BillingInputs;
use crate::metrics::{
    METRICS, REASON_EVENT_BUILD_FAILED, REASON_REALM_MISSING, REASON_REALM_UNRECOGNIZED,
};
use crate::tracker::BillingTracker;

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

pub struct BillingObserver {
    tracker: Option<Arc<dyn BillingTracker>>,
    inputs: BillingInputs,
    query_type: &'static str,
    metrics: ExecMetrics,
    errored: Cell<bool>,
}

impl BillingObserver {
    pub fn new(tracker: Option<Arc<dyn BillingTracker>>, inputs: BillingInputs) -> Self {
        Self {
            tracker,
            inputs,
            query_type: "unknown",
            metrics: ExecMetrics::default(),
            errored: Cell::new(false),
        }
    }

    fn build_event(&self) -> Option<BillingEvent> {
        let correlation_id = correlation_id_string();
        let Some(raw_realm) = self.inputs.realm.as_deref() else {
            tracing::warn!(
                user_id = self.inputs.user_id,
                correlation_id = %correlation_id,
                "billing event skipped: realm missing from JWT claims"
            );
            record_dropped(REASON_REALM_MISSING);
            return None;
        };
        let Some(realm) = normalize_realm(raw_realm) else {
            tracing::warn!(
                user_id = self.inputs.user_id,
                raw_realm = raw_realm,
                correlation_id = %correlation_id,
                "billing event skipped: unrecognized realm value"
            );
            record_dropped(REASON_REALM_UNRECOGNIZED);
            return None;
        };

        let mut builder = BillingEvent::builder(CATEGORY, EVENT_TYPE, realm, UNIT_OF_MEASURE, 1.0);

        if let Some(org_id) = self.inputs.organization_id {
            builder = builder.organization_id(org_id);
        }

        builder = builder.subject(self.inputs.user_id.to_string());

        if let Some(ref id) = labkit::correlation::current() {
            builder = builder.correlation_id(id.as_str());
        }

        if let Some(ref id) = self.inputs.instance_id {
            builder = builder.instance_id(id.as_str());
        }
        if let Some(ref id) = self.inputs.unique_instance_id {
            builder = builder.unique_instance_id(id.as_str());
        }
        if let Some(ref v) = self.inputs.instance_version {
            builder = builder.instance_version(v.as_str());
        }
        if let Some(ref id) = self.inputs.global_user_id {
            builder = builder.global_user_id(id.as_str());
        }
        if let Some(ref h) = self.inputs.host_name {
            builder = builder.host_name(h.as_str());
        }
        if let Some(ns_id) = self.inputs.root_namespace_id {
            builder = builder.root_namespace_id(ns_id);
        }
        if let Some(ref dt) = self.inputs.deployment_type {
            builder = builder.deployment_type(dt.as_str());
        }

        let mut metadata = json!({
            "query_type": self.query_type,
            "feature_qualified_name": feature_qualified_name(&self.inputs.source_type),
        });
        if let (serde_json::Value::Object(map), Ok(serde_json::Value::Object(m))) =
            (&mut metadata, serde_json::to_value(&self.metrics))
        {
            map.extend(m);
        }
        builder = builder.metadata(metadata);

        match builder.build() {
            Ok(event) => Some(event),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    user_id = self.inputs.user_id,
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
    fn set_query_info(&mut self, info: QueryInfo) {
        self.metrics.set_query_info(info);
    }
    fn compiled(&mut self, elapsed: Duration) {
        self.metrics.compiled(elapsed);
    }
    fn executed(&mut self, elapsed: Duration, _: usize) {
        self.metrics.executed(elapsed);
    }
    fn authorized(&mut self, _: Duration) {}
    fn hydrated(&mut self, _: Duration) {}
    fn query_executed(&mut self, _: &str, r: u64, b: u64, m: i64) {
        self.metrics.query_executed(r, b, m);
    }
    fn record_error(&self, _: &PipelineError) {
        self.errored.set(true);
    }

    fn finish(&self, _row_count: usize, _redacted_count: usize) {
        if self.errored.get() {
            return;
        }
        if let Some(ref tracker) = self.tracker
            && let Some(event) = self.build_event()
        {
            let _span =
                tracing::info_span!("billing.track", query_type = self.query_type).entered();
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
    use crate::tracker::InMemoryBillingTracker;

    fn test_inputs() -> BillingInputs {
        BillingInputs {
            user_id: 123,
            source_type: "mcp".into(),
            organization_id: Some(42),
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
        let mut obs = BillingObserver::new(Some(tracker.clone()), test_inputs());
        obs.set_query_type("traversal");
        obs.finish(42, 3);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_skips_on_error() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let mut obs = BillingObserver::new(Some(tracker.clone()), test_inputs());
        obs.set_query_type("traversal");
        obs.record_error(&PipelineError::Execution("test error".into()));
        obs.finish(42, 3);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_emits_with_lowercase_realm_alias() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let inputs = BillingInputs {
            realm: Some("saas".into()),
            ..test_inputs()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), inputs);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_emits_with_self_managed_realm_alias() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let inputs = BillingInputs {
            realm: Some("self-managed".into()),
            ..test_inputs()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), inputs);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn billing_observer_skips_when_realm_absent() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let inputs = BillingInputs {
            realm: None,
            ..test_inputs()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), inputs);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_skips_when_realm_unrecognized() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let inputs = BillingInputs {
            realm: Some("bogus".into()),
            ..test_inputs()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), inputs);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn billing_observer_emits_when_optional_fields_absent() {
        let tracker = Arc::new(InMemoryBillingTracker::new());
        let inputs = BillingInputs {
            organization_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            ..test_inputs()
        };
        let mut obs = BillingObserver::new(Some(tracker.clone()), inputs);
        obs.set_query_type("traversal");
        obs.finish(1, 0);

        assert_eq!(tracker.count(), 1);
    }

    // Smoke test: verifies no panic when billing is disabled (tracker is None).
    // No assertion — the observable behaviour is that finish() silently skips.
    #[test]
    fn billing_observer_skips_when_tracker_none() {
        let mut obs = BillingObserver::new(None, test_inputs());
        obs.set_query_type("traversal");
        obs.finish(1, 0);
    }
}
