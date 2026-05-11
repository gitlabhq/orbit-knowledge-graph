use std::sync::LazyLock;

use gkg_observability::billing::events as spec;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

pub(crate) const REASON_REALM_MISSING: &str = "realm_missing";
pub(crate) const REASON_REALM_UNRECOGNIZED: &str = "realm_unrecognized";
pub(crate) const REASON_EVENT_BUILD_FAILED: &str = "event_build_failed";

pub(crate) static METRICS: LazyLock<BillingMetrics> = LazyLock::new(BillingMetrics::new);

pub(crate) struct BillingMetrics {
    pub emitted: Counter<u64>,
    pub dropped: Counter<u64>,
    pub rejected: Counter<u64>,
}

impl BillingMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            emitted: spec::EVENTS_EMITTED.build_counter_u64(&meter),
            dropped: spec::EVENTS_DROPPED.build_counter_u64(&meter),
            rejected: spec::EVENTS_REJECTED.build_counter_u64(&meter),
        }
    }
}

/// Register every series at 0 at startup so `rate(...) == 0` alerts compare
/// against zero rather than an absent series — OTel only exposes a series
/// once it has been observed at least once. The labelled `dropped` counter
/// is touched per known `reason` value so each labelled series appears.
pub fn register() {
    METRICS.emitted.add(0, &[]);
    METRICS.rejected.add(0, &[]);
    for reason in [
        REASON_REALM_MISSING,
        REASON_REALM_UNRECOGNIZED,
        REASON_EVENT_BUILD_FAILED,
    ] {
        METRICS
            .dropped
            .add(0, &[KeyValue::new(spec::labels::REASON, reason)]);
    }
}
