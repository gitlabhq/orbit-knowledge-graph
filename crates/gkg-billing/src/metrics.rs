use std::sync::LazyLock;

use gkg_observability::billing::events as spec;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

pub(crate) const REASON_REALM_MISSING: &str = "realm_missing";
pub(crate) const REASON_REALM_UNRECOGNIZED: &str = "realm_unrecognized";
pub(crate) const REASON_EVENT_BUILD_FAILED: &str = "event_build_failed";

pub(crate) const REASON_NON_RETRIABLE_STATUS: &str = "non_retriable_status";
pub(crate) const REASON_RETRIES_EXHAUSTED: &str = "retries_exhausted";
pub(crate) const REASON_AUTH: &str = "auth";
// Fallback for any DeliveryFailure variant added upstream (the enum is
// #[non_exhaustive]) that this code does not yet map explicitly.
pub(crate) const REASON_UNKNOWN: &str = "unknown";

pub(crate) static METRICS: LazyLock<BillingMetrics> = LazyLock::new(BillingMetrics::new);

pub(crate) struct BillingMetrics {
    pub emitted: Counter<u64>,
    pub dropped: Counter<u64>,
    pub rejected: Counter<u64>,
    pub delivered: Counter<u64>,
    pub delivery_failed: Counter<u64>,
}

impl BillingMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            emitted: spec::EVENTS_EMITTED.build_counter_u64(&meter),
            dropped: spec::EVENTS_DROPPED.build_counter_u64(&meter),
            rejected: spec::EVENTS_REJECTED.build_counter_u64(&meter),
            delivered: spec::EVENTS_DELIVERED.build_counter_u64(&meter),
            delivery_failed: spec::EVENTS_DELIVERY_FAILED.build_counter_u64(&meter),
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
    METRICS.delivered.add(0, &[]);
    for reason in [
        REASON_REALM_MISSING,
        REASON_REALM_UNRECOGNIZED,
        REASON_EVENT_BUILD_FAILED,
    ] {
        METRICS
            .dropped
            .add(0, &[KeyValue::new(spec::labels::REASON, reason)]);
    }
    for reason in [
        REASON_NON_RETRIABLE_STATUS,
        REASON_RETRIES_EXHAUSTED,
        REASON_AUTH,
    ] {
        METRICS
            .delivery_failed
            .add(0, &[KeyValue::new(spec::labels::REASON, reason)]);
    }
}
