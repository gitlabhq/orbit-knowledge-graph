use std::sync::LazyLock;

use gkg_observability::billing::events as spec;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

pub(crate) static METRICS: LazyLock<BillingMetrics> = LazyLock::new(BillingMetrics::new);

pub(crate) struct BillingMetrics {
    pub emitted: Counter<u64>,
    pub dropped: Counter<u64>,
    pub track_errors: Counter<u64>,
}

impl BillingMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            emitted: spec::EVENTS_EMITTED.build_counter_u64(&meter),
            dropped: spec::EVENTS_DROPPED.build_counter_u64(&meter),
            track_errors: spec::TRACK_ERRORS.build_counter_u64(&meter),
        }
    }
}

/// Pre-register every billing counter series at zero so it appears in
/// `/metrics` before any query runs. The OTel SDK only emits a series after
/// the first `.add()` call, so without this `BillingEventsDroppedToZero`
/// (`rate(emitted) == 0`) compares against absent rather than zero.
///
/// `dropped` is touched once per known `reason` value so the labelled series
/// appear too; new reasons added later only register on first increment.
pub fn register() {
    METRICS.emitted.add(0, &[]);
    METRICS.track_errors.add(0, &[]);
    for reason in [
        crate::billing::REASON_REALM_MISSING,
        crate::billing::REASON_REALM_UNRECOGNIZED,
        crate::billing::REASON_EVENT_BUILD_FAILED,
    ] {
        METRICS
            .dropped
            .add(0, &[KeyValue::new(spec::labels::REASON, reason)]);
    }
}
