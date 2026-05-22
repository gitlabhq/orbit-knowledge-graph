use std::sync::LazyLock;

use gkg_observability::billing::quota as spec;
use gkg_observability::billing::quota::labels::{CACHE, DECISION, SOURCE_TYPE};
use gkg_observability::billing::quota::values::{ALLOW, DENY, FAIL_OPEN, HIT, MISS};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};

pub(super) static QUOTA_METRICS: LazyLock<QuotaMetrics> = LazyLock::new(QuotaMetrics::new);

// `cache.entries` gauge is registered inside `QuotaCache::new()` instead of here
// because it needs a live reference to the moka `Cache` from that constructor.
pub(super) struct QuotaMetrics {
    pub decisions: Counter<u64>,
    pub cdot_duration: Histogram<f64>,
    pub bypassed: Counter<u64>,
}

impl QuotaMetrics {
    fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            decisions: spec::QUOTA_DECISIONS.build_counter_u64(&meter),
            cdot_duration: spec::QUOTA_CDOT_DURATION.build_histogram_f64(&meter),
            bypassed: spec::QUOTA_BYPASSED.build_counter_u64(&meter),
        }
    }
}

/// Pre-seed every labelled counter series at 0 so Prometheus never sees an absent
/// series on the first scrape. OTel only exposes a series after it has been
/// observed at least once.
///
/// `decisions{decision=fail_open, cache=hit}` is intentionally omitted: fail-open
/// outcomes are never inserted into the cache, so that combination cannot occur.
/// Histograms are not pre-seeded — `record(0.0)` would be a real observation that
/// skews `_count` and `_sum`; they appear on the first genuine CDot call.
pub fn register() {
    let metered_types = ["mcp", "rest"];
    let bypass_types = ["frontend", "core", "dws"];

    for cache in [HIT, MISS] {
        for decision in [ALLOW, DENY] {
            for source_type in metered_types {
                QUOTA_METRICS.decisions.add(
                    0,
                    &[
                        KeyValue::new(DECISION, decision),
                        KeyValue::new(CACHE, cache),
                        KeyValue::new(SOURCE_TYPE, source_type),
                    ],
                );
            }
        }
    }
    // fail_open is only ever observed on cache=miss (never cached).
    for source_type in metered_types {
        QUOTA_METRICS.decisions.add(
            0,
            &[
                KeyValue::new(DECISION, FAIL_OPEN),
                KeyValue::new(CACHE, MISS),
                KeyValue::new(SOURCE_TYPE, source_type),
            ],
        );
    }
    for source_type in bypass_types {
        QUOTA_METRICS
            .bypassed
            .add(0, &[KeyValue::new(SOURCE_TYPE, source_type)]);
    }
}
