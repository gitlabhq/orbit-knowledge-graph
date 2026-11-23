use std::sync::LazyLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};
use orbit_observability::billing::quota as spec;
use orbit_observability::billing::quota::labels::{CACHE, DECISION, SOURCE_TYPE};
use orbit_observability::billing::quota::values::{
    ALLOW, DENY, FAIL_CLOSED, FAIL_OPEN, HIT, MISS, SKIPPED,
};

pub(super) static QUOTA_METRICS: LazyLock<QuotaMetrics> = LazyLock::new(QuotaMetrics::new);

pub(super) struct QuotaMetrics {
    pub decisions: Counter<u64>,
    pub cdot_duration: Histogram<f64>,
}

impl QuotaMetrics {
    fn new() -> Self {
        let meter = orbit_observability::meter();
        Self {
            decisions: spec::QUOTA_DECISIONS.build_counter_u64(&meter),
            cdot_duration: spec::QUOTA_CDOT_DURATION.build_histogram_f64(&meter),
        }
    }
}

pub fn register() {
    let metered_types = [
        "mcp",
        "rest",
        "frontend",
        "core",
        "dws",
        "code_intelligence",
    ];

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
    for decision in [FAIL_OPEN, FAIL_CLOSED, SKIPPED] {
        for source_type in metered_types {
            QUOTA_METRICS.decisions.add(
                0,
                &[
                    KeyValue::new(DECISION, decision),
                    KeyValue::new(CACHE, MISS),
                    KeyValue::new(SOURCE_TYPE, source_type),
                ],
            );
        }
    }
}
