//! Central catalogue of every metric the GKG Rust service emits.
//!
//! Each domain submodule declares its metrics as `MetricSpec` constants plus
//! `build_*` functions that construct the matching OTel instrument. The
//! [`catalog()`] function returns the union of every module's `CATALOG` slice,
//! which the `cargo xtask metrics-catalog` command serialises into
//! `orbit-dashboards/gkg-metrics.json`.
//!
//! Consumers elsewhere in the workspace call the `build_*` functions from
//! their existing `Metrics::with_meter` constructors so that metric names,
//! descriptions, units, and histogram buckets live in exactly one place.

pub mod billing;
pub mod buckets;
pub mod indexer;
pub mod query;
pub mod server;

mod types;

pub use types::{MetricKind, MetricSpec, Stability};

/// Shared OTel meter name for every instrument built from this catalog.
///
/// Returning the same `Meter` instance means every `Metrics::new()` across
/// the workspace reports under a single instrumentation scope.
pub fn meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("gkg")
}

/// Flat catalog of every `MetricSpec` emitted by the service.
///
/// The order is deterministic: modules are concatenated in a fixed order and
/// each module's own `CATALOG` preserves declaration order. The xtask sorts
/// by `otel_name` before writing, so this order only matters when the slice
/// is read directly.
pub fn catalog() -> Vec<&'static MetricSpec> {
    let mut v: Vec<&'static MetricSpec> = Vec::new();
    v.extend(indexer::etl::CATALOG);
    v.extend(indexer::scheduler::CATALOG);
    v.extend(indexer::code::CATALOG);
    v.extend(indexer::sdlc::CATALOG);
    v.extend(indexer::deletion::CATALOG);
    v.extend(indexer::migration::CATALOG);
    v.extend(query::pipeline::CATALOG);
    v.extend(query::engine::CATALOG);
    v.extend(server::content::CATALOG);
    v.extend(server::schema_watcher::CATALOG);
    v.extend(billing::events::CATALOG);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn catalog_is_nonempty() {
        assert!(
            catalog().len() >= 58,
            "catalog shrank unexpectedly; did a module fail to register?"
        );
    }

    #[test]
    fn otel_names_are_unique() {
        let mut seen = HashSet::new();
        for spec in catalog() {
            assert!(
                seen.insert(spec.otel_name),
                "duplicate otel_name in catalog: {}",
                spec.otel_name
            );
        }
    }

    #[test]
    fn prom_names_are_unique() {
        let mut seen = HashSet::new();
        for spec in catalog() {
            let name = spec.prom_name();
            assert!(
                seen.insert(name.clone()),
                "duplicate prom_name in catalog: {name} (otel: {})",
                spec.otel_name
            );
        }
    }

    #[test]
    fn every_histogram_has_buckets() {
        for spec in catalog() {
            if spec.kind.is_histogram() {
                assert!(
                    spec.buckets.is_some(),
                    "histogram {} has no buckets; use buckets::* or declare one",
                    spec.otel_name
                );
            } else {
                assert!(
                    spec.buckets.is_none(),
                    "non-histogram {} has buckets, which will be ignored",
                    spec.otel_name
                );
            }
        }
    }

    #[test]
    fn no_banned_suffix_in_otel_name() {
        let banned = [
            "_total",
            "_seconds",
            "_bytes",
            "_milliseconds",
            "_count",
            "_bucket",
            "_sum",
        ];
        for spec in catalog() {
            for suffix in &banned {
                assert!(
                    !spec.otel_name.ends_with(suffix),
                    "otel name {} ends in banned suffix {suffix}; drop it so the Prometheus exporter can add the correct suffix",
                    spec.otel_name
                );
            }
        }
    }
}
