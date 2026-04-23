use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, ObservableGauge, UpDownCounter};
use serde::Serialize;

/// Source-of-truth description of a single metric.
///
/// Declared once per metric as a `pub const` in a domain submodule and
/// collected via the module's `CATALOG` slice. Instruments are built by
/// calling one of the typed `build_*` methods, which panic if the spec's
/// declared `kind` doesn't match the requested instrument type.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct MetricSpec {
    pub otel_name: &'static str,
    pub description: &'static str,
    pub kind: MetricKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<&'static str>,
    pub labels: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<&'static [f64]>,
    pub stability: Stability,
    pub domain: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MetricKind {
    Counter,
    UpDownCounter,
    Gauge,
    ObservableGauge,
    HistogramF64,
    HistogramU64,
}

impl MetricKind {
    pub const fn is_histogram(self) -> bool {
        matches!(self, Self::HistogramF64 | Self::HistogramU64)
    }

    pub const fn is_monotonic(self) -> bool {
        matches!(self, Self::Counter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Stability {
    Stable,
    Experimental,
}

impl MetricSpec {
    /// Build a non-histogram spec with the given kind. Histograms must go
    /// through [`histogram_f64`](Self::histogram_f64) or
    /// [`histogram_u64`](Self::histogram_u64) because they require a bucket
    /// set.
    const fn instrument(
        otel_name: &'static str,
        description: &'static str,
        kind: MetricKind,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        domain: &'static str,
    ) -> Self {
        Self {
            otel_name,
            description,
            kind,
            unit,
            labels,
            buckets: None,
            stability: Stability::Stable,
            domain,
        }
    }

    pub const fn counter(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        domain: &'static str,
    ) -> Self {
        Self::instrument(
            otel_name,
            description,
            MetricKind::Counter,
            unit,
            labels,
            domain,
        )
    }

    pub const fn up_down_counter(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        domain: &'static str,
    ) -> Self {
        Self::instrument(
            otel_name,
            description,
            MetricKind::UpDownCounter,
            unit,
            labels,
            domain,
        )
    }

    pub const fn gauge(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        domain: &'static str,
    ) -> Self {
        Self::instrument(
            otel_name,
            description,
            MetricKind::Gauge,
            unit,
            labels,
            domain,
        )
    }

    pub const fn observable_gauge(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        domain: &'static str,
    ) -> Self {
        Self::instrument(
            otel_name,
            description,
            MetricKind::ObservableGauge,
            unit,
            labels,
            domain,
        )
    }

    pub const fn histogram_f64(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        buckets: &'static [f64],
        domain: &'static str,
    ) -> Self {
        Self {
            otel_name,
            description,
            kind: MetricKind::HistogramF64,
            unit,
            labels,
            buckets: Some(buckets),
            stability: Stability::Stable,
            domain,
        }
    }

    pub const fn histogram_u64(
        otel_name: &'static str,
        description: &'static str,
        unit: Option<&'static str>,
        labels: &'static [&'static str],
        buckets: &'static [f64],
        domain: &'static str,
    ) -> Self {
        Self {
            otel_name,
            description,
            kind: MetricKind::HistogramU64,
            unit,
            labels,
            buckets: Some(buckets),
            stability: Stability::Stable,
            domain,
        }
    }

    /// Prometheus-exposed metric name that `opentelemetry-prometheus` would
    /// produce for this spec.
    ///
    /// Sanitises dots to underscores, appends a unit suffix if the unit maps
    /// to a recognised UCUM suffix, then appends `_total` for counters.
    pub fn prom_name(&self) -> String {
        let mut name = self.otel_name.replace('.', "_");
        if let Some(unit) = self.unit
            && let Some(suffix) = unit_suffix(unit)
        {
            name.push('_');
            name.push_str(suffix);
        }
        if self.kind.is_monotonic() {
            name.push_str("_total");
        }
        name
    }

    pub fn build_counter_u64(&self, meter: &Meter) -> Counter<u64> {
        assert!(
            matches!(self.kind, MetricKind::Counter),
            "build_counter_u64 called on non-counter spec {}",
            self.otel_name
        );
        let mut b = meter
            .u64_counter(self.otel_name)
            .with_description(self.description);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        b.build()
    }

    pub fn build_up_down_counter_i64(&self, meter: &Meter) -> UpDownCounter<i64> {
        assert!(
            matches!(self.kind, MetricKind::UpDownCounter),
            "build_up_down_counter_i64 called on non-up-down-counter spec {}",
            self.otel_name
        );
        let mut b = meter
            .i64_up_down_counter(self.otel_name)
            .with_description(self.description);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        b.build()
    }

    pub fn build_gauge_f64(&self, meter: &Meter) -> Gauge<f64> {
        assert!(
            matches!(self.kind, MetricKind::Gauge),
            "build_gauge_f64 called on non-gauge spec {}",
            self.otel_name
        );
        let mut b = meter
            .f64_gauge(self.otel_name)
            .with_description(self.description);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        b.build()
    }

    pub fn build_histogram_f64(&self, meter: &Meter) -> Histogram<f64> {
        assert!(
            matches!(self.kind, MetricKind::HistogramF64),
            "build_histogram_f64 called on non-f64-histogram spec {}",
            self.otel_name
        );
        let mut b = meter
            .f64_histogram(self.otel_name)
            .with_description(self.description);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        if let Some(buckets) = self.buckets {
            b = b.with_boundaries(buckets.to_vec());
        }
        b.build()
    }

    pub fn build_histogram_u64(&self, meter: &Meter) -> Histogram<u64> {
        assert!(
            matches!(self.kind, MetricKind::HistogramU64),
            "build_histogram_u64 called on non-u64-histogram spec {}",
            self.otel_name
        );
        let mut b = meter
            .u64_histogram(self.otel_name)
            .with_description(self.description);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        if let Some(buckets) = self.buckets {
            b = b.with_boundaries(buckets.to_vec());
        }
        b.build()
    }

    /// Observable gauges require the caller to supply the callback closure,
    /// which this helper wires into the instrument builder.
    pub fn build_observable_gauge_i64<F>(&self, meter: &Meter, callback: F) -> ObservableGauge<i64>
    where
        F: Fn(&dyn opentelemetry::metrics::AsyncInstrument<i64>) + Send + Sync + 'static,
    {
        assert!(
            matches!(self.kind, MetricKind::ObservableGauge),
            "build_observable_gauge_i64 called on non-observable-gauge spec {}",
            self.otel_name
        );
        let mut b = meter
            .i64_observable_gauge(self.otel_name)
            .with_description(self.description)
            .with_callback(callback);
        if let Some(unit) = self.unit {
            b = b.with_unit(unit);
        }
        b.build()
    }
}

fn unit_suffix(unit: &str) -> Option<&'static str> {
    match unit {
        "s" => Some("seconds"),
        "ms" => Some("milliseconds"),
        "us" => Some("microseconds"),
        "ns" => Some("nanoseconds"),
        "min" => Some("minutes"),
        "h" => Some("hours"),
        "d" => Some("days"),
        "By" => Some("bytes"),
        "KiBy" => Some("kibibytes"),
        "MiBy" => Some("mebibytes"),
        "GiBy" => Some("gibibytes"),
        "TiBy" => Some("tebibytes"),
        "1" => Some("ratio"),
        "%" => Some("percent"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEMO: MetricSpec = MetricSpec {
        otel_name: "gkg.etl.messages.processed",
        description: "demo",
        kind: MetricKind::Counter,
        unit: None,
        labels: &["topic"],
        buckets: None,
        stability: Stability::Stable,
        domain: "indexer.etl",
    };

    #[test]
    fn prom_name_counter_no_unit() {
        assert_eq!(DEMO.prom_name(), "gkg_etl_messages_processed_total");
    }

    #[test]
    fn prom_name_histogram_seconds() {
        let spec = MetricSpec {
            otel_name: "gkg.etl.message.duration",
            kind: MetricKind::HistogramF64,
            unit: Some("s"),
            buckets: Some(&[0.1, 0.5]),
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_etl_message_duration_seconds");
    }

    #[test]
    fn prom_name_counter_bytes() {
        let spec = MetricSpec {
            otel_name: "gkg.etl.destination.written",
            unit: Some("By"),
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_etl_destination_written_bytes_total");
    }

    #[test]
    fn prom_name_up_down_counter_no_suffix() {
        let spec = MetricSpec {
            otel_name: "gkg.etl.permits.active",
            kind: MetricKind::UpDownCounter,
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_etl_permits_active");
    }

    #[test]
    fn prom_name_histogram_u64_no_unit() {
        let spec = MetricSpec {
            otel_name: "gkg.query.pipeline.result_set.rows",
            kind: MetricKind::HistogramU64,
            buckets: Some(&[1.0]),
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_query_pipeline_result_set_rows");
    }

    #[test]
    fn prom_name_observable_gauge_no_suffix() {
        let spec = MetricSpec {
            otel_name: "gkg.webserver.schema.state",
            kind: MetricKind::ObservableGauge,
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_webserver_schema_state");
    }

    #[test]
    fn prom_name_unknown_unit_falls_through() {
        let spec = MetricSpec {
            otel_name: "gkg.demo",
            unit: Some("rows"),
            ..DEMO
        };
        assert_eq!(spec.prom_name(), "gkg_demo_total");
    }
}
