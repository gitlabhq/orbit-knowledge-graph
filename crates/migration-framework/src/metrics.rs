use std::time::Instant;

use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use crate::ledger::LedgerMigrationRecord;
use crate::registry::MigrationRegistry;
use crate::types::{Migration, MigrationContext, MigrationStatus};

// These buckets are tuned for today's additive DDL migrations. If prepare()
// starts covering longer-running convergent/finalization work, revisit them.
const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

#[derive(Clone)]
pub struct MigrationMetrics {
    status: Gauge<u64>,
    applied_total: Counter<u64>,
    prepare_duration: Histogram<f64>,
    desired_version: Gauge<u64>,
    current_version: Gauge<u64>,
}

impl MigrationMetrics {
    pub fn new() -> Self {
        let meter = global::meter("gkg_migration_framework");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let status = meter
            .u64_gauge("gkg_migration_status")
            .with_description(
                "Current migration status per version encoded as pending=0, preparing=1, completed=2, failed=3",
            )
            .build();

        let applied_total = meter
            .u64_counter("gkg_migration_applied_total")
            .with_description("Total migration terminal status transitions labelled by outcome")
            .build();

        let prepare_duration = meter
            .f64_histogram("gkg_migration_prepare_duration_seconds")
            .with_unit("s")
            .with_description("Duration of migration prepare() calls")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let desired_version = meter
            .u64_gauge("gkg_migration_desired_version")
            .with_description("Highest registered migration version")
            .build();

        let current_version = meter
            .u64_gauge("gkg_migration_current_version")
            .with_description("Highest completed migration version from the ledger")
            .build();

        Self {
            status,
            applied_total,
            prepare_duration,
            desired_version,
            current_version,
        }
    }

    pub fn record_registry(&self, registry: &MigrationRegistry) {
        let desired = registry
            .migrations()
            .iter()
            .map(|migration| migration.version())
            .max()
            .unwrap_or(0);

        self.desired_version.record(desired, &[]);

        // TODO(#418): Record reconciler-specific gauges/histograms once the
        // reconciler runtime exists.
    }

    pub fn record_ledger_state(&self, records: &[LedgerMigrationRecord]) {
        for record in records {
            self.status
                .record(status_code(record.status), &status_labels(record));
        }

        let current = records
            .iter()
            .filter(|record| record.status == MigrationStatus::Completed)
            .map(|record| record.version)
            .max()
            .unwrap_or(0);

        self.current_version.record(current, &[]);
    }

    pub fn record_transition(
        &self,
        migration: &dyn Migration,
        status: MigrationStatus,
        retry_count: u32,
    ) {
        self.status.record(
            status_code(status),
            &transition_labels(migration, status, retry_count),
        );

        if matches!(status, MigrationStatus::Completed | MigrationStatus::Failed) {
            self.applied_total.add(
                1,
                &[
                    migration_label(migration),
                    version_label(migration.version()),
                    migration_type_label(migration),
                    KeyValue::new("outcome", terminal_outcome(status)),
                ],
            );
        }

        if status == MigrationStatus::Completed {
            self.current_version.record(migration.version(), &[]);
        }
    }

    pub async fn record_prepare<T>(
        &self,
        migration: &dyn Migration,
        ctx: &MigrationContext,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let start = Instant::now();
        let result = operation.await;
        let duration = start.elapsed().as_secs_f64();
        self.prepare_duration.record(
            duration,
            &[
                migration_label(migration),
                version_label(migration.version()),
                migration_type_label(migration),
                KeyValue::new(
                    "outcome",
                    if result.is_ok() { "success" } else { "failure" },
                ),
                KeyValue::new("context", prepare_context_label(ctx)),
            ],
        );
        result
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Keep this in sync with the gkg_migration_status metric description above.
fn status_code(status: MigrationStatus) -> u64 {
    match status {
        MigrationStatus::Pending => 0,
        MigrationStatus::Preparing => 1,
        MigrationStatus::Completed => 2,
        MigrationStatus::Failed => 3,
    }
}

fn terminal_outcome(status: MigrationStatus) -> &'static str {
    match status {
        MigrationStatus::Completed => "success",
        MigrationStatus::Failed => "failure",
        MigrationStatus::Pending | MigrationStatus::Preparing => {
            unreachable!("non-terminal status")
        }
    }
}

// This allocates per observation. That's acceptable for the current control
// plane path and should only be revisited if ledger polling becomes hot.
fn status_labels(record: &LedgerMigrationRecord) -> [KeyValue; 4] {
    [
        KeyValue::new("migration", record.name.clone()),
        KeyValue::new("version", i64::try_from(record.version).unwrap_or(i64::MAX)),
        KeyValue::new("migration_type", record.migration_type.as_str()),
        KeyValue::new("status", record.status.as_str()),
    ]
}

fn transition_labels(
    migration: &dyn Migration,
    status: MigrationStatus,
    retry_count: u32,
) -> [KeyValue; 5] {
    [
        migration_label(migration),
        version_label(migration.version()),
        migration_type_label(migration),
        KeyValue::new("status", status.as_str()),
        KeyValue::new("retry_count", retry_bucket(retry_count)),
    ]
}

fn retry_bucket(retry_count: u32) -> &'static str {
    match retry_count {
        0 => "0",
        1..=3 => "1-3",
        _ => "4+",
    }
}

fn prepare_context_label(_ctx: &MigrationContext) -> &'static str {
    "migration"
}
fn migration_label(migration: &dyn Migration) -> KeyValue {
    KeyValue::new("migration", migration.name().to_owned())
}

fn version_label(version: u64) -> KeyValue {
    KeyValue::new("version", i64::try_from(version).unwrap_or(i64::MAX))
}

fn migration_type_label(migration: &dyn Migration) -> KeyValue {
    KeyValue::new("migration_type", migration.migration_type().as_str())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use opentelemetry::global;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use tokio::time::sleep;

    use super::MigrationMetrics;
    use crate::{
        LedgerMigrationRecord, Migration, MigrationContext, MigrationRegistry, MigrationStatus,
        MigrationType,
    };

    fn setup_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_millis(50))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());
        (provider, exporter)
    }

    fn metric_names(metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics]) -> Vec<String> {
        metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .map(|metric| metric.name().to_string())
            .collect()
    }

    fn find_metric<'a>(
        metrics: &'a [opentelemetry_sdk::metrics::data::ResourceMetrics],
        name: &str,
    ) -> &'a opentelemetry_sdk::metrics::data::Metric {
        metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|metric| metric.name() == name)
            .expect("metric should exist")
    }

    struct TestMigration;

    #[async_trait]
    impl Migration for TestMigration {
        fn version(&self) -> u64 {
            42
        }

        fn name(&self) -> &str {
            "test"
        }

        fn migration_type(&self) -> MigrationType {
            MigrationType::Additive
        }

        async fn prepare(&self, _ctx: &MigrationContext) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn dummy_client() -> clickhouse_client::ArrowClickHouseClient {
        clickhouse_client::ArrowClickHouseClient::new(
            "http://127.0.0.1:1",
            "default",
            "x",
            None,
            &std::collections::HashMap::new(),
        )
    }

    #[tokio::test]
    async fn registers_expected_metrics() {
        let (provider, exporter) = setup_meter_provider();
        let metrics = MigrationMetrics::new();
        let migration = TestMigration;
        let ctx = MigrationContext::new(dummy_client());
        let mut registry = MigrationRegistry::new();

        registry.register(Box::new(TestMigration));
        metrics.record_registry(&registry);
        metrics.record_transition(&migration, MigrationStatus::Pending, 0);
        metrics.record_transition(&migration, MigrationStatus::Completed, 0);
        metrics.record_ledger_state(&[LedgerMigrationRecord {
            version: 42,
            name: "test".to_string(),
            migration_type: MigrationType::Additive,
            status: MigrationStatus::Completed,
            started_at: None,
            completed_at: None,
            error_message: None,
            retry_count: 0,
        }]);
        let _ = metrics
            .record_prepare(&migration, &ctx, async { Ok::<(), anyhow::Error>(()) })
            .await;

        sleep(Duration::from_millis(100)).await;
        provider.force_flush().expect("flush metrics");

        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let names = metric_names(&finished);
        assert!(names.iter().any(|name| name == "gkg_migration_status"));
        assert!(
            names
                .iter()
                .any(|name| name == "gkg_migration_applied_total")
        );
        assert!(
            names
                .iter()
                .any(|name| name == "gkg_migration_prepare_duration_seconds")
        );
        assert!(
            names
                .iter()
                .any(|name| name == "gkg_migration_desired_version")
        );
        assert!(
            names
                .iter()
                .any(|name| name == "gkg_migration_current_version")
        );

        provider.shutdown().expect("shutdown provider");
    }

    #[tokio::test]
    async fn records_terminal_transition_and_versions() {
        let (provider, exporter) = setup_meter_provider();
        let metrics = MigrationMetrics::new();
        let migration = TestMigration;

        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(TestMigration));
        metrics.record_registry(&registry);
        metrics.record_transition(&migration, MigrationStatus::Completed, 1);
        metrics.record_ledger_state(&[LedgerMigrationRecord {
            version: 42,
            name: "test".to_string(),
            migration_type: MigrationType::Additive,
            status: MigrationStatus::Completed,
            started_at: None,
            completed_at: None,
            error_message: None,
            retry_count: 1,
        }]);

        sleep(Duration::from_millis(100)).await;
        provider.force_flush().expect("flush metrics");

        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let applied_total = find_metric(&finished, "gkg_migration_applied_total");
        match applied_total.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points: Vec<_> = sum.data_points().collect();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].value(), 1);
            }
            data => panic!("unexpected metric data: {data:?}"),
        }

        provider.shutdown().expect("shutdown provider");
    }

    #[tokio::test]
    async fn records_prepare_duration_for_failures() {
        let (provider, exporter) = setup_meter_provider();
        let metrics = MigrationMetrics::new();
        let migration = TestMigration;
        let ctx = MigrationContext::new(dummy_client());

        let result = metrics
            .record_prepare(&migration, &ctx, async { Err::<(), _>(anyhow!("boom")) })
            .await;
        assert!(result.is_err());

        sleep(Duration::from_millis(100)).await;
        provider.force_flush().expect("flush metrics");

        let finished = exporter.get_finished_metrics().expect("finished metrics");
        let prepare_duration = find_metric(&finished, "gkg_migration_prepare_duration_seconds");
        match prepare_duration.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                let points: Vec<_> = histogram.data_points().collect();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].count(), 1);
            }
            data => panic!("unexpected metric data: {data:?}"),
        }

        provider.shutdown().expect("shutdown provider");
    }
}
