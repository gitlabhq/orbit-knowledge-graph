//! The live half of the enrich stage: resolve the live properties a query returns
//! and merge them into the hydration property map, before stored dependency columns
//! are stripped.
//!
//! Core hydration has already fetched each live operation's key columns (they are
//! `depends_on` of the live virtual field). This reads those key values per row,
//! runs one [`extensions::live`] call per operation across every provider at once,
//! and writes the resolved values back. A key whose status is not `OK` yields null
//! values and a `_live_status` annotation; a key set past `max_keys` fails the
//! query. The whole step is skipped when no returned property is live, so a query
//! without live properties runs exactly as before.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use extensions::live::{
    KeyStatus, LiveCall, LiveCatalog, LiveProvider, LiveValue, run_live_operations,
};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};
use orbit_arrow::ColumnValue;
use query_engine::compiler::SecurityContext;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::PropertyMap;

/// The `virtual.service` marker the loader puts on a live field.
pub const LIVE_SERVICE: &str = extensions::live::LIVE_SERVICE;
pub const LIVE_STATUS_COLUMN: &str = "_live_status";
pub const DEFAULT_LIVE_BUDGET_MS: u64 = 3000;

/// The live catalog and one provider per package, resolved at startup. Its absence
/// from the pipeline context keeps the hot path unchanged.
#[derive(Clone)]
pub struct LiveRuntime {
    pub catalog: LiveCatalog,
    pub providers: HashMap<String, Arc<dyn LiveProvider>>,
}

/// The query's total live budget, shared across every operation.
#[derive(Debug, Clone, Copy)]
pub struct LiveBudget(pub Duration);

impl Default for LiveBudget {
    fn default() -> Self {
        Self(Duration::from_millis(DEFAULT_LIVE_BUDGET_MS))
    }
}

/// One live property a query returns, taken from a hydration template's virtual
/// columns where the service is [`LIVE_SERVICE`].
#[derive(Debug, Clone)]
pub struct LiveColumn {
    pub entity: String,
    pub operation_id: String,
    pub property: String,
}

#[derive(Debug, Default, Clone)]
pub struct LiveStageStats {
    pub duration: Duration,
    pub key_count: usize,
    pub status_counts: BTreeMap<&'static str, usize>,
}

/// Resolves every live property into `property_map`. Returns the per-stage metrics
/// for the pipeline observer. Errors only when a key set exceeds `max_keys`; a slow
/// or failing provider degrades to null values and a status, never an error.
pub async fn enrich_live_properties(
    runtime: &LiveRuntime,
    security: &SecurityContext,
    user_id: i64,
    live_columns: &[LiveColumn],
    property_map: &mut PropertyMap,
    budget: Duration,
) -> Result<LiveStageStats, PipelineError> {
    if live_columns.is_empty() {
        return Ok(LiveStageStats::default());
    }
    let started = Instant::now();
    let scope_paths: Vec<String> = security
        .traversal_paths
        .iter()
        .map(|path| path.path.to_string())
        .collect();

    let row_keys = index_row_keys(runtime, live_columns, property_map);
    let calls = build_calls(runtime, &row_keys);
    let outcomes = run_calls(runtime, &scope_paths, user_id, calls, budget).await?;

    let mut stats = LiveStageStats {
        key_count: row_keys.values().map(|ops| ops.len()).sum(),
        ..LiveStageStats::default()
    };
    apply_outcomes(live_columns, &row_keys, &outcomes, property_map, &mut stats);
    stats.duration = started.elapsed();
    record_metrics(&stats);
    Ok(stats)
}

struct LiveMetrics {
    duration: Histogram<f64>,
    keys: Histogram<u64>,
    statuses: Counter<u64>,
}

static METRICS: LazyLock<LiveMetrics> = LazyLock::new(|| {
    let meter = orbit_observability::meter();
    LiveMetrics {
        duration: meter
            .f64_histogram("gkg.query.pipeline.live.duration")
            .with_description("Time spent resolving live properties from providers.")
            .with_unit("s")
            .build(),
        keys: meter
            .u64_histogram("gkg.query.pipeline.live.keys")
            .with_description("Distinct live keys resolved per query.")
            .build(),
        statuses: meter
            .u64_counter("gkg.query.pipeline.live.status")
            .with_description("Live key resolutions by per-key status.")
            .build(),
    }
});

fn record_metrics(stats: &LiveStageStats) {
    METRICS.duration.record(stats.duration.as_secs_f64(), &[]);
    METRICS.keys.record(stats.key_count as u64, &[]);
    for (status, count) in &stats.status_counts {
        METRICS
            .statuses
            .add(*count as u64, &[KeyValue::new("status", *status)]);
    }
}

type RowKeyIndex = HashMap<(String, i64), HashMap<String, Vec<String>>>;

/// For every row, the key tuple of each operation that touches its entity. Reads
/// the key columns core hydration already placed in the property map.
fn index_row_keys(
    runtime: &LiveRuntime,
    live_columns: &[LiveColumn],
    property_map: &PropertyMap,
) -> RowKeyIndex {
    let operations: BTreeMap<&str, &str> = live_columns
        .iter()
        .map(|column| (column.operation_id.as_str(), column.entity.as_str()))
        .collect();

    let mut index: RowKeyIndex = HashMap::new();
    for (operation_id, entity) in operations {
        let Some(spec) = runtime.catalog.get(operation_id) else {
            continue;
        };
        let entity = if entity.is_empty() {
            &spec.entity
        } else {
            entity
        };
        for ((row_entity, id), props) in property_map.iter() {
            if row_entity != entity {
                continue;
            }
            let key: Vec<String> = spec.keys.iter().map(|k| key_value(props.get(k))).collect();
            index
                .entry((row_entity.clone(), *id))
                .or_default()
                .insert(operation_id.to_string(), key);
        }
    }
    index
}

/// One call per operation, carrying the distinct key set over every row.
fn build_calls(runtime: &LiveRuntime, row_keys: &RowKeyIndex) -> Vec<LiveCall> {
    let mut keys_by_operation: BTreeMap<String, Vec<Vec<String>>> = BTreeMap::new();
    for ops in row_keys.values() {
        for (operation_id, key) in ops {
            let keys = keys_by_operation.entry(operation_id.clone()).or_default();
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
    }
    keys_by_operation
        .into_iter()
        .filter_map(|(operation_id, keys)| {
            runtime.catalog.get(&operation_id).map(|spec| LiveCall {
                spec: spec.clone(),
                keys,
            })
        })
        .collect()
}

type OutcomeIndex = HashMap<String, extensions::live::LiveOperationOutcome>;

/// Runs the calls grouped by provider package, all providers at once.
async fn run_calls(
    runtime: &LiveRuntime,
    scope_paths: &[String],
    user_id: i64,
    calls: Vec<LiveCall>,
    budget: Duration,
) -> Result<OutcomeIndex, PipelineError> {
    let mut by_provider: BTreeMap<String, Vec<LiveCall>> = BTreeMap::new();
    for call in calls {
        by_provider
            .entry(call.spec.package.clone())
            .or_default()
            .push(call);
    }

    let mut outcomes = OutcomeIndex::new();
    for (package, calls) in by_provider {
        let Some(provider) = runtime.providers.get(&package) else {
            continue;
        };
        let results = run_live_operations(provider.as_ref(), scope_paths, user_id, calls, budget)
            .await
            .map_err(|e| PipelineError::ContentResolution(e.to_string()))?;
        for outcome in results {
            outcomes.insert(outcome.operation_id.clone(), outcome);
        }
    }
    Ok(outcomes)
}

/// Writes resolved values into each row, or null plus a `_live_status` note for a
/// key the provider did not return `OK`.
fn apply_outcomes(
    live_columns: &[LiveColumn],
    row_keys: &RowKeyIndex,
    outcomes: &OutcomeIndex,
    property_map: &mut PropertyMap,
    stats: &mut LiveStageStats,
) {
    let mut columns_by_operation: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for column in live_columns {
        columns_by_operation
            .entry(column.operation_id.as_str())
            .or_default()
            .push(column.property.as_str());
    }

    for ((entity, id), ops) in row_keys {
        let Some(props) = property_map.get_mut(&(entity.clone(), *id)) else {
            continue;
        };
        for (operation_id, key) in ops {
            let Some(properties) = columns_by_operation.get(operation_id.as_str()) else {
                continue;
            };
            let status = outcomes
                .get(operation_id)
                .and_then(|outcome| outcome.statuses.get(key).copied())
                .unwrap_or(KeyStatus::Error);
            *stats.status_counts.entry(status.as_str()).or_default() += 1;

            let resolved = outcomes
                .get(operation_id)
                .and_then(|outcome| outcome.rows.get(key));
            for property in properties {
                let value = (status == KeyStatus::Ok)
                    .then(|| resolved.and_then(|row| row.get(*property)))
                    .flatten()
                    .map(to_column_value)
                    .unwrap_or(ColumnValue::Null);
                props.insert((*property).to_string(), value);
            }
            if status != KeyStatus::Ok {
                note_status(props, operation_id, status);
            }
        }
    }
}

fn note_status(props: &mut HashMap<String, ColumnValue>, operation_id: &str, status: KeyStatus) {
    let operation = operation_id.rsplit('|').next().unwrap_or(operation_id);
    let note = format!("{operation}={}", status.as_str());
    let merged = match props.get(LIVE_STATUS_COLUMN) {
        Some(ColumnValue::String(existing)) => format!("{existing},{note}"),
        _ => note,
    };
    props.insert(LIVE_STATUS_COLUMN.to_string(), ColumnValue::String(merged));
}

fn to_column_value(value: &LiveValue) -> ColumnValue {
    match value {
        LiveValue::String(v) => ColumnValue::String(v.clone()),
        LiveValue::Int(v) => ColumnValue::Int64(*v),
        LiveValue::Float(v) => ColumnValue::Float64(*v),
        LiveValue::Bool(v) => ColumnValue::Bool(*v),
        LiveValue::TimestampUnixMs(v) => ColumnValue::Int64(*v),
    }
}

fn key_value(value: Option<&ColumnValue>) -> String {
    match value {
        Some(ColumnValue::String(v)) => v.clone(),
        Some(ColumnValue::Int64(v)) => v.to_string(),
        Some(ColumnValue::Float64(v)) => v.to_string(),
        Some(ColumnValue::Bool(v)) => v.to_string(),
        Some(ColumnValue::Null) | None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use extensions::live::LiveStream;
    use extensions::package::{Package, short_digest};
    use extensions::proto::LiveRequest;
    use futures::stream;

    const WIDGETS: &str =
        include_str!("../../../../extensions/tests/fixtures/example.widgets.yaml");

    struct StubProvider;

    #[async_trait]
    impl LiveProvider for StubProvider {
        async fn live(&self, request: LiveRequest) -> Result<LiveStream, tonic::Status> {
            use extensions::proto::{LiveResponse, Row, Status as S, Value, live_response, value};
            let mut messages = Vec::new();
            for key in &request.keys {
                if key.values[0] == "fail" {
                    messages.push(Ok(LiveResponse {
                        kind: Some(live_response::Kind::Status(extensions::proto::KeyStatus {
                            key: Some(key.clone()),
                            status: S::NotFound as i32,
                            message: String::new(),
                        })),
                    }));
                    continue;
                }
                let mut properties = std::collections::HashMap::new();
                properties.insert(
                    "colour".to_string(),
                    Value {
                        kind: Some(value::Kind::StringValue(format!("c-{}", key.values[0]))),
                    },
                );
                properties.insert(
                    "weight".to_string(),
                    Value {
                        kind: Some(value::Kind::FloatValue(2.0)),
                    },
                );
                messages.push(Ok(LiveResponse {
                    kind: Some(live_response::Kind::Row(Row {
                        key: Some(key.clone()),
                        properties,
                    })),
                }));
                messages.push(Ok(LiveResponse {
                    kind: Some(live_response::Kind::Status(extensions::proto::KeyStatus {
                        key: Some(key.clone()),
                        status: S::Ok as i32,
                        message: String::new(),
                    })),
                }));
            }
            Ok(Box::pin(stream::iter(messages)))
        }
    }

    fn runtime() -> LiveRuntime {
        let package = Package::parse(WIDGETS, short_digest(WIDGETS)).expect("parses");
        let catalog = LiveCatalog::from_packages(&[package]);
        let mut providers: HashMap<String, Arc<dyn LiveProvider>> = HashMap::new();
        providers.insert("example.widgets".into(), Arc::new(StubProvider));
        LiveRuntime { catalog, providers }
    }

    fn row(name: &str) -> HashMap<String, ColumnValue> {
        HashMap::from([
            ("name".to_string(), ColumnValue::String(name.into())),
            ("status".to_string(), ColumnValue::String("active".into())),
        ])
    }

    #[tokio::test]
    async fn fills_live_values_and_notes_failures() {
        let runtime = runtime();
        let security = SecurityContext::new(42, vec!["1/9000000000042/".into()]).unwrap();
        let live_columns = vec![
            LiveColumn {
                entity: "Widget".into(),
                operation_id: "example.widgets|widgets.get".into(),
                property: "colour".into(),
            },
            LiveColumn {
                entity: "Widget".into(),
                operation_id: "example.widgets|widgets.get".into(),
                property: "weight".into(),
            },
        ];
        let mut property_map: PropertyMap = HashMap::from([
            (("Widget".to_string(), 1), row("ok")),
            (("Widget".to_string(), 2), row("fail")),
        ]);

        let stats = enrich_live_properties(
            &runtime,
            &security,
            42,
            &live_columns,
            &mut property_map,
            Duration::from_secs(3),
        )
        .await
        .expect("enrich runs");

        let ok = &property_map[&("Widget".to_string(), 1)];
        assert_eq!(ok["colour"], ColumnValue::String("c-ok".into()));
        assert_eq!(ok["weight"], ColumnValue::Float64(2.0));
        assert!(
            !ok.contains_key(LIVE_STATUS_COLUMN),
            "an OK row has no status note"
        );

        let failed = &property_map[&("Widget".to_string(), 2)];
        assert_eq!(failed["colour"], ColumnValue::Null);
        assert_eq!(
            failed[LIVE_STATUS_COLUMN],
            ColumnValue::String("widgets.get=NOT_FOUND".into())
        );
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.status_counts.get("OK"), Some(&1));
        assert_eq!(stats.status_counts.get("NOT_FOUND"), Some(&1));
    }
}
