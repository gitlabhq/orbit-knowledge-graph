use std::collections::HashMap;

use clickhouse_client::ArrowClickHouseClient;
use orbit_arrow::ColumnValue;

use super::client_pool::TenantClientPool;
use super::host_map::TenantRoute;

pub const PIPELINE_TRACE_ID: &str = "pipeline_trace_id";
pub const PIPELINE_TRACE_DURATION: &str = "pipeline_trace_duration";
pub const PIPELINE_TRACE_ERROR_COUNT: &str = "pipeline_trace_error_count";
pub const JOB_SPAN_ID: &str = "job_span_id";
pub const JOB_SPAN_DURATION: &str = "job_span_duration";

const PIPELINE_ID_ATTRIBUTE: &str = "cicd.pipeline.run.id";
const JOB_ID_ATTRIBUTE: &str = "cicd.pipeline.task.run.id";

/// Spans are exported when the pipeline finishes, which can trail the row's
/// own timestamps, so the window is widened on both sides rather than pinned
/// to created_at.
const WINDOW_LEAD: i64 = 3_600;
const WINDOW_TRAIL: i64 = 86_400;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TraceLookup {
    PipelineTraceId,
    PipelineTraceDuration,
    PipelineTraceErrorCount,
    JobSpanId,
    JobSpanDuration,
}

impl TraceLookup {
    pub fn from_name(lookup: &str) -> Option<Self> {
        match lookup {
            PIPELINE_TRACE_ID => Some(Self::PipelineTraceId),
            PIPELINE_TRACE_DURATION => Some(Self::PipelineTraceDuration),
            PIPELINE_TRACE_ERROR_COUNT => Some(Self::PipelineTraceErrorCount),
            JOB_SPAN_ID => Some(Self::JobSpanId),
            JOB_SPAN_DURATION => Some(Self::JobSpanDuration),
            _ => None,
        }
    }

    fn id_attribute(self) -> &'static str {
        match self {
            Self::PipelineTraceId | Self::PipelineTraceDuration | Self::PipelineTraceErrorCount => {
                PIPELINE_ID_ATTRIBUTE
            }
            Self::JobSpanId | Self::JobSpanDuration => JOB_ID_ATTRIBUTE,
        }
    }

    fn projection(self) -> &'static str {
        match self {
            Self::PipelineTraceId => "any(trace_id) AS value",
            Self::JobSpanId => "any(span_id) AS value",
            Self::PipelineTraceDuration | Self::JobSpanDuration => {
                "toString(max(duration_nano)) AS value"
            }
            Self::PipelineTraceErrorCount => "toString(countIf(has_error)) AS value",
        }
    }

    pub fn column_value(self, raw: String) -> Option<ColumnValue> {
        match self {
            Self::PipelineTraceId | Self::JobSpanId => {
                (!raw.is_empty()).then_some(ColumnValue::String(raw))
            }
            Self::PipelineTraceDuration | Self::JobSpanDuration | Self::PipelineTraceErrorCount => {
                raw.parse::<i64>().ok().map(ColumnValue::Int64)
            }
        }
    }
}

/// A row's join key plus the time range its spans can fall in. Without the
/// range every lookup scans the tenant's whole retention, because the index
/// table is sorted by time bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceKey {
    pub entity_id: String,
    pub from_unix: i64,
    pub to_unix: i64,
}

pub fn trace_key(props: &HashMap<String, ColumnValue>) -> Option<TraceKey> {
    let entity_id = props
        .get("id")
        .and_then(|value| value.coerce::<i64>())
        .map(|id| id.to_string())?;

    let started = timestamp(props, "created_at").or_else(|| timestamp(props, "started_at"))?;
    let finished = timestamp(props, "finished_at").unwrap_or(started);

    Some(TraceKey {
        entity_id,
        from_unix: started - WINDOW_LEAD,
        to_unix: finished + WINDOW_TRAIL,
    })
}

/// Hydration renders a `type: timestamp` property as an ISO-8601 string, not
/// an epoch, so coercing straight to i64 yields nothing.
fn timestamp(props: &HashMap<String, ColumnValue>, name: &str) -> Option<i64> {
    let value = props.get(name)?;

    let seconds = match value {
        ColumnValue::Int64(seconds) => Some(*seconds),
        ColumnValue::String(text) => parse_epoch_seconds(text),
        _ => value.coerce::<i64>(),
    }?;

    (seconds > 0).then_some(seconds)
}

const HYDRATED_FORMATS: [&str; 4] = [
    "%Y-%m-%dT%H:%M:%S%.fZ",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
];

fn parse_epoch_seconds(text: &str) -> Option<i64> {
    if let Ok(seconds) = text.parse::<i64>() {
        return Some(seconds);
    }

    HYDRATED_FORMATS.iter().find_map(|format| {
        chrono::NaiveDateTime::parse_from_str(text, format)
            .ok()
            .map(|parsed| parsed.and_utc().timestamp())
    })
}

pub fn build_sql(lookup: TraceLookup) -> String {
    format!(
        "SELECT attributes_string[{{attribute:String}}] AS entity_id, {projection} \
         FROM signoz_index_v3 \
         WHERE ts_bucket_start BETWEEN {{from_bucket:UInt64}} AND {{to_bucket:UInt64}} \
         AND timestamp BETWEEN fromUnixTimestamp({{from_unix:UInt64}}) \
         AND fromUnixTimestamp({{to_unix:UInt64}}) \
         AND attributes_string[{{attribute:String}}] IN {{ids:Array(String)}} \
         GROUP BY entity_id",
        projection = lookup.projection()
    )
}

/// The index table buckets by half hour, so the bucket bounds are rounded out
/// from the time range to avoid clipping a span at either edge.
const BUCKET_SECONDS: i64 = 1_800;

pub fn bucket_bounds(from_unix: i64, to_unix: i64) -> (u64, u64) {
    let from = (from_unix - BUCKET_SECONDS).max(0) / BUCKET_SECONDS * BUCKET_SECONDS;
    let to = (to_unix + BUCKET_SECONDS) / BUCKET_SECONDS * BUCKET_SECONDS;
    (from.max(0) as u64, to.max(0) as u64)
}

pub async fn fetch(
    pool: &TenantClientPool,
    route: &TenantRoute,
    lookup: TraceLookup,
    keys: &[TraceKey],
) -> Result<HashMap<String, String>, clickhouse_client::ClickHouseError> {
    let Some(from_unix) = keys.iter().map(|key| key.from_unix).min() else {
        return Ok(HashMap::new());
    };
    let to_unix = keys
        .iter()
        .map(|key| key.to_unix)
        .max()
        .unwrap_or(from_unix);
    let ids: Vec<String> = keys.iter().map(|key| key.entity_id.clone()).collect();
    let (from_bucket, to_bucket) = bucket_bounds(from_unix, to_unix);

    let client: ArrowClickHouseClient = pool.traces_client(route);

    let batches = client
        .query(&build_sql(lookup))
        .param("attribute", lookup.id_attribute())
        .param("from_bucket", from_bucket)
        .param("to_bucket", to_bucket)
        .param("from_unix", from_unix.max(0) as u64)
        .param("to_unix", to_unix.max(0) as u64)
        .param("ids", &ids)
        .fetch_arrow()
        .await?;

    Ok(super::rows::two_string_columns(&batches))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(id: i64, created_at: i64, finished_at: Option<i64>) -> HashMap<String, ColumnValue> {
        let mut props = HashMap::new();
        props.insert("id".to_string(), ColumnValue::Int64(id));
        props.insert("created_at".to_string(), ColumnValue::Int64(created_at));
        if let Some(finished_at) = finished_at {
            props.insert("finished_at".to_string(), ColumnValue::Int64(finished_at));
        }
        props
    }

    #[test]
    fn pipeline_lookups_join_on_the_pipeline_run_attribute() {
        for lookup in [
            TraceLookup::PipelineTraceId,
            TraceLookup::PipelineTraceDuration,
            TraceLookup::PipelineTraceErrorCount,
        ] {
            assert_eq!(lookup.id_attribute(), "cicd.pipeline.run.id");
        }
    }

    #[test]
    fn job_lookups_join_on_the_task_run_attribute() {
        for lookup in [TraceLookup::JobSpanId, TraceLookup::JobSpanDuration] {
            assert_eq!(lookup.id_attribute(), "cicd.pipeline.task.run.id");
        }
    }

    #[test]
    fn every_declared_lookup_name_maps_to_a_variant() {
        for name in [
            PIPELINE_TRACE_ID,
            PIPELINE_TRACE_DURATION,
            PIPELINE_TRACE_ERROR_COUNT,
            JOB_SPAN_ID,
            JOB_SPAN_DURATION,
        ] {
            assert!(TraceLookup::from_name(name).is_some(), "{name}");
        }
    }

    #[test]
    fn an_unknown_lookup_name_maps_to_nothing() {
        assert!(TraceLookup::from_name("not_a_lookup").is_none());
    }

    #[test]
    fn sql_parameterises_the_identifiers_rather_than_interpolating_them() {
        let sql = build_sql(TraceLookup::PipelineTraceId);

        assert!(sql.contains("{ids:Array(String)}"));
        assert!(sql.contains("{attribute:String}"));
        assert!(!sql.contains("cicd.pipeline.run.id"));
    }

    #[test]
    fn sql_bounds_the_scan_by_bucket_and_timestamp() {
        let sql = build_sql(TraceLookup::PipelineTraceId);

        assert!(
            sql.contains("ts_bucket_start BETWEEN {from_bucket:UInt64} AND {to_bucket:UInt64}"),
            "the sort key starts with the bucket, so it must be bounded: {sql}"
        );
        assert!(sql.contains("timestamp BETWEEN fromUnixTimestamp({from_unix:UInt64})"));
    }

    #[test]
    fn a_key_spans_from_before_the_row_started_to_after_it_finished() {
        let key = trace_key(&props(42, 1_000_000, Some(1_000_600))).expect("key");

        assert_eq!(key.entity_id, "42");
        assert_eq!(key.from_unix, 1_000_000 - WINDOW_LEAD);
        assert_eq!(key.to_unix, 1_000_600 + WINDOW_TRAIL);
    }

    #[test]
    fn an_unfinished_row_still_yields_a_window() {
        let key = trace_key(&props(42, 1_000_000, None)).expect("key");

        assert_eq!(key.to_unix, 1_000_000 + WINDOW_TRAIL);
    }

    /// The value hydration actually produces for a `type: timestamp` property.
    fn hydrated(seconds: i64) -> ColumnValue {
        ColumnValue::datetime(chrono::DateTime::from_timestamp(seconds, 0).map(|dt| dt.naive_utc()))
    }

    #[test]
    fn a_key_builds_from_the_timestamps_hydration_produces() {
        let mut props = HashMap::new();
        props.insert("id".to_string(), ColumnValue::Int64(42));
        props.insert("created_at".to_string(), hydrated(1_000_000));
        props.insert("finished_at".to_string(), hydrated(1_000_600));

        let key = trace_key(&props).expect("hydrated timestamps must build a key");

        assert_eq!(key.from_unix, 1_000_000 - WINDOW_LEAD);
        assert_eq!(key.to_unix, 1_000_600 + WINDOW_TRAIL);
    }

    #[test]
    fn every_hydrated_timestamp_shape_parses() {
        for text in [
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:00.123456Z",
            "2024-01-01T00:00:00",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00.123456",
            "1704067200",
        ] {
            assert_eq!(
                parse_epoch_seconds(text),
                Some(1_704_067_200),
                "failed to parse {text}"
            );
        }
    }

    #[test]
    fn an_unparseable_timestamp_has_no_key() {
        let mut props = HashMap::new();
        props.insert("id".to_string(), ColumnValue::Int64(42));
        props.insert(
            "created_at".to_string(),
            ColumnValue::String("not a time".to_string()),
        );

        assert!(trace_key(&props).is_none());
    }

    #[test]
    fn a_row_without_a_timestamp_has_no_key() {
        let mut props = HashMap::new();
        props.insert("id".to_string(), ColumnValue::Int64(42));

        assert!(trace_key(&props).is_none());
    }

    #[test]
    fn a_row_without_an_id_has_no_key() {
        assert!(trace_key(&HashMap::new()).is_none());
    }

    #[test]
    fn bucket_bounds_round_outwards() {
        let (from, to) = bucket_bounds(3_600, 7_200);

        assert!(from <= 3_600, "lower bound must not clip: {from}");
        assert!(to >= 7_200, "upper bound must not clip: {to}");
        assert_eq!(from % BUCKET_SECONDS as u64, 0);
    }

    #[test]
    fn bucket_bounds_never_go_negative() {
        let (from, _) = bucket_bounds(0, 100);

        assert_eq!(from, 0);
    }

    #[test]
    fn id_columns_resolve_to_strings() {
        assert_eq!(
            TraceLookup::PipelineTraceId.column_value("abc123".to_string()),
            Some(ColumnValue::String("abc123".to_string()))
        );
    }

    #[test]
    fn numeric_columns_resolve_to_integers() {
        assert_eq!(
            TraceLookup::PipelineTraceDuration.column_value("4200".to_string()),
            Some(ColumnValue::Int64(4200))
        );
    }

    #[test]
    fn an_empty_trace_id_resolves_to_nothing() {
        assert!(
            TraceLookup::PipelineTraceId
                .column_value(String::new())
                .is_none()
        );
    }

    #[test]
    fn an_unparseable_number_resolves_to_nothing() {
        assert!(
            TraceLookup::JobSpanDuration
                .column_value("not a number".to_string())
                .is_none()
        );
    }
}
