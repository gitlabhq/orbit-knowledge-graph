//! Resolves pipeline and job trace columns from a tenant's Observability
//! ClickHouse.
//!
//! GitLab CI already exports pipeline traces with OpenTelemetry semantic
//! convention attributes, so a graph row's `id` joins straight onto
//! `cicd.pipeline.run.id` or `cicd.pipeline.task.run.id` with no extra
//! instrumentation.

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

/// Span attribute carrying the GitLab pipeline ID, written by
/// `Gitlab::Observability::PipelineConverterBase`.
const PIPELINE_ID_ATTRIBUTE: &str = "cicd.pipeline.run.id";
const JOB_ID_ATTRIBUTE: &str = "cicd.pipeline.task.run.id";

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

    /// Pipeline-level columns aggregate the whole trace; job-level columns read
    /// the single span for that job.
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

/// A pipeline's trace is identified by the pipeline's own ID on its spans, so
/// the join key is the graph row's `id` rendered as a string.
pub fn entity_id(props: &HashMap<String, ColumnValue>) -> Option<String> {
    props
        .get("id")
        .and_then(|value| value.coerce::<i64>())
        .map(|id| id.to_string())
}

pub fn build_sql(lookup: TraceLookup) -> String {
    format!(
        "SELECT attributes_string[{{attribute:String}}] AS entity_id, {projection} \
         FROM signoz_index_v3 \
         WHERE attributes_string[{{attribute:String}}] IN {{ids:Array(String)}} \
         GROUP BY entity_id",
        projection = lookup.projection()
    )
}

pub async fn fetch(
    pool: &TenantClientPool,
    route: &TenantRoute,
    lookup: TraceLookup,
    ids: &[String],
) -> Result<HashMap<String, String>, clickhouse_client::ClickHouseError> {
    let client: ArrowClickHouseClient = pool.traces_client(route);

    let batches = client
        .query(&build_sql(lookup))
        .param("attribute", lookup.id_attribute())
        .param("ids", ids)
        .fetch_arrow()
        .await?;

    Ok(super::rows::two_string_columns(&batches))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            TraceLookup::PipelineTraceErrorCount.column_value("3".to_string()),
            Some(ColumnValue::Int64(3))
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

    #[test]
    fn the_join_key_is_the_row_id_as_a_string() {
        let mut props = HashMap::new();
        props.insert("id".to_string(), ColumnValue::Int64(42));

        assert_eq!(entity_id(&props), Some("42".to_string()));
    }

    #[test]
    fn a_row_without_an_id_has_no_join_key() {
        assert!(entity_id(&HashMap::new()).is_none());
    }
}
