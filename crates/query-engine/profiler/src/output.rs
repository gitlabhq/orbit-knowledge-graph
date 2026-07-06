use serde::Serialize;
use shared::PipelineOutput;

#[derive(Serialize)]
pub struct ProfilerOutput {
    pub correlation_id: String,
    pub query: serde_json::Value,
    pub security_context: SecurityContextInfo,
    pub compilation: CompilationInfo,
    pub queries: Vec<RanQuery>,
    pub summary: ExecutionSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_health: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct PaginationInfo {
    pub has_more: bool,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
pub struct SecurityContextInfo {
    pub org_id: i64,
    pub traversal_paths: Vec<String>,
}

#[derive(Serialize)]
pub struct CompilationInfo {
    pub query_type: String,
    pub parameterized_sql: String,
    pub rendered_sql: String,
    pub hydration_plan: String,
}

/// One actual ClickHouse query the run issued (the base query, then one per
/// hydration pass). Metrics come from the response's `X-ClickHouse-Summary`
/// header, which is accurate for rows/bytes/server-time. Peak memory and
/// ProfileEvents are intentionally absent: they require cross-replica
/// `system.query_log` (which the relay user cannot read), so fetch them
/// out-of-band by `correlation_id`.
#[derive(Serialize)]
pub struct RanQuery {
    pub label: String,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: u64,
    pub elapsed_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_pipeline: Option<String>,
}

#[derive(Serialize)]
pub struct ExecutionSummary {
    pub total_queries: usize,
    pub total_read_rows: u64,
    pub total_read_bytes: u64,
    pub total_elapsed_ms: f64,
    pub result_rows: usize,
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

pub fn build_output(
    query_json: &str,
    org_id: i64,
    traversal_paths: &[String],
    output: &PipelineOutput,
    instance_health: Option<serde_json::Value>,
    correlation_id: &str,
) -> ProfilerOutput {
    let query: serde_json::Value =
        serde_json::from_str(query_json).unwrap_or(serde_json::Value::String(query_json.into()));

    let hydration_plan = format!("{:?}", output.compiled.hydration);

    let queries: Vec<RanQuery> = output
        .execution_log
        .iter()
        .map(|e| RanQuery {
            label: e.label.clone(),
            read_rows: e.stats.read_rows,
            read_bytes: e.stats.read_bytes,
            result_rows: e.stats.result_rows,
            elapsed_ms: ns_to_ms(e.stats.elapsed_ns),
            explain_plan: e.explain_plan.clone(),
            explain_pipeline: e.explain_pipeline.clone(),
        })
        .collect();

    let summary = ExecutionSummary {
        total_queries: queries.len(),
        total_read_rows: queries.iter().map(|q| q.read_rows).sum(),
        total_read_bytes: queries.iter().map(|q| q.read_bytes).sum(),
        total_elapsed_ms: queries.iter().map(|q| q.elapsed_ms).sum(),
        result_rows: output.row_count,
    };

    ProfilerOutput {
        correlation_id: correlation_id.to_string(),
        query,
        security_context: SecurityContextInfo {
            org_id,
            traversal_paths: traversal_paths.to_vec(),
        },
        compilation: CompilationInfo {
            query_type: output.compiled.query_type.to_string(),
            parameterized_sql: output.compiled.base.sql.clone(),
            rendered_sql: output.compiled.base.render(),
            hydration_plan,
        },
        queries,
        summary,
        pagination: output.pagination.as_ref().map(|p| PaginationInfo {
            has_more: p.has_more,
            truncated: p.truncated,
            next_cursor: p.next_cursor.clone(),
        }),
        instance_health,
    }
}
