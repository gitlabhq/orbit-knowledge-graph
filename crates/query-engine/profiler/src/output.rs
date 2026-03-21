use serde::Serialize;
use shared::{PipelineOutput, QueryExecution};

#[derive(Serialize)]
pub struct ProfilerOutput {
    pub query: serde_json::Value,
    pub security_context: SecurityContextInfo,
    pub compilation: CompilationInfo,
    pub executions: Vec<QueryExecution>,
    pub summary: ExecutionSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_health: Option<serde_json::Value>,
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

#[derive(Serialize)]
pub struct ExecutionSummary {
    pub total_queries: usize,
    pub total_read_rows: u64,
    pub total_read_bytes: u64,
    pub total_memory_usage: i64,
    pub total_elapsed_ms: f64,
    pub result_rows: usize,
}

pub fn build_output(
    query_json: &str,
    org_id: i64,
    traversal_paths: &[String],
    output: &PipelineOutput,
    instance_health: Option<serde_json::Value>,
) -> ProfilerOutput {
    let query: serde_json::Value =
        serde_json::from_str(query_json).unwrap_or(serde_json::Value::String(query_json.into()));

    let hydration_plan = format!("{:?}", output.compiled.hydration);
    let execs = &output.execution_log;

    let summary = ExecutionSummary {
        total_queries: execs.len(),
        total_read_rows: execs.iter().map(|e| e.stats.read_rows).sum(),
        total_read_bytes: execs.iter().map(|e| e.stats.read_bytes).sum(),
        total_memory_usage: execs.iter().map(|e| e.stats.memory_usage).sum(),
        total_elapsed_ms: execs.iter().map(|e| e.elapsed_ms).sum(),
        result_rows: output.row_count,
    };

    ProfilerOutput {
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
        executions: output.execution_log.clone(),
        summary,
        instance_health,
    }
}
