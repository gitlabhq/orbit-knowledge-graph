use serde::Serialize;
use shared::QueryExecution;

use crate::executor::ProfilerResult;

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
    result: &ProfilerResult,
    instance_health: Option<serde_json::Value>,
) -> ProfilerOutput {
    let query: serde_json::Value =
        serde_json::from_str(query_json).unwrap_or(serde_json::Value::String(query_json.into()));

    let hydration_plan = match &result.compiled.hydration {
        compiler::HydrationPlan::None => "None",
        compiler::HydrationPlan::Static(_) => "Static",
        compiler::HydrationPlan::Dynamic => "Dynamic",
    };

    let summary = ExecutionSummary {
        total_queries: result.executions.len(),
        total_read_rows: result.executions.iter().map(|e| e.stats.read_rows).sum(),
        total_read_bytes: result.executions.iter().map(|e| e.stats.read_bytes).sum(),
        total_memory_usage: result.executions.iter().map(|e| e.stats.memory_usage).sum(),
        total_elapsed_ms: result.executions.iter().map(|e| e.elapsed_ms).sum(),
        result_rows: result.result_rows,
    };

    ProfilerOutput {
        query,
        security_context: SecurityContextInfo {
            org_id,
            traversal_paths: traversal_paths.to_vec(),
        },
        compilation: CompilationInfo {
            query_type: result.compiled.query_type.to_string(),
            parameterized_sql: result.compiled.base.sql.clone(),
            rendered_sql: result.compiled.base.render(),
            hydration_plan: hydration_plan.into(),
        },
        executions: result.executions.clone(),
        summary,
        instance_health,
    }
}
