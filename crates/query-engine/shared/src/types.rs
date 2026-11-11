use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, ResultContext};
use serde::Serialize;
use std::sync::Arc;
use types::{QueryResult, ResourceAuthorization};

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
}

pub struct ExtractionOutput {
    pub query_result: QueryResult,
}

pub struct AuthorizationOutput {
    pub query_result: QueryResult,
    pub authorizations: Vec<ResourceAuthorization>,
}

pub struct RedactionOutput {
    pub query_result: QueryResult,
    pub redacted_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugQuery {
    pub sql: String,
    pub rendered: String,
}

pub struct HydrationOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redacted_count: usize,
    pub hydration_queries: Vec<DebugQuery>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryExecutionStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub elapsed_ns: u64,
    pub memory_usage: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryExecution {
    pub label: String,
    pub rendered_sql: String,
    pub query_id: String,
    pub elapsed_ms: f64,
    pub stats: QueryExecutionStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_pipeline: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_log: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryExecutionLog(pub Vec<QueryExecution>);

pub struct PipelineOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub compiled: Arc<CompiledQueryContext>,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
    pub redacted_count: usize,
    pub execution_log: Vec<QueryExecution>,
    /// Pagination metadata; always present (`Option` only spares test fixtures).
    pub pagination: Option<PaginationMeta>,
    /// Nodes and edges a live search produced before the stored query ran. The
    /// formatter splices them into the result graph. Source-agnostic: entity types,
    /// ids, and property values only, carried from the package contract.
    pub seed: Option<GraphSeed>,
}

/// The nodes and edges a live search seeded, in a source-agnostic shape the graph
/// formatter can splice into the result.
#[derive(Debug, Clone, Default)]
pub struct GraphSeed {
    pub nodes: Vec<SeedGraphNode>,
    pub edges: Vec<SeedGraphEdge>,
    /// The indexed anchors of an inbound search. They join the result only when a
    /// seed node survives.
    pub anchors: Vec<SeedGraphNode>,
    /// The hops, as (relationship, target entity), that a seed node must complete
    /// in the stored result to stay in it: a MATCH returns only whole paths.
    pub required_hops: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct SeedGraphNode {
    pub entity_type: String,
    pub id: i64,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct SeedGraphEdge {
    pub from_entity: String,
    pub from_id: i64,
    pub to_entity: String,
    pub to_id: i64,
    pub edge_type: String,
}

pub struct PaginationMeta {
    /// Whether the fetched window overflowed: more matching rows exist in the dataset.
    pub has_more: bool,
    /// Same signal as `has_more`, kept explicit so clients can detect incomplete results.
    pub truncated: bool,
}

/// Trims the overfetched probe row and derives honest pagination metadata.
pub fn paginate(query_result: &mut QueryResult, input: &compiler::Input) -> PaginationMeta {
    let window = input.limit as usize;
    let has_more = query_result.len() > window;
    if has_more {
        query_result.truncate(window);
    }
    PaginationMeta {
        has_more,
        truncated: has_more,
    }
}
