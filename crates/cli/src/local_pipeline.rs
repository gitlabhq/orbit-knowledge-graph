//! Local DuckDB query pipeline.
//!
//! ```text
//! Compile → Execute → Extract → Output
//! ```
//!
//! No security, authorization, redaction, or hydration.
//! Everything is synchronous -- DuckDB doesn't need async.

use std::sync::Arc;

use anyhow::Result;
use ontology::Ontology;

use duckdb_client::DuckDbClient;
use query_engine::compiler::compile_local;
use query_engine::shared::{PaginationMeta, PipelineOutput};
use query_engine::types::QueryResult;

/// Execute a query against the local DuckDB graph.
#[allow(dead_code)]
pub fn run(query_json: &str, ontology: &Ontology, client: &DuckDbClient) -> Result<PipelineOutput> {
    let compiled = compile_local(query_json, ontology)
        .map_err(|e| anyhow::anyhow!("compilation error: {e}"))?;
    let compiled = Arc::new(compiled);

    let rendered_sql = compiled.base.render();
    let result_context = compiled.base.result_context.clone();

    let batches = client
        .query_arrow(&rendered_sql)
        .map_err(|e| anyhow::anyhow!("execution error: {e}"))?;

    let mut query_result = QueryResult::from_batches(&batches, &result_context);

    let pagination = compiled.input.cursor.map(|cursor| {
        let total_rows = query_result.authorized_count();
        let has_more = query_result.apply_cursor(cursor.offset, cursor.page_size);
        PaginationMeta {
            has_more,
            total_rows,
        }
    });

    Ok(PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: 0,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![],
        compiled,
        result_context,
        query_result,
        execution_log: vec![],
        pagination,
    })
}
