use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::{error, warn};

use crate::proto::{ExecuteQueryError, ExecuteQueryMessage, execute_query_message};

use query_engine::pipeline::PipelineError;

pub struct QueryRequest {
    pub query: String,
    pub format: i32,
    pub query_type: i32,
}

pub async fn receive_query_request(
    stream: &mut Streaming<ExecuteQueryMessage>,
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
) -> Option<QueryRequest> {
    let first_msg = match stream.next().await {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => {
            error!(error = %e, "Failed to receive initial message");
            let _ = tx.send(Err(e)).await;
            return None;
        }
        None => {
            warn!("Empty stream received");
            let _ = tx.send(Err(Status::invalid_argument("Empty stream"))).await;
            return None;
        }
    };

    match first_msg.content {
        Some(execute_query_message::Content::Request(r)) => Some(QueryRequest {
            query: r.query,
            format: r.format,
            query_type: r.query_type,
        }),
        _ => {
            warn!("Expected ExecuteQueryRequest as first message");
            let _ = tx
                .send(Err(Status::invalid_argument(
                    "Expected ExecuteQueryRequest as first message",
                )))
                .await;
            None
        }
    }
}

pub async fn send_query_error(
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    error: PipelineError,
) {
    error!(error = %error, "Pipeline error");
    let _ = tx
        .send(Ok(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Error(ExecuteQueryError {
                code: error.code().to_string(),
                message: sanitize_error_message(&error),
            })),
        }))
        .await;
}

/// Sanitize error messages before sending to clients.
///
/// Only user-input validation errors are returned verbatim (parse errors,
/// schema violations, reference errors, pagination errors, depth/limit
/// exceeded). These are identified by the `Display` prefix that
/// `QueryError` adds via thiserror.
///
/// All other errors (lowering, enforcement, codegen, ontology, execution,
/// authorization, etc.) may contain ClickHouse table names, column names,
/// SQL fragments, or infrastructure details. These are replaced with a
/// generic message; server-side logs capture the full error.
fn sanitize_error_message(error: &PipelineError) -> String {
    match error {
        PipelineError::Compile {
            message,
            client_safe: true,
        } => message.clone(),
        PipelineError::Compile { .. } => "Query compilation failed.".to_string(),
        PipelineError::Security(_) => "Security context error.".to_string(),
        PipelineError::Execution(msg) => classify_execution_error(msg),
        PipelineError::Authorization(_) => "Authorization failed.".to_string(),
        PipelineError::ContentResolution(_) => {
            "An internal error occurred during content resolution.".to_string()
        }
        PipelineError::Streaming(_) => "An internal error occurred during streaming.".to_string(),
        PipelineError::Custom(_) => "An internal error occurred.".to_string(),
    }
}

/// Classify ClickHouse execution errors into actionable diagnostic messages.
///
/// Parses `Code: NNN` from the error string. No internal details (table
/// names, SQL, infrastructure) are exposed — only the failure class and
/// generic suggestions for refining the query.
fn classify_execution_error(msg: &str) -> String {
    let code = extract_ch_error_code(msg);
    match code {
        Some(241) => {
            // MEMORY_LIMIT_EXCEEDED
            "Query used too much memory. This usually means the query is \
             scanning too much data. Try: add a project_id filter, use \
             node_ids to pin specific entities, or reduce max_hops/max_depth."
                .to_string()
        }
        Some(159) | Some(160) => {
            // TIMEOUT_EXCEEDED / TOO_SLOW
            "Query timed out. The query is likely scanning a large portion \
             of the graph. Try: add selective filters (project_id, state), \
             reduce max_hops/max_depth, specify rel_types, or use node_ids \
             to pin high-cardinality entities like Definition or File."
                .to_string()
        }
        Some(307) => {
            // TOO_MANY_BYTES
            "Query read too much data. Try: add a project_id filter to \
             scope the scan, use node_ids for selective endpoints, or \
             narrow filters on high-cardinality entities."
                .to_string()
        }
        Some(158) => {
            // TOO_MANY_ROWS
            "Query scanned too many rows. Filters like name or path on \
             entities like Definition or File may not be selective enough \
             without project_id scoping. Try: add project_id, use node_ids, \
             or pre-resolve broad filters with a separate lookup query."
                .to_string()
        }
        Some(191) => {
            // SET_SIZE_LIMIT_EXCEEDED
            "Query matched too many IDs in a filter subquery. The filter \
             is not selective enough. Try: add more specific filters, use \
             node_ids for direct ID selection, or scope by project_id."
                .to_string()
        }
        Some(53) => {
            // TYPE_MISMATCH
            "Query has a type mismatch in a filter or aggregation. Check \
             that filter values match the column type (e.g. use integers \
             for ID fields, strings for text fields, DateTime format for \
             date columns)."
                .to_string()
        }
        _ => "Query execution failed.".to_string(),
    }
}

/// Extract ClickHouse error code from an error string.
/// Matches patterns like "Code: 241." or "Code: 241,".
fn extract_ch_error_code(error: &str) -> Option<u32> {
    let start = error.find("Code: ")?;
    let after = &error[start + 6..];
    let end = after.find(|c: char| !c.is_ascii_digit())?;
    after[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_ch_error_code_parses_standard_format() {
        let msg = "query error: bad response: Code: 241. DB::Exception: Memory limit exceeded";
        assert_eq!(extract_ch_error_code(msg), Some(241));
    }

    #[test]
    fn extract_ch_error_code_returns_none_for_unknown() {
        assert_eq!(extract_ch_error_code("some other error"), None);
    }

    #[test]
    fn classify_memory() {
        let msg = "query error: bad response: Code: 241. DB::Exception: Memory limit";
        assert!(classify_execution_error(msg).contains("too much memory"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_timeout() {
        let msg = "Code: 159. DB::Exception: Timeout exceeded";
        assert!(classify_execution_error(msg).contains("timed out"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_too_many_bytes() {
        let msg = "Code: 307. DB::Exception: Too many bytes to read";
        assert!(classify_execution_error(msg).contains("too much data"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_too_many_rows() {
        let msg = "Code: 158. DB::Exception: Too many rows";
        assert!(classify_execution_error(msg).contains("too many rows"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_set_size() {
        let msg = "Code: 191. DB::Exception: Set size limit exceeded";
        assert!(classify_execution_error(msg).contains("too many IDs"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_type_mismatch() {
        let msg = "Code: 53. DB::Exception: Cannot convert String to DateTime64";
        assert!(classify_execution_error(msg).contains("type mismatch"), "got: {}", classify_execution_error(msg));
    }

    #[test]
    fn classify_unknown_falls_back() {
        let msg = "Code: 999. DB::Exception: Something unexpected";
        assert_eq!(classify_execution_error(msg), "Query execution failed.");
    }

    #[test]
    fn classify_no_code_falls_back() {
        assert_eq!(classify_execution_error("connection refused"), "Query execution failed.");
    }
}
