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

/// Classify ClickHouse execution errors into actionable messages.
///
/// Parses `Code: NNN` from the error string and returns a user-facing
/// message with hints for how to refine the query. No internal details
/// (table names, SQL, infrastructure) are exposed — only the limit type
/// and generic suggestions.
fn classify_execution_error(msg: &str) -> String {
    let code = extract_ch_error_code(msg);
    match code {
        Some(241) => {
            // MEMORY_LIMIT_EXCEEDED
            "Query exceeded memory limit. \
             Hints: add filters to narrow the scan (e.g. project_id, state), \
             reduce limit, or use node_ids instead of broad filters."
                .to_string()
        }
        Some(159) | Some(160) => {
            // TIMEOUT_EXCEEDED / TOO_SLOW
            "Query exceeded time limit. \
             Hints: add filters to reduce the data scanned, \
             reduce max_hops/max_depth, or specify rel_types."
                .to_string()
        }
        Some(307) => {
            // TOO_MANY_BYTES (max_bytes_to_read)
            "Query exceeded data read limit. \
             Hints: add filters to narrow the scan (e.g. project_id), \
             use node_ids for selective endpoints, or reduce limit."
                .to_string()
        }
        Some(158) => {
            // TOO_MANY_ROWS (max_rows_to_read)
            "Query exceeded row read limit. \
             Hints: add filters to narrow the scan, \
             use node_ids instead of broad filters."
                .to_string()
        }
        Some(191) => {
            // SET_SIZE_LIMIT_EXCEEDED (max_rows_in_set)
            "Query exceeded IN-subquery size limit. \
             Hints: add more specific filters to reduce the number of \
             matching IDs, or use node_ids for direct ID selection."
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
    fn classify_memory_limit() {
        let msg = "query error: bad response: Code: 241. DB::Exception: Memory limit";
        assert!(classify_execution_error(msg).contains("memory limit"));
    }

    #[test]
    fn classify_timeout() {
        let msg = "Code: 159. DB::Exception: Timeout exceeded";
        assert!(classify_execution_error(msg).contains("time limit"));
    }

    #[test]
    fn classify_bytes_to_read() {
        let msg = "Code: 307. DB::Exception: Too many bytes to read";
        assert!(classify_execution_error(msg).contains("data read limit"));
    }

    #[test]
    fn classify_rows_in_set() {
        let msg = "Code: 191. DB::Exception: Set size limit exceeded";
        assert!(classify_execution_error(msg).contains("IN-subquery size limit"));
    }

    #[test]
    fn classify_unknown_falls_back() {
        let msg = "Code: 999. DB::Exception: Something unexpected";
        assert_eq!(classify_execution_error(msg), "Query execution failed.");
    }
}
