use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::{error, warn};

use crate::proto::{
    ExecuteQueryError, ExecuteQueryMessage, ExecuteQueryRequest, execute_query_message,
};

use query_engine::pipeline::PipelineError;

use crate::pipeline::metrics::failure_reason;

pub async fn send_invalid_request_error(
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    message: String,
) {
    warn!(error = %message, "Rejecting invalid query request");
    let _ = tx
        .send(Ok(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Error(ExecuteQueryError {
                code: "invalid_request".to_string(),
                message,
            })),
        }))
        .await;
}

pub async fn receive_query_request(
    stream: &mut Streaming<ExecuteQueryMessage>,
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
) -> Option<ExecuteQueryRequest> {
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
        Some(execute_query_message::Content::Request(r)) => Some(r),
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
    let client_safe = matches!(
        error,
        PipelineError::Compile {
            client_safe: true,
            ..
        }
    );
    // failure_reason() returns None for Compile (counted on the compiler
    // metric), so fall back to err.code() ("compile_error") for log readers.
    let reason = failure_reason(&error).unwrap_or_else(|| error.code());
    error!(
        code = error.code(),
        failure_reason = reason,
        client_safe,
        error = %error,
        "Pipeline error",
    );
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
            ..
        } => message.clone(),
        PipelineError::Compile { .. } => "Query compilation failed.".to_string(),
        PipelineError::Security(_) => "Security context error.".to_string(),
        PipelineError::Execution(msg) => classify_execution_error(msg),
        PipelineError::Authorization(_) => "Authorization failed.".to_string(),
        PipelineError::ContentResolution(_) => {
            "An internal error occurred during content resolution.".to_string()
        }
        PipelineError::Streaming(_) => "An internal error occurred during streaming.".to_string(),
        PipelineError::Timeout => "Query exceeded the configured stream timeout.".to_string(),
        PipelineError::Custom(_) => "An internal error occurred.".to_string(),
    }
}

/// No internal details (table names, SQL, infrastructure) are exposed — only
/// the failure class and generic suggestions for refining the query.
fn classify_execution_error(msg: &str) -> String {
    match clickhouse_limit(msg) {
        Some("memory_limit") => {
            "Query used too much memory. This usually means the query is \
             scanning too much data. Try: add a project_id filter, use \
             node_ids to pin specific entities, or reduce hops/max_depth."
        }
        Some("too_slow") => {
            "Query timed out. The query is likely scanning a large portion \
             of the graph. Try: add selective filters (project_id, state), \
             reduce hops/max_depth, specify rel_types, or use node_ids \
             to pin high-cardinality entities like Definition or File."
        }
        Some("too_many_bytes") => {
            "Query read too much data. Try: add a project_id filter to \
             scope the scan, use node_ids for selective endpoints, or \
             narrow filters on high-cardinality entities."
        }
        Some("too_many_rows") => {
            "Query scanned too many rows. Filters like name or path on \
             entities like Definition or File may not be selective enough \
             without project_id scoping. Try: add project_id, use node_ids, \
             or pre-resolve broad filters with a separate lookup query."
        }
        Some("set_size_limit") => {
            "Query matched too many IDs in a filter subquery. The filter \
             is not selective enough. Try: add more specific filters, use \
             node_ids for direct ID selection, or scope by project_id."
        }
        Some("type_mismatch") => {
            "Query has a type mismatch in a filter or aggregation. Check \
             that filter values match the column type (e.g. use integers \
             for ID fields, strings for text fields, DateTime format for \
             date columns)."
        }
        _ => "Query execution failed.",
    }
    .to_string()
}

pub(crate) fn clickhouse_limit(msg: &str) -> Option<&'static str> {
    match extract_ch_error_code(msg)? {
        241 => Some("memory_limit"),
        159 | 160 => Some("too_slow"),
        307 => Some("too_many_bytes"),
        158 => Some("too_many_rows"),
        191 => Some("set_size_limit"),
        53 => Some("type_mismatch"),
        _ => None,
    }
}

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
        assert!(
            classify_execution_error(msg).contains("too much memory"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_timeout() {
        let msg = "Code: 159. DB::Exception: Timeout exceeded";
        assert!(
            classify_execution_error(msg).contains("timed out"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_too_many_bytes() {
        let msg = "Code: 307. DB::Exception: Too many bytes to read";
        assert!(
            classify_execution_error(msg).contains("too much data"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_too_many_rows() {
        let msg = "Code: 158. DB::Exception: Too many rows";
        assert!(
            classify_execution_error(msg).contains("too many rows"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_set_size() {
        let msg = "Code: 191. DB::Exception: Set size limit exceeded";
        assert!(
            classify_execution_error(msg).contains("too many IDs"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_type_mismatch() {
        let msg = "Code: 53. DB::Exception: Cannot convert String to DateTime64";
        assert!(
            classify_execution_error(msg).contains("type mismatch"),
            "got: {}",
            classify_execution_error(msg)
        );
    }

    #[test]
    fn classify_unknown_falls_back() {
        let msg = "Code: 999. DB::Exception: Something unexpected";
        assert_eq!(classify_execution_error(msg), "Query execution failed.");
    }

    #[test]
    fn classify_no_code_falls_back() {
        assert_eq!(
            classify_execution_error("connection refused"),
            "Query execution failed."
        );
    }
}
