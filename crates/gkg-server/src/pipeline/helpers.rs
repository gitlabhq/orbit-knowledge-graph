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
/// Compilation and validation errors are safe to return (they describe
/// user input problems). Execution and internal errors may contain
/// ClickHouse table names, SQL fragments, or infrastructure details —
/// replace with a generic message and let server-side logs capture the
/// full error.
fn sanitize_error_message(error: &PipelineError) -> String {
    match error {
        PipelineError::Compile(msg) | PipelineError::Security(msg) => msg.clone(),
        PipelineError::Execution(_) => {
            "Query execution failed. Please retry or contact support.".to_string()
        }
        PipelineError::Authorization(_) => "Authorization failed.".to_string(),
        PipelineError::ContentResolution(_) => {
            "An internal error occurred during content resolution.".to_string()
        }
        PipelineError::Streaming(_) => "An internal error occurred during streaming.".to_string(),
        PipelineError::Custom(_) => "An internal error occurred.".to_string(),
    }
}
