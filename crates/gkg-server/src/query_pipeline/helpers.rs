use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::{error, warn};

use crate::proto::{
    Error as ProtoError, ExecuteQueryMessage, ExecuteToolMessage, execute_query_message,
    execute_tool_message,
};
use crate::tools::ExecutorError;

use super::error::PipelineError;

pub struct QueryRequest {
    pub query_json: String,
}

pub struct ToolRequest {
    pub tool_name: String,
    pub arguments_json: String,
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

    match first_msg.message {
        Some(execute_query_message::Message::Request(r)) => Some(QueryRequest {
            query_json: r.query_json,
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

pub async fn receive_tool_request(
    stream: &mut Streaming<ExecuteToolMessage>,
    tx: &mpsc::Sender<Result<ExecuteToolMessage, Status>>,
) -> Option<ToolRequest> {
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

    match first_msg.message {
        Some(execute_tool_message::Message::Request(r)) => Some(ToolRequest {
            tool_name: r.tool_name,
            arguments_json: r.arguments_json,
        }),
        _ => {
            warn!("Expected ExecuteToolRequest as first message");
            let _ = tx
                .send(Err(Status::invalid_argument(
                    "Expected ExecuteToolRequest as first message",
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
            message: Some(execute_query_message::Message::Error(ProtoError {
                code: error.code().to_string(),
                message: error.to_string(),
            })),
        }))
        .await;
}

pub async fn send_tool_pipeline_error(
    tx: &mpsc::Sender<Result<ExecuteToolMessage, Status>>,
    error: PipelineError,
) {
    error!(error = %error, "Pipeline error");
    let _ = tx
        .send(Ok(ExecuteToolMessage {
            message: Some(execute_tool_message::Message::Error(ProtoError {
                code: error.code().to_string(),
                message: error.to_string(),
            })),
        }))
        .await;
}

pub async fn send_tool_executor_error(
    tx: &mpsc::Sender<Result<ExecuteToolMessage, Status>>,
    error: ExecutorError,
) {
    error!(error = %error, "Tool execution error");
    let _ = tx
        .send(Ok(ExecuteToolMessage {
            message: Some(execute_tool_message::Message::Error(ProtoError {
                code: error.code(),
                message: error.to_string(),
            })),
        }))
        .await;
}
