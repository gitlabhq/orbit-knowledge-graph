use futures::StreamExt;
use orbit_log_fields::{error, info, warn};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::proto::execute_query_message::Content;
use crate::proto::execute_query_result::Content::{FormattedText, ResultJson};
use crate::proto::{
    ExecuteQueryError, ExecuteQueryMessage, ExecuteQueryRequest, ExecuteQueryResult, ResultChunk,
    execute_query_message,
};

use orbit_server_config::GrpcConfig;
use query_engine::compiler::RejectionReason;
use query_engine::pipeline::{ClickHouseLimit, PipelineError};

pub(super) async fn send_query_result(
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    messages: Vec<Content>,
) -> Result<(), PipelineError> {
    info!("Sending final query result");
    for (index, content) in messages.into_iter().enumerate() {
        let message = ExecuteQueryMessage {
            content: Some(content),
        };
        if tx.send(Ok(message)).await.is_err() {
            if index == 0 {
                return Err(PipelineError::client_closed());
            }
            info!("Client left during the result stream");
            break;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct ResultChunking {
    pub chunk_bytes: usize,
    pub max_bytes: usize,
}

impl From<&GrpcConfig> for ResultChunking {
    fn from(grpc: &GrpcConfig) -> Self {
        Self {
            chunk_bytes: grpc.result_chunk_bytes,
            max_bytes: grpc.max_result_bytes,
        }
    }
}

pub fn result_messages(
    mut result: ExecuteQueryResult,
    chunking: Option<ResultChunking>,
) -> Result<Vec<Content>, PipelineError> {
    let Some(chunking) = chunking else {
        return Ok(vec![Content::Result(result)]);
    };
    let text = match result.content.take() {
        Some(ResultJson(text) | FormattedText(text)) => text,
        None => String::new(),
    };
    if text.len() > chunking.max_bytes {
        return Err(PipelineError::Compile {
            message: format!(
                "The query result is {} bytes, over the {}-byte limit. \
                 Return fewer rows or columns, or leave out large fields such as content.",
                text.len(),
                chunking.max_bytes
            ),
            client_safe: true,
            reason: RejectionReason::Limit,
        });
    }
    Ok(text_chunks(&text, chunking.chunk_bytes)
        .map(|data| {
            Content::ResultChunk(ResultChunk {
                data: data.to_owned(),
            })
        })
        .chain([Content::Result(result)])
        .collect())
}

fn text_chunks(text: &str, max_bytes: usize) -> impl Iterator<Item = &str> {
    let mut rest = text;
    std::iter::from_fn(move || {
        let (chunk, tail) = rest.split_at(rest.ceil_char_boundary(max_bytes));
        rest = tail;
        (!chunk.is_empty()).then_some(chunk)
    })
}

pub async fn send_invalid_request_error(
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    message: String,
) {
    warn!(
        safe(ERROR_TYPE) = "invalid_request",
        expanded(ERROR_MESSAGE) = %message,
        "Rejecting invalid query request"
    );
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
            error!(
                safe(ERROR_TYPE) = labkit::grpc::grpc_code_name(e.code().into()),
                expanded(ERROR_MESSAGE) = %e,
                "Failed to receive initial message"
            );
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
    let reason = error.failure_reason().as_str();
    error!(
        safe(ERROR_TYPE) = error.code(),
        safe(REASON) = reason,
        safe(CLIENT_SAFE) = client_safe,
        expanded(ERROR_MESSAGE) = %error,
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
        PipelineError::Security(_) | PipelineError::NoEnabledNamespaces => {
            "Security context error.".to_string()
        }
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
    match ClickHouseLimit::from_message(msg) {
        Some(ClickHouseLimit::MemoryLimit) => {
            "Query used too much memory. This usually means the query is \
             scanning too much data. Try: add a project_id filter, use \
             node_ids to pin specific entities, or reduce hops/max_depth."
        }
        Some(ClickHouseLimit::TooSlow) => {
            "Query timed out. The query is likely scanning a large portion \
             of the graph. Try: add selective filters (project_id, state), \
             reduce hops/max_depth, specify rel_types, or use node_ids \
             to pin high-cardinality entities like Definition or File."
        }
        Some(ClickHouseLimit::TooManyBytes) => {
            "Query read too much data. Try: add a project_id filter to \
             scope the scan, use node_ids for selective endpoints, or \
             narrow filters on high-cardinality entities."
        }
        Some(ClickHouseLimit::TooManyRows) => {
            "Query scanned too many rows. Filters like name or path on \
             entities like Definition or File may not be selective enough \
             without project_id scoping. Try: add project_id, use node_ids, \
             or pre-resolve broad filters with a separate lookup query."
        }
        Some(ClickHouseLimit::SetSizeLimit) => {
            "Query matched too many IDs in a filter subquery. The filter \
             is not selective enough. Try: add more specific filters, use \
             node_ids for direct ID selection, or scope by project_id."
        }
        Some(ClickHouseLimit::TypeMismatch | ClickHouseLimit::BadQueryParameter) => {
            "Query has a type mismatch in a filter or aggregation. Check \
             that filter values match the column type (e.g. use integers \
             for ID fields, strings for text fields, DateTime format for \
             date columns)."
        }
        _ => "Query execution failed.",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::QueryMetadata;

    #[derive(Clone, Default)]
    struct Buffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn query_error_line_names_the_clickhouse_limit() {
        // A second dispatcher stops tracing-core from caching callsite interest
        // from a test thread that has no subscriber.
        let _second = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let (tx, _rx) = mpsc::channel(1);

        let error = PipelineError::Execution(
            "Code: 241. DB::Exception: Memory limit (for query) exceeded".to_owned(),
        );
        send_query_error(&tx, error).await;

        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("error_type=\"execution_error\""), "{logs}");
        assert!(logs.contains("reason_code=\"memory_limit\""), "{logs}");
    }

    const CHUNK_BYTES: usize = 64;
    const CHUNKING: ResultChunking = ResultChunking {
        chunk_bytes: CHUNK_BYTES,
        max_bytes: 1024 * 1024,
    };

    fn chunked(text: &str) -> Result<(Vec<String>, ExecuteQueryResult), PipelineError> {
        let result = ExecuteQueryResult {
            content: Some(FormattedText(text.to_owned())),
            metadata: Some(QueryMetadata {
                row_count: 7,
                ..Default::default()
            }),
        };
        let mut messages = result_messages(result, Some(CHUNKING))?;
        let Some(Content::Result(last)) = messages.pop() else {
            panic!("expected a final result message");
        };
        let chunks = messages
            .into_iter()
            .map(|content| match content {
                Content::ResultChunk(chunk) => chunk.data,
                other => panic!("expected a result chunk, got {other:?}"),
            })
            .collect();
        Ok((chunks, last))
    }

    #[test]
    fn result_messages_split_text_on_char_boundaries_and_keep_metadata() -> Result<(), PipelineError>
    {
        let text = "aé€😀".repeat(CHUNK_BYTES / 4);
        let (chunks, last) = chunked(&text)?;

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.concat(), text);
        assert!(chunks.iter().all(|c| c.len() <= CHUNK_BYTES + 3));
        assert_eq!(last.content, None);
        assert_eq!(last.metadata.map(|m| m.row_count), Some(7));
        assert!(chunked("")?.0.is_empty());
        Ok(())
    }

    #[test]
    fn result_messages_reject_text_over_the_limit_before_any_chunk() {
        let result = ExecuteQueryResult {
            content: Some(ResultJson("x".repeat(CHUNKING.max_bytes + 1))),
            metadata: None,
        };
        let Err(error) = result_messages(result, Some(CHUNKING)) else {
            panic!("expected the size error");
        };
        let at_limit = ExecuteQueryResult {
            content: Some(ResultJson("x".repeat(CHUNKING.max_bytes))),
            metadata: None,
        };
        assert!(result_messages(at_limit, Some(CHUNKING)).is_ok());

        assert_eq!(error.code(), "compile_error");
        assert!(error.is_caller_error(), "{error}");
        assert!(sanitize_error_message(&error).contains("over the 1048576-byte limit"));
    }

    #[tokio::test]
    async fn send_query_result_fails_only_when_the_client_left_before_the_first_message()
    -> Result<(), PipelineError> {
        let messages = || result_messages(ExecuteQueryResult::default(), None);
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert!(send_query_result(&tx, messages()?).await.is_err());

        let (tx, mut rx) = mpsc::channel(1);
        let mut two = messages()?;
        two.extend(messages()?);
        let (sent, first) = tokio::join!(send_query_result(&tx, two), async {
            let first = rx.recv().await;
            rx.close();
            first
        });
        assert!(sent.is_ok());
        assert!(first.is_some());
        assert!(rx.recv().await.is_none());
        Ok(())
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
