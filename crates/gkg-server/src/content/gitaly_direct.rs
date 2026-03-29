use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gitlab_client::GitlabClient;
use gkg_utils::arrow::ColumnValue;
use indexer::modules::code::repository::blob_stream::BlobStream;
use query_engine::pipeline::PipelineError;
use tracing::{debug, warn};

use super::{ColumnResolver, PropertyRow, ResolverContext};

/// Resolves file content by calling the GitLab internal API's `list_blobs`
/// endpoint, which streams blobs from Gitaly via Workhorse.
///
/// Requests are grouped by `project_id` and dispatched concurrently.
/// Uses `<branch>:<path>` revision format so no schema changes are needed.
pub struct GitalyDirectContentResolver {
    client: Arc<GitlabClient>,
}

impl GitalyDirectContentResolver {
    pub fn new(client: Arc<GitlabClient>) -> Self {
        Self { client }
    }
}

/// Extract a required string property from a row, returning `None` if missing.
fn row_string(row: &PropertyRow, key: &str) -> Option<String> {
    row.get(key).and_then(|v| v.as_string().cloned())
}

/// Extract an optional i64 property from a row.
fn row_i64(row: &PropertyRow, key: &str) -> Option<i64> {
    row.get(key).and_then(|v| v.as_int64().copied())
}

/// A single content request extracted from a hydrated property row.
struct ContentRequest {
    row_index: usize,
    project_id: i64,
    revision: String,
    start_byte: Option<i64>,
    end_byte: Option<i64>,
}

#[async_trait]
impl ColumnResolver for GitalyDirectContentResolver {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        if lookup != "blob_content" {
            return Err(PipelineError::ContentResolution(format!(
                "GitalyDirectContentResolver does not support lookup '{lookup}'"
            )));
        }

        let mut results: Vec<Option<ColumnValue>> = vec![None; rows.len()];

        // Extract request parameters from each row.
        let mut requests: Vec<ContentRequest> = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let project_id = match row_i64(row, "project_id") {
                Some(id) => id,
                None => {
                    debug!(row_index = i, "skipping row: missing project_id");
                    continue;
                }
            };
            let branch = match row_string(row, "branch") {
                Some(b) => b,
                None => {
                    debug!(row_index = i, "skipping row: missing branch");
                    continue;
                }
            };
            let path = match row_string(row, "path") {
                Some(p) => p,
                None => {
                    debug!(row_index = i, "skipping row: missing path");
                    continue;
                }
            };

            requests.push(ContentRequest {
                row_index: i,
                project_id,
                revision: format!("{branch}:{path}"),
                start_byte: row_i64(row, "start_byte"),
                end_byte: row_i64(row, "end_byte"),
            });
        }

        if requests.is_empty() {
            return Ok(results);
        }

        // Group by project_id for batched Gitaly calls.
        let mut by_project: HashMap<i64, Vec<&ContentRequest>> = HashMap::new();
        for req in &requests {
            by_project.entry(req.project_id).or_default().push(req);
        }

        // Dispatch concurrent list_blobs calls per project.
        let futures = by_project.iter().map(|(&project_id, reqs)| {
            let client = Arc::clone(&self.client);
            let revisions: Vec<String> = reqs.iter().map(|r| r.revision.clone()).collect();
            async move {
                let stream = client.list_blobs(project_id, &revisions).await;
                (project_id, reqs.len(), stream)
            }
        });

        let responses: Vec<_> = futures::future::join_all(futures).await;

        for (project_id, _req_count, stream_result) in responses {
            let stream = match stream_result {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        project_id,
                        error = %e,
                        "list_blobs failed, content will be missing for this project"
                    );
                    continue;
                }
            };

            // Collect blobs in order from the length-prefixed protobuf stream.
            // Gitaly returns blobs in the same order as the requested revisions.
            let mut blob_stream = BlobStream::new(stream);
            let mut blobs: Vec<Vec<u8>> = Vec::new();

            loop {
                match blob_stream.next_blob().await {
                    Ok(Some(blob)) => blobs.push(blob.data),
                    Ok(None) => break,
                    Err(e) => {
                        warn!(
                            project_id,
                            error = %e,
                            "blob stream decode error, partial results for this project"
                        );
                        break;
                    }
                }
            }

            let project_reqs = &by_project[&project_id];
            for (req, data) in project_reqs.iter().zip(blobs.iter()) {
                let content = slice_content(data, req.start_byte, req.end_byte);
                match String::from_utf8(content) {
                    Ok(s) => results[req.row_index] = Some(ColumnValue::String(s)),
                    Err(_) => {
                        debug!(row_index = req.row_index, "skipping binary blob content");
                    }
                }
            }
        }

        Ok(results)
    }
}

/// Slice content bytes by start/end byte offsets (used for Definition nodes
/// to extract only the relevant portion of a file).
fn slice_content(data: &[u8], start_byte: Option<i64>, end_byte: Option<i64>) -> Vec<u8> {
    let start = start_byte.unwrap_or(0).max(0) as usize;
    let end = end_byte
        .map(|e| (e as usize).min(data.len()))
        .unwrap_or(data.len());

    if start >= data.len() || start >= end {
        return Vec::new();
    }

    data[start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_content_full_file() {
        let data = b"hello world";
        assert_eq!(slice_content(data, None, None), b"hello world");
    }

    #[test]
    fn slice_content_with_range() {
        let data = b"hello world";
        assert_eq!(slice_content(data, Some(6), Some(11)), b"world");
    }

    #[test]
    fn slice_content_start_only() {
        let data = b"hello world";
        assert_eq!(slice_content(data, Some(6), None), b"world");
    }

    #[test]
    fn slice_content_end_only() {
        let data = b"hello world";
        assert_eq!(slice_content(data, None, Some(5)), b"hello");
    }

    #[test]
    fn slice_content_out_of_bounds() {
        let data = b"hello";
        assert_eq!(slice_content(data, Some(100), None), b"");
    }

    #[test]
    fn slice_content_negative_start_clamped() {
        let data = b"hello";
        assert_eq!(slice_content(data, Some(-5), Some(3)), b"hel");
    }
}
