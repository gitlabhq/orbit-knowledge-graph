use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use gitlab_client::{GitlabClient, MergeRequestDiffFileEntry};
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};
use tracing::warn;

use crate::content::metrics;

const MAX_PATHS_PER_REQUEST: usize = 100;
const MAX_RAW_DIFF_BYTES: usize = 5_000_000;

pub struct MergeRequestDiffContentService {
    client: Arc<GitlabClient>,
}

impl MergeRequestDiffContentService {
    pub fn new(client: Arc<GitlabClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ColumnResolver for MergeRequestDiffContentService {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let mut timer = metrics::start_resolve(rows.len());

        let (values, outcome) = match lookup {
            "patch" => self.resolve_per_file_patches(rows).await,
            "raw_patch" => self.resolve_whole_mr_diffs(rows).await,
            "mr_raw_patch" => self.resolve_mr_raw_diffs(rows).await,
            other => {
                return Err(PipelineError::ContentResolution(format!(
                    "mr_diff: unknown lookup '{other}'"
                )));
            }
        };

        timer.set_outcome(outcome);
        Ok(values)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct DiffKey {
    project_id: i64,
    diff_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct MrKey {
    project_id: i64,
    iid: i64,
}

fn mr_key_from_row(props: &PropertyRow) -> Option<MrKey> {
    let project_id = props.get("project_id").and_then(|v| v.coerce::<i64>())?;
    let iid = props.get("iid").and_then(|v| v.coerce::<i64>())?;
    Some(MrKey { project_id, iid })
}

fn diff_key_from_row(props: &PropertyRow) -> Option<DiffKey> {
    let project_id = props.get("project_id").and_then(|v| v.coerce::<i64>())?;
    let diff_id = props.get("id").and_then(|v| v.coerce::<i64>())?;
    Some(DiffKey {
        project_id,
        diff_id,
    })
}

impl MergeRequestDiffContentService {
    async fn resolve_whole_mr_diffs(
        &self,
        rows: &[&PropertyRow],
    ) -> (Vec<Option<ColumnValue>>, &'static str) {
        let row_keys: Vec<Option<DiffKey>> = rows.iter().map(|r| diff_key_from_row(r)).collect();
        let unique_keys: HashSet<DiffKey> = row_keys.iter().flatten().cloned().collect();
        let fetch_count = unique_keys.len();

        let fetches = unique_keys.into_iter().map(|key| {
            let client = Arc::clone(&self.client);
            async move {
                let diff = fetch_raw_diff(&client, &key).await;
                (key, diff)
            }
        });

        let mut error_count = 0;
        let mut diffs_by_key: HashMap<DiffKey, String> = HashMap::new();
        for (key, diff) in futures::future::join_all(fetches).await {
            match diff {
                Some(text) => {
                    diffs_by_key.insert(key, text);
                }
                None => error_count += 1,
            }
        }

        let values = row_keys
            .into_iter()
            .map(|key| {
                let text = diffs_by_key.get(&key?)?;
                into_blob_value(text)
            })
            .collect();

        (values, outcome_label(fetch_count, error_count))
    }

    async fn resolve_mr_raw_diffs(
        &self,
        rows: &[&PropertyRow],
    ) -> (Vec<Option<ColumnValue>>, &'static str) {
        let row_keys: Vec<Option<MrKey>> = rows.iter().map(|r| mr_key_from_row(r)).collect();
        let unique_keys: HashSet<MrKey> = row_keys.iter().flatten().cloned().collect();
        let fetch_count = unique_keys.len();

        let fetches = unique_keys.into_iter().map(|key| {
            let client = Arc::clone(&self.client);
            async move {
                let diff = fetch_mr_raw_diff(&client, &key).await;
                (key, diff)
            }
        });

        let mut error_count = 0;
        let mut diffs_by_key: HashMap<MrKey, String> = HashMap::new();
        for (key, diff) in futures::future::join_all(fetches).await {
            match diff {
                Some(text) => {
                    diffs_by_key.insert(key, text);
                }
                None => error_count += 1,
            }
        }

        let values = row_keys
            .into_iter()
            .map(|key| {
                let text = diffs_by_key.get(&key?)?;
                into_blob_value(text)
            })
            .collect();

        (values, outcome_label(fetch_count, error_count))
    }
}

async fn fetch_mr_raw_diff(client: &GitlabClient, key: &MrKey) -> Option<String> {
    metrics::record_mr_diff_call("mr_raw_diff");

    let mut stream = client
        .get_merge_request_raw_diff_by_iid(key.project_id, key.iid)
        .await
        .map_err(|e| {
            warn!(
                project_id = key.project_id,
                iid = key.iid,
                error = %e,
                "get_merge_request_raw_diff_by_iid failed; diff will be None"
            );
        })
        .ok()?;

    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    project_id = key.project_id,
                    iid = key.iid,
                    error = %e,
                    "mr_raw_diff stream error; diff will be None"
                );
                return None;
            }
        };
        if bytes.len().saturating_add(chunk.len()) > MAX_RAW_DIFF_BYTES {
            warn!(
                project_id = key.project_id,
                iid = key.iid,
                cap = MAX_RAW_DIFF_BYTES,
                "mr_raw_diff exceeds size cap; diff will be None"
            );
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes)
        .map_err(|_| {
            warn!(
                project_id = key.project_id,
                iid = key.iid,
                "mr_raw_diff is not valid UTF-8; diff will be None"
            );
        })
        .ok()
}

async fn fetch_raw_diff(client: &GitlabClient, key: &DiffKey) -> Option<String> {
    metrics::record_mr_diff_call("raw_diff");

    let mut stream = client
        .get_merge_request_raw_diff(key.project_id, key.diff_id)
        .await
        .map_err(|e| {
            warn!(
                project_id = key.project_id,
                diff_id = key.diff_id,
                error = %e,
                "get_merge_request_raw_diff failed; diff will be None"
            );
        })
        .ok()?;

    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    project_id = key.project_id,
                    diff_id = key.diff_id,
                    error = %e,
                    "raw_diff stream error; diff will be None"
                );
                return None;
            }
        };
        if bytes.len().saturating_add(chunk.len()) > MAX_RAW_DIFF_BYTES {
            warn!(
                project_id = key.project_id,
                diff_id = key.diff_id,
                cap = MAX_RAW_DIFF_BYTES,
                "raw_diff exceeds size cap; diff will be None"
            );
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes)
        .map_err(|_| {
            warn!(
                project_id = key.project_id,
                diff_id = key.diff_id,
                "raw_diff is not valid UTF-8; diff will be None"
            );
        })
        .ok()
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct FileKey {
    project_id: i64,
    diff_id: i64,
    path: String,
}

struct DiffFilesRequest {
    project_id: i64,
    diff_id: i64,
    paths: Vec<String>,
}

struct DiffFilesResponse {
    project_id: i64,
    diff_id: i64,
    diffs: Vec<MergeRequestDiffFileEntry>,
}

fn file_key_from_row(props: &PropertyRow) -> Option<FileKey> {
    let too_large: bool = props
        .get("too_large")
        .and_then(|v| v.coerce())
        .unwrap_or(false);
    if too_large {
        return None;
    }

    let project_id = props.get("project_id").and_then(|v| v.coerce::<i64>())?;
    let diff_id = props
        .get("merge_request_diff_id")
        .and_then(|v| v.coerce::<i64>())?;

    let new_path: String = props
        .get("new_path")
        .and_then(|v| v.coerce())
        .unwrap_or_default();
    let old_path: String = props
        .get("old_path")
        .and_then(|v| v.coerce())
        .unwrap_or_default();
    let path = canonical_path(&new_path, &old_path);
    if path.is_empty() {
        return None;
    }

    Some(FileKey {
        project_id,
        diff_id,
        path: path.to_owned(),
    })
}

fn canonical_path<'a>(new_path: &'a str, old_path: &'a str) -> &'a str {
    if !new_path.is_empty() {
        new_path
    } else {
        old_path
    }
}

impl MergeRequestDiffContentService {
    async fn resolve_per_file_patches(
        &self,
        rows: &[&PropertyRow],
    ) -> (Vec<Option<ColumnValue>>, &'static str) {
        let row_keys: Vec<Option<FileKey>> = rows.iter().map(|r| file_key_from_row(r)).collect();
        let requests = build_chunked_requests(&row_keys);
        let request_count = requests.len();

        let fetches = requests.into_iter().map(|request| {
            let client = Arc::clone(&self.client);
            async move { fetch_diff_files(&client, request).await }
        });

        let mut error_count = 0;
        let mut responses: Vec<DiffFilesResponse> = Vec::new();
        for result in futures::future::join_all(fetches).await {
            match result {
                Ok(response) => responses.push(response),
                Err(()) => error_count += 1,
            }
        }

        let entries = index_response_entries(responses);
        let values = row_keys
            .into_iter()
            .map(|key| {
                let entry = entries.get(&key?)?;
                into_blob_value(&entry.diff)
            })
            .collect();

        (values, outcome_label(request_count, error_count))
    }
}

fn build_chunked_requests(row_keys: &[Option<FileKey>]) -> Vec<DiffFilesRequest> {
    let mut paths_by_snapshot: HashMap<(i64, i64), HashSet<String>> = HashMap::new();
    for key in row_keys.iter().flatten() {
        paths_by_snapshot
            .entry((key.project_id, key.diff_id))
            .or_default()
            .insert(key.path.clone());
    }

    paths_by_snapshot
        .into_iter()
        .flat_map(|((project_id, diff_id), paths)| {
            let paths: Vec<String> = paths.into_iter().collect();
            paths
                .chunks(MAX_PATHS_PER_REQUEST)
                .map(|chunk| DiffFilesRequest {
                    project_id,
                    diff_id,
                    paths: chunk.to_vec(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn fetch_diff_files(
    client: &GitlabClient,
    request: DiffFilesRequest,
) -> Result<DiffFilesResponse, ()> {
    metrics::record_mr_diff_call("diff");
    let DiffFilesRequest {
        project_id,
        diff_id,
        paths,
    } = request;

    client
        .list_merge_request_diff_files(project_id, diff_id, &paths)
        .await
        .map(|batch| DiffFilesResponse {
            project_id,
            diff_id,
            diffs: batch.diffs,
        })
        .map_err(|e| {
            warn!(
                project_id,
                diff_id,
                error = %e,
                "list_merge_request_diff_files failed; rows will be None"
            );
        })
}

fn index_response_entries(
    responses: impl IntoIterator<Item = DiffFilesResponse>,
) -> HashMap<FileKey, MergeRequestDiffFileEntry> {
    let mut entries = HashMap::new();
    for response in responses {
        for entry in response.diffs {
            let key = FileKey {
                project_id: response.project_id,
                diff_id: response.diff_id,
                path: canonical_path(&entry.new_path, &entry.old_path).to_owned(),
            };
            entries.insert(key, entry);
        }
    }
    entries
}

fn outcome_label(total_fetches: usize, error_count: usize) -> &'static str {
    match error_count {
        0 => "ok",
        e if e == total_fetches => "failed",
        _ => "partial",
    }
}

fn into_blob_value(text: &str) -> Option<ColumnValue> {
    if text.is_empty() {
        return None;
    }
    metrics::record_blob_bytes(text.len() as u64);
    Some(ColumnValue::String(text.to_owned()))
}
