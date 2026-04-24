use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use gitlab_client::{GitlabClient, MergeRequestDiffFileEntry};
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};
use tracing::warn;

use super::metrics;

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

        let (results, had_errors) = match lookup {
            "patch" => self.resolve_per_file_diffs(rows).await?,
            "raw_patch" => self.resolve_raw_patches(rows).await?,
            other => {
                return Err(PipelineError::ContentResolution(format!(
                    "mr_diff: unknown lookup '{other}'"
                )));
            }
        };

        timer.set_outcome(if had_errors { "error" } else { "mr_diff" });
        Ok(results)
    }
}

// ── lookup: patch (per-file) ────────────────────────────────────────────
//
// Group rows by (project_id, diff_id) → one HTTP call per MR snapshot.
// Match Rails response entries back to rows by (new_path, old_path).

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct FileRowKey {
    project_id: i64,
    diff_id: i64,
    new_path: String,
    old_path: String,
    too_large: bool,
}

impl MergeRequestDiffContentService {
    fn file_row_key(props: &PropertyRow) -> Option<FileRowKey> {
        Some(FileRowKey {
            project_id: props.get("project_id")?.coerce()?,
            diff_id: props.get("merge_request_diff_id")?.coerce()?,
            new_path: props
                .get("new_path")
                .and_then(|v| v.coerce())
                .unwrap_or_default(),
            old_path: props
                .get("old_path")
                .and_then(|v| v.coerce())
                .unwrap_or_default(),
            too_large: props
                .get("too_large")
                .and_then(|v| v.coerce())
                .unwrap_or(false),
        })
    }

    async fn resolve_per_file_diffs(
        &self,
        rows: &[&PropertyRow],
    ) -> Result<(Vec<Option<ColumnValue>>, bool), PipelineError> {
        let keys: Vec<Option<FileRowKey>> = rows.iter().map(|r| Self::file_row_key(r)).collect();

        let mut groups: HashMap<(i64, i64), Vec<String>> = HashMap::new();
        for key in keys.iter().flatten() {
            if key.too_large {
                continue;
            }
            let g = groups.entry((key.project_id, key.diff_id)).or_default();
            let path = if !key.new_path.is_empty() {
                &key.new_path
            } else {
                &key.old_path
            };
            if !g.contains(path) {
                g.push(path.clone());
            }
        }

        let group_futs = groups.into_iter().map(|((project_id, diff_id), paths)| {
            let client = Arc::clone(&self.client);
            async move {
                metrics::record_mr_diff_call();
                match client
                    .list_merge_request_diff_files(project_id, diff_id, &paths)
                    .await
                {
                    Ok(batch) => (Some(((project_id, diff_id), batch)), false),
                    Err(e) => {
                        warn!(
                            project_id,
                            diff_id,
                            error = %e,
                            "list_merge_request_diff_files failed; diff will be None"
                        );
                        (None, true)
                    }
                }
            }
        });

        let mut had_errors = false;
        let mut lookup_map: HashMap<(i64, i64, String, String), MergeRequestDiffFileEntry> =
            HashMap::new();
        for (result, errored) in futures::future::join_all(group_futs).await {
            had_errors |= errored;
            if let Some(((project_id, diff_id), batch)) = result {
                for entry in batch.diffs {
                    lookup_map.insert(
                        (
                            project_id,
                            diff_id,
                            entry.new_path.clone(),
                            entry.old_path.clone(),
                        ),
                        entry,
                    );
                }
            }
        }

        let values = keys
            .into_iter()
            .map(|key| {
                let key = key?;
                if key.too_large {
                    return None;
                }
                let entry =
                    lookup_map.get(&(key.project_id, key.diff_id, key.new_path, key.old_path))?;
                if entry.diff.is_empty() {
                    return None;
                }
                metrics::record_blob_bytes(entry.diff.len() as u64);
                Some(ColumnValue::String(entry.diff.clone()))
            })
            .collect();

        Ok((values, had_errors))
    }
}

// ── lookup: raw_patch (whole-MR) ────────────────────────────────────────
//
// One HTTP call per unique (project_id, diff_id). Rows sharing the same
// MR snapshot reuse a single fetch.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct PatchRowKey {
    project_id: i64,
    diff_id: i64,
}

impl MergeRequestDiffContentService {
    fn patch_row_key(props: &PropertyRow) -> Option<PatchRowKey> {
        Some(PatchRowKey {
            project_id: props.get("project_id")?.coerce()?,
            diff_id: props.get("id")?.coerce()?,
        })
    }

    async fn resolve_raw_patches(
        &self,
        rows: &[&PropertyRow],
    ) -> Result<(Vec<Option<ColumnValue>>, bool), PipelineError> {
        let keys: Vec<Option<PatchRowKey>> = rows.iter().map(|r| Self::patch_row_key(r)).collect();

        let unique: Vec<PatchRowKey> = keys
            .iter()
            .flatten()
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let fetch_futs = unique.into_iter().map(|k| {
            let client = Arc::clone(&self.client);
            async move {
                metrics::record_mr_diff_call();
                let mut stream = match client
                    .get_merge_request_raw_patch(k.project_id, k.diff_id)
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            project_id = k.project_id,
                            diff_id = k.diff_id,
                            error = %e,
                            "get_merge_request_raw_patch failed; patch will be None"
                        );
                        return (None, true);
                    }
                };

                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(b) => bytes.extend_from_slice(&b),
                        Err(e) => {
                            warn!(
                                project_id = k.project_id,
                                diff_id = k.diff_id,
                                error = %e,
                                "raw_patch stream error; patch will be None"
                            );
                            return (None, true);
                        }
                    }
                }

                match String::from_utf8(bytes) {
                    Ok(text) => (Some((k, text)), false),
                    Err(_) => {
                        warn!(
                            project_id = k.project_id,
                            diff_id = k.diff_id,
                            "raw_patch is not valid UTF-8; patch will be None"
                        );
                        (None, true)
                    }
                }
            }
        });

        let mut had_errors = false;
        let mut cache: HashMap<PatchRowKey, String> = HashMap::new();
        for (result, errored) in futures::future::join_all(fetch_futs).await {
            had_errors |= errored;
            if let Some((k, text)) = result {
                cache.insert(k, text);
            }
        }

        let values = keys
            .into_iter()
            .map(|key| {
                let key = key?;
                let text = cache.get(&key)?;
                if text.is_empty() {
                    return None;
                }
                metrics::record_blob_bytes(text.len() as u64);
                Some(ColumnValue::String(text.clone()))
            })
            .collect();

        Ok((values, had_errors))
    }
}
