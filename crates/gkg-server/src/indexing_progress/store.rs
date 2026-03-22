use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use tonic::Status;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStatus {
    pub completed: bool,
    pub has_prior_completion: bool,
}

pub struct IndexingProgressReader {
    graph_client: Arc<ArrowClickHouseClient>,
    datalake_client: Arc<ArrowClickHouseClient>,
}

impl IndexingProgressReader {
    pub fn new(
        graph_client: Arc<ArrowClickHouseClient>,
        datalake_client: Arc<ArrowClickHouseClient>,
    ) -> Self {
        Self {
            graph_client,
            datalake_client,
        }
    }

    pub async fn resolve_traversal_path(
        &self,
        namespace_id: i64,
    ) -> Result<Option<String>, Status> {
        let batches = self
            .datalake_client
            .query(
                "SELECT argMax(traversal_path, version) AS traversal_path \
                 FROM namespace_traversal_paths \
                 WHERE id = {namespace_id:Int64} \
                 GROUP BY id \
                 HAVING NOT argMax(deleted, version)",
            )
            .param("namespace_id", namespace_id)
            .fetch_arrow()
            .await
            .map_err(|e| Status::internal(format!("ClickHouse error: {e}")))?;

        for batch in &batches {
            let Some(paths) =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
            else {
                continue;
            };
            if batch.num_rows() > 0 && !paths.is_null(0) {
                let path = paths.value(0);
                if !path.is_empty() {
                    return Ok(Some(path.to_string()));
                }
            }
        }

        Ok(None)
    }

    pub async fn fetch_sdlc_checkpoint_statuses(
        &self,
        namespace_id: i64,
    ) -> Result<HashMap<String, CheckpointStatus>, Status> {
        let prefix = format!("ns.{namespace_id}.");

        let batches = self
            .graph_client
            .query(
                "SELECT key, \
                        argMax(cursor_values, _version) AS cursor_values, \
                        argMax(watermark, _version) AS watermark \
                 FROM checkpoint \
                 WHERE startsWith(key, {prefix:String}) \
                 GROUP BY key \
                 HAVING NOT argMax(_deleted, _version)",
            )
            .param("prefix", prefix.as_str())
            .fetch_arrow()
            .await
            .map_err(|e| Status::internal(format!("ClickHouse error: {e}")))?;

        let mut statuses = HashMap::new();

        for batch in &batches {
            let (Some(keys), Some(cursors)) = (
                ArrowUtils::get_column_by_name::<StringArray>(batch, "key"),
                ArrowUtils::get_column_by_name::<StringArray>(batch, "cursor_values"),
            ) else {
                continue;
            };
            let watermarks =
                ArrowUtils::get_column_by_name::<TimestampMicrosecondArray>(batch, "watermark");

            for row in 0..batch.num_rows() {
                if keys.is_null(row) {
                    continue;
                }

                let plan_name = extract_plan_name(keys.value(row), &prefix);
                let status = CheckpointStatus {
                    completed: cursor_is_empty(cursors, row),
                    has_prior_completion: watermark_is_past_epoch(watermarks, row),
                };

                if let Some(existing) = statuses.insert(plan_name.to_string(), status) {
                    tracing::warn!(
                        plan_name,
                        "duplicate checkpoint key, previous value: {existing:?}"
                    );
                }
            }
        }

        Ok(statuses)
    }

    pub async fn fetch_indexed_projects(&self, traversal_path: &str) -> Result<i64, Status> {
        let batches = self
            .graph_client
            .query(
                "SELECT count(DISTINCT project_id) AS cnt FROM ( \
                     SELECT project_id \
                     FROM code_indexing_checkpoint \
                     WHERE startsWith(traversal_path, {traversal_path:String}) \
                     GROUP BY traversal_path, project_id, branch \
                     HAVING NOT argMax(_deleted, _version) \
                 )",
            )
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| Status::internal(format!("ClickHouse error: {e}")))?;

        Ok(extract_count(&batches))
    }
}

fn extract_plan_name<'a>(key: &'a str, prefix: &str) -> &'a str {
    key.strip_prefix(prefix).unwrap_or(key)
}

fn cursor_is_empty(cursors: &StringArray, row: usize) -> bool {
    cursors.is_null(row) || cursors.value(row).is_empty() || cursors.value(row) == "null"
}

fn watermark_is_past_epoch(watermarks: Option<&TimestampMicrosecondArray>, row: usize) -> bool {
    watermarks
        .map(|w| !w.is_null(row) && w.value(row) != 0)
        .unwrap_or(false)
}

fn extract_count(batches: &[RecordBatch]) -> i64 {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(counts) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt")
            && !counts.is_null(0)
        {
            return i64::try_from(counts.value(0)).unwrap_or(i64::MAX);
        }
    }
    0
}
