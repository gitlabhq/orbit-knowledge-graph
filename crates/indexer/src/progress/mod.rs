pub mod queries;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, StringArray, UInt64Array};
use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::QueryConfig;
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use parking_lot::Mutex;
use query_engine::compiler::{ResultContext, codegen};
use tracing::{debug, info};

use crate::handler::HandlerError;
use crate::nats::{KvPutOptions, NatsServices};

use gkg_server_config::indexing_progress::{
    CodeMeta, CountsSnapshot, INDEXING_PROGRESS_BUCKET, MetaSnapshot, SdlcMeta, counts_key,
    meta_key,
};

use self::queries::{build_edge_count_query, build_node_count_query, node_count_targets};

const DEBOUNCE_SECS: u64 = 10;

pub struct ProgressWriter {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
    last_update: Mutex<HashMap<i64, Instant>>,
}

impl ProgressWriter {
    pub fn new(client: Arc<ArrowClickHouseClient>, ontology: Arc<Ontology>) -> Self {
        Self {
            client,
            ontology,
            last_update: Mutex::new(HashMap::new()),
        }
    }

    pub async fn write_progress(
        &self,
        nats: &dyn NatsServices,
        namespace_id: i64,
        traversal_path: &str,
        elapsed: std::time::Duration,
        error: Option<&str>,
    ) -> Result<(), HandlerError> {
        if self.is_debounced(namespace_id) {
            debug!(namespace_id, "skipping progress write (debounced)");
            return Ok(());
        }

        let started_at = Instant::now();

        let (node_counts, edge_counts) = self
            .run_count_queries(traversal_path)
            .await
            .map_err(|e| HandlerError::Processing(format!("count query failed: {e}")))?;

        let rollups = rollup_counts(&node_counts, &edge_counts);

        let now = chrono::Utc::now().to_rfc3339();

        for (tp, (nodes, edges)) in &rollups {
            let snapshot = CountsSnapshot {
                updated_at: now.clone(),
                nodes: nodes.clone(),
                edges: edges.clone(),
            };
            let value = serde_json::to_vec(&snapshot)
                .map_err(|e| HandlerError::Processing(format!("serialize counts: {e}")))?;

            let key = counts_key(tp);
            nats.kv_put(
                INDEXING_PROGRESS_BUCKET,
                &key,
                Bytes::from(value),
                KvPutOptions::default(),
            )
            .await
            .map_err(|e| HandlerError::Processing(format!("KV put {key}: {e}")))?;
        }

        let meta = MetaSnapshot {
            state: "idle".to_string(),
            initial_backfill_done: true,
            updated_at: now,
            sdlc: SdlcMeta {
                last_completed_at: chrono::Utc::now().to_rfc3339(),
                last_started_at: String::new(),
                last_duration_ms: elapsed.as_millis() as u64,
                cycle_count: 0,
                last_error: error.unwrap_or("").to_string(),
            },
            code: CodeMeta::default(),
        };
        let meta_value = serde_json::to_vec(&meta)
            .map_err(|e| HandlerError::Processing(format!("serialize meta: {e}")))?;
        let mk = meta_key(namespace_id);
        nats.kv_put(
            INDEXING_PROGRESS_BUCKET,
            &mk,
            Bytes::from(meta_value),
            KvPutOptions::default(),
        )
        .await
        .map_err(|e| HandlerError::Processing(format!("KV put meta: {e}")))?;

        self.record_update(namespace_id);

        let count_duration = started_at.elapsed();
        info!(
            namespace_id,
            kv_keys = rollups.len(),
            count_ms = count_duration.as_millis() as u64,
            "indexing progress written to KV"
        );

        Ok(())
    }

    async fn run_count_queries(
        &self,
        traversal_path: &str,
    ) -> Result<(Vec<NodeCountRow>, Vec<EdgeCountRow>), String> {
        let targets = node_count_targets(&self.ontology);
        if targets.is_empty() {
            return Ok((vec![], vec![]));
        }

        let node_ast = build_node_count_query(&targets, traversal_path);
        let node_pq = codegen(&node_ast, ResultContext::new(), QueryConfig::default())
            .map_err(|e| format!("node codegen: {e}"))?;

        debug!(sql = %node_pq.sql, "executing node count query");

        let mut node_query = self.client.query(&node_pq.sql);
        for (key, param) in &node_pq.params {
            node_query =
                ArrowClickHouseClient::bind_param(node_query, key, &param.value, &param.ch_type);
        }
        let node_batches = node_query
            .fetch_arrow()
            .await
            .map_err(|e| format!("node query: {e}"))?;

        let mut node_rows = Vec::new();
        for batch in &node_batches {
            let Some(entities) = ArrowUtils::get_column_by_name::<StringArray>(batch, "entity")
            else {
                continue;
            };
            let Some(counts) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            let Some(tps) = ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
            else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if entities.is_null(row) || counts.is_null(row) || tps.is_null(row) {
                    continue;
                }
                node_rows.push(NodeCountRow {
                    entity: entities.value(row).to_string(),
                    count: counts.value(row) as i64,
                    traversal_path: tps.value(row).to_string(),
                });
            }
        }

        let edge_ast = build_edge_count_query(traversal_path);
        let edge_pq = codegen(&edge_ast, ResultContext::new(), QueryConfig::default())
            .map_err(|e| format!("edge codegen: {e}"))?;

        debug!(sql = %edge_pq.sql, "executing edge count query");

        let mut edge_query = self.client.query(&edge_pq.sql);
        for (key, param) in &edge_pq.params {
            edge_query =
                ArrowClickHouseClient::bind_param(edge_query, key, &param.value, &param.ch_type);
        }
        let edge_batches = edge_query
            .fetch_arrow()
            .await
            .map_err(|e| format!("edge query: {e}"))?;

        let mut edge_rows = Vec::new();
        for batch in &edge_batches {
            let Some(tps) = ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
            else {
                continue;
            };
            let Some(rels) =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "relationship_kind")
            else {
                continue;
            };
            let Some(counts) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if tps.is_null(row) || rels.is_null(row) || counts.is_null(row) {
                    continue;
                }
                edge_rows.push(EdgeCountRow {
                    traversal_path: tps.value(row).to_string(),
                    relationship_kind: rels.value(row).to_string(),
                    count: counts.value(row) as i64,
                });
            }
        }

        Ok((node_rows, edge_rows))
    }

    fn is_debounced(&self, namespace_id: i64) -> bool {
        let map = self.last_update.lock();
        match map.get(&namespace_id) {
            Some(last) => last.elapsed().as_secs() < DEBOUNCE_SECS,
            None => false,
        }
    }

    fn record_update(&self, namespace_id: i64) {
        self.last_update.lock().insert(namespace_id, Instant::now());
    }
}

#[derive(Debug)]
struct NodeCountRow {
    entity: String,
    count: i64,
    traversal_path: String,
}

#[derive(Debug)]
struct EdgeCountRow {
    traversal_path: String,
    relationship_kind: String,
    count: i64,
}

type RollupMap = HashMap<String, (HashMap<String, i64>, HashMap<String, i64>)>;

fn rollup_counts(node_rows: &[NodeCountRow], edge_rows: &[EdgeCountRow]) -> RollupMap {
    let mut result: RollupMap = HashMap::new();

    for row in node_rows {
        for prefix in traversal_path_prefixes(&row.traversal_path) {
            let entry = result.entry(prefix).or_default();
            *entry.0.entry(row.entity.clone()).or_insert(0) += row.count;
        }
    }

    for row in edge_rows {
        for prefix in traversal_path_prefixes(&row.traversal_path) {
            let entry = result.entry(prefix).or_default();
            *entry.1.entry(row.relationship_kind.clone()).or_insert(0) += row.count;
        }
    }

    result
}

fn traversal_path_prefixes(tp: &str) -> Vec<String> {
    let parts: Vec<&str> = tp.trim_end_matches('/').split('/').collect();
    let mut prefixes = Vec::with_capacity(parts.len());
    for i in 1..=parts.len() {
        prefixes.push(format!("{}/", parts[..i].join("/")));
    }
    prefixes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traversal_path_prefixes_correct() {
        let prefixes = traversal_path_prefixes("1/9970/55154808/");
        assert_eq!(prefixes, vec!["1/", "1/9970/", "1/9970/55154808/"]);
    }

    #[test]
    fn rollup_aggregates_to_ancestors() {
        let node_rows = vec![
            NodeCountRow {
                entity: "Project".to_string(),
                count: 10,
                traversal_path: "1/2/3/".to_string(),
            },
            NodeCountRow {
                entity: "Project".to_string(),
                count: 5,
                traversal_path: "1/2/4/".to_string(),
            },
        ];
        let edge_rows = vec![EdgeCountRow {
            traversal_path: "1/2/3/".to_string(),
            relationship_kind: "IN_PROJECT".to_string(),
            count: 20,
        }];

        let result = rollup_counts(&node_rows, &edge_rows);

        let root = result.get("1/2/").unwrap();
        assert_eq!(root.0.get("Project"), Some(&15));
        assert_eq!(root.1.get("IN_PROJECT"), Some(&20));

        let child = result.get("1/2/3/").unwrap();
        assert_eq!(child.0.get("Project"), Some(&10));
    }
}
