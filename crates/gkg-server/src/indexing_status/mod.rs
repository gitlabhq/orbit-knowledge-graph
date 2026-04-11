pub mod types;

use std::collections::HashMap;
use std::sync::Arc;

use indexer::nats::NatsServices;
use ontology::Ontology;
use tonic::Status;
use tracing::{debug, info};

use crate::proto::{
    CodeOverview, EntityStatus, GetIndexingStatusResponse, IndexingState, IndexingStatusDomain,
    IndexingStatusItem, SdlcProgress,
};

use self::types::{CountsSnapshot, INDEXING_PROGRESS_BUCKET, MetaSnapshot, counts_key, meta_key};

const STALENESS_THRESHOLD_SECS: i64 = 120;

pub struct IndexingStatusService {
    nats: Arc<dyn NatsServices>,
    ontology: Arc<Ontology>,
}

impl IndexingStatusService {
    pub fn new(nats: Arc<dyn NatsServices>, ontology: Arc<Ontology>) -> Self {
        Self { nats, ontology }
    }

    pub async fn get_status(
        &self,
        traversal_path: &str,
    ) -> Result<GetIndexingStatusResponse, Status> {
        if traversal_path.is_empty() {
            return Err(Status::invalid_argument("traversal_path is required"));
        }

        let kv_key = counts_key(traversal_path);
        debug!(key = %kv_key, "reading indexing status from KV");

        let counts_entry = self
            .nats
            .kv_get(INDEXING_PROGRESS_BUCKET, &kv_key)
            .await
            .map_err(|e| Status::internal(format!("NATS KV read failed: {e}")))?;

        let (node_counts, edge_counts, updated_at) = match counts_entry {
            Some(entry) => {
                let snapshot: CountsSnapshot = serde_json::from_slice(&entry.value)
                    .map_err(|e| Status::internal(format!("invalid counts snapshot: {e}")))?;
                (snapshot.nodes, snapshot.edges, snapshot.updated_at)
            }
            None => {
                debug!(key = %kv_key, "no counts snapshot found, returning empty");
                (HashMap::new(), HashMap::new(), String::new())
            }
        };

        let namespace_id = extract_namespace_id(traversal_path);
        let (state, initial_backfill_done, sdlc, code) = match namespace_id {
            Some(ns_id) => self.read_meta(ns_id).await?,
            None => (IndexingState::Pending as i32, false, None, None),
        };

        let stale = is_stale(&updated_at);
        let domains = self.build_domains(&node_counts);
        let edge_counts_map: HashMap<String, i64> = edge_counts;

        info!(
            traversal_path,
            state,
            node_types = domains.iter().map(|d| d.items.len()).sum::<usize>(),
            edge_types = edge_counts_map.len(),
            "indexing status response built"
        );

        Ok(GetIndexingStatusResponse {
            state,
            initial_backfill_done,
            updated_at,
            domains,
            edge_counts: edge_counts_map,
            sdlc,
            code,
            stale,
        })
    }

    async fn read_meta(
        &self,
        namespace_id: i64,
    ) -> Result<(i32, bool, Option<SdlcProgress>, Option<CodeOverview>), Status> {
        let key = meta_key(namespace_id);
        let entry = self
            .nats
            .kv_get(INDEXING_PROGRESS_BUCKET, &key)
            .await
            .map_err(|e| Status::internal(format!("NATS KV meta read failed: {e}")))?;

        match entry {
            Some(entry) => {
                let meta: MetaSnapshot = serde_json::from_slice(&entry.value)
                    .map_err(|e| Status::internal(format!("invalid meta snapshot: {e}")))?;

                let state = match meta.state.as_str() {
                    "indexing" => IndexingState::Indexing as i32,
                    "idle" => IndexingState::Idle as i32,
                    _ => IndexingState::Pending as i32,
                };

                let sdlc = Some(SdlcProgress {
                    last_completed_at: meta.sdlc.last_completed_at,
                    last_started_at: meta.sdlc.last_started_at,
                    last_duration_ms: meta.sdlc.last_duration_ms,
                    cycle_count: meta.sdlc.cycle_count,
                    last_error: meta.sdlc.last_error,
                });

                let code = Some(CodeOverview {
                    projects_indexed: meta.code.projects_indexed,
                    projects_total: meta.code.projects_total,
                    last_indexed_at: meta.code.last_indexed_at,
                    projects: vec![],
                });

                Ok((state, meta.initial_backfill_done, sdlc, code))
            }
            None => Ok((IndexingState::Pending as i32, false, None, None)),
        }
    }

    fn build_domains(&self, node_counts: &HashMap<String, i64>) -> Vec<IndexingStatusDomain> {
        self.ontology
            .domains()
            .map(|domain| {
                let items = domain
                    .node_names
                    .iter()
                    .filter_map(|node_name| {
                        let node = self.ontology.get_node(node_name)?;
                        if !node.has_traversal_path {
                            return None;
                        }
                        let count = node_counts.get(node_name).copied().unwrap_or(0);
                        let status = if count > 0 {
                            EntityStatus::Completed as i32
                        } else {
                            EntityStatus::Pending as i32
                        };
                        Some(IndexingStatusItem {
                            name: node_name.clone(),
                            status,
                            count,
                        })
                    })
                    .collect();

                IndexingStatusDomain {
                    name: domain.name.clone(),
                    items,
                }
            })
            .collect()
    }
}

fn extract_namespace_id(traversal_path: &str) -> Option<i64> {
    let parts: Vec<&str> = traversal_path.trim_end_matches('/').split('/').collect();
    if parts.len() >= 2 {
        parts[1].parse().ok()
    } else {
        None
    }
}

fn is_stale(updated_at: &str) -> bool {
    if updated_at.is_empty() {
        return true;
    }
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(updated_at) else {
        return true;
    };
    let age = chrono::Utc::now() - ts.with_timezone(&chrono::Utc);
    age.num_seconds() > STALENESS_THRESHOLD_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_namespace_id_from_traversal_path() {
        assert_eq!(extract_namespace_id("1/9970/"), Some(9970));
        assert_eq!(extract_namespace_id("1/9970/55154808/"), Some(9970));
        assert_eq!(extract_namespace_id("1/"), None);
        assert_eq!(extract_namespace_id(""), None);
    }

    #[test]
    fn stale_check() {
        assert!(is_stale(""));
        assert!(is_stale("invalid"));
        assert!(is_stale("2020-01-01T00:00:00Z"));

        let recent = chrono::Utc::now().to_rfc3339();
        assert!(!is_stale(&recent));
    }

    #[test]
    fn counts_key_format() {
        assert_eq!(types::counts_key("1/9970/"), "counts.1.9970");
        assert_eq!(
            types::counts_key("1/9970/55154808/"),
            "counts.1.9970.55154808"
        );
    }
}
