use std::collections::HashMap;
use std::sync::Arc;

use indexer::nats::NatsServices;
use ontology::Ontology;
use tonic::Status;
use tracing::{debug, info};

use crate::proto::{
    CodeOverview, EntityStatus, GetGraphStatusResponse, GraphState, GraphStatusDomain,
    GraphStatusItem, SdlcProgress,
};

use gkg_server_config::indexing_progress::{
    CountsSnapshot, INDEXING_PROGRESS_BUCKET, MetaSnapshot, counts_key, meta_key,
};

pub struct GraphStatusService {
    nats: Arc<dyn NatsServices>,
    ontology: Arc<Ontology>,
    staleness_threshold_secs: u64,
}

impl GraphStatusService {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        ontology: Arc<Ontology>,
        staleness_threshold_secs: u64,
    ) -> Self {
        Self {
            nats,
            ontology,
            staleness_threshold_secs,
        }
    }

    pub async fn get_status(&self, traversal_path: &str) -> Result<GetGraphStatusResponse, Status> {
        if traversal_path.is_empty() {
            return Err(Status::invalid_argument("traversal_path is required"));
        }

        let kv_key = counts_key(traversal_path);
        debug!(key = %kv_key, "reading graph status from KV");

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
            None => (GraphState::Pending as i32, false, None, None),
        };

        let stale = is_stale(&updated_at, self.staleness_threshold_secs);
        let domains = self.build_domains(&node_counts);
        let edge_counts_map: HashMap<String, i64> = edge_counts;

        info!(
            traversal_path,
            state,
            node_types = domains.iter().map(|d| d.items.len()).sum::<usize>(),
            edge_types = edge_counts_map.len(),
            "graph status response built"
        );

        Ok(GetGraphStatusResponse {
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
                    "indexing" => GraphState::Indexing as i32,
                    "idle" => GraphState::Idle as i32,
                    _ => GraphState::Pending as i32,
                };

                let sdlc = Some(SdlcProgress {
                    last_completed_at: meta.sdlc.last_completed_at,
                    last_started_at: meta.sdlc.last_started_at,
                    last_duration_ms: meta.sdlc.last_duration_ms as i64,
                    cycle_count: meta.sdlc.cycle_count as i64,
                    last_error: meta.sdlc.last_error,
                });

                let code = Some(CodeOverview {
                    projects_indexed: meta.code.projects_indexed as i32,
                    projects_total: meta.code.projects_total as i32,
                    last_indexed_at: meta.code.last_indexed_at,
                    projects: vec![],
                });

                Ok((state, meta.initial_backfill_done, sdlc, code))
            }
            None => Ok((GraphState::Pending as i32, false, None, None)),
        }
    }

    fn build_domains(&self, node_counts: &HashMap<String, i64>) -> Vec<GraphStatusDomain> {
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
                        Some(GraphStatusItem {
                            name: node_name.clone(),
                            status,
                            count,
                        })
                    })
                    .collect();

                GraphStatusDomain {
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

fn is_stale(updated_at: &str, staleness_threshold_secs: u64) -> bool {
    if updated_at.is_empty() {
        return true;
    }
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(updated_at) else {
        return true;
    };
    let age = chrono::Utc::now() - ts.with_timezone(&chrono::Utc);
    age.num_seconds() > staleness_threshold_secs as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::testkit::mocks::MockNatsServices;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn test_service() -> GraphStatusService {
        GraphStatusService::new(Arc::new(MockNatsServices::new()), test_ontology(), 120)
    }

    #[test]
    fn extract_namespace_id_from_traversal_path() {
        assert_eq!(extract_namespace_id("1/9970/"), Some(9970));
        assert_eq!(extract_namespace_id("1/9970/55154808/"), Some(9970));
        assert_eq!(extract_namespace_id("1/"), None);
        assert_eq!(extract_namespace_id(""), None);
    }

    #[test]
    fn stale_check() {
        assert!(is_stale("", 120));
        assert!(is_stale("invalid", 120));
        assert!(is_stale("2020-01-01T00:00:00Z", 120));

        let recent = chrono::Utc::now().to_rfc3339();
        assert!(!is_stale(&recent, 120));
    }

    #[test]
    fn build_domains_groups_by_ontology_domain() {
        let service = test_service();
        let mut counts = HashMap::new();
        counts.insert("Project".to_string(), 10);
        counts.insert("MergeRequest".to_string(), 5);

        let domains = service.build_domains(&counts);

        assert!(!domains.is_empty());
        let has_project = domains
            .iter()
            .any(|d| d.items.iter().any(|i| i.name == "Project" && i.count == 10));
        assert!(has_project);
    }

    #[test]
    fn build_domains_missing_entity_defaults_to_zero() {
        let service = test_service();
        let counts = HashMap::new();

        let domains = service.build_domains(&counts);

        for domain in &domains {
            for item in &domain.items {
                assert_eq!(item.count, 0, "{} should default to 0", item.name);
                assert_eq!(
                    item.status,
                    EntityStatus::Pending as i32,
                    "{} should be PENDING when count is 0",
                    item.name
                );
            }
        }
    }

    #[test]
    fn build_domains_covers_all_ontology_domains() {
        let service = test_service();
        let ontology = test_ontology();
        let counts = HashMap::new();

        let domains = service.build_domains(&counts);
        let domain_names: Vec<&str> = domains.iter().map(|d| d.name.as_str()).collect();

        for domain in ontology.domains() {
            assert!(
                domain_names.contains(&domain.name.as_str()),
                "missing domain: {}",
                domain.name
            );
        }
    }

    #[test]
    fn build_domains_excludes_non_traversal_path_entities() {
        let service = test_service();
        let mut counts = HashMap::new();
        counts.insert("User".to_string(), 100);

        let domains = service.build_domains(&counts);

        let has_user = domains
            .iter()
            .any(|d| d.items.iter().any(|i| i.name == "User"));
        assert!(
            !has_user,
            "User has no traversal_path and should be excluded"
        );
    }
}
