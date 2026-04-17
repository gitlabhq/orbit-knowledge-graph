use std::collections::HashMap;
use std::sync::Arc;

use indexer::nats::{KvEntry, NatsServices};
use ontology::Ontology;
use serde::de::DeserializeOwned;
use tonic::Status;
use tracing::{debug, error, info};

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
        if !is_valid_traversal_path(traversal_path) {
            return Err(Status::invalid_argument(
                "traversal_path must contain only digits and '/'",
            ));
        }

        // `meta` is keyed by root namespace id (second segment), matching what
        // `ProgressWriter` writes. `counts` is keyed by the full traversal path
        // prefix, so subgroup-level queries share the namespace-level meta.
        let counts_k = counts_key(traversal_path);
        let meta_k = extract_root_namespace_id(traversal_path).map(meta_key);

        debug!(counts = %counts_k, meta = ?meta_k, "reading graph status from KV");

        let (counts_entry, meta_entry) = match meta_k.as_ref() {
            Some(mk) => {
                let (c, m) =
                    tokio::join!(self.read_kv("counts", &counts_k), self.read_kv("meta", mk));
                (c?, m?)
            }
            None => (self.read_kv("counts", &counts_k).await?, None),
        };

        let counts = parse_snapshot::<CountsSnapshot>(counts_entry, "counts")?.unwrap_or_default();
        let (state, initial_backfill_done, sdlc, code) =
            match parse_snapshot::<MetaSnapshot>(meta_entry, "meta")? {
                Some(meta) => into_meta_fields(meta),
                None => (GraphState::Pending as i32, false, None, None),
            };

        let stale = is_stale(&counts.updated_at, self.staleness_threshold_secs);
        let domains = self.build_domains(&counts.nodes);

        info!(
            traversal_path,
            state,
            node_types = domains.iter().map(|d| d.items.len()).sum::<usize>(),
            edge_types = counts.edges.len(),
            stale,
            "graph status response built"
        );

        Ok(GetGraphStatusResponse {
            state,
            initial_backfill_done,
            updated_at: counts.updated_at,
            domains,
            edge_counts: counts.edges,
            sdlc,
            code,
            stale,
        })
    }

    async fn read_kv(&self, kind: &'static str, key: &str) -> Result<Option<KvEntry>, Status> {
        self.nats
            .kv_get(INDEXING_PROGRESS_BUCKET, key)
            .await
            .map_err(|e| {
                error!(kind, error = %e, "NATS KV read failed");
                Status::internal(format!("KV read failed ({kind})"))
            })
    }

    fn build_domains(&self, node_counts: &HashMap<String, i64>) -> Vec<GraphStatusDomain> {
        self.ontology
            .domains()
            .map(|domain| GraphStatusDomain {
                name: domain.name.clone(),
                items: domain
                    .node_names
                    .iter()
                    .filter_map(|name| self.build_item(name, node_counts))
                    .collect(),
            })
            .collect()
    }

    fn build_item(
        &self,
        node_name: &str,
        node_counts: &HashMap<String, i64>,
    ) -> Option<GraphStatusItem> {
        let node = self.ontology.get_node(node_name)?;
        if !node.has_traversal_path {
            return None;
        }
        let count = node_counts.get(node_name).copied().unwrap_or(0);
        Some(GraphStatusItem {
            name: node_name.to_string(),
            status: status_for_count(count),
            count,
        })
    }
}

fn status_for_count(count: i64) -> i32 {
    if count > 0 {
        EntityStatus::Completed as i32
    } else {
        EntityStatus::Pending as i32
    }
}

/// Deserialize an optional KV entry's JSON payload. Absent entry → `Ok(None)`;
/// invalid JSON → `Err(Status::internal)` with `kind` in the log line.
fn parse_snapshot<T: DeserializeOwned>(
    entry: Option<KvEntry>,
    kind: &'static str,
) -> Result<Option<T>, Status> {
    let Some(entry) = entry else { return Ok(None) };
    serde_json::from_slice::<T>(&entry.value)
        .map(Some)
        .map_err(|e| {
            error!(kind, error = %e, "invalid snapshot");
            Status::internal(format!("invalid {kind} snapshot"))
        })
}

fn into_meta_fields(meta: MetaSnapshot) -> (i32, bool, Option<SdlcProgress>, Option<CodeOverview>) {
    let state = match meta.state.as_str() {
        "indexing" => GraphState::Indexing as i32,
        "idle" => GraphState::Idle as i32,
        _ => GraphState::Pending as i32,
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
    });

    (state, meta.initial_backfill_done, sdlc, code)
}

/// Returns the root namespace id from a traversal path like `"1/9970/55154808/"`.
///
/// The root namespace id is the second segment (after the organization id) and
/// is what the indexer writes `meta` snapshots under. Counts snapshots use the
/// full path prefix, so deep lookups read per-subtree counts while sharing the
/// namespace-level meta.
fn extract_root_namespace_id(traversal_path: &str) -> Option<i64> {
    traversal_path
        .trim_end_matches('/')
        .split('/')
        .nth(1)?
        .parse()
        .ok()
}

fn is_valid_traversal_path(traversal_path: &str) -> bool {
    !traversal_path.is_empty()
        && traversal_path
            .chars()
            .all(|c| c.is_ascii_digit() || c == '/')
}

fn is_stale(updated_at: &str, staleness_threshold_secs: u64) -> bool {
    if updated_at.is_empty() {
        return true;
    }
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(updated_at) else {
        return true;
    };
    let age = chrono::Utc::now() - ts.with_timezone(&chrono::Utc);
    let threshold = i64::try_from(staleness_threshold_secs).unwrap_or(i64::MAX);
    age.num_seconds() > threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gkg_server_config::indexing_progress::{CodeMeta, SdlcMeta};
    use indexer::testkit::mocks::MockNatsServices;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn test_service() -> GraphStatusService {
        GraphStatusService::new(Arc::new(MockNatsServices::new()), test_ontology(), 120)
    }

    fn service_with_nats(nats: Arc<MockNatsServices>) -> GraphStatusService {
        GraphStatusService::new(nats, test_ontology(), 120)
    }

    fn seed_counts(mock: &MockNatsServices, tp: &str, snapshot: &CountsSnapshot) {
        mock.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &counts_key(tp),
            Bytes::from(serde_json::to_vec(snapshot).unwrap()),
        );
    }

    fn seed_meta(mock: &MockNatsServices, ns: i64, meta: &MetaSnapshot) {
        mock.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &meta_key(ns),
            Bytes::from(serde_json::to_vec(meta).unwrap()),
        );
    }

    #[test]
    fn extract_root_namespace_id_from_traversal_path() {
        assert_eq!(extract_root_namespace_id("1/9970/"), Some(9970));
        assert_eq!(extract_root_namespace_id("1/9970/55154808/"), Some(9970));
        assert_eq!(extract_root_namespace_id("1/"), None);
        assert_eq!(extract_root_namespace_id(""), None);
    }

    #[test]
    fn is_valid_traversal_path_rejects_non_numeric() {
        assert!(is_valid_traversal_path("1/9970/"));
        assert!(is_valid_traversal_path("1/9970/55154808/"));
        assert!(!is_valid_traversal_path(""));
        assert!(!is_valid_traversal_path("1/abc/"));
        assert!(!is_valid_traversal_path("1.9970"));
        assert!(!is_valid_traversal_path("1/../9970"));
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
    fn is_stale_u64_max_does_not_wrap() {
        let recent = chrono::Utc::now().to_rfc3339();
        assert!(!is_stale(&recent, u64::MAX));
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

    #[tokio::test]
    async fn get_status_rejects_empty_path() {
        let svc = test_service();
        let err = svc.get_status("").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_status_rejects_non_numeric_path() {
        let svc = test_service();
        let err = svc.get_status("1/abc/").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_status_returns_empty_when_kv_missing() {
        let svc = test_service();
        let resp = svc.get_status("1/9970/").await.unwrap();
        assert_eq!(resp.state, GraphState::Pending as i32);
        assert!(!resp.initial_backfill_done);
        assert!(resp.stale);
        assert_eq!(resp.updated_at, "");
        assert!(resp.edge_counts.is_empty());
        for d in &resp.domains {
            for i in &d.items {
                assert_eq!(i.count, 0);
            }
        }
    }

    #[tokio::test]
    async fn get_status_returns_seeded_counts_and_meta() {
        let mock = Arc::new(MockNatsServices::new());

        let mut nodes = HashMap::new();
        nodes.insert("Project".to_string(), 10);
        let mut edges = HashMap::new();
        edges.insert("CONTAINS".to_string(), 25);

        seed_counts(
            &mock,
            "1/9970/",
            &CountsSnapshot {
                updated_at: chrono::Utc::now().to_rfc3339(),
                nodes,
                edges,
            },
        );
        seed_meta(
            &mock,
            9970,
            &MetaSnapshot {
                state: "idle".to_string(),
                initial_backfill_done: true,
                updated_at: chrono::Utc::now().to_rfc3339(),
                sdlc: SdlcMeta {
                    last_completed_at: "2026-04-16T00:00:00Z".to_string(),
                    last_started_at: "2026-04-16T00:00:00Z".to_string(),
                    last_duration_ms: 1234,
                    cycle_count: 42,
                    last_error: String::new(),
                },
                code: CodeMeta::default(),
            },
        );

        let svc = service_with_nats(mock);
        let resp = svc.get_status("1/9970/").await.unwrap();

        assert_eq!(resp.state, GraphState::Idle as i32);
        assert!(resp.initial_backfill_done);
        assert!(!resp.stale);
        assert_eq!(resp.edge_counts.get("CONTAINS"), Some(&25));
        assert_eq!(resp.sdlc.as_ref().unwrap().cycle_count, 42);
    }

    #[tokio::test]
    async fn get_status_malformed_counts_snapshot_returns_internal() {
        let mock = Arc::new(MockNatsServices::new());
        mock.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &counts_key("1/9970/"),
            Bytes::from("not json"),
        );
        let svc = service_with_nats(mock);
        let err = svc.get_status("1/9970/").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            !err.message().contains("expected"),
            "error should not leak serde details: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn get_status_stale_when_updated_at_old() {
        let mock = Arc::new(MockNatsServices::new());
        seed_counts(
            &mock,
            "1/9970/",
            &CountsSnapshot {
                updated_at: "2020-01-01T00:00:00Z".to_string(),
                nodes: HashMap::new(),
                edges: HashMap::new(),
            },
        );
        let svc = service_with_nats(mock);
        let resp = svc.get_status("1/9970/").await.unwrap();
        assert!(resp.stale);
    }
}
