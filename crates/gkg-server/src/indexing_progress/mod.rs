mod store;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tonic::Status;
use tracing::info;

use crate::graph_stats::GraphStatsService;
use crate::proto::{
    CodeIndexingProgress, CodeItem, GetNamespaceIndexingProgressResponse, SdlcDomain,
    SdlcIndexingProgress, SdlcItem,
};

use self::store::{CheckpointStatus, IndexingProgressReader};

const SOURCE_CODE_DOMAIN: &str = "source_code";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverallStatus {
    Queued,
    Indexing,
    ReIndexing,
    Completed,
}

impl fmt::Display for OverallStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued => f.write_str("queued"),
            Self::Indexing => f.write_str("indexing"),
            Self::ReIndexing => f.write_str("re_indexing"),
            Self::Completed => f.write_str("completed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemStatus {
    Pending,
    InProgress,
    Completed,
}

impl fmt::Display for ItemStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::InProgress => f.write_str("in_progress"),
            Self::Completed => f.write_str("completed"),
        }
    }
}

pub struct IndexingProgressService {
    store: IndexingProgressReader,
    graph_stats: GraphStatsService,
    ontology: Arc<Ontology>,
    sdlc_plan_names: HashSet<String>,
}

impl IndexingProgressService {
    pub fn new(
        graph_client: Arc<ArrowClickHouseClient>,
        datalake_client: Arc<ArrowClickHouseClient>,
        ontology: Arc<Ontology>,
    ) -> Self {
        let store = IndexingProgressReader::new(Arc::clone(&graph_client), datalake_client);
        let graph_stats = GraphStatsService::new(graph_client, Arc::clone(&ontology));
        let sdlc_plan_names = collect_sdlc_plan_names(&ontology);
        Self {
            store,
            graph_stats,
            ontology,
            sdlc_plan_names,
        }
    }

    pub async fn get_progress(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<tonic::Response<GetNamespaceIndexingProgressResponse>, Status> {
        let snap = self.fetch_snapshot(namespace_id, traversal_path).await?;
        let overall_status = snap.overall_status(&self.sdlc_plan_names);
        let sdlc_indexing = snap.sdlc_progress(&self.ontology, &self.sdlc_plan_names);
        let code_indexing = snap.code_progress(&self.ontology);

        info!(namespace_id, status = %overall_status, "Namespace indexing progress fetched");

        Ok(tonic::Response::new(GetNamespaceIndexingProgressResponse {
            namespace_id,
            status: overall_status.to_string(),
            sdlc_indexing: Some(sdlc_indexing),
            code_indexing: Some(code_indexing),
        }))
    }

    async fn fetch_snapshot(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<IndexingSnapshot, Status> {
        let (sdlc_statuses, entity_counts, indexed_projects) = tokio::try_join!(
            self.store.fetch_sdlc_checkpoint_statuses(namespace_id),
            self.graph_stats.fetch_entity_counts(traversal_path),
            self.store.fetch_indexed_projects(traversal_path),
        )?;

        let known_projects = entity_counts.get("Project").copied().unwrap_or(0);
        let total_projects = known_projects.max(indexed_projects);

        Ok(IndexingSnapshot {
            sdlc_statuses,
            entity_counts,
            code: CodeProgress {
                total_projects,
                indexed_projects,
            },
        })
    }

    pub async fn resolve_traversal_path(
        &self,
        namespace_id: i64,
    ) -> Result<Option<String>, Status> {
        self.store.resolve_traversal_path(namespace_id).await
    }
}

struct IndexingSnapshot {
    sdlc_statuses: HashMap<String, CheckpointStatus>,
    entity_counts: HashMap<String, i64>,
    code: CodeProgress,
}

impl IndexingSnapshot {
    fn overall_status(&self, sdlc_plan_names: &HashSet<String>) -> OverallStatus {
        // Neither pipeline has started: no SDLC checkpoints exist and no code projects indexed.
        if self.sdlc_statuses.is_empty() && self.code.indexed_projects == 0 {
            return OverallStatus::Queued;
        }

        let all_sdlc_done = sdlc_plan_names.iter().all(|p| {
            self.sdlc_statuses
                .get(p)
                .map(|s| s.completed)
                .unwrap_or(false)
        });

        if all_sdlc_done && self.code.is_complete() {
            return OverallStatus::Completed;
        }

        let any_prior_completion = self.sdlc_statuses.values().any(|s| s.has_prior_completion);

        if any_prior_completion {
            OverallStatus::ReIndexing
        } else {
            OverallStatus::Indexing
        }
    }

    fn sdlc_progress(
        &self,
        ontology: &Ontology,
        sdlc_plan_names: &HashSet<String>,
    ) -> SdlcIndexingProgress {
        let domains = ontology
            .domains()
            .filter(|domain| domain.name != SOURCE_CODE_DOMAIN)
            .map(|domain| {
                let items = domain
                    .node_names
                    .iter()
                    .map(|name| {
                        let status = if !sdlc_plan_names.contains(name) {
                            ItemStatus::Pending
                        } else {
                            match self.sdlc_statuses.get(name) {
                                None => ItemStatus::Pending,
                                Some(s) if s.completed => ItemStatus::Completed,
                                Some(_) => ItemStatus::InProgress,
                            }
                        };

                        SdlcItem {
                            name: name.clone(),
                            status: status.to_string(),
                            count: self.entity_counts.get(name).copied().unwrap_or(0),
                        }
                    })
                    .collect();

                SdlcDomain {
                    name: domain.name.clone(),
                    items,
                }
            })
            .collect();

        SdlcIndexingProgress { domains }
    }

    fn code_progress(&self, ontology: &Ontology) -> CodeIndexingProgress {
        let items = ontology
            .domains()
            .filter(|domain| domain.name == SOURCE_CODE_DOMAIN)
            .flat_map(|domain| domain.node_names.iter())
            .map(|name| CodeItem {
                name: name.clone(),
                count: self.entity_counts.get(name).copied().unwrap_or(0),
            })
            .collect();

        CodeIndexingProgress {
            indexed_projects: self.code.indexed_projects,
            total_projects: self.code.total_projects,
            items,
        }
    }
}

struct CodeProgress {
    total_projects: i64,
    indexed_projects: i64,
}

impl CodeProgress {
    fn is_complete(&self) -> bool {
        self.total_projects > 0 && self.indexed_projects >= self.total_projects
    }
}

fn collect_sdlc_plan_names(ontology: &Ontology) -> HashSet<String> {
    ontology
        .nodes()
        .filter(|node| node.etl.is_some() && node.domain != SOURCE_CODE_DOMAIN)
        .map(|node| node.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn checkpoint(completed: bool, has_prior_completion: bool) -> CheckpointStatus {
        CheckpointStatus {
            completed,
            has_prior_completion,
        }
    }

    fn test_snap(
        sdlc_statuses: HashMap<String, CheckpointStatus>,
        entity_counts: HashMap<String, i64>,
        code: CodeProgress,
    ) -> IndexingSnapshot {
        IndexingSnapshot {
            sdlc_statuses,
            entity_counts,
            code,
        }
    }

    fn dummy_client() -> Arc<ArrowClickHouseClient> {
        Arc::new(ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            &HashMap::new(),
        ))
    }

    fn test_service() -> IndexingProgressService {
        let ontology = test_ontology();
        let client = dummy_client();
        IndexingProgressService::new(client.clone(), client, ontology)
    }

    #[test]
    fn sdlc_item_pending_when_no_checkpoint() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);
        let core = sdlc.domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "pending");
    }

    #[test]
    fn sdlc_item_in_progress_when_cursor_present() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), checkpoint(false, false));
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);
        let core = sdlc.domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "in_progress");
    }

    #[test]
    fn sdlc_item_completed_when_cursor_empty() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), checkpoint(true, true));
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);
        let core = sdlc.domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "completed");
    }

    #[test]
    fn sdlc_item_pending_when_no_etl_plan() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);

        for domain in &sdlc.domains {
            for item in &domain.items {
                let is_sdlc_plan = service.sdlc_plan_names.contains(&item.name);
                if !is_sdlc_plan {
                    assert_eq!(item.status, "pending", "{} should be pending", item.name);
                }
            }
        }
    }

    #[test]
    fn code_progress_tracks_project_counts() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 3,
            },
        );
        let service = test_service();
        let code = snap.code_progress(&service.ontology);
        assert_eq!(code.indexed_projects, 3);
        assert_eq!(code.total_projects, 10);
    }

    #[test]
    fn code_progress_includes_entity_counts() {
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Definition".to_string(), 100);
        let snap = test_snap(
            HashMap::new(),
            entity_counts,
            CodeProgress {
                total_projects: 5,
                indexed_projects: 2,
            },
        );
        let service = test_service();
        let code = snap.code_progress(&service.ontology);
        let definition = code.items.iter().find(|i| i.name == "Definition");
        assert!(definition.is_some());
        assert_eq!(definition.unwrap().count, 100);
    }

    #[test]
    fn code_progress_zero_counts_when_no_data() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let code = snap.code_progress(&service.ontology);
        assert_eq!(code.indexed_projects, 0);
        assert_eq!(code.total_projects, 0);
        for item in &code.items {
            assert_eq!(item.count, 0, "{} should have zero count", item.name);
        }
    }

    #[test]
    fn overall_status_queued_when_no_checkpoints() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::Queued
        );
    }

    #[test]
    fn overall_status_indexing_when_partial_progress() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), checkpoint(true, false));
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::Indexing
        );
    }

    #[test]
    fn overall_status_completed_when_all_done() {
        let service = test_service();
        let mut statuses = HashMap::new();
        for name in &service.sdlc_plan_names {
            statuses.insert(name.clone(), checkpoint(true, true));
        }
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 5,
                indexed_projects: 5,
            },
        );
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::Completed
        );
    }

    #[test]
    fn overall_status_re_indexing_when_code_not_done_but_prior_completion() {
        let service = test_service();
        let mut statuses = HashMap::new();
        for name in &service.sdlc_plan_names {
            statuses.insert(name.clone(), checkpoint(true, true));
        }
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 3,
            },
        );
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::ReIndexing
        );
    }

    #[test]
    fn sdlc_progress_groups_items_by_domain() {
        let mut sdlc_statuses = HashMap::new();
        sdlc_statuses.insert("Project".to_string(), checkpoint(true, true));
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 42);
        let snap = test_snap(
            sdlc_statuses,
            entity_counts,
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);

        assert!(!sdlc.domains.is_empty());
        let core = sdlc.domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "completed");
        assert_eq!(project.count, 42);
    }

    #[test]
    fn sdlc_progress_excludes_source_code_domain() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        let sdlc = snap.sdlc_progress(&service.ontology, &service.sdlc_plan_names);
        assert!(
            sdlc.domains.iter().all(|d| d.name != SOURCE_CODE_DOMAIN),
            "sdlc_progress should not include source_code domain"
        );
    }

    #[test]
    fn code_progress_only_includes_source_code_entities() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 5,
                indexed_projects: 2,
            },
        );
        let service = test_service();
        let ontology = test_ontology();
        let code = snap.code_progress(&service.ontology);

        let source_code_nodes: HashSet<String> = ontology
            .domains()
            .filter(|d| d.name == SOURCE_CODE_DOMAIN)
            .flat_map(|d| d.node_names.iter().cloned())
            .collect();

        for item in &code.items {
            assert!(
                source_code_nodes.contains(&item.name),
                "{} should be a source_code entity",
                item.name
            );
        }
    }

    #[test]
    fn overall_status_re_indexing_when_prior_completion_exists() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), checkpoint(false, true));
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::ReIndexing
        );
    }

    #[test]
    fn overall_status_indexing_when_no_prior_completion() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), checkpoint(false, false));
        let snap = test_snap(
            statuses,
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
        );
        let service = test_service();
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::Indexing
        );
    }

    #[test]
    fn sdlc_plans_exclude_source_code_nodes() {
        let ontology = test_ontology();
        let plans = collect_sdlc_plan_names(&ontology);

        for plan in &plans {
            let node = ontology.nodes().find(|n| &n.name == plan).unwrap();
            assert_ne!(
                node.domain, SOURCE_CODE_DOMAIN,
                "{plan} should not be in source_code domain"
            );
        }
    }
}
