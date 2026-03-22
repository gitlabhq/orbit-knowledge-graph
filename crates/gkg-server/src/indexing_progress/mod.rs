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
    GetNamespaceIndexingProgressResponse, IndexingProgressDomain, IndexingProgressItem,
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
    WaitingForProjects,
    Indexing,
}

impl fmt::Display for ItemStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::InProgress => f.write_str("in_progress"),
            Self::Completed => f.write_str("completed"),
            Self::WaitingForProjects => f.write_str("waiting_for_projects"),
            Self::Indexing => f.write_str("indexing"),
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
        let domains = snap.domain_progress(&self.ontology, &self.sdlc_plan_names);

        info!(namespace_id, status = %overall_status, "Namespace indexing progress fetched");

        Ok(tonic::Response::new(GetNamespaceIndexingProgressResponse {
            namespace_id,
            status: overall_status.to_string(),
            domains,
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

        let project_plan_completed = sdlc_statuses
            .get("Project")
            .map(|s| s.completed)
            .unwrap_or(false);
        let known_projects = entity_counts.get("Project").copied().unwrap_or(0);
        let total_projects = known_projects.max(indexed_projects);

        Ok(IndexingSnapshot {
            sdlc_statuses,
            entity_counts,
            code: CodeProgress {
                total_projects,
                indexed_projects,
            },
            project_plan_completed,
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
    project_plan_completed: bool,
}

impl IndexingSnapshot {
    fn overall_status(&self, sdlc_plan_names: &HashSet<String>) -> OverallStatus {
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

    fn domain_progress(
        &self,
        ontology: &Ontology,
        sdlc_plan_names: &HashSet<String>,
    ) -> Vec<IndexingProgressDomain> {
        let code_status = self.code.status(self.project_plan_completed);

        ontology
            .domains()
            .map(|domain| {
                let items = domain
                    .node_names
                    .iter()
                    .map(|name| {
                        let status = if domain.name == SOURCE_CODE_DOMAIN {
                            code_status
                        } else if !sdlc_plan_names.contains(name) {
                            ItemStatus::Pending
                        } else {
                            match self.sdlc_statuses.get(name) {
                                None => ItemStatus::Pending,
                                Some(s) if s.completed => ItemStatus::Completed,
                                Some(_) => ItemStatus::InProgress,
                            }
                        };

                        IndexingProgressItem {
                            name: name.clone(),
                            status: status.to_string(),
                            count: self.entity_counts.get(name).copied().unwrap_or(0),
                        }
                    })
                    .collect();

                IndexingProgressDomain {
                    name: domain.name.clone(),
                    items,
                }
            })
            .collect()
    }
}

struct CodeProgress {
    total_projects: i64,
    indexed_projects: i64,
}

impl CodeProgress {
    fn is_complete(&self) -> bool {
        self.total_projects == 0 || self.indexed_projects >= self.total_projects
    }

    fn status(&self, project_plan_completed: bool) -> ItemStatus {
        if !project_plan_completed {
            ItemStatus::WaitingForProjects
        } else if self.is_complete() {
            ItemStatus::Completed
        } else {
            ItemStatus::Indexing
        }
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
        project_plan_completed: bool,
    ) -> IndexingSnapshot {
        IndexingSnapshot {
            sdlc_statuses,
            entity_counts,
            code,
            project_plan_completed,
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
            false,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let core = domains.iter().find(|d| d.name == "core").unwrap();
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
            false,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let core = domains.iter().find(|d| d.name == "core").unwrap();
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
            false,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let core = domains.iter().find(|d| d.name == "core").unwrap();
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
            false,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);

        for domain in &domains {
            if domain.name == SOURCE_CODE_DOMAIN {
                continue;
            }
            for item in &domain.items {
                let is_sdlc_plan = service.sdlc_plan_names.contains(&item.name);
                if !is_sdlc_plan {
                    assert_eq!(item.status, "pending", "{} should be pending", item.name);
                }
            }
        }
    }

    #[test]
    fn code_status_waiting_for_projects_when_sdlc_not_done() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 0,
            },
            false,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let source_code = domains
            .iter()
            .find(|d| d.name == SOURCE_CODE_DOMAIN)
            .unwrap();
        for item in &source_code.items {
            assert_eq!(item.status, "waiting_for_projects");
        }
    }

    #[test]
    fn code_status_indexing_when_partially_done() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 3,
            },
            true,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let source_code = domains
            .iter()
            .find(|d| d.name == SOURCE_CODE_DOMAIN)
            .unwrap();
        for item in &source_code.items {
            assert_eq!(item.status, "indexing");
        }
    }

    #[test]
    fn code_status_completed_when_all_indexed() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 10,
            },
            true,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let source_code = domains
            .iter()
            .find(|d| d.name == SOURCE_CODE_DOMAIN)
            .unwrap();
        for item in &source_code.items {
            assert_eq!(item.status, "completed");
        }
    }

    #[test]
    fn code_status_completed_when_no_projects() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 0,
                indexed_projects: 0,
            },
            true,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);
        let source_code = domains
            .iter()
            .find(|d| d.name == SOURCE_CODE_DOMAIN)
            .unwrap();
        for item in &source_code.items {
            assert_eq!(item.status, "completed");
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
            false,
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
            true,
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
            true,
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
            true,
        );
        assert_eq!(
            snap.overall_status(&service.sdlc_plan_names),
            OverallStatus::ReIndexing
        );
    }

    #[test]
    fn domain_response_groups_items_by_domain() {
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
            true,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);

        assert!(!domains.is_empty());
        let core = domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "completed");
        assert_eq!(project.count, 42);
    }

    #[test]
    fn all_code_entities_share_same_status() {
        let snap = test_snap(
            HashMap::new(),
            HashMap::new(),
            CodeProgress {
                total_projects: 10,
                indexed_projects: 3,
            },
            true,
        );
        let service = test_service();
        let domains = snap.domain_progress(&service.ontology, &service.sdlc_plan_names);

        let source_code = domains
            .iter()
            .find(|d| d.name == SOURCE_CODE_DOMAIN)
            .unwrap();
        for item in &source_code.items {
            assert_eq!(
                item.status, "indexing",
                "all source_code items should be 'indexing', but {} was '{}'",
                item.name, item.status
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
            false,
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
            false,
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
