mod store;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tonic::Status;
use tracing::info;

use crate::graph_stats::GraphStatsService;
use crate::proto::{
    GetNamespaceIndexingProgressResponse, IndexingProgressDomain, IndexingProgressItem,
};

use self::store::IndexingProgressStore;

const SOURCE_CODE_DOMAIN: &str = "source_code";

pub struct IndexingProgressService {
    store: IndexingProgressStore,
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
        let store = IndexingProgressStore::new(Arc::clone(&graph_client), datalake_client);
        let graph_stats = GraphStatsService::new(graph_client, Arc::clone(&ontology));
        let sdlc_plan_names = collect_sdlc_plan_names(&ontology);
        Self {
            store,
            graph_stats,
            ontology,
            sdlc_plan_names,
        }
    }

    pub async fn resolve_traversal_path(&self, namespace_id: i64) -> Result<String, Status> {
        self.store.resolve_traversal_path(namespace_id).await
    }

    pub async fn get_progress(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<tonic::Response<GetNamespaceIndexingProgressResponse>, Status> {
        let (sdlc_statuses, entity_counts, indexed_projects) = tokio::try_join!(
            self.store.fetch_sdlc_checkpoint_statuses(namespace_id),
            self.graph_stats.fetch_entity_counts(traversal_path),
            self.store.fetch_indexed_projects(traversal_path),
        )?;

        let project_plan_completed = sdlc_statuses.get("Project").copied().unwrap_or(false);
        let known_projects = entity_counts.get("Project").copied().unwrap_or(0);
        let total_projects = known_projects.max(indexed_projects);
        let code_counts = store::CodeIndexingCounts::new(total_projects, indexed_projects);

        let domains = build_domain_response(
            &self.ontology,
            &sdlc_statuses,
            &self.sdlc_plan_names,
            &entity_counts,
            project_plan_completed,
            &code_counts,
        );

        let overall_status =
            derive_overall_status(&sdlc_statuses, &self.sdlc_plan_names, &code_counts);

        info!(
            namespace_id,
            status = %overall_status,
            "Namespace indexing progress fetched"
        );

        Ok(tonic::Response::new(GetNamespaceIndexingProgressResponse {
            namespace_id,
            status: overall_status,
            domains,
        }))
    }
}

fn collect_sdlc_plan_names(ontology: &Ontology) -> HashSet<String> {
    ontology
        .nodes()
        .filter(|node| node.etl.is_some() && node.domain != SOURCE_CODE_DOMAIN)
        .map(|node| node.name.clone())
        .collect()
}

fn derive_sdlc_item_status(
    plan_name: &str,
    sdlc_statuses: &HashMap<String, bool>,
    sdlc_plans_with_etl: &HashSet<String>,
) -> &'static str {
    if !sdlc_plans_with_etl.contains(plan_name) {
        return "pending";
    }

    match sdlc_statuses.get(plan_name) {
        None => "pending",
        Some(true) => "completed",
        Some(false) => "in_progress",
    }
}

fn derive_code_item_status(
    project_plan_completed: bool,
    code_counts: &store::CodeIndexingCounts,
) -> &'static str {
    if !project_plan_completed {
        return "waiting_for_projects";
    }

    if code_counts.total_projects == 0 {
        return "completed";
    }

    if code_counts.indexed_projects >= code_counts.total_projects {
        "completed"
    } else {
        "indexing"
    }
}

fn build_domain_response(
    ontology: &Ontology,
    sdlc_statuses: &HashMap<String, bool>,
    sdlc_plans_with_etl: &HashSet<String>,
    entity_counts: &HashMap<String, i64>,
    project_plan_completed: bool,
    code_counts: &store::CodeIndexingCounts,
) -> Vec<IndexingProgressDomain> {
    let code_status = derive_code_item_status(project_plan_completed, code_counts);

    ontology
        .domains()
        .map(|domain| {
            let items = domain
                .node_names
                .iter()
                .map(|node_name| {
                    let status = if domain.name == SOURCE_CODE_DOMAIN {
                        code_status
                    } else {
                        derive_sdlc_item_status(node_name, sdlc_statuses, sdlc_plans_with_etl)
                    };
                    let count = entity_counts.get(node_name).copied().unwrap_or(0);

                    IndexingProgressItem {
                        name: node_name.clone(),
                        status: status.to_string(),
                        count,
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

fn derive_overall_status(
    sdlc_statuses: &HashMap<String, bool>,
    sdlc_plans_with_etl: &HashSet<String>,
    code_counts: &store::CodeIndexingCounts,
) -> String {
    let has_any_checkpoint = !sdlc_statuses.is_empty();

    if !has_any_checkpoint && code_counts.indexed_projects == 0 {
        return "queued".to_string();
    }

    let all_sdlc_completed = sdlc_plans_with_etl
        .iter()
        .all(|plan| sdlc_statuses.get(plan).copied().unwrap_or(false));

    let code_completed = code_counts.total_projects == 0
        || code_counts.indexed_projects >= code_counts.total_projects;

    if all_sdlc_completed && code_completed {
        "completed".to_string()
    } else {
        "indexing".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    #[test]
    fn sdlc_item_status_pending_when_no_checkpoint() {
        let statuses = HashMap::new();
        let plans = HashSet::from(["Project".to_string()]);

        assert_eq!(
            derive_sdlc_item_status("Project", &statuses, &plans),
            "pending"
        );
    }

    #[test]
    fn sdlc_item_status_in_progress_when_cursor_present() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), false);
        let plans = HashSet::from(["Project".to_string()]);

        assert_eq!(
            derive_sdlc_item_status("Project", &statuses, &plans),
            "in_progress"
        );
    }

    #[test]
    fn sdlc_item_status_completed_when_cursor_empty() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), true);
        let plans = HashSet::from(["Project".to_string()]);

        assert_eq!(
            derive_sdlc_item_status("Project", &statuses, &plans),
            "completed"
        );
    }

    #[test]
    fn sdlc_item_status_pending_when_no_etl_plan() {
        let statuses = HashMap::new();
        let plans = HashSet::new();

        assert_eq!(
            derive_sdlc_item_status("Project", &statuses, &plans),
            "pending"
        );
    }

    #[test]
    fn code_status_waiting_for_projects_when_sdlc_not_done() {
        let counts = store::CodeIndexingCounts {
            total_projects: 10,
            indexed_projects: 0,
        };
        assert_eq!(
            derive_code_item_status(false, &counts),
            "waiting_for_projects"
        );
    }

    #[test]
    fn code_status_indexing_when_partially_done() {
        let counts = store::CodeIndexingCounts {
            total_projects: 10,
            indexed_projects: 3,
        };
        assert_eq!(derive_code_item_status(true, &counts), "indexing");
    }

    #[test]
    fn code_status_completed_when_all_indexed() {
        let counts = store::CodeIndexingCounts {
            total_projects: 10,
            indexed_projects: 10,
        };
        assert_eq!(derive_code_item_status(true, &counts), "completed");
    }

    #[test]
    fn code_status_completed_when_no_projects() {
        let counts = store::CodeIndexingCounts {
            total_projects: 0,
            indexed_projects: 0,
        };
        assert_eq!(derive_code_item_status(true, &counts), "completed");
    }

    #[test]
    fn overall_status_queued_when_no_checkpoints() {
        let statuses = HashMap::new();
        let plans = HashSet::from(["Project".to_string()]);
        let code_counts = store::CodeIndexingCounts {
            total_projects: 0,
            indexed_projects: 0,
        };

        assert_eq!(
            derive_overall_status(&statuses, &plans, &code_counts),
            "queued"
        );
    }

    #[test]
    fn overall_status_indexing_when_partial_progress() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), true);
        let plans = HashSet::from(["Project".to_string(), "Group".to_string()]);
        let code_counts = store::CodeIndexingCounts {
            total_projects: 0,
            indexed_projects: 0,
        };

        assert_eq!(
            derive_overall_status(&statuses, &plans, &code_counts),
            "indexing"
        );
    }

    #[test]
    fn overall_status_completed_when_all_done() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), true);
        statuses.insert("Group".to_string(), true);
        let plans = HashSet::from(["Project".to_string(), "Group".to_string()]);
        let code_counts = store::CodeIndexingCounts {
            total_projects: 5,
            indexed_projects: 5,
        };

        assert_eq!(
            derive_overall_status(&statuses, &plans, &code_counts),
            "completed"
        );
    }

    #[test]
    fn overall_status_indexing_when_code_not_done() {
        let mut statuses = HashMap::new();
        statuses.insert("Project".to_string(), true);
        let plans = HashSet::from(["Project".to_string()]);
        let code_counts = store::CodeIndexingCounts {
            total_projects: 10,
            indexed_projects: 3,
        };

        assert_eq!(
            derive_overall_status(&statuses, &plans, &code_counts),
            "indexing"
        );
    }

    #[test]
    fn domain_response_groups_items_by_domain() {
        let ontology = test_ontology();
        let mut sdlc_statuses = HashMap::new();
        sdlc_statuses.insert("Project".to_string(), true);
        let sdlc_plans = collect_sdlc_plan_names(&ontology);
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 42);
        let code_counts = store::CodeIndexingCounts {
            total_projects: 0,
            indexed_projects: 0,
        };

        let domains = build_domain_response(
            &ontology,
            &sdlc_statuses,
            &sdlc_plans,
            &entity_counts,
            true,
            &code_counts,
        );

        assert!(!domains.is_empty());
        let core = domains.iter().find(|d| d.name == "core").unwrap();
        let project = core.items.iter().find(|i| i.name == "Project").unwrap();
        assert_eq!(project.status, "completed");
        assert_eq!(project.count, 42);
    }

    #[test]
    fn all_code_entities_share_same_status() {
        let ontology = test_ontology();
        let sdlc_statuses = HashMap::new();
        let sdlc_plans = collect_sdlc_plan_names(&ontology);
        let entity_counts = HashMap::new();
        let code_counts = store::CodeIndexingCounts {
            total_projects: 10,
            indexed_projects: 3,
        };

        let domains = build_domain_response(
            &ontology,
            &sdlc_statuses,
            &sdlc_plans,
            &entity_counts,
            true,
            &code_counts,
        );

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
