mod input;
mod lower;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::QueryConfig;
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use query_engine::compiler::{ResultContext, codegen};
use tonic::Status;
use tracing::{debug, info};

use crate::proto::{GetGraphStatsResponse, GraphStatsDomain, GraphStatsItem, ProjectsStatus};

use self::input::GraphStatsInput;

pub struct GraphStatsService {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
}

impl GraphStatsService {
    pub fn new(client: Arc<ArrowClickHouseClient>, ontology: Arc<Ontology>) -> Self {
        Self { client, ontology }
    }

    pub async fn get_stats(&self, traversal_path: &str) -> Result<GetGraphStatsResponse, Status> {
        if traversal_path.is_empty() {
            return Err(Status::invalid_argument("traversal_path is required"));
        }

        let input = GraphStatsInput::from_ontology(&self.ontology, traversal_path.to_string());

        let entity_counts_future = async {
            if input.nodes.is_empty() {
                return Ok(HashMap::new());
            }
            let ast = lower::lower_entity_counts(&input);
            self.execute_count_query(&ast, "entity counts").await
        };

        let projects_future = async {
            let ast = lower::lower_projects(&input.project_tables, traversal_path);
            self.execute_projects_query(&ast).await
        };

        let (entity_counts, projects) = tokio::try_join!(entity_counts_future, projects_future)?;

        info!(
            entity_count = entity_counts.len(),
            projects_indexed = projects.indexed,
            projects_total = projects.total_known,
            "Graph stats fetched"
        );

        let domains = present_domain_response(&self.ontology, &entity_counts);
        Ok(GetGraphStatsResponse {
            projects: Some(projects),
            domains,
        })
    }

    async fn execute_count_query(
        &self,
        ast: &query_engine::compiler::Node,
        label: &str,
    ) -> Result<HashMap<String, i64>, Status> {
        let batches = self.execute_query(ast, label).await?;

        let mut counts: HashMap<String, i64> = HashMap::new();
        for batch in &batches {
            let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "entity")
            else {
                continue;
            };
            let Some(values) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if labels.is_null(row) || values.is_null(row) {
                    continue;
                }
                let name = labels.value(row);
                let count = values.value(row) as i64;
                *counts.entry(name.to_string()).or_default() += count;
            }
        }

        Ok(counts)
    }

    async fn execute_projects_query(
        &self,
        ast: &query_engine::compiler::Node,
    ) -> Result<ProjectsStatus, Status> {
        let batches = self.execute_query(ast, "projects").await?;

        let mut indexed = 0i64;
        let mut total_known = 0i64;
        for batch in &batches {
            let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "metric")
            else {
                continue;
            };
            let Some(values) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if labels.is_null(row) || values.is_null(row) {
                    continue;
                }
                match labels.value(row) {
                    "indexed" => indexed += values.value(row) as i64,
                    "total_known" => total_known += values.value(row) as i64,
                    _ => {}
                }
            }
        }

        Ok(ProjectsStatus {
            indexed,
            total_known,
        })
    }

    async fn execute_query(
        &self,
        ast: &query_engine::compiler::Node,
        label: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Status> {
        let parameterized = codegen(ast, ResultContext::new(), QueryConfig::default())
            .map_err(|e| Status::internal(format!("codegen error: {e}")))?;

        debug!(sql = %parameterized.sql, label, "Graph stats query compiled");

        let mut query = self.client.query(&parameterized.sql);
        for (key, param) in &parameterized.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }

        query
            .fetch_arrow()
            .await
            .map_err(|e| Status::internal(format!("ClickHouse error: {e}")))
    }
}

fn present_domain_response(
    ontology: &Ontology,
    entity_counts: &HashMap<String, i64>,
) -> Vec<GraphStatsDomain> {
    ontology
        .domains()
        .map(|domain| {
            let items = domain
                .node_names
                .iter()
                .map(|node_name| GraphStatsItem {
                    name: node_name.clone(),
                    count: entity_counts.get(node_name).copied().unwrap_or(0),
                })
                .collect();

            GraphStatsDomain {
                name: domain.name.clone(),
                items,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_client::ClickHouseConfigurationExt;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    #[test]
    fn presents_domain_response_groups_by_domain() {
        let ontology = test_ontology();
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 42);
        entity_counts.insert("User".to_string(), 10);

        let domains = present_domain_response(&ontology, &entity_counts);

        assert!(!domains.is_empty());

        let core_domain = domains.iter().find(|d| d.name == "core");
        assert!(core_domain.is_some(), "should have core domain");

        let core = core_domain.unwrap();
        let project_item = core.items.iter().find(|i| i.name == "Project");
        assert!(project_item.is_some());
        assert_eq!(project_item.unwrap().count, 42);

        let user_item = core.items.iter().find(|i| i.name == "User");
        assert!(user_item.is_some());
        assert_eq!(user_item.unwrap().count, 10);
    }

    #[test]
    fn presents_domain_response_missing_entity_defaults_to_zero() {
        let ontology = test_ontology();
        let entity_counts = HashMap::new();

        let domains = present_domain_response(&ontology, &entity_counts);

        for domain in &domains {
            for item in &domain.items {
                assert_eq!(
                    item.count, 0,
                    "missing entity {} should default to 0",
                    item.name
                );
            }
        }
    }

    #[test]
    fn presents_domain_response_covers_all_domains() {
        let ontology = test_ontology();
        let entity_counts = HashMap::new();

        let domains = present_domain_response(&ontology, &entity_counts);
        let domain_count = ontology.domains().count();

        assert_eq!(domains.len(), domain_count);
    }

    #[tokio::test]
    async fn empty_traversal_path_rejected() {
        let client = Arc::new(gkg_server_config::ClickHouseConfiguration::default().build_client());
        let service = GraphStatsService::new(client, test_ontology());

        let result = service.get_stats("").await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("traversal_path"));
    }
}
