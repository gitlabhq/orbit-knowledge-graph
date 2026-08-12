use std::collections::HashMap;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use tonic::Status;
use tracing::warn;

use super::{execute_query, status_with_state, unknown_status};
use crate::proto::{IndexingState, IndexingStatus, ProjectsStatus};

const PROJECT_NODE: &str = "Project";
const CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

pub struct CodeIndexingState {
    pub projects: ProjectsStatus,
    pub aggregate: Option<IndexingStatus>,
    pub node_states: HashMap<String, IndexingState>,
}

pub async fn get_code_indexing_state(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    traversal_path: &str,
) -> CodeIndexingState {
    let projects = match fetch_project_coverage(client, ontology, traversal_path).await {
        Ok(projects) => projects,
        Err(error) => {
            warn!(traversal_path, %error, "Graph status branch failed");
            return CodeIndexingState {
                projects: ProjectsStatus::default(),
                aggregate: Some(unknown_status()),
                node_states: HashMap::new(),
            };
        }
    };

    let state = derive_state(&projects);

    CodeIndexingState {
        projects,
        aggregate: state.map(status_with_state),
        node_states: resolve_node_states(ontology, state),
    }
}

fn derive_state(projects: &ProjectsStatus) -> Option<IndexingState> {
    if projects.total_known == 0 {
        return None;
    }
    Some(if projects.indexed == 0 {
        IndexingState::NotIndexed
    } else if projects.indexed < projects.total_known {
        IndexingState::Backfilling
    } else {
        IndexingState::Indexed
    })
}

pub(super) fn resolve_node_states(
    ontology: &Ontology,
    state: Option<IndexingState>,
) -> HashMap<String, IndexingState> {
    let Some(state) = state else {
        return HashMap::new();
    };
    ontology
        .nodes()
        .filter(|node| node.pipelines.is_empty())
        .map(|node| (node.name.clone(), state))
        .collect()
}

async fn fetch_project_coverage(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    traversal_path: &str,
) -> Result<ProjectsStatus, Status> {
    let tables = project_tables(ontology)?;
    let sql = projects_sql(&tables.project, &tables.code_checkpoint);
    let batches = execute_query(client, &sql, traversal_path, "projects").await?;

    let mut projects = ProjectsStatus::default();
    for batch in &batches {
        let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "metric") else {
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
                "indexed" => projects.indexed += values.value(row) as i64,
                "total_known" => projects.total_known += values.value(row) as i64,
                _ => {}
            }
        }
    }

    Ok(projects)
}

struct ProjectTables {
    project: String,
    code_checkpoint: String,
}

fn project_tables(ontology: &Ontology) -> Result<ProjectTables, Status> {
    let project = ontology
        .get_node(PROJECT_NODE)
        .ok_or_else(|| Status::internal(format!("ontology missing required node: {PROJECT_NODE}")))?
        .destination_table
        .clone();

    let code_checkpoint = ontology
        .auxiliary_tables()
        .iter()
        .find(|t| t.name.ends_with(CHECKPOINT_TABLE_SUFFIX))
        .ok_or_else(|| {
            Status::internal(format!(
                "ontology missing auxiliary table ending with: {CHECKPOINT_TABLE_SUFFIX}"
            ))
        })?
        .name
        .clone();

    Ok(ProjectTables {
        project,
        code_checkpoint,
    })
}

fn projects_sql(project_table: &str, code_checkpoint_table: &str) -> String {
    format!(
        "SELECT 'total_known' AS metric, uniqExact(p.id) AS cnt \
           FROM {project_table} AS p FINAL \
          WHERE p._deleted = 0 AND startsWith(p.traversal_path, {{path:String}}) \
         UNION ALL \
         SELECT 'indexed' AS metric, uniqExact(c.project_id) AS cnt \
           FROM {code_checkpoint_table} AS c FINAL \
           INNER JOIN {project_table} AS p FINAL ON c.project_id = p.id \
          WHERE p._deleted = 0 AND startsWith(p.traversal_path, {{path:String}}) \
            AND c._deleted = 0 AND startsWith(c.traversal_path, {{path:String}})"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn projects(indexed: i64, total_known: i64) -> ProjectsStatus {
        ProjectsStatus {
            indexed,
            total_known,
        }
    }

    #[test]
    fn code_state_omitted_when_no_known_projects() {
        assert_eq!(derive_state(&projects(0, 0)), None);
    }

    #[test]
    fn code_state_not_indexed_when_nothing_indexed() {
        assert_eq!(
            derive_state(&projects(0, 4)),
            Some(IndexingState::NotIndexed)
        );
    }

    #[test]
    fn code_state_backfilling_when_partial() {
        assert_eq!(
            derive_state(&projects(2, 4)),
            Some(IndexingState::Backfilling)
        );
    }

    #[test]
    fn code_state_indexed_when_complete() {
        assert_eq!(derive_state(&projects(4, 4)), Some(IndexingState::Indexed));
    }

    #[test]
    fn resolve_node_states_assigns_code_state_to_pipelineless_nodes() {
        let ontology = test_ontology();

        let states = resolve_node_states(&ontology, Some(IndexingState::Backfilling));
        assert_eq!(states.get("Definition"), Some(&IndexingState::Backfilling));
        assert_eq!(states.get("File"), Some(&IndexingState::Backfilling));
        assert!(
            !states.contains_key("MergeRequest"),
            "MergeRequest declares pipelines and belongs to the SDLC surface"
        );
        assert!(
            !states.contains_key("User"),
            "User declares global pipelines and belongs to the SDLC surface"
        );
    }

    #[test]
    fn resolve_node_states_empty_when_no_code_state() {
        let ontology = test_ontology();
        assert!(resolve_node_states(&ontology, None).is_empty());
    }

    fn test_projects_sql() -> String {
        projects_sql("v1_gl_project", "v1_code_indexing_checkpoint")
    }

    #[test]
    fn projects_query_includes_both_tables() {
        let sql = test_projects_sql();
        assert!(sql.contains("v1_gl_project"), "SQL: {sql}");
        assert!(sql.contains("v1_code_indexing_checkpoint"), "SQL: {sql}");
    }

    #[test]
    fn projects_query_joins_checkpoints_to_live_projects() {
        let sql = test_projects_sql();
        assert!(sql.contains("INNER JOIN"), "SQL: {sql}");
        assert!(sql.contains("c.project_id = p.id"), "SQL: {sql}");
        assert!(sql.contains("startsWith(p.traversal_path"), "SQL: {sql}");
        assert!(sql.contains("startsWith(c.traversal_path"), "SQL: {sql}");
    }

    #[test]
    fn projects_query_binds_traversal_path() {
        assert!(
            test_projects_sql().contains("{path:String}"),
            "traversal_path must be a bound parameter, not interpolated"
        );
    }

    #[test]
    fn projects_query_uses_uniq() {
        let sql = test_projects_sql();
        assert_eq!(
            sql.matches("uniqExact(").count(),
            2,
            "Should have two uniqExact() calls. SQL: {sql}"
        );
    }

    #[test]
    fn projects_query_filters_deleted_on_both_tables() {
        let sql = test_projects_sql();
        assert_eq!(
            sql.matches("_deleted").count(),
            3,
            "Project coverage should filter deleted checkpoint and project rows. SQL: {sql}"
        );
    }
}
