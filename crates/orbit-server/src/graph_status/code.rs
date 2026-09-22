use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;
use tracing::warn;

use super::execute_query;
use crate::proto::{IndexingState, ProjectsStatus};

const PROJECT_NODE: &str = "Project";
const CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

pub struct CodeIndexingState {
    pub projects: ProjectsStatus,
    pub state: Option<IndexingState>,
}

pub async fn get_code_indexing_state(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    traversal_path: &TraversalPath,
) -> CodeIndexingState {
    match fetch_project_coverage(client, ontology, traversal_path).await {
        Ok(projects) => CodeIndexingState {
            state: derive_state(&projects),
            projects,
        },
        Err(error) => {
            warn!(%traversal_path, %error, "Graph status branch failed");
            CodeIndexingState {
                projects: ProjectsStatus::default(),
                state: Some(IndexingState::Unknown),
            }
        }
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

async fn fetch_project_coverage(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    traversal_path: &TraversalPath,
) -> Result<ProjectsStatus, Status> {
    let tables = project_tables(ontology)?;
    let sql = projects_sql(&tables.project, &tables.code_checkpoint);
    let params = [("path", traversal_path.as_str())];
    let batches = execute_query(client, &sql, &params, "projects").await?;

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
