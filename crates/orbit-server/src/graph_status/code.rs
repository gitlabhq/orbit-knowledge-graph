use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;

use super::execute_query;
use crate::proto::ProjectsStatus;

const PROJECT_NODE: &str = "Project";
const CHECKPOINT_TABLE_SUFFIX: &str = "code_indexing_checkpoint";

pub async fn fetch_project_coverage(
    client: &ArrowClickHouseClient,
    ontology: &Ontology,
    traversal_path: &TraversalPath,
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
