use std::collections::HashMap;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use query_engine::compiler::{Expr, JoinType, Node, Query, SelectExpr, TableRef};
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

// A node with no pipelines is code-derived, so it inherits the single code coverage state.
// Nodes that declare pipelines belong to the SDLC surface and are resolved there.
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
    let ast = lower_projects(&tables, traversal_path);
    let batches = execute_query(client, &ast, "projects").await?;

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

fn lower_projects(tables: &ProjectTables, traversal_path: &str) -> Node {
    let total_known = build_total_known_projects_query(&tables.project, traversal_path);
    let mut indexed =
        build_indexed_projects_query(&tables.project, &tables.code_checkpoint, traversal_path);

    indexed.union_all = vec![total_known];

    Node::Query(Box::new(indexed))
}

fn build_total_known_projects_query(project_table: &str, traversal_path: &str) -> Query {
    let alias = "p";

    let select = vec![
        SelectExpr::new(Expr::string("total_known"), "metric"),
        SelectExpr::new(Expr::func("uniqExact", vec![Expr::col(alias, "id")]), "cnt"),
    ];

    Query {
        select,
        from: TableRef::scan_final(project_table, alias),
        where_clause: Some(live_project_scope_filter(alias, traversal_path)),
        ..Default::default()
    }
}

fn build_indexed_projects_query(
    project_table: &str,
    code_checkpoint_table: &str,
    traversal_path: &str,
) -> Query {
    let checkpoint_alias = "c";
    let project_alias = "p";

    let select = vec![
        SelectExpr::new(Expr::string("indexed"), "metric"),
        SelectExpr::new(
            Expr::func("uniqExact", vec![Expr::col(checkpoint_alias, "project_id")]),
            "cnt",
        ),
    ];

    let from = TableRef::join(
        JoinType::Inner,
        TableRef::scan_final(code_checkpoint_table, checkpoint_alias),
        TableRef::scan_final(project_table, project_alias),
        Expr::eq(
            Expr::col(checkpoint_alias, "project_id"),
            Expr::col(project_alias, "id"),
        ),
    );

    let where_clause = Expr::and(
        live_project_scope_filter(project_alias, traversal_path),
        Expr::and(
            Expr::eq(Expr::col(checkpoint_alias, "_deleted"), Expr::int(0)),
            Expr::func(
                "startsWith",
                vec![
                    Expr::col(checkpoint_alias, "traversal_path"),
                    Expr::string(traversal_path),
                ],
            ),
        ),
    );

    Query {
        select,
        from,
        where_clause: Some(where_clause),
        ..Default::default()
    }
}

fn live_project_scope_filter(alias: &str, traversal_path: &str) -> Expr {
    Expr::and(
        Expr::eq(Expr::col(alias, "_deleted"), Expr::int(0)),
        Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, "traversal_path"),
                Expr::string(traversal_path),
            ],
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use gkg_server_config::QueryConfig;
    use query_engine::compiler::{ResultContext, codegen};
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

    fn compiled_projects_sql(traversal_path: &str) -> String {
        let tables = ProjectTables {
            project: "v1_gl_project".to_string(),
            code_checkpoint: "v1_code_indexing_checkpoint".to_string(),
        };
        let ast = lower_projects(&tables, traversal_path);
        codegen(&ast, ResultContext::new(), QueryConfig::default())
            .unwrap()
            .sql
    }

    #[test]
    fn projects_query_includes_both_tables() {
        let sql = compiled_projects_sql("1/2/");
        assert!(sql.contains("v1_gl_project"), "SQL: {sql}");
        assert!(sql.contains("v1_code_indexing_checkpoint"), "SQL: {sql}");
    }

    #[test]
    fn projects_query_joins_checkpoints_to_live_projects() {
        let sql = compiled_projects_sql("1/2/");
        assert!(sql.contains("INNER JOIN"), "SQL: {sql}");
        assert!(sql.contains("c.project_id = p.id"), "SQL: {sql}");
        assert!(sql.contains("startsWith(p.traversal_path"), "SQL: {sql}");
        assert!(sql.contains("startsWith(c.traversal_path"), "SQL: {sql}");
    }

    #[test]
    fn projects_query_uses_uniq() {
        let sql = compiled_projects_sql("1/2/");
        assert_eq!(
            sql.matches("uniqExact(").count(),
            2,
            "Should have two uniqExact() calls. SQL: {sql}"
        );
    }

    #[test]
    fn projects_query_filters_deleted_on_both_tables() {
        let sql = compiled_projects_sql("1/2/");
        assert_eq!(
            sql.matches("_deleted").count(),
            3,
            "Project coverage should filter deleted checkpoint and project rows. SQL: {sql}"
        );
    }
}
