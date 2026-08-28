use ontology::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

pub struct DeletionStatement {
    /// Unprefixed table name.
    pub table: String,
    /// Full SQL with the schema-version prefix applied to the table name.
    pub sql: String,
}

/// Builds lightweight `DELETE FROM` statements that remove all rows for a
/// namespace across every ontology-driven table.
///
/// For each namespaced node table and the shared edge table, emits:
/// ```sql
/// DELETE FROM {prefixed_table}
/// WHERE startsWith(traversal_path, {traversal_path:String})
///   AND _deleted = false
/// ```
pub fn build_deletion_statements(ontology: &ontology::Ontology) -> Vec<DeletionStatement> {
    build_statements(ontology, None)
}

pub fn build_reconcile_statements(ontology: &ontology::Ontology) -> Vec<DeletionStatement> {
    build_statements(ontology, Some(MOVED_ROW_PREDICATE))
}

const MOVED_ROW_PREDICATE: &str = "\
AND length({current_paths:Array(String)}) > 0 \
AND traversal_path NOT IN {current_paths:Array(String)}";

fn build_statements(
    ontology: &ontology::Ontology,
    extra_predicate: Option<&str>,
) -> Vec<DeletionStatement> {
    let mut statements = Vec::new();

    for node in ontology.nodes() {
        if !node.has_traversal_path {
            continue;
        }
        let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
        statements.push(build_lightweight_delete(
            &node.destination_table,
            &prefixed,
            extra_predicate,
        ));
    }

    for edge_table in ontology.edge_tables() {
        let prefixed = prefixed_table_name(edge_table, *SCHEMA_VERSION);
        statements.push(build_lightweight_delete(
            edge_table,
            &prefixed,
            extra_predicate,
        ));
    }

    statements
}

fn build_lightweight_delete(
    unprefixed_table: &str,
    prefixed_table: &str,
    extra_predicate: Option<&str>,
) -> DeletionStatement {
    let extra = extra_predicate.map(|p| format!(" {p}")).unwrap_or_default();
    let sql = format!(
        "DELETE FROM {prefixed_table} \
         WHERE startsWith({TRAVERSAL_PATH_COLUMN}, {{{TRAVERSAL_PATH_COLUMN}:String}}) \
         AND {DELETED_COLUMN} = false{extra}"
    );
    DeletionStatement {
        table: unprefixed_table.to_string(),
        sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_ontology() -> ontology::Ontology {
        ontology::Ontology::load_embedded().expect("should load ontology")
    }

    fn find_statement<'a>(
        statements: &'a [DeletionStatement],
        table: &str,
    ) -> &'a DeletionStatement {
        statements
            .iter()
            .find(|s| s.table == table)
            .unwrap_or_else(|| panic!("expected statement for table {table}"))
    }

    #[test]
    fn covers_every_namespaced_node_table_plus_edge_table() {
        let ontology = load_ontology();
        let statements = build_deletion_statements(&ontology);

        let generated_tables: Vec<&str> = statements.iter().map(|s| s.table.as_str()).collect();

        let expected_namespaced: Vec<&str> = ontology
            .nodes()
            .filter(|node| node.has_traversal_path)
            .map(|node| node.destination_table.as_str())
            .collect();

        for table in &expected_namespaced {
            assert!(
                generated_tables.contains(table),
                "missing namespaced table {table}: {generated_tables:?}"
            );
        }

        for edge_table in ontology.edge_tables() {
            assert!(
                generated_tables.contains(&edge_table),
                "missing edge table {edge_table}: {generated_tables:?}"
            );
        }

        let edge_table_count = ontology.edge_tables().len();
        let expected_count = expected_namespaced.len() + edge_table_count;
        assert_eq!(
            statements.len(),
            expected_count,
            "should have exactly one statement per namespaced node + edge table"
        );
    }

    #[test]
    fn excludes_nodes_without_traversal_path() {
        let ontology = load_ontology();
        let statements = build_deletion_statements(&ontology);

        let generated_tables: Vec<&str> = statements.iter().map(|s| s.table.as_str()).collect();

        let non_traversal_tables: Vec<&str> = ontology
            .nodes()
            .filter(|node| !node.has_traversal_path)
            .map(|node| node.destination_table.as_str())
            .collect();

        for table in &non_traversal_tables {
            assert!(
                !generated_tables.contains(table),
                "{table} has no traversal_path but was included: {generated_tables:?}"
            );
        }
    }

    #[test]
    fn every_statement_has_required_sql_structure() {
        let ontology = load_ontology();
        let statements = build_deletion_statements(&ontology);

        for statement in &statements {
            let sql = &statement.sql;
            let table = &statement.table;

            let prefixed = prefixed_table_name(table, *SCHEMA_VERSION);
            assert!(
                sql.starts_with(&format!("DELETE FROM {prefixed}")),
                "{table}: should start with DELETE FROM prefixed table: {sql}"
            );
            assert!(
                sql.contains("startsWith(traversal_path, {traversal_path:String})"),
                "{table}: should filter by traversal_path: {sql}"
            );
            assert!(
                sql.contains("_deleted = false"),
                "{table}: should only delete non-deleted rows: {sql}"
            );
        }
    }

    #[test]
    fn reconcile_covers_the_same_tables_as_deletion() {
        let ontology = load_ontology();
        let deletion: Vec<String> = build_deletion_statements(&ontology)
            .into_iter()
            .map(|s| s.table)
            .collect();
        let reconcile = build_reconcile_statements(&ontology);

        assert_eq!(deletion.len(), reconcile.len());
        for statement in &reconcile {
            assert!(deletion.contains(&statement.table));
        }
    }

    #[test]
    fn reconcile_scopes_to_rows_absent_from_current_routes() {
        let ontology = load_ontology();
        let statements = build_reconcile_statements(&ontology);
        let statement = find_statement(&statements, "gl_project");

        assert!(
            statement.sql.contains("_deleted = false"),
            "should keep the non-deleted filter: {}",
            statement.sql
        );
        assert!(
            statement
                .sql
                .contains("traversal_path NOT IN {current_paths:Array(String)}"),
            "should exclude rows still present in the route set: {}",
            statement.sql
        );
        assert!(
            statement
                .sql
                .contains("length({current_paths:Array(String)}) > 0"),
            "should no-op when the root has no current routes yet: {}",
            statement.sql
        );
        assert!(
            !statement.sql.contains("traversal_paths"),
            "should not embed a datalake route subquery in a graph-side query: {}",
            statement.sql
        );
    }
}
