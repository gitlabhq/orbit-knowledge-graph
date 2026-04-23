use ontology::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};

use crate::llqm_v1::ast::{Expr, Insert, InsertSelect, Op, Query, SelectExpr, TableRef};
use crate::llqm_v1::codegen;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

pub struct DeletionStatement {
    /// Unprefixed table name, used for logging and test assertions.
    pub table: String,
    /// Full SQL with the schema-version prefix applied to the table name.
    pub sql: String,
}

/// Builds `INSERT INTO ... SELECT` statements that soft-delete all rows for a namespace
/// across all ontology-driven tables.
///
/// For each namespaced node table and the shared edge table, generates:
/// ```sql
/// INSERT INTO {prefixed_table}
/// SELECT {sort_key_columns..., true, now64(6)}
/// FROM {prefixed_table}
/// WHERE startsWith(traversal_path, {traversal_path:String})
///   AND _deleted = false
/// ```
///
/// The table names are prefixed according to the embedded `SCHEMA_VERSION` so
/// deletions target the same table-set the indexer is currently writing to.
pub fn build_deletion_statements(ontology: &ontology::Ontology) -> Vec<DeletionStatement> {
    let mut statements = Vec::new();

    for node in ontology.nodes() {
        if !node.has_traversal_path {
            continue;
        }

        let sort_key = ontology
            .sort_key_for_table(&node.destination_table)
            .unwrap_or(&node.sort_key);

        let select = build_select_from_sort_key(sort_key);
        let columns = build_destination_columns(sort_key);
        let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
        let statement = build_deletion_insert(&node.destination_table, &prefixed, columns, select);
        statements.push(statement);
    }

    for edge_table in ontology.edge_tables() {
        let config = ontology
            .edge_table_config(edge_table)
            .expect("edge_tables() only returns keys present in edge_table_configs");
        let edge_sort_key = &config.sort_key;
        let edge_select = build_select_from_sort_key(edge_sort_key);
        let edge_columns = build_destination_columns(edge_sort_key);
        let prefixed_edge = prefixed_table_name(edge_table, *SCHEMA_VERSION);
        let edge_statement =
            build_deletion_insert(edge_table, &prefixed_edge, edge_columns, edge_select);
        statements.push(edge_statement);
    }

    statements
}

fn build_select_from_sort_key(sort_key: &[String]) -> Vec<SelectExpr> {
    let mut select: Vec<SelectExpr> = sort_key
        .iter()
        .map(|column| SelectExpr::bare(Expr::col("", column)))
        .collect();
    select.push(SelectExpr::bare(Expr::raw("true")));
    select.push(SelectExpr::bare(Expr::raw("now64(6)")));
    select
}

fn build_destination_columns(sort_key: &[String]) -> Vec<String> {
    let mut columns: Vec<String> = sort_key.to_vec();
    columns.push(DELETED_COLUMN.to_string());
    columns.push(VERSION_COLUMN.to_string());
    columns
}

fn build_deletion_insert(
    unprefixed_table: &str,
    prefixed_table: &str,
    destination_columns: Vec<String>,
    select: Vec<SelectExpr>,
) -> DeletionStatement {
    let where_clause = Expr::and_all([
        Some(Expr::func(
            "startsWith",
            vec![
                Expr::col("", TRAVERSAL_PATH_COLUMN),
                Expr::param(TRAVERSAL_PATH_COLUMN, "String"),
            ],
        )),
        Some(Expr::binary(
            Op::Eq,
            Expr::col("", DELETED_COLUMN),
            Expr::raw("false"),
        )),
    ]);

    let insert_select = InsertSelect {
        insert: Insert {
            table: prefixed_table.to_string(),
            columns: destination_columns,
        },
        query: Query {
            select,
            from: TableRef::scan(prefixed_table, None),
            where_clause,
            order_by: vec![],
            limit: None,
        },
    };

    DeletionStatement {
        table: unprefixed_table.to_string(),
        sql: codegen::emit_insert_select(&insert_select),
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
                sql.starts_with(&format!("INSERT INTO {prefixed} (")),
                "{table}: should start with INSERT INTO prefixed table: {sql}"
            );
            assert!(
                sql.contains(&format!("FROM {prefixed}")),
                "{table}: should SELECT FROM same prefixed table: {sql}"
            );
            assert!(
                sql.contains(", true, now64(6)"),
                "{table}: should select true (deleted) and now64(6) (version): {sql}"
            );
            assert!(
                sql.contains("startsWith(traversal_path, {traversal_path:String})"),
                "{table}: should filter by traversal_path: {sql}"
            );
            assert!(
                sql.contains("(_deleted = false)"),
                "{table}: should only delete non-deleted rows: {sql}"
            );
        }
    }

    #[test]
    fn node_statement_selects_sort_key_columns() {
        let ontology = load_ontology();
        let statements = build_deletion_statements(&ontology);
        let statement = find_statement(&statements, "gl_project");

        assert!(
            statement.sql.contains("traversal_path"),
            "should include traversal_path from sort key: {}",
            statement.sql
        );
        assert!(
            statement.sql.contains("id"),
            "should include id from sort key: {}",
            statement.sql
        );
    }

    #[test]
    fn edge_statements_select_sort_key_columns_for_all_tables() {
        let ontology = load_ontology();
        let statements = build_deletion_statements(&ontology);

        for edge_table in ontology.edge_tables() {
            let config = ontology
                .edge_table_config(edge_table)
                .expect("edge table config must exist");

            let statement = find_statement(&statements, edge_table);
            for column in &config.sort_key {
                assert!(
                    statement.sql.contains(column.as_str()),
                    "{edge_table} deletion should include sort key column {column}: {}",
                    statement.sql
                );
            }
        }
    }
}
