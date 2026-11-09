use query_data_model::{
    Catalog, ClickHouse, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

pub struct DeletionStatement {
    /// Unprefixed table name.
    pub table: String,
    /// Full SQL with the schema-version prefix applied to the table name.
    pub sql: String,
}

/// Builds `INSERT INTO ... SELECT` statements that soft-delete all rows for a
/// namespace across every graph table.
///
/// For each namespaced node table and the shared edge table, emits:
/// ```sql
/// INSERT INTO {prefixed_table} ({sort_key..., unchanging columns..., _deleted, _version})
/// SELECT {sort_key..., unchanging columns..., true, now64(6)}
/// FROM {prefixed_table}
/// WHERE startsWith(traversal_path, {traversal_path:String})
///   AND _deleted = false
/// ```
pub fn build_deletion_statements(catalog: &Catalog<ClickHouse>) -> Vec<DeletionStatement> {
    build_statements(catalog, None)
}

pub struct ReconcileStatement {
    pub live_paths_sql: String,
    pub tombstone: DeletionStatement,
}

pub fn build_reconcile_statements(catalog: &Catalog<ClickHouse>) -> Vec<ReconcileStatement> {
    build_statements(catalog, Some(STALE_PATH_PREDICATE))
        .into_iter()
        .map(|tombstone| {
            let prefixed = prefixed_table_name(&tombstone.table, *SCHEMA_VERSION);
            ReconcileStatement {
                live_paths_sql: format!(
                    "SELECT DISTINCT {TRAVERSAL_PATH_COLUMN} FROM {prefixed} \
                     WHERE startsWith({TRAVERSAL_PATH_COLUMN}, {{{TRAVERSAL_PATH_COLUMN}:String}}) \
                     AND {DELETED_COLUMN} = false"
                ),
                tombstone,
            }
        })
        .collect()
}

const STALE_PATH_PREDICATE: &str = "AND traversal_path IN {stale_paths:Array(String)}";

struct DeletionTable<'a> {
    table: &'a str,
    sort_key: &'a [String],
    carried: Vec<&'a str>,
}

fn deletion_tables(catalog: &Catalog<ClickHouse>) -> Vec<DeletionTable<'_>> {
    let graph = catalog.graph();
    let Ok(schema) = catalog.schema() else {
        return Vec::new();
    };
    let sort_key = |table: &str| schema.table(table).map_or(&[][..], |table| &table.order_by);
    let mut entities: Vec<_> = graph
        .entities()
        .filter(|entity| catalog.has_path_column(entity.id))
        .collect();
    entities.sort_by_key(|entity| entity.name.as_str());
    let mut tables: Vec<DeletionTable<'_>> = entities
        .into_iter()
        .filter_map(|entity| {
            let table = catalog.table_of(entity.id)?;
            let sort_key = sort_key(table);
            let carried = entity
                .properties
                .iter()
                .map(|property| graph.property(*property))
                .filter(|property| !property.mutable && catalog.column(property.id).is_some())
                .map(|property| property.name.as_str())
                .filter(|name| !sort_key.iter().any(|key| key == name))
                .collect();
            Some(DeletionTable {
                table,
                sort_key,
                carried,
            })
        })
        .collect();
    let relationships: Vec<_> = graph
        .relationships()
        .map(|relationship| relationship.id)
        .collect();
    let mut edge_tables = catalog.edge_tables(&relationships);
    edge_tables.extend(catalog.default_edge_table().map(str::to_string));
    edge_tables.sort();
    edge_tables.dedup();
    for table in edge_tables {
        if let Some(table) = schema.table(&table) {
            tables.push(DeletionTable {
                table: &table.name,
                sort_key: &table.order_by,
                carried: Vec::new(),
            });
        }
    }
    tables
}

fn build_statements(
    catalog: &Catalog<ClickHouse>,
    extra_predicate: Option<&str>,
) -> Vec<DeletionStatement> {
    deletion_tables(catalog)
        .into_iter()
        .map(|table| {
            build_deletion_insert(
                table.table,
                &prefixed_table_name(table.table, *SCHEMA_VERSION),
                table.sort_key,
                &table.carried,
                extra_predicate,
            )
        })
        .collect()
}

fn build_deletion_insert(
    unprefixed_table: &str,
    prefixed_table: &str,
    sort_key: &[String],
    carried_columns: &[&str],
    extra_predicate: Option<&str>,
) -> DeletionStatement {
    let carried_cols = sort_key
        .iter()
        .map(String::as_str)
        .chain(carried_columns.iter().copied())
        .collect::<Vec<_>>()
        .join(", ");
    let insert_columns = format!("{carried_cols}, {DELETED_COLUMN}, {VERSION_COLUMN}");
    let extra = extra_predicate.map(|p| format!(" {p}")).unwrap_or_default();
    let sql = format!(
        "INSERT INTO {prefixed_table} ({insert_columns}) \
         SELECT {carried_cols}, true, now64(6) \
         FROM {prefixed_table} \
         WHERE (startsWith({TRAVERSAL_PATH_COLUMN}, {{{TRAVERSAL_PATH_COLUMN}:String}}) \
         AND ({DELETED_COLUMN} = false){extra})"
    );
    DeletionStatement {
        table: unprefixed_table.to_string(),
        sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Catalog<ClickHouse> {
        Catalog::<ClickHouse>::embedded().expect("embedded catalog")
    }

    fn edge_tables() -> Vec<String> {
        let catalog = catalog();
        let relationships: Vec<_> = catalog.graph().relationships().map(|r| r.id).collect();
        catalog.edge_tables(&relationships)
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
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);

        let generated_tables: Vec<&str> = statements.iter().map(|s| s.table.as_str()).collect();

        let expected_namespaced: Vec<&str> = catalog
            .graph()
            .entities()
            .filter(|entity| catalog.has_path_column(entity.id))
            .filter_map(|entity| catalog.table_of(entity.id))
            .collect();

        for table in &expected_namespaced {
            assert!(
                generated_tables.contains(table),
                "missing namespaced table {table}: {generated_tables:?}"
            );
        }

        for edge_table in edge_tables() {
            assert!(
                generated_tables.contains(&edge_table.as_str()),
                "missing edge table {edge_table}: {generated_tables:?}"
            );
        }

        let edge_table_count = edge_tables().len();
        let expected_count = expected_namespaced.len() + edge_table_count;
        assert_eq!(
            statements.len(),
            expected_count,
            "should have exactly one statement per namespaced node + edge table"
        );
    }

    #[test]
    fn excludes_nodes_without_traversal_path() {
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);

        let generated_tables: Vec<&str> = statements.iter().map(|s| s.table.as_str()).collect();

        let non_traversal_tables: Vec<&str> = catalog
            .graph()
            .entities()
            .filter(|entity| !catalog.has_path_column(entity.id))
            .filter_map(|entity| catalog.table_of(entity.id))
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
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);

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
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);
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
    fn node_statements_carry_the_unchanging_columns() {
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);
        let tables = deletion_tables(&catalog);
        let carried = tables
            .iter()
            .find(|table| table.table == "gl_file")
            .expect("gl_file table")
            .carried
            .clone();
        assert!(carried.contains(&"path"), "{carried:?}");
        let statement = find_statement(&statements, "gl_file");
        for column in carried {
            assert_eq!(
                statement.sql.matches(&format!(", {column},")).count(),
                2,
                "{column} must appear in the INSERT list and the SELECT: {}",
                statement.sql
            );
        }
    }

    #[test]
    fn edge_statements_select_sort_key_columns_for_all_tables() {
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog);

        let schema = catalog.schema().expect("schema");
        for edge_table in edge_tables() {
            let table = schema.table(&edge_table).expect("edge table must exist");

            let statement = find_statement(&statements, &edge_table);
            for column in &table.order_by {
                assert!(
                    statement.sql.contains(column.as_str()),
                    "{edge_table} deletion should include sort key column {column}: {}",
                    statement.sql
                );
            }
        }
    }

    #[test]
    fn reconcile_covers_the_same_tables_as_deletion() {
        let catalog = catalog();
        let deletion: Vec<String> = build_deletion_statements(&catalog)
            .into_iter()
            .map(|s| s.table)
            .collect();
        let reconcile = build_reconcile_statements(&catalog);

        assert_eq!(deletion.len(), reconcile.len());
        for statement in &reconcile {
            assert!(deletion.contains(&statement.tombstone.table));
        }
    }

    #[test]
    fn reconcile_tombstones_only_the_stale_paths_it_reads() {
        let statements = build_reconcile_statements(&catalog());
        let statement = statements
            .iter()
            .find(|s| s.tombstone.table == "gl_project")
            .expect("expected reconcile statement for gl_project");
        let (live_paths_sql, sql) = (&statement.live_paths_sql, &statement.tombstone.sql);

        assert!(
            live_paths_sql.starts_with("SELECT DISTINCT traversal_path FROM ")
                && live_paths_sql.contains("startsWith(traversal_path, {traversal_path:String})")
                && live_paths_sql.contains("_deleted = false"),
            "should read the live paths under the root: {live_paths_sql}"
        );
        assert!(
            sql.contains("(_deleted = false)"),
            "should keep the FINAL-safe non-deleted filter: {sql}"
        );
        assert!(
            sql.contains("traversal_path IN {stale_paths:Array(String)}"),
            "should tombstone only the stale paths: {sql}"
        );
        assert!(
            !sql.contains("traversal_paths") && !live_paths_sql.contains("traversal_paths"),
            "should not embed a datalake route subquery in a graph-side query: {sql}"
        );
    }
}
