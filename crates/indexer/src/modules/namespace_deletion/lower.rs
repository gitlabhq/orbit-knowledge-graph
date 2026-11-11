use query_data_model::{
    Catalog, ClickHouse, DELETED_COLUMN, DataModelError, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::lifetime::Lifetime;
pub use crate::lifetime::Tombstone as DeletionStatement;

/// Builds the `INSERT INTO ... SELECT` statements that soft-delete a namespace, in order.
///
/// The edges, in every edge table, of the namespace's live nodes go first, so an edge stored
/// under another namespace's path dies with its endpoint. Then every namespaced node table and
/// every edge table tombstones its rows under the path:
/// ```sql
/// INSERT INTO {prefixed_table} ({sort_key..., unchanging columns..., _deleted, _version})
/// SELECT {sort_key..., unchanging columns..., true, now64(6)}
/// FROM {prefixed_table}
/// WHERE startsWith(traversal_path, {traversal_path:String})
///   AND _deleted = false
/// ```
pub fn build_deletion_statements(
    catalog: &Catalog<ClickHouse>,
) -> Result<Vec<DeletionStatement>, DataModelError> {
    let lifetime = Lifetime::from_catalog(catalog)?;
    let mut statements = lifetime.edges_under_path();
    statements.extend(build_statements(catalog, &lifetime, None)?);
    Ok(statements)
}

pub struct ReconcileStatement {
    pub live_paths_sql: String,
    pub tombstone: DeletionStatement,
}

pub fn build_reconcile_statements(
    catalog: &Catalog<ClickHouse>,
) -> Result<Vec<ReconcileStatement>, DataModelError> {
    let lifetime = Lifetime::from_catalog(catalog)?;
    Ok(
        build_statements(catalog, &lifetime, Some(STALE_PATH_PREDICATE))?
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
            .collect(),
    )
}

const STALE_PATH_PREDICATE: &str = "AND traversal_path IN {stale_paths:Array(String)}";

struct DeletionTable {
    table: String,
    sort_key: Vec<String>,
    carried: Vec<String>,
}

/// Every namespaced node table, then every edge table, keyed by its sort key.
fn deletion_tables(
    catalog: &Catalog<ClickHouse>,
    lifetime: &Lifetime,
) -> Result<Vec<DeletionTable>, DataModelError> {
    let schema = catalog.schema()?;
    let mut tables: Vec<_> = lifetime
        .nodes()
        .iter()
        .map(|node| DeletionTable {
            table: node.table.clone(),
            sort_key: node.sort_key.clone(),
            carried: node.carried.clone(),
        })
        .collect();
    let relationships: Vec<_> = catalog
        .graph()
        .relationships()
        .map(|relationship| relationship.id)
        .collect();
    let mut edge_tables = catalog.edge_tables(&relationships);
    edge_tables.extend(catalog.shared_edge_table().map(str::to_string));
    edge_tables.sort();
    edge_tables.dedup();
    for table in edge_tables {
        let sort_key = schema
            .table(&table)
            .ok_or_else(|| DataModelError::Invalid(format!("{table} has no schema")))?
            .sort_key
            .clone();
        tables.push(DeletionTable {
            table,
            sort_key,
            carried: Vec::new(),
        });
    }
    Ok(tables)
}

fn build_statements(
    catalog: &Catalog<ClickHouse>,
    lifetime: &Lifetime,
    extra_predicate: Option<&str>,
) -> Result<Vec<DeletionStatement>, DataModelError> {
    Ok(deletion_tables(catalog, lifetime)?
        .iter()
        .map(|table| build_deletion_insert(table, extra_predicate))
        .collect())
}

fn build_deletion_insert(
    table: &DeletionTable,
    extra_predicate: Option<&str>,
) -> DeletionStatement {
    let prefixed_table = prefixed_table_name(&table.table, *SCHEMA_VERSION);
    let carried_cols = table
        .sort_key
        .iter()
        .chain(&table.carried)
        .map(String::as_str)
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
        table: table.table.clone(),
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

    /// The node and edge table statements, after the edge cascade that runs first.
    fn table_statements(catalog: &Catalog<ClickHouse>) -> Vec<DeletionStatement> {
        let cascade = Lifetime::from_catalog(catalog)
            .unwrap()
            .edges_under_path()
            .len();
        build_deletion_statements(catalog)
            .unwrap()
            .split_off(cascade)
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
    fn edges_of_the_namespace_nodes_are_tombstoned_first_in_every_edge_table() {
        let catalog = catalog();
        let statements = build_deletion_statements(&catalog).unwrap();
        let cascade = Lifetime::from_catalog(&catalog).unwrap().edges_under_path();
        assert!(!cascade.is_empty());
        assert_eq!(statements[..cascade.len()], cascade[..]);
        let member_of = cascade
            .iter()
            .find(|statement| {
                statement.sql.contains("'MEMBER_OF'")
                    && statement.sql.contains("target_kind = 'Group'")
            })
            .expect("a cascade for the MEMBER_OF edges of a group");
        let group = prefixed_table_name("gl_group", *SCHEMA_VERSION);
        assert!(
            member_of.sql.contains(&format!(
                "e.target_id IN (SELECT id FROM {group} FINAL WHERE startsWith(traversal_path, {{traversal_path:String}}))"
            )),
            "{}",
            member_of.sql
        );
        assert!(
            !cascade
                .iter()
                .any(|statement| statement.sql.contains("_kind = 'User'")),
            "a global entity has no namespace to delete"
        );
    }

    #[test]
    fn covers_every_namespaced_node_table_plus_edge_table() {
        let catalog = catalog();
        let statements = table_statements(&catalog);

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
        let statements = table_statements(&catalog);

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
        let statements = table_statements(&catalog);

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
        let statements = table_statements(&catalog);
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
        let statements = table_statements(&catalog);
        let lifetime = Lifetime::from_catalog(&catalog).unwrap();
        let tables = deletion_tables(&catalog, &lifetime).unwrap();
        let carried = tables
            .iter()
            .find(|table| table.table == "gl_file")
            .expect("gl_file table")
            .carried
            .clone();
        assert!(carried.iter().any(|column| column == "path"), "{carried:?}");
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
        let statements = table_statements(&catalog);

        let schema = catalog.schema().expect("schema");
        for edge_table in edge_tables() {
            let table = schema.table(&edge_table).expect("edge table must exist");

            let statement = find_statement(&statements, &edge_table);
            for column in &table.sort_key {
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
        let deletion: Vec<String> = table_statements(&catalog)
            .into_iter()
            .map(|s| s.table)
            .collect();
        let reconcile = build_reconcile_statements(&catalog).unwrap();

        assert_eq!(deletion.len(), reconcile.len());
        for statement in &reconcile {
            assert!(deletion.contains(&statement.tombstone.table));
        }
    }

    #[test]
    fn reconcile_tombstones_only_the_stale_paths_it_reads() {
        let statements = build_reconcile_statements(&catalog()).unwrap();
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
