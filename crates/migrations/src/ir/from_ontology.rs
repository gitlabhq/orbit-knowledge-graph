use ontology::{
    DELETED_COLUMN, DataType, Field, NodeEntity, Ontology, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use super::{ClickHouseType, ColumnSchema, Engine, TableSchema};

/// Convert a `NodeEntity` into its desired `TableSchema`.
///
/// System columns (`_version`, `_deleted`, and optionally `traversal_path`)
/// are appended after entity-defined fields, matching the patterns
/// in `fixtures/schema/graph.sql`.
pub fn node_to_table_schema(node: &NodeEntity, ontology: &Ontology) -> TableSchema {
    let sort_key = effective_sort_key(node, ontology);
    let has_traversal_path = sort_key.iter().any(|k| k == TRAVERSAL_PATH_COLUMN);
    let mut columns = entity_columns(node);

    if has_traversal_path && !columns.iter().any(|c| c.name == TRAVERSAL_PATH_COLUMN) {
        columns.push(ColumnSchema {
            name: TRAVERSAL_PATH_COLUMN.to_string(),
            column_type: ClickHouseType::String,
            nullable: false,
            default_value: Some("'0/'".to_string()),
        });
    }

    columns.push(version_column());
    columns.push(deleted_column());

    let engine = Engine::ReplacingMergeTree {
        version_column: VERSION_COLUMN.to_string(),
        deleted_column: Some(DELETED_COLUMN.to_string()),
    };

    TableSchema {
        name: node.destination_table.clone(),
        columns,
        engine,
        order_by: sort_key.clone(),
        primary_key: sort_key,
        settings: Vec::new(),
    }
}

/// Generate the edge table schema from ontology-level configuration.
pub fn edge_table_schema(ontology: &Ontology) -> TableSchema {
    let edge_primary_key: Vec<String> = ontology.edge_sort_key().iter().take(4).cloned().collect();

    TableSchema {
        name: ontology.edge_table().to_string(),
        columns: vec![
            ColumnSchema {
                name: TRAVERSAL_PATH_COLUMN.to_string(),
                column_type: ClickHouseType::String,
                nullable: false,
                default_value: Some("'0/'".to_string()),
            },
            ColumnSchema {
                name: "source_id".to_string(),
                column_type: ClickHouseType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "source_kind".to_string(),
                column_type: ClickHouseType::String,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "relationship_kind".to_string(),
                column_type: ClickHouseType::String,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "target_id".to_string(),
                column_type: ClickHouseType::Int64,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "target_kind".to_string(),
                column_type: ClickHouseType::String,
                nullable: false,
                default_value: None,
            },
            version_column(),
            deleted_column(),
        ],
        engine: Engine::ReplacingMergeTree {
            version_column: VERSION_COLUMN.to_string(),
            deleted_column: Some(DELETED_COLUMN.to_string()),
        },
        order_by: ontology.edge_sort_key().to_vec(),
        primary_key: edge_primary_key,
        settings: Vec::new(),
    }
}

/// Collect all desired `TableSchema`s from the ontology (nodes + edge table).
pub fn all_table_schemas(ontology: &Ontology) -> Vec<TableSchema> {
    let mut schemas: Vec<TableSchema> = ontology
        .nodes()
        .map(|node| node_to_table_schema(node, ontology))
        .collect();
    schemas.push(edge_table_schema(ontology));
    schemas
}

fn effective_sort_key(node: &NodeEntity, ontology: &Ontology) -> Vec<String> {
    if node.sort_key.is_empty() {
        ontology.default_entity_sort_key().to_vec()
    } else {
        node.sort_key.clone()
    }
}

fn entity_columns(node: &NodeEntity) -> Vec<ColumnSchema> {
    node.fields
        .iter()
        .map(|field| ColumnSchema {
            name: field.name.clone(),
            column_type: data_type_to_clickhouse(&field.data_type),
            nullable: field.nullable,
            default_value: default_for_field(field),
        })
        .collect()
}

fn data_type_to_clickhouse(data_type: &DataType) -> ClickHouseType {
    match data_type {
        DataType::String | DataType::Enum => ClickHouseType::String,
        DataType::Int => ClickHouseType::Int64,
        DataType::Float => ClickHouseType::Float64,
        DataType::Bool => ClickHouseType::Bool,
        DataType::Date => ClickHouseType::Date32,
        DataType::DateTime => ClickHouseType::DateTime64,
        DataType::Uuid => ClickHouseType::UUID,
    }
}

/// Determine the DEFAULT expression for an entity field.
///
/// Nullable fields get no default (NULL is implicit).
/// Non-nullable fields get a type-appropriate zero value.
fn default_for_field(field: &Field) -> Option<String> {
    if field.nullable {
        return None;
    }

    match field.data_type {
        DataType::String | DataType::Enum => Some("''".to_string()),
        DataType::Int | DataType::Float => None,
        DataType::Bool => Some("false".to_string()),
        DataType::Date | DataType::DateTime | DataType::Uuid => None,
    }
}

fn version_column() -> ColumnSchema {
    ColumnSchema {
        name: VERSION_COLUMN.to_string(),
        column_type: ClickHouseType::DateTime64,
        nullable: false,
        default_value: Some("now64(6)".to_string()),
    }
}

fn deleted_column() -> ColumnSchema {
    ColumnSchema {
        name: DELETED_COLUMN.to_string(),
        column_type: ClickHouseType::Bool,
        nullable: false,
        default_value: Some("false".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology must be valid")
    }

    #[test]
    fn global_node_has_no_traversal_path() {
        let ontology = test_ontology();
        let user = ontology.get_node("User").unwrap();
        let schema = node_to_table_schema(user, &ontology);

        assert_eq!(schema.name, "gl_user");
        assert!(
            !schema
                .columns
                .iter()
                .any(|c| c.name == TRAVERSAL_PATH_COLUMN),
            "global node should not have traversal_path column"
        );
        assert_eq!(schema.order_by, vec!["id"]);
        assert_eq!(schema.primary_key, vec!["id"]);
    }

    #[test]
    fn namespaced_node_has_traversal_path() {
        let ontology = test_ontology();
        let project = ontology.get_node("Project").unwrap();
        let schema = node_to_table_schema(project, &ontology);

        assert_eq!(schema.name, "gl_project");
        assert!(
            schema
                .columns
                .iter()
                .any(|c| c.name == TRAVERSAL_PATH_COLUMN),
            "namespaced node should have traversal_path column"
        );
        assert_eq!(schema.order_by, vec!["traversal_path", "id"]);
    }

    #[test]
    fn node_with_etl_has_deleted_in_engine() {
        let ontology = test_ontology();
        let user = ontology.get_node("User").unwrap();
        let schema = node_to_table_schema(user, &ontology);

        match &schema.engine {
            Engine::ReplacingMergeTree { deleted_column, .. } => {
                assert!(
                    deleted_column.is_some(),
                    "node with ETL should have _deleted in engine"
                );
            }
        }
    }

    #[test]
    fn node_without_etl_still_has_deleted_in_engine() {
        let ontology = test_ontology();
        let branch = ontology.get_node("Branch").unwrap();
        let schema = node_to_table_schema(branch, &ontology);

        match &schema.engine {
            Engine::ReplacingMergeTree { deleted_column, .. } => {
                assert!(
                    deleted_column.is_some(),
                    "all nodes should have _deleted in engine"
                );
            }
        }
    }

    #[test]
    fn all_tables_include_version_and_deleted_columns() {
        let ontology = test_ontology();
        let schemas = all_table_schemas(&ontology);

        for schema in &schemas {
            assert!(
                schema.columns.iter().any(|c| c.name == VERSION_COLUMN),
                "{} missing _version column",
                schema.name
            );
            assert!(
                schema.columns.iter().any(|c| c.name == DELETED_COLUMN),
                "{} missing _deleted column",
                schema.name
            );
        }
    }

    #[test]
    fn edge_table_schema_matches_expected_columns() {
        let ontology = test_ontology();
        let schema = edge_table_schema(&ontology);

        assert_eq!(schema.name, "gl_edge");
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            column_names,
            vec![
                "traversal_path",
                "source_id",
                "source_kind",
                "relationship_kind",
                "target_id",
                "target_kind",
                "_version",
                "_deleted",
            ]
        );
    }

    #[test]
    fn data_type_mapping_is_correct() {
        assert_eq!(
            data_type_to_clickhouse(&DataType::String),
            ClickHouseType::String
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Int),
            ClickHouseType::Int64
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Float),
            ClickHouseType::Float64
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Bool),
            ClickHouseType::Bool
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Date),
            ClickHouseType::Date32
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::DateTime),
            ClickHouseType::DateTime64
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Uuid),
            ClickHouseType::UUID
        );
        assert_eq!(
            data_type_to_clickhouse(&DataType::Enum),
            ClickHouseType::String
        );
    }
}
