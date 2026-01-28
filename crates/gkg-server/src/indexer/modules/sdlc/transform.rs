//! Transform engine for generating SQL from ontology definitions.
//!
//! Uses `NodeEntity.fields` with `Field.source` and `Field.enum_values` to generate
//! extract, transform, and edge SQL statements.

use ontology::{
    DELETED_COLUMN, DataType, EtlConfig, EtlScope, Field, NodeEntity, TRAVERSAL_PATH_COLUMN,
    VERSION_COLUMN,
};

pub const SOURCE_DATA_TABLE: &str = "source_data";

pub fn build_source_query(node: &NodeEntity) -> Option<String> {
    let etl = node.etl.as_ref()?;

    match etl {
        EtlConfig::Table {
            scope,
            source,
            watermark,
            deleted,
            edges,
        } => {
            let mut columns: Vec<String> = node.fields.iter().map(|f| f.source.clone()).collect();

            for column in edges.keys() {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }

            if *scope == EtlScope::Namespaced {
                columns.push(TRAVERSAL_PATH_COLUMN.to_string());
            }
            columns.push(format!("{watermark} AS {VERSION_COLUMN}"));
            columns.push(format!("{deleted} AS {DELETED_COLUMN}"));

            let columns_str = columns.join(", ");
            Some(format!(
                "SELECT {columns_str}
                 FROM {source}
                 WHERE {watermark} > {{last_watermark:String}} AND {watermark} <= {{watermark:String}}
                "
            ))
        }
        EtlConfig::Query { query, .. } => Some(query.clone()),
    }
}

pub fn build_transform_sql(node: &NodeEntity) -> String {
    let columns: Vec<String> = node.fields.iter().map(build_field_expression).collect();

    let mut all_columns = columns;

    let is_namespaced = node
        .etl
        .as_ref()
        .is_some_and(|etl| etl.scope() == EtlScope::Namespaced);

    if is_namespaced {
        all_columns.push(TRAVERSAL_PATH_COLUMN.to_string());
    }
    all_columns.push(VERSION_COLUMN.to_string());
    all_columns.push(DELETED_COLUMN.to_string());

    let columns_str = all_columns.join(", ");

    format!("SELECT {columns_str} FROM {SOURCE_DATA_TABLE}")
}

pub fn build_all_edge_sql(node: &NodeEntity) -> Vec<String> {
    let Some(ref etl) = node.etl else {
        return Vec::new();
    };

    let source_kind = &node.name;
    etl.edges()
        .iter()
        .map(|(source_column, mapping)| {
            let target_kind = &mapping.target_kind;
            let relationship_kind = &mapping.relationship_kind;
            format!(
                r#"SELECT
    id AS source_id,
    '{source_kind}' AS source_kind,
    '{relationship_kind}' AS relationship_kind,
    {source_column} AS target_id,
    '{target_kind}' AS target_kind,
    {VERSION_COLUMN},
    {DELETED_COLUMN}
FROM {SOURCE_DATA_TABLE}
WHERE {source_column} IS NOT NULL"#
            )
        })
        .collect()
}

fn build_field_expression(field: &Field) -> String {
    if field.data_type == DataType::Enum
        && let Some(ref enum_values) = field.enum_values
    {
        let cases: Vec<String> = enum_values
            .iter()
            .map(|(value, label)| format!("WHEN {} = {} THEN '{}'", field.source, value, label))
            .collect();

        return format!(
            "CASE {} ELSE 'unknown' END AS {}",
            cases.join(" "),
            field.name
        );
    }

    if field.source == field.name {
        field.name.clone()
    } else {
        format!("{} AS {}", field.source, field.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{EdgeMapping, EtlScope};
    use std::collections::BTreeMap;

    fn create_user_node() -> NodeEntity {
        let mut enum_values = BTreeMap::new();
        enum_values.insert(0, "human".to_string());
        enum_values.insert(1, "bot".to_string());

        NodeEntity {
            name: "User".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "username".to_string(),
                    source: "username".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                },
                Field {
                    name: "is_admin".to_string(),
                    source: "admin".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "user_type".to_string(),
                    source: "user_type".to_string(),
                    data_type: DataType::Enum,
                    nullable: false,
                    enum_values: Some(enum_values),
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_users".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: "siphon_users".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
        }
    }

    fn create_namespaced_node() -> NodeEntity {
        NodeEntity {
            name: "Group".to_string(),
            fields: vec![Field {
                name: "id".to_string(),
                source: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                enum_values: None,
            }],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_groups".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_namespaces".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
        }
    }

    #[test]
    fn build_source_query_generates_correct_sql_for_table_config() {
        let node = create_user_node();
        let query = build_source_query(&node).unwrap();

        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM siphon_users"));
        assert!(query.contains("id"));
        assert!(query.contains("username"));
        assert!(query.contains("admin"));
        assert!(query.contains("_siphon_replicated_at AS _version"));
        assert!(query.contains("_siphon_deleted AS _deleted"));
        assert!(query.contains("{last_watermark:String}"));
        assert!(query.contains("{watermark:String}"));
    }

    #[test]
    fn build_source_query_excludes_traversal_path_for_global_entities() {
        let node = create_user_node();
        let query = build_source_query(&node).unwrap();

        assert!(!query.contains("traversal_path"));
    }

    #[test]
    fn build_source_query_includes_traversal_path_for_namespaced_entities() {
        let node = create_namespaced_node();
        let query = build_source_query(&node).unwrap();

        assert!(query.contains("traversal_path"));
    }

    #[test]
    fn build_transform_sql_excludes_traversal_path_for_global_entities() {
        let node = create_user_node();
        let sql = build_transform_sql(&node);

        assert!(!sql.contains("traversal_path"));
    }

    #[test]
    fn build_transform_sql_includes_traversal_path_for_namespaced_entities() {
        let node = create_namespaced_node();
        let sql = build_transform_sql(&node);

        assert!(sql.contains("traversal_path"));
    }

    #[test]
    fn build_transform_sql_handles_column_renaming() {
        let node = create_user_node();
        let sql = build_transform_sql(&node);

        assert!(sql.contains("id"));
        assert!(sql.contains("username"));
        assert!(sql.contains("admin AS is_admin"));
        assert!(sql.contains("FROM source_data"));
    }

    #[test]
    fn build_transform_sql_handles_enum_fields() {
        let node = create_user_node();
        let sql = build_transform_sql(&node);

        assert!(sql.contains("WHEN user_type = 0 THEN 'human'"));
        assert!(sql.contains("WHEN user_type = 1 THEN 'bot'"));
        assert!(sql.contains("ELSE 'unknown'"));
        assert!(sql.contains("AS user_type"));
    }

    #[test]
    fn build_all_edge_sql_returns_empty_for_no_edges() {
        let node = create_user_node();
        let edge_sqls = build_all_edge_sql(&node);

        assert!(edge_sqls.is_empty());
    }

    #[test]
    fn build_all_edge_sql_generates_correct_structure() {
        let mut edges = BTreeMap::new();
        edges.insert(
            "owner_id".to_string(),
            EdgeMapping {
                target_kind: "User".to_string(),
                relationship_kind: "owner".to_string(),
            },
        );

        let node = NodeEntity {
            name: "Group".to_string(),
            fields: vec![Field {
                name: "id".to_string(),
                source: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                enum_values: None,
            }],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_groups".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_namespaces".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
        };

        let edge_sqls = build_all_edge_sql(&node);

        assert_eq!(edge_sqls.len(), 1);
        let sql = &edge_sqls[0];
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("'owner' AS relationship_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
        assert!(sql.contains("WHERE owner_id IS NOT NULL"));
    }
}
