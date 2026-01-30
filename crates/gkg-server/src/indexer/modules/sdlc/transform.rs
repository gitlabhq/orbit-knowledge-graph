use super::prepare::{PreparedEdge, PreparedEdgeEtl, PreparedEtlConfig};

pub const SOURCE_DATA_TABLE: &str = "source_data";

pub fn build_transform_sql(config: &PreparedEtlConfig) -> String {
    let mut columns: Vec<&str> = config
        .fields
        .iter()
        .map(|f| f.expression.as_str())
        .collect();

    if config.is_namespaced {
        columns.push("traversal_path");
    }
    columns.push("_version");
    columns.push("_deleted");

    format!("SELECT {} FROM {}", columns.join(", "), SOURCE_DATA_TABLE)
}

pub fn build_all_edge_sql(config: &PreparedEtlConfig) -> Vec<String> {
    config.edges.iter().map(build_edge_sql).collect()
}

/// Build SQL to transform edge ETL data into edge format.
///
/// This is for edges sourced from join tables (no node output).
pub fn build_edge_etl_transform_sql(config: &PreparedEdgeEtl) -> String {
    let filter = config
        .source_type_filter
        .as_ref()
        .map(|f| format!(" AND {}", f))
        .unwrap_or_default();

    format!(
        r#"SELECT
    {} AS source_id,
    {} AS source_kind,
    '{}' AS relationship_kind,
    {} AS target_id,
    {} AS target_kind,
    _version,
    _deleted
FROM {}
WHERE {} IS NOT NULL AND {} IS NOT NULL{}"#,
        config.source_id,
        config.source_kind.to_sql(),
        config.relationship_kind,
        config.target_id,
        config.target_kind.to_sql(),
        SOURCE_DATA_TABLE,
        config.source_id,
        config.target_id,
        filter
    )
}

fn build_edge_sql(edge: &PreparedEdge) -> String {
    let filter = edge
        .type_filter
        .as_ref()
        .map(|f| format!(" AND {}", f))
        .unwrap_or_default();

    format!(
        r#"SELECT
    {} AS source_id,
    {} AS source_kind,
    '{}' AS relationship_kind,
    {} AS target_id,
    {} AS target_kind,
    _version,
    _deleted
FROM {}
WHERE {} IS NOT NULL{}"#,
        edge.source_id,
        edge.source_kind.to_sql(),
        edge.relationship_kind,
        edge.target_id,
        edge.target_kind.to_sql(),
        SOURCE_DATA_TABLE,
        edge.fk_column,
        filter
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::modules::sdlc::prepare::PreparedEtlConfig;
    use ontology::{
        DataType, EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope, Field, NodeEntity,
        Ontology,
    };
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
            redaction: None,
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
            redaction: None,
        }
    }

    #[test]
    fn transform_sql_excludes_traversal_path_for_global() {
        let ontology = Ontology::new();
        let config = PreparedEtlConfig::from_node(&create_user_node(), &ontology).unwrap();
        let sql = build_transform_sql(&config);

        assert!(!sql.contains("traversal_path"));
        assert!(sql.contains("FROM source_data"));
    }

    #[test]
    fn transform_sql_includes_traversal_path_for_namespaced() {
        let ontology = Ontology::new();
        let config = PreparedEtlConfig::from_node(&create_namespaced_node(), &ontology).unwrap();
        let sql = build_transform_sql(&config);

        assert!(sql.contains("traversal_path"));
    }

    #[test]
    fn transform_sql_handles_column_renaming() {
        let ontology = Ontology::new();
        let config = PreparedEtlConfig::from_node(&create_user_node(), &ontology).unwrap();
        let sql = build_transform_sql(&config);

        assert!(sql.contains("admin AS is_admin"));
    }

    #[test]
    fn transform_sql_handles_enum_fields() {
        let ontology = Ontology::new();
        let config = PreparedEtlConfig::from_node(&create_user_node(), &ontology).unwrap();
        let sql = build_transform_sql(&config);

        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN user_type = 0 THEN 'human'"));
    }

    #[test]
    fn edge_sql_empty_for_no_edges() {
        let ontology = Ontology::new();
        let config = PreparedEtlConfig::from_node(&create_user_node(), &ontology).unwrap();
        let sqls = build_all_edge_sql(&config);

        assert!(sqls.is_empty());
    }

    #[test]
    fn edge_sql_outgoing() {
        let ontology = Ontology::new();
        let mut edges = BTreeMap::new();
        edges.insert(
            "owner_id".to_string(),
            EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "owns".to_string(),
                direction: EdgeDirection::Outgoing,
            },
        );

        let node = NodeEntity {
            name: "Group".to_string(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_groups".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_groups".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
            redaction: None,
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let sqls = build_all_edge_sql(&config);

        assert_eq!(sqls.len(), 1);
        let sql = &sqls[0];
        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
    }

    #[test]
    fn edge_sql_incoming() {
        let ontology = Ontology::new();
        let mut edges = BTreeMap::new();
        edges.insert(
            "author_id".to_string(),
            EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "authored".to_string(),
                direction: EdgeDirection::Incoming,
            },
        );

        let node = NodeEntity {
            name: "Note".to_string(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_note".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_notes".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
            redaction: None,
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let sqls = build_all_edge_sql(&config);

        assert_eq!(sqls.len(), 1);
        let sql = &sqls[0];
        assert!(sql.contains("author_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Note' AS target_kind"));
    }
}
