use super::prepare::{PreparedEdge, PreparedEdgeEtl, PreparedEtlConfig};

pub const SOURCE_DATA_TABLE: &str = "source_data";

pub fn build_transform_sql(config: &PreparedEtlConfig) -> String {
    let mut columns: Vec<&str> = config
        .fields
        .iter()
        .map(|f| f.expression.as_str())
        .collect();

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

    let traversal_path_expr = if config.namespaced {
        "traversal_path"
    } else {
        "'0/' AS traversal_path"
    };

    format!(
        r#"SELECT
    {traversal_path_expr},
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
    match &edge.delimiter {
        Some(delimiter) => build_multi_target_edge_sql(edge, delimiter),
        None => build_single_value_edge_sql(edge),
    }
}

fn build_multi_target_edge_sql(edge: &PreparedEdge, delimiter: &str) -> String {
    let exploded_value = format!(
        "CAST(NULLIF(unnest(string_to_array({}, '{}')), '') AS BIGINT)",
        edge.fk_column, delimiter
    );

    let fk_is_source = edge.source_id == edge.fk_column;
    let (source_id, target_id) = if fk_is_source {
        (exploded_value.as_str(), edge.target_id.as_str())
    } else {
        (edge.source_id.as_str(), exploded_value.as_str())
    };

    let traversal_path_expr = if edge.namespaced {
        "traversal_path"
    } else {
        "'0/' AS traversal_path"
    };

    format!(
        r#"SELECT
    {traversal_path_expr},
    {source_id} AS source_id,
    {} AS source_kind,
    '{}' AS relationship_kind,
    {target_id} AS target_id,
    {} AS target_kind,
    _version,
    _deleted
FROM {}
WHERE {} IS NOT NULL AND {} != ''"#,
        edge.source_kind.to_sql(),
        edge.relationship_kind,
        edge.target_kind.to_sql(),
        SOURCE_DATA_TABLE,
        edge.fk_column,
        edge.fk_column,
    )
}

fn build_single_value_edge_sql(edge: &PreparedEdge) -> String {
    let type_filter = edge
        .type_filter
        .as_ref()
        .map(|f| format!(" AND {}", f))
        .unwrap_or_default();

    let traversal_path_expr = if edge.namespaced {
        "traversal_path"
    } else {
        "'0/' AS traversal_path"
    };

    format!(
        r#"SELECT
    {traversal_path_expr},
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
        type_filter
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::prepare::PreparedEtlConfig;
    use ontology::{
        DataType, EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope, Field, NodeEntity,
        Ontology,
    };
    use std::collections::BTreeMap;

    fn create_user_node() -> NodeEntity {
        use ontology::NodeStyle;
        let mut enum_values = BTreeMap::new();
        enum_values.insert(0, "human".to_string());
        enum_values.insert(1, "bot".to_string());

        NodeEntity {
            name: "User".to_string(),
            domain: "core".to_string(),
            description: String::new(),
            label: String::new(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "username".to_string(),
                    source: "username".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "is_admin".to_string(),
                    source: "admin".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "user_type".to_string(),
                    source: "user_type".to_string(),
                    data_type: DataType::Enum,
                    nullable: false,
                    enum_values: Some(enum_values),
                    enum_type: ontology::EnumType::Int,
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_user".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: "siphon_users".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
            redaction: None,
            style: NodeStyle::default(),
        }
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
                delimiter: None,
            },
        );

        let node = NodeEntity {
            name: "Group".to_string(),
            domain: "core".to_string(),
            description: String::new(),
            label: String::new(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_group".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_groups".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
            redaction: None,
            style: ontology::NodeStyle::default(),
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
                delimiter: None,
            },
        );

        let node = NodeEntity {
            name: "Note".to_string(),
            domain: "core".to_string(),
            description: String::new(),
            label: String::new(),
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
            style: ontology::NodeStyle::default(),
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

    #[test]
    fn edge_sql_multi_target_incoming() {
        let ontology = Ontology::new();
        let mut edges = BTreeMap::new();
        edges.insert(
            "assignee_ids".to_string(),
            EdgeMapping {
                target: EdgeTarget::Literal("User".to_string()),
                relationship_kind: "assigned".to_string(),
                direction: EdgeDirection::Incoming,
                delimiter: Some("/".to_string()),
            },
        );

        let node = NodeEntity {
            name: "WorkItem".to_string(),
            domain: "plan".to_string(),
            description: String::new(),
            label: String::new(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_work_item".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "hierarchy_work_items".to_string(),
                watermark: "version".to_string(),
                deleted: "deleted".to_string(),
                edges,
            }),
            redaction: None,
            style: ontology::NodeStyle::default(),
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let sqls = build_all_edge_sql(&config);

        assert_eq!(sqls.len(), 1);
        let sql = &sqls[0];
        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)")
        );
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'WorkItem' AS target_kind"));
    }

    #[test]
    fn edge_sql_multi_target_outgoing() {
        let ontology = Ontology::new();
        let mut edges = BTreeMap::new();
        edges.insert(
            "label_ids".to_string(),
            EdgeMapping {
                target: EdgeTarget::Literal("Label".to_string()),
                relationship_kind: "has_label".to_string(),
                direction: EdgeDirection::Outgoing,
                delimiter: Some("/".to_string()),
            },
        );

        let node = NodeEntity {
            name: "WorkItem".to_string(),
            domain: "plan".to_string(),
            description: String::new(),
            label: String::new(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_work_item".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "hierarchy_work_items".to_string(),
                watermark: "version".to_string(),
                deleted: "deleted".to_string(),
                edges,
            }),
            redaction: None,
            style: ontology::NodeStyle::default(),
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let sqls = build_all_edge_sql(&config);

        assert_eq!(sqls.len(), 1);
        let sql = &sqls[0];
        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'WorkItem' AS source_kind"));
        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(label_ids, '/')), '') AS BIGINT)")
        );
        assert!(sql.contains("'Label' AS target_kind"));
    }
}
