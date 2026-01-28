//! SQL transformation engine for generating queries from ontology definitions.
//!
//! This module provides utilities for generating SQL queries that transform
//! source data according to ontology property configurations.

use ontology::{EdgeGenerationConfig, EtlScope, NodeEntity, PropertyConfig};

/// Engine for generating SQL transformation queries from ontology definitions.
pub struct TransformEngine;

impl TransformEngine {
    /// Generate a SELECT SQL statement that transforms source data according to
    /// the node entity's property configurations.
    ///
    /// This generates column expressions that:
    /// - Map enum values using CASE WHEN statements
    /// - Rename columns where source != property name
    /// - Pass through columns where source == property name
    /// - Automatically include `traversal_path` for namespaced entities
    /// - Automatically include `deleted` column from ETL config (if specified)
    #[must_use]
    pub fn build_transform_sql(node: &NodeEntity) -> String {
        let mut column_expressions: Vec<String> = node
            .property_configs
            .iter()
            .map(|(prop_name, config)| Self::build_column_expression(prop_name, config))
            .collect();

        // Add inferred columns based on ETL configuration.
        // For Query types, the user is responsible for including all columns in their query.
        // For Table types, we automatically add traversal_path and deleted columns.
        if let Some(etl) = &node.etl {
            let is_table_type = matches!(etl, ontology::EtlConfig::Table { .. });

            // Namespaced entities always need traversal_path (if not already defined)
            // Only infer for Table types; Query types should include it in the query
            if is_table_type
                && etl.scope() == EtlScope::Namespaced
                && !node.property_configs.contains_key("traversal_path")
            {
                column_expressions.push("traversal_path".to_string());
            }

            // Add deleted column if specified in ETL config (if not already defined)
            // Only infer for Table types; Query types should include it in the query
            if is_table_type {
                if let Some(deleted_source) = etl.deleted() {
                    if !node.property_configs.contains_key("deleted") {
                        if deleted_source == "deleted" {
                            column_expressions.push("deleted".to_string());
                        } else {
                            column_expressions.push(format!("{} AS deleted", deleted_source));
                        }
                    }
                }
            }
        }

        format!(
            "SELECT\n    {}\nFROM source_data",
            column_expressions.join(",\n    ")
        )
    }

    /// Generate a single column expression for a property.
    fn build_column_expression(prop_name: &str, config: &PropertyConfig) -> String {
        if config.source.is_empty() {
            return prop_name.to_string();
        }

        if let Some(values) = &config.values {
            Self::build_enum_case_expression(&config.source, prop_name, values)
        } else if config.source != prop_name {
            format!("{} AS {}", config.source, prop_name)
        } else {
            prop_name.to_string()
        }
    }

    /// Generate a CASE WHEN expression for enum mapping.
    fn build_enum_case_expression(
        source_column: &str,
        target_name: &str,
        values: &std::collections::BTreeMap<i64, String>,
    ) -> String {
        let when_clauses: Vec<String> = values
            .iter()
            .map(|(key, value)| {
                let escaped_value = value.replace('\'', "''");
                format!("WHEN {} THEN '{}'", key, escaped_value)
            })
            .collect();

        format!(
            "CASE {}\n        {}\n        ELSE 'unknown'\n    END AS {}",
            source_column,
            when_clauses.join("\n        "),
            target_name
        )
    }

    /// Generate SQL for extracting an edge from source data.
    ///
    /// Returns SQL that selects source_id, source_kind, relationship_kind,
    /// target_id, and target_kind from source_data.
    #[must_use]
    pub fn build_edge_sql(node_type: &str, edge: &EdgeGenerationConfig) -> String {
        format!(
            r#"SELECT
    {} AS source_id,
    '{}' AS source_kind,
    '{}' AS relationship_kind,
    id AS target_id,
    '{}' AS target_kind
FROM source_data
WHERE {} IS NOT NULL"#,
            edge.source_column,
            edge.source_kind,
            edge.relationship_type,
            node_type,
            edge.source_column
        )
    }

    /// Generate a source query for table-type ETL configurations.
    ///
    /// For table types, this generates a simple SELECT with watermark filtering.
    /// For query types, this returns the custom query as-is.
    #[must_use]
    pub fn build_source_query(node: &NodeEntity) -> Option<String> {
        let etl = node.etl.as_ref()?;

        match etl {
            ontology::EtlConfig::Table {
                source,
                watermark,
                deleted,
                ..
            } => {
                let mut unique_columns: std::collections::BTreeSet<&str> = node
                    .property_configs
                    .values()
                    .map(|c| c.source.as_str())
                    .collect();

                if let Some(deleted_column) = deleted {
                    unique_columns.insert(deleted_column.as_str());
                }

                let columns_list: Vec<&str> = unique_columns.into_iter().collect();

                Some(format!(
                    "SELECT\n    {}\nFROM {}\nWHERE {} > {{last_watermark:String}}\n  AND {} <= {{watermark:String}}",
                    columns_list.join(",\n    "),
                    source,
                    watermark,
                    watermark
                ))
            }
            ontology::EtlConfig::Query { query, .. } => Some(query.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{EdgeGenerationConfig, EtlConfig, EtlScope, PropertyConfig};
    use std::collections::BTreeMap;

    fn create_test_node() -> NodeEntity {
        let mut property_configs = BTreeMap::new();

        property_configs.insert(
            "id".to_string(),
            PropertyConfig {
                property_type: "int64".to_string(),
                source: "id".to_string(),
                nullable: false,
                values: None,
            },
        );

        property_configs.insert(
            "username".to_string(),
            PropertyConfig {
                property_type: "string".to_string(),
                source: "username".to_string(),
                nullable: true,
                values: None,
            },
        );

        property_configs.insert(
            "is_admin".to_string(),
            PropertyConfig {
                property_type: "boolean".to_string(),
                source: "admin".to_string(),
                nullable: false,
                values: None,
            },
        );

        let mut user_type_values = BTreeMap::new();
        user_type_values.insert(0, "human".to_string());
        user_type_values.insert(1, "support_bot".to_string());
        user_type_values.insert(2, "alert_bot".to_string());

        property_configs.insert(
            "user_type".to_string(),
            PropertyConfig {
                property_type: "enum".to_string(),
                source: "user_type".to_string(),
                nullable: false,
                values: Some(user_type_values),
            },
        );

        NodeEntity {
            name: "User".to_string(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "users".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: "siphon_users".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: None,
            }),
            edge_generation: vec![],
            property_configs,
        }
    }

    #[test]
    fn test_build_transform_sql_generates_correct_enum_case_when() {
        let node = create_test_node();
        let sql = TransformEngine::build_transform_sql(&node);

        assert!(sql.contains("CASE user_type"));
        assert!(sql.contains("WHEN 0 THEN 'human'"));
        assert!(sql.contains("WHEN 1 THEN 'support_bot'"));
        assert!(sql.contains("WHEN 2 THEN 'alert_bot'"));
        assert!(sql.contains("ELSE 'unknown'"));
        assert!(sql.contains("END AS user_type"));
    }

    #[test]
    fn test_build_transform_sql_generates_correct_column_rename() {
        let node = create_test_node();
        let sql = TransformEngine::build_transform_sql(&node);

        assert!(sql.contains("admin AS is_admin"));
    }

    #[test]
    fn test_build_transform_sql_passes_through_matching_columns() {
        let node = create_test_node();
        let sql = TransformEngine::build_transform_sql(&node);

        assert!(sql.contains("username"));
        assert!(!sql.contains("username AS username"));
    }

    #[test]
    fn test_build_edge_sql_generates_correct_edge_extraction() {
        let edge = EdgeGenerationConfig {
            relationship_type: "owner".to_string(),
            source_column: "owner_id".to_string(),
            source_kind: "User".to_string(),
        };

        let sql = TransformEngine::build_edge_sql("Group", &edge);

        assert!(sql.contains("owner_id AS source_id"));
        assert!(sql.contains("'User' AS source_kind"));
        assert!(sql.contains("'owner' AS relationship_kind"));
        assert!(sql.contains("id AS target_id"));
        assert!(sql.contains("'Group' AS target_kind"));
        assert!(sql.contains("WHERE owner_id IS NOT NULL"));
    }

    #[test]
    fn test_build_source_query_generates_correct_watermark_filtering_for_table() {
        let node = create_test_node();
        let sql = TransformEngine::build_source_query(&node);

        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("FROM siphon_users"));
        assert!(sql.contains("{last_watermark:String}"));
        assert!(sql.contains("{watermark:String}"));
    }

    #[test]
    fn test_build_source_query_returns_custom_query_for_query_type() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Query {
            scope: EtlScope::Namespaced,
            source: "siphon_namespaces".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: Some("_siphon_deleted".to_string()),
            query: "SELECT * FROM custom_query WHERE x = 1".to_string(),
        });

        let sql = TransformEngine::build_source_query(&node);

        assert!(sql.is_some());
        assert_eq!(sql.unwrap(), "SELECT * FROM custom_query WHERE x = 1");
    }

    #[test]
    fn test_build_source_query_returns_none_for_no_etl() {
        let mut node = create_test_node();
        node.etl = None;

        let sql = TransformEngine::build_source_query(&node);
        assert!(sql.is_none());
    }

    #[test]
    fn test_build_transform_sql_infers_traversal_path_for_namespaced_table_entities() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Table {
            scope: EtlScope::Namespaced,
            source: "siphon_namespaces".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: None,
        });

        let sql = TransformEngine::build_transform_sql(&node);

        assert!(
            sql.contains("traversal_path"),
            "should include inferred traversal_path column for Table type"
        );
    }

    #[test]
    fn test_build_transform_sql_does_not_infer_traversal_path_for_query_entities() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Query {
            scope: EtlScope::Namespaced,
            source: "siphon_namespaces".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: None,
            query: "SELECT * FROM t WHERE {traversal_path:String} AND {last_watermark:String} AND {watermark:String}".to_string(),
        });

        let sql = TransformEngine::build_transform_sql(&node);

        assert!(
            !sql.contains("\n    traversal_path"),
            "should not infer traversal_path for Query type (user includes it in query)"
        );
    }

    #[test]
    fn test_build_transform_sql_does_not_infer_traversal_path_for_global_entities() {
        let node = create_test_node();
        let sql = TransformEngine::build_transform_sql(&node);

        assert!(
            !sql.contains("traversal_path"),
            "should not include traversal_path for global entities"
        );
    }

    #[test]
    fn test_build_transform_sql_infers_deleted_column_for_table_type() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Table {
            scope: EtlScope::Global,
            source: "siphon_users".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: Some("_siphon_deleted".to_string()),
        });

        let sql = TransformEngine::build_transform_sql(&node);

        assert!(
            sql.contains("_siphon_deleted AS deleted"),
            "should rename deleted column from ETL config for Table type"
        );
    }

    #[test]
    fn test_build_transform_sql_does_not_infer_deleted_for_query_type() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Query {
            scope: EtlScope::Namespaced,
            source: "siphon_namespaces".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: Some("_siphon_deleted".to_string()),
            query: "SELECT * FROM t WHERE {traversal_path:String} AND {last_watermark:String} AND {watermark:String}".to_string(),
        });

        let sql = TransformEngine::build_transform_sql(&node);

        assert!(
            !sql.contains("_siphon_deleted AS deleted"),
            "should not infer deleted column for Query type (user includes it in query)"
        );
    }

    #[test]
    fn test_build_transform_sql_infers_deleted_column_without_rename() {
        let mut node = create_test_node();
        node.etl = Some(EtlConfig::Table {
            scope: EtlScope::Global,
            source: "siphon_users".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: Some("deleted".to_string()),
        });

        let sql = TransformEngine::build_transform_sql(&node);

        assert!(sql.contains("deleted"), "should include deleted column");
        assert!(
            !sql.contains("deleted AS deleted"),
            "should not redundantly rename deleted column"
        );
    }
}
