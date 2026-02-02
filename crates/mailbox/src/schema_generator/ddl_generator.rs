//! ClickHouse DDL generation for plugin tables.

use crate::types::{NodeDefinition, Plugin};

pub fn generate_create_table_ddl(plugin: &Plugin, node_definition: &NodeDefinition) -> String {
    let table_name = plugin.table_name_for_node(&node_definition.name);
    let columns = generate_column_definitions(node_definition);

    format!(
        r#"CREATE TABLE IF NOT EXISTS {table_name} (
    id Int64,
    traversal_path String,
{columns},
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id)"#
    )
}

pub fn generate_alter_table_ddl(
    plugin: &Plugin,
    node_definition: &NodeDefinition,
    new_columns: &[String],
) -> Vec<String> {
    let table_name = plugin.table_name_for_node(&node_definition.name);

    new_columns
        .iter()
        .filter_map(|column_name| {
            node_definition
                .properties
                .iter()
                .find(|p| &p.name == column_name)
                .map(|property| {
                    let clickhouse_type = property.property_type.to_clickhouse_type(property.nullable);
                    format!(
                        "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {clickhouse_type}"
                    )
                })
        })
        .collect()
}

fn generate_column_definitions(node_definition: &NodeDefinition) -> String {
    node_definition
        .properties
        .iter()
        .map(|property| {
            let clickhouse_type = property.property_type.to_clickhouse_type(property.nullable);
            format!("    {} {}", property.name, clickhouse_type)
        })
        .collect::<Vec<_>>()
        .join(",\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PluginSchema, PropertyDefinition, PropertyType};

    fn test_plugin() -> Plugin {
        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("security_scanner_Vulnerability")
                .with_property(PropertyDefinition::new("score", PropertyType::Float))
                .with_property(PropertyDefinition::new("cve_id", PropertyType::String).nullable())
                .with_property(
                    PropertyDefinition::new("severity", PropertyType::Enum)
                        .with_enum_values(vec!["low".into(), "high".into()]),
                ),
        );

        Plugin::new("security-scanner", 42, "hash", schema)
    }

    #[test]
    fn generates_create_table_ddl() {
        let plugin = test_plugin();
        let node = plugin
            .schema
            .get_node("security_scanner_Vulnerability")
            .unwrap();

        let ddl = generate_create_table_ddl(&plugin, node);

        assert!(ddl.contains(
            "CREATE TABLE IF NOT EXISTS gl_plugin_security_scanner_security_scanner_vulnerability"
        ));
        assert!(ddl.contains("id Int64"));
        assert!(ddl.contains("traversal_path String"));
        assert!(ddl.contains("score Float64"));
        assert!(ddl.contains("cve_id Nullable(String)"));
        assert!(ddl.contains("severity String"));
        assert!(ddl.contains("_version DateTime64(6, 'UTC')"));
        assert!(ddl.contains("_deleted Bool"));
        assert!(ddl.contains("ReplacingMergeTree(_version, _deleted)"));
        assert!(ddl.contains("ORDER BY (traversal_path, id)"));
    }

    #[test]
    fn generates_alter_table_ddl() {
        let plugin = test_plugin();
        let node = plugin
            .schema
            .get_node("security_scanner_Vulnerability")
            .unwrap();

        let alter_statements = generate_alter_table_ddl(&plugin, node, &["cve_id".into()]);

        assert_eq!(alter_statements.len(), 1);
        assert!(alter_statements[0].contains("ALTER TABLE"));
        assert!(alter_statements[0].contains("ADD COLUMN IF NOT EXISTS cve_id Nullable(String)"));
    }

    #[test]
    fn generates_multiple_alter_statements() {
        let plugin = test_plugin();
        let node = plugin
            .schema
            .get_node("security_scanner_Vulnerability")
            .unwrap();

        let alter_statements =
            generate_alter_table_ddl(&plugin, node, &["score".into(), "severity".into()]);

        assert_eq!(alter_statements.len(), 2);
    }
}
