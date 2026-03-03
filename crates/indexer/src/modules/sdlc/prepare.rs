use ontology::{
    DataType, EdgeDirection, EdgeEndpointType, EdgeSourceEtlConfig, EdgeTarget, EnumType,
    EtlConfig, EtlScope, Field, NodeEntity, Ontology,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Literal(String),
    Column(String),
}

impl SqlExpr {
    pub fn to_sql(&self) -> String {
        match self {
            SqlExpr::Literal(s) => format!("'{}'", s),
            SqlExpr::Column(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedField {
    pub expression: String,
}

impl PreparedField {
    pub fn from_field(field: &Field) -> Self {
        Self {
            expression: Self::build_expression(field),
        }
    }

    fn build_expression(field: &Field) -> String {
        if field.data_type == DataType::Enum
            && field.enum_type == EnumType::Int
            && let Some(ref values) = field.enum_values
        {
            let cases: Vec<String> = values
                .iter()
                .map(|(k, v)| format!("WHEN {} = {} THEN '{}'", field.source, k, v))
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
}

#[derive(Debug, Clone)]
pub struct PreparedEdge {
    pub fk_column: String,
    pub relationship_kind: String,
    pub source_id: String,
    pub source_kind: SqlExpr,
    pub target_id: String,
    pub target_kind: SqlExpr,
    pub type_filter: Option<String>,
    pub delimiter: Option<String>,
    pub namespaced: bool,
}

#[derive(Debug, Clone)]
pub struct PreparedEtlConfig {
    pub node_kind: String,
    pub destination_table: String,
    pub extract_query: String,
    pub fields: Vec<PreparedField>,
    pub edges: Vec<PreparedEdge>,
}

impl PreparedEtlConfig {
    pub fn from_node(node: &NodeEntity, ontology: &Ontology) -> Option<Self> {
        let etl = node.etl.as_ref()?;

        Some(Self {
            node_kind: node.name.clone(),
            destination_table: node.destination_table.clone(),
            extract_query: build_extract_query(node, etl)?,
            fields: node.fields.iter().map(PreparedField::from_field).collect(),
            edges: prepare_edges(node, etl, ontology),
        })
    }
}

/// Prepared ETL config for edges sourced from join tables.
///
/// Unlike `PreparedEtlConfig`, this only produces edges (no nodes).
/// Both endpoints are determined by columns in the join table.
#[derive(Debug, Clone)]
pub struct PreparedEdgeEtl {
    pub relationship_kind: String,
    pub extract_query: String,
    pub source_id: String,
    pub source_kind: SqlExpr,
    pub target_id: String,
    pub target_kind: SqlExpr,
    pub source_type_filter: Option<String>,
    pub namespaced: bool,
}

impl PreparedEdgeEtl {
    pub fn from_config(
        relationship_kind: &str,
        config: &EdgeSourceEtlConfig,
        ontology: &Ontology,
    ) -> Self {
        let extract_query = build_edge_extract_query(config);
        let (source_id, source_kind, source_type_filter) =
            prepare_endpoint(&config.from, relationship_kind, true, ontology);
        let (target_id, target_kind, _) =
            prepare_endpoint(&config.to, relationship_kind, false, ontology);

        Self {
            relationship_kind: relationship_kind.to_string(),
            extract_query,
            source_id,
            source_kind,
            target_id,
            target_kind,
            source_type_filter,
            namespaced: config.scope == EtlScope::Namespaced,
        }
    }
}

fn build_edge_extract_query(config: &EdgeSourceEtlConfig) -> String {
    let mut columns = vec![
        config.from.id_column.clone(),
        config.to.id_column.clone(),
        format!("{} AS _version", config.watermark),
        format!("{} AS _deleted", config.deleted),
    ];

    if let EdgeEndpointType::Column { column, .. } = &config.from.node_type
        && !columns.contains(column)
    {
        columns.push(column.clone());
    }
    if let EdgeEndpointType::Column { column, .. } = &config.to.node_type
        && !columns.contains(column)
    {
        columns.push(column.clone());
    }

    let namespace_filter = if config.scope == EtlScope::Namespaced {
        columns.push("traversal_path".to_string());
        " AND startsWith(traversal_path, {traversal_path:String})"
    } else {
        ""
    };

    format!(
        "SELECT {} FROM {} WHERE {} > {{last_watermark:String}} AND {} <= {{watermark:String}}{}",
        columns.join(", "),
        config.source,
        config.watermark,
        config.watermark,
        namespace_filter
    )
}

fn prepare_endpoint(
    endpoint: &ontology::EdgeEndpoint,
    relationship_kind: &str,
    is_source: bool,
    ontology: &Ontology,
) -> (String, SqlExpr, Option<String>) {
    let id_column = endpoint.id_column.clone();

    match &endpoint.node_type {
        EdgeEndpointType::Literal(node_type) => {
            (id_column, SqlExpr::Literal(node_type.clone()), None)
        }
        EdgeEndpointType::Column {
            column,
            type_mapping,
        } => {
            let allowed_types = if is_source {
                ontology.get_edge_source_types(relationship_kind)
            } else {
                ontology.get_edge_all_target_types(relationship_kind)
            };
            let type_filter = build_type_filter(column, &allowed_types);
            let sql_expr = build_type_column_expr(column, type_mapping);
            (id_column, sql_expr, type_filter)
        }
    }
}

fn build_type_column_expr(
    column: &str,
    type_mapping: &std::collections::BTreeMap<String, String>,
) -> SqlExpr {
    if type_mapping.is_empty() {
        return SqlExpr::Column(column.to_string());
    }

    let cases: Vec<String> = type_mapping
        .iter()
        .map(|(from, to)| format!("WHEN {} = '{}' THEN '{}'", column, from, to))
        .collect();
    SqlExpr::Column(format!("CASE {} ELSE {} END", cases.join(" "), column))
}

fn build_extract_query(node: &NodeEntity, etl: &EtlConfig) -> Option<String> {
    match etl {
        EtlConfig::Table {
            source,
            watermark,
            deleted,
            scope: _,
            edges,
            ..
        } => {
            let mut columns: Vec<String> = node
                .fields
                .iter()
                .map(|f| {
                    if f.data_type == DataType::Uuid {
                        format!("toString({}) AS {}", f.source, f.source)
                    } else {
                        f.source.clone()
                    }
                })
                .collect();

            for column in edges.keys() {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }

            columns.push(format!("{} AS _version", watermark));
            columns.push(format!("{} AS _deleted", deleted));

            Some(format!(
                "SELECT {} FROM {} WHERE {} > {{last_watermark:String}} AND {} <= {{watermark:String}}",
                columns.join(", "),
                source,
                watermark,
                watermark
            ))
        }
        EtlConfig::Query { query, .. } => Some(query.clone()),
    }
}

fn prepare_edges(node: &NodeEntity, etl: &EtlConfig, ontology: &Ontology) -> Vec<PreparedEdge> {
    let namespaced = etl.scope() == EtlScope::Namespaced;
    etl.edges()
        .iter()
        .map(|(fk_column, mapping)| {
            prepare_edge(
                &node.name,
                fk_column,
                &mapping.target,
                &mapping.relationship_kind,
                mapping.direction,
                mapping.delimiter.as_deref(),
                namespaced,
                ontology,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn prepare_edge(
    node_kind: &str,
    fk_column: &str,
    target: &EdgeTarget,
    relationship_kind: &str,
    direction: EdgeDirection,
    delimiter: Option<&str>,
    namespaced: bool,
    ontology: &Ontology,
) -> PreparedEdge {
    match target {
        EdgeTarget::Literal(target_type) => resolve_literal_edge(
            node_kind,
            fk_column,
            target_type,
            relationship_kind,
            direction,
            delimiter,
            namespaced,
        ),
        EdgeTarget::Column(type_column) => resolve_polymorphic_edge(
            node_kind,
            fk_column,
            type_column,
            relationship_kind,
            direction,
            delimiter,
            namespaced,
            ontology,
        ),
    }
}

fn resolve_literal_edge(
    node_kind: &str,
    fk_column: &str,
    target_type: &str,
    relationship_kind: &str,
    direction: EdgeDirection,
    delimiter: Option<&str>,
    namespaced: bool,
) -> PreparedEdge {
    let (source_id, source_kind, target_id, target_kind) = match direction {
        EdgeDirection::Outgoing => (
            "id".to_string(),
            SqlExpr::Literal(node_kind.to_string()),
            fk_column.to_string(),
            SqlExpr::Literal(target_type.to_string()),
        ),
        EdgeDirection::Incoming => (
            fk_column.to_string(),
            SqlExpr::Literal(target_type.to_string()),
            "id".to_string(),
            SqlExpr::Literal(node_kind.to_string()),
        ),
    };

    PreparedEdge {
        fk_column: fk_column.to_string(),
        relationship_kind: relationship_kind.to_string(),
        source_id,
        source_kind,
        target_id,
        target_kind,
        type_filter: None,
        delimiter: delimiter.map(String::from),
        namespaced,
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_polymorphic_edge(
    node_kind: &str,
    fk_column: &str,
    type_column: &str,
    relationship_kind: &str,
    direction: EdgeDirection,
    delimiter: Option<&str>,
    namespaced: bool,
    ontology: &Ontology,
) -> PreparedEdge {
    let allowed_types = ontology.get_edge_target_types(relationship_kind, node_kind, direction);
    let type_filter = build_type_filter(type_column, &allowed_types);

    let (source_id, source_kind, target_id, target_kind) = match direction {
        EdgeDirection::Outgoing => (
            "id".to_string(),
            SqlExpr::Literal(node_kind.to_string()),
            fk_column.to_string(),
            SqlExpr::Column(type_column.to_string()),
        ),
        EdgeDirection::Incoming => (
            fk_column.to_string(),
            SqlExpr::Column(type_column.to_string()),
            "id".to_string(),
            SqlExpr::Literal(node_kind.to_string()),
        ),
    };

    PreparedEdge {
        fk_column: fk_column.to_string(),
        relationship_kind: relationship_kind.to_string(),
        source_id,
        source_kind,
        target_id,
        target_kind,
        type_filter,
        delimiter: delimiter.map(String::from),
        namespaced,
    }
}

fn build_type_filter(type_column: &str, allowed_types: &[String]) -> Option<String> {
    if allowed_types.is_empty() {
        return None;
    }
    let types_list = allowed_types
        .iter()
        .map(|t| format!("'{}'", t))
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("{} IN ({})", type_column, types_list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{EdgeMapping, EtlConfig, EtlScope, constants::GL_TABLE_PREFIX};
    use std::collections::BTreeMap;

    #[test]
    fn prepared_field_simple() {
        let field = Field {
            name: "id".to_string(),
            source: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "id");
    }

    #[test]
    fn prepared_field_renamed() {
        let field = Field {
            name: "is_admin".to_string(),
            source: "admin".to_string(),
            data_type: DataType::Bool,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "admin AS is_admin");
    }

    #[test]
    fn prepared_field_int_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "inactive".to_string());

        let field = Field {
            name: "status".to_string(),
            source: "status".to_string(),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: Some(values),
            enum_type: EnumType::Int,
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(
            resolved.expression,
            "CASE WHEN status = 0 THEN 'active' WHEN status = 1 THEN 'inactive' ELSE 'unknown' END AS status"
        );
    }

    #[test]
    fn prepared_field_uuid() {
        let field = Field {
            name: "uuid".to_string(),
            source: "uuid".to_string(),
            data_type: DataType::Uuid,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "uuid");
    }

    #[test]
    fn prepared_field_uuid_renamed() {
        let field = Field {
            name: "finding_uuid".to_string(),
            source: "uuid".to_string(),
            data_type: DataType::Uuid,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "uuid AS finding_uuid");
    }

    #[test]
    fn prepared_field_string_enum() {
        let field = Field {
            name: "priority".to_string(),
            source: "priority".to_string(),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::String,
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "priority");
    }

    #[test]
    fn prepared_field_string_enum_renamed() {
        let field = Field {
            name: "priority".to_string(),
            source: "prio".to_string(),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::String,
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "prio AS priority");
    }

    #[test]
    fn prepared_edge_outgoing() {
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
            destination_table: format!("{GL_TABLE_PREFIX}group"),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_groups".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
            ..Default::default()
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let edge = &config.edges[0];

        assert_eq!(edge.source_id, "id");
        assert_eq!(edge.source_kind, SqlExpr::Literal("Group".to_string()));
        assert_eq!(edge.target_id, "owner_id");
        assert_eq!(edge.target_kind, SqlExpr::Literal("User".to_string()));
    }

    #[test]
    fn prepared_edge_incoming() {
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
            destination_table: format!("{GL_TABLE_PREFIX}note"),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_notes".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges,
            }),
            ..Default::default()
        };

        let config = PreparedEtlConfig::from_node(&node, &ontology).unwrap();
        let edge = &config.edges[0];

        assert_eq!(edge.source_id, "author_id");
        assert_eq!(edge.source_kind, SqlExpr::Literal("User".to_string()));
        assert_eq!(edge.target_id, "id");
        assert_eq!(edge.target_kind, SqlExpr::Literal("Note".to_string()));
    }

    #[test]
    fn sql_expr_literal() {
        assert_eq!(SqlExpr::Literal("User".to_string()).to_sql(), "'User'");
    }

    #[test]
    fn sql_expr_column() {
        assert_eq!(SqlExpr::Column("type".to_string()).to_sql(), "type");
    }

    #[test]
    fn build_type_column_expr_no_mapping() {
        let type_mapping = BTreeMap::new();
        let expr = build_type_column_expr("source_type", &type_mapping);
        assert_eq!(expr, SqlExpr::Column("source_type".to_string()));
    }

    #[test]
    fn build_type_column_expr_with_mapping() {
        let mut type_mapping = BTreeMap::new();
        type_mapping.insert("Namespace".to_string(), "Group".to_string());
        type_mapping.insert("Project".to_string(), "Project".to_string());

        let expr = build_type_column_expr("source_type", &type_mapping);

        let sql = expr.to_sql();
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN source_type = 'Namespace' THEN 'Group'"));
        assert!(sql.contains("WHEN source_type = 'Project' THEN 'Project'"));
        assert!(sql.contains("ELSE source_type END"));
    }

    #[test]
    fn prepared_edge_etl_with_type_mapping() {
        use ontology::{EdgeEndpoint, EdgeEndpointType, EdgeSourceEtlConfig};

        let mut type_mapping = BTreeMap::new();
        type_mapping.insert("Namespace".to_string(), "Group".to_string());
        type_mapping.insert("Project".to_string(), "Project".to_string());

        let config = EdgeSourceEtlConfig {
            scope: EtlScope::Namespaced,
            source: "siphon_members".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            from: EdgeEndpoint {
                id_column: "user_id".to_string(),
                node_type: EdgeEndpointType::Literal("User".to_string()),
            },
            to: EdgeEndpoint {
                id_column: "source_id".to_string(),
                node_type: EdgeEndpointType::Column {
                    column: "source_type".to_string(),
                    type_mapping,
                },
            },
        };

        let ontology = Ontology::new()
            .with_nodes(["User", "Group", "Project"])
            .with_edges(["MEMBER_OF"]);

        let prepared = PreparedEdgeEtl::from_config("MEMBER_OF", &config, &ontology);

        assert_eq!(prepared.source_kind, SqlExpr::Literal("User".to_string()));

        let target_sql = prepared.target_kind.to_sql();
        assert!(target_sql.contains("CASE"));
        assert!(target_sql.contains("WHEN source_type = 'Namespace' THEN 'Group'"));
    }
}
