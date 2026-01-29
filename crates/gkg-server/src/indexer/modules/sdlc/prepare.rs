use ontology::{
    DataType, EdgeDirection, EdgeTarget, EtlConfig, EtlScope, Field, NodeEntity, Ontology,
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
}

#[derive(Debug, Clone)]
pub struct PreparedEtlConfig {
    pub node_kind: String,
    pub destination_table: String,
    pub extract_query: String,
    pub fields: Vec<PreparedField>,
    pub edges: Vec<PreparedEdge>,
    pub is_namespaced: bool,
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
            is_namespaced: etl.scope() == EtlScope::Namespaced,
        })
    }
}

fn build_extract_query(node: &NodeEntity, etl: &EtlConfig) -> Option<String> {
    match etl {
        EtlConfig::Table {
            source,
            watermark,
            deleted,
            scope,
            edges,
            ..
        } => {
            let mut columns: Vec<String> = node.fields.iter().map(|f| f.source.clone()).collect();

            for column in edges.keys() {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }

            if *scope == EtlScope::Namespaced {
                columns.push("traversal_path".to_string());
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
    etl.edges()
        .iter()
        .map(|(fk_column, mapping)| {
            prepare_edge(
                &node.name,
                fk_column,
                &mapping.target,
                &mapping.relationship_kind,
                mapping.direction,
                ontology,
            )
        })
        .collect()
}

fn prepare_edge(
    node_kind: &str,
    fk_column: &str,
    target: &EdgeTarget,
    relationship_kind: &str,
    direction: EdgeDirection,
    ontology: &Ontology,
) -> PreparedEdge {
    match target {
        EdgeTarget::Literal(target_type) => resolve_literal_edge(
            node_kind,
            fk_column,
            target_type,
            relationship_kind,
            direction,
        ),
        EdgeTarget::Column(type_column) => resolve_polymorphic_edge(
            node_kind,
            fk_column,
            type_column,
            relationship_kind,
            direction,
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
    }
}

fn resolve_polymorphic_edge(
    node_kind: &str,
    fk_column: &str,
    type_column: &str,
    relationship_kind: &str,
    direction: EdgeDirection,
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
    use ontology::{EdgeMapping, EtlConfig, EtlScope};
    use std::collections::BTreeMap;

    #[test]
    fn prepared_field_simple() {
        let field = Field {
            name: "id".to_string(),
            source: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            enum_values: None,
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
        };
        let resolved = PreparedField::from_field(&field);
        assert_eq!(resolved.expression, "admin AS is_admin");
    }

    #[test]
    fn prepared_field_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "inactive".to_string());

        let field = Field {
            name: "status".to_string(),
            source: "status".to_string(),
            data_type: DataType::Enum,
            nullable: false,
            enum_values: Some(values),
        };
        let resolved = PreparedField::from_field(&field);
        assert!(resolved.expression.contains("CASE"));
        assert!(
            resolved
                .expression
                .contains("WHEN status = 0 THEN 'active'")
        );
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
}
