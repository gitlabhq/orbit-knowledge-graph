use std::collections::BTreeSet;

use ontology::Ontology;

use crate::proto::{
    SchemaDomain, SchemaEdge, SchemaEdgeVariant, SchemaNode, SchemaNodeStyle, SchemaProperty,
    StructuredSchema,
};

pub(super) fn build_structured_schema(
    ontology: &Ontology,
    expand_nodes: &[String],
) -> StructuredSchema {
    let domains: Vec<SchemaDomain> = ontology
        .domains()
        .map(|domain| SchemaDomain {
            name: domain.name.clone(),
            description: domain.description.clone(),
            node_names: domain.node_names.clone(),
        })
        .collect();

    let nodes: Vec<SchemaNode> = ontology
        .nodes()
        .map(|node| {
            let mut schema = SchemaNode {
                name: node.name.clone(),
                domain: node.domain.clone(),
                description: node.description.clone(),
                primary_key: node
                    .primary_keys
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "id".to_string()),
                label_field: node.label.clone(),
                ..Default::default()
            };

            if expand_nodes
                .iter()
                .any(|name| name == "*" || name == &node.name)
            {
                schema.properties = node
                    .fields
                    .iter()
                    .map(|field| SchemaProperty {
                        name: field.name.clone(),
                        data_type: field.data_type.to_string(),
                        nullable: field.nullable,
                        enum_values: field
                            .enum_values
                            .as_ref()
                            .map(|values| values.values().cloned().collect())
                            .unwrap_or_default(),
                        description: field.description.clone().unwrap_or_default(),
                    })
                    .collect();
                schema.style = Some(SchemaNodeStyle {
                    size: node.style.size,
                    color: node.style.color.clone(),
                });
                (schema.outgoing_edges, schema.incoming_edges) =
                    get_node_edge_names(ontology, &node.name);
            }

            schema
        })
        .collect();

    let edges: Vec<SchemaEdge> = ontology
        .edge_names()
        .map(|name| SchemaEdge {
            name: name.to_string(),
            description: ontology
                .get_edge_description(name)
                .unwrap_or_default()
                .to_string(),
            variants: ontology
                .get_edge(name)
                .unwrap_or_default()
                .iter()
                .map(|edge| SchemaEdgeVariant {
                    source_type: edge.source_kind.clone(),
                    target_type: edge.target_kind.clone(),
                })
                .collect(),
        })
        .collect();

    StructuredSchema {
        schema_version: ontology.schema_version().to_string(),
        domains,
        nodes,
        edges,
    }
}

fn get_node_edge_names(ontology: &Ontology, node_name: &str) -> (Vec<String>, Vec<String>) {
    let mut outgoing = BTreeSet::new();
    let mut incoming = BTreeSet::new();

    for edge in ontology.edges() {
        if edge.source_kind == node_name {
            outgoing.insert(edge.relationship_kind.clone());
        }
        if edge.target_kind == node_name {
            incoming.insert(edge.relationship_kind.clone());
        }
    }

    (
        outgoing.into_iter().collect(),
        incoming.into_iter().collect(),
    )
}
