use std::collections::HashMap;

use super::{ClickHouseMapping, EntityLayout, TableLayout, layout::ClickHouse};
use crate::implementations::derive_property_backend_facts;
use crate::{
    DataModelError, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey,
    DenormalizedProperty, Endpoint, EntityId, ForeignKey, GraphCatalog, PathColumn,
    PropertyRealization, TraversalPathLookup,
};
use crate::{Mapping, Relational};

impl Mapping<Relational<ClickHouse>> for ClickHouseMapping {
    fn derive(
        graph: &GraphCatalog,
        storage: &Relational<ClickHouse>,
    ) -> Result<Self, DataModelError> {
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut property_facts = derive_property_backend_facts(&storage.metadata.entities, graph)?;
        let mut tables = HashMap::new();

        for node in &storage.metadata.entities {
            let entity_id =
                graph
                    .entity_id(&node.name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "entity",
                        name: node.name.clone(),
                    })?;
            let mut default_properties = Vec::new();
            for (name, _) in &node.properties {
                let property_id = graph.property_id(entity_id, name).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "property",
                        name: format!("{}.{}", node.name, name),
                    }
                })?;
                if node.default_properties.contains(name) {
                    default_properties.push(property_id);
                }
            }
            if let Some(property) =
                graph.property_id(entity_id, ontology::constants::DEFAULT_PRIMARY_KEY)
            {
                property_facts[property.index()]
                    .realization
                    .get_or_insert_with(|| PropertyRealization::Stored {
                        column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                    });
            }
            entities[entity_id.index()] = Some(EntityLayout {
                table: node.table.clone(),
                has_traversal_path: node.has_traversal_path,
                global: node.global,
                default_properties,
            });
            tables.insert(
                node.table.clone(),
                TableLayout::from_storage(
                    storage.table(&node.table).expect("derived node storage"),
                    Some(entity_id),
                    (!node.global)
                        .then(|| PathColumn {
                            name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                            entity: Some(entity_id),
                        })
                        .into_iter()
                        .collect(),
                    node.has_traversal_path
                        && !node.global
                        && storage
                            .table(&node.table)
                            .expect("derived node layout")
                            .sort_key
                            .first()
                            .map(String::as_str)
                            == Some(ontology::constants::TRAVERSAL_PATH_COLUMN),
                ),
            );
        }

        for table_name in storage
            .graph_tables()
            .iter()
            .map(|table| table.name.as_str())
            .filter(|name| {
                !storage
                    .metadata
                    .entities
                    .iter()
                    .any(|entity| entity.table == *name)
            })
        {
            tables.insert(
                table_name.to_string(),
                TableLayout::from_storage(
                    storage.table(table_name).expect("derived edge storage"),
                    None,
                    vec![PathColumn {
                        name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                        entity: None,
                    }],
                    false,
                ),
            );
        }

        let mut relationships = vec![None; graph.relationships().count()];
        let mut variants = vec![None; graph.variants().count()];
        for relationship in graph.relationships() {
            for edge in storage
                .edge_routes()
                .iter()
                .filter(|edge| edge.relationship == relationship.name)
            {
                if edge.source.is_empty() || edge.target.is_empty() {
                    continue;
                }
                let source = graph.entity_id(&edge.source).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "source entity",
                        name: edge.source.clone(),
                    }
                })?;
                let target = graph.entity_id(&edge.target).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "target entity",
                        name: edge.target.clone(),
                    }
                })?;
                let variant_id = graph
                    .variant_id(relationship.id, source, target)
                    .ok_or_else(|| {
                        DataModelError::Invalid(format!(
                            "missing variant {}({}->{})",
                            relationship.name, edge.source, edge.target
                        ))
                    })?;
                let foreign_key = edge.foreign_key.as_deref().and_then(|column| {
                    let (holder, property, referenced) = match graph.property_id(source, column) {
                        Some(property) => (Endpoint::Source, property, target),
                        None => (Endpoint::Target, graph.property_id(target, column)?, source),
                    };
                    Some(ForeignKey {
                        holder,
                        property,
                        referenced_key: graph
                            .property_id(referenced, ontology::constants::DEFAULT_PRIMARY_KEY)?,
                    })
                });
                variants[variant_id.index()] = foreign_key;
            }
            relationships[relationship.id.index()] = storage
                .relationship_tables(&relationship.name)
                .and_then(|tables| tables.first())
                .cloned();
        }

        let mut denormalized = HashMap::new();
        for property in &storage.metadata.denormalized {
            let entity = graph.entity_id(&property.node_kind).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "entity",
                    name: property.node_kind.clone(),
                }
            })?;
            let Some(property_id) = graph.property_id(entity, &property.property_name) else {
                continue;
            };
            let direction = match property.direction {
                ontology::DenormDirection::Source => DenormalizedDirection::Source,
                ontology::DenormDirection::Target => DenormalizedDirection::Target,
            };
            let Some(relationship) = graph.relationship_id(&property.relationship_kind) else {
                continue;
            };
            denormalized
                .entry(DenormalizedKey {
                    property: property_id,
                    direction,
                })
                .and_modify(|layout: &mut DenormalizedProperty| {
                    layout.relationships.push(relationship)
                })
                .or_insert_with(|| DenormalizedProperty {
                    edge_column: property.edge_column.clone(),
                    tag_key: property.tag_key.clone(),
                    relationships: vec![relationship],
                });
        }

        let mut traversal_path_lookups = HashMap::new();
        for lookup in &storage.metadata.traversal_path_lookups {
            let entity = graph.entity_id(&lookup.entity).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "entity",
                    name: lookup.entity.clone(),
                }
            })?;
            let property = graph
                .property_id(entity, &lookup.key_column)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", lookup.entity, lookup.key_column),
                })?;
            traversal_path_lookups.insert(
                (entity, lookup.kind),
                TraversalPathLookup {
                    table: lookup.source_table.clone(),
                    property,
                },
            );
        }

        for join in storage.joins() {
            let path_columns = join
                .sources
                .iter()
                .filter_map(|source| {
                    Some(PathColumn {
                        name: source.path_column.clone()?,
                        entity: entities.iter().enumerate().find_map(|(entity, layout)| {
                            (layout.as_ref()?.table == source.table).then_some(EntityId(entity))
                        }),
                    })
                })
                .collect();
            let mut layout = TableLayout::from_storage(
                storage.table(&join.table).expect("derived join storage"),
                None,
                path_columns,
                true,
            );
            layout.column_types.clear();
            tables.insert(join.table.clone(), layout);
        }

        Ok(ClickHouseMapping {
            default_edge_table: storage.metadata.default_edge_table.clone(),
            entities,
            relationships,
            variants,
            property_facts,
            tables,
            denormalized: DenormalizedCatalog::new(denormalized),
            traversal_path_lookups,
        })
    }
}
