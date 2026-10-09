use crate::{
    DataModelError, GraphCatalog, MaterializedJoin, MaterializedNode, MaterializedRelationship,
};

pub(super) fn derive(
    join: &super::super::storage::MaterializedJoin,
    graph: &GraphCatalog,
) -> Result<MaterializedJoin, DataModelError> {
    let nodes =
        join.nodes
            .iter()
            .map(|node| {
                let entity = graph.entity_id(&node.entity).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "materialized entity",
                        name: node.entity.clone(),
                    }
                })?;
                Ok(MaterializedNode {
                    entity,
                    source_occurrence: node.source_occurrence,
                    identity_column: node.identity_column.clone(),
                    properties: graph
                        .entity(entity)
                        .properties
                        .iter()
                        .filter_map(|property| {
                            join.sources[node.source_occurrence]
                                .columns
                                .get(&graph.property(*property).name)
                                .map(|column| (*property, column.clone()))
                        })
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, DataModelError>>()?;
    let relationships = join
        .relationships
        .iter()
        .map(|relationship| {
            let variant = graph
                .variant_named(
                    &relationship.kind,
                    &join.nodes[relationship.source_slot].entity,
                    &join.nodes[relationship.target_slot].entity,
                )
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "materialized variant",
                    name: relationship.kind.clone(),
                })?
                .id;
            Ok(MaterializedRelationship {
                variant,
                source_slot: relationship.source_slot,
                target_slot: relationship.target_slot,
                edge_occurrence: relationship.edge_occurrence,
                source_id_column: nodes[relationship.source_slot].identity_column.clone(),
                target_id_column: nodes[relationship.target_slot].identity_column.clone(),
                columns: relationship
                    .edge_occurrence
                    .map(|occurrence| join.sources[occurrence].columns.clone())
                    .unwrap_or_default(),
            })
        })
        .collect::<Result<_, DataModelError>>()?;
    Ok(MaterializedJoin {
        table: join.table.clone(),
        nodes,
        relationships,
        sources: join
            .sources
            .iter()
            .map(|source| source.table.clone())
            .collect(),
    })
}
