use crate::storage::TableLayout;
pub mod storage;
use storage::DuckDbColumn;

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, PropertyId, PropertyRealization,
    QueryBackendCatalog, RelationshipId, TableId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
struct DuckDbEntityLayout {
    table: TableId,
    default_properties: Vec<PropertyId>,
    has_traversal_path: bool,
}

#[derive(Debug)]
pub struct DuckDbCatalog {
    edge_table: TableId,
    storage: crate::storage::StorageCatalog<DuckDbColumn>,
    entities: Vec<Option<DuckDbEntityLayout>>,
    property_facts: Vec<PropertyBackendFacts>,
    relationship_count: usize,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
    type ColumnStorage = DuckDbColumn;
    fn storage(&self) -> &crate::storage::StorageCatalog<DuckDbColumn> {
        &self.storage
    }
    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        let column = self
            .storage
            .column_ref(self.storage.table_id(table)?, column)?;
        self.storage.column(column).storage().query_type
    }
    fn table_path_scopable(&self, _table: &str) -> bool {
        false
    }
    fn table_path_columns(&self, table: &str) -> Option<&[crate::PathColumn]> {
        self.storage.table_id(table).map(|_| [].as_slice())
    }
    fn derive(ontology: &ontology::Ontology, graph: &GraphCatalog) -> Result<Self, DataModelError> {
        Self::from_ontology(ontology, graph)
    }

    fn entity_table_id(&self, entity: EntityId) -> Option<TableId> {
        self.entity(entity).map(|layout| layout.table)
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, _entity: EntityId) -> bool {
        false
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.entity(entity)
            .map(|layout| layout.default_properties.as_slice())
            .unwrap_or_default()
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.property_facts
            .get(property.index())
            .and_then(|facts| facts.realization.as_ref())
    }

    fn property_selectivity(&self, property: PropertyId) -> Option<ontology::FieldSelectivity> {
        self.property_facts
            .get(property.index())
            .map(|facts| facts.selectivity)
    }

    fn has_text_index(&self, _property: PropertyId) -> bool {
        false
    }

    fn default_edge_table_id(&self) -> TableId {
        self.edge_table
    }

    fn relationship_table_id(&self, relationship: RelationshipId) -> Option<TableId> {
        (relationship.index() < self.relationship_count).then_some(self.edge_table)
    }

    fn edge_tables(&self, _relationships: &[RelationshipId]) -> Vec<String> {
        vec![self.default_edge_table().to_string()]
    }

    fn foreign_key(
        &self,
        _graph: &GraphCatalog,
        _relationships: &[RelationshipId],
        _source: EntityId,
        _target: EntityId,
    ) -> Option<crate::ForeignKey> {
        None
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn traversal_path_lookup(
        &self,
        _entity: EntityId,
        _kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        None
    }
}

impl DuckDbCatalog {
    fn entity(&self, id: EntityId) -> Option<&DuckDbEntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }
}

impl DuckDbCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let edge_table = ontology
            .local_edge_table_name()
            .unwrap_or_else(|| ontology.edge_table());
        let mut tables = if ontology.local_entity_names().is_empty() {
            ontology
                .nodes()
                .map(|node| TableLayout::local_node(node, &[]))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            storage::local_tables(ontology)?
        };
        if !tables.iter().any(|table| table.name() == edge_table) {
            tables.push(TableLayout::local_edge(
                edge_table,
                ontology.local_edge_columns(),
            )?);
        }
        let storage = crate::storage::StorageCatalog::new(tables)?;
        let entities = graph
            .entities()
            .map(|entity| {
                let node = ontology.get_node(&entity.name).expect("catalog entity");
                let table = storage.table_id(&node.destination_table)?;
                Some(DuckDbEntityLayout {
                    table,
                    default_properties: entity.properties.clone(),
                    has_traversal_path: ontology
                        .local_entity_fields(&entity.name)
                        .unwrap_or_else(|| node.fields.iter().collect())
                        .iter()
                        .any(|field| field.name == ontology::TRAVERSAL_PATH_COLUMN),
                })
            })
            .collect();
        let edge_table = storage.resolve_table(edge_table)?;
        let property_facts = derive_property_backend_facts(ontology, graph, &storage, true)?;
        Ok(DuckDbCatalog {
            edge_table,
            storage,
            entities,
            property_facts,
            relationship_count: graph.relationships().count(),
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
