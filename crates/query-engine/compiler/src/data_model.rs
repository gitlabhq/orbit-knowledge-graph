use std::collections::HashMap;
use std::sync::Arc;

use ontology::{DataType, EdgeVariantScope, TraversalPathKind};
use query_data_model::{
    ClickHouseDataModel, DataModelError, DuckDbDataModel, EntityId, PropertyId,
    RelationshipVariantId,
};

use crate::input::EntityAuthConfig;
use crate::passes::plan::PlanningModel;

pub trait QueryModel: PlanningModel + Send + Sync {
    fn ontology(&self) -> &ontology::Ontology;
    fn entity_available(&self, entity: EntityId) -> bool;
    fn default_properties(&self, entity: EntityId) -> &[PropertyId];
    fn property_column(&self, property: PropertyId) -> Option<&str>;
    fn table_column_type(&self, table: &str, column: &str) -> Option<DataType>;
    fn has_text_index(&self, property: PropertyId) -> bool;
}

pub trait AuthorizationModel: QueryModel {
    fn entity_auth(&self) -> HashMap<String, EntityAuthConfig>;
    fn is_admin_only(&self, property: PropertyId) -> bool;
    fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope>;
    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<(String, String)>;
    fn redaction_id_column(&self, entity: EntityId) -> &str;
}

pub trait SecurityModel: AuthorizationModel {
    fn table_path_scopable(&self, table: &str) -> bool;
    fn table_has_path_columns(&self, table: &str) -> bool;
    fn table_minimum_access_level(&self, table: &str) -> u32;
}

pub fn clickhouse(
    ontology: Arc<ontology::Ontology>,
) -> Result<Arc<ClickHouseDataModel>, DataModelError> {
    ClickHouseDataModel::derive(ontology).map(Arc::new)
}

pub fn duckdb(ontology: Arc<ontology::Ontology>) -> Result<Arc<DuckDbDataModel>, DataModelError> {
    DuckDbDataModel::derive(ontology).map(Arc::new)
}

impl QueryModel for ClickHouseDataModel {
    fn ontology(&self) -> &ontology::Ontology {
        self.ontology()
    }

    fn entity_available(&self, entity: EntityId) -> bool {
        self.backend().entity(entity).is_some()
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.backend()
            .entity(entity)
            .map(|layout| layout.default_properties.as_slice())
            .unwrap_or_default()
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        self.backend().property_column(property)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<DataType> {
        self.backend()
            .table(table)
            .and_then(|layout| layout.column_types.get(column).copied())
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.backend().has_text_index(property)
    }
}

impl AuthorizationModel for ClickHouseDataModel {
    fn entity_auth(&self) -> HashMap<String, EntityAuthConfig> {
        let graph = self.graph();
        self.authorization()
            .entities()
            .map(|(entity, policy)| {
                (
                    graph.entity(entity).name.clone(),
                    EntityAuthConfig {
                        resource_type: policy.resource_type.clone(),
                        ability: policy.ability.clone(),
                        auth_id_column: self
                            .property_column(policy.id_property)
                            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
                            .to_string(),
                        owner_entity: policy
                            .owner_entity
                            .map(|owner| graph.entity(owner).name.clone()),
                        required_access_level: policy.required_access_level,
                    },
                )
            })
            .collect()
    }

    fn is_admin_only(&self, property: PropertyId) -> bool {
        self.authorization().is_admin_only(property)
    }

    fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope> {
        self.authorization().variant_scope(variant)
    }

    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<(String, String)> {
        let lookup = self.backend().traversal_path_lookup(entity, kind)?;
        Some((
            lookup.table.clone(),
            self.property_column(lookup.property)?.to_string(),
        ))
    }

    fn redaction_id_column(&self, entity: EntityId) -> &str {
        self.authorization()
            .entity(entity)
            .and_then(|policy| self.property_column(policy.id_property))
            .unwrap_or(ontology::constants::DEFAULT_PRIMARY_KEY)
    }
}

impl SecurityModel for ClickHouseDataModel {
    fn table_path_scopable(&self, table: &str) -> bool {
        self.backend()
            .table(table)
            .is_some_and(|layout| layout.path_scopable)
    }

    fn table_has_path_columns(&self, table: &str) -> bool {
        self.backend()
            .table(table)
            .is_none_or(|layout| !layout.path_columns.is_empty())
    }

    fn table_minimum_access_level(&self, table: &str) -> u32 {
        self.backend()
            .table(table)
            .map(|layout| layout.minimum_access_level(self.authorization()))
            .unwrap_or(crate::types::DEFAULT_PATH_ACCESS_LEVEL)
    }
}

impl QueryModel for DuckDbDataModel {
    fn ontology(&self) -> &ontology::Ontology {
        self.ontology()
    }

    fn entity_available(&self, entity: EntityId) -> bool {
        self.backend().entity(entity).is_some()
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.graph().entity(entity).properties.as_slice()
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        let entity = self.graph().property(property).entity;
        self.backend()
            .entity(entity)
            .and_then(|layout| layout.properties.get(&property))
            .map(String::as_str)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<DataType> {
        if table != self.backend().edge_table() {
            return None;
        }
        self.backend().edge_column_type(column)
    }

    fn has_text_index(&self, _property: PropertyId) -> bool {
        false
    }
}
