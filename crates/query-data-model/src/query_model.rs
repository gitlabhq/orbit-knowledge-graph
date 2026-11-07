use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use ontology::{EdgeVariantScope, FieldSelectivity, TraversalPathKind};

use crate::catalog::{Catalog, Model, StorageBackend};
use crate::implementations::relational::RelationalFacts;
use crate::{
    DataModelError, DenormalizedCatalog, Entity, EntityId, ForeignKey, GraphCatalog,
    MaterializedJoin, OrbitQueryModel, PathColumn, Property, PropertyId, PropertyRealization,
    RelationshipId, RelationshipRoute, RelationshipVariant, RelationshipVariantId,
    TraversalPathLookup, VariantRoute,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAuthConfig {
    pub resource_type: String,
    pub ability: String,
    pub auth_id_column: String,
    pub owner_entity: Option<String>,
    pub required_access_level: u32,
}

pub trait QueryFacts: RelationalFacts + std::fmt::Debug + Send + Sync {
    const SELECTIVE_LATEST_ROWS: bool;
    const DEFERRED_NODE_LOADING: bool;
    const REFERENCE_HINTS: bool;
    const ALL_PROPERTIES_BY_DEFAULT: bool;

    fn text_index(&self, property: PropertyId) -> bool;
    fn null_as_epoch(&self, property: PropertyId) -> bool;
    fn high_fan_out(&self, entity: EntityId) -> bool;
    fn denormalized(&self) -> &DenormalizedCatalog;
    fn path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup>;
    fn copies_of<'a>(&'a self, table: &str) -> Vec<&'a str>;
    fn joins(&self) -> &[MaterializedJoin];
}

#[derive(Debug)]
struct Bindings<B: StorageBackend>
where
    B::Store: QueryFacts,
{
    catalog: Catalog<B>,
    homes: Vec<Option<usize>>,
    routes: Vec<Option<VariantRoute>>,
    realizations: Vec<Option<PropertyRealization>>,
    defaults: Vec<Vec<PropertyId>>,
    relationship_tables: Vec<Vec<String>>,
    edge_tables: Vec<String>,
    default_edge_table: String,
    entity_auth: HashMap<String, EntityAuthConfig>,
    anchor_foreign_keys: HashMap<String, EntityId>,
    trusted: bool,
}

impl<B: StorageBackend> Bindings<B>
where
    B::Store: QueryFacts,
{
    fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        let catalog = model.physical::<B>(connection)?;
        let graph = catalog.graph();
        let stores: Vec<_> = catalog.stores().collect();
        let homes: Vec<Option<usize>> = graph
            .entities()
            .map(|entity| {
                stores
                    .iter()
                    .position(|(store, _)| store.source(entity.id).is_some())
            })
            .collect();
        let trusted = stores.iter().all(|(store, _)| store.trusted);
        let routes = graph
            .variants()
            .map(|variant| {
                let table = stores
                    .iter()
                    .find_map(|(_, facts)| facts.relational().edge_table(variant.id))?;
                let foreign_key = stores
                    .iter()
                    .find_map(|(store, _)| store.foreign_key(variant.id));
                Some(VariantRoute {
                    table: table.to_string(),
                    foreign_key,
                })
            })
            .collect::<Vec<_>>();
        let realizations = graph
            .properties()
            .map(|property| {
                let (store, _) = stores.get(homes[property.entity.index()]?)?;
                if store.excludes(property.id) {
                    return None;
                }
                if let Some(lookup) = model.lookup(property.id) {
                    return Some(PropertyRealization::Virtual(ontology::VirtualSource {
                        service: lookup.service.clone(),
                        lookup: lookup.operation.clone(),
                        disabled: lookup.disabled,
                        depends_on: lookup
                            .keys
                            .iter()
                            .map(|key| graph.property(*key).name.clone())
                            .collect(),
                        allowed_ops: lookup.operators.clone(),
                    }));
                }
                Some(PropertyRealization::Stored {
                    column: store.column(property.id)?.to_string(),
                })
            })
            .collect();
        let defaults = graph
            .entities()
            .map(|entity| {
                if homes[entity.id.index()].is_none() {
                    return Vec::new();
                }
                entity
                    .properties
                    .iter()
                    .filter(|property| {
                        B::Store::ALL_PROPERTIES_BY_DEFAULT || entity.defaults.contains(property)
                    })
                    .copied()
                    .collect()
            })
            .collect();
        let edge_table_names: Vec<&str> = stores
            .iter()
            .flat_map(|(_, facts)| {
                facts
                    .relational()
                    .edge_tables
                    .iter()
                    .map(|table| table.name.as_str())
            })
            .collect();
        let relationship_tables = graph
            .relationships()
            .map(|relationship| {
                if let [only] = edge_table_names.as_slice() {
                    return vec![only.to_string()];
                }
                relationship
                    .variants
                    .iter()
                    .filter_map(|variant| Some(routes[variant.index()].as_ref()?.table.clone()))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .collect::<Vec<Vec<String>>>();
        let edge_tables = relationship_tables
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let default_edge_table = stores
            .iter()
            .find_map(|(_, facts)| facts.relational().default_edge_table())
            .map(|table| table.name.clone())
            .unwrap_or_default();
        let access = model.access();
        let mut entity_auth = HashMap::new();
        let mut anchor_foreign_keys = HashMap::new();
        if !trusted {
            for entity in graph.entities() {
                let Some(redaction) = access
                    .entity(entity.id)
                    .and_then(|access| access.redaction.as_ref())
                else {
                    continue;
                };
                let role = access.entity(entity.id).and_then(|access| access.role);
                entity_auth.insert(
                    entity.name.clone(),
                    EntityAuthConfig {
                        resource_type: redaction.resource.clone(),
                        ability: redaction.ability.clone(),
                        auth_id_column: graph.property(redaction.id).name.clone(),
                        owner_entity: redaction
                            .owner
                            .map(|owner| graph.entity(owner).name.clone()),
                        required_access_level: role
                            .unwrap_or(ontology::RequiredRole::Reporter)
                            .as_access_level(),
                    },
                );
            }
            for variant in graph.variants() {
                let anchor =
                    access.variant_scope(variant.id) == Some(EdgeVariantScope::NamespaceAnchor);
                let key = routes[variant.id.index()]
                    .as_ref()
                    .and_then(|route| route.foreign_key);
                if let (true, Some(key)) = (anchor, key) {
                    anchor_foreign_keys
                        .entry(graph.property(key.property).name.clone())
                        .or_insert(variant.target);
                }
            }
        }
        Ok(Self {
            catalog,
            homes,
            routes,
            realizations,
            defaults,
            relationship_tables,
            edge_tables,
            default_edge_table,
            entity_auth,
            anchor_foreign_keys,
            trusted,
        })
    }

    pub(crate) fn home(&self, entity: EntityId) -> Option<(&crate::catalog::Store, &B::Store)> {
        self.catalog.stores().nth(self.homes[entity.index()]?)
    }

    pub(crate) fn facts_of(&self, property: PropertyId) -> Option<&B::Store> {
        let entity = self.graph_catalog().property(property).entity;
        self.home(entity).map(|(_, facts)| facts)
    }

    pub(crate) fn graph_catalog(&self) -> &GraphCatalog {
        self.catalog.graph()
    }

    pub(crate) fn table(
        &self,
        name: &str,
    ) -> Option<&crate::implementations::relational::TableFacts> {
        self.catalog
            .stores()
            .find_map(|(_, facts)| facts.table(name))
    }
}

impl<B: StorageBackend> Bindings<B>
where
    B::Store: QueryFacts,
{
    pub(crate) fn supports_selective_latest_rows(&self) -> bool {
        B::Store::SELECTIVE_LATEST_ROWS
    }

    pub(crate) fn supports_deferred_node_loading(&self) -> bool {
        B::Store::DEFERRED_NODE_LOADING
    }

    pub(crate) fn variant_route(&self, variant: RelationshipVariantId) -> Option<&VariantRoute> {
        self.routes[variant.index()].as_ref()
    }

    pub(crate) fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.catalog
            .stores()
            .flat_map(|(_, facts)| facts.copies_of(table))
            .collect()
    }

    pub(crate) fn materialized_joins(&self) -> &[MaterializedJoin] {
        self.catalog
            .stores()
            .map(|(_, facts)| facts.joins())
            .find(|joins| !joins.is_empty())
            .unwrap_or_default()
    }

    pub(crate) fn entity_table(&self, entity: EntityId) -> Option<&str> {
        let (store, _) = self.home(entity)?;
        Some(&store.source(entity)?.table)
    }

    pub(crate) fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.home(entity)
            .and_then(|(store, _)| store.source(entity))
            .is_some_and(|source| source.path_column.is_some())
    }

    pub(crate) fn entity_is_global(&self, entity: EntityId) -> bool {
        !self.trusted
            && self
                .catalog
                .model()
                .access()
                .entity(entity)
                .is_some_and(|access| access.scope == crate::catalog::Scope::Global)
    }

    pub(crate) fn entity_high_fan_out(&self, entity: EntityId) -> bool {
        self.home(entity)
            .is_some_and(|(_, facts)| facts.high_fan_out(entity))
    }

    pub(crate) fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        B::Store::REFERENCE_HINTS
            && !self
                .graph_catalog()
                .property(property)
                .references
                .is_empty()
    }

    pub(crate) fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        &self.defaults[entity.index()]
    }

    pub(crate) fn property_column(&self, property: PropertyId) -> Option<&str> {
        match self.property_realization(property)? {
            PropertyRealization::Stored { column } => Some(column),
            PropertyRealization::Virtual(_) => None,
        }
    }

    pub(crate) fn property_realization(
        &self,
        property: PropertyId,
    ) -> Option<&PropertyRealization> {
        self.realizations[property.index()].as_ref()
    }

    pub(crate) fn property_selectivity(&self, property: PropertyId) -> Option<FieldSelectivity> {
        Some(self.graph_catalog().property(property).selectivity)
    }

    pub(crate) fn table_column_type(
        &self,
        table: &str,
        column: &str,
    ) -> Option<ontology::DataType> {
        self.table(table)?.column_types.get(column).copied()
    }

    pub(crate) fn has_text_index(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.text_index(property))
    }

    pub(crate) fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.null_as_epoch(property))
    }

    pub(crate) fn property_unique(&self, property: PropertyId) -> bool {
        self.graph_catalog().property(property).unique
    }

    pub(crate) fn property_mutable(&self, property: PropertyId) -> bool {
        self.graph_catalog().property(property).mutable
    }

    pub(crate) fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]> {
        self.table(table)?.path_columns.as_deref()
    }

    pub(crate) fn default_edge_table(&self) -> &str {
        &self.default_edge_table
    }

    pub(crate) fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        match self.relationship_tables[relationship.index()].as_slice() {
            [table] => Some(table),
            _ => None,
        }
    }

    pub(crate) fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        if relationships.is_empty() {
            return self.edge_tables.clone();
        }
        relationships
            .iter()
            .flat_map(|relationship| {
                self.relationship_tables[relationship.index()]
                    .iter()
                    .cloned()
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn foreign_key(
        &self,
        graph: &GraphCatalog,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey> {
        let mut keys = relationships.iter().map(|relationship| {
            let variant = graph.variant_id(*relationship, source, target)?;
            self.variant_route(variant)?.foreign_key
        });
        let first = keys.next()??;
        keys.all(|key| key == Some(first)).then_some(first)
    }

    pub(crate) fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.table(table)?.columns.as_ref()
    }

    pub(crate) fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        Some(&self.table(table)?.sort_key)
    }

    pub(crate) fn denormalized(&self) -> &DenormalizedCatalog {
        static EMPTY: LazyLock<DenormalizedCatalog> = LazyLock::new(DenormalizedCatalog::default);
        self.catalog
            .stores()
            .next()
            .map_or(&EMPTY, |(_, facts)| facts.denormalized())
    }

    pub(crate) fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.home(entity)?.1.path_lookup(entity, kind)
    }
}

impl<B: StorageBackend> Bindings<B>
where
    B::Store: QueryFacts,
{
    pub(crate) fn is_hidden(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).hidden
    }

    pub(crate) fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope> {
        if self.trusted {
            return None;
        }
        self.catalog.model().access().variant_scope(variant)
    }

    pub(crate) fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        &self.anchor_foreign_keys
    }

    pub(crate) fn is_admin_only(&self, property: PropertyId) -> bool {
        !self.trusted && self.catalog.model().access().property(property).admin_only
    }

    pub(crate) fn is_filterable(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).filterable
    }

    pub(crate) fn is_like_allowed(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).like
    }

    pub(crate) fn entity_auth(&self) -> &HashMap<String, EntityAuthConfig> {
        &self.entity_auth
    }

    pub(crate) fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        let name = &self.graph_catalog().entity(entity).name;
        Some(&self.entity_auth.get(name)?.auth_id_column)
    }

    pub(crate) fn required_access_level(&self, entity: EntityId) -> Option<u32> {
        let name = &self.graph_catalog().entity(entity).name;
        Some(self.entity_auth.get(name)?.required_access_level)
    }
}

#[derive(Debug)]
pub struct QueryModel<B: StorageBackend>
where
    B::Store: QueryFacts,
{
    bindings: Bindings<B>,
}

impl<B: StorageBackend> QueryModel<B>
where
    B::Store: QueryFacts,
{
    pub fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        Ok(Self {
            bindings: Bindings::new(model, connection)?,
        })
    }

    pub fn catalog(&self) -> &Catalog<B> {
        &self.bindings.catalog
    }
}

impl<B: StorageBackend> OrbitQueryModel for QueryModel<B>
where
    B::Store: QueryFacts,
{
    fn graph(&self) -> &GraphCatalog {
        self.bindings.graph_catalog()
    }

    fn table_of(&self, entity: EntityId) -> Option<&str> {
        self.bindings.entity_table(entity)
    }

    fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        self.bindings.property_is_foreign_key(property)
    }

    fn required_access_level(&self, entity: EntityId) -> Option<u32> {
        self.bindings.required_access_level(entity)
    }

    fn materialized_joins(&self) -> &[MaterializedJoin] {
        self.bindings.materialized_joins()
    }

    fn property_is_hidden(&self, property: PropertyId) -> bool {
        self.bindings.is_hidden(property)
    }

    fn supports_deferred_node_loading(&self) -> bool {
        self.bindings.supports_deferred_node_loading()
    }

    fn supports_selective_latest_rows(&self) -> bool {
        self.bindings.supports_selective_latest_rows()
    }

    fn entity(&self, name: &str) -> Option<&Entity> {
        self.graph().entity_named(name)
    }

    fn property(&self, entity: &str, property: &str) -> Option<&Property> {
        self.graph().property_named(entity, property)
    }

    fn property_is_nullable(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| property.nullable)
    }

    fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.bindings.null_as_epoch(property)
    }

    fn null_as_epoch_properties(&self, entity: EntityId) -> HashMap<String, ontology::DataType> {
        self.graph()
            .entity(entity)
            .properties
            .iter()
            .filter(|property| self.null_as_epoch(**property))
            .map(|property| {
                let property = self.graph().property(*property);
                (property.name.clone(), property.data_type)
            })
            .collect()
    }

    fn property_for_entity_id(&self, entity: EntityId, property: &str) -> Option<&Property> {
        self.graph()
            .property_id(entity, property)
            .map(|property| self.graph().property(property))
    }

    fn entity_table(&self, entity: &str) -> Option<&str> {
        let entity = self.graph().entity_id(entity)?;
        self.bindings.entity_table(entity)
    }

    fn entity_has_traversal_path(&self, entity: &str) -> bool {
        self.graph()
            .entity_id(entity)
            .is_some_and(|entity| self.bindings.entity_has_traversal_path(entity))
    }

    fn entity_is_global(&self, entity: &str) -> bool {
        self.graph()
            .entity_id(entity)
            .is_some_and(|entity| self.bindings.entity_is_global(entity))
    }

    fn entity_high_fan_out(&self, entity: &str) -> bool {
        self.graph()
            .entity_id(entity)
            .is_some_and(|entity| self.bindings.entity_high_fan_out(entity))
    }

    fn property_column_named(&self, entity: &str, property: &str) -> Option<&str> {
        let property = self.property(entity, property)?;
        self.property_column(property.id)
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        self.bindings.property_column(property)
    }

    fn property_is_stored(&self, property: PropertyId) -> bool {
        self.property_column(property).is_some()
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.bindings.property_realization(property)
    }

    fn property_selectivity(&self, property: PropertyId) -> ontology::FieldSelectivity {
        self.bindings
            .property_selectivity(property)
            .unwrap_or_default()
    }

    fn property_is_virtual(&self, entity: EntityId, property: &str) -> bool {
        self.virtual_source_for_entity_id(entity, property)
            .is_some()
    }

    fn virtual_source_for_entity_id(
        &self,
        entity: EntityId,
        property: &str,
    ) -> Option<&ontology::VirtualSource> {
        let property = self.graph().property_id(entity, property)?;
        let PropertyRealization::Virtual(source) = self.property_realization(property)? else {
            return None;
        };
        Some(source)
    }

    fn virtual_source(&self, entity: &str, property: &str) -> Option<&ontology::VirtualSource> {
        let entity = self.graph().entity_id(entity)?;
        self.virtual_source_for_entity_id(entity, property)
    }

    fn relationship_exists(&self, relationship: &str) -> bool {
        self.graph().relationship_id(relationship).is_some()
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.bindings.default_properties(entity)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        self.bindings.table_column_type(table, column)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.bindings.table_columns(table)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.bindings.table_sort_key(table)
    }

    fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.bindings.equivalent_layouts(table)
    }

    fn in_sort_key(&self, table: &str, column: &str) -> bool {
        self.table_sort_key(table)
            .is_some_and(|key| key.iter().any(|sort_column| sort_column == column))
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.bindings.has_text_index(property)
    }

    fn property_unique(&self, property: PropertyId) -> bool {
        self.bindings.property_unique(property)
    }

    fn property_mutable(&self, property: PropertyId) -> bool {
        self.bindings.property_mutable(property)
    }

    fn admin_only(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.bindings.is_admin_only(property.id))
    }

    fn entity_auth(&self) -> &HashMap<String, crate::EntityAuthConfig> {
        self.bindings.entity_auth()
    }

    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        self.bindings.anchor_foreign_keys()
    }

    fn entity_minimum_access_level(&self, entity: &str) -> Option<u32> {
        let entity = self.graph().entity_id(entity)?;
        self.bindings.required_access_level(entity)
    }

    fn property_is_admin_only(&self, property: PropertyId) -> bool {
        self.bindings.is_admin_only(property)
    }

    fn property_is_filterable(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.bindings.is_filterable(property.id))
    }

    fn property_allows_like(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.bindings.is_like_allowed(property.id))
    }

    fn property_is_free_text(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| match property.data_type {
                ontology::DataType::Uuid => true,
                ontology::DataType::String => {
                    self.property_selectivity(property.id) != ontology::FieldSelectivity::Low
                }
                _ => false,
            })
    }

    fn entity_authorized_by_namespace(&self, entity: &str) -> bool {
        self.entity_auth()
            .get(entity)
            .is_some_and(|auth| matches!(auth.resource_type.as_str(), "project" | "group"))
    }

    fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        self.bindings.redaction_id_column(entity)
    }

    fn table_has_path_columns(&self, table: &str) -> bool {
        self.bindings
            .table_path_columns(table)
            .is_none_or(|columns| !columns.is_empty())
    }

    fn table_minimum_access_level(&self, table: &str) -> u32 {
        self.bindings
            .table_path_columns(table)
            .into_iter()
            .flatten()
            .filter_map(|column| column.entity)
            .filter_map(|entity| self.bindings.required_access_level(entity))
            .max()
            .unwrap_or(ontology::RequiredRole::Reporter.as_access_level())
    }

    fn table_path_columns(&self, table: &str) -> Option<&[crate::PathColumn]> {
        self.bindings.table_path_columns(table)
    }

    fn relationship_table(&self, relationship: &str) -> Option<&str> {
        let relationship = self.graph().relationship_id(relationship)?;
        self.bindings.relationship_table(relationship)
    }

    fn default_edge_table(&self) -> &str {
        self.bindings.default_edge_table()
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        self.bindings.denormalized()
    }

    fn relationship_tables(&self, relationships: &[String]) -> Vec<String> {
        let relationships: Vec<_> = relationships
            .iter()
            .filter_map(|relationship| self.graph().relationship_id(relationship))
            .collect();
        self.bindings.edge_tables(&relationships)
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        self.bindings.edge_tables(relationships)
    }

    fn relationship_tables_between(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Vec<String> {
        let graph = self.graph();
        let (Some(source), Some(target)) = (graph.entity_id(source), graph.entity_id(target))
        else {
            return Vec::new();
        };
        let any = relationships.is_empty() || relationships.iter().any(|kind| kind == "*");
        graph
            .hops(source, crate::Direction::Outgoing)
            .filter(|hop| {
                hop.to == target
                    && (any || relationships.contains(&graph.relationship(hop.relationship).name))
            })
            .filter_map(|hop| self.bindings.variant_route(hop.variant))
            .map(|route| route.table.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn redaction_id_column_named(&self, entity: &str) -> Option<&str> {
        let entity = self.graph().entity_id(entity)?;
        self.redaction_id_column(entity)
    }

    fn relationship_route(&self, relationship: &str) -> Option<RelationshipRoute<'_>> {
        let mut routes = self.relationship_routes(relationship);
        (routes.len() == 1).then(|| routes.remove(0))
    }

    fn variant_route(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<&VariantRoute> {
        let relationship = self.graph().relationship_id(relationship)?;
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        self.bindings
            .variant_route(self.graph().variant_id(relationship, source, target)?)
    }

    fn relationship_routes(&self, relationship: &str) -> Vec<RelationshipRoute<'_>> {
        let Some(relationship) = self.graph().relationship_id(relationship) else {
            return Vec::new();
        };
        let mut routes = std::collections::BTreeMap::<&str, Vec<&RelationshipVariant>>::new();
        for id in &self.graph().relationship(relationship).variants {
            if let Some(route) = self.bindings.variant_route(*id) {
                routes
                    .entry(&route.table)
                    .or_default()
                    .push(self.graph().variant(*id));
            }
        }
        routes
            .into_iter()
            .map(|(table, variants)| RelationshipRoute { table, variants })
            .collect()
    }

    fn foreign_key(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Option<ForeignKey> {
        let relationships: Vec<_> = relationships
            .iter()
            .filter_map(|relationship| self.graph().relationship_id(relationship))
            .collect();
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        self.bindings
            .foreign_key(self.graph(), &relationships, source, target)
    }

    fn variant_scope(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<ontology::EdgeVariantScope> {
        let relationship = self.graph().relationship_id(relationship)?;
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        let variant = self.graph().variant_id(relationship, source, target)?;
        self.bindings.variant_scope(variant)
    }

    fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: ontology::TraversalPathKind,
    ) -> Option<(&str, &str)> {
        let entity = self.graph().entity_id(entity)?;
        let lookup = self.bindings.traversal_path_lookup(entity, kind)?;
        let column = self.bindings.property_column(lookup.property)?;
        Some((&lookup.table, column))
    }
}
