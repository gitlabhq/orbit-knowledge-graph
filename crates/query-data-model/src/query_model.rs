use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use ontology::{EdgeVariantScope, FieldSelectivity, TraversalPathKind};

use crate::catalog::{Catalog, Model, StorageBackend};
use crate::implementations::relational::RelationalFacts;
use crate::{
    DataModelError, DenormalizedCatalog, EntityAuthConfig, EntityId, ForeignKey, GitLabPolicy,
    GraphCatalog, MaterializedJoin, OrbitQueryModel, PathColumn, PropertyId, PropertyRealization,
    RelationalMapping, RelationshipId, RelationshipVariantId, TraversalPathLookup, VariantRoute,
};

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
pub struct QueryModel<B: StorageBackend>
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

impl<B: StorageBackend> QueryModel<B>
where
    B::Store: QueryFacts,
{
    pub fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
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

    pub fn catalog(&self) -> &Catalog<B> {
        &self.catalog
    }

    fn home(&self, entity: EntityId) -> Option<(&crate::catalog::Store, &B::Store)> {
        self.catalog.stores().nth(self.homes[entity.index()]?)
    }

    fn facts_of(&self, property: PropertyId) -> Option<&B::Store> {
        let entity = self.graph_catalog().property(property).entity;
        self.home(entity).map(|(_, facts)| facts)
    }

    fn graph_catalog(&self) -> &GraphCatalog {
        self.catalog.graph()
    }

    fn table(&self, name: &str) -> Option<&crate::implementations::relational::TableFacts> {
        self.catalog
            .stores()
            .find_map(|(_, facts)| facts.table(name))
    }
}

impl<B: StorageBackend + std::fmt::Debug> RelationalMapping for QueryModel<B>
where
    B::Store: QueryFacts,
{
    fn supports_selective_latest_rows(&self) -> bool {
        B::Store::SELECTIVE_LATEST_ROWS
    }

    fn supports_deferred_node_loading(&self) -> bool {
        B::Store::DEFERRED_NODE_LOADING
    }

    fn variant_route(&self, variant: RelationshipVariantId) -> Option<&VariantRoute> {
        self.routes[variant.index()].as_ref()
    }

    fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.catalog
            .stores()
            .flat_map(|(_, facts)| facts.copies_of(table))
            .collect()
    }

    fn materialized_joins(&self) -> &[MaterializedJoin] {
        self.catalog
            .stores()
            .map(|(_, facts)| facts.joins())
            .find(|joins| !joins.is_empty())
            .unwrap_or_default()
    }

    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        let (store, _) = self.home(entity)?;
        Some(&store.source(entity)?.table)
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.home(entity)
            .and_then(|(store, _)| store.source(entity))
            .is_some_and(|source| source.path_column.is_some())
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        !self.trusted
            && self
                .catalog
                .model()
                .access()
                .entity(entity)
                .is_some_and(|access| access.scope == crate::catalog::Scope::Global)
    }

    fn entity_high_fan_out(&self, entity: EntityId) -> bool {
        self.home(entity)
            .is_some_and(|(_, facts)| facts.high_fan_out(entity))
    }

    fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        B::Store::REFERENCE_HINTS
            && !self
                .graph_catalog()
                .property(property)
                .references
                .is_empty()
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        &self.defaults[entity.index()]
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.realizations[property.index()].as_ref()
    }

    fn property_selectivity(&self, property: PropertyId) -> Option<FieldSelectivity> {
        Some(self.graph_catalog().property(property).selectivity)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        self.table(table)?.column_types.get(column).copied()
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.text_index(property))
    }

    fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.null_as_epoch(property))
    }

    fn property_unique(&self, property: PropertyId) -> bool {
        self.graph_catalog().property(property).unique
    }

    fn property_mutable(&self, property: PropertyId) -> bool {
        self.graph_catalog().property(property).mutable
    }

    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]> {
        self.table(table)?.path_columns.as_deref()
    }

    fn default_edge_table(&self) -> &str {
        &self.default_edge_table
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        match self.relationship_tables[relationship.index()].as_slice() {
            [table] => Some(table),
            _ => None,
        }
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
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

    fn foreign_key(
        &self,
        graph: &GraphCatalog,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey> {
        let mut keys = relationships.iter().map(|relationship| {
            let variant = graph.variant_id(*relationship, source, target)?;
            RelationalMapping::variant_route(self, variant)?.foreign_key
        });
        let first = keys.next()??;
        keys.all(|key| key == Some(first)).then_some(first)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.table(table)?.columns.as_ref()
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        Some(&self.table(table)?.sort_key)
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        static EMPTY: LazyLock<DenormalizedCatalog> = LazyLock::new(DenormalizedCatalog::default);
        self.catalog
            .stores()
            .next()
            .map_or(&EMPTY, |(_, facts)| facts.denormalized())
    }

    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.home(entity)?.1.path_lookup(entity, kind)
    }
}

impl<B: StorageBackend + std::fmt::Debug> GitLabPolicy for QueryModel<B>
where
    B::Store: QueryFacts,
{
    fn is_hidden(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).hidden
    }

    fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope> {
        if self.trusted {
            return None;
        }
        self.catalog.model().access().variant_scope(variant)
    }

    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        &self.anchor_foreign_keys
    }

    fn is_admin_only(&self, property: PropertyId) -> bool {
        !self.trusted && self.catalog.model().access().property(property).admin_only
    }

    fn is_filterable(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).filterable
    }

    fn is_like_allowed(&self, property: PropertyId) -> bool {
        self.catalog.model().access().property(property).like
    }

    fn entity_auth(&self) -> &HashMap<String, EntityAuthConfig> {
        &self.entity_auth
    }

    fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        let name = &self.graph_catalog().entity(entity).name;
        Some(&self.entity_auth.get(name)?.auth_id_column)
    }

    fn required_access_level(&self, entity: EntityId) -> Option<u32> {
        let name = &self.graph_catalog().entity(entity).name;
        Some(self.entity_auth.get(name)?.required_access_level)
    }
}

impl<B: StorageBackend + std::fmt::Debug> OrbitQueryModel for QueryModel<B>
where
    B::Store: QueryFacts,
{
    type BackendCatalog = Self;
    type AuthorizationCatalog = Self;

    fn graph(&self) -> &GraphCatalog {
        self.graph_catalog()
    }

    fn query_backend(&self) -> &Self {
        self
    }

    fn query_authorization(&self) -> &Self {
        self
    }
}
