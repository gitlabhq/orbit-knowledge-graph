use std::collections::{BTreeMap, BTreeSet};

use crate::{EdgeVariantScope, TraversalPathKind};

use super::{Column, RelationalFacts, Table};
use crate::catalog::{Catalog, Lookup, Redaction, StorageBackend};
use crate::implementations::clickhouse::backend::IndexKind;
use crate::{
    DenormalizedKey, DenormalizedProperty, Endpoint, EntityId, ForeignKey, MaterializedJoin,
    PropertyId, RelationshipId, RelationshipVariant, RelationshipVariantId, TraversalPathLookup,
};

/// One edge table of a relationship, with the variants it stores.
#[derive(Debug)]
pub struct RelationshipRoute<'a> {
    pub table: &'a str,
    variants: Vec<&'a RelationshipVariant>,
}

impl RelationshipRoute<'_> {
    pub fn variants(&self) -> impl Iterator<Item = &RelationshipVariant> + '_ {
        self.variants.iter().copied()
    }

    pub fn source_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.source)
    }

    pub fn target_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.target)
    }

    pub fn has_source(&self, entity: EntityId) -> bool {
        self.source_entities().any(|source| source == entity)
    }

    pub fn has_target(&self, entity: EntityId) -> bool {
        self.target_entities().any(|target| target == entity)
    }
}

impl<B: StorageBackend> Catalog<B>
where
    B::Store: RelationalFacts,
{
    pub fn table_of(&self, entity: EntityId) -> Option<&str> {
        self.source(entity)?.1.table.as_deref()
    }

    pub fn has_path_column(&self, entity: EntityId) -> bool {
        self.source(entity)
            .is_some_and(|(_, source)| source.path_column.is_some())
    }

    /// The column that stores a property; none when a lookup serves it or its store excludes it.
    pub fn column(&self, property: PropertyId) -> Option<&str> {
        let (store, _) = self.home(self.graph().property(property).entity)?;
        if store.excludes(property) || self.model().lookup(property).is_some() {
            return None;
        }
        store.column(property)
    }

    /// The lookup that serves a property its entity's store holds.
    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        let (store, _) = self.home(self.graph().property(property).entity)?;
        if store.excludes(property) {
            return None;
        }
        self.model().lookup(property)
    }

    pub fn readable(&self, property: PropertyId) -> bool {
        self.column(property).is_some() || self.lookup(property).is_some()
    }

    /// Every store on the connection is trusted, so no read is scoped or redacted.
    pub fn trusted(&self) -> bool {
        self.stores().all(|(store, _)| store.trusted)
    }

    pub fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope> {
        match self.trusted() {
            true => None,
            false => self.model().access().variant_scope(variant),
        }
    }

    pub fn admin_only(&self, property: PropertyId) -> bool {
        !self.trusted() && self.model().access().property(property).admin_only
    }

    pub fn redaction(&self, entity: EntityId) -> Option<&Redaction> {
        match self.trusted() {
            true => None,
            false => self.model().access().entity(entity)?.redaction.as_ref(),
        }
    }

    /// Each foreign key of a namespace-anchor variant or an entity's declared anchor, with the
    /// entity it anchors to; a declared anchor counts only when that entity is stored here.
    pub fn anchor_keys(&self) -> impl Iterator<Item = (PropertyId, EntityId)> + '_ {
        let graph = self.graph();
        let variants = graph
            .variants()
            .filter(|variant| {
                self.variant_scope(variant.id) == Some(EdgeVariantScope::NamespaceAnchor)
            })
            .filter_map(|variant| {
                let key = self
                    .routes(variant.id)
                    .find_map(|route| route.foreign_key)?;
                Some((key.key.property, variant.target))
            });
        let declared = graph.entities().filter_map(|entity| {
            let anchor = self.model().access().entity(entity.id)?.anchor?;
            self.table_of(anchor.entity)
                .map(|_| (anchor.property, anchor.entity))
        });
        variants.chain(declared)
    }

    pub fn edge_table(&self, variant: RelationshipVariantId) -> Option<&str> {
        self.routes(variant)
            .find_map(|route| Some(route.edge?.table))
    }

    pub fn foreign_key(&self, variant: RelationshipVariantId) -> Option<ForeignKey> {
        self.routes(variant)
            .find_map(|route| Some(route.foreign_key?.key))
    }

    /// The foreign key every variant of `relationships` from `source` to `target` shares.
    pub fn shared_foreign_key(
        &self,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey> {
        let graph = self.graph();
        let mut keys = relationships.iter().map(|relationship| {
            self.foreign_key(graph.variant_id(*relationship, source, target)?)
        });
        let first = keys.next()??;
        keys.all(|key| key == Some(first)).then_some(first)
    }

    /// The edge tables of a relationship, each with the variants it stores.
    pub fn edge_routes(&self, relationship: RelationshipId) -> Vec<RelationshipRoute<'_>> {
        let graph = self.graph();
        let mut routes = BTreeMap::<&str, Vec<&RelationshipVariant>>::new();
        for variant in &graph.relationship(relationship).variants {
            if let Some(table) = self.edge_table(*variant) {
                routes
                    .entry(table)
                    .or_default()
                    .push(graph.variant(*variant));
            }
        }
        routes
            .into_iter()
            .map(|(table, variants)| RelationshipRoute { table, variants })
            .collect()
    }

    /// The edge tables storing any variant of `relationships`; of every relationship when none is named.
    pub fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        let graph = self.graph();
        let all: Vec<RelationshipId>;
        let relationships = match relationships {
            [] => {
                all = graph
                    .relationships()
                    .map(|relationship| relationship.id)
                    .collect();
                &all
            }
            named => named,
        };
        relationships
            .iter()
            .flat_map(|relationship| &graph.relationship(*relationship).variants)
            .filter_map(|variant| self.edge_table(*variant))
            .map(str::to_string)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The edge table that many packages' variants share; a migration that rewrites any of
    /// its rows rebuilds every table.
    pub fn shared_edge_table(&self) -> Option<&str> {
        self.stores()
            .find_map(|(_, facts)| facts.relational().shared_edge_table.as_deref())
    }

    /// The edge column of a variant that copies `property` of its `endpoint`.
    pub fn copy(
        &self,
        variant: RelationshipVariantId,
        endpoint: Endpoint,
        property: PropertyId,
    ) -> Option<&str> {
        self.stores()
            .find_map(|(store, _)| store.copy(variant, endpoint, property))
    }

    /// A table any store of the connection holds.
    pub fn physical_table(&self, name: &str) -> Option<&Table> {
        self.stores()
            .find_map(|(_, facts)| facts.relational().table(name))
    }

    /// The copies of a table that store the same rows in another sort order.
    pub fn copies_of(&self, table: &str) -> Vec<&str> {
        self.stores()
            .filter_map(|(_, facts)| facts.read_paths())
            .flat_map(|paths| &paths.copies)
            .filter(|(_, source)| source == table)
            .map(|(copy, _)| copy.as_str())
            .collect()
    }

    pub fn joins(&self) -> impl Iterator<Item = &MaterializedJoin> {
        self.stores()
            .filter_map(|(_, facts)| facts.read_paths())
            .flat_map(|paths| &paths.joins)
    }

    pub fn denormalized(&self, key: DenormalizedKey) -> Option<&DenormalizedProperty> {
        self.stores()
            .find_map(|(_, facts)| facts.read_paths()?.denormalized.property(key))
    }

    /// The home table of a property's entity and the column there that stores it.
    fn stored_column(&self, property: PropertyId) -> Option<(&Table, &Column)> {
        let entity = self.graph().property(property).entity;
        let (store, facts) = self.home(entity)?;
        let table = facts
            .relational()
            .table(store.source(entity)?.table.as_deref()?)?;
        Some((table, table.column(store.column(property)?)?))
    }

    pub fn text_index(&self, property: PropertyId) -> bool {
        self.stored_column(property).is_some_and(|(table, column)| {
            table.indexes.iter().any(|index| {
                index.column == column.name
                    && IndexKind::parse(&index.kind) == Some(IndexKind::Text)
            })
        })
    }

    pub fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.stored_column(property)
            .is_some_and(|(_, column)| column.null_as_epoch)
    }

    pub fn high_fan_out(&self, entity: EntityId) -> bool {
        self.home(entity)
            .and_then(|(_, facts)| facts.read_paths())
            .is_some_and(|paths| paths.high_fan_out.contains(&entity))
    }

    pub fn path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.home(entity)?
            .1
            .read_paths()?
            .path_lookups
            .get(&(entity, kind))
    }
}
