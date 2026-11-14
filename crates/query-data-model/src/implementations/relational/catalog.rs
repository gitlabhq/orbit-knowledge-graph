use std::collections::{BTreeMap, BTreeSet};

use crate::TraversalPathKind;

use super::{Column, ColumnId, EdgeColumns, RelationalFacts, Table, TableId, Target};
use crate::EdgeField;
use crate::catalog::{Catalog, Check, EdgeScope, Lookup, StorageBackend};
use crate::implementations::clickhouse::backend::IndexKind;
use crate::{
    DenormalizedKey, DenormalizedProperty, Endpoint, EntityId, ForeignKey, MaterializedJoin,
    PropertyId, RelationshipId, RelationshipVariant, RelationshipVariantId, TraversalPathLookup,
};

/// One edge table of a relationship, with the variants it stores.
#[derive(Debug)]
pub struct RelationshipRoute<'a> {
    pub table: &'a str,
    pub id: TableId,
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
    /// The column that holds the entity's identity in its home store on this connection.
    pub fn identity_column(&self, entity: EntityId) -> Option<&str> {
        match self.source(entity)?.1.key.as_slice() {
            [key] => Some(key.as_str()),
            _ => None,
        }
    }

    /// The sort key of the entity's table in its home store on this connection.
    pub fn sort_key(&self, entity: EntityId) -> Option<&[String]> {
        self.home(entity)?.1.relational().sort_key(entity)
    }

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

    /// The lookup that serves a property of an entity whose complete store is on this connection;
    /// a partial store holds only the columns it lists.
    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        let entity = self.graph().property(property).entity;
        let (store, _) = self.home(entity)?;
        if store.excludes(property) || !store.complete(entity) {
            return None;
        }
        self.model().lookup(property)
    }

    pub fn readable(&self, property: PropertyId) -> bool {
        self.column(property).is_some() || self.lookup(property).is_some()
    }

    /// Every store on the connection is trusted, so no read is scoped or checked.
    pub fn trusted(&self) -> bool {
        self.stores().all(|(store, _)| store.trusted)
    }

    pub fn admin_only(&self, property: PropertyId) -> bool {
        !self.trusted() && self.model().access().property(property).admin_only
    }

    /// The checks the entity's returned rows go through; none on a trusted connection.
    pub fn checks(&self, entity: EntityId) -> impl Iterator<Item = &Check> {
        let trusted = self.trusted();
        self.model()
            .checks(entity, self.connection())
            .filter(move |_| !trusted)
    }

    /// Each foreign key of a variant whose target holds its source's scope, with that target.
    pub fn anchor_keys(&self) -> impl Iterator<Item = (PropertyId, EntityId)> + '_ {
        self.graph()
            .variants()
            .filter(|variant| self.edge_scope(variant.id) == Some(EdgeScope::Holder))
            .filter_map(|variant| {
                let key = self
                    .routes(variant.id)
                    .find_map(|route| route.foreign_key)?;
                Some((key.key.property, variant.target))
            })
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
            .filter_map(|(table, variants)| {
                let id = self.table_named(table)?;
                Some(RelationshipRoute {
                    table,
                    id,
                    variants,
                })
            })
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

    pub fn copied_property(
        &self,
        variant: RelationshipVariantId,
        endpoint: Endpoint,
        column: &str,
    ) -> Option<PropertyId> {
        self.stores().find_map(|(store, _)| {
            let sources = store.binding(variant)?.copies.get(column)?;
            sources
                .iter()
                .find(|(side, _)| *side == endpoint)
                .map(|(_, property)| *property)
        })
    }

    /// A table any store of the connection holds.
    pub fn physical_table(&self, name: &str) -> Option<&Table> {
        self.stores()
            .find_map(|(_, facts)| facts.relational().table(name))
    }

    fn table_handles(&self) -> impl Iterator<Item = (TableId, &Table)> {
        self.stores().flat_map(|(store, facts)| {
            facts
                .relational()
                .tables
                .iter()
                .enumerate()
                .map(move |(index, table)| {
                    let id = TableId {
                        store: store.id(),
                        index,
                    };
                    (id, table)
                })
        })
    }

    fn table_named(&self, name: &str) -> Option<TableId> {
        self.table_handles()
            .find(|(_, table)| table.name == name)
            .map(|(id, _)| id)
    }

    pub fn table_at(&self, id: TableId) -> &Table {
        let (_, facts) = self
            .stores()
            .find(|(store, _)| store.id() == id.store)
            .expect("a table handle names a store of its connection");
        &facts.relational().tables[id.index]
    }

    pub fn entity_table(&self, entity: EntityId) -> Option<TableId> {
        self.table_named(self.table_of(entity)?)
    }

    pub fn edge_table_ids(&self, relationships: &[RelationshipId]) -> Vec<TableId> {
        self.edge_tables(relationships)
            .iter()
            .filter_map(|name| self.table_named(name))
            .collect()
    }

    pub fn shared_edge_table_id(&self) -> Option<TableId> {
        self.table_named(self.shared_edge_table()?)
    }

    pub fn copies(&self, table: TableId) -> Vec<TableId> {
        self.copies_of(&self.table_at(table).name)
            .into_iter()
            .filter_map(|copy| self.table_named(copy))
            .collect()
    }

    pub fn column_of(&self, target: Target, table: TableId) -> Option<ColumnId> {
        let stored = self.table_at(table);
        let index = match target {
            Target::Property(property) => stored
                .columns
                .iter()
                .position(|column| column.property == Some(property)),
            Target::Attribute(attribute) => {
                let name = &self.graph().attribute(attribute).name;
                let edge = stored.edge.then_some(stored);
                edge.and_then(|table| table.columns.iter().position(|c| &c.name == name))
            }
            Target::Edge(field) => {
                let name = self.edge_column_of(table, field)?;
                stored.columns.iter().position(|column| column.name == name)
            }
        }?;
        Some(ColumnId { table, index })
    }

    fn edge_column_of(&self, table: TableId, field: EdgeField) -> Option<&str> {
        let (_, facts) = self.stores().find(|(store, _)| store.id() == table.store)?;
        let columns = facts.relational().edge_columns.as_ref()?;
        self.table_at(table).edge.then(|| columns.column(field))
    }

    fn stored(&self, column: ColumnId) -> (&Table, &Column) {
        let table = self.table_at(column.table);
        (table, &table.columns[column.index])
    }

    /// The name the backend stores a column under, for the SQL renderer.
    pub fn column_name(&self, column: ColumnId) -> &str {
        &self.stored(column).1.name
    }

    pub fn column_type(&self, column: ColumnId) -> Option<crate::DataType> {
        self.stored(column).1.data_type
    }

    pub fn in_sort_key(&self, column: ColumnId) -> bool {
        let (table, column) = self.stored(column);
        table.sort_key.contains(&column.name)
    }

    pub fn text_indexed(&self, column: ColumnId) -> bool {
        let (table, column) = self.stored(column);
        table.indexes.iter().any(|index| {
            index.column == column.name && IndexKind::parse(&index.kind) == Some(IndexKind::Text)
        })
    }

    /// The edge columns every edge-table store on the connection declares; none when the
    /// connection stores no edges or its stores disagree.
    pub fn edge_columns(&self) -> Option<&EdgeColumns> {
        let mut declared = self
            .stores()
            .filter_map(|(_, facts)| facts.relational().edge_columns.as_ref());
        let first = declared.next()?;
        declared.all(|other| other == first).then_some(first)
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
