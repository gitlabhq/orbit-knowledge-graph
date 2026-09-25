use std::collections::{BTreeSet, HashMap, HashSet};

use query_data_model::{ClickHouseDataModel, DuckDbDataModel, EntityId};

pub trait PlanningModel {
    fn graph(&self) -> &query_data_model::GraphCatalog;
    fn entity_table(&self, entity: EntityId) -> Option<&str>;
    fn entity_has_traversal_path(&self, entity: EntityId) -> bool;
    fn entity_is_global(&self, entity: EntityId) -> bool;
    fn skip_fk_elision(&self) -> bool;
    fn force_join(&self) -> bool;
    fn force_emit_select(&self) -> bool;
    fn default_edge_table(&self) -> &str;
    fn all_edge_tables(&self) -> Vec<String>;
    fn edge_table(&self, relationship: &str) -> Option<&str>;
    fn edge_tables(&self, relationships: &[String]) -> Vec<String> {
        if relationships.is_empty() || relationships == ["*"] {
            return self.all_edge_tables();
        }
        let requested = relationships
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        self.graph()
            .relationships()
            .filter(|relationship| {
                requested.is_empty() || requested.contains(relationship.name.as_str())
            })
            .filter_map(|relationship| self.edge_table(&relationship.name))
            .map(String::from)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    fn foreign_key(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Option<ForeignKey>;
    fn table_columns(&self, table: &str) -> Option<&HashSet<String>>;
    fn table_sort_key(&self, table: &str) -> Option<&[String]>;
    fn denormalized_maps(&self) -> (DenormalizedColumns, DenormalizedRelationships);
    fn traversal_path_lookup(&self, entity: &str) -> Option<(String, String)>;
    fn scope_preserving(&self, relationship: &str, source: &str, target: &str) -> bool;
    fn pruned_scope_endpoint(&self, relationship: &str, source: &str, target: &str)
    -> Option<bool>;
}

pub(super) fn relationship_entities(
    graph: &query_data_model::GraphCatalog,
    relationship: &str,
    endpoint: impl Fn(&query_data_model::RelationshipVariant) -> EntityId,
) -> Vec<String> {
    graph
        .relationship_id(relationship)
        .map(|id| {
            graph
                .relationship(id)
                .variants
                .iter()
                .map(|variant| endpoint(graph.variant(*variant)))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|entity| graph.entity(entity).name.clone())
                .collect()
        })
        .unwrap_or_default()
}

pub struct ForeignKey {
    pub holder: String,
    pub column: String,
}

pub type DenormalizedKey = (String, String, String);
pub type DenormalizedColumns = HashMap<DenormalizedKey, (String, String)>;
pub type DenormalizedRelationships = HashMap<DenormalizedKey, Vec<String>>;

impl PlanningModel for ClickHouseDataModel {
    fn graph(&self) -> &query_data_model::GraphCatalog {
        self.graph()
    }

    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.backend()
            .entity(entity)
            .map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.backend()
            .entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        self.backend()
            .entity(entity)
            .is_some_and(|layout| layout.global)
    }

    fn skip_fk_elision(&self) -> bool {
        false
    }

    fn force_join(&self) -> bool {
        false
    }

    fn force_emit_select(&self) -> bool {
        false
    }

    fn default_edge_table(&self) -> &str {
        self.backend().default_edge_table()
    }

    fn all_edge_tables(&self) -> Vec<String> {
        self.backend()
            .edge_tables()
            .map(|table| table.name.clone())
            .collect()
    }

    fn edge_table(&self, relationship: &str) -> Option<&str> {
        let id = self.graph().relationship_id(relationship)?;
        self.backend().relationship_table(id)
    }

    fn foreign_key(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Option<ForeignKey> {
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        let mut foreign_keys = relationships.iter().map(|name| {
            let relationship = self.graph().relationship_id(name)?;
            let variant = self.graph().variant_id(relationship, source, target)?;
            let property = self.backend().variant(variant)?.foreign_key?;
            Some(ForeignKey {
                holder: self
                    .graph()
                    .entity(self.graph().property(property).entity)
                    .name
                    .clone(),
                column: self.backend().property_column(property)?.to_string(),
            })
        });
        let first = foreign_keys.next()??;
        foreign_keys
            .all(|foreign_key| {
                foreign_key.is_some_and(|foreign_key| {
                    foreign_key.holder == first.holder && foreign_key.column == first.column
                })
            })
            .then_some(first)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.backend().table(table).map(|layout| &layout.columns)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.backend()
            .table(table)
            .map(|layout| layout.sort_key.as_slice())
    }

    fn denormalized_maps(&self) -> (DenormalizedColumns, DenormalizedRelationships) {
        let backend = self.backend();
        (
            backend.denormalized_columns().clone(),
            backend.denormalized_relationships().clone(),
        )
    }

    fn traversal_path_lookup(&self, entity: &str) -> Option<(String, String)> {
        let entity = self.graph().entity_id(entity)?;
        let lookup = self
            .backend()
            .traversal_path_lookup(entity, ontology::TraversalPathKind::Id)?;
        Some((
            lookup.table.clone(),
            self.backend().property_column(lookup.property)?.to_string(),
        ))
    }

    fn scope_preserving(&self, relationship: &str, source: &str, target: &str) -> bool {
        let Some(variant) = self
            .graph()
            .relationship_id(relationship)
            .zip(self.graph().entity_id(source))
            .zip(self.graph().entity_id(target))
            .and_then(|((relationship, source), target)| {
                self.graph().variant_id(relationship, source, target)
            })
        else {
            return false;
        };
        self.authorization()
            .variant_scope(variant)
            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
    }

    fn pruned_scope_endpoint(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<bool> {
        let variant = self
            .graph()
            .relationship_id(relationship)
            .zip(self.graph().entity_id(source))
            .zip(self.graph().entity_id(target))
            .and_then(|((relationship, source), target)| {
                self.graph().variant_id(relationship, source, target)
            })?;
        match self.authorization().variant_scope(variant)? {
            ontology::EdgeVariantScope::PruneToSource => Some(true),
            ontology::EdgeVariantScope::PruneToTarget => Some(false),
            _ => None,
        }
    }
}

impl PlanningModel for DuckDbDataModel {
    fn graph(&self) -> &query_data_model::GraphCatalog {
        self.graph()
    }

    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.backend()
            .entity(entity)
            .map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.backend().entity(entity).is_some_and(|layout| {
            layout
                .properties
                .values()
                .any(|column| column == "traversal_path")
        })
    }

    fn entity_is_global(&self, _entity: EntityId) -> bool {
        false
    }

    fn skip_fk_elision(&self) -> bool {
        true
    }

    fn force_join(&self) -> bool {
        true
    }

    fn force_emit_select(&self) -> bool {
        true
    }

    fn default_edge_table(&self) -> &str {
        self.backend().edge_table()
    }

    fn all_edge_tables(&self) -> Vec<String> {
        vec![self.backend().edge_table().to_string()]
    }

    fn edge_table(&self, relationship: &str) -> Option<&str> {
        let id = self.graph().relationship_id(relationship)?;
        self.backend().relationship_table(id)
    }

    fn foreign_key(
        &self,
        _relationships: &[String],
        _source: &str,
        _target: &str,
    ) -> Option<ForeignKey> {
        None
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        (table == self.backend().edge_table()).then(|| self.backend().edge_columns())
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.graph().entities().find_map(|entity| {
            self.backend()
                .entity(entity.id)
                .filter(|layout| layout.table == table)
                .map(|layout| layout.sort_key.as_slice())
        })
    }

    fn denormalized_maps(&self) -> (DenormalizedColumns, DenormalizedRelationships) {
        (HashMap::new(), HashMap::new())
    }

    fn traversal_path_lookup(&self, _entity: &str) -> Option<(String, String)> {
        None
    }

    fn scope_preserving(&self, _relationship: &str, _source: &str, _target: &str) -> bool {
        false
    }

    fn pruned_scope_endpoint(
        &self,
        _relationship: &str,
        _source: &str,
        _target: &str,
    ) -> Option<bool> {
        None
    }
}
