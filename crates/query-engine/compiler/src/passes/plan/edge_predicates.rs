use std::collections::HashSet;

use ontology::constants::*;

use super::requirements::{
    Column, Predicate, id_list, live, property_filter, relationship_kinds, tag_filter,
};

use super::context::PlanningContext;
use super::{DenormalizedKey, Hop};
use query_data_model::QueryDataModel;

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub(super) fn filtered_edge_predicates(
        &self,
        alias: &str,
        hop: &Hop,
        tagged: &mut HashSet<(String, String)>,
    ) -> Vec<Predicate> {
        let mut predicates = self.edge_predicates(alias, hop, false);
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        self.push_denorm_tags(&mut predicates, hop, alias, tagged);
        predicates.extend(self.node_id_predicates(alias, hop));
        predicates
    }

    pub(super) fn edge_predicates(
        &self,
        alias: &str,
        hop: &Hop,
        skip_deleted: bool,
    ) -> Vec<Predicate> {
        let mut predicates = Vec::new();
        let (start, end) = hop.direction.edge_columns();
        if let Some(filter) = relationship_kinds(alias, &hop.rel_types) {
            predicates.push(filter);
        }
        for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
            if let Some(node) = self.nodes.get(node_alias)
                && let Some(entity) = &node.entity
            {
                let kind = if column == SOURCE_ID_COLUMN {
                    SOURCE_KIND_COLUMN
                } else {
                    TARGET_KIND_COLUMN
                };
                predicates.push(Predicate::EntityKind {
                    column: Column::new(alias, kind),
                    entity: entity.clone(),
                });
            }
        }
        if !skip_deleted {
            predicates.push(live(alias));
        }
        if let Some(table) = self.model.table(&hop.edge_table) {
            let mut seen = HashSet::new();
            for node_alias in [&hop.from_node, &hop.to_node] {
                if let Some(node) = self.nodes.get(node_alias) {
                    for (property, filter) in &node.filters {
                        if table.column(property).is_some()
                            && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                            && seen.insert(property)
                        {
                            predicates.push(property_filter(alias, property, filter));
                        }
                    }
                }
            }
        }
        predicates
    }

    pub(super) fn push_denorm_tags(
        &self,
        predicates: &mut Vec<Predicate>,
        hop: &Hop,
        alias: &str,
        tagged: &mut HashSet<(String, String)>,
    ) {
        if crate::passes::normalize::is_wildcard(&hop.rel_types) {
            return;
        }
        let (start, end) = hop.direction.edge_columns();
        for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
            let Some(node) = self.nodes.get(node_alias) else {
                continue;
            };
            let direction = if column == SOURCE_ID_COLUMN {
                query_data_model::DenormalizedDirection::Source
            } else {
                query_data_model::DenormalizedDirection::Target
            };
            for (property_name, filter) in &node.filters {
                let tag = (node_alias.clone(), property_name.clone());
                if tagged.contains(&tag) {
                    continue;
                }
                let Some(property) = filter.property else {
                    continue;
                };
                let key = DenormalizedKey {
                    property,
                    direction,
                };
                if let Some(facts) = self.denormalized.get(&key)
                    && hop
                        .relationships
                        .iter()
                        .any(|relationship| facts.relationships.contains(relationship))
                    && let Some(predicate) =
                        tag_filter(alias, &facts.edge_column, &facts.tag_key, &filter.filter)
                {
                    predicates.push(predicate);
                    tagged.insert(tag);
                }
            }
        }
    }

    pub(super) fn node_id_predicates(&self, alias: &str, hop: &Hop) -> Vec<Predicate> {
        let (start, end) = hop.direction.edge_columns();
        let mut predicates = Vec::new();
        for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
            let Some(node) = self.nodes.get(node_alias) else {
                continue;
            };
            if let Some(range) = &node.id_range {
                predicates.push(Predicate::IdRange {
                    column: Column::new(alias, column),
                    start: range.start,
                    end: range.end,
                });
            }
            if !node.node_ids.is_empty() {
                predicates.push(id_list(alias, column, &node.node_ids));
            }
        }
        predicates
    }
}
