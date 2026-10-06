use std::collections::HashSet;

use ontology::constants::*;

use super::requirements::{Predicate, id_list, live, relationship_kinds, tag_filter};

use super::context::PlanningContext;
use super::{DenormalizedKey, Hop};
use crate::error::Result;
use query_data_model::QueryDataModel;
use query_data_model::bindings::RelationId;

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub(super) fn filtered_edge_predicates(
        &self,
        relation: RelationId,
        hop: &Hop,
        tagged: &mut HashSet<(String, String)>,
    ) -> Result<Vec<Predicate>> {
        let mut predicates = self.edge_predicates(relation, hop, false)?;
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| {
                    self.property_filter(self.column(relation, property)?, filter)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        self.push_denorm_tags(&mut predicates, hop, relation, tagged)?;
        predicates.extend(self.node_id_predicates(relation, hop)?);
        Ok(predicates)
    }

    pub(super) fn edge_predicates(
        &self,
        relation: RelationId,
        hop: &Hop,
        skip_deleted: bool,
    ) -> Result<Vec<Predicate>> {
        let mut predicates = Vec::new();
        let (start, end) = hop.direction.edge_columns();
        if let Some(filter) = relationship_kinds(
            self.column(relation, RELATIONSHIP_KIND_COLUMN)?,
            &hop.rel_types,
        ) {
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
                    column: self.column(relation, kind)?,
                    entity: entity.clone(),
                });
            }
        }
        if !skip_deleted && let Some(column) = self.deletion_column(relation, &hop.edge_table)? {
            predicates.push(live(column));
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
                            predicates.push(
                                self.property_filter(self.column(relation, property)?, filter)?,
                            );
                        }
                    }
                }
            }
        }
        Ok(predicates)
    }

    pub(super) fn push_denorm_tags(
        &self,
        predicates: &mut Vec<Predicate>,
        hop: &Hop,
        relation: RelationId,
        tagged: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        if crate::passes::normalize::is_wildcard(&hop.rel_types) {
            return Ok(());
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
                    && let Some(predicate) = tag_filter(
                        self.column(relation, &facts.edge_column)?,
                        &facts.tag_key,
                        &filter.filter,
                    )
                {
                    predicates.push(predicate);
                    tagged.insert(tag);
                }
            }
        }
        Ok(())
    }

    pub(super) fn node_id_predicates(
        &self,
        relation: RelationId,
        hop: &Hop,
    ) -> Result<Vec<Predicate>> {
        let (start, end) = hop.direction.edge_columns();
        let mut predicates = Vec::new();
        for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
            let Some(node) = self.nodes.get(node_alias) else {
                continue;
            };
            if let Some(range) = &node.id_range {
                predicates.push(Predicate::IdRange {
                    column: self.column(relation, column)?,
                    start: range.start,
                    end: range.end,
                });
            }
            if !node.node_ids.is_empty() {
                predicates.push(id_list(self.column(relation, column)?, &node.node_ids));
            }
        }
        Ok(predicates)
    }
}
