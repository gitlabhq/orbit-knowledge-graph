//! Step for a relation stored as edge rows: read the current rows under the relation's predicates,
//! narrow by known endpoint ids, join to the chain, and expose the edge columns.

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::super::relations::select_edge_layouts;
use super::super::{EdgeRelation, Pattern};
use super::{IdSet, RelationBuilder};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::passes::plan::graph::{ReadOwner, RelationGraph, RelationId};
use crate::passes::plan::predicates::*;
use crate::passes::plan::sort_key;

impl<M: OrbitQueryModel + ?Sized> RelationBuilder<'_, M> {
    pub(super) fn add_edge(&mut self, index: usize, relation: &EdgeRelation) -> Result<()> {
        let pattern = self.pattern;
        let alias = &relation.alias;
        let endpoints = [&relation.from_node, &relation.to_node];
        for node in endpoints {
            if self.ids(node).is_none() && pattern.has_selective_uncovered_filter(node) {
                self.node_ids(node)?;
            }
        }
        let endpoint_ids: Vec<(&String, IdSet)> = endpoints
            .into_iter()
            .filter_map(|node| Some((node, self.ids(node)?.clone())))
            .collect();
        let mut physical = relation.clone();
        if relation.max_hops == 1 {
            let narrowed: Vec<&str> = endpoint_ids
                .iter()
                .filter_map(|(node, _)| relation.endpoint_column(node))
                .collect();
            select_edge_layouts(pattern.model, &mut physical.edge_tables, &narrowed);
        }
        let mut predicates = self.edge_predicates(&physical);
        for (node, ids) in endpoint_ids {
            if let Some(column) = relation.endpoint_column(node) {
                predicates.push(membership(Column::new(alias, column), ids));
            }
        }
        for node in endpoints {
            if pattern.nodes[node].joined || !pattern.has_uncovered_filter(&pattern.nodes[node]) {
                continue;
            }
            let ids = self.node_ids(node)?;
            let column = relation.endpoint_column(node).expect("relation endpoint");
            predicates.push(membership(Column::new(alias, column), ids));
        }
        let mut source = if relation.max_hops > 1 {
            self.multi_hop_source(&physical)?
        } else {
            self.edge_source(&physical, predicates)?
        };
        self.bind_relation_ids(index, relation, source);
        if relation.max_hops == 1
            && (pattern.relations.len() > 1
                || endpoints
                    .into_iter()
                    .any(|node| pattern.nodes[node].narrowed))
        {
            source = self.graph.bind(source, alias);
        }
        self.join_edge(relation, source)?;
        self.push_edge_outputs(relation);
        Ok(())
    }

    fn edge_predicates(&mut self, relation: &EdgeRelation) -> Vec<Predicate> {
        let pattern = self.pattern;
        let alias = &relation.alias;
        let endpoints = [&relation.from_node, &relation.to_node];
        let (source_node, target_node) = relation.stored_endpoints();
        let mut predicates: Vec<Predicate> = relationship_kinds(alias, &relation.rel_types)
            .into_iter()
            .collect();
        predicates.extend(pattern.entity_kind(alias, source_node, SOURCE_ID_COLUMN));
        predicates.extend(pattern.entity_kind(alias, target_node, TARGET_ID_COLUMN));
        predicates.push(live(alias));
        predicates.extend(
            pattern
                .edge_column_filters(relation)
                .into_iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        predicates.extend(
            relation
                .filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        for node in endpoints {
            for (filter_index, (property, filter)) in pattern.nodes[node].filters.iter().enumerate()
            {
                let tag = (node.clone(), property.clone(), filter_index);
                if self.tagged.contains(&tag) {
                    continue;
                }
                if let Some(predicate) = pattern.denorm_tag(relation, node, filter, alias) {
                    predicates.push(predicate);
                    self.tagged.insert(tag);
                }
            }
        }
        for node in endpoints {
            let column = relation.endpoint_column(node).expect("relation endpoint");
            predicates.extend(pattern.nodes[node].id_predicates(Column::new(alias, column)));
        }
        predicates
    }

    fn edge_source(
        &mut self,
        relation: &EdgeRelation,
        predicates: Vec<Predicate>,
    ) -> Result<RelationId> {
        let pattern = self.pattern;
        let alias = &relation.alias;
        if pattern.aggregate() && pattern.relations.len() == 1 {
            let Some(table) = relation.single_edge_table() else {
                let rows = plain_edge_rows(&mut self.graph, relation, true)?;
                return Ok(self.graph.filter(rows, predicates));
            };
            let rows = plain_edge_rows(&mut self.graph, relation, false)?;
            let key = sort_key(pattern.model, table)?;
            let identity = predicates
                .iter()
                .filter(|predicate| predicate.filters_sort_key(key, true))
                .cloned()
                .collect();
            let rows = self.graph.filter(rows, identity);
            return Ok(self.graph.latest(rows, alias, key.to_vec(), predicates));
        }
        if relation.single_edge_table().is_some()
            || pattern.model.supports_selective_latest_rows()
                && predicates.iter().any(Predicate::is_selective)
        {
            return current_edge_rows(&mut self.graph, pattern, relation, alias, predicates);
        }
        let rows = plain_edge_rows(&mut self.graph, relation, true)?;
        Ok(self.graph.filter(rows, predicates))
    }

    fn join_edge(&mut self, relation: &EdgeRelation, source: RelationId) -> Result<()> {
        let alias = &relation.alias;
        let (start, end) = relation.direction.edge_columns();
        let endpoints = [(&relation.from_node, start), (&relation.to_node, end)];
        if let Some(previous) = self.root.take() {
            let (node, column, binding) = endpoints
                .iter()
                .find_map(|(node, column)| {
                    self.binding(node)
                        .cloned()
                        .map(|binding| (*node, *column, binding))
                })
                .ok_or_else(|| QueryError::Lowering("stored relation is disconnected".into()))?;
            let join_predicates = endpoints
                .iter()
                .filter(|(other, _)| *other != node)
                .filter_map(|(other, other_column)| {
                    let other = self.binding(other)?;
                    Some(column_eq(
                        Column::new(&other.alias, &other.column),
                        Column::new(alias, *other_column),
                    ))
                })
                .collect();
            self.root = Some(self.graph.join(
                previous,
                source.alias(alias),
                (
                    Column::new(&binding.alias, &binding.column),
                    Column::new(alias, column),
                ),
                join_predicates,
            ));
        } else {
            self.root = Some(source);
        }
        for (node, column) in endpoints {
            if self.binding(node).is_none() {
                self.set_binding(node, alias, column, None, None);
            }
        }
        Ok(())
    }

    fn push_edge_outputs(&mut self, relation: &EdgeRelation) {
        let alias = relation.alias.as_str();
        let column = |name: &str| OutputValue::Column(Column::new(alias, name));
        self.push_relation_outputs(
            relation,
            [
                column(RELATIONSHIP_KIND_COLUMN),
                column(SOURCE_ID_COLUMN),
                column(SOURCE_KIND_COLUMN),
                column(TARGET_ID_COLUMN),
                column(TARGET_KIND_COLUMN),
            ],
        );
        if relation.max_hops > 1 && !self.pattern.aggregate() {
            self.outputs.push(Projection::new(
                column(PATH_NODES_COLUMN),
                format!("{}{PATH_NODES_COLUMN}", relation.column_prefix()),
            ));
        }
    }
}

pub(super) fn current_edge_rows<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    pattern: &Pattern<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    predicates: Vec<Predicate>,
) -> Result<RelationId> {
    let model = pattern.model;
    let owner = ReadOwner::Relationship(relation.input_index);
    if let Some(table) = relation.single_edge_table() {
        return Ok(graph.current_rows(
            table,
            alias,
            owner,
            sort_key(model, table)?,
            predicates,
            model.supports_selective_latest_rows(),
        ));
    }
    edge_union(
        graph,
        &relation.edge_tables,
        alias,
        alias,
        |graph, table| {
            Ok(graph.current_rows(
                table,
                alias,
                owner.clone(),
                sort_key(model, table)?,
                predicates.clone(),
                model.supports_selective_latest_rows(),
            ))
        },
    )
}

fn plain_edge_rows(
    graph: &mut RelationGraph,
    relation: &EdgeRelation,
    final_: bool,
) -> Result<RelationId> {
    let alias = &relation.alias;
    let owner = ReadOwner::Relationship(relation.input_index);
    if let Some(table) = relation.single_edge_table() {
        let read = graph.read(table, alias, final_);
        return Ok(graph.owned(read, owner, alias));
    }
    let route_alias = format!("_{alias}");
    edge_union(
        graph,
        &relation.edge_tables,
        alias,
        &route_alias,
        |graph, table| {
            let read = graph.read(table, &route_alias, final_);
            Ok(graph.owned(read, owner.clone(), &route_alias))
        },
    )
}

fn edge_union(
    graph: &mut RelationGraph,
    tables: &[String],
    alias: &str,
    route_alias: &str,
    mut route: impl FnMut(&mut RelationGraph, &str) -> Result<RelationId>,
) -> Result<RelationId> {
    let mut routes = Vec::new();
    for table in tables {
        let rows = route(graph, table)?;
        let outputs = EDGE_RESERVED_COLUMNS
            .iter()
            .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN, &DELETED_COLUMN])
            .map(|column| Projection::col(route_alias, column))
            .collect();
        routes.push(graph.project(rows, outputs).into());
    }
    Ok(graph.union_all(routes, alias))
}
