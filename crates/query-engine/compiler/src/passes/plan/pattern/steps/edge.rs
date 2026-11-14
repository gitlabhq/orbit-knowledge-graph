//! Step for a relation stored as edge rows: read the current rows under the relation's predicates,
//! narrow by known endpoint ids, join to the chain, and expose the edge columns.

use crate::passes::codegen::SqlBackend;
use crate::passes::plan::build::stored;
use query_data_model::EdgeField;
use query_data_model::constants::*;
use query_data_model::implementations::relational::{RelationalBackend, TableId};

use super::super::relations::select_edge_layouts;
use super::super::{EdgeRelation, Pattern};
use super::{IdSet, RelationBuilder};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::passes::plan::current::latest_rows;
use crate::passes::plan::predicates::*;
use crate::passes::plan::rows::{ReadScope, Reads, Rel};
use crate::passes::plan::sort_key;

impl<B> RelationBuilder<'_, '_, '_, B>
where
    B: SqlBackend,
{
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
                .filter_map(|(node, _)| relation.endpoint_column(node).transpose())
                .collect::<Result<_>>()?;
            select_edge_layouts(pattern.model, &mut physical.edge_tables, &narrowed);
        }
        let mut predicates = self.edge_predicates(&physical)?;
        for (node, ids) in &endpoint_ids {
            if let Some(column) = relation.endpoint_column(node)? {
                predicates.push(membership(Column::new(alias, column), ids.clone()));
            }
        }
        for node in endpoints {
            if pattern.nodes[node].joined
                || !pattern.has_uncovered_filter(&pattern.nodes[node])
                || endpoint_ids.iter().any(|(bound, _)| *bound == node)
            {
                continue;
            }
            let ids = self.node_ids(node)?;
            let column = relation.endpoint_column(node)?.expect("relation endpoint");
            predicates.push(membership(Column::new(alias, column), ids));
        }
        let source = if relation.max_hops > 1 {
            self.multi_hop_source(&physical)?
        } else {
            self.edge_source(&physical, predicates)?
        };
        self.bind_relation_ids(index, relation, &source)?;
        let wrapped = relation.max_hops == 1 && pattern.relations.len() > 1;
        self.join_edge(relation, &source, wrapped)?;
        self.push_edge_outputs(relation)?;
        Ok(())
    }

    fn edge_predicates(&mut self, relation: &EdgeRelation) -> Result<Vec<Predicate>> {
        let pattern = self.pattern;
        let alias = &relation.alias;
        let endpoints = [&relation.from_node, &relation.to_node];
        let (source_node, target_node) = relation.stored_endpoints();
        let graph = pattern.model.graph();
        let kind = Column::new(alias, relation.column(EdgeField::Kind)?);
        let mut predicates: Vec<Predicate> = relationship_kinds(kind, relation.kinds())
            .into_iter()
            .collect();
        let source_kind = Column::new(alias, relation.column(EdgeField::SourceKind)?);
        let target_kind = Column::new(alias, relation.column(EdgeField::TargetKind)?);
        predicates.extend(pattern.kind_predicate(source_kind, source_node));
        predicates.extend(pattern.kind_predicate(target_kind, target_node));
        predicates.push(live(alias));
        predicates.extend(
            pattern
                .edge_column_filters(relation)
                .into_iter()
                .map(|(column, _, filter)| filter.at(Column::new(alias, column), None)),
        );
        for (_, filter) in &relation.filters {
            predicates.push(relation.filter_at(alias, filter, graph)?);
        }
        for node in endpoints {
            for (filter_index, (target, filter)) in pattern.nodes[node].filters.iter().enumerate() {
                let tag = (node.clone(), *target, filter_index);
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
            let column = relation.endpoint_column(node)?.expect("relation endpoint");
            predicates.extend(pattern.nodes[node].id_predicates(Column::new(alias, column)));
        }
        Ok(predicates)
    }

    fn edge_source(&mut self, relation: &EdgeRelation, predicates: Vec<Predicate>) -> Result<Rel> {
        let pattern = self.pattern;
        let alias = &relation.alias;
        if pattern.aggregate() && pattern.relations.len() == 1 {
            let Some(table) = relation.single_edge_table() else {
                let rows = plain_edge_rows(&mut self.reads, relation, true)?;
                return self.reads.filtered(&rows, &predicates);
            };
            let table = &pattern.model.table_at(table).name;
            let rows = plain_edge_rows(&mut self.reads, relation, false)?;
            let key = sort_key(pattern.model, table)?;
            let identity: Vec<_> = predicates
                .iter()
                .filter(|predicate| predicate.filters_sort_key(key, true))
                .cloned()
                .collect();
            let rows = self.reads.filtered(&rows, &identity)?;
            let latest = self.reads.subquery(alias, |reads| {
                let rows = reads.read(&rows, None)?;
                latest_rows(reads.q, rows, alias, key)
            })?;
            let rows = self.reads.q.read(&latest, Some(alias))?;
            let rows = self.reads.filter(rows, &predicates)?;
            self.condition = predicates;
            return self.reads.share(rows, Some(alias), false);
        }
        let span = self.reads.id_range_span;
        if relation.single_edge_table().is_some()
            || !stored::<B>()
                && predicates
                    .iter()
                    .any(|predicate| predicate.is_selective(span))
        {
            return current_edge_rows(&mut self.reads, pattern, relation, alias, predicates);
        }
        let rows = plain_edge_rows(&mut self.reads, relation, true)?;
        self.reads.filtered(&rows, &predicates)
    }

    /// Joins the edge rows to the chain; `wrapped` keeps them in their own block when other relations join too.
    fn join_edge(&mut self, relation: &EdgeRelation, source: &Rel, wrapped: bool) -> Result<()> {
        let alias = &relation.alias;
        let (start, end) = relation.id_columns()?;
        let endpoints = [(&relation.from_node, start), (&relation.to_node, end)];
        let rows = match wrapped {
            true => {
                let bound = self.reads.subquery(alias, |reads| {
                    let rows = reads.read(source, Some(alias))?;
                    Ok(reads.q.select_all(rows)?)
                })?;
                self.reads.q.read(&bound, Some(alias))?
            }
            false => self.reads.read(source, Some(alias))?,
        };
        if let Some(previous) = self.root.take() {
            let (node, column, binding) = endpoints
                .iter()
                .find_map(|(node, column)| {
                    self.binding(node)
                        .cloned()
                        .map(|binding| (*node, *column, binding))
                })
                .ok_or_else(|| QueryError::Lowering("stored relation is disconnected".into()))?;
            let join_predicates: Vec<_> = endpoints
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
            let on = (
                Column::new(&binding.alias, &binding.column),
                Column::new(alias, column),
            );
            self.root = Some(self.reads.join(previous, rows, on, &join_predicates)?);
        } else {
            self.root = Some(rows);
        }
        for (node, column) in endpoints {
            if self.binding(node).is_none() {
                self.set_binding(node, alias, column, None, None);
            }
        }
        Ok(())
    }

    fn push_edge_outputs(&mut self, relation: &EdgeRelation) -> Result<()> {
        let alias = relation.alias.as_str();
        let column = |name: &str| OutputValue::Column(Column::new(alias, name));
        self.push_relation_outputs(
            relation,
            relation.edges()?.all().map(|(_, name)| column(name)),
        );
        if relation.max_hops > 1 && !self.pattern.aggregate() {
            self.outputs.push(Projection::new(
                column(path_nodes_column()),
                relation.path_nodes_column(),
            ));
        }
        Ok(())
    }
}

pub(super) fn current_edge_rows<B>(
    reads: &mut Reads<'_, '_, B>,
    pattern: &Pattern<'_, B>,
    relation: &EdgeRelation,
    alias: &str,
    predicates: Vec<Predicate>,
) -> Result<Rel>
where
    B: SqlBackend,
{
    let model = pattern.model;
    let bound = reads.relationship_bound(relation.input_index);
    if let Some(table) = relation.single_edge_table() {
        let table = &model.table_at(table).name;
        let key = sort_key(model, table)?;
        return reads.current_rows(table, alias, bound, key, predicates, ReadScope::Edge);
    }
    edge_union(reads, relation, alias, alias, |reads, table| {
        let key = sort_key(model, table)?;
        reads.current_rows(
            table,
            alias,
            bound,
            key,
            predicates.clone(),
            ReadScope::Edge,
        )
    })
}

fn plain_edge_rows<B: RelationalBackend>(
    reads: &mut Reads<'_, '_, B>,
    relation: &EdgeRelation,
    final_: bool,
) -> Result<Rel> {
    let alias = &relation.alias;
    let bound = reads.relationship_bound(relation.input_index);
    if let Some(table) = relation.single_edge_table() {
        let table = &reads.q.catalog().table_at(table).name;
        return reads.table(table, alias, final_, bound, &[]);
    }
    let route_alias = union_arm_alias(alias);
    edge_union(reads, relation, alias, &route_alias, |reads, table| {
        reads.table(table, &route_alias, final_, bound, &[])
    })
}

fn edge_union<B: RelationalBackend>(
    reads: &mut Reads<'_, '_, B>,
    relation: &EdgeRelation,
    alias: &str,
    route_alias: &str,
    mut route: impl FnMut(&mut Reads<'_, '_, B>, &str) -> Result<Rel>,
) -> Result<Rel> {
    let fields = relation.edges()?.all().map(|(_, column)| column);
    let outputs: Vec<Projection> = [TRAVERSAL_PATH_COLUMN]
        .into_iter()
        .chain(fields)
        .chain([SOURCE_TAGS_COLUMN, TARGET_TAGS_COLUMN, DELETED_COLUMN])
        .map(|column| Projection::col(route_alias, column))
        .collect();
    let catalog = reads.q.catalog();
    let routes = relation
        .edge_tables
        .iter()
        .map(|table: &TableId| route(reads, &catalog.table_at(*table).name))
        .collect::<Result<Vec<_>>>()?;
    reads.union(alias, |reads| {
        let mut arms = Vec::new();
        for rows in &routes {
            let rows = reads.read(rows, Some(route_alias))?;
            arms.push(reads.select(rows, &outputs)?);
        }
        Ok(arms)
    })
}
