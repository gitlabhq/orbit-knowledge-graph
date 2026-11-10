//! Step for a variable-length relation: expand hop by hop from the pinned end and union the requested
//! depths, carrying the path so far.

use crate::passes::codegen::SqlBackend;
use query_data_model::constants::*;
use query_data_model::implementations::relational::plan::Relation;

use super::super::relations::{edge_columns, select_edge_layouts, stored_order};
use super::super::{EdgeRelation, Pattern};
use super::edge::current_edge_rows;
use super::{IdSet, RelationBuilder};
use crate::constants::*;
use crate::error::Result;
use crate::passes::plan::predicates::*;
use crate::passes::plan::rows::{Reads, Rel};
use query_data_model::Direction;

impl<B> RelationBuilder<'_, '_, '_, B>
where
    B: SqlBackend,
{
    /// Walks a variable-length relation from its pinned end, so the walk reverses when only the target is pinned.
    pub(super) fn multi_hop_source(&mut self, relation: &EdgeRelation) -> Result<Rel> {
        let pattern = self.pattern;
        let mut expansion = relation.clone();
        let mut source_ids = self.ids(&relation.from_node).cloned();
        let mut target_ids = self.ids(&relation.to_node).cloned();
        let reverse = relation.direction != Direction::Both
            && !pattern.endpoint_is_selective(&relation.from_node)
            && pattern.endpoint_is_selective(&relation.to_node);
        if reverse {
            expansion = expansion.reversed();
            std::mem::swap(&mut source_ids, &mut target_ids);
        }
        select_edge_layouts(
            pattern.model,
            &mut expansion.edge_tables,
            &[edge_columns(expansion.direction).0],
        );
        let mut walk = Walk {
            reads: &mut self.reads,
            pattern,
            relation: &expansion,
            alias: &relation.alias,
        };
        walk.expand([source_ids, target_ids])
    }
}

struct Walk<'w, 'q, 'c, 'p, B: SqlBackend> {
    reads: &'w mut Reads<'q, 'c, B>,
    pattern: &'w Pattern<'p, B>,
    relation: &'w EdgeRelation,
    alias: &'w str,
}

impl<B: SqlBackend> Walk<'_, '_, '_, '_, B> {
    fn expand(&mut self, [source_ids, target_ids]: [Option<IdSet>; 2]) -> Result<Rel> {
        let (pattern, relation) = (self.pattern, self.relation);
        let covered = pattern.edge_column_filters(relation);
        let required: Vec<String> = covered
            .iter()
            .map(|(property, _)| (*property).to_string())
            .collect();
        let (start_id, end_id) = edge_columns(relation.direction);
        let toward_sources = relation.direction == Direction::Incoming;
        let path_id = if toward_sources { start_id } else { end_id };
        let from = &pattern.nodes[&relation.from_node];
        let tagged = relation.edge_tables.iter().all(|table| {
            pattern
                .model
                .table_facts(table)
                .is_none_or(|facts| facts.columns.contains(SOURCE_TAGS_COLUMN))
        });
        let shape = Depth {
            required: &required,
            tagged,
        };

        let first_hop = hop_alias(1);
        let mut first_predicates = vec![live(&first_hop)];
        first_predicates.extend(pattern.entity_kind(&first_hop, &relation.from_node, start_id));
        if let Some(ids) = source_ids {
            first_predicates.push(membership(Column::new(&first_hop, start_id), ids));
        }
        first_predicates.extend(from.id_predicates(Column::new(&first_hop, start_id)));
        first_predicates.extend(from.filters.iter().filter_map(|(_, filter)| {
            pattern.denorm_tag(relation, &relation.from_node, filter, &first_hop)
        }));
        first_predicates.extend(
            covered
                .iter()
                .map(|(property, filter)| property_filter(&first_hop, property, filter)),
        );
        let first = self.hop_rows(&first_hop, first_predicates)?;
        let first_path = OutputValue::Path(vec![(
            Column::new(&first_hop, path_id),
            Column::new(&first_hop, kind_column(path_id)),
        )]);
        let rows = self.reads.read(&first, None)?;
        let mut previous = self.project_depth(
            rows,
            1,
            (&first_hop, &first_hop, &first_hop),
            first_path,
            &shape,
        )?;
        let mut depths = Vec::new();
        if relation.min_hops <= 1 {
            depths.push(previous.clone());
        }
        for depth in 2..=relation.max_hops {
            let previous_alias = reached_depth_alias(depth - 1);
            let current_alias = hop_alias(depth);
            let current = self.hop_rows(
                &current_alias,
                vec![
                    live(&current_alias),
                    membership(
                        Column::new(&current_alias, start_id),
                        (previous.clone(), end_id.to_string()),
                    ),
                ],
            )?;
            let left = self.reads.read(&previous, Some(&previous_alias))?;
            let right = self.reads.read(&current, Some(&current_alias))?;
            let joined = self.reads.join(
                left,
                right,
                (
                    Column::new(&previous_alias, end_id),
                    Column::new(&current_alias, start_id),
                ),
                &[column_eq(
                    Column::new(&previous_alias, kind_column(end_id)),
                    Column::new(&current_alias, kind_column(start_id)),
                )],
            )?;
            let (source, target) =
                stored_order(relation.direction, &previous_alias, &current_alias);
            let step = (
                Column::new(&previous_alias, path_nodes_column()),
                Column::new(&current_alias, path_id),
                Column::new(&current_alias, kind_column(path_id)),
            );
            let path = if toward_sources {
                OutputValue::PathPrepend {
                    path: step.0,
                    id: step.1,
                    kind: step.2,
                }
            } else {
                OutputValue::PathAppend {
                    path: step.0,
                    id: step.1,
                    kind: step.2,
                }
            };
            previous = self.project_depth(
                joined,
                depth,
                (&previous_alias, source, target),
                path,
                &shape,
            )?;
            if depth >= relation.min_hops.max(1) {
                depths.push(previous.clone());
            }
        }
        let (pattern, alias) = (self.pattern, self.alias);
        self.reads.union(alias, |reads| {
            depths
                .iter()
                .map(|depth| {
                    complete_depth(reads, pattern, relation, alias, depth, target_ids.as_ref())
                })
                .collect()
        })
    }

    fn hop_rows(&mut self, alias: &str, mut predicates: Vec<Predicate>) -> Result<Rel> {
        let relation = self.relation;
        predicates.extend(relationship_kinds(alias, relation.kinds()));
        predicates.extend(
            relation
                .filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        current_edge_rows(self.reads, self.pattern, relation, alias, predicates)
    }

    fn project_depth(
        &mut self,
        input: Relation,
        depth: u32,
        (meta, source, target): (&str, &str, &str),
        path: OutputValue,
        shape: &Depth<'_>,
    ) -> Result<Rel> {
        let tags = |alias, column| shape.tagged.then(|| Projection::col(alias, column));
        let mut outputs: Vec<_> = [
            Some(Projection::col(meta, RELATIONSHIP_KIND_COLUMN)),
            Some(Projection::col(source, SOURCE_ID_COLUMN)),
            Some(Projection::col(source, SOURCE_KIND_COLUMN)),
            tags(source, SOURCE_TAGS_COLUMN),
            Some(Projection::col(target, TARGET_ID_COLUMN)),
            Some(Projection::col(target, TARGET_KIND_COLUMN)),
            tags(target, TARGET_TAGS_COLUMN),
            Some(Projection::new(path, path_nodes_column())),
            Some(Projection::new(OutputValue::Depth(depth), depth_column())),
            Some(Projection::col(meta, DELETED_COLUMN)),
            Some(Projection::col(meta, TRAVERSAL_PATH_COLUMN)),
        ]
        .into_iter()
        .flatten()
        .collect();
        outputs.extend(
            shape
                .required
                .iter()
                .map(|column| Projection::col(meta, column)),
        );
        let rows = self.reads.select(input, &outputs)?;
        self.reads.shared(rows, None, false)
    }
}

struct Depth<'r> {
    required: &'r [String],
    tagged: bool,
}

fn complete_depth<B: SqlBackend>(
    reads: &mut Reads<'_, '_, B>,
    pattern: &Pattern<'_, B>,
    relation: &EdgeRelation,
    alias: &str,
    depth: &Rel,
    target_ids: Option<&IdSet>,
) -> Result<Relation> {
    let end_id = edge_columns(relation.direction).1;
    let mut predicates: Vec<Predicate> = pattern
        .entity_kind(alias, &relation.to_node, end_id)
        .into_iter()
        .collect();
    if let Some(ids) = target_ids {
        predicates.push(membership(Column::new(alias, end_id), ids.clone()));
    }
    predicates.extend(pattern.nodes[&relation.to_node].id_predicates(Column::new(alias, end_id)));
    let rows = reads.read(depth, Some(alias))?;
    let rows = reads.filter(rows, &predicates)?;
    Ok(reads.q.select_all(rows)?)
}
