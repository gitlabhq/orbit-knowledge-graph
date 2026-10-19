//! Step for a variable-length relation: expand hop by hop from the pinned end and union the requested
//! depths, carrying the path so far.

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::super::relations::select_edge_layouts;
use super::super::{EdgeRelation, Pattern};
use super::edge::current_edge_rows;
use super::{IdSet, RelationBuilder};
use crate::constants::*;
use crate::error::Result;
use crate::input::Direction;
use crate::passes::plan::graph::{RelationGraph, RelationId};
use crate::passes::plan::predicates::*;

impl<M: OrbitQueryModel + ?Sized> RelationBuilder<'_, M> {
    /// Walks a variable-length relation from its pinned end, so the walk reverses when only the target is pinned.
    pub(super) fn multi_hop_source(&mut self, relation: &EdgeRelation) -> Result<RelationId> {
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
            &[expansion.direction.edge_columns().0],
        );
        multi_hop(
            &mut self.graph,
            pattern,
            &expansion,
            &relation.alias,
            [source_ids, target_ids],
            reverse,
        )
    }
}

fn multi_hop<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    pattern: &Pattern<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    [source_ids, target_ids]: [Option<IdSet>; 2],
    reverse_output: bool,
) -> Result<RelationId> {
    let covered = pattern.edge_column_filters(relation);
    let required: Vec<String> = covered
        .iter()
        .map(|(property, _)| (*property).to_string())
        .collect();
    let (start_id, end_id) = relation.direction.edge_columns();
    let path_id = if reverse_output { start_id } else { end_id };
    let from = &pattern.nodes[&relation.from_node];

    let mut first_predicates = vec![live("e1")];
    first_predicates.extend(pattern.entity_kind("e1", &relation.from_node, start_id));
    if let Some(ids) = source_ids {
        first_predicates.push(membership(Column::new("e1", start_id), ids));
    }
    first_predicates.extend(id_predicates(from, Column::new("e1", start_id)));
    first_predicates.extend(
        from.filters.iter().filter_map(|(_, filter)| {
            pattern.denorm_tag(relation, &relation.from_node, filter, "e1")
        }),
    );
    first_predicates.extend(
        covered
            .iter()
            .map(|(property, filter)| property_filter("e1", property, filter)),
    );
    let first = hop_rows(graph, pattern, relation, "e1", first_predicates)?;
    let first_path = OutputValue::Path(vec![(
        Column::new("e1", path_id),
        Column::new("e1", kind_column(path_id)),
    )]);
    let mut previous = project_depth(graph, first, 1, ("e1", "e1", "e1"), first_path, &required);
    let mut depths = Vec::new();
    if relation.min_hops <= 1 {
        depths.push(
            complete_depth(
                graph,
                pattern,
                relation,
                alias,
                previous,
                target_ids.as_ref(),
            )
            .into(),
        );
    }
    for depth in 2..=relation.max_hops {
        let previous_alias = format!("_depth_{}", depth - 1);
        let current_alias = format!("e{depth}");
        let current = hop_rows(
            graph,
            pattern,
            relation,
            &current_alias,
            vec![
                live(&current_alias),
                membership(
                    Column::new(&current_alias, start_id),
                    (previous, end_id.to_string()),
                ),
            ],
        )?;
        let joined = graph.join(
            previous.alias(&previous_alias),
            current.alias(&current_alias),
            (
                Column::new(&previous_alias, end_id),
                Column::new(&current_alias, start_id),
            ),
            vec![column_eq(
                Column::new(&previous_alias, kind_column(end_id)),
                Column::new(&current_alias, kind_column(start_id)),
            )],
        );
        let (source, target) = relation
            .direction
            .stored_order(&previous_alias, &current_alias);
        let step = (
            Column::new(&previous_alias, PATH_NODES_COLUMN),
            Column::new(&current_alias, path_id),
            Column::new(&current_alias, kind_column(path_id)),
        );
        let path = if reverse_output {
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
        previous = project_depth(
            graph,
            joined,
            depth,
            (&previous_alias, source, target),
            path,
            &required,
        );
        if depth >= relation.min_hops.max(1) {
            depths.push(
                complete_depth(
                    graph,
                    pattern,
                    relation,
                    alias,
                    previous,
                    target_ids.as_ref(),
                )
                .into(),
            );
        }
    }
    Ok(graph.union_all(depths, alias))
}

fn hop_rows<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    pattern: &Pattern<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    mut predicates: Vec<Predicate>,
) -> Result<RelationId> {
    predicates.extend(relationship_kinds(alias, &relation.rel_types));
    predicates.extend(
        relation
            .filters
            .iter()
            .map(|(property, filter)| property_filter(alias, property, filter)),
    );
    current_edge_rows(graph, pattern, relation, alias, predicates)
}

fn complete_depth<M: OrbitQueryModel + ?Sized>(
    graph: &mut RelationGraph,
    pattern: &Pattern<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
    depth: RelationId,
    target_ids: Option<&IdSet>,
) -> RelationId {
    let end_id = relation.direction.edge_columns().1;
    let mut predicates: Vec<Predicate> = pattern
        .entity_kind(alias, &relation.to_node, end_id)
        .into_iter()
        .collect();
    if let Some(ids) = target_ids {
        predicates.push(membership(Column::new(alias, end_id), ids.clone()));
    }
    predicates.extend(id_predicates(
        &pattern.nodes[&relation.to_node],
        Column::new(alias, end_id),
    ));
    graph.filter(depth.alias(alias), predicates)
}

fn project_depth(
    graph: &mut RelationGraph,
    input: RelationId,
    depth: u32,
    (meta, source, target): (&str, &str, &str),
    path: OutputValue,
    required: &[String],
) -> RelationId {
    let mut outputs = vec![
        Projection::col(meta, RELATIONSHIP_KIND_COLUMN),
        Projection::col(source, SOURCE_ID_COLUMN),
        Projection::col(source, SOURCE_KIND_COLUMN),
        Projection::col(source, SOURCE_TAGS_COLUMN),
        Projection::col(target, TARGET_ID_COLUMN),
        Projection::col(target, TARGET_KIND_COLUMN),
        Projection::col(target, TARGET_TAGS_COLUMN),
        Projection::new(path, PATH_NODES_COLUMN),
        Projection::new(OutputValue::Depth(depth), DEPTH_COLUMN),
        Projection::col(meta, DELETED_COLUMN),
        Projection::col(meta, TRAVERSAL_PATH_COLUMN),
    ];
    outputs.extend(required.iter().map(|column| Projection::col(meta, column)));
    graph.project(input, outputs)
}
