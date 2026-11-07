//! Neighbors: one edge scan per direction arm around the center binding. The planner splits each
//! direction by whether the edge row carries the center's path, and answers center filters from
//! denormalized edge tags where every relationship carries them. Arms emit their own redaction
//! columns since the center edge column differs per direction.

use ontology::constants::*;
use query_data_model::plan::logical::{Expr as Logical, LogicalOperation, Order as Sort};
use query_data_model::{
    DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Direction, RelationshipId,
};

use super::filters::BoundFilter;
use super::node::Node;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::logical::facts::{anchors, facts};
use crate::passes::codegen::RelationalModel;
use crate::passes::plan::Term;
use crate::passes::plan::predicates::{Column, OutputValue};

mod emit;

pub(crate) use emit::emit;

struct Tag<'p> {
    facts: &'p DenormalizedProperty,
    values: Vec<String>,
}

struct CenterFilter<'p> {
    property: String,
    filter: BoundFilter,
    source: Option<Tag<'p>>,
    target: Option<Tag<'p>>,
}

impl CenterFilter<'_> {
    fn tag(&self, direction: DenormalizedDirection) -> Option<&Tag<'_>> {
        match direction {
            DenormalizedDirection::Source => self.source.as_ref(),
            DenormalizedDirection::Target => self.target.as_ref(),
        }
    }
}

pub(crate) struct Neighbors<'p> {
    center: Node<'p>,
    filters: Vec<CenterFilter<'p>>,
    direction: Direction,
    kinds: Vec<String>,
    outgoing_tables: Vec<String>,
    incoming_tables: Vec<String>,
    has_non_denorm: bool,
    fused_table: Option<String>,
    outgoing_kinds: Vec<String>,
    incoming_kinds: Vec<String>,
    incoming_scope_preserving: Vec<String>,
    incoming_cross_scope: Vec<String>,
    outgoing_scope_preserving: Vec<String>,
    outgoing_cross_scope: Vec<String>,
    outgoing_neighbor_paths: Vec<(String, String)>,
    incoming_neighbor_paths: Vec<(String, String)>,
    center_sort_key: Vec<String>,
    order: Option<(OutputValue, bool)>,
    limit: u32,
}

impl<'p> Neighbors<'p> {
    pub fn new<M: RelationalModel>(query: Query<'p>, model: &'p M) -> Result<Self> {
        let (from, relationships, requested) = query
            .operations()
            .find_map(|(_, operation)| match operation {
                LogicalOperation::Neighbors {
                    from,
                    relationships,
                    direction,
                } => Some((*from, relationships.as_slice(), *direction)),
                _ => None,
            })
            .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;
        let facts = facts(query);
        let mut anchors = anchors(query, &facts);
        let [_] = anchors.as_slice() else {
            return Err(QueryError::Lowering(
                "neighbors query requires exactly one node".into(),
            ));
        };
        let anchor = anchors.remove(0);
        if anchor.binding.output != from {
            return Err(QueryError::Lowering(
                "neighbors center is not a binding".into(),
            ));
        }
        let center_entity = anchor.binding.entity;
        let mut center = Node::new(query, anchor, model);
        let graph = query.model.graph();
        let names: Vec<String> = relationships
            .iter()
            .map(|relationship| graph.relationship(*relationship).name.clone())
            .collect();
        let routes: Vec<_> = relationships
            .iter()
            .map(|relationship| (*relationship, model.routes(*relationship)))
            .collect();
        let touches = |routes: &[query_data_model::RelationshipRoute<'_>], source: bool| {
            routes.iter().any(|route| match source {
                true => route.has_source(center_entity),
                false => route.has_target(center_entity),
            })
        };
        let touching = |source: bool| -> Vec<(RelationshipId, &str)> {
            routes
                .iter()
                .zip(&names)
                .filter(|((_, routes), _)| touches(routes, source))
                .map(|((relationship, _), name)| (*relationship, name.as_str()))
                .collect()
        };
        let (outgoing, incoming) = (touching(true), touching(false));
        let arm_kinds = |touching: &[(RelationshipId, &str)]| match touching.is_empty() {
            true => names.clone(),
            false => touching.iter().map(|(_, name)| name.to_string()).collect(),
        };
        let neighbor_paths = |source: bool| {
            let mut pairs = Vec::new();
            for ((_, routes), name) in routes.iter().zip(&names) {
                for variant in routes.iter().flat_map(|route| route.variants()) {
                    let (center, neighbor) = match source {
                        true => (variant.source, variant.target),
                        false => (variant.target, variant.source),
                    };
                    if center != center_entity {
                        continue;
                    }
                    let owned = model
                        .edge_scope(variant.id)
                        .is_some_and(|scope| match source {
                            true => scope.edge_path_is_target_path(),
                            false => scope.edge_path_is_source_path(),
                        });
                    if owned {
                        pairs.push((name.clone(), graph.entity(neighbor).name.clone()));
                    }
                }
            }
            pairs
        };
        let split_by_path = |touching: &[(RelationshipId, &str)], center_is_target: bool| {
            let (mut carrying, mut rest) = (Vec::new(), Vec::new());
            for (relationship, name) in touching {
                let mut found = false;
                let mut carries = true;
                for variant in model
                    .routes(*relationship)
                    .iter()
                    .flat_map(|route| route.variants())
                    .filter(|variant| match center_is_target {
                        true => variant.target == center_entity,
                        false => variant.source == center_entity,
                    })
                {
                    found = true;
                    let source = &graph.entity(variant.source).name;
                    carries &= model.edge_scope(variant.id).is_some_and(|scope| {
                        match center_is_target {
                            // The edge row carries the target's path; a source shares it only where the ontology says so.
                            true => scope.is_scope_preserving(),
                            false => {
                                scope.propagates_to_source(name, source)
                                    || scope.edge_path_is_source_path()
                            }
                        }
                    });
                }
                match found && carries {
                    true => carrying.push(name.to_string()),
                    false => rest.push(name.to_string()),
                }
            }
            (carrying, rest)
        };
        let direction = match (requested, outgoing.is_empty(), incoming.is_empty()) {
            (Direction::Both, false, true) => Direction::Outgoing,
            (Direction::Both, true, false) => Direction::Incoming,
            (direction, _, _) => direction,
        };
        let (incoming_scope_preserving, incoming_cross_scope) = split_by_path(&incoming, true);
        let (outgoing_scope_preserving, outgoing_cross_scope) = split_by_path(&outgoing, false);
        let filters: Vec<CenterFilter<'p>> = std::mem::take(&mut center.filters)
            .into_iter()
            .map(|(property, filter)| {
                let tag = |direction| {
                    let key = DenormalizedKey {
                        property: filter.property?,
                        direction,
                    };
                    let facts = model.denormalized().property(key)?;
                    let values = filter.test.tags(&facts.tag_key)?;
                    Some(Tag { facts, values })
                };
                CenterFilter {
                    source: tag(DenormalizedDirection::Source),
                    target: tag(DenormalizedDirection::Target),
                    property,
                    filter,
                }
            })
            .collect();
        let covered = |tag: Option<&Tag<'_>>, touching: &[(RelationshipId, &str)]| {
            touching.is_empty()
                || tag.is_some_and(|tag| {
                    touching.iter().all(|(relationship, _)| {
                        model.routes(*relationship).iter().any(|route| {
                            route.source_entities().any(|source| {
                                route.target_entities().any(|target| {
                                    tag.facts
                                        .variants
                                        .contains(&(*relationship, source, target))
                                })
                            })
                        })
                    })
                })
        };
        let has_non_denorm = center.range.is_some()
            || filters.iter().any(|filter| {
                (direction != Direction::Incoming && !covered(filter.source.as_ref(), &outgoing))
                    || (direction != Direction::Outgoing
                        && !covered(filter.target.as_ref(), &incoming))
            });
        let all_tables = model.edge_tables(relationships);
        let tables_for = |source: bool| -> Vec<String> {
            let mut tables: Vec<String> = routes
                .iter()
                .flat_map(|(_, routes)| routes)
                .filter(|route| match source {
                    true => route.has_source(center_entity),
                    false => route.has_target(center_entity),
                })
                .map(|route| route.table.to_string())
                .collect();
            tables.sort();
            tables.dedup();
            match tables.is_empty() {
                true => all_tables.clone(),
                false => tables,
            }
        };
        let (outgoing_tables, incoming_tables) = (tables_for(true), tables_for(false));
        let mut tables = outgoing_tables.clone();
        tables.extend(incoming_tables.iter().cloned());
        tables.sort();
        tables.dedup();
        let fused_table = (direction == Direction::Both
            && !has_non_denorm
            && center.pinned.len() != 1
            && center.uses_default_pk()
            && tables.len() == 1)
            .then(|| tables.remove(0));
        let center_sort_key = match center.table {
            Some(table) => super::sort_key(model, table)?.to_vec(),
            None => Vec::new(),
        };
        let order = query
            .operations()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Order(keys) => Some(keys),
                _ => None,
            })
            .flatten()
            .filter_map(|(key, sort)| match key {
                Logical::Property { binding, property } => Some((*binding, *property, *sort)),
                _ => None,
            })
            .last()
            .map(|(binding, property, sort)| {
                let alias = query.name(binding);
                let declared = graph.property(property);
                let column = Column::new(alias, &declared.name);
                let value = match alias == center.alias && model.null_as_epoch(property) {
                    true => OutputValue::EpochAsNull(column, declared.data_type),
                    false => OutputValue::Column(column),
                };
                (value, sort == Sort::Descending)
            });
        let (rows, _) = query
            .limit()
            .ok_or_else(|| QueryError::PipelineInvariant("the surfaces always limit".into()))?;
        Ok(Self {
            outgoing_kinds: arm_kinds(&outgoing),
            incoming_kinds: arm_kinds(&incoming),
            outgoing_neighbor_paths: neighbor_paths(true),
            incoming_neighbor_paths: neighbor_paths(false),
            center,
            filters,
            direction,
            kinds: names,
            outgoing_tables,
            incoming_tables,
            has_non_denorm,
            fused_table,
            incoming_scope_preserving,
            incoming_cross_scope,
            outgoing_scope_preserving,
            outgoing_cross_scope,
            center_sort_key,
            order,
            limit: u32::try_from(rows).unwrap_or(u32::MAX),
        })
    }

    pub fn center(&self) -> &str {
        self.center.alias
    }
}

pub(crate) fn stable_order(plan: &Neighbors<'_>) -> Vec<Term> {
    match plan.direction {
        Direction::Both => vec![
            Term::Output(redaction_id_column(plan.center.alias)),
            Term::Output(neighbor_id_column().to_string()),
            Term::Output(relationship_type_column().to_string()),
            Term::Output(neighbor_is_outgoing_column().to_string()),
        ],
        _ => [
            TRAVERSAL_PATH_COLUMN,
            RELATIONSHIP_KIND_COLUMN,
            SOURCE_ID_COLUMN,
            TARGET_ID_COLUMN,
            SOURCE_KIND_COLUMN,
            TARGET_KIND_COLUMN,
        ]
        .into_iter()
        .map(|column| Term::Column(Column::new(neighbor_edge_alias(), column)))
        .collect(),
    }
}

#[derive(Clone, Copy)]
enum CenterBound {
    Path,
    Id,
}

#[derive(Clone)]
struct Arm<'p> {
    direction: Direction,
    kinds: &'p [String],
    bound: Option<CenterBound>,
}

fn arms<'p>(plan: &'p Neighbors<'_>) -> Vec<Arm<'p>> {
    let (center, center_has_tp) = (&plan.center.pinned, plan.center.has_traversal_path);
    let arms_for = |direction: Direction| -> Vec<Arm<'p>> {
        let (all, carrying, rest) = match direction {
            Direction::Incoming => (
                &plan.incoming_kinds,
                &plan.incoming_scope_preserving,
                &plan.incoming_cross_scope,
            ),
            _ => (
                &plan.outgoing_kinds,
                &plan.outgoing_scope_preserving,
                &plan.outgoing_cross_scope,
            ),
        };
        let arm = |kinds: &'p [String], bound| Arm {
            direction,
            kinds,
            bound,
        };
        if center.is_empty() || !center_has_tp {
            return vec![arm(all, None)];
        }
        if carrying.is_empty() {
            let guard = (direction == Direction::Incoming).then_some(CenterBound::Id);
            return vec![arm(all, guard)];
        }
        if rest.is_empty() {
            return vec![arm(all, Some(CenterBound::Path))];
        }
        vec![
            arm(carrying, Some(CenterBound::Path)),
            arm(rest, Some(CenterBound::Id)),
        ]
    };
    match plan.direction {
        Direction::Both => [arms_for(Direction::Outgoing), arms_for(Direction::Incoming)].concat(),
        direction => arms_for(direction),
    }
}
