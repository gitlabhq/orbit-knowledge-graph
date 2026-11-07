//! Neighbors: one edge scan per direction arm around the center binding. The planner splits each
//! direction by whether the edge row carries the center's path, and answers center filters from
//! denormalized edge tags where every relationship carries them. Arms emit their own redaction
//! columns since the center edge column differs per direction.

use ontology::constants::*;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Column as Output, Expr, Function, JoinKind, Order, Relation, RelationalBuilder,
    Subquery,
};
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
use crate::passes::lower::Term;
use crate::passes::lower::build::expr::{id_range, ids, relationship_kinds, tags};
use crate::passes::lower::build::predicates::{Lowering, Shared};
use crate::passes::lower::build::sources::{Scope, columns, dedup, deleted, edge_scan, select};
use crate::passes::lower::build::{
    Built, and, binary, call, col, computed, conjoin, disjoin, eq, filtered, int, scan, stored,
    string,
};
use crate::passes::plan::predicates::{Column, OutputValue};

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

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Neighbors<'_>,
) -> Built<Relation> {
    let order = &plan.order;
    let tail = |q: &mut RelationalBuilder<'_, B>, rows: Relation| -> Built<Relation> {
        let rows = match order {
            Some((value, descending)) => {
                let shared = Shared::default();
                let lowering = Lowering {
                    scope: &[&rows],
                    shared: &shared,
                };
                let value = lowering.output::<B>(value)?;
                let order = Order {
                    value,
                    descending: *descending,
                };
                q.order(rows, vec![order])?
            }
            None => rows,
        };
        q.limit(rows, Some(plan.limit), None)
    };
    if let Some(table) = &plan.fused_table {
        let (rows, columns) = fused(q, plan, table)?;
        let rows = tail(q, rows)?;
        return q.select(rows, columns);
    }
    let arms = arms(plan);
    let rows = match arms.iter().any(|arm| arm.bound.is_some()) {
        true => Some(center_rows(q, plan)?),
        false => None,
    };
    let context = Context {
        plan,
        center_rows: rows,
    };
    if let [only] = arms.as_slice() {
        let (rows, columns) = context.arm(q, only)?;
        let rows = tail(q, rows)?;
        return q.select(rows, columns);
    }
    let mut built = Vec::new();
    for arm in &arms {
        let (rows, columns) = context.arm(q, arm)?;
        built.push(q.select(rows, columns)?);
    }
    let rows = q.union(built)?;
    tail(q, rows)
}

fn center_rows<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Neighbors<'_>,
) -> Built<Subquery> {
    let center = &plan.center;
    let table = center.table.unwrap_or_default();
    let alias = center_rows_cte();
    q.cte(alias, !stored::<B>(), |q| {
        let latest = q.subquery(alias, |q| {
            let rows = scan(q, table, alias, false)?;
            let id = col(&[&rows], alias, DEFAULT_PRIMARY_KEY)?;
            let rows = q.filter(rows, ids::<B>(id, &center.pinned))?;
            let keys = plan
                .center_sort_key
                .iter()
                .map(|key| rows.qualified(alias, key))
                .collect::<Built<Vec<_>>>()?;
            let rows = match stored::<B>() {
                true => {
                    let orders = keys.into_iter().map(|key| Order {
                        value: key.expr(),
                        descending: false,
                    });
                    q.order(rows, orders.collect())?
                }
                false => {
                    let version = rows.qualified(alias, VERSION_COLUMN)?;
                    q.latest(rows, keys, version)?
                }
            };
            q.select_all(rows)
        })?;
        let rows = q.read(&latest, Some(alias))?;
        let live = deleted::<B>(&[&rows], alias)?;
        let rows = filtered(q, rows, vec![live])?;
        let names = [DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN];
        let items = columns(&[&rows], alias, names, false)?;
        select(q, rows, items)
    })
}

struct Context<'p> {
    plan: &'p Neighbors<'p>,
    center_rows: Option<Subquery>,
}

impl Context<'_> {
    fn lookup<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        value: Expr,
        column: &str,
    ) -> Built<Expr> {
        let body = self.center_rows.clone().ok_or(
            query_data_model::implementations::relational::plan::RelationalError::Incomplete(
                "the center rows",
            ),
        )?;
        let alias = center_rows_cte();
        let keys = q.subquery("subquery", |q| {
            let rows = q.read(&body, Some(alias))?;
            let items = columns(&[&rows], alias, [column], false)?;
            select(q, rows, items)
        })?;
        Ok(keys.contains(value))
    }

    fn arm<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        arm: &Arm<'_>,
    ) -> Built<(Relation, Vec<Output>)> {
        let plan = self.plan;
        let center = &plan.center;
        let (center_alias, entity) = (center.alias, center.entity);
        let table = center.table.unwrap_or_default();
        let edge = neighbor_edge_alias();
        let (center_edge, center_kind, neighbor_id, neighbor_type, outgoing, direction) =
            match arm.direction {
                Direction::Outgoing => (
                    SOURCE_ID_COLUMN,
                    SOURCE_KIND_COLUMN,
                    TARGET_ID_COLUMN,
                    TARGET_KIND_COLUMN,
                    1,
                    DenormalizedDirection::Source,
                ),
                Direction::Incoming => (
                    TARGET_ID_COLUMN,
                    TARGET_KIND_COLUMN,
                    SOURCE_ID_COLUMN,
                    SOURCE_KIND_COLUMN,
                    0,
                    DenormalizedDirection::Target,
                ),
                Direction::Both => unreachable!(),
            };
        let arm_where = |q: &mut RelationalBuilder<'_, B>,
                         scope: &Scope<'_>,
                         alias: &str|
         -> Built<Vec<Option<Expr>>> {
            let mut parts = vec![Some(eq(col(scope, alias, center_kind)?, string(entity)))];
            if !center.pinned.is_empty() {
                parts.push(Some(ids::<B>(
                    col(scope, alias, center_edge)?,
                    &center.pinned,
                )));
            }
            if let Some(kinds) = relationship_kinds::<B>(scope, alias, arm.kinds)? {
                parts.push(Some(kinds));
            }
            match arm.bound {
                Some(CenterBound::Path) => {
                    let value = col(scope, alias, TRAVERSAL_PATH_COLUMN)?;
                    parts.push(Some(self.lookup(q, value, TRAVERSAL_PATH_COLUMN)?));
                }
                Some(CenterBound::Id) => {
                    let value = col(scope, alias, center_edge)?;
                    parts.push(Some(self.lookup(q, value, DEFAULT_PRIMARY_KEY)?));
                }
                None => {}
            }
            parts.push(deleted::<B>(scope, alias)?);
            Ok(parts)
        };
        let (tables, paths) = match arm.direction {
            Direction::Outgoing => (&plan.outgoing_tables, &plan.outgoing_neighbor_paths),
            _ => (&plan.incoming_tables, &plan.incoming_neighbor_paths),
        };
        let (mut rows, outer) = edge_scan(q, tables, edge, arm_where)?;
        let mut tail = outer;
        let needs_center_table = !center.uses_default_pk();
        if plan.has_non_denorm {
            let mut names = vec![DEFAULT_PRIMARY_KEY, DELETED_COLUMN];
            if needs_center_table {
                names.push(center.redaction);
            }
            let (joined, live) = dedup(q, center_alias, table, &names, |scope| {
                let mut parts = Vec::new();
                for filter in &plan.filters {
                    let column = Column::new(center_alias, &filter.property);
                    parts.push(filter.filter.lower::<B>(scope, &column)?);
                }
                if !center.pinned.is_empty() {
                    let id = col(scope, center_alias, DEFAULT_PRIMARY_KEY)?;
                    parts.push(ids::<B>(id, &center.pinned));
                }
                if let Some((start, end)) = center.range {
                    let id = col(scope, center_alias, DEFAULT_PRIMARY_KEY)?;
                    parts.push(id_range(id, start, end));
                }
                Ok(parts)
            })?;
            let on = eq(
                col(&[&rows, &joined], edge, center_edge)?,
                col(&[&rows, &joined], center_alias, DEFAULT_PRIMARY_KEY)?,
            );
            rows = q.join(rows, joined, JoinKind::Inner(on))?;
            tail.push(live);
        }
        if !center.uses_default_pk() && !plan.has_non_denorm {
            let joined = scan(q, table, center_alias, true)?;
            let on = eq(
                col(&[&rows, &joined], edge, center_edge)?,
                col(&[&rows, &joined], center_alias, DEFAULT_PRIMARY_KEY)?,
            );
            rows = q.join(rows, joined, JoinKind::Inner(on))?;
            tail.push(deleted::<B>(&[&rows], center_alias)?);
        }
        let scope = [&rows];
        let mut predicates = Vec::new();
        for tag in plan
            .filters
            .iter()
            .filter_map(|filter| filter.tag(direction))
        {
            let column = col(&scope, edge, &tag.facts.edge_column)?;
            predicates.push(Some(tags(column, &tag.values)));
        }
        predicates.extend(tail);
        let mut items = vec![
            (
                neighbor_id_column().to_string(),
                col(&scope, edge, neighbor_id)?,
            ),
            (
                neighbor_type_column().to_string(),
                col(&scope, edge, neighbor_type)?,
            ),
            (
                relationship_type_column().to_string(),
                col(&scope, edge, RELATIONSHIP_KIND_COLUMN)?,
            ),
            (neighbor_is_outgoing_column().to_string(), int(outgoing)),
            (
                neighbor_traversal_path_column().to_string(),
                neighbor_path(&scope, edge, neighbor_type, paths)?,
            ),
        ];
        if center.uses_default_pk() {
            items.push((
                redaction_id_column(center_alias),
                col(&scope, edge, center_edge)?,
            ));
        } else {
            let redaction = col(&scope, center_alias, center.redaction)?;
            items.push((redaction_id_column(center_alias), redaction));
            let pk = col(&scope, center_alias, DEFAULT_PRIMARY_KEY)?;
            items.push((primary_key_column(center_alias), pk));
        }
        items.push((redaction_type_column(center_alias), string(entity)));
        if center.has_traversal_path {
            let path = col(&scope, edge, TRAVERSAL_PATH_COLUMN)?;
            items.push((traversal_path_column(center_alias), path));
        }
        let rows = filtered(q, rows, predicates)?;
        computed(q, rows, items)
    }
}

fn neighbor_path(
    scope: &Scope<'_>,
    edge: &str,
    neighbor_type: &str,
    owned: &[(String, String)],
) -> Built<Expr> {
    let mut pairs = Vec::new();
    for (relationship, entity) in owned {
        pairs.push(and(
            eq(
                col(scope, edge, RELATIONSHIP_KIND_COLUMN)?,
                string(relationship),
            ),
            eq(col(scope, edge, neighbor_type)?, string(entity)),
        ));
    }
    Ok(match disjoin(pairs) {
        Some(owned) => call(
            Function::If,
            vec![owned, col(scope, edge, TRAVERSAL_PATH_COLUMN)?, string("")],
        ),
        None => string(""),
    })
}

const ROW_COLUMN: &str = "_orbit_arm_row";

/// Direction::Both collapsed into a single edge scan (see `fused_both_eligible`).
///
/// Inner query: scan the edge once with `WHERE (source side) OR (target side)`,
/// projecting `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each
/// row yields one entry per matched arm. Outer query: project the `_orbit_*`
/// columns out of the tuple. `arrayFilter` (not `multiIf`) keeps self-loop
/// semantics: when both arms match the same edge, both rows are emitted.
fn fused<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Neighbors<'_>,
    table: &str,
) -> Built<(Relation, Vec<Output>)> {
    let center = &plan.center;
    let (center_alias, entity) = (center.alias, center.entity);
    let edge = neighbor_edge_alias();
    let relationship = relationship_type_column();
    let path = traversal_path_column(center_alias);
    let inner = "_orbit_fused";
    let body = q.subquery(inner, |q| {
        let rows = scan(q, table, edge, false)?;
        let scope = [&rows];
        let side = |kind: &str, id: &str, direction| -> Built<Expr> {
            let mut parts = vec![eq(col(&scope, edge, kind)?, string(entity))];
            if !center.pinned.is_empty() {
                parts.push(ids::<B>(col(&scope, edge, id)?, &center.pinned));
            }
            for tag in plan
                .filters
                .iter()
                .filter_map(|filter| filter.tag(direction))
            {
                parts.push(tags(
                    col(&scope, edge, &tag.facts.edge_column)?,
                    &tag.values,
                ));
            }
            Ok(conjoin(parts).expect("fused arm predicate always has the center-kind conjunct"))
        };
        let source = || {
            side(
                SOURCE_KIND_COLUMN,
                SOURCE_ID_COLUMN,
                DenormalizedDirection::Source,
            )
        };
        let target = || {
            side(
                TARGET_KIND_COLUMN,
                TARGET_ID_COLUMN,
                DenormalizedDirection::Target,
            )
        };
        let tuple = |matched: Expr, outgoing: i64, neighbor: &str, kind: &str, own: &str| {
            Ok::<_, query_data_model::implementations::relational::plan::RelationalError>(call(
                Function::Tuple,
                vec![
                    matched,
                    int(outgoing),
                    col(&scope, edge, neighbor)?,
                    col(&scope, edge, kind)?,
                    col(&scope, edge, own)?,
                ],
            ))
        };
        let outgoing = tuple(
            source()?,
            1,
            TARGET_ID_COLUMN,
            TARGET_KIND_COLUMN,
            SOURCE_ID_COLUMN,
        )?;
        let incoming = tuple(
            target()?,
            0,
            SOURCE_ID_COLUMN,
            SOURCE_KIND_COLUMN,
            TARGET_ID_COLUMN,
        )?;
        let matched = Expr::Lambda {
            param: "_orbit_arm".to_string(),
            body: Box::new(call(
                Function::TupleElement,
                vec![Expr::Variable("_orbit_arm".to_string()), int(1)],
            )),
        };
        let row = call(
            Function::Unnest,
            vec![call(
                Function::ArrayFilter,
                vec![matched, call(Function::Array, vec![outgoing, incoming])],
            )],
        );
        let mut items = vec![
            (ROW_COLUMN.to_string(), row),
            (
                relationship.to_string(),
                col(&scope, edge, RELATIONSHIP_KIND_COLUMN)?,
            ),
        ];
        if center.has_traversal_path {
            items.push((path.clone(), col(&scope, edge, TRAVERSAL_PATH_COLUMN)?));
        }
        let mut predicates = vec![Some(binary(BinaryOp::Or, source()?, target()?))];
        if let Some(kinds) = relationship_kinds::<B>(&scope, edge, &plan.kinds)? {
            predicates.push(Some(kinds));
        }
        predicates.push(deleted::<B>(&scope, edge)?);
        let rows = filtered(q, rows, predicates)?;
        select(q, rows, items)
    })?;
    let rows = q.read(&body, Some(inner))?;
    let scope = [&rows];
    let element = |position: i64| -> Built<Expr> {
        Ok(call(
            Function::TupleElement,
            vec![col(&scope, inner, ROW_COLUMN)?, int(position)],
        ))
    };
    let mut items = vec![
        (neighbor_id_column().to_string(), element(3)?),
        (neighbor_type_column().to_string(), element(4)?),
        (relationship.to_string(), col(&scope, inner, relationship)?),
        (neighbor_is_outgoing_column().to_string(), element(2)?),
        (redaction_id_column(center_alias), element(5)?),
        (redaction_type_column(center_alias), string(entity)),
    ];
    if center.has_traversal_path {
        items.push((path.clone(), col(&scope, inner, &path)?));
    }
    computed(q, rows, items)
}
