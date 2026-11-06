//! Neighbors emits its own redaction columns directly (rather than relying on
//! the result-contract pass's _orbit_* injection) since the center edge column differs
//! per direction arm.

use ontology::constants::*;
use query_data_model::DenormalizedDirection;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Column as Output, Expr, Function, JoinKind, Order, Relation, RelationalBuilder,
    Subquery,
};

use crate::ast::{self, OrderExpr};
use crate::constants::*;
use crate::input::*;
use crate::passes::lower::build::expr::{id_range, ids, relationship_kinds};
use crate::passes::lower::build::predicates::{Lowering, Shared};
use crate::passes::lower::build::sources::{
    Scope, bound_filter, columns, dedup, deleted, denorm_tags, edge_scan, select,
};
use crate::passes::lower::build::{
    Built, and, binary, call, col, computed, conjoin, disjoin, eq, filtered, int, scan, stored,
    string,
};
use crate::passes::plan::predicates::Column;
use crate::passes::plan::{Neighbors, Plan, property_value};

pub(crate) fn stable_order(plan: &Plan<Neighbors>) -> Vec<OrderExpr> {
    match plan.operation.direction {
        Direction::Both => vec![
            OrderExpr::asc(ast::Expr::ident(redaction_id_column(
                &plan.operation.center,
            ))),
            OrderExpr::asc(ast::Expr::ident(neighbor_id_column())),
            OrderExpr::asc(ast::Expr::ident(relationship_type_column())),
            OrderExpr::asc(ast::Expr::ident(neighbor_is_outgoing_column())),
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
        .map(|column| OrderExpr::asc(ast::Expr::col(neighbor_edge_alias(), column)))
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

fn arms<'p>(plan: &'p Neighbors, center: &[i64], center_has_tp: bool) -> Vec<Arm<'p>> {
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
    plan: &Plan<Neighbors>,
    input: &Input,
) -> Built<Relation> {
    let operation = &plan.operation;
    let center = &plan.nodes[&operation.center];
    let order = input.order_by.as_ref().map(|order| {
        let value = property_value(
            &plan.nodes,
            &order.node,
            Column::new(&order.node, &order.property),
        );
        (value, order.direction == OrderDirection::Desc)
    });
    let tail = |q: &mut RelationalBuilder<'_, B>, rows: Relation| -> Built<Relation> {
        let rows = match &order {
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
        q.limit(rows, Some(input.limit), None)
    };
    if let Some(table) = &operation.fused_table {
        let (rows, columns) = fused(q, plan, table)?;
        let rows = tail(q, rows)?;
        return q.select(rows, columns);
    }
    let arms = arms(operation, &center.node_ids, center.has_traversal_path);
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
    plan: &Plan<Neighbors>,
) -> Built<Subquery> {
    let center = &plan.nodes[&plan.operation.center];
    let table = center.table.clone().unwrap_or_default();
    let alias = center_rows_cte();
    q.cte(alias, !stored::<B>(), |q| {
        let latest = q.subquery(alias, |q| {
            let rows = scan(q, &table, alias, false)?;
            let id = col(&[&rows], alias, DEFAULT_PRIMARY_KEY)?;
            let rows = q.filter(rows, ids::<B>(id, &center.node_ids))?;
            let keys = plan
                .operation
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
    plan: &'p Plan<Neighbors>,
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
        let operation = &self.plan.operation;
        let center_alias = operation.center.as_str();
        let center = &self.plan.nodes[center_alias];
        let entity = center.entity.clone().unwrap_or_default();
        let table = center.table.clone().unwrap_or_default();
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
            let mut parts = vec![Some(eq(col(scope, alias, center_kind)?, string(&entity)))];
            if !center.node_ids.is_empty() {
                parts.push(Some(ids::<B>(
                    col(scope, alias, center_edge)?,
                    &center.node_ids,
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
            Direction::Outgoing => (
                &operation.edge.outgoing_tables,
                &operation.outgoing_neighbor_paths,
            ),
            _ => (
                &operation.edge.incoming_tables,
                &operation.incoming_neighbor_paths,
            ),
        };
        let (mut rows, outer) = edge_scan(q, tables, edge, arm_where)?;
        let mut tail = outer;
        let needs_center_table = !center.uses_default_pk();
        if operation.has_non_denorm {
            let mut names = vec![DEFAULT_PRIMARY_KEY, DELETED_COLUMN];
            if needs_center_table {
                names.push(center.redaction_id_column.as_str());
            }
            let (joined, live) = dedup(q, center_alias, &table, &names, |scope| {
                let mut parts = Vec::new();
                for (property, filter) in &center.filters {
                    parts.push(bound_filter::<B>(scope, center_alias, property, filter)?);
                }
                if !center.node_ids.is_empty() {
                    let id = col(scope, center_alias, DEFAULT_PRIMARY_KEY)?;
                    parts.push(ids::<B>(id, &center.node_ids));
                }
                if let Some(range) = &center.id_range {
                    let id = col(scope, center_alias, DEFAULT_PRIMARY_KEY)?;
                    parts.push(id_range(id, range.start, range.end));
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
        if !center.uses_default_pk() && !operation.has_non_denorm {
            let joined = scan(q, &table, center_alias, true)?;
            let on = eq(
                col(&[&rows, &joined], edge, center_edge)?,
                col(&[&rows, &joined], center_alias, DEFAULT_PRIMARY_KEY)?,
            );
            rows = q.join(rows, joined, JoinKind::Inner(on))?;
            tail.push(deleted::<B>(&[&rows], center_alias)?);
        }
        let scope = [&rows];
        let mut predicates = Vec::new();
        for (_, filter) in &center.filters {
            let Some(property) = filter.property else {
                continue;
            };
            let key = query_data_model::DenormalizedKey {
                property,
                direction,
            };
            if let Some(facts) = self.plan.denormalized.get(&key)
                && let Some(tags) = denorm_tags(
                    &scope,
                    edge,
                    &facts.edge_column,
                    &facts.tag_key,
                    &filter.filter,
                )?
            {
                predicates.push(Some(tags));
            }
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
            let redaction = col(&scope, center_alias, &center.redaction_id_column)?;
            items.push((redaction_id_column(center_alias), redaction));
            let pk = col(&scope, center_alias, DEFAULT_PRIMARY_KEY)?;
            items.push((primary_key_column(center_alias), pk));
        }
        items.push((redaction_type_column(center_alias), string(&entity)));
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
    plan: &Plan<Neighbors>,
    table: &str,
) -> Built<(Relation, Vec<Output>)> {
    let center_alias = plan.operation.center.as_str();
    let center = &plan.nodes[center_alias];
    let entity = center.entity.clone().unwrap_or_default();
    let edge = neighbor_edge_alias();
    let relationship = relationship_type_column();
    let path = traversal_path_column(center_alias);
    let inner = "_orbit_fused";
    let body = q.subquery(inner, |q| {
        let rows = scan(q, table, edge, false)?;
        let scope = [&rows];
        let side = |kind: &str, id: &str, direction| -> Built<Expr> {
            let mut parts = vec![eq(col(&scope, edge, kind)?, string(&entity))];
            if !center.node_ids.is_empty() {
                parts.push(ids::<B>(col(&scope, edge, id)?, &center.node_ids));
            }
            for (_, filter) in &center.filters {
                let Some(property) = filter.property else {
                    continue;
                };
                let key = query_data_model::DenormalizedKey {
                    property,
                    direction,
                };
                if let Some(facts) = plan.denormalized.get(&key)
                    && let Some(tags) = denorm_tags(
                        &scope,
                        edge,
                        &facts.edge_column,
                        &facts.tag_key,
                        &filter.filter,
                    )?
                {
                    parts.push(tags);
                }
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
        if let Some(types) = &plan.operation.edge.rel_type_filter
            && let Some(kinds) = relationship_kinds::<B>(&scope, edge, types)?
        {
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
        (redaction_type_column(center_alias), string(&entity)),
    ];
    if center.has_traversal_path {
        items.push((path.clone(), col(&scope, inner, &path)?));
    }
    computed(q, rows, items)
}
