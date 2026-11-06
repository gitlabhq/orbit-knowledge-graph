//! Generates forward + backward frontier CTEs (UNION ALL of depth arms),
//! then combines via direct (depth-1) + intersection (forward meets backward).
//! Dedup is baked into anchor CTEs.

use std::collections::HashMap;

use ontology::constants::*;
use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, JoinKind, Order, Relation, RelationalBuilder, Subquery,
};
use serde_json::Value;

use crate::ast::{self, OrderExpr};
use crate::constants::*;
use crate::input::*;
use crate::passes::lower::build::expr::{id_range, ids, in_list, relationship_kinds};
use crate::passes::lower::build::sources::{
    Scope, bound_filter, columns, dedup, denorm_tags, final_edge_scan, select,
};
use crate::passes::lower::build::{
    Built, and, binary, call, col, computed, conjoin, eq, filtered, int, membership, stored, string,
};
use crate::passes::plan::{NodePlan, PathFinding, Plan};

pub(crate) fn stable_order() -> Vec<OrderExpr> {
    let text = |column: &str| {
        OrderExpr::asc(ast::Expr::func(
            ast::Function::ToString,
            vec![ast::Expr::col(paths_alias(), column)],
        ))
    };
    vec![text(path_column()), text(edge_kinds_column())]
}

enum AnchorFilter {
    Ids(Vec<i64>),
    Shared(String),
}

struct Anchor {
    filter: Option<AnchorFilter>,
    shared: Option<String>,
    has_tp: bool,
}

#[derive(Default)]
struct Bodies(Vec<(String, Subquery)>);

impl Bodies {
    fn get(&self, name: &str) -> Built<&Subquery> {
        self.0
            .iter()
            .find(|(body, _)| body == name)
            .map(|(_, body)| body)
            .ok_or(
                query_data_model::implementations::relational::plan::RelationalError::UnknownTable(
                    name.to_string(),
                ),
            )
    }

    fn member<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        value: Expr,
        name: &str,
        key: &str,
    ) -> Built<Expr> {
        let body = self.get(name)?.clone();
        membership(q, value, &body, name, &[key])
    }
}

fn anchor_filter<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    bodies: &Bodies,
    filter: &AnchorFilter,
    value: Expr,
) -> Built<Expr> {
    match filter {
        AnchorFilter::Ids(values) => {
            let values = values.iter().map(|id| Value::from(*id)).collect();
            Ok(in_list::<B>(value, SqlType::Int64, values)
                .expect("pinned anchors carry at least one id"))
        }
        AnchorFilter::Shared(name) => bodies.member(q, value, name, DEFAULT_PRIMARY_KEY),
    }
}

fn anchor<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    bodies: &mut Bodies,
    node: &NodePlan,
    force_cte: bool,
) -> Built<Anchor> {
    let alias = &node.alias;
    let table = node.table.as_deref().unwrap_or("");
    if !force_cte && node.is_id_pinned_only() {
        return Ok(Anchor {
            filter: (!node.node_ids.is_empty()).then(|| AnchorFilter::Ids(node.node_ids.clone())),
            shared: None,
            has_tp: false,
        });
    }
    if node.node_ids.is_empty() && node.filters.is_empty() && node.id_range.is_none() {
        return Ok(Anchor {
            filter: None,
            shared: None,
            has_tp: false,
        });
    }
    let name = node_filter_cte(alias);
    let has_tp = node.has_traversal_path;
    let body = q.cte(&name, false, |q| {
        let mut names = vec![DEFAULT_PRIMARY_KEY];
        if has_tp {
            names.push(TRAVERSAL_PATH_COLUMN);
        }
        let mut inner = names.clone();
        inner.push(DELETED_COLUMN);
        let (rows, live) = dedup(q, alias, table, &inner, |scope| {
            let mut parts = Vec::new();
            for (property, filter) in &node.filters {
                parts.push(bound_filter::<B>(scope, alias, property, filter)?);
            }
            if !node.node_ids.is_empty() {
                let id = col(scope, alias, DEFAULT_PRIMARY_KEY)?;
                parts.push(ids::<B>(id, &node.node_ids));
            }
            if let Some(range) = &node.id_range {
                let id = col(scope, alias, DEFAULT_PRIMARY_KEY)?;
                parts.push(id_range(id, range.start, range.end));
            }
            Ok(parts)
        })?;
        let rows = filtered(q, rows, vec![live])?;
        let items = columns(&[&rows], alias, names, false)?;
        let (rows, outputs) = computed(q, rows, items)?;
        let order = Order {
            value: col(&[&rows], alias, DEFAULT_PRIMARY_KEY)?,
            descending: false,
        };
        let rows = q.order(rows, vec![order])?;
        let limit = crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32;
        let rows = q.limit(rows, Some(limit), None)?;
        q.select(rows, outputs)
    })?;
    bodies.0.push((name.clone(), body));
    Ok(Anchor {
        filter: Some(AnchorFilter::Shared(name.clone())),
        shared: Some(name),
        has_tp,
    })
}

fn scope_cte<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    bodies: &mut Bodies,
    start: &Anchor,
    end: &Anchor,
) -> Built<Option<String>> {
    let (Some(start_cte), Some(end_cte)) = (&start.shared, &end.shared) else {
        return Ok(None);
    };
    if !start.has_tp || !end.has_tp {
        return Ok(None);
    }
    let (start_cte, end_cte) = (bodies.get(start_cte)?.clone(), bodies.get(end_cte)?.clone());
    let body = q.cte(path_scope_cte(), false, |q| {
        let mut arms = Vec::new();
        for (cte, alias) in [
            (&start_cte, path_scope_start_alias()),
            (&end_cte, path_scope_end_alias()),
        ] {
            let rows = q.read(cte, Some(alias))?;
            let path = col(&[&rows], alias, TRAVERSAL_PATH_COLUMN)?;
            let aggregated = q.aggregate(rows, vec![path], Vec::new())?;
            let items = vec![(
                TRAVERSAL_PATH_COLUMN.to_string(),
                aggregated.groups[0].expr(),
            )];
            arms.push(select(q, aggregated.rows, items)?);
        }
        q.union(arms)
    })?;
    bodies.0.push((path_scope_cte().to_string(), body));
    Ok(Some(path_scope_cte().to_string()))
}

#[derive(Clone, Copy, PartialEq)]
enum Way {
    Forward,
    Backward,
}

struct Frontier<'a> {
    rel_type_filter: &'a Option<Vec<String>>,
    anchor_entity: &'a str,
    edge_tables: &'a [String],
    scope: Option<&'a str>,
    include_tp: bool,
    anchor: &'a Anchor,
    tags: &'a [(String, crate::passes::plan::BoundFilter)],
    tag_direction: query_data_model::DenormalizedDirection,
    denormalized:
        &'a HashMap<query_data_model::DenormalizedKey, query_data_model::DenormalizedProperty>,
}

fn frontier<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    bodies: &Bodies,
    max_depth: u32,
    way: Way,
    options: &Frontier<'_>,
) -> Built<Relation> {
    let mut arms = Vec::new();
    for depth in 1..=max_depth {
        arms.push(frontier_arm(q, bodies, depth, way, options)?);
    }
    match arms.len() {
        1 => Ok(arms.remove(0)),
        _ => q.union(arms),
    }
}

fn frontier_arm<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    bodies: &Bodies,
    depth: u32,
    way: Way,
    options: &Frontier<'_>,
) -> Built<Relation> {
    let (anchor_col, next_col, next_kind_col, anchor_kind_col) = match way {
        Way::Forward => (
            SOURCE_ID_COLUMN,
            TARGET_ID_COLUMN,
            TARGET_KIND_COLUMN,
            SOURCE_KIND_COLUMN,
        ),
        Way::Backward => (
            TARGET_ID_COLUMN,
            SOURCE_ID_COLUMN,
            SOURCE_KIND_COLUMN,
            TARGET_KIND_COLUMN,
        ),
    };
    let kinds = options.rel_type_filter.as_deref().unwrap_or(&[]);
    let first = hop_alias(1);
    let last = hop_alias(depth);
    let mut rows = final_edge_scan(q, options.edge_tables, &first)?;
    for index in 2..=depth {
        let previous = hop_alias(index - 1);
        let current = hop_alias(index);
        let next = final_edge_scan(q, options.edge_tables, &current)?;
        let scope = [&rows, &next];
        let mut on = and(
            eq(
                col(&scope, &previous, next_col)?,
                col(&scope, &current, anchor_col)?,
            ),
            eq(
                col(&scope, &previous, next_kind_col)?,
                col(&scope, &current, anchor_kind_col)?,
            ),
        );
        if options.include_tp {
            on = and(
                on,
                eq(
                    col(&scope, &previous, TRAVERSAL_PATH_COLUMN)?,
                    col(&scope, &current, TRAVERSAL_PATH_COLUMN)?,
                ),
            );
        }
        if let Some(kinds) = relationship_kinds::<B>(&scope, &current, kinds)? {
            on = and(on, kinds);
        }
        if let Some(shared) = options.scope {
            let path = col(&scope, &current, TRAVERSAL_PATH_COLUMN)?;
            on = and(on, bodies.member(q, path, shared, TRAVERSAL_PATH_COLUMN)?);
        }
        rows = q.join(rows, next, JoinKind::Inner(on))?;
    }
    let scope = [&rows];
    let mut predicates = Vec::new();
    if let Some(filter) = &options.anchor.filter {
        let value = col(&scope, &first, anchor_col)?;
        predicates.push(anchor_filter(q, bodies, filter, value)?);
    }
    predicates.extend(relationship_kinds::<B>(&scope, &first, kinds)?);
    predicates.push(eq(
        col(&scope, &first, anchor_kind_col)?,
        string(options.anchor_entity),
    ));
    if let Some(shared) = options.scope {
        let path = col(&scope, &first, TRAVERSAL_PATH_COLUMN)?;
        predicates.push(bodies.member(q, path, shared, TRAVERSAL_PATH_COLUMN)?);
    }
    predicates.extend(tags(&scope, &first, options)?);
    let range = match way {
        Way::Forward => 1..=depth,
        Way::Backward => 1..=depth.saturating_sub(1),
    };
    let mut steps = Vec::new();
    for index in range {
        let hop = hop_alias(index);
        let pair = vec![
            col(&scope, &hop, next_col)?,
            col(&scope, &hop, next_kind_col)?,
        ];
        steps.push(call(Function::Tuple, pair));
    }
    let path_nodes = match steps.is_empty() {
        true => Expr::EmptyArray(vec![SqlType::Int64, SqlType::String]),
        false => call(Function::Array, steps),
    };
    let mut kinds_of = Vec::new();
    for index in 1..=depth {
        kinds_of.push(col(&scope, &hop_alias(index), RELATIONSHIP_KIND_COLUMN)?);
    }
    let mut items = vec![
        (
            anchor_node_column().to_string(),
            col(&scope, &first, anchor_col)?,
        ),
        (end_node_column().to_string(), col(&scope, &last, next_col)?),
        (
            end_kind_column().to_string(),
            col(&scope, &last, next_kind_col)?,
        ),
        (path_nodes_column().to_string(), path_nodes),
        (
            frontier_edge_kinds_column().to_string(),
            call(Function::Array, kinds_of),
        ),
        (depth_column().to_string(), int(i64::from(depth))),
    ];
    if options.include_tp {
        let path = col(&scope, &first, TRAVERSAL_PATH_COLUMN)?;
        items.push((TRAVERSAL_PATH_COLUMN.to_string(), path));
    }
    let rows = filtered(q, rows, vec![conjoin(predicates)])?;
    select(q, rows, items)
}

fn tags(scope: &Scope<'_>, alias: &str, options: &Frontier<'_>) -> Built<Vec<Expr>> {
    let mut found = Vec::new();
    for (_, filter) in options.tags {
        let Some(property) = filter.property else {
            continue;
        };
        let key = query_data_model::DenormalizedKey {
            property,
            direction: options.tag_direction,
        };
        if let Some(facts) = options.denormalized.get(&key)
            && let Some(tags) = denorm_tags(
                scope,
                alias,
                &facts.edge_column,
                &facts.tag_key,
                &filter.filter,
            )?
        {
            found.push(tags);
        }
    }
    Ok(found)
}

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<PathFinding>,
    input: &Input,
) -> Built<Relation> {
    let pf = &plan.operation;
    let start = &plan.nodes[&pf.start];
    let end = &plan.nodes[&pf.end];
    let mut bodies = Bodies::default();
    let start_anchor = anchor(q, &mut bodies, start, pf.scoped_by_tp)?;
    let end_anchor = anchor(q, &mut bodies, end, pf.scoped_by_tp)?;
    let scope = scope_cte(q, &mut bodies, &start_anchor, &end_anchor)?;
    let start_entity = start.entity.as_deref().unwrap_or("");
    let end_entity = end.entity.as_deref().unwrap_or("");
    let options = Frontier {
        rel_type_filter: &pf.edge.rel_type_filter,
        anchor_entity: start_entity,
        edge_tables: &pf.edge.tables,
        scope: scope.as_deref(),
        include_tp: pf.scoped_by_tp,
        anchor: &start_anchor,
        tags: &start.filters,
        tag_direction: query_data_model::DenormalizedDirection::Source,
        denormalized: &plan.denormalized,
    };
    let forward = q.cte(forward_cte(), false, |q| {
        frontier(q, &bodies, pf.forward_depth, Way::Forward, &options)
    })?;
    let backward = match pf.backward_depth > 0 {
        true => {
            let options = Frontier {
                anchor_entity: end_entity,
                anchor: &end_anchor,
                tags: &end.filters,
                tag_direction: query_data_model::DenormalizedDirection::Target,
                ..options
            };
            Some(q.cte(backward_cte(), false, |q| {
                frontier(q, &bodies, pf.backward_depth, Way::Backward, &options)
            })?)
        }
        false => None,
    };
    let start_tuple = |scope: &Scope<'_>, alias: &str| -> Built<Expr> {
        let node = col(scope, alias, anchor_node_column())?;
        Ok(call(Function::Tuple, vec![node, string(start_entity)]))
    };
    let paths = q.subquery(paths_alias(), |q| {
        let rows = q.read(&forward, Some(forward_cte()))?;
        let scope = [&rows];
        let mut predicates = vec![
            binary(
                BinaryOp::Eq,
                col(&scope, forward_cte(), depth_column())?,
                int(1),
            ),
            eq(
                col(&scope, forward_cte(), end_kind_column())?,
                string(end_entity),
            ),
        ];
        let reached = col(&scope, forward_cte(), end_node_column())?;
        if end.is_id_pinned_only() {
            let values = end.node_ids.iter().map(|id| Value::from(*id)).collect();
            predicates.extend(in_list::<B>(reached, SqlType::Int64, values));
        } else if !end.filters.is_empty() || end.id_range.is_some() {
            let name = node_filter_cte(&end.alias);
            predicates.push(bodies.member(q, reached, &name, DEFAULT_PRIMARY_KEY)?);
        }
        let items = vec![
            (
                path_start_column().to_string(),
                col(&scope, forward_cte(), anchor_node_column())?,
            ),
            (
                path_end_column().to_string(),
                col(&scope, forward_cte(), end_node_column())?,
            ),
            (
                depth_column().to_string(),
                col(&scope, forward_cte(), depth_column())?,
            ),
            (
                path_column().to_string(),
                call(
                    Function::ArrayConcat,
                    vec![
                        call(Function::Array, vec![start_tuple(&scope, forward_cte())?]),
                        col(&scope, forward_cte(), path_nodes_column())?,
                    ],
                ),
            ),
            (
                edge_kinds_column().to_string(),
                col(&scope, forward_cte(), frontier_edge_kinds_column())?,
            ),
        ];
        let rows = filtered(q, rows, vec![conjoin(predicates)])?;
        let direct = select(q, rows, items)?;
        let Some(backward) = &backward else {
            return Ok(direct);
        };
        let intersection = intersection(q, plan, &forward, backward, start_entity, end_entity)?;
        q.union(vec![direct, intersection])
    })?;
    let rows = q.read(&paths, Some(paths_alias()))?;
    let scope = [&rows];
    let names = [path_column(), edge_kinds_column(), depth_column()];
    let items = columns(&scope, paths_alias(), names, false)?;
    let (rows, outputs) = computed(q, rows, items)?;
    let order = Order {
        value: col(&[&rows], paths_alias(), depth_column())?,
        descending: false,
    };
    let rows = q.order(rows, vec![order])?;
    let rows = match stored::<B>() {
        true => rows,
        false => {
            let by = vec![
                col(&[&rows], paths_alias(), path_start_column())?,
                col(&[&rows], paths_alias(), path_end_column())?,
            ];
            q.limit_by(rows, 1, by)?
        }
    };
    let rows = q.limit(rows, Some(input.limit), None)?;
    q.select(rows, outputs)
}

fn intersection<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<PathFinding>,
    forward: &Subquery,
    backward: &Subquery,
    start_entity: &str,
    end_entity: &str,
) -> Built<Relation> {
    let (f, b) = (forward_cte(), backward_cte());
    let left = q.read(forward, Some(f))?;
    let right = q.read(backward, Some(b))?;
    let scope = [&left, &right];
    let mut on = and(
        eq(
            col(&scope, f, end_node_column())?,
            col(&scope, b, end_node_column())?,
        ),
        eq(
            col(&scope, f, end_kind_column())?,
            col(&scope, b, end_kind_column())?,
        ),
    );
    if plan.operation.scoped_by_tp {
        on = and(
            on,
            eq(
                col(&scope, f, TRAVERSAL_PATH_COLUMN)?,
                col(&scope, b, TRAVERSAL_PATH_COLUMN)?,
            ),
        );
    }
    let rows = q.join(left, right, JoinKind::Inner(on))?;
    let scope = [&rows];
    let depth = || -> Built<Expr> {
        Ok(binary(
            BinaryOp::Add,
            col(&scope, f, depth_column())?,
            col(&scope, b, depth_column())?,
        ))
    };
    let tuple = |alias: &str, entity: &str| -> Built<Expr> {
        let node = col(&scope, alias, anchor_node_column())?;
        Ok(call(Function::Tuple, vec![node, string(entity)]))
    };
    let reverse = |column: &str| -> Built<Expr> {
        Ok(call(Function::ArrayReverse, vec![col(&scope, b, column)?]))
    };
    let items = vec![
        (
            path_start_column().to_string(),
            col(&scope, f, anchor_node_column())?,
        ),
        (
            path_end_column().to_string(),
            col(&scope, b, anchor_node_column())?,
        ),
        (depth_column().to_string(), depth()?),
        (
            path_column().to_string(),
            call(
                Function::ArrayConcat,
                vec![
                    call(Function::Array, vec![tuple(f, start_entity)?]),
                    col(&scope, f, path_nodes_column())?,
                    reverse(path_nodes_column())?,
                    call(Function::Array, vec![tuple(b, end_entity)?]),
                ],
            ),
        ),
        (
            edge_kinds_column().to_string(),
            call(
                Function::ArrayConcat,
                vec![
                    col(&scope, f, frontier_edge_kinds_column())?,
                    reverse(frontier_edge_kinds_column())?,
                ],
            ),
        ),
    ];
    let bound = binary(BinaryOp::Le, depth()?, int(plan.operation.max_depth as i64));
    let rows = filtered(q, rows, vec![Some(bound)])?;
    select(q, rows, items)
}
