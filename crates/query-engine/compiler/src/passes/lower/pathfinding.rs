use ontology::constants::*;
use query_data_model::bindings::{ColumnRef, DefinitionId, QueryBindings, ScopeId};
use query_data_model::{DenormalizedDirection, QueryDataModel};

use super::context::LoweringContext;
use super::sql::{
    denorm_tag_expr, filter_to_expr, id_list_predicate, id_range_predicate, rel_kind_filter,
};
use crate::ast::*;
use crate::config::BindingNames;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan::{NodePlan, PathFinding, Plan};

pub fn emit_pathfinding(
    plan: &Plan<PathFinding>,
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<(Node, Vec<OrderExpr>)> {
    let root = bindings.root();
    let mut context = LoweringContext {
        model,
        bindings,
        names,
    };
    let operation = &plan.operation;
    let start = &plan.nodes[&operation.start];
    let end = &plan.nodes[&operation.end];
    let start_entity = start.entity.as_deref().unwrap_or("");
    let end_entity = end.entity.as_deref().unwrap_or("");
    let mut ctes = Vec::new();
    let start_anchor = anchor(&mut context, root, start, operation.scoped_by_tp, &mut ctes)?;
    let end_anchor = anchor(&mut context, root, end, operation.scoped_by_tp, &mut ctes)?;
    let path_scope = scope_definition(&mut context, root, start, end, start_anchor, end_anchor)?;
    let scope_definition = path_scope.as_ref().map(|cte| cte.name);
    ctes.extend(path_scope);
    let forward = frontier(
        &mut context,
        root,
        plan,
        start,
        start_anchor,
        scope_definition,
        false,
    )?;
    let forward_id = forward.name;
    let forward_outputs: Vec<_> = forward
        .query
        .select
        .iter()
        .map(|select| select.alias.expect("frontier output"))
        .collect();
    ctes.push(forward);
    let backward_id = if operation.backward_depth > 0 {
        let backward = frontier(
            &mut context,
            root,
            plan,
            end,
            end_anchor,
            scope_definition,
            true,
        )?;
        let definition = backward.name;
        ctes.push(backward);
        let outputs = ctes
            .last()
            .unwrap()
            .query
            .select
            .iter()
            .map(|select| select.alias.expect("frontier output"))
            .collect::<Vec<_>>();
        Some((definition, outputs))
    } else {
        None
    };

    let direct_scope = context.scope(root)?;
    let (from, forward) = context.reference(direct_scope, forward_id, FORWARD_ALIAS)?;
    let col = |index| {
        context
            .bindings
            .column(direct_scope, forward, forward_outputs[index])
            .map(Expr::Column)
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))
    };
    let start_tuple = Expr::func(Function::Tuple, vec![col(0)?, Expr::string(start_entity)]);
    let direct_path = Expr::func(
        Function::ArrayConcat,
        vec![Expr::func(Function::Array, vec![start_tuple]), col(3)?],
    );
    let kinds = col(4)?;
    let depth = col(5)?;
    let end_kind = col(2)?;
    let end_column = context
        .bindings
        .column(direct_scope, forward, forward_outputs[1])
        .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
    let endpoint = endpoint_filter(&mut context, direct_scope, end_column, end, end_anchor)?;
    let mut direct = Query::new(direct_scope, from);
    direct.select = vec![
        context.select(direct_scope, depth.clone(), DEPTH_COLUMN)?,
        context.select(direct_scope, direct_path, path_column())?,
        context.select(direct_scope, kinds, edge_kinds_column())?,
    ];
    direct.where_clause = Expr::and_all([
        Some(Expr::eq(depth, Expr::int(1))),
        Some(Expr::eq(end_kind, Expr::string(end_entity))),
        endpoint,
    ]);
    let path_outputs = direct
        .select
        .iter()
        .map(|select| select.alias.expect("path output"))
        .collect::<Vec<_>>();
    let mut paths = vec![direct];
    if let Some((backward_id, backward_outputs)) = backward_id {
        let scope = context.scope(root)?;
        let (left, forward) = context.reference(scope, forward_id, FORWARD_ALIAS)?;
        let (right, backward) = context.reference(scope, backward_id, BACKWARD_ALIAS)?;
        let col = |relation, index| {
            let exports = if relation == forward {
                &forward_outputs
            } else {
                &backward_outputs
            };
            context
                .bindings
                .column(scope, relation, exports[index])
                .map(Expr::Column)
                .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))
        };
        let depth = Expr::binary(Op::Add, col(forward, 5)?, col(backward, 5)?);
        let mut on = Expr::eq(col(forward, 1)?, col(backward, 1)?);
        if operation.scoped_by_tp {
            on = Expr::and(on, Expr::eq(col(forward, 6)?, col(backward, 6)?));
        }
        let start_tuple = Expr::func(
            Function::Tuple,
            vec![col(forward, 0)?, Expr::string(start_entity)],
        );
        let end_tuple = Expr::func(
            Function::Tuple,
            vec![col(backward, 0)?, Expr::string(end_entity)],
        );
        let path = Expr::func(
            Function::ArrayConcat,
            vec![
                Expr::func(Function::Array, vec![start_tuple]),
                col(forward, 3)?,
                Expr::func(Function::ArrayReverse, vec![col(backward, 3)?]),
                Expr::func(Function::Array, vec![end_tuple]),
            ],
        );
        let kinds = Expr::func(
            Function::ArrayConcat,
            vec![
                col(forward, 4)?,
                Expr::func(Function::ArrayReverse, vec![col(backward, 4)?]),
            ],
        );
        let mut intersection = Query::new(scope, TableRef::join(JoinType::Inner, left, right, on));
        intersection.select = vec![
            context.select(scope, depth.clone(), DEPTH_COLUMN)?,
            context.select(scope, path, path_column())?,
            context.select(scope, kinds, edge_kinds_column())?,
        ];
        intersection.where_clause = Some(Expr::binary(
            Op::Le,
            depth,
            Expr::int(operation.max_depth as i64),
        ));
        paths.push(intersection);
    }
    let (from, relation) = if paths.len() == 1 {
        context.derived(root, paths.pop().unwrap(), PATHS_ALIAS)?
    } else {
        context.union(root, paths, PATHS_ALIAS)?
    };
    let mut query = Query::new(root, from);
    query.ctes = ctes;
    for (index, name) in [
        (1, path_column()),
        (2, edge_kinds_column()),
        (0, DEPTH_COLUMN),
    ] {
        let column = context
            .bindings
            .column(root, relation, path_outputs[index])
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        query
            .select
            .push(context.select(root, Expr::Column(column), name)?);
    }
    query.order_by = vec![OrderExpr::asc(Expr::Column(
        context
            .bindings
            .column(root, relation, path_outputs[0])
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?,
    ))];
    query.limit = Some(input.limit);
    let stable_order = [path_outputs[1], path_outputs[2]]
        .into_iter()
        .map(|export| {
            let column = context
                .bindings
                .column(root, relation, export)
                .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
            Ok(OrderExpr::asc(Expr::func(
                Function::ToString,
                vec![Expr::Column(column)],
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((Node::Query(Box::new(query)), stable_order))
}

fn anchor<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    root: ScopeId,
    node: &NodePlan,
    force: bool,
    ctes: &mut Vec<Cte>,
) -> Result<Option<DefinitionId>> {
    if (!force && !node.node_ids.is_empty())
        || (node.node_ids.is_empty() && node.filters.is_empty() && node.id_range.is_none())
    {
        return Ok(None);
    }
    let body = context.scope(root)?;
    let scan_scope = context.scope(body)?;
    let (from, scan) = context.scan(
        scan_scope,
        node.table.as_deref().unwrap_or(""),
        &node.alias,
        true,
    )?;
    let mut predicates = Vec::new();
    for (name, filter) in &node.filters {
        predicates.push(filter_to_expr(
            context.column(scan_scope, scan, name)?,
            filter
                .filter
                .rhs_column
                .as_ref()
                .map(|(_, name)| context.column(scan_scope, scan, name))
                .transpose()?,
            filter,
        ));
    }
    let id = context.column(scan_scope, scan, DEFAULT_PRIMARY_KEY)?;
    if !node.node_ids.is_empty() {
        predicates.push(id_list_predicate(id, &node.node_ids));
    }
    if let Some(range) = &node.id_range {
        predicates.push(id_range_predicate(id, range));
    }
    let deletion = context.deletion(scan_scope, scan)?;
    let mut inner = Query::new(scan_scope, from);
    let mut columns = vec![DEFAULT_PRIMARY_KEY];
    if node.has_traversal_path {
        columns.push(TRAVERSAL_PATH_COLUMN);
    }
    for name in &columns {
        context.project(&mut inner, scan, name, name)?;
    }
    let deleted_export = if let Some(Expr::BinaryOp { left, .. }) = deletion {
        let Expr::Column(column) = *left else {
            unreachable!()
        };
        let name = context.names.exports[&column.export()].clone();
        let select = context.select(scan_scope, Expr::Column(column), &name)?;
        let export = select.alias.unwrap();
        inner.select.push(select);
        Some(export)
    } else {
        None
    };
    inner.where_clause = Expr::conjoin(predicates);
    let (from, relation) = context.derived(body, inner, &node.alias)?;
    let mut query = Query::new(body, from);
    for name in columns {
        context.project(&mut query, relation, name, name)?;
    }
    if let Some(export) = deleted_export {
        let column = context
            .bindings
            .column(body, relation, export)
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        query.where_clause = Some(super::sql::deleted_false(column));
    }
    query.limit = Some(crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32);
    let cte = context.define(root, query, &node_filter_cte(&node.alias))?;
    let definition = cte.name;
    ctes.push(cte);
    Ok(Some(definition))
}

fn scope_definition<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    root: ScopeId,
    start: &NodePlan,
    end: &NodePlan,
    start_anchor: Option<DefinitionId>,
    end_anchor: Option<DefinitionId>,
) -> Result<Option<Cte>> {
    let (Some(start_anchor), Some(end_anchor)) = (start_anchor, end_anchor) else {
        return Ok(None);
    };
    if !start.has_traversal_path || !end.has_traversal_path {
        return Ok(None);
    }
    let body = context.scope(root)?;
    let mut arms = Vec::new();
    for (definition, hint) in [
        (start_anchor, "_path_scope_start"),
        (end_anchor, "_path_scope_end"),
    ] {
        let scope = context.scope(body)?;
        let (from, relation) = context.reference(scope, definition, hint)?;
        let mut arm = Query::new(scope, from);
        context.project(
            &mut arm,
            relation,
            TRAVERSAL_PATH_COLUMN,
            TRAVERSAL_PATH_COLUMN,
        )?;
        arm.group_by = vec![Expr::Column(context.column(
            scope,
            relation,
            TRAVERSAL_PATH_COLUMN,
        )?)];
        arms.push(arm);
    }
    let (from, relation) = context.union(body, arms, "_path_scope")?;
    let mut query = Query::new(body, from);
    context.project(
        &mut query,
        relation,
        TRAVERSAL_PATH_COLUMN,
        TRAVERSAL_PATH_COLUMN,
    )?;
    context
        .define(root, query, "_path_scope_traversal_paths")
        .map(Some)
}

fn endpoint_filter<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    scope: ScopeId,
    column: ColumnRef,
    node: &NodePlan,
    anchor: Option<DefinitionId>,
) -> Result<Option<Expr>> {
    if !node.node_ids.is_empty() {
        return Ok(Some(id_list_predicate(column, &node.node_ids)));
    }
    anchor
        .map(|definition| context.membership(scope, column, definition))
        .transpose()
}

fn frontier<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    root: ScopeId,
    plan: &Plan<PathFinding>,
    node: &NodePlan,
    anchor: Option<DefinitionId>,
    path_scope: Option<DefinitionId>,
    backward: bool,
) -> Result<Cte> {
    let body = context.scope(root)?;
    let operation = &plan.operation;
    let max_depth = if backward {
        operation.backward_depth
    } else {
        operation.forward_depth
    };
    let mut arms = Vec::new();
    for depth in 1..=max_depth {
        let scope = context.scope(body)?;
        let (anchor_col, next_col, kind_col, anchor_kind) = if backward {
            (
                TARGET_ID_COLUMN,
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_KIND_COLUMN,
            )
        } else {
            (
                SOURCE_ID_COLUMN,
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_KIND_COLUMN,
            )
        };
        let (mut from, first) = context.edge_scan(scope, &operation.edge.tables, "e1")?;
        let mut scans = vec![first];
        let first_filter = if backward {
            &operation.backward_first_hop_filter
        } else {
            &operation.forward_first_hop_filter
        };
        let types = first_filter
            .as_ref()
            .or(operation.edge.rel_type_filter.as_ref());
        let mut predicates = Vec::new();
        if let Some(types) = types {
            predicates.extend(rel_kind_filter(
                context.column(scope, first, RELATIONSHIP_KIND_COLUMN)?,
                types,
            ));
        }
        let anchor_column = context.column(scope, first, anchor_col)?;
        if let Some(definition) = anchor {
            predicates.push(context.membership(scope, anchor_column, definition)?);
        } else if !node.node_ids.is_empty() {
            predicates.push(id_list_predicate(anchor_column, &node.node_ids));
        }
        predicates.push(Expr::eq(
            Expr::Column(context.column(scope, first, anchor_kind)?),
            Expr::string(node.entity.as_deref().unwrap_or("")),
        ));
        if let Some(definition) = path_scope {
            predicates.push(context.membership(
                scope,
                context.column(scope, first, TRAVERSAL_PATH_COLUMN)?,
                definition,
            )?);
        }
        if operation.edge.tables.len() == 1 {
            predicates.extend(context.deletion(scope, first)?);
        }
        let direction = if backward {
            DenormalizedDirection::Target
        } else {
            DenormalizedDirection::Source
        };
        for (_, filter) in &node.filters {
            let Some(property) = filter.property else {
                continue;
            };
            if let Some(facts) = plan.denormalized.get(&query_data_model::DenormalizedKey {
                property,
                direction,
            }) {
                predicates.extend(denorm_tag_expr(
                    context.column(scope, first, &facts.edge_column)?,
                    &facts.tag_key,
                    &filter.filter,
                ));
            }
        }
        for step in 2..=depth {
            let (right, relation) =
                context.edge_scan(scope, &operation.edge.tables, &format!("e{step}"))?;
            let previous = *scans.last().unwrap();
            let mut conditions = vec![Expr::eq(
                Expr::Column(context.column(scope, previous, next_col)?),
                Expr::Column(context.column(scope, relation, anchor_col)?),
            )];
            if operation.scoped_by_tp {
                conditions.push(Expr::eq(
                    Expr::Column(context.column(scope, previous, TRAVERSAL_PATH_COLUMN)?),
                    Expr::Column(context.column(scope, relation, TRAVERSAL_PATH_COLUMN)?),
                ));
            }
            if let Some(types) = &operation.edge.rel_type_filter {
                conditions.extend(rel_kind_filter(
                    context.column(scope, relation, RELATIONSHIP_KIND_COLUMN)?,
                    types,
                ));
            }
            if let Some(definition) = path_scope {
                conditions.push(context.membership(
                    scope,
                    context.column(scope, relation, TRAVERSAL_PATH_COLUMN)?,
                    definition,
                )?);
            }
            if operation.edge.tables.len() == 1 {
                conditions.extend(context.deletion(scope, relation)?);
            }
            from = TableRef::join(
                JoinType::Inner,
                from,
                right,
                Expr::conjoin(conditions).unwrap(),
            );
            scans.push(relation);
        }
        let last = *scans.last().unwrap();
        let count = if backward {
            scans.len() - 1
        } else {
            scans.len()
        };
        let tuples = scans
            .iter()
            .take(count)
            .map(|relation| {
                Ok(Expr::func(
                    Function::Tuple,
                    vec![
                        Expr::Column(context.column(scope, *relation, next_col)?),
                        Expr::Column(context.column(scope, *relation, kind_col)?),
                    ],
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let path = if tuples.is_empty() {
            Expr::EmptyTupleArray(vec![SqlType::Int64, SqlType::String])
        } else {
            Expr::func(Function::Array, tuples)
        };
        let kinds = Expr::func(
            Function::Array,
            scans
                .iter()
                .map(|relation| {
                    context
                        .column(scope, *relation, RELATIONSHIP_KIND_COLUMN)
                        .map(Expr::Column)
                })
                .collect::<Result<_>>()?,
        );
        let mut query = Query::new(scope, from);
        for (relation, column, name) in [
            (first, anchor_col, ANCHOR_ID_COLUMN),
            (last, next_col, END_ID_COLUMN),
            (last, kind_col, END_KIND_COLUMN),
        ] {
            context.project(&mut query, relation, column, name)?;
        }
        query
            .select
            .push(context.select(scope, path, PATH_NODES_COLUMN)?);
        query
            .select
            .push(context.select(scope, kinds, FRONTIER_EDGE_KINDS_COLUMN)?);
        query
            .select
            .push(context.select(scope, Expr::int(depth as i64), DEPTH_COLUMN)?);
        if operation.scoped_by_tp {
            context.project(
                &mut query,
                first,
                TRAVERSAL_PATH_COLUMN,
                TRAVERSAL_PATH_COLUMN,
            )?;
        }
        query.where_clause = Expr::conjoin(predicates);
        arms.push(query);
    }
    let exports = arms[0]
        .select
        .iter()
        .map(|select| select.alias.expect("frontier export"))
        .collect::<Vec<_>>();
    let (from, relation) = context.union(body, arms, "_frontier")?;
    let mut query = Query::new(body, from);
    for export in exports {
        let column = context
            .bindings
            .column(body, relation, export)
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        let name = context.names.exports[&export].clone();
        query
            .select
            .push(context.select(body, Expr::Column(column), &name)?);
    }
    context.define(
        root,
        query,
        if backward { BACKWARD_CTE } else { FORWARD_CTE },
    )
}
