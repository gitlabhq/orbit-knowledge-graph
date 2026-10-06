use ontology::constants::*;
use query_data_model::bindings::{QueryBindings, RelationId, ScopeId};
use query_data_model::{DenormalizedDirection, QueryDataModel};

use super::NodeBinding;
use super::context::LoweringContext;
use super::sql::{
    denorm_tag_expr, filter_to_expr, id_list_predicate, id_range_predicate, rel_kind_filter,
};
use crate::ast::*;
use crate::config::BindingNames;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::{Neighbors, Plan};

pub fn emit_neighbors(
    plan: &Plan<Neighbors>,
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<(Node, NodeBinding, Vec<OrderExpr>)> {
    let root = bindings.root();
    let mut context = LoweringContext {
        model,
        bindings,
        names,
    };
    let mut query = if let Some(table) = &plan.operation.fused_table {
        fused(&mut context, root, plan, table)?
    } else if plan.operation.direction == Direction::Both {
        let outgoing = context.scope(root)?;
        let incoming = context.scope(root)?;
        let outgoing = directional(&mut context, outgoing, plan, Direction::Outgoing)?;
        let incoming = directional(&mut context, incoming, plan, Direction::Incoming)?;
        let outputs = outgoing
            .select
            .iter()
            .filter_map(|select| select.alias)
            .collect::<Vec<_>>();
        let (from, relation) = context.union(root, vec![outgoing, incoming], "_neighbors")?;
        let mut query = Query::new(root, from);
        for export in outputs {
            let column = context
                .bindings
                .column(root, relation, export)
                .map_err(|error| QueryError::Lowering(error.to_string()))?;
            let name = context.names.exports[&export].clone();
            query
                .select
                .push(context.select(root, Expr::Column(column), &name)?);
        }
        query
    } else {
        directional(&mut context, root, plan, plan.operation.direction)?
    };
    if let Some(order) = &input.order_by {
        let export = query
            .select
            .iter()
            .filter_map(|select| select.alias)
            .find(|export| {
                context.names.exports[export] == order.property
                    || context.names.exports[export] == format!("{}_{}", order.node, order.property)
            })
            .ok_or_else(|| {
                QueryError::Lowering("neighbors order has no projected output".into())
            })?;
        query.order_by.push(OrderExpr {
            expr: Expr::Output(export),
            desc: order.direction == OrderDirection::Desc,
        });
    }
    query.limit = Some(input.limit);
    let center = &plan.nodes[&plan.operation.center];
    let role_identity = if !plan.operation.has_non_denorm && center.uses_default_pk() {
        Some(query.select[4].expr.clone())
    } else {
        None
    };
    let order = match plan.operation.direction {
        Direction::Incoming => vec![0, 4, 2],
        Direction::Outgoing => vec![4, 0, 2],
        Direction::Both => vec![4, 0, 2, 3],
    };
    let stable_order = order
        .into_iter()
        .map(|index| {
            OrderExpr::asc(Expr::Output(
                query.select[index].alias.expect("neighbor output"),
            ))
        })
        .collect();
    Ok((
        Node::Query(Box::new(query)),
        NodeBinding::Projected { role_identity },
        stable_order,
    ))
}

fn center_scan<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    parent: ScopeId,
    plan: &Plan<Neighbors>,
) -> Result<(TableRef, RelationId, Option<Expr>)> {
    let center = &plan.nodes[&plan.operation.center];
    let body = context.scope(parent)?;
    let (from, relation) = context.scan(
        body,
        center.table.as_deref().unwrap_or(""),
        &center.alias,
        true,
    )?;
    let id = context.column(body, relation, DEFAULT_PRIMARY_KEY)?;
    let mut predicates = Vec::new();
    for (name, filter) in &center.filters {
        predicates.push(filter_to_expr(
            context.column(body, relation, name)?,
            filter
                .filter
                .rhs_column
                .as_ref()
                .map(|(_, name)| context.column(body, relation, name))
                .transpose()?,
            filter,
        ));
    }
    if !center.node_ids.is_empty() {
        predicates.push(id_list_predicate(id, &center.node_ids));
    }
    if let Some(range) = &center.id_range {
        predicates.push(id_range_predicate(id, range));
    }
    let deletion = context.deletion(body, relation)?;
    let mut query = Query::new(body, from);
    context.project(
        &mut query,
        relation,
        DEFAULT_PRIMARY_KEY,
        DEFAULT_PRIMARY_KEY,
    )?;
    if !center.uses_default_pk() {
        context.project(
            &mut query,
            relation,
            &center.redaction_id_column,
            &center.redaction_id_column,
        )?;
    }
    let deleted_export = if let Some(Expr::BinaryOp { left, .. }) = deletion {
        let select = context.select(body, *left, "_deleted")?;
        let export = select.alias;
        query.select.push(select);
        export
    } else {
        None
    };
    query.where_clause = Expr::conjoin(predicates);
    let (from, relation) = context.derived(parent, query, &center.alias)?;
    let deletion = deleted_export
        .map(|export| {
            context
                .bindings
                .column(parent, relation, export)
                .map(super::sql::deleted_false)
                .map_err(|error| QueryError::Lowering(error.to_string()))
        })
        .transpose()?;
    Ok((from, relation, deletion))
}

fn edge_predicates<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    scope: ScopeId,
    relation: RelationId,
    plan: &Plan<Neighbors>,
    direction: Direction,
) -> Result<Vec<Expr>> {
    let center = &plan.nodes[&plan.operation.center];
    let (id, kind) = if direction == Direction::Outgoing {
        (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN)
    } else {
        (TARGET_ID_COLUMN, TARGET_KIND_COLUMN)
    };
    let mut predicates = vec![Expr::eq(
        Expr::Column(context.column(scope, relation, kind)?),
        Expr::string(center.entity.as_deref().unwrap_or("")),
    )];
    if !center.node_ids.is_empty() {
        predicates.push(id_list_predicate(
            context.column(scope, relation, id)?,
            &center.node_ids,
        ));
    }
    if let Some(types) = &plan.operation.edge.rel_type_filter {
        predicates.extend(rel_kind_filter(
            context.column(scope, relation, RELATIONSHIP_KIND_COLUMN)?,
            types,
        ));
    }
    if direction == Direction::Incoming
        && !center.node_ids.is_empty()
        && let Some((table, key)) = &plan.operation.center_tp_lookup
    {
        let body = context.scope(scope)?;
        let (from, lookup) = context.scan(body, table, "_tpd", false)?;
        let mut query = Query::new(body, from);
        context.project(
            &mut query,
            lookup,
            TRAVERSAL_PATH_COLUMN,
            TRAVERSAL_PATH_COLUMN,
        )?;
        query.where_clause = Expr::and_all([
            Some(id_list_predicate(
                context.column(body, lookup, key)?,
                &center.node_ids,
            )),
            context.deletion(body, lookup)?,
        ]);
        predicates.push(Expr::InSelect {
            expr: Box::new(Expr::Column(context.column(
                scope,
                relation,
                TRAVERSAL_PATH_COLUMN,
            )?)),
            query: Box::new(query),
        });
    }
    predicates.extend(context.deletion(scope, relation)?);
    Ok(predicates)
}

fn denormalized_predicates<M: QueryDataModel + ?Sized>(
    context: &LoweringContext<'_, M>,
    scope: ScopeId,
    relation: RelationId,
    plan: &Plan<Neighbors>,
    direction: DenormalizedDirection,
) -> Result<Vec<Expr>> {
    let mut predicates = Vec::new();
    for (_, filter) in &plan.nodes[&plan.operation.center].filters {
        let Some(property) = filter.property else {
            continue;
        };
        if let Some(facts) = plan.denormalized.get(&query_data_model::DenormalizedKey {
            property,
            direction,
        }) {
            predicates.extend(denorm_tag_expr(
                context.column(scope, relation, &facts.edge_column)?,
                &facts.tag_key,
                &filter.filter,
            ));
        }
    }
    Ok(predicates)
}

fn directional<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    scope: ScopeId,
    plan: &Plan<Neighbors>,
    direction: Direction,
) -> Result<Query> {
    let center = &plan.nodes[&plan.operation.center];
    let outgoing = direction == Direction::Outgoing;
    let (center_id, neighbor_id, neighbor_kind) = if outgoing {
        (SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN)
    } else {
        (TARGET_ID_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN)
    };
    let tables = if outgoing {
        &plan.operation.edge.outgoing_tables
    } else {
        &plan.operation.edge.incoming_tables
    };
    let (mut from, relation, mut predicates) = if let [table] = tables.as_slice() {
        let (from, relation) = context.scan(scope, table, "e", false)?;
        let predicates = edge_predicates(context, scope, relation, plan, direction)?;
        (from, relation, predicates)
    } else {
        let mut arms = Vec::new();
        for table in tables {
            let body = context.scope(scope)?;
            let (from, relation) = context.scan(body, table, "_e", false)?;
            let mut query = Query::new(body, from);
            for name in EDGE_RESERVED_COLUMNS {
                context.project(&mut query, relation, name, name)?;
            }
            query.where_clause =
                Expr::conjoin(edge_predicates(context, body, relation, plan, direction)?);
            arms.push(query);
        }
        let (from, relation) = context.union(scope, arms, "e")?;
        (from, relation, vec![])
    };
    predicates.extend(denormalized_predicates(
        context,
        scope,
        relation,
        plan,
        if outgoing {
            DenormalizedDirection::Source
        } else {
            DenormalizedDirection::Target
        },
    )?);
    let identity = Expr::Column(context.column(scope, relation, center_id)?);
    let center_relation = if plan.operation.has_non_denorm {
        let (scan, center_relation, deletion) = center_scan(context, scope, plan)?;
        from = TableRef::join(
            JoinType::Inner,
            from,
            scan,
            Expr::eq(
                identity.clone(),
                Expr::Column(context.column(scope, center_relation, DEFAULT_PRIMARY_KEY)?),
            ),
        );
        predicates.extend(deletion);
        Some(center_relation)
    } else if !center.uses_default_pk() {
        let (scan, center_relation) = context.scan(
            scope,
            center.table.as_deref().unwrap_or(""),
            &center.alias,
            true,
        )?;
        from = TableRef::join(
            JoinType::Inner,
            from,
            scan,
            Expr::eq(
                identity.clone(),
                Expr::Column(context.column(scope, center_relation, DEFAULT_PRIMARY_KEY)?),
            ),
        );
        predicates.extend(context.deletion(scope, center_relation)?);
        Some(center_relation)
    } else {
        None
    };
    let mut query = Query::new(scope, from);
    for (column, name) in [
        (neighbor_id, neighbor_id_column()),
        (neighbor_kind, neighbor_type_column()),
        (RELATIONSHIP_KIND_COLUMN, relationship_type_column()),
    ] {
        context.project(&mut query, relation, column, name)?;
    }
    query.select.push(context.select(
        scope,
        Expr::int(i64::from(outgoing)),
        neighbor_is_outgoing_column(),
    )?);
    if center.uses_default_pk() {
        query
            .select
            .push(context.select(scope, identity, &redaction_id_column(&center.alias))?);
    } else {
        let center_relation = center_relation.expect("redaction table constructed");
        context.project(
            &mut query,
            center_relation,
            &center.redaction_id_column,
            &redaction_id_column(&center.alias),
        )?;
        context.project(
            &mut query,
            center_relation,
            DEFAULT_PRIMARY_KEY,
            &primary_key_column(&center.alias),
        )?;
    }
    query.select.push(context.select(
        scope,
        Expr::string(center.entity.as_deref().unwrap_or("")),
        &redaction_type_column(&center.alias),
    )?);
    if center.has_traversal_path {
        context.project(
            &mut query,
            relation,
            TRAVERSAL_PATH_COLUMN,
            &traversal_path_column(&center.alias),
        )?;
    }
    query.where_clause = Expr::conjoin(predicates);
    Ok(query)
}

fn fused<M: QueryDataModel + ?Sized>(
    context: &mut LoweringContext<'_, M>,
    scope: ScopeId,
    plan: &Plan<Neighbors>,
    table: &str,
) -> Result<Query> {
    let center = &plan.nodes[&plan.operation.center];
    let body = context.scope(scope)?;
    let (from, edge) = context.scan(body, table, "e", false)?;
    let mut arms = Vec::new();
    for (kind, id, direction) in [
        (
            SOURCE_KIND_COLUMN,
            SOURCE_ID_COLUMN,
            DenormalizedDirection::Source,
        ),
        (
            TARGET_KIND_COLUMN,
            TARGET_ID_COLUMN,
            DenormalizedDirection::Target,
        ),
    ] {
        let mut predicates = vec![Expr::eq(
            Expr::Column(context.column(body, edge, kind)?),
            Expr::string(center.entity.as_deref().unwrap_or("")),
        )];
        if !center.node_ids.is_empty() {
            predicates.push(id_list_predicate(
                context.column(body, edge, id)?,
                &center.node_ids,
            ));
        }
        predicates.extend(denormalized_predicates(
            context, body, edge, plan, direction,
        )?);
        arms.push(Expr::conjoin(predicates).unwrap());
    }
    let col = |name| context.column(body, edge, name).map(Expr::Column);
    let outgoing = Expr::func(
        Function::Tuple,
        vec![
            arms[0].clone(),
            Expr::int(1),
            col(TARGET_ID_COLUMN)?,
            col(TARGET_KIND_COLUMN)?,
            col(SOURCE_ID_COLUMN)?,
        ],
    );
    let incoming = Expr::func(
        Function::Tuple,
        vec![
            arms[1].clone(),
            Expr::int(0),
            col(SOURCE_ID_COLUMN)?,
            col(SOURCE_KIND_COLUMN)?,
            col(TARGET_ID_COLUMN)?,
        ],
    );
    let matched = Expr::func(
        Function::ArrayFilter,
        vec![
            Expr::lambda(
                "_gkg_arm",
                Expr::func(
                    Function::TupleElement,
                    vec![Expr::ident("_gkg_arm"), Expr::int(1)],
                ),
            ),
            Expr::func(Function::Array, vec![outgoing, incoming]),
        ],
    );
    let mut inner = Query::new(body, from);
    inner.select.push(context.select(
        body,
        Expr::func(Function::Unnest, vec![matched]),
        "_gkg_arm_row",
    )?);
    context.project(
        &mut inner,
        edge,
        RELATIONSHIP_KIND_COLUMN,
        relationship_type_column(),
    )?;
    if center.has_traversal_path {
        context.project(
            &mut inner,
            edge,
            TRAVERSAL_PATH_COLUMN,
            &traversal_path_column(&center.alias),
        )?;
    }
    let mut predicates = vec![Expr::binary(Op::Or, arms.remove(0), arms.remove(0))];
    if let Some(types) = &plan.operation.edge.rel_type_filter {
        predicates.extend(rel_kind_filter(
            context.column(body, edge, RELATIONSHIP_KIND_COLUMN)?,
            types,
        ));
    }
    predicates.extend(context.deletion(body, edge)?);
    inner.where_clause = Expr::conjoin(predicates);
    let exports = inner
        .select
        .iter()
        .map(|select| select.alias.expect("fused output"))
        .collect::<Vec<_>>();
    let (from, relation) = context.derived(scope, inner, "_gkg_fused")?;
    let row = Expr::Column(
        context
            .bindings
            .column(scope, relation, exports[0])
            .map_err(|error| QueryError::Lowering(error.to_string()))?,
    );
    let element = |index| Expr::func(Function::TupleElement, vec![row.clone(), Expr::int(index)]);
    let mut query = Query::new(scope, from);
    query
        .select
        .push(context.select(scope, element(3), neighbor_id_column())?);
    query
        .select
        .push(context.select(scope, element(4), neighbor_type_column())?);
    let kind = context
        .bindings
        .column(scope, relation, exports[1])
        .map_err(|error| QueryError::Lowering(error.to_string()))?;
    query
        .select
        .push(context.select(scope, Expr::Column(kind), relationship_type_column())?);
    query
        .select
        .push(context.select(scope, element(2), neighbor_is_outgoing_column())?);
    query
        .select
        .push(context.select(scope, element(5), &redaction_id_column(&center.alias))?);
    query.select.push(context.select(
        scope,
        Expr::string(center.entity.as_deref().unwrap_or("")),
        &redaction_type_column(&center.alias),
    )?);
    if center.has_traversal_path {
        let name = traversal_path_column(&center.alias);
        let path = context
            .bindings
            .column(scope, relation, exports[2])
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        query
            .select
            .push(context.select(scope, Expr::Column(path), &name)?);
    }
    Ok(query)
}
