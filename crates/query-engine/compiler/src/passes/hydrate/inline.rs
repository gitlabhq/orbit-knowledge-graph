//! Neighbors and path finding read node properties and owner ids inside the base query.
//!
//! ```sql
//! WITH page AS MATERIALIZED (base),
//!      page_nodes AS MATERIALIZED (SELECT node_ref, node_kind FROM page ARRAY JOIN page node tuples),
//!      node_properties AS MATERIALIZED (
//!        SELECT latest row FROM (UNION ALL of one raw read per entity)
//!        WHERE (node_kind, node_ref) IN (SELECT node_kind, node_ref FROM page_nodes)),
//!      lookup AS (SELECT mapFromArrays(groupArray(node_key), groupArray(node_value)) FROM node_properties)
//! SELECT page columns, owner and properties from lookup FROM page CROSS JOIN lookup
//! ```

use std::collections::BTreeSet;

use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
use query_data_model::OrbitQueryModel;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::{AggFunction, ColumnSelection, Direction, Input, InputNode, QueryType};
use crate::passes::hydrate::DynamicEntityColumns;
use crate::passes::lower::query_types::hydration::properties_json;
use crate::passes::lower::sql::{deleted_false, latest_row_dedup};
use crate::passes::plan::query_types::hydration::{
    self as hydration_plan, HydrationCompileOptions, HydrationNodePlan,
};

struct PageNode {
    id: String,
    kind: String,
    owner_output: Option<String>,
    properties_output: String,
}

pub fn inline(
    node: Node,
    input: &Input,
    specs: &[DynamicEntityColumns],
    model: &(impl OrbitQueryModel + ?Sized),
) -> Result<Node> {
    let Node::Query(mut base) = node else {
        return Ok(node);
    };
    let Some(properties) = node_properties(input, specs, model)? else {
        return Ok(Node::Query(base));
    };
    let nodes = page_node_columns(input, &base);
    let order_by = base
        .order_by
        .iter()
        .map(|order| {
            Some(OrderExpr {
                expr: on_page(&order.expr, &base)?,
                desc: order.desc,
            })
        })
        .collect::<Option<Vec<_>>>();
    let outputs: Vec<String> = base.select.iter().filter_map(|s| s.alias.clone()).collect();
    let mut ctes = std::mem::take(&mut base.ctes);
    ctes.push(materialized(page_cte(), page(*base, order_by.is_none())));
    ctes.push(materialized(
        page_nodes_cte(),
        page_nodes(input.query_type, &nodes),
    ));
    ctes.push(materialized(node_properties_cte(), properties));
    ctes.push(Cte::new(lookup_cte(), lookup()));

    let mut select: Vec<SelectExpr> = outputs
        .iter()
        .map(|name| match input.query_type {
            QueryType::PathFinding if name == path_column() => {
                SelectExpr::new(enriched_path(), name.clone())
            }
            _ => SelectExpr::new(page_col(name), name.clone()),
        })
        .collect();
    for node in &nodes {
        let value = node_value(page_col(&node.kind), page_col(&node.id));
        if let Some(owner) = &node.owner_output {
            select.push(SelectExpr::new(tuple_element(value.clone(), 1), owner));
        }
        select.push(SelectExpr::new(
            tuple_element(value, 2),
            &node.properties_output,
        ));
    }
    Ok(Node::Query(Box::new(Query {
        ctes,
        select,
        from: TableRef::Join {
            join_type: JoinType::Cross,
            left: Box::new(TableRef::cte(page_cte(), page_cte())),
            right: Box::new(TableRef::cte(lookup_cte(), lookup_cte())),
            on: Expr::Literal(serde_json::Value::Bool(true)),
        },
        order_by: order_by.unwrap_or_else(|| vec![OrderExpr::asc(page_col(row_position_column()))]),
        ..Default::default()
    })))
}

fn page_node_columns(input: &Input, base: &Query) -> Vec<PageNode> {
    let selected = |column: String| base.selects_alias(&column).then_some(column);
    let mut nodes: Vec<PageNode> = input
        .nodes
        .iter()
        .filter_map(|node| {
            Some(PageNode {
                id: selected(primary_key_column(&node.id))
                    .or_else(|| selected(redaction_id_column(&node.id)))?,
                kind: selected(redaction_type_column(&node.id))?,
                owner_output: None,
                properties_output: properties_column(&node.id),
            })
        })
        .collect();
    if input.query_type == QueryType::Neighbors {
        nodes.push(PageNode {
            id: neighbor_id_column().into(),
            kind: neighbor_type_column().into(),
            owner_output: Some(neighbor_owner_column().into()),
            properties_output: neighbor_properties_column().into(),
        });
    }
    nodes
}

fn on_page(expr: &Expr, base: &Query) -> Option<Expr> {
    if let Some(output) = base.select.iter().find(|output| {
        output.expr == *expr
            || matches!(expr, Expr::Identifier(name) if output.alias.as_ref() == Some(name))
    }) {
        return output.alias.as_deref().map(page_col);
    }
    match expr {
        Expr::FuncCall { name, args } => Some(Expr::func(
            *name,
            args.iter()
                .map(|arg| on_page(arg, base))
                .collect::<Option<_>>()?,
        )),
        Expr::Literal(_) | Expr::Param { .. } => Some(expr.clone()),
        _ => None,
    }
}

fn node_properties(
    input: &Input,
    specs: &[DynamicEntityColumns],
    model: &(impl OrbitQueryModel + ?Sized),
) -> Result<Option<Query>> {
    let nodes: Vec<InputNode> = reachable_entities(input, model)
        .into_iter()
        .filter(|entity| model.entity_table(entity).is_some())
        .map(|entity| InputNode {
            id: hydration_node_alias().to_string(),
            columns: Some(ColumnSelection::List(
                specs
                    .iter()
                    .find(|spec| spec.entity_type == entity)
                    .map(|spec| spec.columns.clone())
                    .unwrap_or_default(),
            )),
            entity: Some(entity),
            ..InputNode::default()
        })
        .collect();
    if nodes.is_empty() {
        return Ok(None);
    }
    let input = Input {
        query_type: QueryType::Hydration,
        nodes,
        ..Input::default()
    };
    let plan = hydration_plan::plan(&input, model, HydrationCompileOptions::default())?;
    let reads = plan
        .operation
        .nodes
        .iter()
        .map(|node| raw_read(node, model))
        .collect();
    Ok(Some(latest_page_rows(union(reads))))
}

/// Never references the page: the analyzer re-resolves the page CTE at every reference.
fn raw_read(node: &HydrationNodePlan, model: &(impl OrbitQueryModel + ?Sized)) -> Query {
    let alias = &node.alias;
    let column = |name: &str| Expr::col(alias, name);
    let owner = model
        .redaction_id_column_named(&node.entity)
        .unwrap_or(DEFAULT_PRIMARY_KEY);
    let id = column(&node.id_property);
    Query {
        select: vec![
            SelectExpr::new(Expr::string(&node.entity), node_kind_column()),
            SelectExpr::new(id.clone(), node_ref_column()),
            SelectExpr::new(
                Expr::func(
                    Function::ToString,
                    vec![Expr::func(
                        Function::Tuple,
                        node.sort_key
                            .iter()
                            .filter(|key| *key != TRAVERSAL_PATH_COLUMN)
                            .chain(
                                (!node.sort_key.contains(&node.id_property))
                                    .then_some(&node.id_property),
                            )
                            .map(|key| column(key))
                            .collect(),
                    )],
                ),
                node_row_column(),
            ),
            SelectExpr::new(column(VERSION_COLUMN), VERSION_COLUMN),
            SelectExpr::new(column(DELETED_COLUMN), DELETED_COLUMN),
            SelectExpr::new(node_key(Expr::string(&node.entity), id), node_key_column()),
            SelectExpr::new(
                Expr::func(Function::Tuple, vec![column(owner), properties_json(node)]),
                node_value_column(),
            ),
        ],
        from: TableRef::scan(&node.table, alias),
        ..Default::default()
    }
}

fn latest_page_rows(reads: Query) -> Query {
    let read = |name: &str| Expr::col(node_reads_alias(), name);
    let on_page = |columns: &[&str]| Expr::InSelect {
        expr: Box::new(Expr::func(
            Function::Tuple,
            columns.iter().map(|c| read(c)).collect(),
        )),
        query: Box::new(page_nodes_of(columns)),
    };
    let (order_by, limit_by) = latest_row_dedup(
        node_reads_alias(),
        &[node_kind_column().into(), node_row_column().into()],
    );
    let latest = Query {
        select: [node_key_column(), node_value_column(), DELETED_COLUMN]
            .into_iter()
            .map(|name| SelectExpr::new(read(name), name))
            .collect(),
        from: TableRef::subquery(reads, node_reads_alias()),
        where_clause: Some(on_page(&[node_kind_column(), node_ref_column()])),
        order_by,
        limit_by,
        ..Default::default()
    };
    Query {
        select: [node_key_column(), node_value_column()]
            .into_iter()
            .map(|name| SelectExpr::new(Expr::col(node_reads_alias(), name), name))
            .collect(),
        from: TableRef::subquery(latest, node_reads_alias()),
        where_clause: Some(deleted_false(node_reads_alias())),
        ..Default::default()
    }
}

fn page_nodes_of(columns: &[&str]) -> Query {
    Query {
        select: columns
            .iter()
            .map(|&column| SelectExpr::new(Expr::col(page_nodes_cte(), column), column))
            .collect(),
        from: TableRef::cte(page_nodes_cte(), page_nodes_cte()),
        ..Default::default()
    }
}

fn reachable_entities(input: &Input, model: &(impl OrbitQueryModel + ?Sized)) -> BTreeSet<String> {
    let rel_types = match input.query_type {
        QueryType::Neighbors => input.neighbors.as_ref().map(|n| &n.rel_types),
        _ => input.path.as_ref().map(|p| &p.rel_types),
    };
    let graph = model.graph();
    let wanted = |relationship| {
        rel_types.is_none_or(|types| {
            types.is_empty()
                || types
                    .iter()
                    .any(|name| graph.relationship_id(name) == Some(relationship))
        })
    };
    let variants = graph
        .variants()
        .filter(|variant| wanted(variant.relationship));
    let center = input
        .nodes
        .first()
        .and_then(|node| node.entity.as_deref())
        .and_then(|name| graph.entity_id(name))
        .filter(|_| input.query_type == QueryType::Neighbors);
    let direction = input
        .neighbors
        .as_ref()
        .map(|n| n.direction)
        .unwrap_or_default();
    input
        .nodes
        .iter()
        .filter_map(|node| node.entity.clone())
        .chain(variants.flat_map(|variant| {
            let to_source =
                center.is_none_or(|c| direction != Direction::Outgoing && variant.target == c);
            let to_target =
                center.is_none_or(|c| direction != Direction::Incoming && variant.source == c);
            [(to_source, variant.source), (to_target, variant.target)]
                .into_iter()
                .filter(|(reached, _)| *reached)
                .map(|(_, entity)| graph.entity(entity).name.clone())
        }))
        .collect()
}

fn page(base: Query, with_position: bool) -> Query {
    if !with_position {
        return base;
    }
    Query {
        select: vec![
            SelectExpr::star(),
            SelectExpr::new(
                Expr::func(Function::RowNumberInAllBlocks, vec![]),
                row_position_column(),
            ),
        ],
        from: TableRef::subquery(base, page_cte()),
        ..Default::default()
    }
}

fn page_nodes(query_type: QueryType, nodes: &[PageNode]) -> Query {
    let pair = |id: Expr, kind: Expr| Expr::func(Function::Tuple, vec![id, kind]);
    let mut groups = vec![Expr::func(
        Function::Array,
        nodes
            .iter()
            .map(|node| pair(page_col(&node.id), page_col(&node.kind)))
            .collect(),
    )];
    if query_type == QueryType::PathFinding {
        let path_node = Expr::ident(page_node_alias());
        groups.push(Expr::func(
            Function::ArrayMap,
            vec![
                Expr::lambda(
                    page_node_alias(),
                    pair(
                        tuple_element(path_node.clone(), 1),
                        tuple_element(path_node, 2),
                    ),
                ),
                page_col(path_column()),
            ],
        ));
    }
    let node = Expr::col(page_node_alias(), page_node_alias());
    Query {
        select: [node_ref_column(), node_kind_column()]
            .into_iter()
            .zip(1..)
            .map(|(name, index)| SelectExpr::new(tuple_element(node.clone(), index), name))
            .collect(),
        from: TableRef::subquery(
            Query {
                select: vec![SelectExpr::new(
                    Expr::func(
                        Function::Unnest,
                        vec![Expr::func(Function::ArrayConcat, groups)],
                    ),
                    page_node_alias(),
                )],
                from: TableRef::cte(page_cte(), page_cte()),
                ..Default::default()
            },
            page_node_alias(),
        ),
        ..Default::default()
    }
}

fn lookup() -> Query {
    let collect = |column: &str| {
        Expr::aggregate(
            AggFunction::Collect,
            Some(Expr::col(node_properties_cte(), column)),
        )
    };
    Query {
        select: vec![SelectExpr::new(
            Expr::func(
                Function::MapFromArrays,
                vec![collect(node_key_column()), collect(node_value_column())],
            ),
            lookup_cte(),
        )],
        from: TableRef::cte(node_properties_cte(), node_properties_cte()),
        ..Default::default()
    }
}

fn enriched_path() -> Expr {
    let path_node = Expr::ident(page_node_alias());
    let value = node_value(
        tuple_element(path_node.clone(), 2),
        tuple_element(path_node.clone(), 1),
    );
    Expr::func(
        Function::ArrayMap,
        vec![
            Expr::lambda(
                page_node_alias(),
                Expr::func(Function::TupleConcat, vec![path_node, value]),
            ),
            page_col(path_column()),
        ],
    )
}

fn node_value(kind: Expr, id: Expr) -> Expr {
    Expr::func(
        Function::ArrayElement,
        vec![Expr::col(lookup_cte(), lookup_cte()), node_key(kind, id)],
    )
}

fn node_key(kind: Expr, id: Expr) -> Expr {
    let id = Expr::func(Function::ToString, vec![id]);
    Expr::func(Function::Concat, vec![kind, Expr::string(":"), id])
}

fn tuple_element(value: Expr, index: i64) -> Expr {
    Expr::func(Function::TupleElement, vec![value, Expr::int(index)])
}

fn page_col(column: &str) -> Expr {
    Expr::col(page_cte(), column)
}

fn union(mut arms: Vec<Query>) -> Query {
    let mut first = arms.remove(0);
    first.union_all.extend(arms);
    first
}

fn materialized(name: &str, query: Query) -> Cte {
    Cte {
        materialized: true,
        ..Cte::new(name, query)
    }
}
