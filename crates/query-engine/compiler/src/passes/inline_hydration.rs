//! Neighbors and path finding read node properties and owner ids inside the base query.
//!
//! ```sql
//! WITH page AS MATERIALIZED (base),
//!      page_nodes AS MATERIALIZED (SELECT node_ref, node_kind, node_path FROM page ...),
//!      node_properties AS MATERIALIZED (latest row of each page node, one arm per entity)
//! SELECT page columns, owner and properties looked up by kind and id
//! FROM page ORDER BY the base order, mapped to page columns
//! ```

use std::collections::BTreeSet;

use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use query_data_model::OrbitQueryModel;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::{AggFunction, ColumnSelection, Input, InputNode, QueryType};
use crate::passes::hydrate::DynamicEntityColumns;
use crate::passes::lower::query_types::hydration::{latest_row_arm, properties_json};
use crate::passes::plan::query_types::hydration::{
    self as hydration_plan, HydrationCompileOptions,
};

pub fn inline(
    node: Node,
    input: &Input,
    specs: &[DynamicEntityColumns],
    model: &(impl OrbitQueryModel + ?Sized),
) -> Result<Node> {
    let Node::Query(mut base) = node else {
        return Ok(node);
    };
    let arms = property_arms(input, specs, model)?;
    if arms.is_empty() {
        return Ok(Node::Query(base));
    }
    let static_nodes: Vec<StaticNode> = input
        .nodes
        .iter()
        .filter_map(|node| {
            let id = [primary_key_column(&node.id), redaction_id_column(&node.id)]
                .into_iter()
                .find(|column| base.selects_alias(column))?;
            let kind = redaction_type_column(&node.id);
            let path = traversal_path_column(&node.id);
            base.selects_alias(&kind).then(|| StaticNode {
                alias: node.id.clone(),
                id,
                kind,
                path: base.selects_alias(&path).then_some(path),
            })
        })
        .collect();
    let outputs: Vec<String> = base.select.iter().filter_map(|s| s.alias.clone()).collect();
    let neighbor_path = base
        .selects_alias(neighbor_traversal_path_column())
        .then(|| page_col(neighbor_traversal_path_column()));
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
    let mut ctes = std::mem::take(&mut base.ctes);
    ctes.push(materialized(page_cte(), page(*base, order_by.is_none())));
    ctes.push(materialized(
        page_nodes_cte(),
        page_nodes(input.query_type, &static_nodes, neighbor_path),
    ));
    ctes.push(materialized(node_properties_cte(), union(arms)));

    let lookup = lookup_map();
    let mut select: Vec<SelectExpr> = outputs
        .iter()
        .map(|name| match input.query_type {
            QueryType::PathFinding if name == path_column() => {
                SelectExpr::new(enriched_path(&lookup), name.clone())
            }
            _ => SelectExpr::new(page_col(name), name.clone()),
        })
        .collect();
    if input.query_type == QueryType::Neighbors {
        let neighbor = node_value(
            &lookup,
            page_col(neighbor_type_column()),
            page_col(neighbor_id_column()),
        );
        select.push(SelectExpr::new(
            tuple_element(neighbor.clone(), 1),
            neighbor_owner_column(),
        ));
        select.push(SelectExpr::new(
            tuple_element(neighbor, 2),
            neighbor_properties_column(),
        ));
    }
    for node in &static_nodes {
        let value = node_value(&lookup, page_col(&node.kind), page_col(&node.id));
        select.push(SelectExpr::new(
            tuple_element(value, 2),
            properties_column(&node.alias),
        ));
    }
    Ok(Node::Query(Box::new(Query {
        ctes,
        select,
        from: TableRef::cte(page_cte(), page_cte()),
        order_by: order_by.unwrap_or_else(|| vec![OrderExpr::asc(page_col(row_position_column()))]),
        ..Default::default()
    })))
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

fn property_arms(
    input: &Input,
    specs: &[DynamicEntityColumns],
    model: &(impl OrbitQueryModel + ?Sized),
) -> Result<Vec<Query>> {
    let nodes: Vec<InputNode> = reachable_entities(input, model)
        .into_iter()
        .filter(|entity| model.entity_table(entity).is_some())
        .map(|entity| {
            let columns = specs
                .iter()
                .find(|spec| spec.entity_type == entity)
                .map(|spec| spec.columns.clone())
                .unwrap_or_default();
            InputNode {
                id: hydration_node_alias().to_string(),
                entity: Some(entity),
                columns: Some(ColumnSelection::List(columns)),
                ..InputNode::default()
            }
        })
        .collect();
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    let input = Input {
        query_type: QueryType::Hydration,
        nodes,
        ..Input::default()
    };
    let plan = hydration_plan::plan(&input, model, HydrationCompileOptions::default())?;
    Ok(plan
        .operation
        .nodes
        .iter()
        .map(|node| {
            let alias = &node.alias;
            let owner = model
                .redaction_id_column_named(&node.entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY);
            let key = node_key(
                Expr::string(&node.entity),
                Expr::col(alias, &node.id_property),
            );
            let value = Expr::func(
                Function::Tuple,
                vec![Expr::col(alias, owner), properties_json(node)],
            );
            let of_kind = |select: SelectExpr, path: Option<Op>| Query {
                select: vec![select],
                from: TableRef::cte(page_nodes_cte(), page_nodes_cte()),
                where_clause: Expr::conjoin(
                    std::iter::once(Expr::eq(
                        Expr::col(page_nodes_cte(), node_kind_column()),
                        Expr::string(&node.entity),
                    ))
                    .chain(path.map(|op| {
                        Expr::binary(
                            op,
                            Expr::col(page_nodes_cte(), node_path_column()),
                            Expr::string(""),
                        )
                    }))
                    .collect(),
                ),
                ..Default::default()
            };
            let ids = Expr::InSelect {
                expr: Box::new(Expr::col(alias, &node.id_property)),
                query: Box::new(of_kind(
                    SelectExpr::col(page_nodes_cte(), node_ref_column()),
                    None,
                )),
            };
            let known_paths = Expr::InSelect {
                expr: Box::new(Expr::col(alias, TRAVERSAL_PATH_COLUMN)),
                query: Box::new(of_kind(
                    SelectExpr::col(page_nodes_cte(), node_path_column()),
                    Some(Op::Ne),
                )),
            };
            let unknown_path = Expr::binary(
                Op::Gt,
                Expr::Scalar(Box::new(of_kind(
                    SelectExpr::new(
                        Expr::Aggregate {
                            function: AggFunction::Count,
                            argument: None,
                            distinct: false,
                            condition: None,
                        },
                        node_ref_column(),
                    ),
                    Some(Op::Eq),
                ))),
                Expr::int(0),
            );
            let paths = model
                .entity_has_traversal_path(&node.entity)
                .then(|| Expr::or_all([Some(known_paths), Some(unknown_path)]))
                .flatten();
            latest_row_arm(
                node,
                Expr::conjoin(std::iter::once(ids).chain(paths).collect()),
                &[owner],
                vec![
                    SelectExpr::new(key, node_key_column()),
                    SelectExpr::new(value, node_value_column()),
                ],
            )
        })
        .collect())
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
    let mut entities: BTreeSet<String> = input
        .nodes
        .iter()
        .filter_map(|node| node.entity.clone())
        .collect();
    for variant in graph
        .variants()
        .filter(|variant| wanted(variant.relationship))
    {
        entities.insert(graph.entity(variant.source).name.clone());
        entities.insert(graph.entity(variant.target).name.clone());
    }
    entities
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

struct StaticNode {
    alias: String,
    id: String,
    kind: String,
    path: Option<String>,
}

fn page_nodes(
    query_type: QueryType,
    static_nodes: &[StaticNode],
    neighbor_path: Option<Expr>,
) -> Query {
    let from_page = |node_ref: Expr, node_kind: Expr, node_path: Option<Expr>| Query {
        select: vec![
            SelectExpr::new(node_ref, node_ref_column()),
            SelectExpr::new(node_kind, node_kind_column()),
            SelectExpr::new(
                node_path.unwrap_or_else(|| Expr::string("")),
                node_path_column(),
            ),
        ],
        from: TableRef::cte(page_cte(), page_cte()),
        ..Default::default()
    };
    let mut arms: Vec<Query> = static_nodes
        .iter()
        .map(|node| {
            from_page(
                page_col(&node.id),
                page_col(&node.kind),
                node.path.as_deref().map(page_col),
            )
        })
        .collect();
    arms.push(match query_type {
        QueryType::Neighbors => from_page(
            page_col(neighbor_id_column()),
            page_col(neighbor_type_column()),
            neighbor_path,
        ),
        _ => {
            let path_node = Expr::col(page_node_alias(), page_node_alias());
            Query {
                select: vec![
                    SelectExpr::new(tuple_element(path_node.clone(), 1), node_ref_column()),
                    SelectExpr::new(tuple_element(path_node, 2), node_kind_column()),
                    SelectExpr::new(Expr::string(""), node_path_column()),
                ],
                from: TableRef::subquery(
                    Query {
                        select: vec![SelectExpr::new(
                            Expr::func(Function::Unnest, vec![page_col(path_column())]),
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
    });
    union(arms)
}

fn lookup_map() -> Expr {
    let collect = |column: &str| Expr::Aggregate {
        function: AggFunction::Collect,
        argument: Some(Box::new(Expr::col(node_properties_cte(), column))),
        distinct: false,
        condition: None,
    };
    Expr::Scalar(Box::new(Query {
        select: vec![SelectExpr::new(
            Expr::func(
                Function::MapFromArrays,
                vec![collect(node_key_column()), collect(node_value_column())],
            ),
            node_value_column(),
        )],
        from: TableRef::cte(node_properties_cte(), node_properties_cte()),
        ..Default::default()
    }))
}

fn enriched_path(lookup: &Expr) -> Expr {
    let param = page_node_alias();
    let path_node = Expr::ident(param);
    let value = node_value(
        lookup,
        tuple_element(path_node.clone(), 2),
        tuple_element(path_node.clone(), 1),
    );
    Expr::func(
        Function::ArrayMap,
        vec![
            Expr::lambda(
                param,
                Expr::func(
                    Function::Tuple,
                    vec![
                        tuple_element(path_node.clone(), 1),
                        tuple_element(path_node, 2),
                        tuple_element(value.clone(), 1),
                        tuple_element(value, 2),
                    ],
                ),
            ),
            page_col(path_column()),
        ],
    )
}

fn node_value(lookup: &Expr, kind: Expr, id: Expr) -> Expr {
    Expr::func(
        Function::ArrayElement,
        vec![lookup.clone(), node_key(kind, id)],
    )
}

fn node_key(kind: Expr, id: Expr) -> Expr {
    Expr::func(
        Function::Concat,
        vec![
            kind,
            Expr::string(":"),
            Expr::func(Function::ToString, vec![id]),
        ],
    )
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
