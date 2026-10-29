//! Neighbors and path finding read node properties and owner ids inside the base query.
//!
//! ```sql
//! WITH page AS MATERIALIZED (SELECT *, rowNumberInAllBlocks() AS row_position FROM (base) AS page),
//!      page_nodes AS MATERIALIZED (SELECT node_ref, node_kind FROM page ...),
//!      node_properties AS MATERIALIZED (latest row of each page node, one arm per entity)
//! SELECT page columns, owner and properties looked up by kind and id
//! FROM page ORDER BY row_position
//! ```

use std::collections::BTreeSet;

use ontology::constants::DEFAULT_PRIMARY_KEY;
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
            base.selects_alias(&kind).then(|| StaticNode {
                alias: node.id.clone(),
                id,
                kind,
            })
        })
        .collect();
    let outputs: Vec<String> = base.select.iter().filter_map(|s| s.alias.clone()).collect();
    let mut ctes = std::mem::take(&mut base.ctes);
    ctes.push(materialized(page_cte(), page(*base)));
    ctes.push(materialized(
        page_nodes_cte(),
        page_nodes(input.query_type, &static_nodes),
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
        order_by: vec![OrderExpr::asc(page_col(row_position_column()))],
        ..Default::default()
    })))
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
            let ids = Expr::InSelect {
                expr: Box::new(Expr::col(alias, &node.id_property)),
                query: Box::new(Query {
                    select: vec![SelectExpr::col(page_nodes_cte(), node_ref_column())],
                    from: TableRef::cte(page_nodes_cte(), page_nodes_cte()),
                    where_clause: Some(Expr::eq(
                        Expr::col(page_nodes_cte(), node_kind_column()),
                        Expr::string(&node.entity),
                    )),
                    ..Default::default()
                }),
            };
            latest_row_arm(
                node,
                Some(ids),
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

fn page(base: Query) -> Query {
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
}

fn page_nodes(query_type: QueryType, static_nodes: &[StaticNode]) -> Query {
    let from_page = |node_ref: Expr, node_kind: Expr| Query {
        select: vec![
            SelectExpr::new(node_ref, node_ref_column()),
            SelectExpr::new(node_kind, node_kind_column()),
        ],
        from: TableRef::cte(page_cte(), page_cte()),
        ..Default::default()
    };
    let mut arms: Vec<Query> = static_nodes
        .iter()
        .map(|node| from_page(page_col(&node.id), page_col(&node.kind)))
        .collect();
    arms.push(match query_type {
        QueryType::Neighbors => from_page(
            page_col(neighbor_id_column()),
            page_col(neighbor_type_column()),
        ),
        _ => {
            let path_node = Expr::col(page_node_alias(), page_node_alias());
            Query {
                select: vec![
                    SelectExpr::new(tuple_element(path_node.clone(), 1), node_ref_column()),
                    SelectExpr::new(tuple_element(path_node, 2), node_kind_column()),
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
