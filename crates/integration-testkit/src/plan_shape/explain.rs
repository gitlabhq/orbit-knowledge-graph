use compiler::ast::{Expr, Node, Query, TableRef};
use compiler::input::{ColumnSelection, Input, InputFilter, OrderDirection};
use compiler::passes::plan::{FkShape, HydrationStrategy, Plan, Strategy};
use std::collections::HashMap;

use super::pattern::Expression as S;

pub fn logical(input: &Input) -> S {
    S::node(
        "Logical",
        std::iter::once(S::atom(input.query_type))
            .chain(input.nodes.iter().map(|node| {
                S::node(
                    "Node",
                    [
                        S::atom(&node.id),
                        S::atom(node.entity.as_deref().unwrap_or("Unresolved")),
                        S::node("Ids", node.node_ids.iter().map(S::atom)),
                        S::node(
                            "Columns",
                            match &node.columns {
                                Some(ColumnSelection::List(columns)) => {
                                    columns.iter().map(S::atom).collect()
                                }
                                Some(ColumnSelection::All) => vec![S::atom("*")],
                                None => vec![],
                            },
                        ),
                        filters(&node.filters),
                    ],
                )
            }))
            .chain(input.relationships.iter().map(|edge| {
                let (source, target, direction) = match edge.direction {
                    compiler::input::Direction::Incoming => (&edge.to, &edge.from, "Outgoing"),
                    compiler::input::Direction::Outgoing => (&edge.from, &edge.to, "Outgoing"),
                    compiler::input::Direction::Both => (&edge.from, &edge.to, "Both"),
                };
                S::node(
                    "Relationship",
                    [
                        S::atom(source),
                        S::atom(target),
                        S::atom(direction),
                        S::node("Kinds", edge.types.iter().map(S::atom)),
                        S::node("Hops", [S::atom(edge.hops.min), S::atom(edge.hops.max)]),
                        filters(&edge.filters),
                    ],
                )
            }))
            .chain([
                S::node(
                    "Groups",
                    input.aggregation.group_by.iter().map(|group| {
                        S::node(
                            "Group",
                            [
                                S::atom(group.node()),
                                S::atom(group.property().unwrap_or("Node")),
                                S::atom(group.truncate().map_or("None", |unit| unit.name())),
                                S::atom(group.output_name()),
                            ],
                        )
                    }),
                ),
                S::node(
                    "Measures",
                    input.aggregation.metrics.iter().map(|metric| {
                        S::node(
                            "Measure",
                            [
                                S::atom(metric.expr.function()),
                                S::atom(metric.expr.node()),
                                S::atom(metric.expr.property().unwrap_or("Node")),
                                S::atom(metric.output_name()),
                            ],
                        )
                    }),
                ),
                S::node(
                    "OrderBy",
                    input.order_by.iter().map(|order| {
                        S::node(
                            if order.direction == OrderDirection::Desc {
                                "Desc"
                            } else {
                                "Asc"
                            },
                            [S::atom(&order.node), S::atom(&order.property)],
                        )
                    }),
                ),
                S::node(
                    "AggregateOrder",
                    input.aggregation.sort.iter().map(|order| {
                        S::node(
                            if order.direction == OrderDirection::Desc {
                                "Desc"
                            } else {
                                "Asc"
                            },
                            [S::atom(&order.column)],
                        )
                    }),
                ),
                S::node("Limit", [S::atom(input.limit)]),
            ]),
    )
}

fn filters(filters: &HashMap<String, Vec<InputFilter>>) -> S {
    let mut ordered: Vec<_> = filters.iter().collect();
    ordered.sort_by_key(|(property, _)| *property);
    S::node(
        "Filters",
        ordered.into_iter().flat_map(|(property, filters)| {
            filters.iter().map(move |filter| {
                let value = match &filter.rhs_column {
                    Some((node, column)) => S::node("Column", [S::atom(node), S::atom(column)]),
                    None => S::atom(
                        filter
                            .value
                            .as_ref()
                            .map_or("null".into(), ToString::to_string),
                    ),
                };
                S::node(
                    "Predicate",
                    [
                        S::atom(property),
                        S::atom(
                            filter
                                .op
                                .map_or_else(|| "eq".into(), |op| op.as_ref().to_string()),
                        ),
                        value,
                    ],
                )
            })
        }),
    )
}

pub fn physical(plan: &Plan, ast: &Node) -> S {
    use compiler::passes::plan::PlanBody;
    let strategy = match &plan.body {
        PlanBody::Traversal { strategy } | PlanBody::Aggregation { strategy, .. } => match strategy
        {
            Strategy::SingleNode(root) => S::node("SingleNode", [physical_tree(root)]),
            Strategy::Flat(flat) => S::node("Flat", [physical_source(&flat.source)]),
            Strategy::Fk(FkShape::Star { center, .. }) => S::node("FkStar", [S::atom(center)]),
            Strategy::Fk(FkShape::Chain(root)) => S::node("FkChain", [physical_tree(root)]),
        },
        PlanBody::Neighbors { .. } => S::node("Neighbors", []),
        PlanBody::PathFinding(_) => S::node("PathFinding", []),
        PlanBody::Hydration { .. } => S::node("Hydration", []),
    };
    let mut nodes: Vec<_> = plan.nodes.values().collect();
    nodes.sort_by_key(|node| &node.alias);
    S::node(
        "Physical",
        [
            S::node("Strategy", [strategy]),
            S::node(
                "Cascades",
                match &plan.body {
                    PlanBody::Traversal {
                        strategy: Strategy::Flat(flat),
                    }
                    | PlanBody::Aggregation {
                        strategy: Strategy::Flat(flat),
                        ..
                    } => flat
                        .cascades
                        .iter()
                        .enumerate()
                        .filter_map(|(index, anchor)| {
                            anchor.as_ref().map(|anchor| {
                                S::node("Anchor", [S::atom(index), physical_tree(anchor)])
                            })
                        })
                        .collect(),
                    _ => vec![],
                },
            ),
            S::node(
                "Narrowing",
                match &plan.body {
                    PlanBody::Traversal {
                        strategy: Strategy::Flat(flat),
                    }
                    | PlanBody::Aggregation {
                        strategy: Strategy::Flat(flat),
                        ..
                    } => {
                        let mut definitions: Vec<_> = flat
                            .narrowing
                            .iter()
                            .map(|(alias, keys)| ("Keys", alias, keys))
                            .chain(
                                flat.node_narrowing
                                    .iter()
                                    .map(|(alias, keys)| ("NodeKeys", alias, keys)),
                            )
                            .collect();
                        definitions.sort_by_key(|(kind, alias, _)| (*kind, *alias));
                        definitions
                            .into_iter()
                            .map(|(kind, alias, keys)| {
                                S::node(kind, [S::atom(alias), physical_tree(keys)])
                            })
                            .chain(std::iter::once(S::node(
                                "FilterSteps",
                                flat.filters.iter().enumerate().map(|(index, filters)| {
                                    S::node(
                                        "Hop",
                                        [
                                            S::atom(index),
                                            S::node(
                                                "Definitions",
                                                filters.definitions.iter().map(S::atom),
                                            ),
                                            S::node(
                                                "Predicates",
                                                filters.predicates.iter().map(expression),
                                            ),
                                        ],
                                    )
                                }),
                            )))
                            .collect()
                    }
                    PlanBody::Traversal {
                        strategy: Strategy::Fk(FkShape::Star { candidates, .. }),
                    }
                    | PlanBody::Aggregation {
                        strategy: Strategy::Fk(FkShape::Star { candidates, .. }),
                        ..
                    } => {
                        use compiler::passes::plan::fk::TargetNarrowing;
                        let mut targets: Vec<_> = candidates.targets.iter().collect();
                        targets.sort_by_key(|(alias, _)| *alias);
                        vec![
                            S::node(
                                "Definitions",
                                candidates.definitions.iter().map(|(name, keys)| {
                                    S::node("Keys", [S::atom(name), physical_tree(keys)])
                                }),
                            ),
                            S::node(
                                "CenterFilter",
                                candidates.center_filter.iter().map(expression),
                            ),
                            S::node(
                                "Targets",
                                targets.into_iter().map(|(alias, target)| {
                                    S::node(
                                        "Target",
                                        [
                                            S::atom(alias),
                                            match target {
                                                TargetNarrowing::Reference(name) => {
                                                    S::node("Reference", [S::atom(name)])
                                                }
                                                TargetNarrowing::Define { name, keys } => S::node(
                                                    "Define",
                                                    [S::atom(name), physical_tree(keys)],
                                                ),
                                            },
                                        ],
                                    )
                                }),
                            ),
                        ]
                    }
                    _ => vec![],
                },
            ),
            S::node(
                "Nodes",
                nodes.into_iter().map(|node| {
                    S::node(
                        "Node",
                        [
                            S::atom(&node.alias),
                            S::atom(node.table.as_deref().unwrap_or("Unavailable")),
                            S::atom(match node.hydration {
                                HydrationStrategy::Join => "Join",
                                HydrationStrategy::FilterOnly => "FilterOnly",
                                HydrationStrategy::Skip => "Skip",
                            }),
                        ],
                    )
                }),
            ),
            S::node(
                "Hops",
                plan.hops.iter().map(|hop| {
                    S::node(
                        "Hop",
                        [
                            S::atom(&hop.from_node),
                            S::atom(&hop.to_node),
                            S::atom(&hop.edge_table),
                            S::node("Depth", [S::atom(hop.min_hops), S::atom(hop.max_hops)]),
                            S::node("Cascade", [S::atom(hop.cascade_anchor)]),
                        ],
                    )
                }),
            ),
            match ast {
                Node::Query(value) => query(value),
                Node::Insert(_) => S::node("Insert", []),
            },
        ],
    )
}

fn physical_tree(plan: &compiler::passes::plan::physical::PhysicalPlan) -> S {
    S::node(
        "SourceFragment",
        [
            S::node(
                "Outputs",
                plan.outputs.iter().map(|column| {
                    S::node(
                        "Output",
                        [
                            S::atom(column.alias.as_deref().unwrap_or("Unaliased")),
                            expression(&column.expr),
                        ],
                    )
                }),
            ),
            physical_source(&plan.source),
        ],
    )
}

fn physical_source(plan: &compiler::passes::plan::physical::PhysicalSource) -> S {
    use compiler::passes::plan::physical::PhysicalSource;
    match plan {
        PhysicalSource::Union { alias, arms, .. } => S::node(
            "Union",
            std::iter::once(S::atom(alias)).chain(arms.iter().map(physical_tree)),
        ),
        PhysicalSource::Scan {
            table,
            alias,
            final_,
            ..
        } => S::node(
            "Read",
            [
                S::atom(table),
                S::atom(alias),
                S::atom(if *final_ { "Final" } else { "Plain" }),
            ],
        ),
        PhysicalSource::Filter { predicate, input } => {
            S::node("Filter", [expression(predicate), physical_source(input)])
        }
        PhysicalSource::KeyFilter { value, keys, input } => S::node(
            "KeyFilter",
            [
                expression(value),
                physical_tree(keys),
                physical_source(input),
            ],
        ),
        PhysicalSource::Scope { alias, input } => {
            S::node("Scope", [S::atom(alias), physical_source(input)])
        }
        PhysicalSource::Latest {
            alias,
            sort_key,
            input,
        } => S::node(
            "Latest",
            [
                S::atom(alias),
                S::node("Key", sort_key.iter().map(S::atom)),
                physical_source(input),
            ],
        ),
        PhysicalSource::Join {
            kind,
            condition,
            left,
            right,
        } => S::node(
            "Join",
            [
                S::atom(kind),
                expression(condition),
                physical_source(left),
                physical_source(right),
            ],
        ),
    }
}

fn expression(value: &Expr) -> S {
    match value {
        Expr::Column { table, column } => S::node("Column", [S::atom(table), S::atom(column)]),
        Expr::Identifier(name) => S::node("Identifier", [S::atom(name)]),
        Expr::Literal(value) | Expr::Param { value, .. } => S::node("Literal", [S::atom(value)]),
        Expr::FuncCall { name, args } => S::node(
            "Call",
            std::iter::once(S::atom(name)).chain(args.iter().map(expression)),
        ),
        Expr::BinaryOp { op, left, right } => {
            S::node(&op.to_string(), [expression(left), expression(right)])
        }
        Expr::UnaryOp { op, expr } => S::node(&op.to_string(), [expression(expr)]),
        Expr::Lambda { param, body } => S::node("Lambda", [S::atom(param), expression(body)]),
        Expr::InSubquery {
            expr,
            cte_name,
            column,
        } => S::node(
            "InCte",
            [expression(expr), S::atom(cte_name), S::atom(column)],
        ),
        Expr::InSelect { expr, query: inner } => {
            S::node("InQuery", [expression(expr), query(inner)])
        }
        Expr::Scalar(inner) => S::node("Scalar", [query(inner)]),
        Expr::Star => S::atom("Star"),
    }
}

fn relation(value: &TableRef) -> S {
    match value {
        TableRef::Scan {
            table,
            alias,
            final_,
            ..
        } => S::node(
            "Scan",
            [
                S::atom(table),
                S::atom(alias),
                S::atom(if *final_ { "Final" } else { "Plain" }),
            ],
        ),
        TableRef::Join {
            join_type,
            left,
            right,
            on,
        } => S::node(
            "Join",
            [
                S::atom(join_type),
                expression(on),
                relation(left),
                relation(right),
            ],
        ),
        TableRef::Subquery {
            query: inner,
            alias,
        } => S::node("Subquery", [S::atom(alias), query(inner)]),
        TableRef::Union { queries, alias } => S::node(
            "Union",
            std::iter::once(S::atom(alias)).chain(queries.iter().map(query)),
        ),
    }
}

fn query(value: &Query) -> S {
    let mut parts = vec![
        S::node(
            "Ctes",
            value
                .ctes
                .iter()
                .map(|cte| S::node("Cte", [S::atom(&cte.name), query(&cte.query)])),
        ),
        S::node(
            "Select",
            value.select.iter().map(|select| {
                S::node(
                    "Output",
                    [
                        S::atom(select.alias.as_deref().unwrap_or("Unaliased")),
                        expression(&select.expr),
                    ],
                )
            }),
        ),
        relation(&value.from),
        S::node("Where", value.where_clause.iter().map(expression)),
        S::node("GroupBy", value.group_by.iter().map(expression)),
        S::node("Having", value.having.iter().map(expression)),
        S::node(
            "OrderBy",
            value.order_by.iter().map(|order| {
                S::node(
                    if order.desc { "Desc" } else { "Asc" },
                    [expression(&order.expr)],
                )
            }),
        ),
    ];
    if value.distinct {
        parts.push(S::node("Distinct", []));
    }
    if let Some(limit) = value.limit {
        parts.push(S::node("Limit", [S::atom(limit)]));
    }
    if let Some((limit, keys)) = &value.limit_by {
        parts.push(S::node(
            "LimitBy",
            std::iter::once(S::atom(limit)).chain(keys.iter().map(expression)),
        ));
    }
    if !value.union_all.is_empty() {
        parts.push(S::node("UnionAll", value.union_all.iter().map(query)));
    }
    S::node("Query", parts)
}
use query_engine::compiler;
