use compiler::ast::{Expr, Node, Op, Query, SelectExpr, TableRef};
use compiler::input::{AggFunction, ColumnSelection, Input, InputFilter, OrderDirection};
use compiler::passes::plan::physical::{PhysicalPlan, PhysicalSource};
use compiler::passes::plan::{Plan, PlanBody};
use query_engine::compiler;
use std::collections::HashMap;

use super::operator::Operator;
use super::pattern::Expression as Tree;

fn leaf(label: Operator, text: impl Into<String>) -> Tree {
    Tree::node(label, text, vec![])
}

fn filter(predicates: Vec<String>, input: Tree) -> Tree {
    if predicates.is_empty() {
        return input;
    }
    if input.label == Operator::Filter {
        let mut input = input;
        input.items.extend(predicates);
        input
    } else {
        Tree::node(Operator::Filter, predicates.join(", "), vec![input])
    }
}

pub fn logical(input: &Input) -> Tree {
    let mut children: Vec<_> = input
        .nodes
        .iter()
        .map(|node| {
            let mut predicates = input_filters(&node.id, &node.filters);
            if !node.node_ids.is_empty() {
                predicates.push(format!("{}.id IN {:?}", node.id, node.node_ids));
            }
            if let Some(range) = &node.id_range {
                predicates.extend([
                    format!("{}.id >= {}", node.id, range.start),
                    format!("{}.id <= {}", node.id, range.end),
                ]);
            }
            let scan = filter(
                predicates,
                leaf(
                    Operator::NodeScan,
                    format!(
                        "{} AS {}",
                        node.entity.as_deref().unwrap_or("Unresolved"),
                        node.id
                    ),
                ),
            );
            match &node.columns {
                Some(ColumnSelection::List(columns)) => Tree::node(
                    Operator::Project,
                    columns
                        .iter()
                        .map(|column| format!("{}.{column}", node.id))
                        .collect::<Vec<_>>()
                        .join(", "),
                    vec![scan],
                ),
                Some(ColumnSelection::All) => {
                    Tree::node(Operator::Project, format!("{}.*", node.id), vec![scan])
                }
                None => scan,
            }
        })
        .collect();
    children.extend(input.relationships.iter().enumerate().map(|(index, edge)| {
        let (source, target, arrow) = match edge.direction {
            compiler::input::Direction::Incoming => (&edge.to, &edge.from, "->"),
            compiler::input::Direction::Outgoing => (&edge.from, &edge.to, "->"),
            compiler::input::Direction::Both => (&edge.from, &edge.to, "--"),
        };
        let alias = format!("e{index}");
        let depth = if edge.hops.min == 1 && edge.hops.max == 1 {
            String::new()
        } else {
            format!(" HOPS {}..{}", edge.hops.min, edge.hops.max)
        };
        filter(
            input_filters(&alias, &edge.filters),
            leaf(
                Operator::EdgeScan,
                format!(
                    "{} {source}{arrow}{target} AS {alias}{depth}",
                    edge.types.join("|")
                ),
            ),
        )
    }));
    let mut tree = Tree::node(Operator::Input, input.query_type.to_string(), children);
    if !input.aggregation.metrics.is_empty() || !input.aggregation.group_by.is_empty() {
        let groups = input.aggregation.group_by.iter().map(|group| {
            let value = group.property().map_or_else(
                || group.node().into(),
                |property| format!("{}.{property}", group.node()),
            );
            let value = group.truncate().map_or_else(
                || value.clone(),
                |unit| format!("date_trunc({}, {value})", unit.name()),
            );
            format!("group {value} AS {}", group.output_name())
        });
        let metrics = input.aggregation.metrics.iter().map(|metric| {
            let argument = metric.expr.property().map_or_else(
                || metric.expr.node().into(),
                |property| format!("{}.{property}", metric.expr.node()),
            );
            format!(
                "{}({argument}) AS {}",
                metric.expr.function().to_string().to_uppercase(),
                metric.output_name()
            )
        });
        tree = Tree::node(
            Operator::Aggregate,
            groups.chain(metrics).collect::<Vec<_>>().join(", "),
            vec![tree],
        );
    }
    if let Some(order) = &input.order_by {
        tree = Tree::node(
            Operator::Sort,
            format!(
                "{}.{}{}",
                order.node,
                order.property,
                if order.direction == OrderDirection::Desc {
                    " DESC"
                } else {
                    ""
                }
            ),
            vec![tree],
        );
    }
    if let Some(order) = &input.aggregation.sort {
        tree = Tree::node(
            Operator::Sort,
            format!(
                "{}{}",
                order.column,
                if order.direction == OrderDirection::Desc {
                    " DESC"
                } else {
                    ""
                }
            ),
            vec![tree],
        );
    }
    Tree::node(Operator::Limit, input.limit.to_string(), vec![tree])
}

fn input_filters(alias: &str, filters: &HashMap<String, Vec<InputFilter>>) -> Vec<String> {
    let mut ordered: Vec<_> = filters.iter().collect();
    ordered.sort_by_key(|(property, _)| *property);
    ordered
        .into_iter()
        .flat_map(|(property, filters)| {
            filters.iter().map(move |filter| {
                let value = filter.rhs_column.as_ref().map_or_else(
                    || literal(filter.value.as_ref().unwrap_or(&serde_json::Value::Null)),
                    |(node, column)| format!("{node}.{column}"),
                );
                let operator = filter
                    .op
                    .map_or_else(|| "eq".into(), |op| op.as_ref().to_string());
                let operator = match operator.as_str() {
                    "eq" => "=",
                    "ne" => "!=",
                    "gt" => ">",
                    "gte" => ">=",
                    "lt" => "<",
                    "lte" => "<=",
                    "in" => "IN",
                    other => other,
                };
                format!("{alias}.{property} {operator} {value}")
            })
        })
        .collect()
}

pub fn physical(plan: &Plan, ast: &Node) -> (Tree, Tree) {
    let planned = match &plan.body {
        PlanBody::Traversal { execution } | PlanBody::Aggregation { execution, .. } => {
            let source = Tree::node(
                Operator::Project,
                projections(&execution.outputs),
                vec![physical_source(&execution.source)],
            );
            if execution.definitions.is_empty() {
                source
            } else {
                Tree::node(
                    Operator::With,
                    "",
                    execution
                        .definitions
                        .iter()
                        .map(|(name, keys)| {
                            Tree::node(Operator::Cte, name, vec![physical_tree(keys)])
                        })
                        .chain([source])
                        .collect(),
                )
            }
        }
        PlanBody::Neighbors { .. } => leaf(Operator::Neighbors, ""),
        PlanBody::PathFinding(_) => leaf(Operator::PathFinding, ""),
        PlanBody::Hydration { .. } => leaf(Operator::Hydration, ""),
    };
    let emitted = match ast {
        Node::Query(value) => query(value),
        Node::Insert(_) => leaf(Operator::Insert, ""),
    };
    (planned, emitted)
}

fn physical_tree(plan: &PhysicalPlan) -> Tree {
    Tree::node(
        Operator::Project,
        projections(&plan.outputs),
        vec![physical_source(&plan.source)],
    )
}

fn physical_source(plan: &PhysicalSource) -> Tree {
    match plan {
        PhysicalSource::Union { alias, arms, .. } => Tree::node(
            Operator::Union,
            format!("ALL AS {alias}"),
            arms.iter().map(physical_tree).collect(),
        ),
        PhysicalSource::Scan {
            table,
            alias,
            final_,
            ..
        } => scan(table, alias, *final_),
        PhysicalSource::Filter { predicate, input } => {
            filter(conjuncts(predicate), physical_source(input))
        }
        PhysicalSource::KeyFilter { value, keys, input } => Tree::node(
            Operator::SemiJoin,
            format!("{} IN subquery", expression(value)),
            vec![physical_source(input), physical_tree(keys)],
        ),
        PhysicalSource::Scope { alias, input } => {
            Tree::node(Operator::Bind, alias, vec![physical_source(input)])
        }
        PhysicalSource::Latest {
            alias,
            sort_key,
            input,
        } => Tree::node(
            Operator::Deduplicate,
            format!(
                "LimitBy {}",
                sort_key
                    .iter()
                    .map(|column| format!("{alias}.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            vec![physical_source(input)],
        ),
        PhysicalSource::Join {
            kind,
            condition,
            left,
            right,
        } => Tree::node(
            Operator::Join,
            join_head(&kind.to_string(), condition),
            vec![physical_source(left), physical_source(right)],
        ),
    }
}

fn scan(table: &str, alias: &str, final_: bool) -> Tree {
    let scan = leaf(Operator::Scan, format!("Table({table}) AS {alias}"));
    if final_ {
        Tree::node(Operator::Deduplicate, "Final", vec![scan])
    } else {
        scan
    }
}

fn join_head(kind: &str, condition: &Expr) -> String {
    format!(
        "{}ON {}",
        if kind == "INNER" {
            String::new()
        } else {
            format!("{kind} ")
        },
        expression(condition)
    )
}

fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        value => value.to_string(),
    }
}

fn expression(value: &Expr) -> String {
    match value {
        Expr::Column { table, column } => format!("{table}.{column}"),
        Expr::Identifier(name) => name.clone(),
        Expr::Literal(value) | Expr::Param { value, .. } => literal(value),
        Expr::FuncCall { name, args } => format!(
            "{name}({})",
            args.iter().map(expression).collect::<Vec<_>>().join(", ")
        ),
        Expr::BinaryOp { op, left, right } => {
            let operand = |value: &Expr| {
                let text = expression(value);
                if matches!(value, Expr::BinaryOp { op: child, .. } if child != op || !matches!(op, Op::And | Op::Or))
                {
                    format!("({text})")
                } else {
                    text
                }
            };
            format!("{} {op} {}", operand(left), operand(right))
        }
        Expr::UnaryOp { op, expr } => format!("{op}({})", expression(expr)),
        Expr::Lambda { param, body } => format!("{param} -> {}", expression(body)),
        Expr::InSubquery {
            expr,
            cte_name,
            column,
        } => format!("{} IN {cte_name}.{column}", expression(expr)),
        Expr::InSelect { expr, .. } => format!("{} IN subquery", expression(expr)),
        Expr::Scalar(_) => "scalar(subquery)".into(),
        Expr::Star => "*".into(),
    }
}

fn conjuncts(value: &Expr) -> Vec<String> {
    match value {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => conjuncts(left)
            .into_iter()
            .chain(conjuncts(right))
            .collect(),
        _ => vec![expression(value)],
    }
}

fn query_filter(predicate: &Expr, input: Tree) -> Tree {
    match predicate {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => query_filter(right, query_filter(left, input)),
        Expr::InSelect { expr, query: keys } => Tree::node(
            Operator::SemiJoin,
            format!("{} IN subquery", expression(expr)),
            vec![input, query(keys)],
        ),
        _ => filter(vec![expression(predicate)], input),
    }
}

fn projections(values: &[SelectExpr]) -> String {
    values
        .iter()
        .map(|value| {
            value.alias.as_ref().map_or_else(
                || expression(&value.expr),
                |alias| format!("{} AS {alias}", expression(&value.expr)),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn relation(value: &TableRef) -> Tree {
    match value {
        TableRef::Scan {
            table,
            alias,
            final_,
            ..
        } => scan(table, alias, *final_),
        TableRef::Join {
            join_type,
            left,
            right,
            on,
        } => Tree::node(
            Operator::Join,
            join_head(&join_type.to_string(), on),
            vec![relation(left), relation(right)],
        ),
        TableRef::Subquery {
            query: inner,
            alias,
        } => Tree::node(Operator::Bind, alias, vec![query(inner)]),
        TableRef::Union { queries, alias } => Tree::node(
            Operator::Union,
            format!("ALL AS {alias}"),
            queries.iter().map(query).collect(),
        ),
    }
}

fn query(value: &Query) -> Tree {
    let mut tree = relation(&value.from);
    if let Some(predicate) = &value.where_clause {
        tree = query_filter(predicate, tree);
    }
    let projection = projections(&value.select);
    let aggregate = !value.group_by.is_empty() || value.select.iter().any(|value| {
        matches!(&value.expr, Expr::FuncCall { name, .. } if [AggFunction::Count, AggFunction::Sum, AggFunction::Avg, AggFunction::Min, AggFunction::Max, AggFunction::Collect].iter().any(|function| name == function.as_sql() || name == function.as_sql_if()))
    });
    if !aggregate {
        tree = Tree::node(Operator::Project, projection, vec![tree]);
    } else {
        let items = value
            .group_by
            .iter()
            .map(|key| format!("group {}", expression(key)))
            .chain([projection])
            .collect::<Vec<_>>()
            .join(", ");
        tree = Tree::node(Operator::Aggregate, items, vec![tree]);
    }
    if let Some(predicate) = &value.having {
        tree = query_filter(predicate, tree);
    }
    if value.distinct {
        tree = Tree::node(Operator::Distinct, "", vec![tree]);
    }
    if !value.order_by.is_empty() {
        tree = Tree::node(
            Operator::Sort,
            value
                .order_by
                .iter()
                .map(|order| {
                    format!(
                        "{}{}",
                        expression(&order.expr),
                        if order.desc { " DESC" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            vec![tree],
        );
    }
    if let Some((limit, keys)) = &value.limit_by {
        tree = Tree::node(
            Operator::Deduplicate,
            format!(
                "LimitBy {limit} BY {}",
                keys.iter().map(expression).collect::<Vec<_>>().join(", ")
            ),
            vec![tree],
        );
    }
    if !value.union_all.is_empty() {
        tree = Tree::node(
            Operator::Union,
            "ALL",
            std::iter::once(tree)
                .chain(value.union_all.iter().map(query))
                .collect(),
        );
    }
    if let Some(limit) = value.limit {
        tree = Tree::node(Operator::Limit, limit.to_string(), vec![tree]);
    }
    if !value.ctes.is_empty() {
        tree = Tree::node(
            Operator::With,
            "",
            value
                .ctes
                .iter()
                .map(|cte| Tree::node(Operator::Cte, &cte.name, vec![query(&cte.query)]))
                .chain([tree])
                .collect(),
        );
    }
    tree
}

#[test]
fn exact_assertions_preserve_nested_filter_order() {
    let predicates = [
        Expr::eq(Expr::col("p", "id"), Expr::int(1)),
        Expr::eq(Expr::col("p", "star_count"), Expr::int(2)),
    ];
    let source = predicates.iter().fold(
        PhysicalSource::Scan {
            table: "gl_project".into(),
            alias: "p".into(),
            final_: false,
            relationship: None,
        },
        |input, predicate| PhysicalSource::Filter {
            predicate: predicate.clone(),
            input: Box::new(input),
        },
    );
    let assertions: super::Assertions = orbit_utils::yaml::from_str(
        "exact: (Filter p.id = 1, p.star_count = 2 (Scan Table(gl_project) AS p))",
    )
    .unwrap();
    assertions
        .check(&physical_source(&source), "planned")
        .unwrap();
    let emitted = query_filter(
        &Expr::conjoin(predicates.to_vec()).unwrap(),
        scan("gl_project", "p", false),
    );
    assertions.check(&emitted, "emitted").unwrap();
}
