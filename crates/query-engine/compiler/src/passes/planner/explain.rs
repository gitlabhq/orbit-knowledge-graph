use super::*;
use crate::passes::plan_node::PlanNode;

pub fn explain(bound: &BoundCatalog, plan: &Plan<Logical>) -> String {
    node(bound, plan).render(0)
}

pub fn explain_clickhouse(bound: &BoundCatalog, plan: &Plan<ClickHouse>) -> String {
    physical_node(bound, plan).render(0)
}

pub fn explain_duckdb(bound: &BoundCatalog, plan: &Plan<DuckDb>) -> String {
    duckdb_node(bound, plan).render(0)
}

fn physical_node(bound: &BoundCatalog, plan: &Plan<ClickHouse>) -> PlanNode {
    let children = plan
        .inputs
        .iter()
        .map(|input| physical_node(bound, input))
        .collect();
    let (label, head, items) = match &plan.operator {
        Operator::Scan(scan) => {
            let access = match &scan.access {
                ClickHouseAccess::Table(access) => format!("Table({})", access.layout.table.0),
                ClickHouseAccess::EdgeTables(access) => format!(
                    "EdgeTables({})",
                    access
                        .layouts
                        .iter()
                        .map(|layout| layout.table.0.as_str())
                        .collect::<Vec<_>>()
                        .join("|")
                ),
                ClickHouseAccess::DenormalizedJoin(access) => {
                    format!("Denormalized({})", access.layout.table.0)
                }
            };
            (
                "Scan",
                format!("{access} AS {}", relation_alias(bound, scan.relation)),
                vec![],
            )
        }
        Operator::CurrentRows { keys, strategy } => (
            "Deduplicate",
            format!("{strategy:?}"),
            keys.iter().map(|key| expression_text(bound, key)).collect(),
        ),
        Operator::Extension(extension) => match extension {
            ClickHouseExtension::FusedNeighbors { columns, .. } => (
                "FusedNeighbors",
                String::new(),
                columns
                    .iter()
                    .map(|column| {
                        format!(
                            "{} AS {}",
                            expression_text(bound, &column.expression),
                            bound.outputs[&column.output].name
                        )
                    })
                    .collect(),
            ),
        },
        operator => operator_parts(bound, operator),
    };
    PlanNode {
        label: label.into(),
        head,
        items,
        children,
    }
}

fn duckdb_node(bound: &BoundCatalog, plan: &Plan<DuckDb>) -> PlanNode {
    let children = plan
        .inputs
        .iter()
        .map(|input| duckdb_node(bound, input))
        .collect();
    let (label, head, items) = match &plan.operator {
        Operator::Scan(scan) => match &scan.access {
            DuckDbAccess::Table(access) => (
                "Scan",
                format!(
                    "Table({}) AS {}",
                    access.layout.table.0,
                    relation_alias(bound, scan.relation)
                ),
                vec![],
            ),
        },
        Operator::CurrentRows { keys, .. } => (
            "Deduplicate",
            "CurrentSnapshot".into(),
            keys.iter().map(|key| expression_text(bound, key)).collect(),
        ),
        Operator::Extension(()) => unreachable!(),
        operator => operator_parts(bound, operator),
    };
    PlanNode {
        label: label.into(),
        head,
        items,
        children,
    }
}

fn operator_parts<F: Flavor>(
    bound: &BoundCatalog,
    operator: &Operator<F>,
) -> (&'static str, String, Vec<String>) {
    match operator {
        Operator::Filter(expression) => ("Filter", expression_text(bound, expression), vec![]),
        Operator::Project(columns) => (
            "Project",
            String::new(),
            columns
                .iter()
                .map(|column| {
                    format!(
                        "{} AS {}",
                        expression_text(bound, &column.expression),
                        bound.outputs[&column.output].name
                    )
                })
                .collect(),
        ),
        Operator::Join(conditions) => (
            "Join",
            format!(
                "ON {}",
                conditions
                    .iter()
                    .map(|condition| expression_text(bound, condition))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            vec![],
        ),
        Operator::SemiJoin(condition) => ("SemiJoin", expression_text(bound, condition), vec![]),
        Operator::Aggregate { groups, metrics } => (
            "Aggregate",
            String::new(),
            groups
                .iter()
                .map(|group| {
                    format!(
                        "group {} AS {}",
                        expression_text(bound, &group.expression),
                        bound.outputs[&group.output].name
                    )
                })
                .chain(metrics.iter().map(|metric| {
                    format!(
                        "{} AS {}",
                        expression_text(bound, &metric.expression),
                        bound.outputs[&metric.output].name
                    )
                }))
                .collect(),
        ),
        Operator::Union => ("Union", String::new(), vec![]),
        Operator::Bind(relation) => ("Bind", format!("r{}", relation.0), vec![]),
        Operator::Sort(keys) => (
            "Sort",
            String::new(),
            keys.iter()
                .map(|key| {
                    format!(
                        "{}{}",
                        expression_text(bound, &key.expression),
                        if key.descending { " DESC" } else { "" }
                    )
                })
                .collect(),
        ),
        Operator::Limit(limit) => ("Limit", limit.to_string(), vec![]),
        Operator::Scan(_) | Operator::CurrentRows { .. } | Operator::Extension(_) => unreachable!(),
    }
}

fn node(bound: &BoundCatalog, plan: &Plan<Logical>) -> PlanNode {
    let children = plan.inputs.iter().map(|input| node(bound, input)).collect();
    let (label, head, items) = match &plan.operator {
        Operator::Scan(scan) => {
            let metadata = &bound.relations[&scan.relation];
            match metadata.origin {
                RelationOrigin::Node { input } => (
                    "NodeScan",
                    format!(
                        "{} AS {}",
                        bound.entities[&metadata.entity.unwrap()].name,
                        bound.input.nodes[input.0].id
                    ),
                    vec![],
                ),
                RelationOrigin::Edge {
                    input: Some(input), ..
                } => {
                    let edge = &bound.input.relationships[input.0];
                    (
                        "EdgeScan",
                        format!(
                            "{} {}->{} AS e{}",
                            metadata
                                .relationships
                                .iter()
                                .map(|kind| bound.relationships[kind].name.as_str())
                                .collect::<Vec<_>>()
                                .join("|"),
                            edge.from,
                            edge.to,
                            input.0
                        ),
                        vec![],
                    )
                }
                RelationOrigin::Edge { .. } => ("EdgeScan", relation(bound, scan.relation), vec![]),
            }
        }
        Operator::Filter(expression) => ("Filter", expression_text(bound, expression), vec![]),
        Operator::Project(columns) => (
            "Project",
            String::new(),
            columns
                .iter()
                .map(|column| {
                    format!(
                        "{} AS {}",
                        expression_text(bound, &column.expression),
                        bound.outputs[&column.output].name
                    )
                })
                .collect(),
        ),
        Operator::Join(conditions) => (
            "Join",
            conditions
                .iter()
                .map(|condition| expression_text(bound, condition))
                .collect::<Vec<_>>()
                .join(" AND "),
            vec![],
        ),
        Operator::SemiJoin(condition) => ("SemiJoin", expression_text(bound, condition), vec![]),
        Operator::Aggregate { groups, metrics } => (
            "Aggregate",
            String::new(),
            groups
                .iter()
                .map(|group| {
                    format!(
                        "group {} AS {}",
                        expression_text(bound, &group.expression),
                        bound.outputs[&group.output].name
                    )
                })
                .chain(metrics.iter().map(|metric| {
                    format!(
                        "{} AS {}",
                        expression_text(bound, &metric.expression),
                        bound.outputs[&metric.output].name
                    )
                }))
                .collect(),
        ),
        Operator::Union => ("Union", String::new(), vec![]),
        Operator::Bind(relation) => ("Bind", format!("r{}", relation.0), vec![]),
        Operator::Sort(keys) => (
            "Sort",
            String::new(),
            keys.iter()
                .map(|key| {
                    format!(
                        "{}{}",
                        expression_text(bound, &key.expression),
                        if key.descending { " DESC" } else { "" }
                    )
                })
                .collect(),
        ),
        Operator::Limit(limit) => ("Limit", limit.to_string(), vec![]),
        Operator::CurrentRows { keys, .. } => (
            "CurrentRows",
            String::new(),
            keys.iter().map(|key| expression_text(bound, key)).collect(),
        ),
        Operator::Extension(()) => unreachable!(),
    };
    PlanNode {
        label: label.into(),
        head,
        items,
        children,
    }
}

fn relation(bound: &BoundCatalog, relation: RelationId) -> String {
    let metadata = &bound.relations[&relation];
    match metadata.origin {
        RelationOrigin::Node { input } => format!(
            "Node({}) AS {}",
            bound.entities[&metadata.entity.unwrap()].name,
            bound.input.nodes[input.0].id
        ),
        RelationOrigin::Edge {
            input: Some(input), ..
        } => format!(
            "Edge({}) AS e{}",
            metadata
                .relationships
                .iter()
                .map(|kind| bound.relationships[kind].name.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            input.0
        ),
        RelationOrigin::Edge { depth, hop, .. } => format!(
            "Edge({}) depth={:?} hop={:?}",
            metadata
                .relationships
                .iter()
                .map(|kind| bound.relationships[kind].name.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            depth,
            hop
        ),
    }
}

fn relation_alias(bound: &BoundCatalog, relation: RelationId) -> String {
    match bound.relations[&relation].origin {
        RelationOrigin::Node { input } => bound.input.nodes[input.0].id.clone(),
        RelationOrigin::Edge {
            input: Some(input), ..
        } => format!("e{}", input.0),
        RelationOrigin::Edge { depth: _, hop, .. } => {
            format!("e{}", hop.unwrap_or(relation.0 + 1))
        }
    }
}

fn expression_text(bound: &BoundCatalog, expression: &Expr) -> String {
    match expression {
        Expr::Column(column) => {
            let column = &bound.columns[column];
            format!("{}.{}", relation_alias(bound, column.relation), column.name)
        }
        Expr::Output(output) => bound.outputs[output].name.clone(),
        Expr::Literal(Value::Int(value)) => value.to_string(),
        Expr::Literal(Value::Float(value)) => value.clone(),
        Expr::Literal(Value::String(value)) => format!("'{value}'"),
        Expr::Literal(Value::Bool(value)) => value.to_string(),
        Expr::Compare { op, left, right } => format!(
            "{} {} {}",
            expression_text(bound, left),
            match op {
                CompareOp::Eq => "=",
                CompareOp::Ne => "!=",
                CompareOp::Lt => "<",
                CompareOp::Le => "<=",
                CompareOp::Gt => ">",
                CompareOp::Ge => ">=",
            },
            expression_text(bound, right)
        ),
        Expr::Filter {
            op, left, right, ..
        } => right.as_ref().map_or_else(
            || format!("{} {op:?}", expression_text(bound, left)),
            |right| {
                format!(
                    "{} {op:?} {}",
                    expression_text(bound, left),
                    expression_text(bound, right)
                )
            },
        ),
        Expr::And(values) => values
            .iter()
            .map(|value| expression_text(bound, value))
            .collect::<Vec<_>>()
            .join(" AND "),
        Expr::Or(values) => values
            .iter()
            .map(|value| expression_text(bound, value))
            .collect::<Vec<_>>()
            .join(" OR "),
        Expr::In { value, .. } => format!("{} IN [...]", expression_text(bound, value)),
        Expr::DateTrunc { unit, value } => format!(
            "date_trunc({}, {})",
            unit.name(),
            expression_text(bound, value)
        ),
        Expr::Aggregate { function, value } => value.as_ref().map_or_else(
            || format!("{}()", function.as_sql()),
            |value| format!("{}({})", function.as_sql(), expression_text(bound, value)),
        ),
        Expr::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| expression_text(bound, value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Tuple(values) => format!(
            "tuple({})",
            values
                .iter()
                .map(|value| expression_text(bound, value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::JsonObject(_) => "json_object(...)".into(),
        Expr::Stringify(value) => format!("stringify({})", expression_text(bound, value)),
        Expr::ListContains { list, .. } => {
            format!("list_contains({})", expression_text(bound, list))
        }
        Expr::TokenMatch { value, .. } => format!("token_match({})", expression_text(bound, value)),
    }
}
