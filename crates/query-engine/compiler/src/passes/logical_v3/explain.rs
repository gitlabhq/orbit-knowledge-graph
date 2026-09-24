use super::{CompareOp, Expr, LogicalOp, LogicalSource, Value};
use crate::passes::plan_node::PlanNode;

impl super::Rel {
    pub fn explain(&self) -> String {
        self.to_node().render(0)
    }

    pub fn to_node(&self) -> PlanNode {
        let children = self.inputs.iter().map(Self::to_node).collect();
        match &self.op {
            LogicalOp::Scan(LogicalSource::Node { entity, alias, .. }) => {
                node("Scan", format!("Node({entity}) AS {alias}"), vec![], children)
            }
            LogicalOp::Scan(LogicalSource::Edge {
                relationships,
                alias,
                ..
            }) => node(
                "Scan",
                format!("Edge({}) AS {alias}", relationships.join("|")),
                vec![],
                children,
            ),
            LogicalOp::Filter(predicate) => {
                node("Filter", expression(predicate), vec![], children)
            }
            LogicalOp::Project(columns) => node(
                "Project",
                String::new(),
                columns
                    .iter()
                    .map(|column| format!("{} AS {}", expression(&column.expression), column.name))
                    .collect(),
                children,
            ),
            LogicalOp::Join(conditions) => node(
                "Join",
                String::new(),
                conditions.iter().map(expression).collect(),
                children,
            ),
            LogicalOp::SemiJoin(condition) => {
                node("SemiJoin", expression(condition), vec![], children)
            }
            LogicalOp::Aggregate { groups, metrics } => node(
                "Aggregate",
                String::new(),
                groups
                    .iter()
                    .map(|group| {
                        format!("group {} AS {}", expression(&group.expression), group.name)
                    })
                    .chain(metrics.iter().map(|metric| {
                        format!("{} AS {}", expression(&metric.expression), metric.name)
                    }))
                    .collect(),
                children,
            ),
            LogicalOp::Alias { alias, .. } => node("Alias", alias.clone(), vec![], children),
            LogicalOp::Union => node("Union", String::new(), vec![], children),
            LogicalOp::Sort(keys) => node(
                "Sort",
                String::new(),
                keys.iter()
                    .map(|key| {
                        format!(
                            "{}{}",
                            expression(&key.expression),
                            if key.descending { " DESC" } else { "" }
                        )
                    })
                    .collect(),
                children,
            ),
            LogicalOp::Limit(limit) => node("Limit", limit.to_string(), vec![], children),
            LogicalOp::LatestBy(keys) => node(
                "LatestBy",
                String::new(),
                keys.iter().map(expression).collect(),
                children,
            ),
        }
    }
}

fn node(label: &str, head: String, items: Vec<String>, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        label: label.into(),
        head,
        items,
        children,
    }
}

fn expression(value: &Expr) -> String {
    match value {
        Expr::Column(column) => format!("r{}.{}", column.relation.0, column.name),
        Expr::Identifier(identifier) => identifier.clone(),
        Expr::Literal(value) => value_text(value),
        Expr::Compare { op, left, right } => {
            format!("{} {} {}", expression(left), compare_text(*op), expression(right))
        }
        Expr::Filter {
            op, left, right, ..
        } => right.as_ref().map_or_else(
            || format!("{} {op:?}", expression(left)),
            |right| format!("{} {op:?} {}", expression(left), expression(right)),
        ),
        Expr::And(expressions) => expressions
            .iter()
            .map(expression)
            .collect::<Vec<_>>()
            .join(" AND "),
        Expr::Or(expressions) => expressions
            .iter()
            .map(expression)
            .collect::<Vec<_>>()
            .join(" OR "),
        Expr::In { value, values, .. } => {
            format!("{} IN [{}]", expression(value), values.iter().map(value_text).collect::<Vec<_>>().join(", "))
        }
        Expr::DateTrunc { unit, value } => {
            format!("date_trunc({}, {})", unit.name(), expression(value))
        }
        Expr::Aggregate { function, value } => value.as_ref().map_or_else(
            || format!("{}()", function.as_sql()),
            |value| format!("{}({})", function.as_sql(), expression(value)),
        ),
        Expr::Array(values) => format!(
            "[{}]",
            values.iter().map(expression).collect::<Vec<_>>().join(", ")
        ),
        Expr::Tuple(values) => format!(
            "tuple({})",
            values.iter().map(expression).collect::<Vec<_>>().join(", ")
        ),
        Expr::JsonObject(entries) => format!(
            "json_object({})",
            entries
                .iter()
                .map(|(key, value)| format!("{key}: {}", expression(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Stringify(value) => format!("stringify({})", expression(value)),
        Expr::PathPrefixAny { value, prefixes } => {
            format!("path_prefix_any({}, {prefixes:?})", expression(value))
        }
        Expr::ListContains { list, value } => {
            format!("list_contains({}, {})", expression(list), value_text(value))
        }
        Expr::TokenMatch { value, token, .. } => {
            format!("token_match({}, {})", expression(value), value_text(token))
        }
    }
}

fn compare_text(operator: CompareOp) -> &'static str {
    match operator {
        CompareOp::Eq => "=",
        CompareOp::Ne => "!=",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
    }
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.clone(),
        Value::String(value) => format!("'{value}'"),
        Value::Bool(value) => value.to_string(),
    }
}
