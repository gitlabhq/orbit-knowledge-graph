//! A logical plan as an indented tree of operators, in the model's names: the binding, entity,
//! relationship and property names a query was written with, not plan, output or catalog ids.

use query_data_model::plan::logical::{
    Columns, Comparison, Expr, Literal, LogicalOperation, LogicalPlan, Matching, Measure, Operand,
    Order, Predicate, Projection,
};
use query_data_model::plan::tracking::{NodeId, Ref};
use query_data_model::{Direction, Model, RelationshipId};

pub fn tree(plan: &LogicalPlan, model: &Model) -> Vec<String> {
    let mut lines = Vec::new();
    walk(&Names { plan, model }, plan.root(), 0, &mut lines);
    lines
}

struct Names<'a> {
    plan: &'a LogicalPlan,
    model: &'a Model,
}

fn walk(names: &Names, node: NodeId, depth: usize, lines: &mut Vec<String>) {
    lines.push(format!("{}{}", "  ".repeat(depth), names.operation(node)));
    for input in names.plan.inputs(node) {
        walk(names, *input, depth + 1, lines);
    }
}

fn list<T>(items: &[T], show: impl Fn(&T) -> String) -> String {
    items.iter().map(show).collect::<Vec<_>>().join(", ")
}

impl Names<'_> {
    fn binding(&self, binding: Ref) -> String {
        self.plan.name(binding).unwrap_or("_").to_string()
    }

    /// The binding a node introduces: its last named output.
    fn introduced(&self, node: NodeId) -> String {
        let named: Vec<Ref> = self
            .plan
            .outputs(node)
            .filter(|output| self.plan.name(*output).is_some())
            .collect();
        named
            .last()
            .map_or("_".into(), |output| self.binding(*output))
    }

    fn property(&self, id: query_data_model::PropertyId) -> &str {
        &self.model.graph().property(id).name
    }

    fn relationships(&self, ids: &[RelationshipId]) -> String {
        let graph = self.model.graph();
        match ids.len() {
            0 => "*".into(),
            1..=3 => ids
                .iter()
                .map(|id| graph.relationship(*id).name.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            n => format!("{n} relationships"),
        }
    }

    fn arrow(&self, from: &str, ids: &[RelationshipId], direction: Direction, to: &str) -> String {
        let rels = self.relationships(ids);
        match direction {
            Direction::Outgoing => format!("{from} -[{rels}]-> {to}"),
            Direction::Incoming => format!("{from} <-[{rels}]- {to}"),
            Direction::Both => format!("{from} -[{rels}]- {to}"),
        }
    }

    fn operation(&self, node: NodeId) -> String {
        let to = || self.introduced(node);
        match self.plan.operation(node) {
            LogicalOperation::Entities(entity) => {
                format!(
                    "Entities {}: {}",
                    to(),
                    self.model.graph().entity(*entity).name
                )
            }
            LogicalOperation::Filter(predicate) => format!("Filter {}", self.predicate(predicate)),
            LogicalOperation::Follow {
                from,
                relationships,
                direction,
                range,
                ..
            } => {
                let hops = match (range.min, range.max) {
                    (1, 1) => String::new(),
                    (min, max) => format!(" *{min}..{max}"),
                };
                let arrow = self.arrow(&self.binding(*from), relationships, *direction, &to());
                format!("Follow {arrow}{hops}")
            }
            LogicalOperation::Neighbors {
                from,
                relationships,
                direction,
            } => {
                format!(
                    "Neighbors {}",
                    self.arrow(&self.binding(*from), relationships, *direction, &to())
                )
            }
            LogicalOperation::Shortest {
                from,
                to: end,
                relationships,
                max_depth,
            } => {
                let arrow = self.arrow(
                    &self.binding(*from),
                    relationships,
                    Direction::Both,
                    &self.binding(*end),
                );
                format!("Shortest {arrow} *..{max_depth}")
            }
            LogicalOperation::Aggregate { groups, measures } => format!(
                "Aggregate {} by {}",
                list(measures, |m| self.measure(m)),
                list(groups, |g| self.expr(g))
            ),
            LogicalOperation::Select(projections) => {
                format!("Select {}", list(projections, |p| self.projection(p)))
            }
            LogicalOperation::Order(keys) => format!(
                "Order {}",
                list(keys, |(expr, order)| match order {
                    Order::Ascending => format!("{} asc", self.expr(expr)),
                    Order::Descending => format!("{} desc", self.expr(expr)),
                })
            ),
            LogicalOperation::Limit {
                rows,
                skip: Some(skip),
            } => format!("Limit {rows} skip {skip}"),
            LogicalOperation::Limit { rows, skip: None } => format!("Limit {rows}"),
            LogicalOperation::Union => "Union".into(),
            LogicalOperation::Read(body) => format!("Read {}", self.plan.body(*body).0),
            LogicalOperation::Structural(structural) => format!(
                "Structural {} matches ast('{}', {})",
                self.expr(&structural.target),
                structural.pattern.text(),
                <&str>::from(structural.language)
            ),
        }
    }

    fn projection(&self, projection: &Projection) -> String {
        match projection {
            Projection::Graph {
                binding,
                columns: Columns::Default,
            } => self.binding(*binding),
            Projection::Graph {
                binding,
                columns: Columns::All,
            } => format!("{}{{*}}", self.binding(*binding)),
            Projection::Graph {
                binding,
                columns: Columns::Listed(ids),
            } => {
                format!(
                    "{}{{{}}}",
                    self.binding(*binding),
                    list(ids, |id| self.property(*id).to_string())
                )
            }
            Projection::Value(expr) => self.expr(expr),
        }
    }

    fn measure(&self, measure: &Measure) -> String {
        match measure {
            Measure::Count(None) => "count(*)".into(),
            Measure::Count(Some(e)) => format!("count({})", self.expr(e)),
            Measure::CountDistinct(e) => format!("count(distinct {})", self.expr(e)),
            Measure::Sum(e) => format!("sum({})", self.expr(e)),
            Measure::Avg(e) => format!("avg({})", self.expr(e)),
            Measure::Min(e) => format!("min({})", self.expr(e)),
            Measure::Max(e) => format!("max({})", self.expr(e)),
        }
    }

    fn expr(&self, expr: &Expr) -> String {
        match expr {
            Expr::Property { binding, property } => {
                format!("{}.{}", self.binding(*binding), self.property(*property))
            }
            Expr::Truncate {
                binding,
                property,
                unit,
            } => {
                format!(
                    "{}({}.{})",
                    <&str>::from(*unit),
                    self.binding(*binding),
                    self.property(*property)
                )
            }
            Expr::Node(binding) | Expr::Output(binding) => self.binding(*binding),
            Expr::Edge { binding, field } => {
                format!("{}.{field:?}", self.binding(*binding)).to_lowercase()
            }
            Expr::Attribute { binding, attribute } => {
                format!(
                    "{}.{}",
                    self.binding(*binding),
                    self.model.graph().attribute(*attribute).name
                )
            }
        }
    }

    fn predicate(&self, predicate: &Predicate) -> String {
        match predicate {
            Predicate::Compare {
                left,
                comparison,
                right,
            } => {
                let right = match right {
                    Operand::Literal(literal) => literal_text(literal),
                    Operand::Expr(expr) => self.expr(expr),
                };
                format!(
                    "{} {} {right}",
                    self.expr(left),
                    comparison_text(*comparison)
                )
            }
            Predicate::In { left, values } => {
                format!("{} in [{}]", self.expr(left), list(values, literal_text))
            }
            Predicate::InTuple { left, values } => format!(
                "({}) in [{}]",
                list(left, |e| self.expr(e)),
                list(values, |tuple| format!("({})", list(tuple, literal_text)))
            ),
            Predicate::Text {
                left,
                matching,
                needle,
            } => {
                format!(
                    "{} {} '{needle}'",
                    self.expr(left),
                    matching_text(*matching)
                )
            }
            Predicate::Null { left, null: true } => format!("{} is null", self.expr(left)),
            Predicate::Null { left, null: false } => format!("{} is not null", self.expr(left)),
            Predicate::And(all) => all
                .iter()
                .map(|p| self.predicate(p))
                .collect::<Vec<_>>()
                .join(" and "),
            Predicate::Not(inner) => format!("not ({})", self.predicate(inner)),
        }
    }
}

fn literal_text(literal: &Literal) -> String {
    match literal {
        Literal::String(text) | Literal::Temporal(text) => format!("'{text}'"),
        Literal::Int(number) => number.to_string(),
        Literal::Float(number) => number.to_string(),
        Literal::Bool(flag) => flag.to_string(),
    }
}

fn comparison_text(comparison: Comparison) -> &'static str {
    match comparison {
        Comparison::Eq => "=",
        Comparison::Ne => "<>",
        Comparison::Lt => "<",
        Comparison::Le => "<=",
        Comparison::Gt => ">",
        Comparison::Ge => ">=",
    }
}

fn matching_text(matching: Matching) -> &'static str {
    match matching {
        Matching::Contains => "contains",
        Matching::StartsWith => "starts with",
        Matching::EndsWith => "ends with",
        Matching::Token => "matches token",
        Matching::AllTokens => "matches all tokens",
        Matching::AnyTokens => "matches any token",
    }
}
