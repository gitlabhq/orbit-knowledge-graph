use query_data_model::Direction;
use query_data_model::plan::logical::{
    Columns, Comparison, EdgeField, Expr, Literal, LogicalOperation, LogicalPlan, Matching,
    Measure, Operand, Order, Predicate, Projection,
};
use query_data_model::plan::tracking::{NodeId, Ref};
use query_data_model::{Model, RelationshipId};
use query_engine::compiler;

use super::explain::{filter, text_literal};
use super::operator::Operator;
use super::pattern::Expression as Tree;

struct Render<'p> {
    plan: &'p LogicalPlan,
    model: &'p Model,
    edges: Vec<Ref>,
}

pub fn logical(plan: &LogicalPlan, model: &Model) -> Tree {
    let query = compiler::logical::Query::new(plan, model);
    let mut edges: Vec<Ref> = query.steps().iter().map(|step| step.edge).collect();
    for node in plan.nodes() {
        if let LogicalOperation::Neighbors { .. } = plan.operation(node) {
            let outputs: Vec<Ref> = plan.outputs(node).collect();
            edges.extend(outputs.len().checked_sub(2).map(|index| outputs[index]));
        }
    }
    let render = Render { plan, model, edges };
    render.node(plan.root())
}

impl Render<'_> {
    fn node(&self, node: NodeId) -> Tree {
        let inputs: Vec<Tree> = self
            .plan
            .inputs(node)
            .iter()
            .map(|input| self.node(*input))
            .collect();
        let (label, text) = match self.plan.operation(node) {
            LogicalOperation::Entities(entity) => {
                let name = self
                    .plan
                    .outputs(node)
                    .last()
                    .map(|output| self.name(output));
                let entity = &self.model.graph().entity(*entity).name;
                (
                    Operator::Entities,
                    format!("{entity} AS {}", name.unwrap_or_default()),
                )
            }
            LogicalOperation::Filter(predicate) => {
                let [input] = <[Tree; 1]>::try_from(inputs).expect("a filter has one input");
                return filter(self.conjuncts(predicate), input);
            }
            LogicalOperation::Follow {
                from,
                relationships,
                direction,
                range,
                ..
            } => {
                let outputs: Vec<Ref> = self.plan.outputs(node).collect();
                let [.., edge, to] = outputs[..] else {
                    unreachable!("a follow declares an edge and a node")
                };
                let hops = match (range.min, range.max) {
                    (1, 1) => String::new(),
                    (min, max) => format!(" HOPS {min}..{max}"),
                };
                (
                    Operator::Follow,
                    format!(
                        "{} {}{}{} AS {}{hops}",
                        self.relationships(relationships),
                        self.name(*from),
                        arrow(*direction),
                        self.name(to),
                        self.name(edge)
                    ),
                )
            }
            LogicalOperation::Neighbors {
                from,
                relationships,
                direction,
            } => {
                let far = self
                    .plan
                    .outputs(node)
                    .last()
                    .expect("neighbors declare a node");
                (
                    Operator::Neighbors,
                    format!(
                        "{} {}{}{}",
                        self.relationships(relationships),
                        self.name(*from),
                        arrow(*direction),
                        self.name(far)
                    ),
                )
            }
            LogicalOperation::Shortest {
                from,
                to,
                relationships,
                max_depth,
            } => (
                Operator::Shortest,
                format!(
                    "{} {}->{} DEPTH {max_depth}",
                    self.relationships(relationships),
                    self.name(*from),
                    self.name(*to)
                ),
            ),
            LogicalOperation::Aggregate { groups, measures } => {
                let outputs: Vec<Ref> = self.plan.outputs(node).collect();
                let names = outputs[outputs.len() - groups.len() - measures.len()..]
                    .iter()
                    .map(|output| self.name(*output));
                let items = groups
                    .iter()
                    .map(|group| format!("group {}", self.expr(*group)))
                    .chain(measures.iter().map(|measure| self.measure(*measure)))
                    .zip(names)
                    .map(|(item, name)| format!("{item} AS {name}"));
                (Operator::Aggregate, items.collect::<Vec<_>>().join(", "))
            }
            LogicalOperation::Select(items) => {
                let outputs: Vec<Ref> = self.plan.outputs(node).collect();
                let names = outputs[outputs.len() - items.len()..]
                    .iter()
                    .map(|output| self.name(*output));
                let items = items
                    .iter()
                    .zip(names)
                    .map(|(item, name)| self.projection(item, &name));
                (Operator::Select, items.collect::<Vec<_>>().join(", "))
            }
            LogicalOperation::Order(keys) => {
                let keys = keys.iter().map(|(key, order)| match order {
                    Order::Ascending => self.expr(*key),
                    Order::Descending => format!("{} DESC", self.expr(*key)),
                });
                (Operator::Sort, keys.collect::<Vec<_>>().join(", "))
            }
            LogicalOperation::Limit { rows, skip } => {
                let input = Tree::node(Operator::Limit, rows.to_string(), inputs);
                return match skip {
                    Some(skip) => Tree::node(Operator::Offset, skip.to_string(), vec![input]),
                    None => input,
                };
            }
            LogicalOperation::Union => (Operator::Union, String::new()),
            LogicalOperation::Read(_) => (Operator::Scan, String::new()),
            LogicalOperation::Structural(structural) => {
                let language: &str = structural.language.into();
                (
                    Operator::Structural,
                    format!(
                        "{} MATCHES ast('{}', '{language}')",
                        self.expr(structural.target),
                        structural.pattern.text()
                    ),
                )
            }
        };
        Tree::node(label, text, inputs)
    }

    fn name(&self, output: Ref) -> String {
        if let Some(index) = self.edges.iter().position(|edge| *edge == output) {
            return format!("e{index}");
        }
        self.plan.name(output).unwrap_or("_").to_string()
    }

    fn relationships(&self, relationships: &[RelationshipId]) -> String {
        match relationships {
            [] => "*".to_string(),
            named => named
                .iter()
                .map(|id| self.model.graph().relationship(*id).name.as_str())
                .collect::<Vec<_>>()
                .join("|"),
        }
    }

    fn conjuncts(&self, predicate: &Predicate) -> Vec<String> {
        match predicate {
            Predicate::And(all) => all.iter().map(|inner| self.predicate(inner)).collect(),
            other => vec![self.predicate(other)],
        }
    }

    fn predicate(&self, predicate: &Predicate) -> String {
        match predicate {
            Predicate::Compare {
                left,
                comparison,
                right,
            } => {
                let operator = match comparison {
                    Comparison::Eq => "=",
                    Comparison::Ne => "!=",
                    Comparison::Lt => "<",
                    Comparison::Le => "<=",
                    Comparison::Gt => ">",
                    Comparison::Ge => ">=",
                };
                let right = match right {
                    Operand::Literal(literal) => self.literal(literal),
                    Operand::Expr(expr) => self.expr(*expr),
                };
                format!("{} {operator} {right}", self.expr(*left))
            }
            Predicate::In { left, values } => {
                let values: Vec<String> = values.iter().map(|value| self.literal(value)).collect();
                format!("{} IN [{}]", self.expr(*left), values.join(", "))
            }
            Predicate::InTuple { left, values } => {
                let left: Vec<String> = left.iter().map(|expr| self.expr(*expr)).collect();
                let values: Vec<String> = values
                    .iter()
                    .map(|tuple| {
                        let parts: Vec<String> =
                            tuple.iter().map(|value| self.literal(value)).collect();
                        format!("({})", parts.join(", "))
                    })
                    .collect();
                format!("({}) IN [{}]", left.join(", "), values.join(", "))
            }
            Predicate::Text {
                left,
                matching,
                needle,
            } => {
                let function = match matching {
                    Matching::Contains => "contains",
                    Matching::StartsWith => "starts_with",
                    Matching::EndsWith => "ends_with",
                    Matching::Token => "token_match",
                    Matching::AllTokens => "all_tokens",
                    Matching::AnyTokens => "any_tokens",
                };
                format!("{function}({}, {})", self.expr(*left), text_literal(needle))
            }
            Predicate::Null { left, null: true } => format!("{} IS NULL", self.expr(*left)),
            Predicate::Null { left, null: false } => format!("{} IS NOT NULL", self.expr(*left)),
            Predicate::And(all) => all
                .iter()
                .map(|inner| format!("({})", self.predicate(inner)))
                .collect::<Vec<_>>()
                .join(" AND "),
            Predicate::Not(inner) => format!("NOT ({})", self.predicate(inner)),
        }
    }

    fn literal(&self, literal: &Literal) -> String {
        match literal {
            Literal::String(text) | Literal::Temporal(text) => text_literal(text),
            Literal::Int(value) => value.to_string(),
            Literal::Float(value) => value.to_string(),
            Literal::Bool(value) => value.to_string(),
        }
    }

    fn expr(&self, expr: Expr) -> String {
        let graph = self.model.graph();
        match expr {
            Expr::Property { binding, property } => {
                format!("{}.{}", self.name(binding), graph.property(property).name)
            }
            Expr::Truncate {
                binding,
                property,
                unit,
            } => {
                let unit: &str = unit.into();
                format!(
                    "date_trunc({unit}, {}.{})",
                    self.name(binding),
                    graph.property(property).name
                )
            }
            Expr::Node(binding) | Expr::Output(binding) => self.name(binding),
            Expr::Edge {
                binding,
                field: EdgeField::Kind,
            } => format!("type({})", self.name(binding)),
            Expr::Edge { binding, field } => {
                let field = match field {
                    EdgeField::SourceId => "source_id",
                    EdgeField::SourceKind => "source_kind",
                    EdgeField::TargetId => "target_id",
                    EdgeField::TargetKind => "target_kind",
                    EdgeField::Kind => "relationship_kind",
                };
                format!("{}.{field}", self.name(binding))
            }
            Expr::Attribute { binding, attribute } => {
                format!("{}.{}", self.name(binding), graph.attribute(attribute).name)
            }
        }
    }

    fn measure(&self, measure: Measure) -> String {
        let (function, input) = match measure {
            Measure::Count(input) => ("COUNT", input),
            Measure::CountDistinct(input) => ("COUNT_DISTINCT", Some(input)),
            Measure::Sum(input) => ("SUM", Some(input)),
            Measure::Avg(input) => ("AVG", Some(input)),
            Measure::Min(input) => ("MIN", Some(input)),
            Measure::Max(input) => ("MAX", Some(input)),
        };
        format!(
            "{function}({})",
            input.map(|input| self.expr(input)).unwrap_or_default()
        )
    }

    fn projection(&self, item: &Projection, name: &str) -> String {
        match item {
            Projection::Graph { binding, columns } => {
                let binding = self.name(*binding);
                match columns {
                    Columns::Default => binding,
                    Columns::All => format!("{binding}.*"),
                    Columns::Listed(properties) => {
                        let graph = self.model.graph();
                        let listed: Vec<&str> = properties
                            .iter()
                            .map(|id| graph.property(*id).name.as_str())
                            .collect();
                        format!("{binding}{{{}}}", listed.join(", "))
                    }
                }
            }
            Projection::Value(expr) => match self.expr(*expr) {
                value if value == name => value,
                value => format!("{value} AS {name}"),
            },
        }
    }
}

fn arrow(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing => "->",
        Direction::Incoming => "<-",
        Direction::Both => "--",
    }
}
