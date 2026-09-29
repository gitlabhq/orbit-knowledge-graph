use std::fmt::{self, Debug, Display};

use super::generic::{Assignment, Expr, Node, Op, Program, ValueId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SExpression {
    Atom(String),
    List(Vec<Self>),
}

impl SExpression {
    pub fn atom(value: impl ToString) -> Self {
        Self::Atom(value.to_string())
    }

    pub fn node(name: &str, children: impl IntoIterator<Item = Self>) -> Self {
        Self::List(std::iter::once(Self::atom(name)).chain(children).collect())
    }
}

impl Display for SExpression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Atom(atom)
                if atom.is_empty()
                    || atom
                        .chars()
                        .any(|c| c.is_whitespace() || matches!(c, '(' | ')' | '"' | '\\')) =>
            {
                write!(
                    formatter,
                    "{}",
                    serde_json::to_string(atom).map_err(|_| fmt::Error)?
                )
            }
            Self::Atom(atom) => formatter.write_str(atom),
            Self::List(children) => {
                formatter.write_str("(")?;
                for (index, child) in children.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(" ")?;
                    }
                    write!(formatter, "{child}")?;
                }
                formatter.write_str(")")
            }
        }
    }
}

pub fn value(value: ValueId) -> SExpression {
    SExpression::atom(format!("{value:?}"))
}

fn expression<F: Debug>(expr: &Expr<F>) -> SExpression {
    match expr {
        Expr::Value(id) => value(*id),
        Expr::Bool(literal) => SExpression::atom(literal),
        Expr::Int64(literal) => SExpression::atom(literal),
        Expr::UInt64(literal) => SExpression::atom(literal),
        Expr::Float64(literal) => SExpression::atom(literal),
        Expr::String(literal) => SExpression::node("String", [SExpression::atom(literal)]),
        Expr::Null(data_type) => {
            SExpression::node("Null", [SExpression::atom(format!("{data_type:?}"))])
        }
        Expr::Call {
            function,
            arguments,
        } => SExpression::node(&format!("{function:?}"), arguments.iter().map(expression)),
        Expr::Cast { value, data_type } => SExpression::node(
            "Cast",
            [
                expression(value),
                SExpression::atom(format!("{data_type:?}")),
            ],
        ),
    }
}

fn assignment<F: Debug>(assignment: &Assignment<F>) -> SExpression {
    SExpression::node(
        "Assign",
        [value(assignment.output), expression(&assignment.expression)],
    )
}

pub fn tree<S, F: Debug, E>(
    node: &Node<S, F, E>,
    source: &impl Fn(&S) -> SExpression,
    extension: &impl Fn(&E) -> SExpression,
) -> SExpression {
    let (name, mut fields) = match &node.op {
        Op::Read(read) => ("Read", vec![source(read)]),
        Op::Extension(payload) => ("Extension", vec![extension(payload)]),
        Op::Reference { subplan, exports } => (
            "Reference",
            vec![
                SExpression::atom(subplan.0),
                SExpression::node(
                    "Exports",
                    exports
                        .iter()
                        .map(|(from, to)| SExpression::node("Map", [value(*from), value(*to)])),
                ),
            ],
        ),
        Op::Filter(predicate) => ("Filter", vec![expression(predicate)]),
        Op::Project(assignments) => (
            "Project",
            vec![SExpression::node(
                "Outputs",
                assignments.iter().map(assignment),
            )],
        ),
        Op::Aggregate { groups, measures } => (
            "Aggregate",
            vec![
                SExpression::node("Groups", groups.iter().map(assignment)),
                SExpression::node(
                    "Measures",
                    measures.iter().map(|measure| {
                        SExpression::node(
                            "Measure",
                            [
                                value(measure.output),
                                SExpression::atom(measure.function),
                                SExpression::node(
                                    "Argument",
                                    measure.argument.iter().map(expression),
                                ),
                                SExpression::node(
                                    "Distinct",
                                    [SExpression::atom(measure.distinct)],
                                ),
                                SExpression::node("Filter", measure.filter.iter().map(expression)),
                            ],
                        )
                    }),
                ),
            ],
        ),
        Op::Join { kind, condition } => (
            "Join",
            vec![
                SExpression::atom(format!("{kind:?}")),
                expression(condition),
            ],
        ),
        Op::Union { outputs, arms } => (
            "Union",
            vec![
                SExpression::node("Outputs", outputs.iter().copied().map(value)),
                SExpression::node(
                    "Arms",
                    arms.iter()
                        .map(|arm| SExpression::node("Values", arm.iter().copied().map(value))),
                ),
            ],
        ),
        Op::Sort(keys) => (
            "Sort",
            keys.iter()
                .map(|key| {
                    SExpression::node(
                        "Key",
                        [
                            value(key.value),
                            SExpression::atom(if key.descending { "Desc" } else { "Asc" }),
                            SExpression::atom(if key.nulls_first {
                                "NullsFirst"
                            } else {
                                "NullsLast"
                            }),
                        ],
                    )
                })
                .collect(),
        ),
        Op::Limit(limit) => ("Limit", vec![SExpression::atom(limit)]),
    };

    fields.extend(
        node.inputs
            .iter()
            .map(|input| tree(input, source, extension)),
    );
    SExpression::node(name, fields)
}

pub fn program<S, F: Debug, E>(
    program: &Program<S, F, E>,
    source: &impl Fn(&S) -> SExpression,
    extension: &impl Fn(&E) -> SExpression,
) -> SExpression {
    let definitions = program.subplans.iter().enumerate().map(|(index, node)| {
        SExpression::node(
            "Subplan",
            [SExpression::atom(index), tree(node, source, extension)],
        )
    });
    SExpression::node(
        "Program",
        definitions.chain(std::iter::once(tree(&program.root, source, extension))),
    )
}
