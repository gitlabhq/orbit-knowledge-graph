use std::collections::HashSet;

use crate::error::Result;

use super::{Expr, Function, ValueId, ValueType, Values, require};

pub type Schema = Vec<ValueId>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubplanId(pub usize);

#[derive(Clone, Debug, PartialEq)]
pub struct Program<S, F, E> {
    pub subplans: Vec<Node<S, F, E>>,
    pub root: Node<S, F, E>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Assignment<F> {
    pub output: ValueId,
    pub expression: Expr<F>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub enum AggregateFunction {
    Count,
    Sum,
    #[strum(serialize = "avg")]
    Average,
    #[strum(serialize = "min")]
    Minimum,
    #[strum(serialize = "max")]
    Maximum,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Measure<F> {
    pub output: ValueId,
    pub function: AggregateFunction,
    pub argument: Option<Expr<F>>,
    pub distinct: bool,
    pub filter: Option<Expr<F>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Semi,
    Anti,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SortKey {
    pub value: ValueId,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Node<S, F, E> {
    pub op: Op<S, F, E>,
    pub inputs: Vec<Self>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Op<S, F, E> {
    Read(S),
    Reference {
        subplan: SubplanId,
        exports: Vec<(ValueId, ValueId)>,
    },
    Filter(Expr<F>),
    Project(Vec<Assignment<F>>),
    Aggregate {
        groups: Vec<Assignment<F>>,
        measures: Vec<Measure<F>>,
    },
    Join {
        kind: JoinKind,
        condition: Expr<F>,
    },
    Union {
        outputs: Schema,
        arms: Vec<Schema>,
    },
    Sort(Vec<SortKey>),
    Limit(u32),
    Extension(E),
}

pub trait Operation {
    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema>;
    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId));

    fn retain_outputs(&mut self, _required: &Schema) -> bool {
        false
    }

    fn unique_keys(&self) -> Vec<Schema> {
        Vec::new()
    }

    fn key_coverage(&self) -> super::facts::KeyCoverage {
        super::facts::KeyCoverage::Unknown
    }
}

impl<S: Operation, F, E: Operation> Node<S, F, E> {
    pub fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        self.visit_mut(&mut |node| match &mut node.op {
            Op::Read(source) => source.map_values(map),
            Op::Extension(extension) => extension.map_values(map),
            Op::Reference { exports, .. } => {
                for (source, output) in exports {
                    map(source);
                    map(output);
                }
            }
            Op::Filter(expression)
            | Op::Join {
                condition: expression,
                ..
            } => expression.map_values(map),
            Op::Project(assignments) => {
                for assignment in assignments {
                    assignment.expression.map_values(map);
                    map(&mut assignment.output);
                }
            }
            Op::Aggregate { groups, measures } => {
                for group in groups {
                    group.expression.map_values(map);
                    map(&mut group.output);
                }
                for measure in measures {
                    for expression in measure.argument.iter_mut().chain(measure.filter.iter_mut()) {
                        expression.map_values(map);
                    }
                    map(&mut measure.output);
                }
            }
            Op::Union { outputs, arms } => {
                for value in outputs.iter_mut().chain(arms.iter_mut().flatten()) {
                    map(value);
                }
            }
            Op::Sort(keys) => {
                for key in keys {
                    map(&mut key.value);
                }
            }
            Op::Limit(_) => {}
        });
    }
}

impl<S, F, E> Node<S, F, E> {
    pub fn map_extensions<T>(self, map: &mut impl FnMut(E) -> T) -> Node<S, F, T> {
        let op = match self.op {
            Op::Read(source) => Op::Read(source),
            Op::Reference { subplan, exports } => Op::Reference { subplan, exports },
            Op::Filter(predicate) => Op::Filter(predicate),
            Op::Project(assignments) => Op::Project(assignments),
            Op::Aggregate { groups, measures } => Op::Aggregate { groups, measures },
            Op::Join { kind, condition } => Op::Join { kind, condition },
            Op::Union { outputs, arms } => Op::Union { outputs, arms },
            Op::Sort(keys) => Op::Sort(keys),
            Op::Limit(limit) => Op::Limit(limit),
            Op::Extension(extension) => Op::Extension(map(extension)),
        };
        Node {
            op,
            inputs: self
                .inputs
                .into_iter()
                .map(|input| input.map_extensions(map))
                .collect(),
        }
    }

    pub fn map_sources<T>(self, map: &mut impl FnMut(S) -> Result<T>) -> Result<Node<T, F, E>> {
        self.expand_sources(&mut |source| {
            Ok(Node {
                op: Op::Read(map(source)?),
                inputs: vec![],
            })
        })
    }

    pub fn expand_sources<T>(
        self,
        map: &mut impl FnMut(S) -> Result<Node<T, F, E>>,
    ) -> Result<Node<T, F, E>> {
        let inputs: Vec<_> = self
            .inputs
            .into_iter()
            .map(|input| input.expand_sources(map))
            .collect::<Result<_>>()?;

        let op = match self.op {
            Op::Read(source) => {
                require(inputs.is_empty(), "read cannot have inputs")?;
                return map(source);
            }
            Op::Filter(predicate) => Op::Filter(predicate),
            Op::Reference { subplan, exports } => Op::Reference { subplan, exports },
            Op::Project(assignments) => Op::Project(assignments),
            Op::Aggregate { groups, measures } => Op::Aggregate { groups, measures },
            Op::Join { kind, condition } => Op::Join { kind, condition },
            Op::Union { outputs, arms } => Op::Union { outputs, arms },
            Op::Sort(keys) => Op::Sort(keys),
            Op::Limit(limit) => Op::Limit(limit),
            Op::Extension(extension) => Op::Extension(extension),
        };

        Ok(Node { op, inputs })
    }

    pub fn visit_mut(&mut self, callback: &mut impl FnMut(&mut Self)) {
        for input in &mut self.inputs {
            input.visit_mut(callback);
        }

        callback(self);
    }
}

impl<S: Operation, F: Function, E: Operation> Node<S, F, E> {
    pub fn output(&self, values: &Values) -> Result<Schema> {
        self.output_with(values, &[])
    }

    pub fn output_with(&self, values: &Values, subplans: &[Schema]) -> Result<Schema> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.output_with(values, subplans))
            .collect::<Result<Vec<_>>>()?;

        let arity = match &self.op {
            Op::Read(_) | Op::Reference { .. } => 0,
            Op::Join { .. } => 2,
            Op::Union { arms, .. } => arms.len(),
            Op::Extension(_) => inputs.len(),
            _ => 1,
        };
        require(inputs.len() == arity, "invalid plan input count")?;

        let output = match &self.op {
            Op::Read(source) => source.output(&inputs, values)?,
            Op::Reference { subplan, exports } => {
                let schema = subplans.get(subplan.0).ok_or_else(|| {
                    crate::error::QueryError::PipelineInvariant(
                        "subplan reference is missing, forward, or cyclic".into(),
                    )
                })?;

                for (source, target) in exports {
                    require(schema.contains(source), "subplan export is unavailable")?;
                    require(
                        values.data_type(*source)? == values.data_type(*target)?,
                        "subplan export type mismatch",
                    )?;
                }

                exports.iter().map(|(_, target)| *target).collect()
            }
            Op::Extension(extension) => extension.output(&inputs, values)?,
            Op::Project(assignments) => {
                for assignment in assignments {
                    require(
                        &assignment.expression.data_type(&inputs[0], values)?
                            == values.data_type(assignment.output)?,
                        "projection output type mismatch",
                    )?;
                }

                assignments
                    .iter()
                    .map(|assignment| assignment.output)
                    .collect()
            }
            Op::Filter(predicate) => {
                require_boolean(predicate.data_type(&inputs[0], values)?)?;
                inputs[0].clone()
            }
            Op::Aggregate { groups, measures } => {
                let input = &inputs[0];
                let mut output = Vec::new();

                for group in groups {
                    require(
                        &group.expression.data_type(input, values)?
                            == values.data_type(group.output)?,
                        "group output type mismatch",
                    )?;
                    output.push(group.output);
                }

                for measure in measures {
                    if let Some(filter) = &measure.filter {
                        require_boolean(filter.data_type(input, values)?)?;
                    }

                    let argument = measure
                        .argument
                        .as_ref()
                        .map(|arg| arg.data_type(input, values))
                        .transpose()?;
                    let expected = measure
                        .function
                        .return_type(argument.as_ref(), measure.distinct)?;
                    require(
                        &expected == values.data_type(measure.output)?,
                        "aggregate output type mismatch",
                    )?;
                    output.push(measure.output);
                }

                require(!output.is_empty(), "aggregate requires an output")?;
                output
            }
            Op::Join { kind, condition } => {
                let available: Schema = inputs.iter().flatten().copied().collect();
                require_unique(&available)?;
                require_boolean(condition.data_type(&available, values)?)?;

                match kind {
                    JoinKind::Inner => available,
                    JoinKind::Semi | JoinKind::Anti => inputs[0].clone(),
                }
            }
            Op::Union { outputs, arms } => {
                require(!arms.is_empty(), "union requires an input")?;
                for (arm, input) in arms.iter().zip(&inputs) {
                    require(arm.len() == outputs.len(), "union width mismatch")?;
                    for (source, target) in arm.iter().zip(outputs) {
                        require(input.contains(source), "union uses an unavailable value")?;
                        require(
                            values.data_type(*source)? == values.data_type(*target)?,
                            "union type mismatch",
                        )?;
                    }
                }

                outputs.clone()
            }
            Op::Sort(keys) => {
                require(
                    keys.iter().all(|key| inputs[0].contains(&key.value)),
                    "sort uses an unavailable value",
                )?;
                inputs[0].clone()
            }
            Op::Limit(_) => inputs[0].clone(),
        };

        require_unique(&output)?;
        for value in &output {
            values.data_type(*value)?;
        }

        Ok(output)
    }
}

impl<S: Operation, F: Function, E: Operation> Program<S, F, E> {
    pub fn output(&self, values: &Values) -> Result<Schema> {
        let mut schemas = Vec::new();

        for subplan in &self.subplans {
            schemas.push(subplan.output_with(values, &schemas)?);
        }

        self.root.output_with(values, &schemas)
    }
}

impl AggregateFunction {
    pub fn return_type(self, argument: Option<&ValueType>, distinct: bool) -> Result<ValueType> {
        require(
            !distinct || argument.is_some(),
            "distinct aggregate requires an argument",
        )?;
        if self == Self::Count {
            return Ok(ValueType::UInt64);
        }

        let argument = argument.ok_or_else(|| {
            crate::error::QueryError::PipelineInvariant("aggregate requires an argument".into())
        })?;
        let base = match argument {
            ValueType::Nullable(inner) => inner.as_ref(),
            other => other,
        };

        if matches!(self, Self::Sum | Self::Average) {
            require(
                matches!(
                    base,
                    ValueType::Int64 | ValueType::UInt64 | ValueType::Float64
                ),
                "numeric aggregate requires a number",
            )?;
        } else {
            require(
                !matches!(
                    base,
                    ValueType::List(_) | ValueType::Record(_) | ValueType::Nullable(_)
                ),
                "aggregate requires a scalar",
            )?;
        }

        let result = if self == Self::Average {
            ValueType::Float64
        } else {
            base.clone()
        };
        Ok(ValueType::Nullable(Box::new(result)))
    }
}

fn require_unique(schema: &Schema) -> Result<()> {
    let mut seen = HashSet::new();
    require(
        schema.iter().all(|value| seen.insert(value)),
        "duplicate plan value",
    )
}

fn require_boolean(data_type: ValueType) -> Result<()> {
    require(
        data_type == ValueType::Bool || data_type == ValueType::Nullable(Box::new(ValueType::Bool)),
        "predicate must be boolean",
    )
}
