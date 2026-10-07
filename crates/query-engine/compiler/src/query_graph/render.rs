use super::*;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, LoweredOperation<'catalog>>
{
    pub fn render(&self, root: BlockId) -> Result<String> {
        self.validate_lowered(root)?;
        self.render_block(root, true)
    }

    pub fn render_parameterized(
        mut self,
        root: BlockId,
    ) -> Result<(
        String,
        std::collections::HashMap<String, orbit_utils::query_types::ParamValue>,
    )> {
        self.validate_lowered(root)?;
        let mut bindings = orbit_utils::query_types::ParamBindings::default();
        for block in &mut self.blocks {
            if let Body::Select {
                outputs, operation, ..
            } = &mut block.body
            {
                for output in outputs {
                    output.value.bind_parameters(&mut bindings);
                }
                operation.bind_parameters(&mut bindings);
            }
        }
        Ok((self.render_block(root, true)?, bindings.into_map()))
    }

    fn render_block(&self, id: BlockId, public: bool) -> Result<String> {
        let block = self.block(id)?;
        let mut sql = String::new();
        if !block.definitions.is_empty() {
            sql.push_str(
                if block
                    .definitions
                    .iter()
                    .any(|definition| definition.recursive)
                {
                    "WITH RECURSIVE "
                } else {
                    "WITH "
                },
            );
            let definitions = block
                .definitions
                .iter()
                .enumerate()
                .map(|(slot, definition)| {
                    Ok(format!(
                        "{} AS ({})",
                        definition_name(DefinitionId { block: id, slot }),
                        self.render_block(definition.body, false)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&definitions.join(", "));
            sql.push(' ');
        }
        match &block.body {
            Body::Select {
                outputs, operation, ..
            } => {
                let projection = outputs
                    .iter()
                    .enumerate()
                    .map(|(slot, output)| {
                        let name = if public {
                            quoted(&output.label)
                        } else {
                            output_name(OutputId { block: id, slot })
                        };
                        Ok(format!(
                            "{} AS {name}",
                            self.render_expression_at(&output.value, true)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&format!("SELECT {}", projection.join(", ")));
                let mut needed = Vec::new();
                for output in outputs {
                    collect_columns(&output.value, &mut needed)?;
                }
                let (input, tail, groups) = self.render_operation(id, operation, &needed)?;
                sql.push_str(&format!(" FROM ({input}) AS q"));
                if !groups.is_empty() {
                    sql.push_str(&format!(
                        " GROUP BY {}",
                        groups
                            .iter()
                            .map(|group| self.render_expression_at(group, true))
                            .collect::<Result<Vec<_>>>()?
                            .join(", ")
                    ));
                }
                sql.push_str(&tail);
            }
            Body::UnionAll { arms, labels } => {
                let arms = arms
                    .iter()
                    .map(|arm| {
                        let projection = labels
                            .iter()
                            .enumerate()
                            .map(|(slot, label)| {
                                let name = if public {
                                    quoted(label)
                                } else {
                                    output_name(OutputId { block: id, slot })
                                };
                                format!(
                                    "u.{} AS {name}",
                                    output_name(OutputId { block: *arm, slot })
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        Ok(format!(
                            "(SELECT {projection} FROM ({}) AS u)",
                            self.render_block(*arm, false)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&arms.join(" UNION ALL "));
            }
        }
        Ok(sql)
    }

    fn render_column(&self, column: ColumnRef<'catalog>) -> Result<String> {
        let port = match column.port {
            Port::Stored(column) => quoted(column.name()),
            Port::Output(output) => {
                self.output_label(output)?;
                output_name(output)
            }
        };
        Ok(format!("{}.{port}", relation_name(column.relation)))
    }

    fn render_operation(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        needed: &[ColumnRef<'catalog>],
    ) -> Result<(String, String, Vec<Expression<'catalog>>)> {
        use Relational::*;
        let columns = needed;
        let selection = |columns: &[ColumnRef<'catalog>]| {
            if columns.is_empty() {
                "1 AS _unit".into()
            } else {
                columns
                    .iter()
                    .map(|column| format!("q.{}", value_name(*column)))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        Ok(match operation {
            One => ("SELECT 1 AS _unit".into(), String::new(), vec![]),
            Source { relation, read } => {
                let source = match self.relation(*relation)?.source {
                    crate::query_graph::Source::Stored(table) => quoted(table.name()),
                    crate::query_graph::Source::Derived(body) => {
                        format!("({})", self.render_block(body, false)?)
                    }
                    crate::query_graph::Source::Definition(definition) => {
                        definition_name(definition)
                    }
                };
                let projection = columns
                    .iter()
                    .map(|column| {
                        Ok(format!(
                            "{} AS {}",
                            self.render_column(*column)?,
                            value_name(*column)
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let projection = if projection.is_empty() {
                    "1 AS _unit".into()
                } else {
                    projection
                };
                (
                    format!(
                        "SELECT {projection} FROM {source} AS {}{}",
                        relation_name(*relation),
                        if matches!(read, ReadMode::Current) {
                            " FINAL"
                        } else {
                            ""
                        }
                    ),
                    String::new(),
                    vec![],
                )
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left_columns = self.operation_outputs(left)?;
                let right_columns = self.operation_outputs(right)?;
                let mut required = needed.to_vec();
                collect_columns(condition, &mut required)?;
                let left_needed = required
                    .iter()
                    .filter(|column| left_columns.contains(column))
                    .copied()
                    .collect::<Vec<_>>();
                let right_needed = required
                    .iter()
                    .filter(|column| right_columns.contains(column))
                    .copied()
                    .collect::<Vec<_>>();
                let left = self.materialize_operation(block, left, &left_needed)?;
                let right = self.materialize_operation(block, right, &right_needed)?;
                if matches!(kind, JoinKind::Membership) {
                    let Expression::Equal(value, key) = condition else {
                        return Err(GraphError::JoinShape);
                    };
                    let (Expression::Column(value), Expression::Column(key)) =
                        (value.as_ref(), key.as_ref())
                    else {
                        return Err(GraphError::JoinShape);
                    };
                    return Ok((
                        format!(
                            "SELECT {} FROM ({left}) AS q WHERE q.{} IN (SELECT k.{} FROM ({right}) AS k)",
                            selection(columns),
                            value_name(*value),
                            value_name(*key)
                        ),
                        String::new(),
                        vec![],
                    ));
                }
                let projection = columns
                    .iter()
                    .map(|column| {
                        format!(
                            "{}.{}",
                            if left_columns.contains(column) {
                                "l"
                            } else {
                                "r"
                            },
                            value_name(*column)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let projection = if projection.is_empty() {
                    "1 AS _unit".into()
                } else {
                    projection
                };
                let condition = self.render_expression_with(condition, &|column| {
                    let side = if left_columns.contains(&column) {
                        "l"
                    } else if right_columns.contains(&column) {
                        "r"
                    } else {
                        return Err(GraphError::OperationVisibility);
                    };
                    Ok(format!("{side}.{}", value_name(column)))
                })?;
                let keyword = match kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Cross => "CROSS JOIN",
                    JoinKind::Semi => "LEFT SEMI JOIN",
                    JoinKind::Membership => unreachable!(),
                };
                (
                    format!(
                        "SELECT {projection} FROM ({left}) AS l {keyword} ({right}) AS r{}",
                        if matches!(kind, JoinKind::Cross) {
                            String::new()
                        } else {
                            format!(" ON {condition}")
                        }
                    ),
                    String::new(),
                    vec![],
                )
            }
            Filter { input, predicate } => {
                let mut required = needed.to_vec();
                collect_columns(predicate, &mut required)?;
                if let Source { relation, read } = input.as_ref()
                    && let crate::query_graph::Source::Stored(table) =
                        self.relation(*relation)?.source
                {
                    let projection = needed
                        .iter()
                        .map(|column| {
                            Ok(format!(
                                "{} AS {}",
                                self.render_column(*column)?,
                                value_name(*column)
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    let projection = if projection.is_empty() {
                        "1 AS _unit".into()
                    } else {
                        projection
                    };
                    return Ok((
                        format!(
                            "SELECT {projection} FROM {} AS {}{} WHERE {}",
                            quoted(table.name()),
                            relation_name(*relation),
                            if matches!(read, ReadMode::Current) {
                                " FINAL"
                            } else {
                                ""
                            },
                            self.render_expression_at(predicate, false)?
                        ),
                        String::new(),
                        vec![],
                    ));
                }
                let input = self.materialize_operation(block, input, &required)?;
                (
                    format!(
                        "SELECT {} FROM ({input}) AS q WHERE {}",
                        selection(columns),
                        self.render_expression_at(predicate, true)?
                    ),
                    String::new(),
                    vec![],
                )
            }
            Aggregate { input, groups } => {
                let mut required = needed.to_vec();
                for group in groups {
                    collect_columns(group, &mut required)?;
                }
                (
                    self.materialize_operation(block, input, &required)?,
                    String::new(),
                    groups.clone(),
                )
            }
            Sort { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().map(|(column, _)| *column));
                let (sql, tail, groups) = self.render_operation(block, input, &required)?;
                if !tail.is_empty() {
                    return Err(GraphError::JoinShape);
                }
                let order = keys
                    .iter()
                    .map(|(column, descending)| {
                        format!(
                            "q.{} {}",
                            value_name(*column),
                            if *descending { "DESC" } else { "ASC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (sql, format!(" ORDER BY {order}"), groups)
            }
            FirstBy { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().copied());
                let (sql, tail, groups) = self.render_operation(block, input, &required)?;
                (
                    sql,
                    format!(
                        "{tail} LIMIT 1 BY {}",
                        keys.iter()
                            .map(|column| format!("q.{}", value_name(*column)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    groups,
                )
            }
            Limit { input, count } => {
                let (sql, tail, groups) = self.render_operation(block, input, needed)?;
                (sql, format!("{tail} LIMIT {count}"), groups)
            }
            Expand { input, column } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, [*column]);
                let sql = self.materialize_operation(block, input, &required)?;
                let projection = required
                    .iter()
                    .map(|value| {
                        if value == column {
                            format!("arrayJoin(q.{0}) AS {0}", value_name(*value))
                        } else {
                            format!("q.{}", value_name(*value))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    format!("SELECT {projection} FROM ({sql}) AS q"),
                    String::new(),
                    vec![],
                )
            }
            Materialize { input, .. } => (
                self.materialize_operation(block, input, needed)?,
                String::new(),
                vec![],
            ),
            Latest { requirement, .. } => match *requirement {},
        })
    }

    fn materialize_operation(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        needed: &[ColumnRef<'catalog>],
    ) -> Result<String> {
        if operation.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        let (sql, tail, groups) = self.render_operation(block, operation, needed)?;
        if tail.is_empty() && groups.is_empty() {
            return Ok(sql);
        }
        let projection = if needed.is_empty() {
            "1 AS _unit".into()
        } else {
            needed
                .iter()
                .map(|column| format!("q.{}", value_name(*column)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let group = if groups.is_empty() {
            String::new()
        } else {
            format!(
                " GROUP BY {}",
                groups
                    .iter()
                    .map(|group| self.render_expression_at(group, true))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )
        };
        Ok(format!(
            "SELECT {projection} FROM ({sql}) AS q{group}{tail}"
        ))
    }

    fn render_expression_at(
        &self,
        expression: &Expression<'catalog>,
        materialized: bool,
    ) -> Result<String> {
        self.render_expression_with(expression, &|column| {
            if materialized {
                Ok(format!("q.{}", value_name(column)))
            } else {
                self.render_column(column)
            }
        })
    }

    fn render_expression_with(
        &self,
        expression: &Expression<'catalog>,
        column: &dyn Fn(ColumnRef<'catalog>) -> Result<String>,
    ) -> Result<String> {
        Ok(match expression {
            Expression::Column(reference) => column(*reference)?,
            Expression::Literal { data_type, value } => orbit_utils::query_types::ParamValue {
                data_type: *data_type,
                value: value.clone(),
            }
            .render_clickhouse_literal(),
            Expression::Integer(value) => value.to_string(),
            Expression::Boolean(value) => value.to_string(),
            Expression::Text(value) => {
                format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
            }
            Expression::Count => "COUNT(*)".into(),
            Expression::ScalarQuery(reference) => {
                let Source::Derived(body) = self.relation(reference.relation)?.source else {
                    return Err(GraphError::ExpressionType);
                };
                let Port::Output(output) = reference.port else {
                    return Err(GraphError::ExpressionType);
                };
                format!(
                    "(SELECT {} FROM ({}))",
                    output_name(output),
                    self.render_block(body, false)?
                )
            }
            Expression::PathDepth(value) => format!(
                "toInt64(countSubstrings({}, '/'))",
                self.render_expression_with(value, column)?
            ),
            Expression::Aggregate { function, value } => {
                let name = match function {
                    crate::input::AggFunction::Count => "count",
                    crate::input::AggFunction::Sum => "sum",
                    crate::input::AggFunction::Avg => "avg",
                    crate::input::AggFunction::Min => "min",
                    crate::input::AggFunction::Max => "max",
                    crate::input::AggFunction::Collect => "groupArray",
                };
                format!("{name}({})", self.render_expression_with(value, column)?)
            }
            Expression::InQuery { value, key } => {
                let Source::Definition(definition) = self.relation(key.relation)?.source else {
                    return Err(GraphError::ExpressionType);
                };
                let Port::Output(output) = key.port else {
                    return Err(GraphError::ExpressionType);
                };
                format!(
                    "{} IN (SELECT {} FROM {})",
                    self.render_expression_with(value, column)?,
                    output_name(output),
                    definition_name(definition)
                )
            }
            Expression::Add(left, right) => format!(
                "({} + {})",
                self.render_expression_with(left, column)?,
                self.render_expression_with(right, column)?
            ),
            Expression::Reverse(value) => format!(
                "arrayReverse({})",
                self.render_expression_with(value, column)?
            ),
            Expression::EmptyArray(element) => format!(
                "CAST([], '{}')",
                value_type_name(&ValueType::Array(Box::new(element.clone())))
            ),
            Expression::Strings(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| self
                        .render_expression_with(&Expression::Text(value.clone()), column))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ),
            Expression::JsonObject(fields) => {
                if fields.is_empty() {
                    "'{}'".into()
                } else {
                    let mut entries = Vec::new();
                    for (name, value) in fields {
                        entries.push(
                            self.render_expression_with(&Expression::Text(name.clone()), column)?,
                        );
                        entries.push(self.render_expression_with(value, column)?);
                    }
                    format!("toJSONString(map({}))", entries.join(", "))
                }
            }
            Expression::Prefixes {
                value,
                paths,
                array,
            } => {
                let value = self.render_expression_with(value, column)?;
                if *array {
                    format!(
                        "arrayExists(_gkg_path -> startsWith({value}, _gkg_path), {})",
                        self.render_expression_with(paths, column)?
                    )
                } else if let Expression::Strings(paths) = paths.as_ref() {
                    format!(
                        "({})",
                        paths
                            .iter()
                            .map(|path| Ok(format!(
                                "startsWith({value}, {})",
                                self.render_expression_with(
                                    &Expression::Text(path.clone()),
                                    column
                                )?
                            )))
                            .collect::<Result<Vec<_>>>()?
                            .join(" OR ")
                    )
                } else if let Expression::Array(paths) = paths.as_ref() {
                    format!(
                        "({})",
                        paths
                            .iter()
                            .map(|path| Ok(format!(
                                "startsWith({value}, {})",
                                self.render_expression_with(path, column)?
                            )))
                            .collect::<Result<Vec<_>>>()?
                            .join(" OR ")
                    )
                } else {
                    format!(
                        "arrayExists(_gkg_path -> startsWith({value}, _gkg_path), {})",
                        self.render_expression_with(paths, column)?
                    )
                }
            }
            Expression::HasAny(value, values) => {
                if let Expression::Array(values) = values.as_ref()
                    && let [element] = values.as_slice()
                {
                    format!(
                        "has({}, {})",
                        self.render_expression_with(value, column)?,
                        self.render_expression_with(element, column)?
                    )
                } else {
                    format!(
                        "hasAny({}, {})",
                        self.render_expression_with(value, column)?,
                        self.render_expression_with(values, column)?
                    )
                }
            }
            Expression::Predicate {
                operator,
                value,
                argument,
                fold_case,
            } => {
                use crate::input::FilterOp;
                let value = self.render_expression_with(value, column)?;
                let argument = argument
                    .as_ref()
                    .map(|argument| self.render_expression_with(argument, column))
                    .transpose()?;
                let comparison = match operator {
                    FilterOp::Eq => Some("="),
                    FilterOp::Ne => Some("!="),
                    FilterOp::Gt => Some(">"),
                    FilterOp::Lt => Some("<"),
                    FilterOp::Gte => Some(">="),
                    FilterOp::Lte => Some("<="),
                    FilterOp::In => Some("IN"),
                    _ => None,
                };
                if let Some(comparison) = comparison {
                    format!(
                        "({value} {comparison} {})",
                        argument.ok_or(GraphError::ExpressionType)?
                    )
                } else if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
                    format!(
                        "({value} IS {}NULL)",
                        if *operator == FilterOp::IsNotNull {
                            "NOT "
                        } else {
                            ""
                        }
                    )
                } else {
                    let fold = |value: String| {
                        if *fold_case {
                            format!("lower({value})")
                        } else {
                            value
                        }
                    };
                    let value = fold(value);
                    let argument = fold(argument.ok_or(GraphError::ExpressionType)?);
                    if *operator == FilterOp::Contains {
                        return Ok(format!("multiSearchAny({value}, [{argument}])"));
                    }
                    let function = match operator {
                        FilterOp::AllTokens => "hasAllTokens",
                        FilterOp::AnyTokens => "hasAnyTokens",
                        FilterOp::TokenMatch => "hasToken",
                        FilterOp::StartsWith => "startsWith",
                        FilterOp::EndsWith => "endsWith",
                        _ => return Err(GraphError::ExpressionType),
                    };
                    format!("{function}({value}, {argument})")
                }
            }
            Expression::Bucket { unit, value } => crate::passes::codegen::clickhouse::time_bucket(
                *unit,
                &self.render_expression_with(value, column)?,
            ),
            Expression::LatestPath {
                path,
                version,
                deletion,
            } => format!(
                "coalesce(if(argMaxOrNull({}, {}), NULL, argMaxOrNull({}, {})), '{}')",
                column(*deletion)?,
                column(*version)?,
                column(*path)?,
                column(*version)?,
                crate::scope::UNRESOLVED_PATH
            ),
            Expression::Parameter { name, data_type } => format!(
                "{{{name}:{}}}",
                orbit_utils::clickhouse::type_name(*data_type)
            ),
            Expression::ToString(value) => {
                format!("toString({})", self.render_expression_with(value, column)?)
            }
            Expression::Excerpt { value, max_chars } => {
                let value = self.render_expression_with(value, column)?;
                let shortened = format!("substringUTF8({value}, 1, {max_chars})");
                format!(
                    "concat({shortened}, if(length({value}) > length({shortened}), '{}', ''))",
                    crate::passes::response_policy::TEXT_TRUNCATION_SUFFIX
                )
            }
            Expression::CountIf(condition) => format!(
                "countIf({})",
                self.render_expression_with(condition, column)?
            ),
            Expression::Sum { value, condition } => match condition {
                Some(condition) => format!(
                    "sumIf({}, {})",
                    self.render_expression_with(value, column)?,
                    self.render_expression_with(condition, column)?
                ),
                None => format!("SUM({})", self.render_expression_with(value, column)?),
            },
            Expression::Tuple(values) | Expression::Array(values) | Expression::Concat(values) => {
                let function = match expression {
                    Expression::Tuple(_) => "tuple",
                    Expression::Array(_) => "array",
                    _ => "arrayConcat",
                };
                let values = values
                    .iter()
                    .map(|value| self.render_expression_with(value, column))
                    .collect::<Result<Vec<_>>>()?;
                format!("{function}({})", values.join(", "))
            }
            Expression::Field { tuple, index } => format!(
                "tupleElement({}, {})",
                self.render_expression_with(tuple, column)?,
                index + 1
            ),
            Expression::Keep { condition, value } => format!(
                "arrayFilter(_keep -> {}, [{}])",
                self.render_expression_with(condition, column)?,
                self.render_expression_with(value, column)?
            ),
            Expression::Integers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expression::StartsWith(left, right) => format!(
                "startsWith({}, {})",
                self.render_expression_with(left, column)?,
                self.render_expression_with(right, column)?
            ),
            Expression::Equal(left, right)
            | Expression::And(left, right)
            | Expression::Or(left, right)
            | Expression::In(left, right)
            | Expression::Greater(left, right)
            | Expression::GreaterEqual(left, right)
            | Expression::LessEqual(left, right) => {
                let operator = match expression {
                    Expression::Equal(..) => "=",
                    Expression::Greater(..) => ">",
                    Expression::GreaterEqual(..) => ">=",
                    Expression::LessEqual(..) => "<=",
                    Expression::In(..) => "IN",
                    Expression::Or(..) => "OR",
                    _ => "AND",
                };
                format!(
                    "({} {operator} {})",
                    self.render_expression_with(left, column)?,
                    self.render_expression_with(right, column)?
                )
            }
        })
    }
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn value_type_name(ty: &ValueType) -> String {
    match ty {
        ValueType::Scalar(ty) => orbit_utils::clickhouse::type_name(*ty),
        ValueType::Array(element) => format!("Array({})", value_type_name(element)),
        ValueType::Tuple(fields) => format!(
            "Tuple({})",
            fields
                .iter()
                .map(value_type_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn extend_columns<'a>(
    needed: &mut Vec<ColumnRef<'a>>,
    columns: impl IntoIterator<Item = ColumnRef<'a>>,
) {
    for column in columns {
        if !needed.contains(&column) {
            needed.push(column);
        }
    }
}

fn collect_columns<'a>(expression: &Expression<'a>, needed: &mut Vec<ColumnRef<'a>>) -> Result<()> {
    expression.columns(&mut |column| {
        extend_columns(needed, [column]);
        Ok(())
    })
}
fn value_name(column: ColumnRef<'_>) -> String {
    let port = match column.port {
        Port::Stored(column) => format!("stored_{}", column.ordinal()),
        Port::Output(output) => output_name(output),
    };
    quoted(&format!("{}_{}", relation_name(column.relation), port))
}
pub(super) fn relation_name(id: RelationId) -> String {
    format!("r{}_{}", id.block.slot, id.slot)
}
pub(super) fn output_name(id: OutputId) -> String {
    format!("o{}_{}", id.block.slot, id.slot)
}
pub(super) fn definition_name(id: DefinitionId) -> String {
    format!("d{}_{}", id.block.slot, id.slot)
}
