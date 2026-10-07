use super::*;

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, Infallible> {
    pub(super) fn render_expression_with(
        &self,
        expression: &Expression<'a>,
        column: &dyn Fn(ColumnRef<'a>) -> Result<String>,
    ) -> Result<String> {
        use Expression::*;
        let render = |value: &Expression<'a>| self.render_expression_with(value, column);
        Ok(match expression {
            Column(reference) => column(*reference)?,
            Literal { data_type, value } => orbit_utils::query_types::ParamValue {
                data_type: *data_type,
                value: value.clone(),
            }
            .render_clickhouse_literal(),
            Parameter { name, data_type } => format!(
                "{{{name}:{}}}",
                orbit_utils::clickhouse::type_name(*data_type)
            ),
            Integer(value) => value.to_string(),
            Boolean(value) => value.to_string(),
            Text(value) => text_literal(value),
            Count => "COUNT(*)".into(),
            ScalarQuery(reference) => {
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
            InQuery { value, key } => {
                let Source::Definition(definition) = self.relation(key.relation)?.source else {
                    return Err(GraphError::ExpressionType);
                };
                let Port::Output(output) = key.port else {
                    return Err(GraphError::ExpressionType);
                };
                format!(
                    "{} IN (SELECT {} FROM {})",
                    render(value)?,
                    output_name(output),
                    definition_name(definition)
                )
            }
            PathDepth(value) => format!("toInt64(countSubstrings({}, '/'))", render(value)?),
            Aggregate { function, value } => {
                let name = match function {
                    crate::input::AggFunction::Count => "count",
                    crate::input::AggFunction::Sum => "sum",
                    crate::input::AggFunction::Avg => "avg",
                    crate::input::AggFunction::Min => "min",
                    crate::input::AggFunction::Max => "max",
                    crate::input::AggFunction::Collect => "groupArray",
                };
                format!("{name}({})", render(value)?)
            }
            Reverse(value) => format!("arrayReverse({})", render(value)?),
            EmptyArray(element) => format!(
                "CAST([], '{}')",
                value_type_name(&ValueType::Array(Box::new(element.clone())))
            ),
            Strings(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| text_literal(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Integers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            JsonObject(fields) => {
                if fields.is_empty() {
                    "'{}'".into()
                } else {
                    let mut entries = Vec::new();
                    for (name, value) in fields {
                        entries.push(text_literal(name));
                        entries.push(render(value)?);
                    }
                    format!("toJSONString(map({}))", entries.join(", "))
                }
            }
            Prefixes {
                value,
                paths,
                array,
            } => {
                let value = render(value)?;
                let individual = if *array {
                    None
                } else {
                    match paths.as_ref() {
                        Strings(paths) => Some(
                            paths
                                .iter()
                                .map(|path| text_literal(path))
                                .collect::<Vec<_>>(),
                        ),
                        Array(paths) => Some(paths.iter().map(render).collect::<Result<Vec<_>>>()?),
                        _ => None,
                    }
                };
                if let Some(paths) = individual {
                    format!(
                        "({})",
                        paths
                            .iter()
                            .map(|path| format!("startsWith({value}, {path})"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    )
                } else {
                    format!(
                        "arrayExists(_gkg_path -> startsWith({value}, _gkg_path), {})",
                        render(paths)?
                    )
                }
            }
            HasAny(value, values) => {
                if let Array(values) = values.as_ref()
                    && let [element] = values.as_slice()
                {
                    format!("has({}, {})", render(value)?, render(element)?)
                } else {
                    format!("hasAny({}, {})", render(value)?, render(values)?)
                }
            }
            Predicate {
                operator,
                value,
                argument,
                fold_case,
            } => render_predicate(
                *operator,
                render(value)?,
                argument.as_ref().map(|value| render(value)).transpose()?,
                *fold_case,
            )?,
            Bucket { unit, value } => {
                crate::passes::codegen::clickhouse::time_bucket(*unit, &render(value)?)
            }
            LatestPath {
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
            ToString(value) => format!("toString({})", render(value)?),
            Excerpt { value, max_chars } => {
                let value = render(value)?;
                let shortened = format!("substringUTF8({value}, 1, {max_chars})");
                format!(
                    "concat({shortened}, if(length({value}) > length({shortened}), '{}', ''))",
                    crate::passes::response_policy::TEXT_TRUNCATION_SUFFIX
                )
            }
            CountIf(condition) => format!("countIf({})", render(condition)?),
            Sum { value, condition } => match condition {
                Some(condition) => format!("sumIf({}, {})", render(value)?, render(condition)?),
                None => format!("SUM({})", render(value)?),
            },
            Tuple(values) | Array(values) | Concat(values) => {
                let function = match expression {
                    Tuple(_) => "tuple",
                    Array(_) => "array",
                    _ => "arrayConcat",
                };
                format!(
                    "{function}({})",
                    values
                        .iter()
                        .map(render)
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                )
            }
            Field { tuple, index } => format!("tupleElement({}, {})", render(tuple)?, index + 1),
            Keep { condition, value } => format!(
                "arrayFilter(_keep -> {}, [{}])",
                render(condition)?,
                render(value)?
            ),
            StartsWith(left, right) => format!("startsWith({}, {})", render(left)?, render(right)?),
            Equal(left, right)
            | And(left, right)
            | Or(left, right)
            | In(left, right)
            | Greater(left, right)
            | GreaterEqual(left, right)
            | LessEqual(left, right)
            | Add(left, right) => {
                let operator = match expression {
                    Equal(..) => "=",
                    Greater(..) => ">",
                    GreaterEqual(..) => ">=",
                    LessEqual(..) => "<=",
                    In(..) => "IN",
                    Or(..) => "OR",
                    Add(..) => "+",
                    _ => "AND",
                };
                format!("({} {operator} {})", render(left)?, render(right)?)
            }
        })
    }
}

fn render_predicate(
    operator: crate::input::FilterOp,
    value: String,
    argument: Option<String>,
    fold_case: bool,
) -> Result<String> {
    use crate::input::FilterOp;
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
        return Ok(format!(
            "({value} {comparison} {})",
            argument.ok_or(GraphError::ExpressionType)?
        ));
    }
    if matches!(operator, FilterOp::IsNull | FilterOp::IsNotNull) {
        return Ok(format!(
            "({value} IS {}NULL)",
            if operator == FilterOp::IsNotNull {
                "NOT "
            } else {
                ""
            }
        ));
    }
    let fold = |value: String| {
        if fold_case {
            format!("lower({value})")
        } else {
            value
        }
    };
    let value = fold(value);
    let argument = fold(argument.ok_or(GraphError::ExpressionType)?);
    if operator == FilterOp::Contains {
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
    Ok(format!("{function}({value}, {argument})"))
}

fn text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
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
