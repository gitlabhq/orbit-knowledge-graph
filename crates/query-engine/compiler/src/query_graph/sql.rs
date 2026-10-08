use super::*;
use orbit_utils::query_types::{ParamBindings, ParamValue};
use std::collections::HashMap;

struct Renderer<'graph, 'catalog, M: QueryDataModel + ?Sized> {
    graph: &'graph QueryGraph<'catalog, M>,
    parameters: ParamBindings,
}

impl<M: QueryDataModel + ?Sized> LoweredGraph<'_, M> {
    pub fn render(&self, query: QueryId) -> Result<(String, HashMap<String, ParamValue>)> {
        let mut renderer = Renderer {
            graph: &self.0,
            parameters: ParamBindings::default(),
        };
        let sql = renderer.query(query, true)?;
        Ok((sql, renderer.parameters.into_map()))
    }
}

impl<'a, M: QueryDataModel + ?Sized> Renderer<'_, 'a, M> {
    fn query(&mut self, id: QueryId, public: bool) -> Result<String> {
        let query = self.graph.get(id)?;
        if public && query.parent.is_some() {
            return Err(Error::Ownership);
        }
        let rows = self.graph.rows(id)?;
        let definitions = query
            .definitions
            .iter()
            .map(|(_, definition)| {
                Ok(format!(
                    "cte_{} AS ({})",
                    definition.query.slot,
                    self.query(definition.query, false)?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let prefix = if definitions.is_empty() {
            String::new()
        } else {
            format!("WITH {} ", definitions.join(", "))
        };
        let sql = self.rows(rows)?;
        if !public {
            return Ok(format!("{prefix}{sql}"));
        }
        let outputs = rows
            .columns
            .iter()
            .map(|column| format!("q.{} AS {}", name(column), quoted(column.name())))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("{prefix}SELECT {outputs} FROM ({sql}) AS q"))
    }

    fn rows(&mut self, rows: &Rows<'a>) -> Result<String> {
        use OperationKind::*;
        let select = selection(&rows.columns, "q");
        Ok(match rows.kind() {
            Unit => "SELECT 1 AS _unit".into(),
            Scan { table, read, .. } => {
                let projection = rows
                    .columns
                    .iter()
                    .map(|column| format!("s.{} AS {}", quoted(column.name()), name(column)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "SELECT {projection} FROM {} AS s{}",
                    quoted(table.name()),
                    if *read == super::Read::Current {
                        " FINAL"
                    } else {
                        ""
                    }
                )
            }
            Read { query, .. } => {
                let original = &self.graph.rows(*query)?.columns;
                let values = original
                    .iter()
                    .zip(&rows.columns)
                    .map(|(old, new)| format!("q.{} AS {}", name(old), name(new)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let is_cte = self.graph.queries.iter().any(|owner| {
                    owner
                        .definitions
                        .iter()
                        .any(|(_, definition)| definition.query == *query)
                });
                let source = if is_cte {
                    format!("cte_{}", query.slot)
                } else {
                    format!("({})", self.query(*query, false)?)
                };
                format!("SELECT {values} FROM {source} AS q")
            }
            Filter { input, predicate } => {
                let source = self.rows(input)?;
                let predicate =
                    self.expression(predicate, &|column| format!("q.{}", name(column)))?;
                format!("SELECT {select} FROM ({source}) AS q WHERE {predicate}")
            }
            Select { input, values } => {
                let source = self.rows(input)?;
                let projection = self.projection(values, &rows.columns)?;
                format!("SELECT {projection} FROM ({source}) AS q")
            }
            Aggregate {
                input,
                groups,
                measures,
            } => {
                let source = self.rows(input)?;
                let mut values = self.projection(groups, &rows.columns[..groups.len()])?;
                let measures = self.projection(measures, &rows.columns[groups.len()..])?;
                if !values.is_empty() && !measures.is_empty() {
                    values.push_str(", ");
                }
                values.push_str(&measures);
                let groups = groups
                    .iter()
                    .map(|group| {
                        self.expression(&group.value, &|column| format!("q.{}", name(column)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group = if groups.is_empty() {
                    String::new()
                } else {
                    format!(" GROUP BY {}", groups.join(", "))
                };
                format!("SELECT {values} FROM ({source}) AS q{group}")
            }
            Expand { input, value } => {
                let source = self.rows(input)?;
                let value =
                    self.expression(&value.value, &|column| format!("q.{}", name(column)))?;
                let output = rows.columns.last().ok_or(Error::Column)?;
                let input = selection(&input.columns, "q");
                format!(
                    "SELECT {input}, arrayJoin({value}) AS {} FROM ({source}) AS q",
                    name(output)
                )
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left_sql = self.rows(left)?;
                let right_sql = self.rows(right)?;
                let side = |column: &Column| {
                    if left.columns.contains(column) {
                        format!("l.{}", name(column))
                    } else {
                        format!("r.{}", name(column))
                    }
                };
                let projection = rows.columns.iter().map(side).collect::<Vec<_>>().join(", ");
                if *kind == super::Join::Membership {
                    let ExprKind::Binary {
                        operator: Operator::Equal,
                        left: value,
                        right: key,
                    } = condition.kind()
                    else {
                        return Err(Error::Type);
                    };
                    let value = self.expression(value, &side)?;
                    let key = self.expression(key, &side)?;
                    format!(
                        "SELECT {projection} FROM ({left_sql}) AS l WHERE {value} IN (SELECT {key} FROM ({right_sql}) AS r)"
                    )
                } else {
                    let keyword = match kind {
                        super::Join::Inner => "INNER JOIN",
                        super::Join::Semi => "LEFT SEMI JOIN",
                        super::Join::Cross => "CROSS JOIN",
                        _ => unreachable!(),
                    };
                    let condition = if *kind == super::Join::Cross {
                        String::new()
                    } else {
                        format!(" ON {}", self.expression(condition, &side)?)
                    };
                    format!(
                        "SELECT {projection} FROM ({left_sql}) AS l {keyword} ({right_sql}) AS r{condition}"
                    )
                }
            }
            Sort { input, keys } => {
                let source = self.rows(input)?;
                let order = self.order(keys)?;
                format!("SELECT {select} FROM ({source}) AS q ORDER BY {order}")
            }
            Limit { input, count } => {
                if let Sort { input, keys } = input.kind() {
                    let source = self.rows(input)?;
                    let order = self.order(keys)?;
                    format!("SELECT {select} FROM ({source}) AS q ORDER BY {order} LIMIT {count}")
                } else {
                    format!(
                        "SELECT {select} FROM ({}) AS q LIMIT {count}",
                        self.rows(input)?
                    )
                }
            }
            FirstBy { input, keys } => {
                let OperationKind::Sort { input, keys: order } = input.kind() else {
                    return Err(Error::Latest);
                };
                let source = self.rows(input)?;
                let order = self.order(order)?;
                let keys = keys
                    .iter()
                    .map(|column| format!("q.{}", name(column)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SELECT {select} FROM ({source}) AS q ORDER BY {order} LIMIT 1 BY {keys}")
            }
            Latest { .. } => return Err(Error::Latest),
            Union { arms } => arms
                .iter()
                .map(|arm| {
                    let columns = &self.graph.rows(*arm)?.columns;
                    let projection = columns
                        .iter()
                        .zip(&rows.columns)
                        .map(|(old, new)| format!("q.{} AS {}", name(old), name(new)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(format!(
                        "(SELECT {projection} FROM ({}) AS q)",
                        self.query(*arm, false)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" UNION ALL "),
        })
    }

    fn projection(&mut self, values: &[Named], columns: &[Column]) -> Result<String> {
        values
            .iter()
            .zip(columns)
            .map(|(value, column)| {
                Ok(format!(
                    "{} AS {}",
                    self.expression(&value.value, &|column| format!("q.{}", name(column)))?,
                    name(column)
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.join(", "))
    }

    fn order(&mut self, keys: &[Order]) -> Result<String> {
        keys.iter()
            .map(|key| {
                Ok(format!(
                    "{} {}",
                    self.expression(&key.value, &|column| format!("q.{}", name(column)))?,
                    if key.descending { "DESC" } else { "ASC" }
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(|keys| keys.join(", "))
    }

    fn expression(
        &mut self,
        expression: &Expr,
        column: &dyn Fn(&Column) -> String,
    ) -> Result<String> {
        Ok(match expression.kind() {
            ExprKind::Column(value) => column(value),
            ExprKind::Literal(value) => {
                if value.value.is_null() {
                    return Ok(format!(
                        "CAST(NULL AS Nullable({}))",
                        orbit_utils::clickhouse::type_name(value.data_type)
                    ));
                }
                let parameter = self.parameters.intern(value.data_type, &value.value);
                format!(
                    "{{{parameter}:{}}}",
                    orbit_utils::clickhouse::type_name(value.data_type)
                )
            }
            ExprKind::Scalar { query, column } => format!(
                "(SELECT {} FROM ({}))",
                name(column),
                self.query(*query, false)?
            ),
            ExprKind::Binary {
                operator,
                left,
                right,
            } => {
                let operator = match operator {
                    Operator::Equal => "=",
                    Operator::NotEqual => "!=",
                    Operator::Greater => ">",
                    Operator::GreaterEqual => ">=",
                    Operator::Less => "<",
                    Operator::LessEqual => "<=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    Operator::Add => "+",
                    Operator::In => "IN",
                };
                format!(
                    "({} {operator} {})",
                    self.expression(left, column)?,
                    self.expression(right, column)?
                )
            }
            ExprKind::Aggregate {
                function,
                arguments,
                filter,
            } => {
                let function = match function {
                    Aggregate::Count => "count",
                    Aggregate::Sum => "sum",
                    Aggregate::Average => "avg",
                    Aggregate::Min => "min",
                    Aggregate::Max => "max",
                    Aggregate::Collect => "groupArray",
                    Aggregate::ArgMax => "argMaxOrNull",
                };
                let mut arguments = arguments
                    .iter()
                    .map(|value| self.expression(value, column))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(filter) = filter {
                    arguments.push(self.expression(filter, column)?);
                }
                format!(
                    "{function}{}({})",
                    if filter.is_some() { "If" } else { "" },
                    arguments.join(", ")
                )
            }
            ExprKind::Call {
                function,
                arguments,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| self.expression(argument, column))
                    .collect::<Result<Vec<_>>>()?;
                match function {
                    Function::EmptyArray(element) => format!(
                        "CAST([], '{}')",
                        type_name(&ValueType::Array(Box::new(element.clone())))
                    ),
                    Function::TupleField(index) => {
                        format!("tupleElement({}, {})", arguments[0], index + 1)
                    }
                    Function::SingletonIf => {
                        format!("arrayFilter(_keep -> {}, [{}])", arguments[0], arguments[1])
                    }
                    Function::Contains => {
                        format!("multiSearchAny({}, [{}])", arguments[0], arguments[1])
                    }
                    Function::StartsWithAny => {
                        format!(
                            "arrayExists(prefix -> startsWith({}, prefix), {})",
                            arguments[0], arguments[1]
                        )
                    }
                    Function::IsNull => format!("({} IS NULL)", arguments[0]),
                    Function::IsNotNull => format!("({} IS NOT NULL)", arguments[0]),
                    Function::PathDepth => {
                        format!("toInt64(countSubstrings({}, '/'))", arguments[0])
                    }
                    Function::TimeBucket(unit) => {
                        crate::passes::codegen::clickhouse::time_bucket(*unit, &arguments[0])
                    }
                    Function::Excerpt(count) => {
                        let value = &arguments[0];
                        let short = format!("substringUTF8({value}, 1, {count})");
                        format!(
                            "concat({short}, if(length({value}) > length({short}), '{}', ''))",
                            crate::passes::response_policy::TEXT_TRUNCATION_SUFFIX
                        )
                    }
                    Function::JsonObject(names) => {
                        let values = names
                            .iter()
                            .zip(arguments)
                            .flat_map(|(name, value)| {
                                [
                                    format!("'{}'", name.replace('\\', "\\\\").replace('\'', "''")),
                                    value,
                                ]
                            })
                            .collect::<Vec<_>>();
                        if values.is_empty() {
                            "'{}'".into()
                        } else {
                            format!("toJSONString(map({}))", values.join(", "))
                        }
                    }
                    function => {
                        let name = match function {
                            Function::StartsWith => "startsWith",
                            Function::EndsWith => "endsWith",
                            Function::HasAny => "hasAny",
                            Function::Lower => "lower",
                            Function::ToString => "toString",
                            Function::Tuple => "tuple",
                            Function::Array => "array",
                            Function::ArrayConcat => "arrayConcat",
                            Function::ArrayReverse => "arrayReverse",
                            Function::Coalesce => "coalesce",
                            Function::If => "if",
                            Function::TokenMatch => "hasToken",
                            Function::AllTokens => "hasAllTokens",
                            Function::AnyTokens => "hasAnyTokens",
                            _ => unreachable!(),
                        };
                        format!("{name}({})", arguments.join(", "))
                    }
                }
            }
        })
    }
}

fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn name(column: &Column) -> String {
    format!("c{}_{}", column.0.source, column.0.slot)
}
fn selection(columns: &[Column], alias: &str) -> String {
    if columns.is_empty() {
        "1 AS _unit".into()
    } else {
        columns
            .iter()
            .map(|column| format!("{alias}.{}", name(column)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
fn type_name(value: &ValueType) -> String {
    match value {
        ValueType::Scalar(scalar) => orbit_utils::clickhouse::type_name(*scalar),
        ValueType::Array(element) => format!("Array({})", type_name(element)),
        ValueType::Tuple(fields) => format!(
            "Tuple({})",
            fields.iter().map(type_name).collect::<Vec<_>>().join(", ")
        ),
    }
}
