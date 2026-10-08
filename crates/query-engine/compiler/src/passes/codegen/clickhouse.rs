use orbit_server_config::QueryConfig;
use orbit_utils::query_types::ParamBindings;
use serde_json::Value;
use std::collections::HashMap;

use super::{ParamValue, ParameterizedQuery, SqlDialect};
use crate::ast::{
    Cte, Expr, Function, Insert, JoinType, Node, Op, Query, SqlType, TableRef, TextMatch,
};
use crate::error::{QueryError, Result};
use crate::passes::enforce::ResultContext;
use crate::query_graph::{LoweredGraph, QueryId};

pub fn codegen(
    ast: &Node,
    result_context: ResultContext,
    query_config: QueryConfig,
) -> Result<ParameterizedQuery> {
    let (mut sql, params) = emit_simple_query(ast)?;
    if matches!(ast, Node::Query(_)) {
        append_settings(&mut sql, &query_config)?;
    }
    Ok(ParameterizedQuery {
        sql,
        params,
        result_context,
        query_config,
        dialect: SqlDialect::ClickHouse,
    })
}

pub fn codegen_graph<'a, M: query_data_model::QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    result_context: ResultContext,
    query_config: QueryConfig,
) -> Result<ParameterizedQuery> {
    let (mut sql, params) = graph.render(root)?;
    append_settings(&mut sql, &query_config)?;
    Ok(ParameterizedQuery {
        sql,
        params,
        result_context,
        query_config,
        dialect: SqlDialect::ClickHouse,
    })
}

fn append_settings(sql: &mut String, config: &QueryConfig) -> Result<()> {
    let mut settings = config
        .to_clickhouse_settings()
        .map_err(QueryError::Codegen)?;
    settings.extend(config.compiler_derived.to_clickhouse_settings());
    if !settings.is_empty() {
        sql.push_str(&format!(
            " SETTINGS {}",
            settings
                .iter()
                .map(|(name, value)| format!("{name} = {value}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

pub(crate) fn time_bucket(unit: crate::input::TruncateUnit, value: &str) -> String {
    use crate::input::TruncateUnit;
    let function = match unit {
        TruncateUnit::Minute => "toStartOfMinute",
        TruncateUnit::Hour => "toStartOfHour",
        TruncateUnit::Day => "toStartOfDay",
        TruncateUnit::Week => "toStartOfWeek",
        TruncateUnit::Month => "toStartOfMonth",
        TruncateUnit::Quarter => "toStartOfQuarter",
        TruncateUnit::Year => "toStartOfYear",
    };
    let bucket = format!("{function}({value})");
    match unit.result_type() {
        SqlType::Timestamp { .. } => format!("toDateTime64({bucket}, 0)"),
        _ => format!("toDate32({bucket})"),
    }
}

/// Accepts trusted internal SQL only; it does not apply request authorization.
pub fn emit_simple_query(node: &Node) -> Result<(String, HashMap<String, ParamValue>)> {
    let mut context = Context::default();
    let sql = match node {
        Node::Query(query) => context.emit_query(query)?,
        Node::Insert(insert) => context.emit_insert(insert)?,
    };
    Ok((sql, context.params.into_map()))
}

#[derive(Default)]
struct Context {
    params: ParamBindings,
}

impl Context {
    fn emit_insert(&mut self, insert: &Insert) -> Result<String> {
        let rows = insert
            .values()
            .iter()
            .map(|row| {
                Ok(format!(
                    "({})",
                    row.iter()
                        .map(|value| self.emit_expr(value))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(format!(
            "INSERT INTO {} ({}) VALUES {}",
            insert.table(),
            insert.columns().join(", "),
            rows.join(", ")
        ))
    }

    fn emit_query(&mut self, query: &Query) -> Result<String> {
        let mut parts = Vec::new();
        if !query.ctes.is_empty() {
            parts.push(self.emit_ctes(&query.ctes)?);
        }
        parts.push(self.emit_query_body(query)?);
        Ok(parts.join(" "))
    }

    fn emit_ctes(&mut self, definitions: &[Cte]) -> Result<String> {
        let keyword = if definitions.iter().any(|definition| definition.recursive) {
            "WITH RECURSIVE"
        } else {
            "WITH"
        };
        let definitions = definitions
            .iter()
            .map(|definition| {
                let materialized = if definition.materialized {
                    "MATERIALIZED "
                } else {
                    ""
                };
                Ok(format!(
                    "{} AS {materialized}({})",
                    definition.name,
                    self.emit_query(&definition.query)?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(format!("{keyword} {}", definitions.join(", ")))
    }

    fn emit_query_body(&mut self, query: &Query) -> Result<String> {
        let projection = query
            .select
            .iter()
            .map(|output| {
                let value = self.emit_expr(&output.expr)?;
                Ok(match &output.alias {
                    Some(label) => format!("{value} AS {label}"),
                    None => value,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut parts = vec![
            format!(
                "SELECT {}{}",
                if query.distinct { "DISTINCT " } else { "" },
                projection.join(", ")
            ),
            format!("FROM {}", self.emit_table_ref(&query.from)?),
        ];
        if let Some(predicate) = &query.where_clause {
            parts.push(format!("WHERE {}", self.emit_expr(predicate)?));
        }
        if !query.group_by.is_empty() {
            let groups = query
                .group_by
                .iter()
                .map(|value| self.emit_expr(value))
                .collect::<Result<Vec<_>>>()?;
            parts.push(format!("GROUP BY {}", groups.join(", ")));
        }
        if let Some(predicate) = &query.having {
            parts.push(format!("HAVING {}", self.emit_expr(predicate)?));
        }
        for arm in &query.union_all {
            let sql = self.emit_query(arm)?;
            parts.push(if arm.ctes.is_empty() {
                format!("UNION ALL {sql}")
            } else {
                format!("UNION ALL ({sql})")
            });
        }
        if !query.union_all.is_empty()
            && (query.limit.is_some() || query.limit_by.is_some() || !query.order_by.is_empty())
        {
            parts = vec![format!("SELECT * FROM ({})", parts.join(" "))];
        }
        if !query.order_by.is_empty() {
            let order = query
                .order_by
                .iter()
                .map(|key| {
                    Ok(format!(
                        "{} {}",
                        self.emit_expr(&key.expr)?,
                        if key.desc { "DESC" } else { "ASC" }
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            parts.push(format!("ORDER BY {}", order.join(", ")));
        }
        if let Some((count, keys)) = &query.limit_by {
            let keys = keys
                .iter()
                .map(|key| self.emit_expr(key))
                .collect::<Result<Vec<_>>>()?;
            parts.push(format!("LIMIT {count} BY {}", keys.join(", ")));
        }
        if let Some(limit) = query.limit {
            parts.push(format!("LIMIT {limit}"));
        }
        Ok(parts.join(" "))
    }

    fn emit_expr(&mut self, expression: &Expr) -> Result<String> {
        Ok(match expression {
            Expr::Column { table, column } => format!("{table}.{column}"),
            Expr::Identifier(name) => name.clone(),
            Expr::EmptyTupleArray(fields) => format!(
                "CAST([], 'Array(Tuple({}))')",
                fields
                    .iter()
                    .map(|field| orbit_utils::clickhouse::type_name(*field))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expr::Literal(value) => self.emit_literal(value),
            Expr::Param { data_type, value } => self.emit_param(*data_type, value),
            Expr::FuncCall { name, args } => {
                if !name.accepts_arity(args.len()) {
                    return Err(QueryError::Codegen(format!(
                        "{name} does not accept {} arguments",
                        args.len()
                    )));
                }
                let args = args
                    .iter()
                    .map(|argument| self.emit_expr(argument))
                    .collect::<Result<Vec<_>>>()?;
                format!("{}({})", function_name(*name), args.join(", "))
            }
            Expr::TimeBucket { unit, value } => time_bucket(*unit, &self.emit_expr(value)?),
            Expr::TextSearch { mode, value, query } => {
                let (value, query) = (self.emit_expr(value)?, self.emit_expr(query)?);
                match mode {
                    TextMatch::Contains => format!("multiSearchAny({value}, [{query}])"),
                    TextMatch::TokenMatch => format!("hasToken({value}, {query})"),
                    TextMatch::AllTokens => format!("hasAllTokens({value}, {query})"),
                    TextMatch::AnyTokens => format!("hasAnyTokens({value}, {query})"),
                }
            }
            Expr::Aggregate {
                function,
                argument,
                distinct,
                condition,
            } => {
                super::validate_aggregate(*function, argument.as_deref(), *distinct)
                    .map_err(QueryError::Codegen)?;
                let base = match function {
                    crate::input::AggFunction::Count => "count",
                    crate::input::AggFunction::Sum => "sum",
                    crate::input::AggFunction::Avg => "avg",
                    crate::input::AggFunction::Min => "min",
                    crate::input::AggFunction::Max => "max",
                    crate::input::AggFunction::Collect => "groupArray",
                };
                let name = if *distinct || condition.is_some() {
                    format!(
                        "{base}{}{}",
                        if *distinct { "Distinct" } else { "" },
                        if condition.is_some() { "If" } else { "" }
                    )
                } else if base == "groupArray" {
                    base.into()
                } else {
                    base.to_uppercase()
                };
                let mut args = argument
                    .iter()
                    .map(|value| self.emit_expr(value))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(condition) = condition {
                    args.push(self.emit_expr(condition)?);
                }
                format!("{name}({})", args.join(", "))
            }
            Expr::Lambda { param, body } => format!("{param} -> {}", self.emit_expr(body)?),
            Expr::BinaryOp { op, left, right } => {
                let (left, right) = (self.emit_expr(left)?, self.emit_expr(right)?);
                if *op == Op::In {
                    format!("{left} IN {right}")
                } else {
                    format!("({left} {op} {right})")
                }
            }
            Expr::UnaryOp { op, expr } => {
                let value = self.emit_expr(expr)?;
                if matches!(op, Op::IsNull | Op::IsNotNull) {
                    format!("({value} {op})")
                } else {
                    format!("({op} {value})")
                }
            }
            Expr::InSubquery {
                expr,
                cte_name,
                column,
            } => format!(
                "{} IN (SELECT {column} FROM {cte_name})",
                self.emit_expr(expr)?
            ),
            Expr::InSelect { expr, query } => {
                format!("{} IN ({})", self.emit_expr(expr)?, self.emit_query(query)?)
            }
            Expr::Scalar(query) => format!("({})", self.emit_query(query)?),
            Expr::Star => "*".into(),
        })
    }

    fn emit_param(&mut self, data_type: SqlType, value: &Value) -> String {
        if value.is_null() {
            return "NULL".into();
        }
        if let Value::Array(values) = value
            && !matches!(data_type, SqlType::Array(_))
        {
            return format!(
                "({})",
                values
                    .iter()
                    .map(|value| self.emit_param(data_type, value))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let name = self.params.intern(data_type, value);
        format!(
            "{{{name}:{}}}",
            orbit_utils::clickhouse::type_name(data_type)
        )
    }

    fn emit_literal(&mut self, value: &Value) -> String {
        if let Value::Array(values) = value {
            format!(
                "({})",
                values
                    .iter()
                    .map(|value| self.emit_param(SqlType::from_value(value), value))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            self.emit_param(SqlType::from_value(value), value)
        }
    }

    fn emit_table_ref(&mut self, source: &TableRef) -> Result<String> {
        Ok(match source {
            TableRef::Scan {
                table,
                alias,
                final_,
                ..
            } => format!("{table} AS {alias}{}", if *final_ { " FINAL" } else { "" }),
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let (left, right) = (self.emit_table_ref(left)?, self.emit_table_ref(right)?);
                if *join_type == JoinType::Cross {
                    format!("{left} INNER JOIN {right} ON 1")
                } else {
                    format!("{left} {join_type} JOIN {right} ON {}", self.emit_expr(on)?)
                }
            }
            TableRef::Union { queries, alias } => {
                let arms = queries
                    .iter()
                    .map(|query| self.emit_query(query))
                    .collect::<Result<Vec<_>>>()?;
                format!("({}) AS {alias}", arms.join(" UNION ALL "))
            }
            TableRef::Subquery { query, alias } => {
                format!("({}) AS {alias}", self.emit_query(query)?)
            }
        })
    }
}

pub(crate) fn function_name(function: Function) -> &'static str {
    match function {
        Function::StartsWith => "startsWith",
        Function::EndsWith => "endsWith",
        Function::Lower => "lower",
        Function::ToString => "toString",
        Function::ToJson => "toJSONString",
        Function::Object => "map",
        Function::If => "if",
        Function::Coalesce => "coalesce",
        Function::ByteLength => "length",
        Function::Substring => "substringUTF8",
        Function::Concat => "concat",
        Function::CountSubstrings => "countSubstrings",
        Function::Array => "array",
        Function::Tuple => "tuple",
        Function::ArrayConcat => "arrayConcat",
        Function::ArrayReverse => "arrayReverse",
        Function::ArrayResize => "arrayResize",
        Function::ArrayContains => "has",
        Function::ArrayContainsAny => "hasAny",
        Function::ArrayContainsAll => "hasAll",
        Function::ArrayFilter => "arrayFilter",
        Function::ArrayMap => "arrayMap",
        Function::ArrayExists => "arrayExists",
        Function::Unnest => "arrayJoin",
        Function::TupleElement => "tupleElement",
        Function::ArgMax => "argMax",
        Function::ArgMaxOrNull => "argMaxOrNull",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{OrderExpr, SelectExpr};

    fn emit(query: Query) -> ParameterizedQuery {
        codegen(
            &Node::Query(Box::new(query)),
            ResultContext::new(),
            QueryConfig::default(),
        )
        .unwrap()
    }

    #[test]
    fn select_binds_values_and_keeps_public_labels() {
        let result = emit(Query {
            select: vec![
                SelectExpr::new(Expr::col("n", "id"), "node_id"),
                SelectExpr::new(Expr::col("n", "label"), "node_type"),
            ],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::eq(Expr::col("n", "label"), Expr::lit("User"))),
            limit: Some(10),
            ..Default::default()
        });
        assert_eq!(
            result.sql,
            "SELECT n.id AS node_id, n.label AS node_type FROM nodes AS n WHERE (n.label = {p0:String}) LIMIT 10"
        );
        assert_eq!(result.params["p0"].value, "User");
    }

    #[test]
    fn joins_and_grouped_having_keep_their_inputs() {
        let count = Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("n", "id")));
        let result = emit(Query {
            select: vec![
                SelectExpr::new(Expr::col("n", "label"), "type"),
                SelectExpr::new(count.clone(), "count"),
            ],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("nodes", "n"),
                TableRef::scan("edges", "e"),
                Expr::eq(Expr::col("n", "id"), Expr::col("e", "source_id")),
            ),
            group_by: vec![Expr::col("n", "label")],
            having: Some(Expr::binary(Op::Gt, count.clone(), Expr::lit(5))),
            order_by: vec![OrderExpr::desc(count)],
            ..Default::default()
        });
        assert_eq!(
            result.sql,
            "SELECT n.label AS type, COUNT(n.id) AS count FROM nodes AS n INNER JOIN edges AS e ON (n.id = e.source_id) GROUP BY n.label HAVING (COUNT(n.id) > {p0:Int64}) ORDER BY COUNT(n.id) DESC"
        );
    }

    #[test]
    fn null_boolean_and_membership_expressions_render() {
        let result = emit(Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan("nodes", "n"),
            where_clause: Expr::and_all([
                Some(Expr::binary(
                    Op::In,
                    Expr::col("n", "label"),
                    Expr::lit(serde_json::json!(["User", "Project"])),
                )),
                Expr::or_all([
                    Some(Expr::binary(
                        Op::Gt,
                        Expr::col("n", "created_at"),
                        Expr::lit("2024-01-01"),
                    )),
                    Some(Expr::unary(Op::IsNull, Expr::col("n", "deleted_at"))),
                ]),
            ]),
            ..Default::default()
        });
        assert!(result.sql.contains("n.label IN ({p0:String}, {p1:String})"));
        assert!(
            result
                .sql
                .contains("((n.created_at > {p2:String}) OR (n.deleted_at IS NULL))")
        );
    }

    #[test]
    fn parameters_are_interned_by_value_and_type() {
        let mut context = Context::default();
        assert_eq!(context.emit_literal(&Value::from("dup")), "{p0:String}");
        assert_eq!(context.emit_literal(&Value::from("dup")), "{p0:String}");
        assert_eq!(context.emit_literal(&Value::from(42)), "{p1:Int64}");
        assert_eq!(context.emit_literal(&Value::from(true)), "{p2:Bool}");
        assert_eq!(context.emit_literal(&Value::Null), "NULL");
        let array = serde_json::json!(["1/2/", "1/3/"]);
        let ty = SqlType::Array(crate::ast::ScalarType::String);
        assert_eq!(context.emit_param(ty, &array), "{p3:Array(String)}");
        assert_eq!(context.emit_param(ty, &array), "{p3:Array(String)}");
        assert_eq!(
            context.emit_param(
                SqlType::Timestamp {
                    precision: 6,
                    timezone: Some(crate::ast::TimeZone::Utc)
                },
                &Value::from("dup")
            ),
            "{p4:DateTime64(6, 'UTC')}"
        );
        assert_eq!(context.params.into_map().len(), 5);
    }

    #[test]
    fn subquery_join_and_scalar_aggregate_render() {
        let inner = Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "source_id")],
            from: TableRef::scan("gl_edge", "e"),
            group_by: vec![Expr::col("e", "source_id")],
            having: Some(Expr::eq(
                Expr::func(
                    Function::ArgMax,
                    vec![Expr::col("e", "_deleted"), Expr::col("e", "_version")],
                ),
                Expr::lit(false),
            )),
            ..Default::default()
        };
        let result = emit(Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::subquery(inner, "deduped"),
                Expr::eq(Expr::col("u", "id"), Expr::col("deduped", "source_id")),
            ),
            ..Default::default()
        });
        assert!(result.sql.contains("INNER JOIN (SELECT"));
        assert!(result.sql.contains("HAVING"));
        assert!(result.sql.contains(") AS deduped ON"));
        let result = emit(Query {
            select: vec![SelectExpr::new(
                Expr::aggregate(crate::input::AggFunction::Count, None),
                "total",
            )],
            from: TableRef::scan("nodes", "n"),
            having: Some(Expr::lit(true)),
            ..Default::default()
        });
        assert!(result.sql.contains("HAVING") && !result.sql.contains("GROUP BY"));
    }

    #[test]
    fn union_limit_applies_to_all_arms() {
        let result = emit(Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::scan("gl_user", "u"),
            union_all: vec![Query {
                select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
                from: TableRef::scan("gl_project", "p"),
                ..Default::default()
            }],
            limit: Some(10),
            ..Default::default()
        });
        assert_eq!(
            result.sql,
            "SELECT * FROM (SELECT u.id AS id FROM gl_user AS u UNION ALL SELECT p.id AS id FROM gl_project AS p) LIMIT 10"
        );
    }

    #[test]
    fn recursive_ctes_and_derived_unions_render() {
        let arm = Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        };
        let result = emit(Query {
            ctes: vec![Cte {
                name: "paths".into(),
                query: Box::new(Query {
                    union_all: vec![arm.clone()],
                    ..arm.clone()
                }),
                recursive: true,
                materialized: false,
            }],
            select: vec![SelectExpr::new(Expr::col("all", "id"), "id")],
            from: TableRef::Union {
                queries: vec![arm.clone(), arm],
                alias: "all".into(),
            },
            ..Default::default()
        });
        assert!(result.sql.contains("WITH RECURSIVE"));
        assert!(result.sql.contains("UNION ALL"));
        assert!(result.sql.contains(") AS all"));
    }

    #[test]
    fn insert_binds_values_and_omits_query_settings() {
        let insert = Insert::new(
            "gl_schema_versions",
            vec!["key".into(), "version".into()],
            vec![
                vec![Expr::string("graph"), Expr::int(3)],
                vec![Expr::string("datalake"), Expr::int(1)],
            ],
        );
        let result = codegen(
            &Node::Insert(Box::new(insert)),
            ResultContext::new(),
            QueryConfig {
                use_query_cache: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            result.sql,
            "INSERT INTO gl_schema_versions (key, version) VALUES ({p0:String}, {p1:Int64}), ({p2:String}, {p3:Int64})"
        );
        assert_eq!(result.params["p0"].value, "graph");
        assert_eq!(result.params["p1"].value, 3);
    }

    #[test]
    fn trusted_current_read_keeps_parameters() {
        let query = Query {
            select: vec![
                SelectExpr::new(Expr::col("t", "key"), "key"),
                SelectExpr::new(Expr::col("t", "version"), "version"),
            ],
            from: TableRef::scan_final("gl_schema_versions", "t"),
            where_clause: Some(Expr::eq(Expr::col("t", "key"), Expr::string("graph"))),
            ..Default::default()
        };
        let (sql, params) = emit_simple_query(&Node::Query(Box::new(query))).unwrap();
        assert_eq!(
            sql,
            "SELECT t.key AS key, t.version AS version FROM gl_schema_versions AS t FINAL WHERE (t.key = {p0:String})"
        );
        assert_eq!(params["p0"].value, "graph");
    }

    #[test]
    fn settings_and_result_contract_survive_codegen() {
        let query = Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan_final("nodes", "n"),
            limit: Some(100),
            ..Default::default()
        };
        assert!(!emit(query.clone()).sql.contains("SETTINGS"));
        let mut context = ResultContext::new();
        context.add_node("u", "User");
        let mut config = QueryConfig {
            use_query_cache: Some(true),
            query_cache_ttl: Some(60),
            ..Default::default()
        };
        config.compiler_derived.optimize_move_to_prewhere_if_final = true;
        config
            .compiler_derived
            .use_index_for_in_with_subqueries_max_values = Some(100_000);
        let result = codegen(&Node::Query(Box::new(query)), context, config).unwrap();
        for expected in [
            "LIMIT 100 SETTINGS",
            "use_query_cache = 1",
            "query_cache_ttl = 60",
            "optimize_move_to_prewhere_if_final = 1",
            "use_index_for_in_with_subqueries_max_values = 100000",
        ] {
            assert!(result.sql.contains(expected), "{}", result.sql);
        }
        assert_eq!(result.result_context.get("u").unwrap().entity_type, "User");
    }

    #[test]
    fn rendered_parameters_preserve_unknown_placeholders() {
        let query = ParameterizedQuery {
            sql: "SELECT {p0:String}, {p1:Array(Int64)}, {unknown:String}".into(),
            params: HashMap::from([
                (
                    "p0".into(),
                    ParamValue {
                        data_type: SqlType::String,
                        value: Value::from("User"),
                    },
                ),
                (
                    "p1".into(),
                    ParamValue {
                        data_type: SqlType::Array(crate::ast::ScalarType::Int64),
                        value: serde_json::json!([10, 20]),
                    },
                ),
            ]),
            result_context: ResultContext::new(),
            query_config: QueryConfig::default(),
            dialect: SqlDialect::ClickHouse,
        };
        assert_eq!(query.render(), "SELECT 'User', [10, 20], {unknown:String}");
    }
}
