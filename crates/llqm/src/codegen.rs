//! ClickHouse SQL code generation from Substrait plans.
//!
//! Walks the Substrait relation tree bottom-up, computing schemas at each node
//! and collecting SQL clauses (SELECT, FROM, WHERE, ORDER BY, LIMIT).

use std::collections::HashMap;

use serde_json::Value;
use substrait::proto::{
    self, Expression, FunctionArgument, ReadRel, Rel, RelRoot,
    expression::{
        self, FieldReference, Literal, ScalarFunction, field_reference, literal::LiteralType,
        reference_segment,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument, join_rel, plan_rel, read_rel, rel, set_rel, sort_field, r#type,
};
use thiserror::Error;

use crate::expr::DataType;
use crate::plan::{Plan, Schema, SchemaColumn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A parameterized ClickHouse SQL query with extracted parameter values.
#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, ParamValue>,
}

/// A query parameter with its ClickHouse type and JSON value.
#[derive(Debug, Clone)]
pub struct ParamValue {
    pub ch_type: String,
    pub value: Value,
}

#[derive(Debug, Error)]
pub enum CodegenError {
    #[error("unsupported relation type: {0}")]
    UnsupportedRelation(String),
    #[error("unsupported expression type: {0}")]
    UnsupportedExpression(String),
    #[error("missing required field: {0}")]
    MissingField(String),
    #[error("schema error: {0}")]
    SchemaError(String),
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate a parameterized ClickHouse SQL query from a plan.
///
/// Literals become auto-numbered parameters (`{p0:String}`, `{p1:Int64}`).
/// Named parameters (`Expr::Param`) become `{name:Type}` placeholders.
pub fn emit_clickhouse_sql(plan: &Plan) -> Result<ParameterizedQuery, CodegenError> {
    let root = get_root(&plan.inner)?;
    let output_names = &root.names;
    let rel = root
        .input
        .as_ref()
        .ok_or_else(|| CodegenError::MissingField("RelRoot.input".into()))?;

    let mut ctx = CodegenContext::from_plan(&plan.inner);

    // Emit CTEs first (each CTE may add params and use functions from its own plan)
    let cte_sql = if !plan.ctes.is_empty() {
        let has_recursive = plan.ctes.iter().any(|c| c.recursive);
        let keyword = if has_recursive {
            "WITH RECURSIVE"
        } else {
            "WITH"
        };

        let cte_parts: Vec<String> = plan
            .ctes
            .iter()
            .map(|cte| {
                let cte_root = get_root(&cte.plan.inner)?;
                let cte_output_names = &cte_root.names;
                let cte_rel = cte_root
                    .input
                    .as_ref()
                    .ok_or_else(|| CodegenError::MissingField("CTE RelRoot.input".into()))?;

                // Build a context from the CTE's plan for function resolution
                let mut cte_ctx = CodegenContext::from_plan(&cte.plan.inner);
                let (cte_parts, _) = cte_ctx.emit_query(cte_rel, Some(cte_output_names))?;

                // Merge params from CTE into the main context
                ctx.params.extend(cte_ctx.params);

                Ok(format!("{} AS ({})", cte.name, cte_parts.to_sql()))
            })
            .collect::<Result<Vec<_>, CodegenError>>()?;

        Some(format!("{keyword} {}", cte_parts.join(", ")))
    } else {
        None
    };

    let (mut parts, _schema) = ctx.emit_query(rel, Some(output_names))?;

    if let Some(cte_prefix) = cte_sql {
        parts.ctes.insert(0, cte_prefix);
    }

    Ok(ParameterizedQuery {
        sql: parts.to_sql(),
        params: ctx.params,
    })
}

// ---------------------------------------------------------------------------
// Internal: CodegenContext
// ---------------------------------------------------------------------------

struct CodegenContext {
    params: HashMap<String, ParamValue>,
    param_counter: usize,
    function_names: HashMap<u32, String>,
}

impl CodegenContext {
    fn from_plan(plan: &proto::Plan) -> Self {
        let function_names: HashMap<u32, String> = plan
            .extensions
            .iter()
            .filter_map(|ext| match &ext.mapping_type {
                Some(MappingType::ExtensionFunction(f)) => {
                    Some((f.function_anchor, f.name.clone()))
                }
                _ => None,
            })
            .collect();

        Self {
            params: HashMap::new(),
            param_counter: 0,
            function_names,
        }
    }

    fn next_param_name(&mut self) -> String {
        let name = format!("p{}", self.param_counter);
        self.param_counter += 1;
        name
    }
}

// ---------------------------------------------------------------------------
// Internal: SQL clause collector
// ---------------------------------------------------------------------------

#[derive(Default)]
struct QueryParts {
    ctes: Vec<String>,
    select: Vec<String>,
    from: String,
    where_clauses: Vec<String>,
    group_by: Vec<String>,
    having: Option<String>,
    order_by: Vec<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    union_all: Vec<String>,
}

impl QueryParts {
    fn to_sql(&self) -> String {
        let mut sql = String::new();

        // WITH clause (CTEs)
        if !self.ctes.is_empty() {
            sql.push_str(&self.ctes.join(" "));
            sql.push(' ');
        }

        let select = if self.select.is_empty() {
            "*".to_string()
        } else {
            self.select.join(", ")
        };

        sql.push_str(&format!("SELECT {select} FROM {}", self.from));

        if !self.where_clauses.is_empty() {
            let combined = if self.where_clauses.len() == 1 {
                self.where_clauses[0].clone()
            } else {
                self.where_clauses
                    .iter()
                    .map(|w| format!("({w})"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            sql.push_str(&format!(" WHERE {combined}"));
        }

        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }

        if let Some(having) = &self.having {
            sql.push_str(&format!(" HAVING {having}"));
        }

        // UNION ALL
        for union_sql in &self.union_all {
            sql.push_str(&format!(" UNION ALL {union_sql}"));
        }

        if !self.order_by.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", self.order_by.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql
    }
}

// ---------------------------------------------------------------------------
// Internal: Relation handlers
// ---------------------------------------------------------------------------

impl CodegenContext {
    /// Walk the relation tree, returning SQL parts and the output schema.
    fn emit_query(
        &mut self,
        rel: &Rel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        match &rel.rel_type {
            Some(rel::RelType::Read(read)) => self.emit_read(read),
            Some(rel::RelType::Filter(filter)) => self.emit_filter(filter, output_names),
            Some(rel::RelType::Project(project)) => self.emit_project(project, output_names),
            Some(rel::RelType::Join(join)) => self.emit_join(join, output_names),
            Some(rel::RelType::Sort(sort)) => self.emit_sort(sort, output_names),
            Some(rel::RelType::Fetch(fetch)) => self.emit_fetch(fetch, output_names),
            Some(rel::RelType::Aggregate(agg)) => self.emit_aggregate(agg, output_names),
            Some(rel::RelType::Set(set)) => self.emit_set(set, output_names),
            Some(rel::RelType::ExtensionSingle(ext)) => {
                self.emit_extension_single(ext, output_names)
            }
            _ => Err(CodegenError::UnsupportedRelation(
                "unknown or missing relation type".into(),
            )),
        }
    }

    fn emit_read(&mut self, read: &proto::ReadRel) -> Result<(QueryParts, Schema), CodegenError> {
        let base_schema = read
            .base_schema
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("ReadRel.base_schema".into()))?;

        let table_name = match &read.read_type {
            Some(read_rel::ReadType::NamedTable(nt)) => {
                nt.names.first().cloned().unwrap_or_default()
            }
            _ => {
                return Err(CodegenError::UnsupportedRelation(
                    "non-NamedTable read type".into(),
                ));
            }
        };

        // Check for raw FROM clause or alias in metadata
        let metadata = get_read_metadata(read);
        let raw_from = metadata
            .as_ref()
            .and_then(|v| v.get("raw_from"))
            .and_then(|v| v.as_str());
        let alias_str = metadata
            .as_ref()
            .and_then(|v| v.get("alias"))
            .and_then(|v| v.as_str());

        if let Some(raw_from) = raw_from {
            // Raw FROM — emit as-is, schema columns are unqualified
            let schema = schema_from_base(base_schema, "");
            return Ok((
                QueryParts {
                    from: raw_from.to_string(),
                    ..Default::default()
                },
                schema,
            ));
        }

        let alias = alias_str
            .map(String::from)
            .unwrap_or_else(|| table_name.clone());
        let schema = schema_from_base(base_schema, &alias);

        let from = if alias == table_name {
            table_name
        } else {
            format!("{table_name} AS {alias}")
        };

        Ok((
            QueryParts {
                from,
                ..Default::default()
            },
            schema,
        ))
    }

    fn emit_filter(
        &mut self,
        filter: &proto::FilterRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let input = filter
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("FilterRel.input".into()))?;

        // Detect HAVING: FilterRel on top of AggregateRel
        let is_having = matches!(&input.rel_type, Some(rel::RelType::Aggregate(_)));

        let (mut parts, schema) = self.emit_query(input, output_names)?;

        let condition = filter
            .condition
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("FilterRel.condition".into()))?;
        let condition_sql = self.emit_expr(condition, &schema)?;

        if is_having {
            parts.having = Some(condition_sql);
        } else {
            parts.where_clauses.push(condition_sql);
        }

        Ok((parts, schema))
    }

    fn emit_project(
        &mut self,
        project: &proto::ProjectRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let input = project
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("ProjectRel.input".into()))?;
        // The project's child doesn't get output_names — only the project uses them.
        let (mut parts, input_schema) = self.emit_query(input, None)?;

        let names = output_names.cloned().unwrap_or_default();
        let mut select_items = Vec::new();
        let mut output_columns = Vec::new();

        for (i, expr) in project.expressions.iter().enumerate() {
            let expr_sql = self.emit_expr(expr, &input_schema)?;
            let alias = names.get(i).cloned().unwrap_or_else(|| format!("_col{i}"));

            // Skip redundant alias for positional field references
            // (e.g., `table.id` with alias `id`), but always emit alias
            // for raw SQL or function calls where the name matters.
            let is_field_ref = matches!(&expr.rex_type, Some(expression::RexType::Selection(_)));
            if is_field_ref && (expr_sql == alias || expr_sql.ends_with(&format!(".{alias}"))) {
                select_items.push(expr_sql.clone());
            } else {
                select_items.push(format!("{expr_sql} AS {alias}"));
            }

            let data_type = infer_expr_type(expr, &input_schema);
            let table_alias = infer_expr_table(expr, &input_schema);
            output_columns.push(SchemaColumn {
                table_alias,
                name: alias,
                data_type,
            });
        }

        parts.select = select_items;
        let output_schema = Schema {
            columns: output_columns,
        };

        Ok((parts, output_schema))
    }

    fn emit_join(
        &mut self,
        join: &proto::JoinRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let left_rel = join
            .left
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("JoinRel.left".into()))?;
        let right_rel = join
            .right
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("JoinRel.right".into()))?;

        // Both sides are walked without output_names (they're FROM-clause components).
        let (left_parts, left_schema) = self.emit_from_item(left_rel)?;
        let (right_parts, right_schema) = self.emit_from_item(right_rel)?;
        let merged = Schema::merge(&left_schema, &right_schema);

        let join_type_str = match join_rel::JoinType::try_from(join.r#type) {
            Ok(join_rel::JoinType::Inner) => "INNER JOIN",
            Ok(join_rel::JoinType::Left) => "LEFT JOIN",
            Ok(join_rel::JoinType::Right) => "RIGHT JOIN",
            Ok(join_rel::JoinType::Outer) => "FULL JOIN",
            _ => "JOIN",
        };

        let on_sql = match &join.expression {
            Some(expr) => self.emit_expr(expr, &merged)?,
            None => "TRUE".into(),
        };

        let from = format!(
            "{} {join_type_str} {} ON {}",
            left_parts.from, right_parts.from, on_sql
        );

        // Merge where clauses from both sides (from subqueries or nested filters).
        let mut parts = QueryParts {
            from,
            ..Default::default()
        };
        parts.where_clauses.extend(left_parts.where_clauses);
        parts.where_clauses.extend(right_parts.where_clauses);

        // If the join is the top-level relation, pass output_names up.
        // (Typically joins are wrapped in a Project.)
        let _ = output_names;

        Ok((parts, merged))
    }

    fn emit_sort(
        &mut self,
        sort: &proto::SortRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let input = sort
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("SortRel.input".into()))?;
        let (mut parts, schema) = self.emit_query(input, output_names)?;

        for sf in &sort.sorts {
            let expr = sf
                .expr
                .as_ref()
                .ok_or_else(|| CodegenError::MissingField("SortField.expr".into()))?;
            let expr_sql = self.emit_expr(expr, &schema)?;

            let dir = match &sf.sort_kind {
                Some(sort_field::SortKind::Direction(d)) => {
                    match sort_field::SortDirection::try_from(*d) {
                        Ok(sort_field::SortDirection::DescNullsFirst)
                        | Ok(sort_field::SortDirection::DescNullsLast) => "DESC",
                        _ => "ASC",
                    }
                }
                _ => "ASC",
            };

            parts.order_by.push(format!("{expr_sql} {dir}"));
        }

        Ok((parts, schema))
    }

    #[allow(deprecated)] // FetchRel count/offset oneofs are deprecated but simpler
    fn emit_fetch(
        &mut self,
        fetch: &proto::FetchRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        use substrait::proto::fetch_rel::{CountMode, OffsetMode};

        let input = fetch
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("FetchRel.input".into()))?;
        let (mut parts, schema) = self.emit_query(input, output_names)?;

        if let Some(CountMode::Count(c)) = &fetch.count_mode
            && *c >= 0
        {
            parts.limit = Some(*c);
        }
        if let Some(OffsetMode::Offset(o)) = &fetch.offset_mode
            && *o >= 0
        {
            parts.offset = Some(*o);
        }

        Ok((parts, schema))
    }

    fn emit_aggregate(
        &mut self,
        agg: &proto::AggregateRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let input = agg
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("AggregateRel.input".into()))?;
        let (mut parts, input_schema) = self.emit_query(input, None)?;

        let names = output_names.cloned().unwrap_or_default();
        let mut select_items = Vec::new();
        let mut output_columns = Vec::new();
        let mut group_by_items = Vec::new();
        let mut col_idx = 0;

        // Grouping expressions
        for grouping_expr in &agg.grouping_expressions {
            let expr_sql = self.emit_expr(grouping_expr, &input_schema)?;
            let alias = names
                .get(col_idx)
                .cloned()
                .unwrap_or_else(|| format!("_col{col_idx}"));

            // Check if we can skip the alias
            let is_field_ref =
                matches!(&grouping_expr.rex_type, Some(expression::RexType::Selection(_)));
            if is_field_ref && (expr_sql == alias || expr_sql.ends_with(&format!(".{alias}"))) {
                select_items.push(expr_sql.clone());
            } else {
                select_items.push(format!("{expr_sql} AS {alias}"));
            }
            group_by_items.push(expr_sql);

            let data_type = infer_expr_type(grouping_expr, &input_schema);
            let table_alias = infer_expr_table(grouping_expr, &input_schema);
            output_columns.push(SchemaColumn {
                table_alias,
                name: alias,
                data_type,
            });
            col_idx += 1;
        }

        // Aggregate measures
        for measure in &agg.measures {
            let alias = names
                .get(col_idx)
                .cloned()
                .unwrap_or_else(|| format!("_col{col_idx}"));

            let measure_sql = if let Some(agg_func) = &measure.measure {
                let func_name = self
                    .function_names
                    .get(&agg_func.function_reference)
                    .cloned()
                    .unwrap_or_else(|| format!("unknown_fn_{}", agg_func.function_reference));
                let args: Vec<String> = agg_func
                    .arguments
                    .iter()
                    .map(|a| self.emit_func_arg(a, &input_schema))
                    .collect::<Result<_, _>>()?;
                format!("{func_name}({})", args.join(", "))
            } else {
                "NULL".into()
            };

            select_items.push(format!("{measure_sql} AS {alias}"));
            output_columns.push(SchemaColumn {
                table_alias: String::new(),
                name: alias,
                data_type: DataType::String,
            });
            col_idx += 1;
        }

        parts.select = select_items;
        parts.group_by = group_by_items;

        let output_schema = Schema {
            columns: output_columns,
        };

        Ok((parts, output_schema))
    }

    fn emit_set(
        &mut self,
        set: &proto::SetRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        if set.inputs.is_empty() {
            return Err(CodegenError::MissingField("SetRel.inputs".into()));
        }

        let op = set_rel::SetOp::try_from(set.op)
            .unwrap_or(set_rel::SetOp::UnionAll);

        if op != set_rel::SetOp::UnionAll {
            return Err(CodegenError::UnsupportedRelation(format!(
                "SetRel op {:?} not supported, only UNION ALL",
                op
            )));
        }

        // Check if this SetRel has an alias (used as derived table in FROM)
        let metadata = set.advanced_extension.as_ref().and_then(|ext| {
            ext.optimization.first().and_then(|any| {
                if any.type_url == "llqm/set_metadata" {
                    serde_json::from_slice::<serde_json::Value>(&any.value).ok()
                } else {
                    None
                }
            })
        });
        let alias = metadata
            .as_ref()
            .and_then(|v| v.get("alias"))
            .and_then(|v| v.as_str());

        // Extract stored column names from metadata (if any)
        let stored_names: Option<Vec<String>> = metadata
            .as_ref()
            .and_then(|v| v.get("column_names"))
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect());

        // Use stored names for union arm output, falling back to passed output_names
        let arm_names = stored_names.as_ref().or(output_names);

        // Generate each input as a full SELECT statement
        let first_input = &set.inputs[0];
        let (first_parts, first_schema) = self.emit_query(first_input, arm_names)?;
        let mut union_sql = first_parts.to_sql();

        for input in &set.inputs[1..] {
            let (input_parts, _) = self.emit_query(input, arm_names)?;
            union_sql.push_str(&format!(" UNION ALL {}", input_parts.to_sql()));
        }

        // If aliased, wrap as derived table
        if let Some(alias) = alias {
            let schema = Schema {
                columns: first_schema
                    .columns
                    .iter()
                    .map(|c| SchemaColumn {
                        table_alias: alias.into(),
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    })
                    .collect(),
            };
            Ok((
                QueryParts {
                    from: format!("({union_sql}) AS {alias}"),
                    ..Default::default()
                },
                schema,
            ))
        } else {
            // Return the UNION ALL as a complete SQL with proper parts structure
            Ok((
                QueryParts {
                    from: format!("({union_sql})"),
                    ..Default::default()
                },
                first_schema,
            ))
        }
    }

    fn emit_extension_single(
        &mut self,
        ext: &proto::ExtensionSingleRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Schema), CodegenError> {
        let detail = ext.detail.as_ref();
        let is_subquery = detail
            .map(|d| d.type_url == "llqm/subquery_metadata")
            .unwrap_or(false);

        if !is_subquery {
            return Err(CodegenError::UnsupportedRelation(
                "unsupported ExtensionSingleRel type".into(),
            ));
        }

        let inner = ext
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("ExtensionSingleRel.input".into()))?;

        let metadata: serde_json::Value = detail
            .and_then(|d| serde_json::from_slice(&d.value).ok())
            .unwrap_or_default();
        let alias = metadata
            .get("subquery_alias")
            .and_then(|v| v.as_str())
            .unwrap_or("_sub");

        let (inner_parts, inner_schema) = self.emit_query(inner, output_names)?;
        let inner_sql = inner_parts.to_sql();

        let schema = Schema {
            columns: inner_schema
                .columns
                .iter()
                .map(|c| SchemaColumn {
                    table_alias: alias.into(),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
        };

        Ok((
            QueryParts {
                from: format!("({inner_sql}) AS {alias}"),
                ..Default::default()
            },
            schema,
        ))
    }

    /// Emit a relation as a FROM-clause item. If it's a simple Read, Join,
    /// Set (UNION ALL), or ExtensionSingle (subquery), inline it. Otherwise,
    /// wrap it in a subquery.
    fn emit_from_item(&mut self, rel: &Rel) -> Result<(QueryParts, Schema), CodegenError> {
        match &rel.rel_type {
            Some(rel::RelType::Read(_))
            | Some(rel::RelType::Join(_))
            | Some(rel::RelType::Set(_))
            | Some(rel::RelType::ExtensionSingle(_)) => self.emit_query(rel, None),
            _ => {
                // Subquery: generate full SQL and wrap
                let (inner_parts, schema) = self.emit_query(rel, None)?;
                let sql = inner_parts.to_sql();
                Ok((
                    QueryParts {
                        from: format!("({sql}) AS _sub"),
                        ..Default::default()
                    },
                    schema,
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: Expression handlers
// ---------------------------------------------------------------------------

impl CodegenContext {
    fn emit_expr(&mut self, expr: &Expression, schema: &Schema) -> Result<String, CodegenError> {
        match &expr.rex_type {
            Some(expression::RexType::Literal(lit)) => self.emit_literal(lit),
            Some(expression::RexType::Selection(field_ref)) => emit_field_ref(field_ref, schema),
            Some(expression::RexType::ScalarFunction(func)) => {
                self.emit_scalar_function(func, schema)
            }
            Some(expression::RexType::IfThen(if_then)) => self.emit_if_then(if_then, schema),
            Some(expression::RexType::Cast(cast_expr)) => self.emit_cast(cast_expr, schema),
            Some(expression::RexType::SingularOrList(sol)) => {
                self.emit_singular_or_list(sol, schema)
            }
            _ => Err(CodegenError::UnsupportedExpression(
                "unknown expression type".into(),
            )),
        }
    }

    fn emit_literal(&mut self, lit: &Literal) -> Result<String, CodegenError> {
        let (ch_type, value) = match &lit.literal_type {
            Some(LiteralType::String(s)) => ("String", Value::String(s.clone())),
            Some(LiteralType::I64(n)) => ("Int64", Value::Number((*n).into())),
            Some(LiteralType::Fp64(f)) => {
                let n = serde_json::Number::from_f64(*f)
                    .ok_or_else(|| CodegenError::UnsupportedExpression("NaN/Inf float".into()))?;
                ("Float64", Value::Number(n))
            }
            Some(LiteralType::Boolean(b)) => ("Bool", Value::Bool(*b)),
            Some(LiteralType::Null(_)) => return Ok("NULL".into()),
            _ => {
                return Err(CodegenError::UnsupportedExpression(
                    "unsupported literal type".into(),
                ));
            }
        };

        let name = self.next_param_name();
        let placeholder = format!("{{{name}:{ch_type}}}");
        self.params.insert(
            name,
            ParamValue {
                ch_type: ch_type.into(),
                value,
            },
        );
        Ok(placeholder)
    }

    fn emit_scalar_function(
        &mut self,
        func: &ScalarFunction,
        schema: &Schema,
    ) -> Result<String, CodegenError> {
        let name = self
            .function_names
            .get(&func.function_reference)
            .cloned()
            .unwrap_or_else(|| format!("unknown_fn_{}", func.function_reference));

        // Named parameter
        if name == "__param" {
            return self.emit_named_param(func);
        }

        // Raw SQL escape hatch
        if name == "__raw_sql" {
            return extract_string_arg(&func.arguments[0]);
        }

        // Binary operator
        if let Some(op_sql) = binary_op_sql(&name) {
            let left = self.emit_func_arg(&func.arguments[0], schema)?;
            let right = self.emit_func_arg(&func.arguments[1], schema)?;
            if name == "in" {
                return Ok(format!("{left} IN {right}"));
            }
            return Ok(format!("({left} {op_sql} {right})"));
        }

        // Unary operator
        if let Some(result) = self.try_emit_unary(&name, &func.arguments, schema)? {
            return Ok(result);
        }

        // Regular function call
        let args: Vec<String> = func
            .arguments
            .iter()
            .map(|a| self.emit_func_arg(a, schema))
            .collect::<Result<_, _>>()?;
        Ok(format!("{name}({})", args.join(", ")))
    }

    fn emit_named_param(&self, func: &ScalarFunction) -> Result<String, CodegenError> {
        let name = extract_string_arg(&func.arguments[0])?;
        let ch_type = extract_string_arg(&func.arguments[1])?;
        Ok(format!("{{{name}:{ch_type}}}"))
    }

    fn try_emit_unary(
        &mut self,
        name: &str,
        args: &[FunctionArgument],
        schema: &Schema,
    ) -> Result<Option<String>, CodegenError> {
        let result = match name {
            "not" => {
                let inner = self.emit_func_arg(&args[0], schema)?;
                Some(format!("(NOT {inner})"))
            }
            "is_null" => {
                let inner = self.emit_func_arg(&args[0], schema)?;
                Some(format!("({inner} IS NULL)"))
            }
            "is_not_null" => {
                let inner = self.emit_func_arg(&args[0], schema)?;
                Some(format!("({inner} IS NOT NULL)"))
            }
            _ => None,
        };
        Ok(result)
    }

    fn emit_if_then(
        &mut self,
        if_then: &expression::IfThen,
        schema: &Schema,
    ) -> Result<String, CodegenError> {
        let mut sql = "CASE".to_string();
        for clause in &if_then.ifs {
            let cond = clause
                .r#if
                .as_ref()
                .ok_or_else(|| CodegenError::MissingField("IfClause.if".into()))?;
            let then = clause
                .then
                .as_ref()
                .ok_or_else(|| CodegenError::MissingField("IfClause.then".into()))?;
            let cond_sql = self.emit_expr(cond, schema)?;
            let then_sql = self.emit_expr(then, schema)?;
            sql.push_str(&format!(" WHEN {cond_sql} THEN {then_sql}"));
        }
        if let Some(else_expr) = &if_then.r#else {
            let else_sql = self.emit_expr(else_expr, schema)?;
            sql.push_str(&format!(" ELSE {else_sql}"));
        }
        sql.push_str(" END");
        Ok(sql)
    }

    fn emit_cast(
        &mut self,
        cast_expr: &expression::Cast,
        schema: &Schema,
    ) -> Result<String, CodegenError> {
        let inner = cast_expr
            .input
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("Cast.input".into()))?;
        let inner_sql = self.emit_expr(inner, schema)?;
        let target_type = cast_expr
            .r#type
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("Cast.type".into()))?;
        let type_str = substrait_type_to_data_type(target_type)
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "String".into());
        Ok(format!("CAST({inner_sql} AS {type_str})"))
    }

    fn emit_singular_or_list(
        &mut self,
        sol: &expression::SingularOrList,
        schema: &Schema,
    ) -> Result<String, CodegenError> {
        let value = sol
            .value
            .as_ref()
            .ok_or_else(|| CodegenError::MissingField("SingularOrList.value".into()))?;
        let value_sql = self.emit_expr(value, schema)?;
        let options: Vec<String> = sol
            .options
            .iter()
            .map(|o| self.emit_expr(o, schema))
            .collect::<Result<_, _>>()?;
        Ok(format!("{value_sql} IN ({})", options.join(", ")))
    }

    fn emit_func_arg(
        &mut self,
        arg: &FunctionArgument,
        schema: &Schema,
    ) -> Result<String, CodegenError> {
        match &arg.arg_type {
            Some(function_argument::ArgType::Value(expr)) => self.emit_expr(expr, schema),
            _ => Err(CodegenError::UnsupportedExpression(
                "non-value function argument".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: Helpers
// ---------------------------------------------------------------------------

fn get_root(plan: &proto::Plan) -> Result<&RelRoot, CodegenError> {
    let plan_rel = plan
        .relations
        .first()
        .ok_or_else(|| CodegenError::MissingField("Plan.relations".into()))?;
    match &plan_rel.rel_type {
        Some(plan_rel::RelType::Root(root)) => Ok(root),
        _ => Err(CodegenError::MissingField("PlanRel.root".into())),
    }
}

fn get_read_metadata(read: &ReadRel) -> Option<serde_json::Value> {
    let ext = read.advanced_extension.as_ref()?;
    let any = ext.optimization.first()?;
    if any.type_url != "llqm/read_metadata" {
        return None;
    }
    serde_json::from_slice(&any.value).ok()
}

fn emit_field_ref(field_ref: &FieldReference, schema: &Schema) -> Result<String, CodegenError> {
    let index = get_field_index(field_ref)?;
    let col = schema.columns.get(index).ok_or_else(|| {
        CodegenError::SchemaError(format!(
            "field index {index} out of range (schema has {} columns)",
            schema.columns.len()
        ))
    })?;

    if col.table_alias.is_empty() {
        Ok(col.name.clone())
    } else {
        Ok(format!("{}.{}", col.table_alias, col.name))
    }
}

fn get_field_index(field_ref: &FieldReference) -> Result<usize, CodegenError> {
    match &field_ref.reference_type {
        Some(field_reference::ReferenceType::DirectReference(seg)) => match &seg.reference_type {
            Some(reference_segment::ReferenceType::StructField(sf)) => Ok(sf.field as usize),
            _ => Err(CodegenError::UnsupportedExpression(
                "non-struct field reference".into(),
            )),
        },
        _ => Err(CodegenError::UnsupportedExpression(
            "non-direct field reference".into(),
        )),
    }
}

fn extract_string_arg(arg: &FunctionArgument) -> Result<String, CodegenError> {
    match &arg.arg_type {
        Some(function_argument::ArgType::Value(Expression {
            rex_type:
                Some(expression::RexType::Literal(Literal {
                    literal_type: Some(LiteralType::String(s)),
                    ..
                })),
        })) => Ok(s.clone()),
        _ => Err(CodegenError::UnsupportedExpression(
            "expected string literal argument".into(),
        )),
    }
}

fn schema_from_base(base: &proto::NamedStruct, alias: &str) -> Schema {
    let types = base
        .r#struct
        .as_ref()
        .map(|s| &s.types)
        .cloned()
        .unwrap_or_default();

    Schema {
        columns: base
            .names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let data_type = types
                    .get(i)
                    .and_then(substrait_type_to_data_type)
                    .unwrap_or(DataType::String);
                SchemaColumn {
                    table_alias: alias.into(),
                    name: name.clone(),
                    data_type,
                }
            })
            .collect(),
    }
}

fn substrait_type_to_data_type(t: &proto::Type) -> Option<DataType> {
    match &t.kind {
        Some(r#type::Kind::String(_)) => Some(DataType::String),
        Some(r#type::Kind::I64(_)) => Some(DataType::Int64),
        Some(r#type::Kind::Fp64(_)) => Some(DataType::Float64),
        Some(r#type::Kind::Bool(_)) => Some(DataType::Bool),
        Some(r#type::Kind::List(list)) => list
            .r#type
            .as_ref()
            .and_then(|inner| substrait_type_to_data_type(inner))
            .map(|inner_dt| DataType::Array(Box::new(inner_dt))),
        #[allow(deprecated)]
        Some(r#type::Kind::Timestamp(_)) => Some(DataType::DateTime),
        _ => None,
    }
}

fn binary_op_sql(name: &str) -> Option<&'static str> {
    match name {
        "equal" => Some("="),
        "not_equal" => Some("!="),
        "lt" => Some("<"),
        "lte" => Some("<="),
        "gt" => Some(">"),
        "gte" => Some(">="),
        "and" => Some("AND"),
        "or" => Some("OR"),
        "add" => Some("+"),
        "like" => Some("LIKE"),
        "ilike" => Some("ILIKE"),
        "in" => Some("IN"),
        _ => None,
    }
}

fn resolve_field<'a>(expr: &Expression, schema: &'a Schema) -> Option<&'a SchemaColumn> {
    match &expr.rex_type {
        Some(expression::RexType::Selection(field_ref)) => get_field_index(field_ref)
            .ok()
            .and_then(|i| schema.columns.get(i)),
        _ => None,
    }
}

fn infer_expr_type(expr: &Expression, schema: &Schema) -> DataType {
    if let Some(col) = resolve_field(expr, schema) {
        return col.data_type.clone();
    }
    match &expr.rex_type {
        Some(expression::RexType::Literal(lit)) => match &lit.literal_type {
            Some(LiteralType::String(_)) => DataType::String,
            Some(LiteralType::I64(_)) => DataType::Int64,
            Some(LiteralType::Fp64(_)) => DataType::Float64,
            Some(LiteralType::Boolean(_)) => DataType::Bool,
            _ => DataType::String,
        },
        _ => DataType::String,
    }
}

fn infer_expr_table(expr: &Expression, schema: &Schema) -> String {
    resolve_field(expr, schema)
        .map(|c| c.table_alias.clone())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::*;
    use crate::plan::PlanBuilder;

    /// Build a plan from a closure and emit ClickHouse SQL.
    fn build_and_emit(
        f: impl FnOnce(&mut PlanBuilder) -> crate::plan::TypedRel,
    ) -> ParameterizedQuery {
        let mut b = PlanBuilder::new();
        let root = f(&mut b);
        let plan = b.build(root);
        emit_clickhouse_sql(&plan).unwrap()
    }

    #[test]
    fn simple_select_from_where_order_limit() {
        let pq = build_and_emit(|b| {
            b.read(
                "siphon_user",
                "t",
                &[
                    ("id", DataType::Int64),
                    ("name", DataType::String),
                    ("_siphon_replicated_at", DataType::String),
                    ("_siphon_deleted", DataType::Bool),
                ],
            )
            .filter(
                b,
                and([
                    gt(
                        col("t", "_siphon_replicated_at"),
                        param("last_watermark", DataType::String),
                    ),
                    le(
                        col("t", "_siphon_replicated_at"),
                        param("watermark", DataType::String),
                    ),
                ]),
            )
            .sort(b, &[(col("t", "id"), SortDir::Asc)])
            .project(
                b,
                &[
                    (col("t", "id"), "id"),
                    (col("t", "name"), "name"),
                    (col("t", "_siphon_replicated_at"), "_version"),
                    (col("t", "_siphon_deleted"), "_deleted"),
                ],
            )
            .fetch(b, 1000, None)
        });

        assert_eq!(
            pq.sql,
            "SELECT t.id, t.name, \
             t._siphon_replicated_at AS _version, \
             t._siphon_deleted AS _deleted \
             FROM siphon_user AS t \
             WHERE ((t._siphon_replicated_at > {last_watermark:String}) \
             AND (t._siphon_replicated_at <= {watermark:String})) \
             ORDER BY t.id ASC \
             LIMIT 1000"
        );
        assert!(pq.params.is_empty());
    }

    #[test]
    fn join_with_literals() {
        let pq = build_and_emit(|b| {
            let users = b.read(
                "gl_user",
                "u",
                &[("id", DataType::Int64), ("username", DataType::String)],
            );
            let edges = b.read(
                "gl_edge",
                "e0",
                &[
                    ("source_id", DataType::Int64),
                    ("target_id", DataType::Int64),
                    ("relationship_kind", DataType::String),
                ],
            );

            users
                .join(
                    b,
                    JoinType::Inner,
                    edges,
                    and([
                        eq(col("u", "id"), col("e0", "source_id")),
                        eq(col("e0", "relationship_kind"), string("AUTHORED")),
                    ]),
                )
                .project(
                    b,
                    &[
                        (col("u", "username"), "username"),
                        (col("e0", "target_id"), "target_id"),
                    ],
                )
                .fetch(b, 25, None)
        });

        assert_eq!(
            pq.sql,
            "SELECT u.username, e0.target_id \
             FROM gl_user AS u INNER JOIN gl_edge AS e0 \
             ON ((u.id = e0.source_id) AND (e0.relationship_kind = {p0:String})) \
             LIMIT 25"
        );
        assert_eq!(pq.params.len(), 1);
        assert_eq!(pq.params["p0"].value, Value::String("AUTHORED".into()));
        assert_eq!(pq.params["p0"].ch_type, "String");
    }

    #[test]
    fn three_way_join() {
        let pq = build_and_emit(|b| {
            let u = b.read(
                "gl_user",
                "u",
                &[
                    ("id", DataType::Int64),
                    ("username", DataType::String),
                    ("traversal_path", DataType::String),
                ],
            );
            let e = b.read(
                "gl_edge",
                "e0",
                &[
                    ("source_id", DataType::Int64),
                    ("target_id", DataType::Int64),
                    ("relationship_kind", DataType::String),
                    ("traversal_path", DataType::String),
                ],
            );
            let n = b.read(
                "gl_note",
                "n",
                &[
                    ("id", DataType::Int64),
                    ("confidential", DataType::Bool),
                    ("traversal_path", DataType::String),
                    ("created_at", DataType::String),
                ],
            );

            u.join(
                b,
                JoinType::Inner,
                e,
                and([
                    starts_with(col("e0", "traversal_path"), col("u", "traversal_path")),
                    eq(col("u", "id"), col("e0", "source_id")),
                    eq(col("e0", "relationship_kind"), string("AUTHORED")),
                ]),
            )
            .join(
                b,
                JoinType::Inner,
                n,
                and([
                    starts_with(col("e0", "traversal_path"), col("n", "traversal_path")),
                    eq(col("e0", "target_id"), col("n", "id")),
                ]),
            )
            .filter(
                b,
                and([
                    eq(col("n", "confidential"), boolean(true)),
                    starts_with(
                        col("n", "traversal_path"),
                        param("traversal_path", DataType::String),
                    ),
                ]),
            )
            .sort(b, &[(col("n", "created_at"), SortDir::Desc)])
            .project(
                b,
                &[
                    (col("u", "username"), "u_username"),
                    (col("n", "confidential"), "n_confidential"),
                ],
            )
            .fetch(b, 25, None)
        });

        assert!(pq.sql.contains("gl_user AS u INNER JOIN gl_edge AS e0"));
        assert!(pq.sql.contains("INNER JOIN gl_note AS n"));
        assert!(
            pq.sql
                .contains("startsWith(e0.traversal_path, u.traversal_path)")
        );
        assert!(pq.sql.contains("(e0.relationship_kind = {p0:String})"));
        assert!(pq.sql.contains("(n.confidential = {p1:Bool})"));
        assert!(
            pq.sql
                .contains("startsWith(n.traversal_path, {traversal_path:String})")
        );
        assert!(pq.sql.contains("ORDER BY n.created_at DESC"));
        assert!(pq.sql.contains("LIMIT 25"));
        assert!(pq.sql.contains("u.username AS u_username"));
        assert!(pq.sql.contains("n.confidential AS n_confidential"));
        assert_eq!(pq.params.len(), 2);
        assert_eq!(pq.params["p0"].value, Value::String("AUTHORED".into()));
        assert_eq!(pq.params["p1"].value, Value::Bool(true));
    }

    #[test]
    fn if_then_case_expression() {
        let pq = build_and_emit(|b| {
            b.read(
                "source_data",
                "t",
                &[("state", DataType::Int64), ("name", DataType::String)],
            )
            .project(
                b,
                &[
                    (
                        if_then(
                            vec![
                                (eq(col("t", "state"), int(0)), string("active")),
                                (eq(col("t", "state"), int(1)), string("blocked")),
                            ],
                            Some(string("unknown")),
                        ),
                        "state",
                    ),
                    (col("t", "name"), "name"),
                ],
            )
        });

        assert!(pq.sql.contains("CASE WHEN (t.state = {p0:Int64}) THEN {p1:String} WHEN (t.state = {p2:Int64}) THEN {p3:String} ELSE {p4:String} END AS state"));
        assert_eq!(pq.params["p0"].value, Value::Number(0.into()));
        assert_eq!(pq.params["p1"].value, Value::String("active".into()));
        assert_eq!(pq.params["p4"].value, Value::String("unknown".into()));
    }

    #[test]
    fn is_not_null_filter() {
        let pq = build_and_emit(|b| {
            b.read(
                "source_data",
                "t",
                &[("fk", DataType::Int64), ("id", DataType::Int64)],
            )
            .filter(b, is_not_null(col("t", "fk")))
            .project(b, &[(col("t", "id"), "id")])
        });

        assert_eq!(
            pq.sql,
            "SELECT t.id FROM source_data AS t WHERE (t.fk IS NOT NULL)"
        );
    }

    #[test]
    fn in_list_expression() {
        let pq = build_and_emit(|b| {
            b.read(
                "t",
                "t",
                &[("type", DataType::String), ("id", DataType::Int64)],
            )
            .filter(
                b,
                in_list(
                    col("t", "type"),
                    vec![string("A"), string("B"), string("C")],
                ),
            )
            .project(b, &[(col("t", "id"), "id")])
        });

        assert!(
            pq.sql
                .contains("t.type IN ({p0:String}, {p1:String}, {p2:String})")
        );
        assert_eq!(pq.params["p0"].value, Value::String("A".into()));
        assert_eq!(pq.params["p1"].value, Value::String("B".into()));
        assert_eq!(pq.params["p2"].value, Value::String("C".into()));
    }

    #[test]
    fn function_call_starts_with() {
        let pq = build_and_emit(|b| {
            b.read(
                "gl_note",
                "n",
                &[
                    ("id", DataType::Int64),
                    ("traversal_path", DataType::String),
                ],
            )
            .filter(b, starts_with(col("n", "traversal_path"), string("1/2/")))
            .project(b, &[(col("n", "id"), "id")])
        });

        assert_eq!(
            pq.sql,
            "SELECT n.id FROM gl_note AS n WHERE startsWith(n.traversal_path, {p0:String})"
        );
        assert_eq!(pq.params["p0"].value, Value::String("1/2/".into()));
    }

    #[test]
    fn cast_expression() {
        let pq = build_and_emit(|b| {
            b.read("t", "t", &[("val", DataType::String)])
                .project(b, &[(cast(col("t", "val"), DataType::Int64), "val_int")])
        });

        assert_eq!(pq.sql, "SELECT CAST(t.val AS Int64) AS val_int FROM t");
    }

    #[test]
    fn named_params_not_in_param_map() {
        let pq = build_and_emit(|b| {
            b.read("t", "t", &[("a", DataType::String)])
                .filter(b, eq(col("t", "a"), param("my_param", DataType::String)))
                .project(b, &[(col("t", "a"), "a")])
        });

        assert!(pq.sql.contains("{my_param:String}"));
        assert!(pq.params.is_empty());
    }

    #[test]
    fn select_alias_skipped_when_matches_column() {
        let pq = build_and_emit(|b| {
            b.read(
                "siphon_user",
                "siphon_user",
                &[("id", DataType::Int64), ("name", DataType::String)],
            )
            .project(
                b,
                &[
                    (col("siphon_user", "id"), "id"),
                    (col("siphon_user", "name"), "name"),
                ],
            )
        });

        assert_eq!(
            pq.sql,
            "SELECT siphon_user.id, siphon_user.name FROM siphon_user"
        );
    }

    #[test]
    fn offset_emitted_when_nonzero() {
        let pq = build_and_emit(|b| {
            b.read("t", "t", &[("id", DataType::Int64)])
                .project(b, &[(col("t", "id"), "id")])
                .fetch(b, 10, Some(5))
        });

        assert_eq!(pq.sql, "SELECT t.id FROM t LIMIT 10 OFFSET 5");
    }

    #[test]
    fn raw_expr_emitted_verbatim() {
        let pq = build_and_emit(|b| {
            b.read(
                "t",
                "t",
                &[("id", DataType::Int64), ("ver", DataType::String)],
            )
            .filter(
                b,
                and([
                    gt(
                        raw("_siphon_replicated_at"),
                        param("last_watermark", DataType::String),
                    ),
                    le(
                        raw("_siphon_replicated_at"),
                        param("watermark", DataType::String),
                    ),
                ]),
            )
            .project(
                b,
                &[
                    (raw("id"), "id"),
                    (raw("name"), "name"),
                    (raw("_siphon_replicated_at"), "_version"),
                ],
            )
            .fetch(b, 1000, None)
        });

        assert!(
            pq.sql.contains("_siphon_replicated_at AS _version"),
            "sql: {}",
            pq.sql
        );
        assert!(
            pq.sql
                .contains("(_siphon_replicated_at > {last_watermark:String})"),
            "sql: {}",
            pq.sql
        );
        assert!(
            pq.sql
                .contains("(_siphon_replicated_at <= {watermark:String})"),
            "sql: {}",
            pq.sql
        );
        assert!(pq.sql.contains("LIMIT 1000"), "sql: {}", pq.sql);
        assert!(pq.params.is_empty());
    }

    #[test]
    fn read_raw_from_clause() {
        let pq = build_and_emit(|b| {
            b.read_raw(
                "siphon_projects p INNER JOIN traversal_paths tp ON p.id = tp.id",
                &[("id", DataType::Int64), ("name", DataType::String)],
            )
            .project(b, &[(raw("p.id"), "id"), (raw("p.name"), "name")])
        });

        assert!(
            pq.sql
                .contains("FROM siphon_projects p INNER JOIN traversal_paths tp ON p.id = tp.id"),
            "sql: {}",
            pq.sql
        );
        assert!(pq.sql.contains("p.id"), "sql: {}", pq.sql);
    }

    #[test]
    fn extract_query_pattern() {
        let pq = build_and_emit(|b| {
            b.read(
                "siphon_users",
                "siphon_users",
                &[
                    ("id", DataType::Int64),
                    ("username", DataType::String),
                    ("_siphon_replicated_at", DataType::String),
                    ("_siphon_deleted", DataType::Bool),
                ],
            )
            .filter(
                b,
                and([
                    gt(
                        raw("_siphon_replicated_at"),
                        param("last_watermark", DataType::String),
                    ),
                    le(
                        raw("_siphon_replicated_at"),
                        param("watermark", DataType::String),
                    ),
                ]),
            )
            .sort(b, &[(raw("id"), SortDir::Asc)])
            .project(
                b,
                &[
                    (raw("id"), "id"),
                    (raw("username"), "username"),
                    (raw("_siphon_replicated_at"), "_version"),
                    (raw("_siphon_deleted"), "_deleted"),
                ],
            )
            .fetch(b, 1_000_000, None)
        });

        assert!(pq.sql.contains("SELECT id AS id, username AS username, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("FROM siphon_users"), "sql: {}", pq.sql);
        assert!(
            pq.sql
                .contains("(_siphon_replicated_at > {last_watermark:String})"),
            "sql: {}",
            pq.sql
        );
        assert!(
            pq.sql
                .contains("(_siphon_replicated_at <= {watermark:String})"),
            "sql: {}",
            pq.sql
        );
        assert!(pq.sql.contains("ORDER BY id ASC"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("LIMIT 1000000"), "sql: {}", pq.sql);
        assert!(pq.params.is_empty());
    }

    // ── Phase 3: Query-engine feature tests ──────────────────────────

    #[test]
    fn like_operator() {
        let pq = build_and_emit(|b| {
            b.read(
                "gl_user",
                "u",
                &[("id", DataType::Int64), ("username", DataType::String)],
            )
            .filter(b, like(col("u", "username"), string("%admin%")))
            .project(b, &[(col("u", "id"), "id")])
        });

        assert_eq!(
            pq.sql,
            "SELECT u.id FROM gl_user AS u WHERE (u.username LIKE {p0:String})"
        );
        assert_eq!(pq.params["p0"].value, Value::String("%admin%".into()));
    }

    #[test]
    fn ilike_operator() {
        let pq = build_and_emit(|b| {
            b.read("gl_user", "u", &[("id", DataType::Int64), ("name", DataType::String)])
                .filter(b, ilike(col("u", "name"), string("%Test%")))
                .project(b, &[(col("u", "id"), "id")])
        });

        assert!(pq.sql.contains("(u.name ILIKE {p0:String})"), "sql: {}", pq.sql);
    }

    #[test]
    fn in_operator_binary() {
        let pq = build_and_emit(|b| {
            b.read("gl_user", "u", &[("id", DataType::Int64), ("label", DataType::String)])
                .filter(
                    b,
                    is_in(
                        col("u", "label"),
                        param("types", DataType::array(DataType::String)),
                    ),
                )
                .project(b, &[(col("u", "id"), "id")])
        });

        assert_eq!(
            pq.sql,
            "SELECT u.id FROM gl_user AS u WHERE u.label IN {types:Array(String)}"
        );
    }

    #[test]
    fn aggregate_group_by() {
        let pq = build_and_emit(|b| {
            let t = b.read(
                "nodes",
                "n",
                &[("id", DataType::Int64), ("label", DataType::String)],
            );
            b.aggregate(
                t,
                &[(col("n", "label"), "type")],
                &[("COUNT", "count", vec![col("n", "id")])],
            )
        });

        assert_eq!(
            pq.sql,
            "SELECT n.label AS type, COUNT(n.id) AS count FROM nodes AS n GROUP BY n.label"
        );
    }

    #[test]
    fn aggregate_with_having() {
        let pq = build_and_emit(|b| {
            let t = b.read("nodes", "n", &[("id", DataType::Int64), ("label", DataType::String)]);
            let agged = b.aggregate(
                t,
                &[(col("n", "label"), "type")],
                &[("COUNT", "count", vec![col("n", "id")])],
            );
            // HAVING: FilterRel on top of AggregateRel
            b.filter(agged, gt(raw("COUNT(n.id)"), int(5)))
        });

        assert!(pq.sql.contains("GROUP BY n.label"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("HAVING (COUNT(n.id) > {p0:Int64})"), "sql: {}", pq.sql);
        assert!(!pq.sql.contains("WHERE"), "HAVING should not produce WHERE: {}", pq.sql);
    }

    #[test]
    fn aggregate_with_order_by_and_limit() {
        let pq = build_and_emit(|b| {
            let t = b.read("nodes", "n", &[("id", DataType::Int64), ("label", DataType::String)]);
            b.aggregate(
                t,
                &[(col("n", "label"), "type")],
                &[("COUNT", "count", vec![col("n", "id")])],
            )
            .sort(b, &[(raw("COUNT(n.id)"), SortDir::Desc)])
            .fetch(b, 10, None)
        });

        assert!(pq.sql.contains("GROUP BY n.label"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("ORDER BY COUNT(n.id) DESC"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("LIMIT 10"), "sql: {}", pq.sql);
    }

    #[test]
    fn union_all_as_derived_table() {
        let pq = build_and_emit(|b| {
            let p1 = b
                .read("gl_edge", "e1", &[("source_id", DataType::Int64)])
                .project(b, &[(col("e1", "source_id"), "id")]);
            let p2 = b
                .read("gl_edge", "e2", &[("source_id", DataType::Int64)])
                .project(b, &[(col("e2", "source_id"), "id")]);

            b.union_all(vec![p1, p2], "all_edges")
                .project(b, &[(col("all_edges", "id"), "id")])
        });

        assert!(pq.sql.contains("UNION ALL"), "sql: {}", pq.sql);
        assert!(pq.sql.contains(") AS all_edges"), "sql: {}", pq.sql);
    }

    #[test]
    fn subquery_in_from() {
        let pq = build_and_emit(|b| {
            b.read(
                "gl_project",
                "p",
                &[("id", DataType::Int64), ("name", DataType::String)],
            )
            .filter(b, eq(col("p", "name"), string("test")))
            .project(
                b,
                &[(col("p", "id"), "id"), (col("p", "name"), "name")],
            )
            .subquery(b, "sub")
            .project(b, &[(col("sub", "id"), "id")])
        });

        assert!(pq.sql.contains("(SELECT"), "expected subquery: {}", pq.sql);
        assert!(pq.sql.contains(") AS sub"), "expected alias: {}", pq.sql);
        assert!(pq.sql.contains("gl_project AS p"), "expected inner table: {}", pq.sql);
    }

    #[test]
    fn subquery_in_join() {
        let pq = build_and_emit(|b| {
            // Inner subquery: deduplication via GROUP BY + HAVING
            let e = b.read(
                "gl_edge",
                "e",
                &[
                    ("source_id", DataType::Int64),
                    ("_deleted", DataType::Bool),
                    ("_version", DataType::String),
                ],
            );
            let deduped = b
                .aggregate(
                    e,
                    &[(col("e", "source_id"), "source_id")],
                    &[("argMax", "is_deleted", vec![col("e", "_deleted"), col("e", "_version")])],
                )
                .filter(b, eq(raw("argMax(e._deleted, e._version)"), boolean(false)))
                .subquery(b, "deduped_e");

            // Outer: join user with deduped edges
            b.read("gl_user", "u", &[("id", DataType::Int64)])
                .join(
                    b,
                    JoinType::Inner,
                    deduped,
                    eq(col("u", "id"), col("deduped_e", "source_id")),
                )
                .project(b, &[(col("u", "id"), "id")])
        });

        assert!(pq.sql.contains("INNER JOIN (SELECT"), "expected join with subquery: {}", pq.sql);
        assert!(pq.sql.contains("HAVING"), "expected HAVING in subquery: {}", pq.sql);
        assert!(pq.sql.contains(") AS deduped_e ON"), "expected subquery alias: {}", pq.sql);
    }

    #[test]
    fn cte_with_recursive() {
        // CTE body
        let mut b1 = PlanBuilder::new();
        let base = b1
            .read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&mut b1, &[(col("p", "id"), "node_id")]);
        let recursive = b1
            .read("path_cte", "c", &[("node_id", DataType::Int64)])
            .project(&mut b1, &[(col("c", "node_id"), "node_id")]);
        let cte_root = b1
            .union_all(vec![base, recursive], "cte_body")
            .project(&mut b1, &[(col("cte_body", "node_id"), "node_id")]);
        let cte_plan = b1.build(cte_root);

        // Main query
        let mut b2 = PlanBuilder::new();
        let main_root = b2
            .read("path_cte", "r", &[("node_id", DataType::Int64)])
            .project(&mut b2, &[(col("r", "node_id"), "id")])
            .fetch(&b2, 10, None);
        let plan = b2.build_with_ctes(
            main_root,
            vec![crate::plan::CteDef {
                name: "path_cte".into(),
                plan: cte_plan,
                recursive: true,
            }],
        );

        let pq = emit_clickhouse_sql(&plan).unwrap();

        assert!(pq.sql.contains("WITH RECURSIVE"), "expected WITH RECURSIVE: {}", pq.sql);
        assert!(pq.sql.contains("path_cte AS ("), "expected CTE name: {}", pq.sql);
        assert!(pq.sql.contains("UNION ALL"), "expected UNION ALL in CTE body: {}", pq.sql);
        assert!(pq.sql.contains("LIMIT 10"), "expected LIMIT: {}", pq.sql);
    }

    #[test]
    fn argmax_aggregate_function() {
        let pq = build_and_emit(|b| {
            let t = b.read(
                "gl_edge",
                "e",
                &[
                    ("source_id", DataType::Int64),
                    ("_deleted", DataType::Bool),
                    ("_version", DataType::String),
                ],
            );
            b.aggregate(
                t,
                &[(col("e", "source_id"), "source_id")],
                &[("argMax", "is_deleted", vec![col("e", "_deleted"), col("e", "_version")])],
            )
        });

        assert_eq!(
            pq.sql,
            "SELECT e.source_id, argMax(e._deleted, e._version) AS is_deleted FROM gl_edge AS e GROUP BY e.source_id"
        );
    }
}
