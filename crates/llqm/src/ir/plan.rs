//! Plan builder that produces Substrait plans from the expression DSL.
//!
//! The builder tracks schemas so column references use names (`col("u", "id")`)
//! that get resolved to positional Substrait field references automatically.

use std::collections::HashMap;

use substrait::proto::{
    self, aggregate_rel,
    expression::{self},
    extensions::{
        simple_extension_declaration::{ExtensionFunction, MappingType},
        SimpleExtensionDeclaration, SimpleExtensionUrn,
    },
    fetch_rel, plan_rel, read_rel, rel, rel_common, set_rel, sort_field, AggregateFunction,
    AggregateRel, Expression, FetchRel, FilterRel, FunctionArgument, Plan as SubstraitPlan,
    PlanRel, ProjectRel, ReadRel, Rel, RelCommon, RelRoot, SetRel, SortRel,
};

use crate::ir::expr::{BinaryOp, DataType, Expr, JoinType, LiteralValue, SortDir};
use crate::ir::substrait::{
    binary_op_substrait_name, bool_type, build_named_struct, make_any, make_field_ref,
    make_literal_arg, make_metadata, make_scalar_fn, make_value_arg, string_type,
    to_substrait_join_type, to_substrait_literal, to_substrait_type, unary_op_substrait_name,
};

// ---------------------------------------------------------------------------
// Schema tracking
// ---------------------------------------------------------------------------

/// A column in a relation's output schema.
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub table_alias: String,
    pub name: String,
    pub data_type: DataType,
}

/// Output schema of a relation, used for resolving named column references
/// to positional Substrait field indices.
#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<SchemaColumn>,
}

impl Schema {
    /// Find a column by table alias and name, returning its index.
    pub fn find(&self, table: &str, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.table_alias == table && c.name == name)
    }

    pub fn merge(left: &Schema, right: &Schema) -> Schema {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().cloned());
        Schema { columns }
    }
}

/// A Substrait relation paired with its output schema.
///
/// This is the unit of composition between pipeline phases. Each phase
/// takes a `TypedRel` and returns a new one, sharing the `PlanBuilder`
/// for function registry consistency.
///
/// Most methods are available both on `PlanBuilder` (takes `TypedRel` as arg)
/// and on `TypedRel` itself (takes `&mut PlanBuilder` as arg) for chaining:
///
/// ```text
/// b.read("gl_user", "u", &cols)
///     .filter(&mut b, cond)
///     .sort(&mut b, &keys)
///     .project(&mut b, &items)
///     .fetch(&b, 100, None)
/// ```
pub struct TypedRel {
    pub rel: Rel,
    pub schema: Schema,
}

impl TypedRel {
    fn wrap(rel_type: rel::RelType, schema: Schema) -> Self {
        Self {
            rel: Rel {
                rel_type: Some(rel_type),
            },
            schema,
        }
    }

    /// Chainable: `WHERE condition`
    pub fn filter(self, b: &mut PlanBuilder, condition: Expr) -> Self {
        b.filter(self, condition)
    }

    /// Chainable: `SELECT expr1 AS alias1, ...`
    pub fn project(self, b: &mut PlanBuilder, exprs: &[(Expr, &str)]) -> Self {
        b.project(self, exprs)
    }

    /// Chainable: `ORDER BY key1 dir1, ...`
    pub fn sort(self, b: &mut PlanBuilder, keys: &[(Expr, SortDir)]) -> Self {
        b.sort(self, keys)
    }

    /// Chainable: `LIMIT count [OFFSET offset]`
    pub fn fetch(self, b: &PlanBuilder, count: u64, offset: Option<u64>) -> Self {
        b.fetch(self, count, offset)
    }

    /// Chainable: `self JOIN right ON condition`
    pub fn join(self, b: &mut PlanBuilder, jt: JoinType, right: TypedRel, on: Expr) -> Self {
        b.join(jt, self, right, on)
    }

    /// Chainable: wrap as `(SELECT ...) AS alias`
    pub fn subquery(self, b: &mut PlanBuilder, alias: &str) -> Self {
        b.subquery(self, alias)
    }
}

// ---------------------------------------------------------------------------
// Extension function registry
// ---------------------------------------------------------------------------

struct FunctionRegistry {
    urn: SimpleExtensionUrn,
    declarations: Vec<SimpleExtensionDeclaration>,
    anchors: HashMap<String, u32>,
    next_anchor: u32,
}

impl FunctionRegistry {
    fn new() -> Self {
        Self {
            urn: SimpleExtensionUrn {
                extension_urn_anchor: 1,
                urn: "urn:llqm:functions".into(),
            },
            declarations: Vec::new(),
            anchors: HashMap::new(),
            next_anchor: 1,
        }
    }

    /// Return the anchor for `name`, registering it if new.
    #[allow(deprecated)] // extension_uri_reference is deprecated but required by prost
    fn ensure(&mut self, name: &str) -> u32 {
        if let Some(&anchor) = self.anchors.get(name) {
            return anchor;
        }
        let anchor = self.next_anchor;
        self.next_anchor += 1;
        self.declarations.push(SimpleExtensionDeclaration {
            mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                extension_uri_reference: 0,
                extension_urn_reference: self.urn.extension_urn_anchor,
                function_anchor: anchor,
                name: name.into(),
            })),
        });
        self.anchors.insert(name.into(), anchor);
        anchor
    }
}

// ---------------------------------------------------------------------------
// Plan wrapper
// ---------------------------------------------------------------------------

/// A Common Table Expression (CTE) for WITH clauses.
pub struct CteDef {
    pub name: String,
    pub plan: Plan,
    pub recursive: bool,
}

/// A built Substrait plan ready for codegen or DataFusion consumption.
pub struct Plan {
    pub inner: SubstraitPlan,
    /// CTEs for the WITH clause (not part of Substrait, stored as metadata).
    pub ctes: Vec<CteDef>,
}

impl Plan {
    /// Access the raw Substrait plan (e.g. for `datafusion-substrait`).
    pub fn substrait_plan(&self) -> &SubstraitPlan {
        &self.inner
    }

    /// Consume into the raw Substrait plan.
    pub fn into_substrait_plan(self) -> SubstraitPlan {
        self.inner
    }
}

// ---------------------------------------------------------------------------
// PlanBuilder
// ---------------------------------------------------------------------------

/// Builds Substrait plans from the expression DSL.
///
/// # Recommended relation ordering
///
/// ```text
/// read / join  ->  filter  ->  sort  ->  project  ->  fetch
/// ```
///
/// This produces the simplest SQL. All field references in filter, sort, and
/// project resolve against the same base schema (the read/join output).
pub struct PlanBuilder {
    functions: FunctionRegistry,
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {
            functions: FunctionRegistry::new(),
        }
    }

    // -- Relation builders --------------------------------------------------

    /// Table scan: `FROM table AS alias`
    pub fn read(&mut self, table: &str, alias: &str, columns: &[(&str, DataType)]) -> TypedRel {
        let schema = Schema {
            columns: columns
                .iter()
                .map(|(name, dt)| SchemaColumn {
                    table_alias: alias.into(),
                    name: (*name).into(),
                    data_type: dt.clone(),
                })
                .collect(),
        };

        let read = ReadRel {
            base_schema: Some(build_named_struct(columns)),
            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                names: vec![table.into()],
                advanced_extension: None,
            })),
            advanced_extension: Some(make_metadata(
                "llqm/read_metadata",
                serde_json::json!({ "alias": alias }),
            )),
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Read(Box::new(read)), schema)
    }

    /// Raw FROM clause: `FROM <raw_from_sql>`
    ///
    /// Escape hatch for migration from raw SQL. The `raw_from` string is
    /// emitted verbatim as the FROM clause. The `columns` parameter defines
    /// the output schema for column resolution in downstream relations.
    /// Columns have empty table alias (unqualified).
    pub fn read_raw(&mut self, raw_from: &str, columns: &[(&str, DataType)]) -> TypedRel {
        let schema = Schema {
            columns: columns
                .iter()
                .map(|(name, dt)| SchemaColumn {
                    table_alias: String::new(),
                    name: (*name).into(),
                    data_type: dt.clone(),
                })
                .collect(),
        };

        let read = ReadRel {
            base_schema: Some(build_named_struct(columns)),
            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                names: vec!["__raw".into()],
                advanced_extension: None,
            })),
            advanced_extension: Some(make_metadata(
                "llqm/read_metadata",
                serde_json::json!({ "raw_from": raw_from }),
            )),
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Read(Box::new(read)), schema)
    }

    /// `left JOIN right ON condition`
    pub fn join(
        &mut self,
        join_type: JoinType,
        left: TypedRel,
        right: TypedRel,
        on: Expr,
    ) -> TypedRel {
        let merged = Schema::merge(&left.schema, &right.schema);
        let resolved_on = self.resolve_expr(&on, &merged);

        let join = proto::JoinRel {
            left: Some(Box::new(left.rel)),
            right: Some(Box::new(right.rel)),
            expression: Some(Box::new(resolved_on)),
            r#type: to_substrait_join_type(join_type) as i32,
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Join(Box::new(join)), merged)
    }

    /// `WHERE condition`
    pub fn filter(&mut self, input: TypedRel, condition: Expr) -> TypedRel {
        let resolved = self.resolve_expr(&condition, &input.schema);
        let schema = input.schema;

        let filter = FilterRel {
            input: Some(Box::new(input.rel)),
            condition: Some(Box::new(resolved)),
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Filter(Box::new(filter)), schema)
    }

    /// `SELECT expr1 AS alias1, expr2 AS alias2, ...`
    ///
    /// Each `(Expr, &str)` pair is an expression and its output alias.
    pub fn project(&mut self, input: TypedRel, exprs: &[(Expr, &str)]) -> TypedRel {
        let resolved: Vec<Expression> = exprs
            .iter()
            .map(|(e, _)| self.resolve_expr(e, &input.schema))
            .collect();

        let output_schema = Schema {
            columns: exprs
                .iter()
                .map(|(expr, alias)| {
                    let data_type = infer_data_type(expr, &input.schema);
                    let table_alias = infer_table(expr);
                    SchemaColumn {
                        table_alias,
                        name: (*alias).into(),
                        data_type,
                    }
                })
                .collect(),
        };

        let input_count = input.schema.columns.len();
        let emit = (input_count..input_count + exprs.len())
            .map(|i| i as i32)
            .collect();

        let project = ProjectRel {
            common: Some(RelCommon {
                emit_kind: Some(rel_common::EmitKind::Emit(rel_common::Emit {
                    output_mapping: emit,
                })),
                ..Default::default()
            }),
            input: Some(Box::new(input.rel)),
            expressions: resolved,
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Project(Box::new(project)), output_schema)
    }

    /// `ORDER BY key1 dir1, key2 dir2, ...`
    pub fn sort(&mut self, input: TypedRel, keys: &[(Expr, SortDir)]) -> TypedRel {
        let sort_fields: Vec<proto::SortField> = keys
            .iter()
            .map(|(expr, dir)| {
                let resolved = self.resolve_expr(expr, &input.schema);
                proto::SortField {
                    expr: Some(resolved),
                    sort_kind: Some(sort_field::SortKind::Direction(match dir {
                        SortDir::Asc => sort_field::SortDirection::AscNullsLast as i32,
                        SortDir::Desc => sort_field::SortDirection::DescNullsLast as i32,
                    })),
                }
            })
            .collect();

        let sort = SortRel {
            input: Some(Box::new(input.rel)),
            sorts: sort_fields,
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Sort(Box::new(sort)), input.schema)
    }

    /// `LIMIT count [OFFSET offset]`
    #[allow(deprecated)] // FetchRel count/offset fields are deprecated but simpler
    pub fn fetch(&self, input: TypedRel, count: u64, offset: Option<u64>) -> TypedRel {
        let fetch = FetchRel {
            input: Some(Box::new(input.rel)),
            count_mode: Some(fetch_rel::CountMode::Count(count as i64)),
            offset_mode: offset.map(|o| fetch_rel::OffsetMode::Offset(o as i64)),
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Fetch(Box::new(fetch)), input.schema)
    }

    /// `SELECT agg_exprs... FROM input GROUP BY group_exprs...`
    ///
    /// `group_exprs` are `(Expr, &str)` pairs: the grouping expression and its
    /// output alias. `agg_exprs` are `(&str, &str, Vec<Expr>)` triples:
    /// function name, output alias, and arguments.
    ///
    /// The output schema contains the grouping columns first, then the
    /// aggregate columns.
    #[allow(deprecated)] // AggregateFunction.args
    pub fn aggregate(
        &mut self,
        input: TypedRel,
        group_exprs: &[(Expr, &str)],
        agg_exprs: &[(&str, &str, Vec<Expr>)],
    ) -> TypedRel {
        let grouping_expressions: Vec<Expression> = group_exprs
            .iter()
            .map(|(e, _)| self.resolve_expr(e, &input.schema))
            .collect();
        let expression_references: Vec<u32> = (0..group_exprs.len() as u32).collect();

        let measures: Vec<aggregate_rel::Measure> = agg_exprs
            .iter()
            .map(|(func_name, _alias, args)| {
                let resolved_args: Vec<FunctionArgument> = args
                    .iter()
                    .map(|a| make_value_arg(self.resolve_expr(a, &input.schema)))
                    .collect();
                let anchor = self.functions.ensure(func_name);
                aggregate_rel::Measure {
                    measure: Some(AggregateFunction {
                        function_reference: anchor,
                        arguments: resolved_args,
                        output_type: Some(string_type()),
                        phase: proto::AggregationPhase::InitialToResult as i32,
                        sorts: Vec::new(),
                        invocation: proto::aggregate_function::AggregationInvocation::All as i32,
                        options: Vec::new(),
                        args: Vec::new(),
                    }),
                    filter: None,
                }
            })
            .collect();

        #[allow(deprecated)]
        let grouping = aggregate_rel::Grouping {
            grouping_expressions: Vec::new(),
            expression_references,
        };

        // Build output schema: group columns first, then aggregate columns
        let mut output_columns: Vec<SchemaColumn> = group_exprs
            .iter()
            .map(|(expr, alias)| SchemaColumn {
                table_alias: infer_table(expr),
                name: (*alias).into(),
                data_type: infer_data_type(expr, &input.schema),
            })
            .collect();
        for (_func_name, alias, _args) in agg_exprs {
            output_columns.push(SchemaColumn {
                table_alias: String::new(),
                name: (*alias).into(),
                data_type: DataType::String,
            });
        }

        let agg = AggregateRel {
            input: Some(Box::new(input.rel)),
            groupings: vec![grouping],
            measures,
            grouping_expressions,
            ..Default::default()
        };

        TypedRel::wrap(
            rel::RelType::Aggregate(Box::new(agg)),
            Schema {
                columns: output_columns,
            },
        )
    }

    /// `UNION ALL` of multiple typed relations.
    ///
    /// All inputs must have compatible schemas. The output schema is taken
    /// from the first input, with column table aliases updated to the union's
    /// alias. The `alias` is stored in metadata for codegen to emit
    /// `(...) AS alias` when used as a derived table.
    pub fn union_all(&mut self, inputs: Vec<TypedRel>, alias: &str) -> TypedRel {
        assert!(!inputs.is_empty(), "union_all requires at least one input");

        let col_names: Vec<String> = inputs[0]
            .schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let schema = Schema {
            columns: inputs[0]
                .schema
                .columns
                .iter()
                .map(|c| SchemaColumn {
                    table_alias: alias.into(),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
        };
        let rels: Vec<Rel> = inputs.into_iter().map(|t| t.rel).collect();

        let set = SetRel {
            inputs: rels,
            op: set_rel::SetOp::UnionAll as i32,
            advanced_extension: Some(make_metadata(
                "llqm/set_metadata",
                serde_json::json!({ "alias": alias, "column_names": col_names }),
            )),
            ..Default::default()
        };

        TypedRel::wrap(rel::RelType::Set(set), schema)
    }

    /// Wrap a `TypedRel` as a named derived table (subquery in FROM clause).
    ///
    /// The `alias` is stored in metadata and the schema columns get updated
    /// to use the new alias as their table qualifier.
    pub fn subquery(&mut self, input: TypedRel, alias: &str) -> TypedRel {
        let metadata = serde_json::json!({ "subquery_alias": alias });

        let ext_single = proto::ExtensionSingleRel {
            common: None,
            input: Some(Box::new(input.rel)),
            detail: Some(make_any("llqm/subquery_metadata", &metadata)),
        };

        let schema = Schema {
            columns: input
                .schema
                .columns
                .iter()
                .map(|c| SchemaColumn {
                    table_alias: alias.into(),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
        };

        TypedRel::wrap(rel::RelType::ExtensionSingle(Box::new(ext_single)), schema)
    }

    /// Finalize the plan. Output column names come from the root relation's schema.
    pub fn build(self, root: TypedRel) -> Plan {
        let output_names: Vec<String> =
            root.schema.columns.iter().map(|c| c.name.clone()).collect();

        #[allow(deprecated)]
        let plan = SubstraitPlan {
            extension_uris: Vec::new(),
            extension_urns: vec![self.functions.urn],
            extensions: self.functions.declarations,
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(root.rel),
                    names: output_names,
                })),
            }],
            ..Default::default()
        };

        Plan {
            inner: plan,
            ctes: Vec::new(),
        }
    }

    /// Finalize the plan with CTEs in the WITH clause.
    ///
    /// Each `CteDef` produces a `WITH [RECURSIVE] name AS (SELECT ...)` prefix.
    pub fn build_with_ctes(self, root: TypedRel, ctes: Vec<CteDef>) -> Plan {
        let mut plan = self.build(root);
        plan.ctes = ctes;
        plan
    }

    // -- Expression resolution ----------------------------------------------

    fn resolve_expr(&mut self, expr: &Expr, schema: &Schema) -> Expression {
        match expr {
            Expr::Column { table, name } => {
                let index = schema.find(table, name).unwrap_or_else(|| {
                    let available: Vec<String> = schema
                        .columns
                        .iter()
                        .map(|c| format!("{}.{}", c.table_alias, c.name))
                        .collect();
                    panic!("column {table}.{name} not found in schema; available: {available:?}")
                });
                make_field_ref(index)
            }

            Expr::Literal(lit) => Expression {
                rex_type: Some(expression::RexType::Literal(to_substrait_literal(lit))),
            },

            Expr::Param { name, data_type } => {
                let anchor = self.functions.ensure("__param");
                make_scalar_fn(
                    anchor,
                    vec![
                        make_literal_arg(&LiteralValue::String(name.clone())),
                        make_literal_arg(&LiteralValue::String(data_type.to_string())),
                    ],
                    to_substrait_type(data_type.clone()),
                )
            }

            Expr::BinaryOp { op, left, right } => {
                let l = self.resolve_expr(left, schema);
                let r = self.resolve_expr(right, schema);
                let fn_name = binary_op_substrait_name(*op);
                let anchor = self.functions.ensure(fn_name);
                make_scalar_fn(
                    anchor,
                    vec![make_value_arg(l), make_value_arg(r)],
                    bool_type(),
                )
            }

            Expr::UnaryOp { op, operand } => {
                let inner = self.resolve_expr(operand, schema);
                let fn_name = unary_op_substrait_name(*op);
                let anchor = self.functions.ensure(fn_name);
                make_scalar_fn(anchor, vec![make_value_arg(inner)], bool_type())
            }

            Expr::FuncCall { name, args } => {
                let resolved_args: Vec<FunctionArgument> = args
                    .iter()
                    .map(|a| make_value_arg(self.resolve_expr(a, schema)))
                    .collect();
                let anchor = self.functions.ensure(name);
                make_scalar_fn(anchor, resolved_args, string_type())
            }

            Expr::Cast { expr, target_type } => {
                let inner = self.resolve_expr(expr, schema);
                Expression {
                    rex_type: Some(expression::RexType::Cast(Box::new(expression::Cast {
                        input: Some(Box::new(inner)),
                        r#type: Some(to_substrait_type(target_type.clone())),
                        failure_behavior: expression::cast::FailureBehavior::ThrowException as i32,
                    }))),
                }
            }

            Expr::IfThen { ifs, else_expr } => {
                let clauses: Vec<expression::if_then::IfClause> = ifs
                    .iter()
                    .map(|(cond, then)| expression::if_then::IfClause {
                        r#if: Some(self.resolve_expr(cond, schema)),
                        then: Some(self.resolve_expr(then, schema)),
                    })
                    .collect();
                let else_resolved = else_expr
                    .as_ref()
                    .map(|e| Box::new(self.resolve_expr(e, schema)));
                Expression {
                    rex_type: Some(expression::RexType::IfThen(Box::new(expression::IfThen {
                        ifs: clauses,
                        r#else: else_resolved,
                    }))),
                }
            }

            Expr::InList { expr, list } => {
                let value = self.resolve_expr(expr, schema);
                let options: Vec<Expression> =
                    list.iter().map(|e| self.resolve_expr(e, schema)).collect();
                Expression {
                    rex_type: Some(expression::RexType::SingularOrList(Box::new(
                        expression::SingularOrList {
                            value: Some(Box::new(value)),
                            options,
                        },
                    ))),
                }
            }

            Expr::Raw(sql) => {
                let anchor = self.functions.ensure("__raw_sql");
                make_scalar_fn(
                    anchor,
                    vec![make_literal_arg(&LiteralValue::String(sql.clone()))],
                    string_type(),
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers (plan-level, not Substrait)
// ---------------------------------------------------------------------------

fn infer_data_type(expr: &Expr, schema: &Schema) -> DataType {
    match expr {
        Expr::Column { table, name } => schema
            .find(table, name)
            .map(|i| schema.columns[i].data_type.clone())
            .unwrap_or(DataType::String),
        Expr::Literal(LiteralValue::String(_)) | Expr::Param { .. } => DataType::String,
        Expr::Literal(LiteralValue::Int64(_)) => DataType::Int64,
        Expr::Literal(LiteralValue::Float64(_)) => DataType::Float64,
        Expr::Literal(LiteralValue::Bool(_)) => DataType::Bool,
        Expr::Literal(LiteralValue::Null) => DataType::String,
        Expr::Cast { target_type, .. } => target_type.clone(),
        Expr::BinaryOp { op, .. } => match op {
            BinaryOp::Add => DataType::Int64,
            _ => DataType::Bool,
        },
        Expr::FuncCall { .. } | Expr::IfThen { .. } | Expr::InList { .. } | Expr::Raw(_) => {
            DataType::String
        }
        Expr::UnaryOp { .. } => DataType::Bool,
    }
}

fn infer_table(expr: &Expr) -> String {
    match expr {
        Expr::Column { table, .. } => table.clone(),
        _ => String::new(),
    }
}
