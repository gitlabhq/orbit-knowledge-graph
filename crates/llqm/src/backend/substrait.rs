//! Substrait encode/decode and protobuf helpers.
//!
//! This is the only file that imports the `substrait` crate.
//! Contains both low-level protobuf construction helpers and the
//! `encode()` function that walks the `Rel`/`Expr` tree to produce
//! Substrait protobuf structures for the DataFusion backend.

use std::collections::HashMap;

use substrait::proto::{
    self, AggregateFunction, AggregateRel, Expression, FetchRel, FilterRel, FunctionArgument,
    NamedStruct, PlanRel, ProjectRel, ReadRel, Rel, RelCommon, RelRoot, SetRel, SortRel,
    aggregate_rel, expression,
    expression::{
        FieldReference, Literal, ReferenceSegment, ScalarFunction, field_reference,
        literal::LiteralType, reference_segment,
    },
    extensions as ext, fetch_rel, join_rel, plan_rel, read_rel, rel, rel_common, set_rel,
    sort_field, r#type,
};

use crate::ir::expr::{BinaryOp, DataType, Expr, JoinType, LiteralValue, UnaryOp};
use crate::ir::plan::{self as v2, Measure, Plan, RAW_FROM_TAG};

// ---------------------------------------------------------------------------
// Low-level Substrait protobuf helpers
// ---------------------------------------------------------------------------

fn make_metadata(type_url: &str, json: serde_json::Value) -> ext::AdvancedExtension {
    ext::AdvancedExtension {
        optimization: vec![make_any(type_url, &json)],
        enhancement: None,
    }
}

fn make_any(type_url: &str, json: &serde_json::Value) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.into(),
        value: serde_json::to_vec(json).expect("json serialization"),
    }
}

fn build_named_struct(columns: &[(&str, DataType)]) -> NamedStruct {
    NamedStruct {
        names: columns.iter().map(|(n, _)| (*n).into()).collect(),
        r#struct: Some(r#type::Struct {
            types: columns
                .iter()
                .map(|(_, dt)| to_substrait_type(dt.clone()))
                .collect(),
            ..Default::default()
        }),
    }
}

fn make_field_ref(index: usize) -> Expression {
    Expression {
        rex_type: Some(expression::RexType::Selection(Box::new(FieldReference {
            reference_type: Some(field_reference::ReferenceType::DirectReference(
                ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                        reference_segment::StructField {
                            field: index as i32,
                            child: None,
                        },
                    ))),
                },
            )),
            root_type: Some(field_reference::RootType::RootReference(
                field_reference::RootReference {},
            )),
        }))),
    }
}

fn to_substrait_literal(lit: &LiteralValue) -> Literal {
    let literal_type = match lit {
        LiteralValue::String(s) => Some(LiteralType::String(s.clone())),
        LiteralValue::Int64(n) => Some(LiteralType::I64(*n)),
        LiteralValue::Float64(f) => Some(LiteralType::Fp64(*f)),
        LiteralValue::Bool(b) => Some(LiteralType::Boolean(*b)),
        LiteralValue::Null => Some(LiteralType::Null(proto::Type::default())),
    };
    Literal {
        nullable: false,
        type_variation_reference: 0,
        literal_type,
    }
}

fn make_literal_arg(lit: &LiteralValue) -> substrait::proto::FunctionArgument {
    substrait::proto::FunctionArgument {
        arg_type: Some(substrait::proto::function_argument::ArgType::Value(
            Expression {
                rex_type: Some(expression::RexType::Literal(to_substrait_literal(lit))),
            },
        )),
    }
}

fn make_value_arg(expr: Expression) -> substrait::proto::FunctionArgument {
    substrait::proto::FunctionArgument {
        arg_type: Some(substrait::proto::function_argument::ArgType::Value(expr)),
    }
}

#[allow(deprecated)]
fn make_scalar_fn(
    anchor: u32,
    arguments: Vec<FunctionArgument>,
    output_type: proto::Type,
) -> Expression {
    Expression {
        rex_type: Some(expression::RexType::ScalarFunction(ScalarFunction {
            function_reference: anchor,
            arguments,
            output_type: Some(output_type),
            options: Vec::new(),
            args: Vec::new(),
        })),
    }
}

fn to_substrait_type(dt: DataType) -> proto::Type {
    let kind = match dt {
        DataType::String => r#type::Kind::String(r#type::String {
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        }),
        DataType::Int64 => r#type::Kind::I64(r#type::I64 {
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        }),
        DataType::Float64 => r#type::Kind::Fp64(r#type::Fp64 {
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        }),
        DataType::Bool => r#type::Kind::Bool(r#type::Boolean {
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        }),
        DataType::Array(inner) => r#type::Kind::List(Box::new(r#type::List {
            r#type: Some(Box::new(to_substrait_type(*inner))),
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        })),
        #[allow(deprecated)]
        DataType::DateTime => r#type::Kind::Timestamp(r#type::Timestamp {
            nullability: r#type::Nullability::Required as i32,
            type_variation_reference: 0,
        }),
    };
    proto::Type { kind: Some(kind) }
}

fn bool_type() -> proto::Type {
    to_substrait_type(DataType::Bool)
}

fn string_type() -> proto::Type {
    to_substrait_type(DataType::String)
}

fn to_substrait_join_type(jt: JoinType) -> join_rel::JoinType {
    match jt {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::Cross => join_rel::JoinType::Inner,
    }
}

fn binary_op_substrait_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "equal",
        BinaryOp::Ne => "not_equal",
        BinaryOp::Lt => "lt",
        BinaryOp::Le => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Ge => "gte",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Add => "add",
        BinaryOp::Sub => "subtract",
        BinaryOp::Mul => "multiply",
        BinaryOp::Div => "divide",
        BinaryOp::Mod => "modulus",
        BinaryOp::Like => "like",
        BinaryOp::ILike => "ilike",
        BinaryOp::In => "in",
    }
}

fn unary_op_substrait_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
        UnaryOp::IsNull => "is_null",
        UnaryOp::IsNotNull => "is_not_null",
    }
}

// ---------------------------------------------------------------------------
// Encode: Plan → substrait::proto::Plan
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("column {table}.{name} not found in schema; available: {available:?}")]
    ColumnNotFound {
        table: String,
        name: String,
        available: Vec<String>,
    },

    #[error("CTEs are not supported in Substrait encoding (use ClickHouse backend)")]
    UnsupportedCtes,

    #[error("DISTINCT is not supported in Substrait encoding (use ClickHouse backend)")]
    UnsupportedDistinct,
}

/// Encode a `Plan` into a `substrait::proto::Plan`.
pub fn encode(plan: &Plan) -> Result<proto::Plan, EncodeError> {
    if !plan.ctes.is_empty() {
        return Err(EncodeError::UnsupportedCtes);
    }

    let mut fns = FunctionRegistry::new();
    let root_rel = encode_rel(&mut fns, &plan.root)?;

    let (urn, declarations) = fns.into_declarations();

    #[allow(deprecated)]
    Ok(proto::Plan {
        extension_uris: Vec::new(),
        extension_urns: vec![urn],
        extensions: declarations,
        relations: vec![PlanRel {
            rel_type: Some(plan_rel::RelType::Root(RelRoot {
                input: Some(root_rel),
                names: plan.output_names.clone(),
            })),
        }],
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// Function registry for Substrait extension declarations
// ---------------------------------------------------------------------------

struct FunctionRegistry {
    urn: proto::extensions::SimpleExtensionUrn,
    declarations: Vec<proto::extensions::SimpleExtensionDeclaration>,
    anchors: HashMap<String, u32>,
    next_anchor: u32,
}

impl FunctionRegistry {
    fn new() -> Self {
        Self {
            urn: proto::extensions::SimpleExtensionUrn {
                extension_urn_anchor: 1,
                urn: "urn:llqm:functions".into(),
            },
            declarations: Vec::new(),
            anchors: HashMap::new(),
            next_anchor: 1,
        }
    }

    #[allow(deprecated)]
    fn ensure(&mut self, name: &str) -> u32 {
        if let Some(&anchor) = self.anchors.get(name) {
            return anchor;
        }
        let anchor = self.next_anchor;
        self.next_anchor += 1;
        self.declarations
            .push(proto::extensions::SimpleExtensionDeclaration {
                mapping_type: Some(
                    proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                        proto::extensions::simple_extension_declaration::ExtensionFunction {
                            extension_uri_reference: 0,
                            extension_urn_reference: self.urn.extension_urn_anchor,
                            function_anchor: anchor,
                            name: name.into(),
                        },
                    ),
                ),
            });
        self.anchors.insert(name.into(), anchor);
        anchor
    }

    fn into_declarations(
        self,
    ) -> (
        proto::extensions::SimpleExtensionUrn,
        Vec<proto::extensions::SimpleExtensionDeclaration>,
    ) {
        (self.urn, self.declarations)
    }
}

// ---------------------------------------------------------------------------
// Schema tracking for positional column resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SchemaColumn {
    table_alias: String,
    name: String,
}

#[derive(Debug, Clone)]
struct Schema {
    columns: Vec<SchemaColumn>,
}

impl Schema {
    fn find(&self, table: &str, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.table_alias == table && c.name == name)
    }

    fn merge(left: &Schema, right: &Schema) -> Schema {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().cloned());
        Schema { columns }
    }

    fn available_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| format!("{}.{}", c.table_alias, c.name))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Rel encoding
// ---------------------------------------------------------------------------

fn encode_rel(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    match &rel.kind {
        v2::RelKind::Read { .. } => encode_read(rel),
        v2::RelKind::Filter { .. } => encode_filter(fns, rel),
        v2::RelKind::Project { .. } => encode_project(fns, rel),
        v2::RelKind::Join { .. } => encode_join(fns, rel),
        v2::RelKind::Sort { .. } => encode_sort(fns, rel),
        v2::RelKind::Fetch { .. } => encode_fetch(fns, rel),
        v2::RelKind::Aggregate { .. } => encode_aggregate(fns, rel),
        v2::RelKind::UnionAll { .. } => encode_union_all(fns, rel),
        v2::RelKind::Subquery { .. } => encode_subquery(fns, rel),
        v2::RelKind::Distinct => Err(EncodeError::UnsupportedDistinct),
    }
}

fn encode_read(rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Read {
        table,
        alias,
        columns,
    } = &rel.kind
    else {
        unreachable!()
    };

    let col_pairs: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.clone()))
        .collect();

    let is_raw = table == RAW_FROM_TAG;
    let (table_name, metadata) = if is_raw {
        ("__raw".into(), serde_json::json!({ "raw_from": alias }))
    } else {
        (table.clone(), serde_json::json!({ "alias": alias }))
    };

    let substrait_read = ReadRel {
        base_schema: Some(build_named_struct(&col_pairs)),
        read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
            names: vec![table_name],
            advanced_extension: None,
        })),
        advanced_extension: Some(make_metadata("llqm/read_metadata", metadata)),
        ..Default::default()
    };
    Ok(Rel {
        rel_type: Some(rel::RelType::Read(Box::new(substrait_read))),
    })
}

fn encode_filter(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Filter { condition } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;
    let schema = collect_schema(&rel.inputs[0]);
    let condition = encode_expr(fns, condition, &schema)?;

    Ok(Rel {
        rel_type: Some(rel::RelType::Filter(Box::new(FilterRel {
            input: Some(Box::new(input)),
            condition: Some(Box::new(condition)),
            ..Default::default()
        }))),
    })
}

fn encode_project(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Project { expressions } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;
    let schema = collect_schema(&rel.inputs[0]);
    let input_count = schema.columns.len();

    let encoded_expressions: Vec<Expression> = expressions
        .iter()
        .map(|(e, _)| encode_expr(fns, e, &schema))
        .collect::<Result<_, _>>()?;

    let emit = (input_count..input_count + expressions.len())
        .map(|i| i as i32)
        .collect();

    Ok(Rel {
        rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
            common: Some(RelCommon {
                emit_kind: Some(rel_common::EmitKind::Emit(rel_common::Emit {
                    output_mapping: emit,
                })),
                ..Default::default()
            }),
            input: Some(Box::new(input)),
            expressions: encoded_expressions,
            ..Default::default()
        }))),
    })
}

fn encode_join(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Join {
        join_type,
        condition,
    } = &rel.kind
    else {
        unreachable!()
    };
    let left = encode_rel(fns, &rel.inputs[0])?;
    let right = encode_rel(fns, &rel.inputs[1])?;
    let left_schema = collect_schema(&rel.inputs[0]);
    let right_schema = collect_schema(&rel.inputs[1]);
    let merged = Schema::merge(&left_schema, &right_schema);
    let condition = encode_expr(fns, condition, &merged)?;

    Ok(Rel {
        rel_type: Some(rel::RelType::Join(Box::new(proto::JoinRel {
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
            expression: Some(Box::new(condition)),
            r#type: to_substrait_join_type(*join_type) as i32,
            ..Default::default()
        }))),
    })
}

fn encode_sort(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Sort { sorts } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;
    let schema = collect_schema(&rel.inputs[0]);

    let encoded_sorts: Vec<proto::SortField> = sorts
        .iter()
        .map(|s| {
            let expr = encode_expr(fns, &s.expr, &schema)?;
            Ok(proto::SortField {
                expr: Some(expr),
                sort_kind: Some(sort_field::SortKind::Direction(match s.direction {
                    crate::ir::expr::SortDir::Asc => sort_field::SortDirection::AscNullsLast as i32,
                    crate::ir::expr::SortDir::Desc => {
                        sort_field::SortDirection::DescNullsLast as i32
                    }
                })),
            })
        })
        .collect::<Result<_, EncodeError>>()?;

    Ok(Rel {
        rel_type: Some(rel::RelType::Sort(Box::new(SortRel {
            input: Some(Box::new(input)),
            sorts: encoded_sorts,
            ..Default::default()
        }))),
    })
}

#[allow(deprecated)]
fn encode_fetch(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Fetch { limit, offset } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;

    Ok(Rel {
        rel_type: Some(rel::RelType::Fetch(Box::new(FetchRel {
            input: Some(Box::new(input)),
            count_mode: Some(fetch_rel::CountMode::Count(*limit as i64)),
            offset_mode: offset.map(|o| fetch_rel::OffsetMode::Offset(o as i64)),
            ..Default::default()
        }))),
    })
}

#[allow(deprecated)]
fn encode_aggregate(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Aggregate { group_by, measures } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;
    let schema = collect_schema(&rel.inputs[0]);

    let grouping_expressions: Vec<Expression> = group_by
        .iter()
        .map(|e| encode_expr(fns, e, &schema))
        .collect::<Result<_, _>>()?;

    let expression_references: Vec<u32> = (0..group_by.len() as u32).collect();

    let encoded_measures: Vec<aggregate_rel::Measure> = measures
        .iter()
        .map(|m| encode_measure(fns, m, &schema))
        .collect::<Result<_, _>>()?;

    let grouping = aggregate_rel::Grouping {
        grouping_expressions: Vec::new(),
        expression_references,
    };

    Ok(Rel {
        rel_type: Some(rel::RelType::Aggregate(Box::new(AggregateRel {
            input: Some(Box::new(input)),
            groupings: vec![grouping],
            measures: encoded_measures,
            grouping_expressions,
            ..Default::default()
        }))),
    })
}

#[allow(deprecated)]
fn encode_measure(
    fns: &mut FunctionRegistry,
    measure: &Measure,
    schema: &Schema,
) -> Result<aggregate_rel::Measure, EncodeError> {
    let arguments: Vec<FunctionArgument> = measure
        .args
        .iter()
        .map(|a| Ok(make_value_arg(encode_expr(fns, a, schema)?)))
        .collect::<Result<_, EncodeError>>()?;

    let anchor = fns.ensure(&measure.function);

    let filter = match &measure.filter {
        Some(f) => Some(encode_expr(fns, f, schema)?),
        None => None,
    };

    Ok(aggregate_rel::Measure {
        measure: Some(AggregateFunction {
            function_reference: anchor,
            arguments,
            output_type: Some(string_type()),
            phase: proto::AggregationPhase::InitialToResult as i32,
            sorts: Vec::new(),
            invocation: proto::aggregate_function::AggregationInvocation::All as i32,
            options: Vec::new(),
            args: Vec::new(),
        }),
        filter,
    })
}

fn encode_union_all(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::UnionAll { alias } = &rel.kind else {
        unreachable!()
    };

    let inputs: Vec<Rel> = rel
        .inputs
        .iter()
        .map(|r| encode_rel(fns, r))
        .collect::<Result<_, _>>()?;

    let col_names: Vec<String> = if let Some(first) = rel.inputs.first() {
        first.output_names().into_iter().collect()
    } else {
        Vec::new()
    };

    Ok(Rel {
        rel_type: Some(rel::RelType::Set(SetRel {
            inputs,
            op: set_rel::SetOp::UnionAll as i32,
            advanced_extension: Some(make_metadata(
                "llqm/set_metadata",
                serde_json::json!({ "alias": alias, "column_names": col_names }),
            )),
            ..Default::default()
        })),
    })
}

fn encode_subquery(fns: &mut FunctionRegistry, rel: &v2::Rel) -> Result<Rel, EncodeError> {
    let v2::RelKind::Subquery { alias } = &rel.kind else {
        unreachable!()
    };
    let input = encode_rel(fns, &rel.inputs[0])?;
    let metadata = serde_json::json!({ "subquery_alias": alias });

    Ok(Rel {
        rel_type: Some(rel::RelType::ExtensionSingle(Box::new(
            proto::ExtensionSingleRel {
                common: None,
                input: Some(Box::new(input)),
                detail: Some(make_any("llqm/subquery_metadata", &metadata)),
            },
        ))),
    })
}

// ---------------------------------------------------------------------------
// Expression encoding
// ---------------------------------------------------------------------------

fn encode_expr(
    fns: &mut FunctionRegistry,
    expr: &Expr,
    schema: &Schema,
) -> Result<Expression, EncodeError> {
    match expr {
        Expr::Column { table, name } => {
            let index = schema
                .find(table, name)
                .ok_or_else(|| EncodeError::ColumnNotFound {
                    table: table.clone(),
                    name: name.clone(),
                    available: schema.available_columns(),
                })?;
            Ok(make_field_ref(index))
        }
        Expr::Literal(lit) => Ok(Expression {
            rex_type: Some(expression::RexType::Literal(to_substrait_literal(lit))),
        }),
        Expr::Param { name, data_type } => {
            let anchor = fns.ensure("__param");
            Ok(make_scalar_fn(
                anchor,
                vec![
                    make_literal_arg(&LiteralValue::String(name.clone())),
                    make_literal_arg(&LiteralValue::String(data_type.to_string())),
                ],
                to_substrait_type(data_type.clone()),
            ))
        }
        Expr::BinaryOp { op, left, right } => {
            let l = encode_expr(fns, left, schema)?;
            let r = encode_expr(fns, right, schema)?;
            let fn_name = binary_op_substrait_name(*op);
            let anchor = fns.ensure(fn_name);
            Ok(make_scalar_fn(
                anchor,
                vec![make_value_arg(l), make_value_arg(r)],
                bool_type(),
            ))
        }
        Expr::UnaryOp { op, operand } => {
            let inner = encode_expr(fns, operand, schema)?;
            let fn_name = unary_op_substrait_name(*op);
            let anchor = fns.ensure(fn_name);
            Ok(make_scalar_fn(
                anchor,
                vec![make_value_arg(inner)],
                bool_type(),
            ))
        }
        Expr::FuncCall { name, args } => {
            let resolved_args: Vec<FunctionArgument> = args
                .iter()
                .map(|a| Ok(make_value_arg(encode_expr(fns, a, schema)?)))
                .collect::<Result<_, EncodeError>>()?;
            let anchor = fns.ensure(name);
            Ok(make_scalar_fn(anchor, resolved_args, string_type()))
        }
        Expr::Cast { expr, target_type } => {
            let inner = encode_expr(fns, expr, schema)?;
            Ok(Expression {
                rex_type: Some(expression::RexType::Cast(Box::new(expression::Cast {
                    input: Some(Box::new(inner)),
                    r#type: Some(to_substrait_type(target_type.clone())),
                    failure_behavior: expression::cast::FailureBehavior::ThrowException as i32,
                }))),
            })
        }
        Expr::IfThen { ifs, else_expr } => {
            let clauses: Vec<expression::if_then::IfClause> = ifs
                .iter()
                .map(|(cond, then)| {
                    Ok(expression::if_then::IfClause {
                        r#if: Some(encode_expr(fns, cond, schema)?),
                        then: Some(encode_expr(fns, then, schema)?),
                    })
                })
                .collect::<Result<_, EncodeError>>()?;
            let else_resolved = match else_expr {
                Some(e) => Some(Box::new(encode_expr(fns, e, schema)?)),
                None => None,
            };
            Ok(Expression {
                rex_type: Some(expression::RexType::IfThen(Box::new(expression::IfThen {
                    ifs: clauses,
                    r#else: else_resolved,
                }))),
            })
        }
        Expr::InList { expr, list } => {
            let value = encode_expr(fns, expr, schema)?;
            let options: Vec<Expression> = list
                .iter()
                .map(|e| encode_expr(fns, e, schema))
                .collect::<Result<_, _>>()?;
            Ok(Expression {
                rex_type: Some(expression::RexType::SingularOrList(Box::new(
                    expression::SingularOrList {
                        value: Some(Box::new(value)),
                        options,
                    },
                ))),
            })
        }
        Expr::StructField { expr, field } => {
            let inner = encode_expr(fns, expr, schema)?;
            let anchor = fns.ensure("__struct_field");
            Ok(make_scalar_fn(
                anchor,
                vec![
                    make_value_arg(inner),
                    make_literal_arg(&LiteralValue::String(field.clone())),
                ],
                string_type(),
            ))
        }
        Expr::Raw(sql) => {
            let anchor = fns.ensure("__raw_sql");
            Ok(make_scalar_fn(
                anchor,
                vec![make_literal_arg(&LiteralValue::String(sql.clone()))],
                string_type(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Schema collection (delegates to Rel::output_columns)
// ---------------------------------------------------------------------------

fn collect_schema(rel: &v2::Rel) -> Schema {
    Schema {
        columns: rel
            .output_columns()
            .into_iter()
            .map(|(table_alias, name)| SchemaColumn { table_alias, name })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::*;
    use crate::ir::plan::{CteDef, Measure, Rel as V2Rel};

    fn extension_fn_names(plan: &proto::Plan) -> Vec<String> {
        plan.extensions
            .iter()
            .filter_map(|ext| {
                if let Some(
                    proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                        f,
                    ),
                ) = &ext.mapping_type
                {
                    Some(f.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn encode_simple_read() {
        let plan = V2Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);

        if let Some(plan_rel::RelType::Root(root)) = &substrait_plan.relations[0].rel_type {
            assert_eq!(root.names, vec!["id"]);
            assert!(root.input.is_some());
        } else {
            panic!("expected root relation");
        }
    }

    #[test]
    fn encode_filter_and_fetch() {
        let plan = V2Rel::read(
            "gl_project",
            "p",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .filter(col("p", "id").eq(int(42)))
        .project(&[(col("p", "name"), "name")])
        .fetch(10, None)
        .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);

        let fn_names = extension_fn_names(&substrait_plan);
        assert!(
            fn_names.contains(&"equal".into()),
            "functions: {fn_names:?}"
        );
    }

    #[test]
    fn encode_join() {
        let plan = V2Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .join(
                JoinType::Inner,
                V2Rel::read("gl_merge_request", "mr", &[("project_id", DataType::Int64)]),
                col("p", "id").eq(col("mr", "project_id")),
            )
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        let substrait_plan = encode(&plan).unwrap();

        // Walk to find the join
        if let Some(plan_rel::RelType::Root(root)) = &substrait_plan.relations[0].rel_type {
            let input = root.input.as_ref().unwrap();
            // Project → Join
            assert!(
                matches!(input.rel_type, Some(rel::RelType::Project(_))),
                "expected Project, got: {:?}",
                input.rel_type
            );
        }
    }

    #[test]
    fn encode_aggregate() {
        let plan = V2Rel::read(
            "gl_project",
            "p",
            &[("namespace_id", DataType::Int64), ("id", DataType::Int64)],
        )
        .aggregate(
            &[col("p", "namespace_id")],
            &[Measure::new("count", &[col("p", "id")], "cnt")],
        )
        .into_plan();

        let substrait_plan = encode(&plan).unwrap();

        let fn_names = extension_fn_names(&substrait_plan);
        assert!(
            fn_names.contains(&"count".into()),
            "functions: {fn_names:?}"
        );
    }

    #[test]
    fn encode_union_all() {
        let a = V2Rel::read("t1", "a", &[("id", DataType::Int64)])
            .project(&[(col("a", "id"), "start_id")]);
        let b = V2Rel::read("t2", "b", &[("id", DataType::Int64)])
            .project(&[(col("b", "id"), "start_id")]);

        let plan = V2Rel::union_all(vec![a, b], "combined")
            .project(&[(col("combined", "start_id"), "start_id")])
            .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);
    }

    #[test]
    fn encode_sort() {
        let plan = V2Rel::read(
            "gl_project",
            "p",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .sort(&[(col("p", "name"), SortDir::Desc)])
        .project(&[(col("p", "name"), "name")])
        .fetch(10, None)
        .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);
    }

    #[test]
    fn encode_rejects_ctes() {
        let cte_plan = V2Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "node_id")])
            .into_plan();

        let plan = V2Rel::read("base", "b", &[("node_id", DataType::Int64)])
            .project(&[(col("b", "node_id"), "node_id")])
            .into_plan_with_ctes(vec![CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        assert!(encode(&plan).is_err());
    }

    #[test]
    fn encode_complex_expr_variants() {
        let plan = V2Rel::read(
            "t",
            "t",
            &[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("score", DataType::Float64),
            ],
        )
        .filter(
            col("t", "id")
                .gt(int(10))
                .and(col("t", "name").like(string("%test%")))
                .and(col("t", "id").is_not_null()),
        )
        .project(&[
            (col("t", "name"), "name"),
            (col("t", "id").cast(DataType::String), "id_str"),
            (func("upper", vec![col("t", "name")]), "upper_name"),
        ])
        .into_plan();

        let substrait_plan = encode(&plan).unwrap();

        let fn_names = extension_fn_names(&substrait_plan);
        for expected in ["gt", "like", "is_not_null", "upper"] {
            assert!(
                fn_names.contains(&expected.into()),
                "missing {expected}: {fn_names:?}"
            );
        }
    }

    #[test]
    fn encode_subquery() {
        let plan = V2Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .subquery("sq")
            .project(&[(col("sq", "id"), "id")])
            .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);
    }

    #[test]
    fn encode_in_list() {
        let plan = V2Rel::read("t", "t", &[("id", DataType::Int64)])
            .filter(col("t", "id").in_list(vec![int(1), int(2), int(3)]))
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);
    }

    #[test]
    fn encode_if_then() {
        let plan = V2Rel::read(
            "t",
            "t",
            &[("id", DataType::Int64), ("status", DataType::String)],
        )
        .project(&[(
            Expr::IfThen {
                ifs: vec![(col("t", "id").gt(int(0)), string("positive"))],
                else_expr: Some(Box::new(string("non-positive"))),
            },
            "label",
        )])
        .into_plan();

        let substrait_plan = encode(&plan).unwrap();
        assert_eq!(substrait_plan.relations.len(), 1);
    }

    #[test]
    fn encode_column_not_found_gives_helpful_error() {
        let plan = V2Rel::read("t", "t", &[("id", DataType::Int64)])
            .filter(col("t", "nonexistent").eq(int(1)))
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let err = encode(&plan).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nonexistent"), "error: {msg}");
        assert!(
            msg.contains("t.id"),
            "error should list available columns: {msg}"
        );
    }
}
