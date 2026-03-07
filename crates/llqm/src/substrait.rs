//! Low-level Substrait protobuf construction and reading helpers.
//!
//! Encoding: `DataType`/`Expr` → Substrait protobuf structs (used by `plan.rs`).
//! Decoding: Substrait protobuf → `DataType`/`Schema` (used by `codegen.rs`).

use substrait::proto::{
    self,
    expression::{
        self, field_reference, literal::LiteralType, reference_segment, FieldReference, Literal,
        ReferenceSegment, ScalarFunction,
    },
    extensions as ext, function_argument, join_rel, r#type, Expression, FunctionArgument,
    NamedStruct, ReadRel,
};

use crate::expr::{BinaryOp, DataType, JoinType, LiteralValue, UnaryOp};
use crate::plan::{Schema, SchemaColumn};

// ---------------------------------------------------------------------------
// Metadata construction
// ---------------------------------------------------------------------------

pub fn make_metadata(type_url: &str, json: serde_json::Value) -> ext::AdvancedExtension {
    ext::AdvancedExtension {
        optimization: vec![make_any(type_url, &json)],
        enhancement: None,
    }
}

pub fn make_any(type_url: &str, json: &serde_json::Value) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.into(),
        value: serde_json::to_vec(json).expect("json serialization"),
    }
}

pub fn build_named_struct(columns: &[(&str, DataType)]) -> NamedStruct {
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

// ---------------------------------------------------------------------------
// Expression construction
// ---------------------------------------------------------------------------

pub fn make_field_ref(index: usize) -> Expression {
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

pub fn to_substrait_literal(lit: &LiteralValue) -> Literal {
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

pub fn make_literal_arg(lit: &LiteralValue) -> FunctionArgument {
    FunctionArgument {
        arg_type: Some(function_argument::ArgType::Value(Expression {
            rex_type: Some(expression::RexType::Literal(to_substrait_literal(lit))),
        })),
    }
}

pub fn make_value_arg(expr: Expression) -> FunctionArgument {
    FunctionArgument {
        arg_type: Some(function_argument::ArgType::Value(expr)),
    }
}

#[allow(deprecated)] // ScalarFunction.args is deprecated
pub fn make_scalar_fn(
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

// ---------------------------------------------------------------------------
// Type conversion
// ---------------------------------------------------------------------------

pub fn to_substrait_type(dt: DataType) -> proto::Type {
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

pub fn bool_type() -> proto::Type {
    to_substrait_type(DataType::Bool)
}

pub fn string_type() -> proto::Type {
    to_substrait_type(DataType::String)
}

pub fn substrait_type_to_data_type(t: &proto::Type) -> Option<DataType> {
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

// ---------------------------------------------------------------------------
// Join / operator name mapping
// ---------------------------------------------------------------------------

pub fn to_substrait_join_type(jt: JoinType) -> join_rel::JoinType {
    match jt {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::Cross => join_rel::JoinType::Inner,
    }
}

pub fn binary_op_substrait_name(op: BinaryOp) -> &'static str {
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
        BinaryOp::Like => "like",
        BinaryOp::ILike => "ilike",
        BinaryOp::In => "in",
    }
}

pub fn unary_op_substrait_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
        UnaryOp::IsNull => "is_null",
        UnaryOp::IsNotNull => "is_not_null",
    }
}

// ---------------------------------------------------------------------------
// Metadata reading
// ---------------------------------------------------------------------------

pub fn get_read_metadata(read: &ReadRel) -> Option<serde_json::Value> {
    let ext = read.advanced_extension.as_ref()?;
    let any = ext.optimization.first()?;
    if any.type_url != "llqm/read_metadata" {
        return None;
    }
    serde_json::from_slice(&any.value).ok()
}

pub fn schema_from_base(base: &proto::NamedStruct, alias: &str) -> Schema {
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
