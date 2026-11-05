use super::render::DuckDbSql;
use crate::implementations::relational::plan::{Expr, Function};
use crate::implementations::relational::render::Renderer;

pub(super) fn call(
    renderer: &mut Renderer<'_, DuckDbSql>,
    function: Function,
    args: &[Expr],
) -> String {
    let mut arg = |index: usize| renderer.expr(&args[index]);
    match function {
        Function::Tuple => {
            let fields: Vec<_> = (0..args.len())
                .map(|index| format!("_{} := {}", index + 1, arg(index)))
                .collect();
            format!("struct_pack({})", fields.join(", "))
        }
        Function::If => {
            let (condition, then, otherwise) = (arg(0), arg(1), arg(2));
            format!("CASE WHEN {condition} THEN {then} ELSE {otherwise} END")
        }
        Function::ToString => format!("CAST({} AS VARCHAR)", arg(0)),
        Function::ToJson => format!("to_json({})", arg(0)),
        Function::ArrayFilter | Function::ArrayMap | Function::ArrayExists => {
            let (predicate, array) = (arg(0), arg(1));
            match function {
                Function::ArrayFilter => format!("list_filter({array}, {predicate})"),
                Function::ArrayMap => format!("list_transform({array}, {predicate})"),
                _ => format!("(len(list_filter({array}, {predicate})) > 0)"),
            }
        }
        Function::ByteLength => format!("octet_length(encode({}))", arg(0)),
        Function::CountSubstrings => {
            let (text, separator) = (arg(0), arg(1));
            format!("(len(string_split({text}, {separator})) - 1)")
        }
        function => format!("{}({})", name(function), renderer.list(args)),
    }
}

fn name(function: Function) -> &'static str {
    match function {
        Function::StartsWith => "starts_with",
        Function::EndsWith => "ends_with",
        Function::Lower => "lower",
        Function::Substring => "substring",
        Function::ArrayContains => "list_contains",
        Function::ArrayContainsAny => "list_has_any",
        Function::ArrayContainsAll => "list_has_all",
        Function::Array => "list_value",
        Function::ArrayConcat => "list_concat",
        Function::ArrayReverse => "list_reverse",
        Function::ArrayResize => "list_resize",
        Function::Object => "json_object",
        Function::Unnest => "unnest",
        Function::TupleElement => "struct_extract_at",
        Function::Coalesce => "coalesce",
        Function::NullIf => "nullif",
        Function::Concat => "concat",
        Function::Tuple
        | Function::If
        | Function::ToString
        | Function::ToJson
        | Function::ArrayFilter
        | Function::ArrayMap
        | Function::ArrayExists
        | Function::ByteLength
        | Function::CountSubstrings => unreachable!("{function:?} renders as a special form"),
    }
}
