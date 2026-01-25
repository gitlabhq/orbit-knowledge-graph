use parser_core::kotlin::types::KotlinImportType;

use crate::analysis::types::{ImportType, ImportedSymbolNode};

// Imports

/// Returns the name of the imported symbol and the full import path.
pub(crate) fn full_import_path(import: &ImportedSymbolNode) -> (String, String) {
    let name = match import.import_type {
        ImportType::Kotlin(KotlinImportType::Import) => import
            .identifier
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_default(),
        ImportType::Kotlin(KotlinImportType::AliasedImport) => import
            .identifier
            .as_ref()
            .map(|i| i.alias.clone().unwrap_or_else(|| i.name.clone()))
            .unwrap_or_default(),
        _ => return (String::new(), import.import_path.clone()),
    };

    (name.clone(), format!("{}.{}", import.import_path, name))
}

// Expressions

pub(crate) fn get_unary_operator_function(operator: &str) -> Option<String> {
    let result = match operator {
        "+" => "unaryPlus",
        "++" => "inc",
        "-" => "unaryMinus",
        "--" => "dec",
        "!" => "not",
        "[]" => "get",
        _ => return None,
    };

    Some(result.to_string())
}

pub(crate) fn get_binary_operator_function(operator: &str) -> Option<String> {
    let result = match operator {
        "+" => "plus",
        "-" => "minus",
        "*" => "times",
        "/" => "div",
        "%" => "rem",
        ".." => "rangeTo",
        "..<" => "rangeUntil",
        "in" | "!in" => "contains",
        "+=" => "plusAssign",
        "-=" => "minusAssign",
        "*=" => "timesAssign",
        "/=" => "divAssign",
        "%=" => "remAssign",
        "==" | "!=" => "equals",
        ">" | "<" | ">=" | "<=" => "compareTo",
        _ => return None,
    };

    Some(result.to_string())
}
