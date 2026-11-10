use crate::DataType;

pub fn storage_type(data_type: DataType, nullable: bool) -> String {
    let base = match data_type {
        DataType::String | DataType::Uuid => "String",
        DataType::Enum => return enum_type(nullable),
        DataType::Int => "Int64",
        DataType::Float => "Float64",
        DataType::Bool => "Bool",
        DataType::Date => "Date32",
        DataType::DateTime => "DateTime64(0, 'UTC')",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

fn enum_type(nullable: bool) -> String {
    if nullable {
        "LowCardinality(Nullable(String))".to_string()
    } else {
        "LowCardinality(String)".to_string()
    }
}

pub fn data_type(storage_type: &str) -> Option<DataType> {
    let inner = unwrap(
        "Nullable",
        unwrap("LowCardinality", unwrap("Nullable", storage_type)),
    );
    let name = inner.split('(').next()?.trim();
    match name {
        "String" | "FixedString" => Some(DataType::String),
        "UUID" => Some(DataType::Uuid),
        "Bool" => Some(DataType::Bool),
        "Float32" | "Float64" => Some(DataType::Float),
        "Date" | "Date32" => Some(DataType::Date),
        "DateTime" | "DateTime64" => Some(DataType::DateTime),
        integer if integer.starts_with("Int") || integer.starts_with("UInt") => Some(DataType::Int),
        _ => None,
    }
}

pub fn nullable(storage_type: &str) -> bool {
    unwrap("LowCardinality", storage_type).starts_with("Nullable(")
}

fn unwrap<'a>(wrapper: &str, storage_type: &'a str) -> &'a str {
    storage_type
        .strip_prefix(wrapper)
        .and_then(|rest| rest.strip_prefix('('))
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(storage_type)
}
