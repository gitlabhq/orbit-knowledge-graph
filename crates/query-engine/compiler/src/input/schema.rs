use std::sync::OnceLock;

fn pattern(pointer: &str) -> regex::Regex {
    let schema: serde_json::Value = serde_json::from_str(query_data_model::surface::json::SCHEMA)
        .expect("the query schema is JSON");
    let pattern = schema
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("the query schema defines {pointer}"));
    regex::Regex::new(pattern).expect("the query schema patterns are regexes")
}

pub(crate) fn order_by_regex() -> &'static regex::Regex {
    static REGEX: OnceLock<regex::Regex> = OnceLock::new();
    REGEX.get_or_init(|| pattern("/properties/order_by/pattern"))
}

pub(crate) fn aggregation_sort_regex() -> &'static regex::Regex {
    static REGEX: OnceLock<regex::Regex> = OnceLock::new();
    REGEX.get_or_init(|| pattern("/properties/aggregation_sort/pattern"))
}

pub(crate) fn node_ref_regex() -> &'static regex::Regex {
    static REGEX: OnceLock<regex::Regex> = OnceLock::new();
    REGEX.get_or_init(|| pattern("/$defs/NodeOrPropertyRef/pattern"))
}
