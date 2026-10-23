use std::collections::BTreeMap;
use std::path::PathBuf;

use pest_meta::ast::{Expr, Rule};
use pest_meta::parser::{self, Rule as MetaRule};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryContract {
    limits: Limits,
    filter_operators: BTreeMap<String, Vec<String>>,
    aggregations: BTreeMap<String, Vec<String>>,
    ordering: Vec<String>,
    path_types: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_hops: u32,
    max_depth: u32,
    max_nodes: usize,
    max_relationships: usize,
    max_node_ids: usize,
    max_in_values: usize,
    max_node_filters: usize,
    max_relationship_filters: usize,
    max_columns: usize,
    max_relationship_types: usize,
    max_aggregations: usize,
    max_group_by_keys: usize,
    max_filter_entries_per_property: usize,
    max_predicates: usize,
    max_limit: u32,
    max_skip: u32,
    max_identifier_length: usize,
    max_filter_string_length: usize,
    max_predicate_depth: usize,
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/passes/frontend/gql/query.pest");

    let contract_path = env_path("CONFIG_DIR").join("query_contract.yaml");
    let schema_path = env_path("SCHEMA_DIR").join("graph_query.schema.json");
    println!("cargo:rerun-if-changed={}", contract_path.display());
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let contract: QueryContract =
        serde_saphyr::from_str(&read(&contract_path)).unwrap_or_else(|error| {
            panic!(
                "{} is not valid contract YAML: {error}",
                contract_path.display()
            )
        });
    validate_contract(&contract);

    let schema: Value = serde_json::from_str(&read(&schema_path))
        .unwrap_or_else(|error| panic!("{} is not valid JSON: {error}", schema_path.display()));
    check_schema(&schema, &contract);

    let grammar_path = PathBuf::from("src/passes/frontend/gql/query.pest");
    let grammar = parse_grammar(&read(&grammar_path));
    check_grammar(&grammar, &contract.limits);

    let generated = generate_rust(&contract);
    std::fs::write(env_path("OUT_DIR").join("query_contract.rs"), generated)
        .expect("write generated query contract");
}

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be set via .cargo/config.toml [env]")),
    )
}

fn read(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn validate_contract(contract: &QueryContract) {
    const DATA_TYPES: &[&str] = &[
        "string", "integer", "number", "boolean", "date", "datetime", "enum", "uuid",
    ];
    let operations = contract
        .filter_operators
        .iter()
        .chain(contract.aggregations.iter())
        .map(|(operation, types)| (operation.as_str(), types))
        .chain(std::iter::once(("ordering", &contract.ordering)));
    for (operation, types) in operations {
        for data_type in types {
            assert!(
                DATA_TYPES.contains(&data_type.as_str()),
                "query contract operation `{operation}` has unknown data type `{data_type}`"
            );
        }
    }
    assert_eq!(
        contract.limits.max_node_ids, contract.limits.max_in_values,
        "GQL general and IN lists share one bound"
    );
}

fn check_schema(schema: &Value, contract: &QueryContract) {
    let limits = &contract.limits;
    check_object_keys(
        schema,
        "/$defs/OperatorFilter/properties",
        contract.filter_operators.keys(),
    );
    check_enum(schema, "/$defs/PathType/enum", &contract.path_types);
    check_aggregation_keys(schema, contract.aggregations.keys());

    check_maximum(
        schema,
        "/$defs/RelationshipSelector/properties/hops/items",
        limits.max_hops,
    );
    check_maximum(
        schema,
        "/$defs/PathConfig/properties/max_depth",
        limits.max_depth,
    );
    check_maximum(schema, "/properties/limit", limits.max_limit);
    check_max_length(schema, "/$defs/Identifier", limits.max_identifier_length);
    check_max_length(
        schema,
        "/$defs/SearchPattern",
        limits.max_filter_string_length,
    );
    check_string_branch_max_length(
        schema,
        "/$defs/FilterValue/oneOf",
        limits.max_filter_string_length,
    );

    check_max_items(schema, "/properties/nodes", limits.max_nodes);
    check_max_items(
        schema,
        "/properties/relationships",
        limits.max_relationships,
    );
    check_max_items(schema, "/properties/aggregations", limits.max_aggregations);
    check_max_items(schema, "/properties/group_by", limits.max_group_by_keys);
    check_max_items(
        schema,
        "/$defs/NodeSelector/properties/node_ids",
        limits.max_node_ids,
    );
    check_max_properties(
        schema,
        "/$defs/NodeSelector/properties/filters",
        limits.max_node_filters,
    );
    check_max_properties(
        schema,
        "/$defs/RelationshipSelector/properties/filters",
        limits.max_relationship_filters,
    );
    check_array_branch_max_items(schema, "/$defs/ColumnSelection/oneOf", limits.max_columns);
    check_array_branch_max_items(
        schema,
        "/$defs/RelationshipType/oneOf",
        limits.max_relationship_types,
    );
    check_max_items(
        schema,
        "/$defs/PathConfig/properties/rel_types",
        limits.max_relationship_types,
    );
    check_max_items(
        schema,
        "/$defs/NeighborsConfig/properties/rel_types",
        limits.max_relationship_types,
    );
    check_array_branch_max_items(schema, "/$defs/FilterValue/oneOf", limits.max_in_values);
    check_max_items(
        schema,
        "/$defs/OperatorFilter/properties/in",
        limits.max_in_values,
    );
    check_array_branch_max_items(
        schema,
        "/$defs/FilterEntry/oneOf",
        limits.max_filter_entries_per_property,
    );
}

fn check_grammar(grammar: &[Rule], limits: &Limits) {
    for (rule, repeated_rule, maximum) in [
        (
            "UnescapedSymbolicName",
            "IdentifierPart",
            limits.max_identifier_length,
        ),
        (
            "EscapedSymbolicName",
            "IdentifierPart",
            limits.max_identifier_length,
        ),
        ("NodeProjection", "ProjectionProperty", limits.max_columns),
        (
            "RelationshipTypes",
            "SchemaName",
            limits.max_relationship_types,
        ),
        ("InListLiteral", "Value", limits.max_in_values),
        ("ListLiteral", "Value", limits.max_node_ids),
        (
            "PatternElement",
            "PatternElementChain",
            limits.max_relationships,
        ),
        ("AndExpression", "NotExpression", limits.max_predicates),
        ("NodeMapLiteral", "MapEntry", limits.max_node_filters),
        (
            "RelationshipMapLiteral",
            "MapEntry",
            limits.max_relationship_filters,
        ),
        ("NotExpression", "Negation", limits.max_predicate_depth + 1),
    ] {
        assert_eq!(
            repetition_max(grammar_rule(grammar, rule), repeated_rule),
            maximum,
            "GQL grammar rule `{rule}` must match query_contract.yaml"
        );
    }

    for (rule, limit) in [
        ("HopCount", limits.max_hops),
        ("LimitCount", limits.max_limit),
        ("SkipCount", limits.max_skip),
    ] {
        assert_eq!(
            numeric_max(grammar_rule(grammar, rule)),
            limit + 1,
            "GQL grammar rule `{rule}` must include one overflow sentinel"
        );
    }
}

fn parse_grammar(source: &str) -> Vec<Rule> {
    let pairs = parser::parse(MetaRule::grammar_rules, source)
        .unwrap_or_else(|error| panic!("invalid GQL grammar: {error}"));
    parser::consume_rules(pairs).unwrap_or_else(|errors| {
        panic!(
            "invalid GQL grammar: {}",
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        )
    })
}

fn grammar_rule<'a>(grammar: &'a [Rule], name: &str) -> &'a Rule {
    grammar
        .iter()
        .find(|rule| rule.name == name)
        .unwrap_or_else(|| panic!("GQL grammar is missing rule `{name}`"))
}

fn repetition_max(rule: &Rule, repeated_rule: &str) -> usize {
    fn find(expr: &Expr, repeated_rule: &str, found: &mut Vec<usize>) {
        match expr {
            Expr::RepMinMax(inner, _, maximum) if contains_ident(inner, repeated_rule) => {
                found.push(*maximum as usize);
            }
            Expr::Seq(left, right) | Expr::Choice(left, right) => {
                find(left, repeated_rule, found);
                find(right, repeated_rule, found);
            }
            Expr::Opt(inner)
            | Expr::Rep(inner)
            | Expr::RepOnce(inner)
            | Expr::RepExact(inner, _)
            | Expr::RepMin(inner, _)
            | Expr::RepMax(inner, _)
            | Expr::RepMinMax(inner, _, _)
            | Expr::NegPred(inner)
            | Expr::PosPred(inner) => find(inner, repeated_rule, found),
            _ => {}
        }
    }

    let mut found = Vec::new();
    find(&rule.expr, repeated_rule, &mut found);
    let [maximum] = found.as_slice() else {
        panic!(
            "GQL grammar rule `{}` must have one bounded repetition of `{repeated_rule}`",
            rule.name
        );
    };
    *maximum
}

fn contains_ident(expr: &Expr, expected: &str) -> bool {
    match expr {
        Expr::Ident(name) => name == expected,
        Expr::Seq(left, right) | Expr::Choice(left, right) => {
            contains_ident(left, expected) || contains_ident(right, expected)
        }
        Expr::Opt(inner)
        | Expr::Rep(inner)
        | Expr::RepOnce(inner)
        | Expr::RepExact(inner, _)
        | Expr::RepMin(inner, _)
        | Expr::RepMax(inner, _)
        | Expr::RepMinMax(inner, _, _)
        | Expr::NegPred(inner)
        | Expr::PosPred(inner) => contains_ident(inner, expected),
        _ => false,
    }
}

fn numeric_max(rule: &Rule) -> u32 {
    fn visit(expr: &Expr, maximum: &mut u32) {
        match expr {
            Expr::Str(value) => {
                if let Ok(value) = value.parse() {
                    *maximum = (*maximum).max(value);
                }
            }
            Expr::Seq(left, right) | Expr::Choice(left, right) => {
                visit(left, maximum);
                visit(right, maximum);
            }
            Expr::Opt(inner)
            | Expr::Rep(inner)
            | Expr::RepOnce(inner)
            | Expr::RepExact(inner, _)
            | Expr::RepMin(inner, _)
            | Expr::RepMax(inner, _)
            | Expr::RepMinMax(inner, _, _)
            | Expr::NegPred(inner)
            | Expr::PosPred(inner) => visit(inner, maximum),
            _ => {}
        }
    }

    let mut maximum = 0;
    visit(&rule.expr, &mut maximum);
    maximum
}

fn generate_rust(contract: &QueryContract) -> String {
    let limits = &contract.limits;
    let mut output = format!(
        "pub const MAX_HOPS_CAP: u32 = {};\n\
         pub const MAX_DEPTH_CAP: u32 = {};\n\
         pub const MAX_NODES_CAP: usize = {};\n\
         pub const MAX_RELS_CAP: usize = {};\n\
         pub const MAX_NODE_IDS: usize = {};\n\
         pub const MAX_IN_VALUES: usize = {};\n\
         pub const MAX_FILTERS_PER_NODE: usize = {};\n\
         pub const MAX_FILTERS_PER_REL: usize = {};\n\
         pub const MAX_COLUMNS: usize = {};\n\
         pub const MAX_REL_TYPES: usize = {};\n\
         pub const MAX_AGGREGATIONS: usize = {};\n\
         pub const MAX_GROUP_BY_KEYS: usize = {};\n\
         pub const MAX_FILTER_ENTRIES_PER_PROPERTY: usize = {};\n\
         pub const MAX_PREDICATES: usize = {};\n\
         pub const MAX_LIMIT: u32 = {};\n\
         pub const MAX_SKIP: u32 = {};\n\
         pub const MAX_IDENTIFIER_LEN: usize = {};\n\
         pub const MAX_FILTER_STRING_LEN: usize = {};\n\
         pub const MAX_PREDICATE_DEPTH: usize = {};\n\n",
        limits.max_hops,
        limits.max_depth,
        limits.max_nodes,
        limits.max_relationships,
        limits.max_node_ids,
        limits.max_in_values,
        limits.max_node_filters,
        limits.max_relationship_filters,
        limits.max_columns,
        limits.max_relationship_types,
        limits.max_aggregations,
        limits.max_group_by_keys,
        limits.max_filter_entries_per_property,
        limits.max_predicates,
        limits.max_limit,
        limits.max_skip,
        limits.max_identifier_length,
        limits.max_filter_string_length,
        limits.max_predicate_depth,
    );

    let path_types = contract
        .path_types
        .iter()
        .map(|path_type| format!("\"{path_type}\""))
        .collect::<Vec<_>>()
        .join(", ");
    output.push_str(&format!(
        "#[cfg(test)]\npub const PATH_TYPES: &[&str] = &[{path_types}];\n"
    ));

    output.push_str("\npub fn filter_operator_accepts(op: crate::input::FilterOp, data_type: ontology::DataType) -> bool {\n    match op {\n");
    for (operation, types) in &contract.filter_operators {
        output.push_str(&format!(
            "        crate::input::FilterOp::{} => {},\n",
            filter_variant(operation),
            type_match(types)
        ));
    }
    output.push_str("    }\n}\n\n");

    output.push_str("pub fn aggregation_accepts(function: crate::input::AggFunction, data_type: ontology::DataType) -> bool {\n    match function {\n");
    for (aggregation, types) in &contract.aggregations {
        output.push_str(&format!(
            "        crate::input::AggFunction::{} => {},\n",
            aggregation_variant(aggregation),
            type_match(types)
        ));
    }
    output.push_str("    }\n}\n\n");
    output.push_str(&format!(
        "pub fn ordering_accepts(data_type: ontology::DataType) -> bool {{\n    {}\n}}\n",
        type_match(&contract.ordering)
    ));
    output
}

fn type_match(types: &[String]) -> String {
    if types.is_empty() {
        return "false".into();
    }
    let variants = types
        .iter()
        .map(|data_type| match data_type.as_str() {
            "string" => "ontology::DataType::String",
            "integer" => "ontology::DataType::Int",
            "number" => "ontology::DataType::Float",
            "boolean" => "ontology::DataType::Bool",
            "date" => "ontology::DataType::Date",
            "datetime" => "ontology::DataType::DateTime",
            "enum" => "ontology::DataType::Enum",
            "uuid" => "ontology::DataType::Uuid",
            other => panic!("unknown data type `{other}`"),
        })
        .collect::<Vec<_>>()
        .join(" | ");
    format!("matches!(data_type, {variants})")
}

fn filter_variant(operation: &str) -> &str {
    match operation {
        "eq" => "Eq",
        "ne" => "Ne",
        "gt" => "Gt",
        "lt" => "Lt",
        "gte" => "Gte",
        "lte" => "Lte",
        "in" => "In",
        "contains" => "Contains",
        "starts_with" => "StartsWith",
        "ends_with" => "EndsWith",
        "is_null" => "IsNull",
        "is_not_null" => "IsNotNull",
        "token_match" => "TokenMatch",
        "all_tokens" => "AllTokens",
        "any_tokens" => "AnyTokens",
        other => panic!("unknown filter operator `{other}`"),
    }
}

fn aggregation_variant(aggregation: &str) -> &str {
    match aggregation {
        "count" => "Count",
        "sum" => "Sum",
        "avg" => "Avg",
        "min" => "Min",
        "max" => "Max",
        "collect" => "Collect",
        other => panic!("unknown aggregation `{other}`"),
    }
}

fn check_aggregation_keys<'a>(schema: &Value, expected: impl Iterator<Item = &'a String>) {
    let mut properties: Vec<&str> = object(schema, "/$defs/Aggregation/properties")
        .keys()
        .map(String::as_str)
        .filter(|key| *key != "as")
        .collect();
    let mut expected: Vec<&str> = expected.map(String::as_str).collect();
    properties.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        properties, expected,
        "aggregation functions must match query_contract.yaml"
    );

    let mut required = array(schema, "/$defs/Aggregation/oneOf")
        .iter()
        .map(|branch| {
            let required = branch
                .get("required")
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("aggregation oneOf branch must have required"));
            let [function] = required.as_slice() else {
                panic!("aggregation oneOf branch must require one function");
            };
            function
                .as_str()
                .unwrap_or_else(|| panic!("aggregation required value must be a string"))
        })
        .collect::<Vec<_>>();
    required.sort_unstable();
    assert_eq!(
        required, expected,
        "aggregation required branches must match query_contract.yaml"
    );
}

fn check_object_keys<'a>(
    schema: &Value,
    pointer: &str,
    expected: impl Iterator<Item = &'a String>,
) {
    let mut actual: Vec<&str> = object(schema, pointer).keys().map(String::as_str).collect();
    let mut expected: Vec<&str> = expected.map(String::as_str).collect();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "`{pointer}` must match query_contract.yaml"
    );
}

fn check_enum(schema: &Value, pointer: &str, expected: &[String]) {
    let actual = array(schema, pointer)
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("`{pointer}` contains a non-string"))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "`{pointer}` must match query_contract.yaml"
    );
}

fn check_maximum(schema: &Value, pointer: &str, expected: u32) {
    check_unsigned(schema, &format!("{pointer}/maximum"), u64::from(expected));
}

fn check_max_items(schema: &Value, pointer: &str, expected: usize) {
    check_unsigned(schema, &format!("{pointer}/maxItems"), expected as u64);
}

fn check_max_length(schema: &Value, pointer: &str, expected: usize) {
    check_unsigned(schema, &format!("{pointer}/maxLength"), expected as u64);
}

fn check_max_properties(schema: &Value, pointer: &str, expected: usize) {
    check_unsigned(schema, &format!("{pointer}/maxProperties"), expected as u64);
}

fn check_unsigned(schema: &Value, pointer: &str, expected: u64) {
    let actual = schema
        .pointer(pointer)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing integer `{pointer}`"));
    assert_eq!(
        actual, expected,
        "`{pointer}` must match query_contract.yaml"
    );
}

fn check_array_branch_max_items(schema: &Value, pointer: &str, expected: usize) {
    let branches = array(schema, pointer);
    let matches = branches
        .iter()
        .filter(|branch| branch.get("type").and_then(Value::as_str) == Some("array"))
        .collect::<Vec<_>>();
    let [branch] = matches.as_slice() else {
        panic!("`{pointer}` must have one array branch");
    };
    assert_eq!(
        branch.get("maxItems").and_then(Value::as_u64),
        Some(expected as u64),
        "array branch under `{pointer}` must match query_contract.yaml"
    );
}

fn check_string_branch_max_length(schema: &Value, pointer: &str, expected: usize) {
    let branches = array(schema, pointer);
    let matches = branches
        .iter()
        .filter(|branch| branch.get("type").and_then(Value::as_str) == Some("string"))
        .collect::<Vec<_>>();
    let [branch] = matches.as_slice() else {
        panic!("`{pointer}` must have one string branch");
    };
    assert_eq!(
        branch.get("maxLength").and_then(Value::as_u64),
        Some(expected as u64),
        "string branch under `{pointer}` must match query_contract.yaml"
    );
}

fn object<'a>(schema: &'a Value, pointer: &str) -> &'a serde_json::Map<String, Value> {
    schema
        .pointer(pointer)
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing object `{pointer}`"))
}

fn array<'a>(schema: &'a Value, pointer: &str) -> &'a Vec<Value> {
    schema
        .pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing array `{pointer}`"))
}
