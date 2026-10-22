//! Checks that limits expressed in the JSON schema match the shared query limits.

use std::path::PathBuf;

use serde_json::Value;

include!("src/query_limits.rs");
include!("src/passes/frontend/json_dsl/schema_contract.rs");

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/query_limits.rs");
    println!("cargo:rerun-if-changed=src/passes/frontend/json_dsl/schema_contract.rs");
    println!("cargo:rerun-if-changed=src/passes/frontend/gql/query.pest");

    let schema_dir = PathBuf::from(std::env::var("SCHEMA_DIR").unwrap_or_else(|_| {
        panic!("SCHEMA_DIR must be set via .cargo/config.toml [env] when building the compiler")
    }));
    let schema_path = schema_dir.join("graph_query.schema.json");
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let raw = std::fs::read_to_string(&schema_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", schema_path.display()));
    let schema: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", schema_path.display()));

    check_object_keys(
        &schema,
        "/$defs/OperatorFilter/properties",
        "FilterOp",
        EXPECTED_FILTER_OPS,
    );
    check_enum(
        &schema,
        "/$defs/PathType/enum",
        "PathType",
        EXPECTED_PATH_TYPES,
    );

    check_maximum(
        &schema,
        "/$defs/RelationshipSelector/properties/hops/items",
        MAX_HOPS_CAP,
    );
    check_maximum(
        &schema,
        "/$defs/PathConfig/properties/max_depth",
        MAX_DEPTH_CAP,
    );
    check_maximum(&schema, "/properties/limit", MAX_LIMIT);
    check_max_length(&schema, "/$defs/Identifier", MAX_IDENTIFIER_LEN);
    check_max_length(&schema, "/$defs/SearchPattern", MAX_FILTER_STRING_LEN);
    check_string_branch_max_length(&schema, "/$defs/FilterValue/oneOf", MAX_FILTER_STRING_LEN);

    check_max_items(&schema, "/properties/nodes", MAX_NODES_CAP);
    check_max_items(&schema, "/properties/relationships", MAX_RELS_CAP);
    check_max_items(&schema, "/properties/aggregations", MAX_AGGREGATIONS);
    check_max_items(&schema, "/properties/group_by", MAX_GROUP_BY_KEYS);
    check_max_items(
        &schema,
        "/$defs/NodeSelector/properties/node_ids",
        MAX_NODE_IDS,
    );

    check_max_properties(
        &schema,
        "/$defs/NodeSelector/properties/filters",
        MAX_FILTERS_PER_NODE,
    );
    check_max_properties(
        &schema,
        "/$defs/RelationshipSelector/properties/filters",
        MAX_FILTERS_PER_REL,
    );

    check_array_branch_max_items(&schema, "/$defs/ColumnSelection/oneOf", MAX_COLUMNS);

    check_array_branch_max_items(&schema, "/$defs/RelationshipType/oneOf", MAX_REL_TYPES);
    check_max_items(
        &schema,
        "/$defs/PathConfig/properties/rel_types",
        MAX_REL_TYPES,
    );
    check_max_items(
        &schema,
        "/$defs/NeighborsConfig/properties/rel_types",
        MAX_REL_TYPES,
    );

    check_array_branch_max_items(&schema, "/$defs/FilterValue/oneOf", MAX_IN_VALUES);
    check_max_items(
        &schema,
        "/$defs/OperatorFilter/properties/in",
        MAX_IN_VALUES,
    );

    check_array_branch_max_items(
        &schema,
        "/$defs/FilterEntry/oneOf",
        MAX_FILTER_ENTRIES_PER_PROPERTY,
    );

    let grammar =
        std::fs::read_to_string("src/passes/frontend/gql/query.pest").expect("read GQL grammar");
    check_grammar_bound(
        &grammar,
        "UnescapedSymbolicName",
        &format!("IdentifierPart{{0,{}}}", MAX_IDENTIFIER_LEN - 1),
    );
    check_grammar_bound(
        &grammar,
        "EscapedSymbolicName",
        &format!("IdentifierPart{{0,{}}}", MAX_IDENTIFIER_LEN - 1),
    );
    check_grammar_bound(
        &grammar,
        "NodeProjection",
        &format!("ProjectionProperty){{0,{}}}", MAX_COLUMNS - 1),
    );
    check_grammar_bound(
        &grammar,
        "RelationshipTypes",
        &format!("SchemaName){{0,{}}}", MAX_REL_TYPES - 1),
    );
    check_grammar_bound(
        &grammar,
        "InListLiteral",
        &format!("Value){{0,{}}}", MAX_IN_VALUES - 1),
    );
    check_grammar_bound(
        &grammar,
        "ListLiteral",
        &format!("Value){{0,{}}}", MAX_NODE_IDS - 1),
    );
    check_grammar_bound(
        &grammar,
        "PatternElement",
        &format!("PatternElementChain){{0,{MAX_RELS_CAP}}}"),
    );
    check_grammar_bound(
        &grammar,
        "AndExpression",
        &format!("NotExpression){{0,{MAX_PREDICATES}}}"),
    );
    check_grammar_bound(
        &grammar,
        "NodeMapLiteral",
        &format!("MapEntry){{0,{}}}", MAX_FILTERS_PER_NODE - 1),
    );
    check_grammar_bound(
        &grammar,
        "RelationshipMapLiteral",
        &format!("MapEntry){{0,{}}}", MAX_FILTERS_PER_REL - 1),
    );
    check_grammar_bound(
        &grammar,
        "NotExpression",
        &format!("Negation{{0,{MAX_PREDICATE_DEPTH}}}"),
    );
    check_grammar_numeric_max(&grammar, "HopCount", MAX_HOPS_CAP);
    check_grammar_numeric_max(&grammar, "LimitCount", MAX_LIMIT);
    check_grammar_numeric_max(&grammar, "SkipCount", MAX_SKIP);
    assert_eq!(
        MAX_NODE_IDS, MAX_IN_VALUES,
        "GQL ListLiteral and InListLiteral share the Value bound; keep MAX_NODE_IDS and MAX_IN_VALUES equal"
    );
}

fn check_grammar_bound(grammar: &str, rule: &str, bound: &str) {
    let production = grammar
        .lines()
        .find(|line| line.starts_with(&format!("{rule} =")))
        .unwrap_or_else(|| panic!("GQL grammar is missing rule `{rule}`"));
    assert!(
        production.contains(bound),
        "GQL grammar rule `{rule}` must contain `{bound}` to match query_limits.rs"
    );
}

fn check_grammar_numeric_max(grammar: &str, rule: &str, expected: u32) {
    let production = grammar
        .lines()
        .find(|line| line.starts_with(&format!("{rule} =")))
        .unwrap_or_else(|| panic!("GQL grammar is missing rule `{rule}`"));
    let maximum = production
        .split('"')
        .nth(1)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| panic!("GQL grammar rule `{rule}` must start with its numeric maximum"));
    assert_eq!(
        maximum, expected,
        "GQL grammar rule `{rule}` maximum must match query_limits.rs"
    );
}

// Locates the array branch by content, not index, so branch reordering can't skip it.
fn check_array_branch_max_items(schema: &Value, ptr: &str, expected: usize) {
    let branches = schema
        .pointer(ptr)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing array `{ptr}`"));

    let array_branches: Vec<&Value> = branches
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("array"))
        .collect();

    let [branch] = array_branches.as_slice() else {
        panic!(
            "`{ptr}` must have exactly one `type: array` branch to guard, found {}",
            array_branches.len()
        );
    };

    let actual = branch
        .get("maxItems")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("array branch under `{ptr}` is missing integer `maxItems`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: array branch under `{ptr}` maxItems = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}

fn check_enum(schema: &Value, ptr: &str, def: &str, expected: &[&str]) {
    let actual = schema
        .pointer(ptr)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing `{ptr}`"));

    let actual: Vec<&str> = actual
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("`{ptr}` contains a non-string enum value: {v}"))
        })
        .collect();
    assert_sorted_eq(ptr, def, actual, expected);
}

fn check_object_keys(schema: &Value, ptr: &str, def: &str, expected: &[&str]) {
    let actual = schema
        .pointer(ptr)
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing object `{ptr}`"));
    let actual: Vec<&str> = actual.keys().map(String::as_str).collect();
    assert_sorted_eq(ptr, def, actual, expected);
}

fn assert_sorted_eq(ptr: &str, def: &str, mut actual: Vec<&str>, expected: &[&str]) {
    let mut expected: Vec<&str> = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();

    assert_eq!(
        actual, expected,
        "DRIFT: `{ptr}` = {actual:?} but compiler `{def}` = {expected:?}. \
         Update either the schema or src/passes/frontend/json_dsl/schema_contract.rs so they match."
    );
}

fn check_maximum(schema: &Value, ptr: &str, expected: u32) {
    let field = format!("{ptr}/maximum");
    let actual = schema
        .pointer(&field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing integer `{field}`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: `{field}` = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}

fn check_max_items(schema: &Value, ptr: &str, expected: usize) {
    let field = format!("{ptr}/maxItems");
    let actual = schema
        .pointer(&field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing integer `{field}`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: `{field}` = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}

fn check_max_length(schema: &Value, ptr: &str, expected: usize) {
    let field = format!("{ptr}/maxLength");
    let actual = schema
        .pointer(&field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing integer `{field}`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: `{field}` = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}

fn check_string_branch_max_length(schema: &Value, ptr: &str, expected: usize) {
    let branches = schema
        .pointer(ptr)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing array `{ptr}`"));
    let string_branches: Vec<&Value> = branches
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("string"))
        .collect();
    let [branch] = string_branches.as_slice() else {
        panic!(
            "`{ptr}` must have exactly one `type: string` branch to guard, found {}",
            string_branches.len()
        );
    };
    let actual = branch
        .get("maxLength")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("string branch under `{ptr}` is missing integer `maxLength`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: string branch under `{ptr}` maxLength = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}

fn check_max_properties(schema: &Value, ptr: &str, expected: usize) {
    let field = format!("{ptr}/maxProperties");
    let actual = schema
        .pointer(&field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing integer `{field}`"));
    assert_eq!(
        actual, expected as u64,
        "DRIFT: `{field}` = {actual} but compiler cap = {expected}. \
         Update either the schema or src/query_limits.rs so they match."
    );
}
