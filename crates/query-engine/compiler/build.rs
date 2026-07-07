//! Fails the build if `config/schemas/graph_query.schema.json` and the mirrors
//! in `src/schema_limits.rs` drift. Prior art: `crates/gkg-analytics/build.rs`.

use std::path::PathBuf;

use serde_json::Value;

include!("src/schema_limits.rs");

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/schema_limits.rs");

    let schema_dir = PathBuf::from(std::env::var("SCHEMA_DIR").unwrap_or_else(|_| {
        panic!("SCHEMA_DIR must be set via .cargo/config.toml [env] when building the compiler")
    }));
    let schema_path = schema_dir.join("graph_query.schema.json");
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let raw = std::fs::read_to_string(&schema_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", schema_path.display()));
    let schema: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", schema_path.display()));

    check_enum(
        &schema,
        "/$defs/PropertyFilter/properties/op/enum",
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
        "/$defs/RelationshipSelector/properties/max_hops",
        MAX_HOPS_CAP,
    );
    check_maximum(
        &schema,
        "/$defs/RelationshipSelector/properties/min_hops",
        MAX_HOPS_CAP,
    );
    check_maximum(
        &schema,
        "/$defs/PathConfig/properties/max_depth",
        MAX_DEPTH_CAP,
    );

    check_max_items(&schema, "/properties/nodes", MAX_NODES_CAP);
    check_max_items(&schema, "/properties/relationships", MAX_RELS_CAP);
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
    check_in_branch_max_items(&schema, MAX_IN_VALUES);

    check_array_branch_max_items(
        &schema,
        "/$defs/FilterEntry/oneOf",
        MAX_FILTER_ENTRIES_PER_PROPERTY,
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
         Update either the schema or src/schema_limits.rs so they match."
    );
}

// Locates the `op == "in"` branch by its `if` condition, not index.
fn check_in_branch_max_items(schema: &Value, expected: usize) {
    let all_of = schema
        .pointer("/$defs/PropertyFilter/allOf")
        .and_then(Value::as_array)
        .expect("graph_query.schema.json is missing `/$defs/PropertyFilter/allOf`");

    let branch = all_of
        .iter()
        .find(|b| b.pointer("/if/properties/op/const").and_then(Value::as_str) == Some("in"))
        .expect("PropertyFilter allOf has no `op == \"in\"` branch to guard");

    let actual = branch
        .pointer("/then/properties/value/maxItems")
        .and_then(Value::as_u64)
        .expect("PropertyFilter `in` branch is missing integer value maxItems");
    assert_eq!(
        actual, expected as u64,
        "DRIFT: PropertyFilter `in`-branch value maxItems = {actual} but compiler cap = {expected}. \
         Update either the schema or src/schema_limits.rs so they match."
    );
}

fn check_enum(schema: &Value, ptr: &str, def: &str, expected: &[&str]) {
    let actual = schema
        .pointer(ptr)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("graph_query.schema.json is missing `{ptr}`"));

    let mut actual: Vec<&str> = actual
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("`{ptr}` contains a non-string enum value: {v}"))
        })
        .collect();
    let mut expected: Vec<&str> = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();

    assert_eq!(
        actual, expected,
        "DRIFT: `{ptr}` = {actual:?} but compiler `{def}` = {expected:?}. \
         Update either the schema or src/schema_limits.rs so they match."
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
         Update either the schema or src/schema_limits.rs so they match."
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
         Update either the schema or src/schema_limits.rs so they match."
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
         Update either the schema or src/schema_limits.rs so they match."
    );
}
