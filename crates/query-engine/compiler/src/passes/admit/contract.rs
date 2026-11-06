use std::collections::BTreeSet;

use query_data_model::ClickHouseDataModel;
use query_data_model::plan::logical::limits::*;
use query_data_model::plan::logical::{Comparison, Matching, Predicate};
use query_data_model::surface::{gql::GRAMMAR, json::SCHEMA};
use serde_json::Value;

use super::limits::{
    MAX_FILTER_ENTRIES_PER_PROPERTY, MAX_FILTERS_PER_NODE, MAX_FILTERS_PER_REL, MAX_PREDICATES,
};

fn schema() -> Value {
    serde_json::from_str(SCHEMA).expect("the query schema is JSON")
}

fn number(schema: &Value, pointer: &str, key: &str) -> u64 {
    schema
        .pointer(pointer)
        .and_then(|node| node.get(key))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("the schema has no {key} at {pointer}"))
}

fn branch(schema: &Value, pointer: &str, kind: &str, key: &str) -> u64 {
    let branches: Vec<&Value> = schema
        .pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("the schema has no array at {pointer}"))
        .iter()
        .filter(|branch| branch.get("type").and_then(Value::as_str) == Some(kind))
        .collect();
    let [only] = branches[..] else {
        panic!("{pointer} needs exactly one {kind} branch");
    };
    only.get(key)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("the {kind} branch at {pointer} has no {key}"))
}

fn rule(name: &str) -> &'static str {
    GRAMMAR
        .lines()
        .find(|line| line.starts_with(&format!("{name} =")))
        .unwrap_or_else(|| panic!("the grammar has no rule {name}"))
}

fn repeats(name: &str, item: &str, bound: usize) {
    let expected = format!("{item}){{0,{bound}}}");
    assert!(
        rule(name).contains(&expected),
        "{name} must repeat at most {bound}: {}",
        rule(name)
    );
}

fn sentinel(name: &str, limit: u64) {
    let first = rule(name)
        .split('"')
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok());
    assert_eq!(
        first,
        Some(limit + 1),
        "{name} must start with the overflow sentinel {}",
        limit + 1
    );
}

#[test]
fn json_schema_limits_match_the_engine() {
    let schema = schema();
    let s = &schema;
    let defs = "/$defs";
    let cases = [
        (
            number(
                s,
                &format!("{defs}/RelationshipSelector/properties/hops/items"),
                "maximum",
            ),
            u64::from(MAX_HOPS),
        ),
        (
            number(
                s,
                &format!("{defs}/PathConfig/properties/max_depth"),
                "maximum",
            ),
            u64::from(MAX_PATH_DEPTH),
        ),
        (number(s, "/properties/limit", "maximum"), MAX_ROWS),
        (
            number(s, &format!("{defs}/Identifier"), "maxLength"),
            MAX_IDENTIFIER as u64,
        ),
        (
            number(s, &format!("{defs}/SearchPattern"), "maxLength"),
            MAX_TEXT as u64,
        ),
        (
            branch(
                s,
                &format!("{defs}/FilterValue/oneOf"),
                "string",
                "maxLength",
            ),
            MAX_TEXT as u64,
        ),
        (number(s, "/properties/nodes", "maxItems"), MAX_NODES as u64),
        (
            number(s, "/properties/aggregations", "maxItems"),
            MAX_MEASURES as u64,
        ),
        (
            number(s, "/properties/group_by", "maxItems"),
            MAX_GROUPS as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/NodeSelector/properties/node_ids"),
                "maxItems",
            ),
            MAX_PINS as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/NodeSelector/properties/filters"),
                "maxProperties",
            ),
            MAX_FILTERS_PER_NODE as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/RelationshipSelector/properties/filters"),
                "maxProperties",
            ),
            MAX_FILTERS_PER_REL as u64,
        ),
        (
            branch(
                s,
                &format!("{defs}/ColumnSelection/oneOf"),
                "array",
                "maxItems",
            ),
            MAX_COLUMNS as u64,
        ),
        (
            branch(
                s,
                &format!("{defs}/RelationshipType/oneOf"),
                "array",
                "maxItems",
            ),
            MAX_RELATIONSHIP_TYPES as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/PathConfig/properties/rel_types"),
                "maxItems",
            ),
            MAX_RELATIONSHIP_TYPES as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/NeighborsConfig/properties/rel_types"),
                "maxItems",
            ),
            MAX_RELATIONSHIP_TYPES as u64,
        ),
        (
            branch(s, &format!("{defs}/FilterValue/oneOf"), "array", "maxItems"),
            MAX_IN_VALUES as u64,
        ),
        (
            number(
                s,
                &format!("{defs}/OperatorFilter/properties/in"),
                "maxItems",
            ),
            MAX_IN_VALUES as u64,
        ),
        (
            branch(s, &format!("{defs}/FilterEntry/oneOf"), "array", "maxItems"),
            MAX_FILTER_ENTRIES_PER_PROPERTY as u64,
        ),
    ];
    for (index, (schema, engine)) in cases.into_iter().enumerate() {
        assert_eq!(
            schema, engine,
            "schema limit {index} drifted from the engine"
        );
    }
    assert_eq!(
        number(s, "/properties/relationships", "maxItems"),
        MAX_NODES as u64,
        "a pattern of {MAX_NODES} nodes needs as many relationships"
    );
}

#[test]
fn json_schema_operators_match_the_predicates() {
    let schema = schema();
    let declared: BTreeSet<&str> = schema
        .pointer("/$defs/OperatorFilter/properties")
        .and_then(Value::as_object)
        .expect("the schema declares operator filters")
        .keys()
        .map(String::as_str)
        .collect();
    let model = ClickHouseDataModel::embedded().expect("the embedded model loads");
    let model = model.catalog().model();
    let statement = crate::surface::parse(
        "MATCH (u:User {id: 1}) RETURN u",
        crate::Frontend::Gql,
        model.logical(),
    )
    .expect("the query parses");
    let left = crate::logical::Query::new(&statement.plan, model).leaves()[0].left;
    let comparisons = [
        Comparison::Eq,
        Comparison::Ne,
        Comparison::Lt,
        Comparison::Le,
        Comparison::Gt,
        Comparison::Ge,
    ];
    let matchings = [
        Matching::Contains,
        Matching::StartsWith,
        Matching::EndsWith,
        Matching::Token,
        Matching::AllTokens,
        Matching::AnyTokens,
    ];
    let predicates = comparisons
        .into_iter()
        .map(|comparison| left.compare(comparison, 1))
        .chain(
            matchings
                .into_iter()
                .map(|matching| left.matches(matching, "abc")),
        )
        .chain([left.one_of([]), left.is_null(true), left.is_null(false)]);
    let supported: BTreeSet<&str> = predicates
        .map(|predicate: Predicate| predicate.operator().expect("a leaf has an operator"))
        .collect();
    assert_eq!(declared, supported);
    let paths = schema.pointer("/$defs/PathType/enum").cloned();
    assert_eq!(paths, Some(serde_json::json!(["shortest"])));
}

#[test]
fn gql_grammar_limits_match_the_engine() {
    repeats("NodeProjection", "ProjectionProperty", MAX_COLUMNS);
    repeats("RelationshipTypes", "SchemaName", MAX_RELATIONSHIP_TYPES);
    repeats("InListLiteral", "Value", MAX_PINS);
    repeats("ListLiteral", "Value", MAX_IN_VALUES);
    repeats("PatternElement", "PatternElementChain", MAX_NODES);
    repeats("AndExpression", "NotExpression", MAX_PREDICATES);
    repeats("NodeMapLiteral", "MapEntry", MAX_FILTERS_PER_NODE);
    repeats("RelationshipMapLiteral", "MapEntry", MAX_FILTERS_PER_REL);
    sentinel("HopCount", u64::from(MAX_HOPS));
    sentinel("LimitCount", MAX_ROWS);
    sentinel("SkipCount", MAX_SKIP);
    let identifier = format!("IdentifierPart{{0,{MAX_IDENTIFIER}}}");
    assert!(rule("UnescapedSymbolicName").contains(&identifier));
    let nesting = format!("Negation{{0,{}}}", MAX_NESTING + 1);
    assert!(rule("NotExpression").contains(&nesting));
}
