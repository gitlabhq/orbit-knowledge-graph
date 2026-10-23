use std::path::PathBuf;

use pest_meta::ast::{Expr, Rule};
use pest_meta::parser::{self, Rule as MetaRule};
use serde_json::Value;

mod config {
    include!("src/query_contract/config.rs");
}
use config::QueryContract;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/passes/frontend/gql/query.pest");

    let contract_path = env_path("CONFIG_DIR").join("query_contract.yaml");
    let schema_path = env_path("SCHEMA_DIR").join("graph_query.schema.json");
    println!("cargo:rerun-if-changed={}", contract_path.display());
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let contract: QueryContract = orbit_utils::yaml::from_str(&read(&contract_path))
        .unwrap_or_else(|error| {
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
    check_grammar(&grammar, &contract);
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
    assert!(
        !contract.ordering.is_empty(),
        "ordering types must not be empty"
    );
    assert_eq!(
        contract.limits.max_node_ids, contract.limits.max_in_values,
        "GQL general and IN lists share one bound"
    );
    assert_eq!(
        contract.limits.max_hops, contract.limits.max_depth,
        "GQL uses one numeric rule for traversal hops and path depth"
    );
    for rule in &contract.input_rules {
        assert!(!rule.code.is_empty());
        assert!(!rule.reject.is_empty());
        assert!(!rule.message.is_empty());
    }
    for limit in [
        contract.limits.max_id_range_span,
        contract.limits.max_path_anchor_range,
        contract.limits.max_path_anchor_rows,
    ] {
        assert!(limit > 0, "runtime limits must be positive");
    }
    assert!(contract.limits.min_search_pattern_length > 0);
}

fn check_schema(schema: &Value, contract: &QueryContract) {
    check_keys(
        object(schema, "/$defs/OperatorFilter/properties").keys(),
        contract.filter_operators.keys(),
        "filter operators",
    );
    check_values(
        array(schema, "/$defs/PathType/enum"),
        &contract.path_types,
        "path types",
    );
    check_aggregation_keys(schema, contract.aggregations.keys());
    for (pointer, checks) in &contract.schema_limits {
        for check in checks {
            let expected = contract
                .limits
                .get(&check.limit)
                .unwrap_or_else(|| panic!("unknown contract limit `{}`", check.limit));
            let value = schema
                .pointer(pointer)
                .unwrap_or_else(|| panic!("graph_query.schema.json is missing `{pointer}`"));
            let actual = match check.branch.as_deref() {
                Some(kind) => unique_branch(value, pointer, kind)
                    .get(limit_keyword(kind))
                    .and_then(Value::as_u64),
                None => value.as_u64(),
            };
            assert_eq!(
                actual,
                Some(expected),
                "`{pointer}` must match `{}`",
                check.limit
            );
        }
    }
    let identifier_length = contract.limits.max_identifier_length - 1;
    let expected = format!("{{0,{identifier_length}}}");
    for pointer in [
        "/properties/order_by/pattern",
        "/properties/aggregation_sort/pattern",
        "/$defs/NodeOrPropertyRef/pattern",
        "/$defs/NodePropertyRef/pattern",
    ] {
        assert!(
            schema
                .pointer(pointer)
                .and_then(Value::as_str)
                .is_some_and(|pattern| pattern.contains(&expected)),
            "`{pointer}` must contain `{expected}`"
        );
    }
}

fn limit_keyword(kind: &str) -> &'static str {
    match kind {
        "array" => "maxItems",
        "string" => "maxLength",
        _ => panic!("unsupported schema branch `{kind}`"),
    }
}

fn unique_branch<'a>(value: &'a Value, pointer: &str, kind: &str) -> &'a Value {
    let matches = value
        .as_array()
        .unwrap_or_else(|| panic!("`{pointer}` must be an array"))
        .iter()
        .filter(|branch| branch.get("type").and_then(Value::as_str) == Some(kind))
        .collect::<Vec<_>>();
    let [branch] = matches.as_slice() else {
        panic!("`{pointer}` must have one `{kind}` branch");
    };
    branch
}

fn check_aggregation_keys<'a>(schema: &Value, expected: impl Iterator<Item = &'a String>) {
    let mut expected = expected.map(String::as_str).collect::<Vec<_>>();
    let mut properties = object(schema, "/$defs/Aggregation/properties")
        .keys()
        .map(String::as_str)
        .filter(|key| *key != "as")
        .collect::<Vec<_>>();
    properties.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        properties, expected,
        "aggregation functions must match query_contract.yaml"
    );

    let mut required = array(schema, "/$defs/Aggregation/oneOf")
        .iter()
        .map(|branch| {
            let required = branch.get("required").and_then(Value::as_array).unwrap();
            let [function] = required.as_slice() else {
                panic!("aggregation branch must require one function");
            };
            function.as_str().unwrap()
        })
        .collect::<Vec<_>>();
    required.sort_unstable();
    assert_eq!(
        required, expected,
        "aggregation branches must match query_contract.yaml"
    );
}

fn check_keys<'a, A, B>(actual: A, expected: B, name: &str)
where
    A: Iterator<Item = &'a String>,
    B: Iterator<Item = &'a String>,
{
    let mut actual = actual.map(String::as_str).collect::<Vec<_>>();
    let mut expected = expected.map(String::as_str).collect::<Vec<_>>();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{name} must match query_contract.yaml");
}

fn check_values(actual: &[Value], expected: &[String], name: &str) {
    let actual = actual
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{name} must match query_contract.yaml");
}

fn check_grammar(grammar: &[Rule], contract: &QueryContract) {
    for check in &contract.grammar_limits {
        let expected = contract
            .limits
            .get(&check.limit)
            .unwrap_or_else(|| panic!("unknown contract limit `{}`", check.limit))
            + check.offset;
        let actual = match check.repeated.as_deref() {
            Some(repeated) => repetition_max(grammar_rule(grammar, &check.rule), repeated) as u64,
            None => numeric_max(grammar_rule(grammar, &check.rule)) as u64,
        };
        assert_eq!(
            actual, expected,
            "GQL rule `{}` must match `{}`",
            check.rule, check.limit
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
            "GQL rule `{}` must have one bounded repetition of `{repeated_rule}`",
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
