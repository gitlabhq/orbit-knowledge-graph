//! Checks that limits expressed in the GQL grammar match the shared query limits.

include!("src/query_limits.rs");

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/query_limits.rs");
    println!("cargo:rerun-if-changed=src/passes/frontend/gql/query.pest");

    let grammar =
        std::fs::read_to_string("src/passes/frontend/gql/query.pest").expect("read GQL grammar");
    check_grammar_bound(
        &grammar,
        "UnescapedSymbolicName",
        &format!("IdentifierPart{{0,{MAX_IDENTIFIER_LEN}}}"),
    );
    check_grammar_bound(
        &grammar,
        "NodeProjection",
        &format!("ProjectionProperty){{0,{MAX_COLUMNS}}}"),
    );
    check_grammar_bound(
        &grammar,
        "RelationshipTypes",
        &format!("SchemaName){{0,{MAX_REL_TYPES}}}"),
    );
    check_grammar_bound(
        &grammar,
        "InListLiteral",
        &format!("Value){{0,{MAX_NODE_IDS}}}"),
    );
    check_grammar_bound(
        &grammar,
        "ListLiteral",
        &format!("Value){{0,{MAX_IN_VALUES}}}"),
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
        &format!("MapEntry){{0,{MAX_FILTERS_PER_NODE}}}"),
    );
    check_grammar_bound(
        &grammar,
        "RelationshipMapLiteral",
        &format!("MapEntry){{0,{MAX_FILTERS_PER_REL}}}"),
    );
    check_grammar_bound(
        &grammar,
        "NotExpression",
        &format!("Negation{{0,{}}}", MAX_PREDICATE_DEPTH + 1),
    );
    check_grammar_numeric_sentinel(&grammar, "HopCount", MAX_HOPS_CAP);
    check_grammar_numeric_sentinel(&grammar, "LimitCount", MAX_LIMIT);
    check_grammar_numeric_sentinel(&grammar, "SkipCount", MAX_SKIP);
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

fn check_grammar_numeric_sentinel(grammar: &str, rule: &str, limit: u32) {
    let production = grammar
        .lines()
        .find(|line| line.starts_with(&format!("{rule} =")))
        .unwrap_or_else(|| panic!("GQL grammar is missing rule `{rule}`"));
    let sentinel = production
        .split('"')
        .nth(1)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| {
            panic!("GQL grammar rule `{rule}` must start with its overflow sentinel")
        });
    assert_eq!(
        sentinel,
        limit + 1,
        "GQL grammar rule `{rule}` overflow sentinel must be one above query_limits.rs"
    );
}
