use query_data_model::plan::logical::limits::*;
use query_data_model::surface::gql::GRAMMAR;

use super::limits::{MAX_FILTERS_PER_NODE, MAX_FILTERS_PER_REL, MAX_PREDICATES};

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
