use pest::Parser as _;

use super::{QueryParser, Rule, parse_query};
use crate::query_limits::{
    MAX_COLUMNS, MAX_FILTER_STRING_LEN, MAX_FILTERS_PER_NODE, MAX_FILTERS_PER_REL, MAX_HOPS_CAP,
    MAX_IDENTIFIER_LEN, MAX_IN_VALUES, MAX_LIMIT, MAX_NODE_IDS, MAX_PREDICATE_DEPTH,
    MAX_PREDICATES, MAX_REL_TYPES, MAX_RELS_CAP, MAX_SKIP,
};

fn accepts(rule: Rule, input: &str) -> bool {
    QueryParser::parse(rule, input)
        .ok()
        .and_then(|mut pairs| pairs.next())
        .is_some_and(|pair| pair.as_span().end() == input.len())
}

fn joined(count: usize, separator: &str, item: impl Fn(usize) -> String) -> String {
    (0..count).map(item).collect::<Vec<_>>().join(separator)
}

fn assert_limit(rule: Rule, maximum: usize, value: impl Fn(usize) -> String) {
    assert!(
        accepts(rule, &value(maximum)),
        "{rule:?} must accept {maximum}"
    );
    assert!(
        !accepts(rule, &value(maximum + 1)),
        "{rule:?} must reject {}",
        maximum + 1
    );
}

#[test]
fn identifier_rules_match_the_shared_limit() {
    let maximum = format!("a{}", "b".repeat(MAX_IDENTIFIER_LEN - 1));
    let too_long = format!("{maximum}b");
    assert!(accepts(Rule::UnescapedSymbolicName, &maximum));
    assert!(!accepts(Rule::UnescapedSymbolicName, &too_long));
    assert!(accepts(Rule::EscapedSymbolicName, &format!("`{maximum}`")));
    assert!(!accepts(
        Rule::EscapedSymbolicName,
        &format!("`{too_long}`")
    ));
}

#[test]
fn collection_rules_match_shared_limits() {
    let list = |count| format!("[{}]", joined(count, ",", |_| "1".into()));
    let types = |count| joined(count, "|", |i| format!(":R{i}"));
    let projection = |count| format!("n{{{}}}", joined(count, ",", |i| format!(".p{i}")));

    assert_limit(Rule::InListLiteral, MAX_IN_VALUES, list);
    assert!(!accepts(Rule::InListLiteral, "[]"));
    assert_limit(Rule::ListLiteral, MAX_NODE_IDS, list);
    assert!(!accepts(Rule::ListLiteral, "[]"));
    assert_limit(Rule::RelationshipTypes, MAX_REL_TYPES, types);
    assert_limit(Rule::NodeProjection, MAX_COLUMNS, projection);
    assert_limit(Rule::AndExpression, MAX_PREDICATES, |count| {
        joined(count, " AND ", |i| format!("n.p{i} = {i}"))
    });
    assert_limit(Rule::PatternElement, MAX_RELS_CAP, |count| {
        format!("(n0){}", joined(count, "", |i| format!("-->(n{})", i + 1)))
    });
    assert_limit(Rule::NodeMapLiteral, MAX_FILTERS_PER_NODE, |count| {
        format!("{{{}}}", joined(count, ",", |i| format!("p{i}:{i}")))
    });
    assert_limit(Rule::RelationshipMapLiteral, MAX_FILTERS_PER_REL, |count| {
        format!("{{{}}}", joined(count, ",", |i| format!("p{i}:{i}")))
    });
    assert_limit(Rule::NotExpression, MAX_PREDICATE_DEPTH, |count| {
        format!("{}n.p = 1", "NOT ".repeat(count))
    });
}

#[test]
fn gql_rejects_the_101st_id_in_both_list_spellings() {
    for count in [MAX_NODE_IDS, MAX_NODE_IDS + 1] {
        let ids = joined(count, ",", |i| i.to_string());
        for query in [
            format!("MATCH (n:Project {{id: [{ids}]}}) RETURN n"),
            format!("MATCH (n:Project) WHERE n.id IN [{ids}] RETURN n"),
        ] {
            assert_eq!(parse_query(&query).is_ok(), count == MAX_NODE_IDS);
        }
    }
}

#[test]
fn strings_reject_raw_controls_and_match_the_shared_limit() {
    let query = |value: &str| format!("MATCH (n:Project) WHERE n.name = '{value}' RETURN n.name");
    assert!(parse_query(&query(&"a".repeat(MAX_FILTER_STRING_LEN))).is_ok());
    let error = parse_query(&query(&"a".repeat(MAX_FILTER_STRING_LEN + 1)))
        .expect_err("oversized string must fail")
        .to_string();
    assert!(error.contains("string literal must not exceed"), "{error}");
    assert!(!accepts(Rule::StringLiteral, "'raw\0control'"));
    assert!(!accepts(Rule::StringLiteral, "'raw\nnewline'"));
}

#[test]
fn numeric_clause_rules_match_shared_limits() {
    for (valid, invalid) in [
        (
            format!("MATCH (n:Project) RETURN n LIMIT {MAX_LIMIT}"),
            format!("MATCH (n:Project) RETURN n LIMIT {}", MAX_LIMIT + 1),
        ),
        (
            format!("MATCH (n:Project) RETURN n SKIP {MAX_SKIP}"),
            format!("MATCH (n:Project) RETURN n SKIP {}", MAX_SKIP + 1),
        ),
        (
            format!("MATCH (a:Project)-[*1..{MAX_HOPS_CAP}]->(b:Project) RETURN b"),
            format!(
                "MATCH (a:Project)-[*1..{}]->(b:Project) RETURN b",
                MAX_HOPS_CAP + 1
            ),
        ),
    ] {
        assert!(parse_query(&valid).is_ok(), "{valid}");
        assert!(parse_query(&invalid).is_err(), "{invalid}");
    }
}
