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

fn repeated(prefix: &str, item: &str, separator: &str, count: usize, suffix: &str) -> String {
    format!("{prefix}{}{suffix}", vec![item; count].join(separator))
}

fn assert_limit(
    rule: Rule,
    maximum: usize,
    prefix: &str,
    item: &str,
    separator: &str,
    suffix: &str,
) {
    assert!(
        accepts(rule, &repeated(prefix, item, separator, maximum, suffix)),
        "{rule:?} must accept {maximum}"
    );
    assert!(
        !accepts(
            rule,
            &repeated(prefix, item, separator, maximum + 1, suffix)
        ),
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
    assert_limit(Rule::InListLiteral, MAX_IN_VALUES, "[", "1", ",", "]");
    assert!(!accepts(Rule::InListLiteral, "[]"));
    assert_limit(Rule::ListLiteral, MAX_NODE_IDS, "[", "1", ",", "]");
    assert!(!accepts(Rule::ListLiteral, "[]"));
    assert_limit(Rule::RelationshipTypes, MAX_REL_TYPES, "", ":R", "|", "");
    assert_limit(Rule::NodeProjection, MAX_COLUMNS, "n{", ".p", ",", "}");
    assert_limit(
        Rule::AndExpression,
        MAX_PREDICATES,
        "",
        "n.p = 1",
        " AND ",
        "",
    );
    assert_limit(Rule::PatternElement, MAX_RELS_CAP, "(n)", "-->(n)", "", "");
    assert_limit(
        Rule::NodeMapLiteral,
        MAX_FILTERS_PER_NODE,
        "{",
        "p:1",
        ",",
        "}",
    );
    assert_limit(
        Rule::RelationshipMapLiteral,
        MAX_FILTERS_PER_REL,
        "{",
        "p:1",
        ",",
        "}",
    );
    assert_limit(
        Rule::NotExpression,
        MAX_PREDICATE_DEPTH,
        "",
        "NOT ",
        "",
        "n.p = 1",
    );
}

#[test]
fn gql_rejects_the_101st_id_in_both_list_spellings() {
    for count in [MAX_NODE_IDS, MAX_NODE_IDS + 1] {
        let ids = vec!["1"; count].join(",");
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
    for (rule, maximum) in [
        (Rule::LimitCount, MAX_LIMIT),
        (Rule::SkipCount, MAX_SKIP),
        (Rule::HopCount, MAX_HOPS_CAP),
    ] {
        assert!(accepts(rule, &maximum.to_string()));
        assert!(!accepts(rule, &(maximum + 1).to_string()));
    }
}
