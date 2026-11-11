//! The largest limits the GQL grammar parses. The grammar admits one item past each limit, so the
//! engine, not the parser, reports the overflow.

use orbit_server_config::QueryLimits;

use super::GRAMMAR;

fn rule(name: &str) -> &'static str {
    GRAMMAR
        .lines()
        .find(|line| line.starts_with(&format!("{name} =")))
        .unwrap_or_else(|| panic!("the grammar has no rule {name}"))
}

/// The `N` of the first `{marker}N}` in rule `name`.
fn repeat(name: &str, marker: &str) -> u64 {
    let rule = rule(name);
    rule.find(marker)
        .and_then(|start| rule[start + marker.len()..].split('}').next())
        .and_then(|bound| bound.parse().ok())
        .unwrap_or_else(|| panic!("{name} has no bound after {marker}: {rule}"))
}

/// One below the overflow sentinel, the first literal of rule `name`.
fn sentinel(name: &str) -> u64 {
    rule(name)
        .split('"')
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{name} does not start with an overflow sentinel"))
        - 1
}

/// Each grammar-bounded limit: its `query.limits` key, its value, and the largest value the
/// grammar parses.
pub fn ceilings(limits: &QueryLimits) -> Vec<(&'static str, u64, u64)> {
    let size = |value: usize| value as u64;
    vec![
        (
            "columns",
            size(limits.columns),
            repeat("NodeProjection", "ProjectionProperty){0,"),
        ),
        (
            "relationship_types",
            size(limits.relationship_types),
            repeat("RelationshipTypes", "SchemaName){0,"),
        ),
        (
            "pinned_ids",
            size(limits.pinned_ids),
            repeat("InListLiteral", "Value){0,"),
        ),
        (
            "in_values",
            size(limits.in_values),
            repeat("ListLiteral", "Value){0,"),
        ),
        (
            "nodes",
            size(limits.nodes),
            repeat("PatternElement", "PatternElementChain){0,"),
        ),
        (
            "predicates",
            size(limits.predicates),
            repeat("AndExpression", "NotExpression){0,"),
        ),
        (
            "node_filter_properties",
            size(limits.node_filter_properties),
            repeat("NodeMapLiteral", "MapEntry){0,"),
        ),
        (
            "relationship_filter_properties",
            size(limits.relationship_filter_properties),
            repeat("RelationshipMapLiteral", "MapEntry){0,"),
        ),
        ("hops", u64::from(limits.hops), sentinel("HopCount")),
        (
            "path_depth",
            u64::from(limits.path_depth),
            sentinel("HopCount"),
        ),
        ("max_rows", limits.max_rows, sentinel("LimitCount")),
        ("max_skip", limits.max_skip, sentinel("SkipCount")),
        (
            "identifier_bytes",
            size(limits.identifier_bytes),
            repeat("UnescapedSymbolicName", "IdentifierPart{0,"),
        ),
        (
            "nesting",
            size(limits.nesting),
            repeat("NotExpression", "Negation{0,") - 1,
        ),
    ]
}

/// Rejects a limit the grammar cannot parse one past.
pub fn admits(limits: &QueryLimits) -> Result<(), String> {
    match ceilings(limits)
        .into_iter()
        .find(|(_, limit, ceiling)| limit > ceiling)
    {
        Some((key, limit, ceiling)) => Err(format!(
            "query.limits.{key} is {limit}, above {ceiling}, the most the GQL grammar parses"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use orbit_server_config::AppConfig;

    use super::*;

    #[test]
    fn shipped_limits_match_the_grammar() {
        let limits = AppConfig::embedded_defaults().query.limits;
        for (key, limit, ceiling) in ceilings(&limits) {
            assert_eq!(
                limit, ceiling,
                "query.limits.{key} must match the GQL grammar"
            );
        }
        assert_eq!(admits(&limits), Ok(()));
    }

    #[test]
    fn a_limit_past_the_grammar_is_rejected() {
        let mut limits = AppConfig::embedded_defaults().query.limits;
        limits.columns += 1;
        assert_eq!(
            admits(&limits),
            Err("query.limits.columns is 51, above 50, the most the GQL grammar parses".into())
        );
        limits.columns -= 2;
        assert_eq!(admits(&limits), Ok(()));
    }
}
