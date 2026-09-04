//! Complexity caps for GQL queries.

use compiler::Input;
use compiler::input::{ColumnSelection, InputFilter, InputNode};

use crate::Error;

pub(crate) const MAX_IDENTIFIER_LEN: usize = 64;
pub const MAX_PATTERN_NODES: usize = 5;
pub const MAX_PATTERN_HOPS: usize = 5;
pub const MAX_HOP_BOUND: u32 = 3;
pub const MAX_RELATIONSHIP_TYPES: usize = 10;
pub const MAX_ELEMENT_IDS: usize = 500;
pub const MAX_LIST_VALUES: usize = 100;
pub const MAX_STRING_LEN: usize = 1024;
pub const MAX_PREDICATE_PROPERTIES_PER_NODE: usize = 10;
pub const MAX_PREDICATE_PROPERTIES_PER_RELATIONSHIP: usize = 5;
pub const MAX_PREDICATES_PER_PROPERTY: usize = 10;
pub const MAX_RETURNED_PROPERTIES_PER_NODE: usize = 50;
pub const MAX_AGGREGATES: usize = 10;
pub const MAX_GROUP_BY_KEYS: usize = 4;
pub const MAX_LIMIT: u32 = 1000;

const _: () = {
    use compiler::schema_limits as backstop;
    assert!(MAX_PATTERN_NODES <= backstop::MAX_NODES_CAP);
    assert!(MAX_PATTERN_HOPS <= backstop::MAX_RELS_CAP);
    assert!(MAX_HOP_BOUND <= backstop::MAX_HOPS_CAP);
    assert!(MAX_HOP_BOUND <= backstop::MAX_DEPTH_CAP);
    assert!(MAX_RELATIONSHIP_TYPES <= backstop::MAX_REL_TYPES);
    assert!(MAX_ELEMENT_IDS <= backstop::MAX_NODE_IDS);
    assert!(MAX_LIST_VALUES <= backstop::MAX_IN_VALUES);
    assert!(MAX_STRING_LEN <= backstop::MAX_FILTER_VALUE_LEN);
    assert!(MAX_PREDICATE_PROPERTIES_PER_NODE <= backstop::MAX_FILTERS_PER_NODE);
    assert!(MAX_PREDICATE_PROPERTIES_PER_RELATIONSHIP <= backstop::MAX_FILTERS_PER_REL);
    assert!(MAX_PREDICATES_PER_PROPERTY <= backstop::MAX_FILTER_ENTRIES_PER_PROPERTY);
    assert!(MAX_RETURNED_PROPERTIES_PER_NODE <= backstop::MAX_COLUMNS);
    assert!(MAX_LIMIT <= backstop::MAX_LIMIT);
};

fn depth(msg: impl Into<String>) -> Error {
    Error::Depth(msg.into())
}

fn limit(msg: impl Into<String>) -> Error {
    Error::Limit(msg.into())
}

pub(crate) fn check(input: &Input) -> Result<(), Error> {
    if !(1..=MAX_LIMIT).contains(&input.limit) {
        return Err(limit(format!("LIMIT must be between 1 and {MAX_LIMIT}")));
    }
    if input.nodes.len() > MAX_PATTERN_NODES {
        return Err(depth(format!(
            "MATCH binds {} nodes, at most {MAX_PATTERN_NODES} are allowed",
            input.nodes.len()
        )));
    }
    if input.relationships.len() > MAX_PATTERN_HOPS {
        return Err(depth(format!(
            "MATCH has {} hops, at most {MAX_PATTERN_HOPS} are allowed",
            input.relationships.len()
        )));
    }
    if input.aggregation.metrics.len() > MAX_AGGREGATES {
        return Err(limit(format!(
            "RETURN has {} aggregates, at most {MAX_AGGREGATES} are allowed",
            input.aggregation.metrics.len()
        )));
    }
    if input.aggregation.group_by.len() > MAX_GROUP_BY_KEYS {
        return Err(limit(format!(
            "GROUP BY has {} keys, at most {MAX_GROUP_BY_KEYS} are allowed",
            input.aggregation.group_by.len()
        )));
    }
    for rel in &input.relationships {
        if rel.hops.max > MAX_HOP_BOUND {
            return Err(depth(format!(
                "hop bound {} between {} and {} exceeds {MAX_HOP_BOUND}",
                rel.hops.max, rel.from, rel.to
            )));
        }
        check_rel_types(&rel.types)?;
        if rel.filters.len() > MAX_PREDICATE_PROPERTIES_PER_RELATIONSHIP {
            return Err(limit(format!(
                "relationship between {} and {} is filtered on {} properties, \
                 at most {MAX_PREDICATE_PROPERTIES_PER_RELATIONSHIP} are allowed",
                rel.from,
                rel.to,
                rel.filters.len()
            )));
        }
        for (prop, filters) in &rel.filters {
            check_property_filters(prop, filters)?;
        }
    }
    if let Some(path) = &input.path {
        if path.max_depth > MAX_HOP_BOUND {
            return Err(depth(format!(
                "SHORTEST hop bound {} exceeds {MAX_HOP_BOUND}",
                path.max_depth
            )));
        }
        check_rel_types(&path.rel_types)?;
    }
    if let Some(neighbors) = &input.neighbors {
        check_rel_types(&neighbors.rel_types)?;
    }
    input.nodes.iter().try_for_each(check_node)
}

fn check_node(node: &InputNode) -> Result<(), Error> {
    if node.node_ids.len() > MAX_ELEMENT_IDS {
        return Err(limit(format!(
            "element_id({}) IN lists {} ids, at most {MAX_ELEMENT_IDS} are allowed",
            node.id,
            node.node_ids.len()
        )));
    }
    if node.filters.len() > MAX_PREDICATE_PROPERTIES_PER_NODE {
        return Err(limit(format!(
            "{} is filtered on {} properties, at most {MAX_PREDICATE_PROPERTIES_PER_NODE} are allowed",
            node.id,
            node.filters.len()
        )));
    }
    if let Some(ColumnSelection::List(cols)) = &node.columns
        && cols.len() > MAX_RETURNED_PROPERTIES_PER_NODE
    {
        return Err(limit(format!(
            "RETURN selects {} properties of {}, at most {MAX_RETURNED_PROPERTIES_PER_NODE} are allowed",
            cols.len(),
            node.id
        )));
    }
    for (prop, filters) in &node.filters {
        check_property_filters(prop, filters)?;
    }
    Ok(())
}

fn check_rel_types(types: &[String]) -> Result<(), Error> {
    if types.len() > MAX_RELATIONSHIP_TYPES {
        return Err(limit(format!(
            "relationship lists {} types, at most {MAX_RELATIONSHIP_TYPES} are allowed",
            types.len()
        )));
    }
    Ok(())
}

fn check_property_filters(prop: &str, filters: &[InputFilter]) -> Result<(), Error> {
    if filters.len() > MAX_PREDICATES_PER_PROPERTY {
        return Err(limit(format!(
            "{prop} has {} predicates, at most {MAX_PREDICATES_PER_PROPERTY} are allowed",
            filters.len()
        )));
    }
    for filter in filters {
        if let Some(value) = &filter.value {
            check_value(prop, value)?;
        }
    }
    Ok(())
}

fn check_value(prop: &str, value: &serde_json::Value) -> Result<(), Error> {
    match value {
        serde_json::Value::String(s) if s.len() > MAX_STRING_LEN => Err(limit(format!(
            "string value on {prop} exceeds {MAX_STRING_LEN} characters"
        ))),
        serde_json::Value::Array(items) if items.len() > MAX_LIST_VALUES => Err(limit(format!(
            "list on {prop} has {} values, at most {MAX_LIST_VALUES} are allowed",
            items.len()
        ))),
        serde_json::Value::Array(items) => items.iter().try_for_each(|v| check_value(prop, v)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Params, parse};

    fn reject(gql: &str) -> Error {
        parse(gql, &Params::new()).expect_err("query should exceed a cap")
    }

    fn reject_with(gql: &str, params: Params) -> Error {
        parse(gql, &params).expect_err("query should exceed a cap")
    }

    fn accept(gql: &str) {
        parse(gql, &Params::new()).unwrap_or_else(|e| panic!("{e}\n{gql}"));
    }

    fn joined(n: usize, f: impl Fn(usize) -> String, sep: &str) -> String {
        (0..n).map(f).collect::<Vec<_>>().join(sep)
    }

    #[test]
    fn limit_out_of_range() {
        assert_eq!(
            reject("MATCH (p:Project) RETURN p LIMIT 0").to_string(),
            "LIMIT must be between 1 and 1000"
        );
        assert_eq!(
            reject("MATCH (p:Project) RETURN p LIMIT 1001").to_string(),
            "LIMIT must be between 1 and 1000"
        );
        accept("MATCH (p:Project) RETURN p LIMIT 1000");
    }

    #[test]
    fn too_many_pattern_nodes() {
        let chain = joined(
            MAX_PATTERN_NODES + 1,
            |i| format!("(n{i}:Project)"),
            "-[:R]->",
        );
        assert_eq!(
            reject(&format!("MATCH {chain} RETURN n0")).to_string(),
            "MATCH binds 6 nodes, at most 5 are allowed"
        );
    }

    #[test]
    fn too_many_hops_across_patterns() {
        let patterns = joined(
            MAX_PATTERN_HOPS + 1,
            |i| format!("(a:Project)-[:R{i}]->(b:Project)"),
            ", ",
        );
        assert_eq!(
            reject(&format!("MATCH {patterns} RETURN a")).to_string(),
            "MATCH has 6 hops, at most 5 are allowed"
        );
    }

    #[test]
    fn hop_bound_above_cap() {
        assert_eq!(
            reject("MATCH (a:Project)-[:R]->{1,4}(b:Project) RETURN a").to_string(),
            "hop bound 4 between a and b exceeds 3"
        );
        assert_eq!(
            reject("MATCH (a:Project)-[:R*..4]->(b:Project) RETURN a").to_string(),
            "hop bound 4 between a and b exceeds 3"
        );
        accept("MATCH (a:Project)-[:R]->{1,3}(b:Project) RETURN a");
    }

    #[test]
    fn shortest_hop_bound_above_cap() {
        let gql = "MATCH SHORTEST (a:Project)-[:R]->{1,4}(b:Project) \
                   WHERE element_id(a) = 1 AND element_id(b) = 2 RETURN a.name";
        assert_eq!(reject(gql).to_string(), "SHORTEST hop bound 4 exceeds 3");
    }

    #[test]
    fn too_many_relationship_types() {
        let types = joined(MAX_RELATIONSHIP_TYPES + 1, |i| format!("R{i}"), "|");
        let err = reject(&format!(
            "MATCH (a:Project)-[:{types}]->(b:Project) RETURN a"
        ));
        assert_eq!(
            err.to_string(),
            "relationship lists 11 types, at most 10 are allowed"
        );
        let err = reject(&format!("MATCH (a:Project)-[:{types}]->(b) RETURN a"));
        assert_eq!(
            err.to_string(),
            "relationship lists 11 types, at most 10 are allowed"
        );
    }

    #[test]
    fn too_many_element_ids() {
        let ids = joined(MAX_ELEMENT_IDS + 1, |i| i.to_string(), ", ");
        let err = reject(&format!(
            "MATCH (p:Project) WHERE element_id(p) IN [{ids}] RETURN p"
        ));
        assert_eq!(
            err.to_string(),
            "element_id(p) IN lists 501 ids, at most 500 are allowed"
        );
    }

    #[test]
    fn too_many_list_values() {
        let values = joined(MAX_LIST_VALUES + 1, |i| i.to_string(), ", ");
        let err = reject(&format!(
            "MATCH (p:Project) WHERE p.star_count IN [{values}] RETURN p"
        ));
        assert_eq!(
            err.to_string(),
            "list on star_count has 101 values, at most 100 are allowed"
        );
    }

    #[test]
    fn overlong_string_in_literal_and_parameter() {
        let long = "x".repeat(MAX_STRING_LEN + 1);
        let err = reject(&format!(
            "MATCH (p:Project) WHERE p.name = '{long}' RETURN p"
        ));
        assert_eq!(
            err.to_string(),
            "string value on name exceeds 1024 characters"
        );

        let mut params = Params::new();
        params.insert("names".into(), serde_json::json!(["ok", long]));
        let err = reject_with("MATCH (p:Project) WHERE p.name IN $names RETURN p", params);
        assert_eq!(
            err.to_string(),
            "string value on name exceeds 1024 characters"
        );
    }

    #[test]
    fn too_many_filtered_properties_on_node() {
        let terms = joined(
            MAX_PREDICATE_PROPERTIES_PER_NODE + 1,
            |i| format!("p.f{i} = {i}"),
            " AND ",
        );
        let err = reject(&format!("MATCH (p:Project) WHERE {terms} RETURN p"));
        assert_eq!(
            err.to_string(),
            "p is filtered on 11 properties, at most 10 are allowed"
        );
    }

    #[test]
    fn too_many_filtered_properties_on_relationship() {
        let props = joined(
            MAX_PREDICATE_PROPERTIES_PER_RELATIONSHIP + 1,
            |i| format!("f{i}: {i}"),
            ", ",
        );
        let err = reject(&format!(
            "MATCH (a:Project)-[:R {{{props}}}]->(b:Project) RETURN a"
        ));
        assert_eq!(
            err.to_string(),
            "relationship between a and b is filtered on 6 properties, at most 5 are allowed"
        );
    }

    #[test]
    fn too_many_predicates_on_one_property() {
        let terms = joined(
            MAX_PREDICATES_PER_PROPERTY + 1,
            |i| format!("p.star_count > {i}"),
            " AND ",
        );
        let err = reject(&format!("MATCH (p:Project) WHERE {terms} RETURN p"));
        assert_eq!(
            err.to_string(),
            "star_count has 11 predicates, at most 10 are allowed"
        );
    }

    #[test]
    fn too_many_returned_properties() {
        let items = joined(
            MAX_RETURNED_PROPERTIES_PER_NODE + 1,
            |i| format!("p.c{i}"),
            ", ",
        );
        let err = reject(&format!("MATCH (p:Project) RETURN {items}"));
        assert_eq!(
            err.to_string(),
            "RETURN selects 51 properties of p, at most 50 are allowed"
        );
    }

    #[test]
    fn too_many_aggregates() {
        let aggs = joined(MAX_AGGREGATES + 1, |i| format!("count(p) AS c{i}"), ", ");
        let err = reject(&format!("MATCH (p:Project) RETURN {aggs}"));
        assert_eq!(
            err.to_string(),
            "RETURN has 11 aggregates, at most 10 are allowed"
        );
    }

    #[test]
    fn too_many_group_by_keys() {
        let keys = joined(MAX_GROUP_BY_KEYS + 1, |i| format!("p.k{i}"), ", ");
        let err = reject(&format!(
            "MATCH (p:Project) RETURN count(p) AS n GROUP BY {keys}"
        ));
        assert_eq!(
            err.to_string(),
            "GROUP BY has 5 keys, at most 4 are allowed"
        );
    }
}
