use compiler::input::{Direction, InputGroupByKey, OrderDirection};
use compiler::{ColumnSelection, FilterOp, Input, QueryError, QueryType};
use ontology::{DataType, Ontology};
use serde_json::json;

use crate::{MAX_NESTING_DEPTH, MAX_STATEMENT_BYTES, Parameters};

fn ontology() -> Ontology {
    Ontology::new()
        .with_nodes(["User", "Project", "Note", "Group"])
        .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
        .with_fields(
            "User",
            [
                ("username", DataType::String),
                ("state", DataType::String),
                ("created_at", DataType::DateTime),
            ],
        )
        .with_fields(
            "Note",
            [
                ("confidential", DataType::Bool),
                ("created_at", DataType::DateTime),
            ],
        )
        .with_fields("Project", [("name", DataType::String)])
        .with_fields("Group", [("name", DataType::String)])
}

fn lower(statement: &str) -> Input {
    lower_with(statement, Parameters::new())
}

fn lower_with(statement: &str, params: Parameters) -> Input {
    crate::lower(statement, &params, &ontology()).unwrap_or_else(|e| panic!("{statement}\n{e}"))
}

fn reject(statement: &str) -> QueryError {
    crate::lower(statement, &Parameters::new(), &ontology())
        .err()
        .unwrap_or_else(|| panic!("should reject: {statement}"))
}

fn ids(input: &Input) -> Vec<&str> {
    input.nodes.iter().map(|n| n.id.as_str()).collect()
}

#[test]
fn left_arrow_lowers_to_canonical_from_to() {
    let input = lower("MATCH (n:Note)<-[:AUTHORED]-(u:User {id: 1}) RETURN n");
    assert_eq!(ids(&input), ["n", "u"]);
    let rel = &input.relationships[0];
    assert_eq!((rel.from.as_str(), rel.to.as_str()), ("u", "n"));
    assert_eq!(rel.direction, Direction::Outgoing);
    assert_eq!(input.nodes[0].columns, None);
    assert_eq!(input.nodes[1].columns, None);
    assert_eq!(input.nodes[1].node_ids, [1]);
}

#[test]
fn return_star_selects_every_column() {
    let input = lower("MATCH (n:Note)<-[:AUTHORED]-(u:User {id: 1}) RETURN *");
    assert!(
        input
            .nodes
            .iter()
            .all(|n| n.columns == Some(ColumnSelection::All))
    );
}

#[test]
fn undirected_edge_is_direction_both() {
    let input = lower("MATCH (u:User {id: 1})-[:MEMBER_OF]-(g:Group) RETURN g.name");
    assert_eq!(input.relationships[0].direction, Direction::Both);
    assert_eq!(
        input.nodes[1].columns,
        Some(ColumnSelection::List(vec!["name".into()]))
    );
}

#[test]
fn comma_patterns_share_variables() {
    let input = lower(
        "MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note), (u)-[:MEMBER_OF]->(g:Group) RETURN n.confidential, g.name",
    );
    assert_eq!(ids(&input), ["u", "n", "g"]);
    assert_eq!(input.relationships.len(), 2);
    assert_eq!(input.relationships[1].from, "u");
}

#[test]
fn id_predicates_lower_to_node_ids_and_id_range() {
    let input = lower("MATCH (u:User) WHERE u.id IN [1, 2] RETURN u.username");
    assert_eq!(input.nodes[0].node_ids, [1, 2]);

    let input = lower("MATCH (u:User) WHERE u.id >= 5 AND u.id <= 9 RETURN u.username");
    let range = input.nodes[0].id_range.as_ref().unwrap();
    assert_eq!((range.start, range.end), (5, 9));
    assert!(input.nodes[0].filters.is_empty());

    let input = lower("MATCH (u:User {id: '42'}) WHERE u.id > 5 RETURN u.username");
    assert_eq!(input.nodes[0].node_ids, [42]);
    assert_eq!(input.nodes[0].filters["id"][0].op, Some(FilterOp::Gt));
}

#[test]
fn filters_group_by_property_and_sort_by_operator() {
    let input = lower(
        "MATCH (u:User {state: 'active'})
         WHERE u.created_at <= '2025' AND u.created_at >= date('2024-01-01') AND u.username IS NOT NULL
         RETURN u.username",
    );
    let filters = &input.nodes[0].filters;
    assert_eq!(
        filters.keys().collect::<Vec<_>>(),
        ["created_at", "state", "username"]
    );
    let created = &filters["created_at"];
    assert_eq!(created[0].op, Some(FilterOp::Gte));
    assert_eq!(created[0].value, Some(json!("2024-01-01")));
    assert_eq!(created[1].op, Some(FilterOp::Lte));
    assert_eq!(filters["username"][0].op, Some(FilterOp::IsNotNull));
    assert_eq!(filters["username"][0].value, None);
}

#[test]
fn token_functions_are_predicates() {
    let input =
        lower("MATCH (u:User) WHERE any_tokens(u.username, 'ada lovelace') RETURN u.username");
    let filter = &input.nodes[0].filters["username"][0];
    assert_eq!(filter.op, Some(FilterOp::AnyTokens));
    assert_eq!(filter.value, Some(json!("ada lovelace")));
}

#[test]
fn reserved_words_are_valid_schema_names_and_backticked_variables() {
    let input = lower("MATCH (`end`:Project {id: 1})-[:CONTAINS]->(p:Project) RETURN `end`.name");
    assert_eq!(ids(&input), ["end", "p"]);
    assert_eq!(input.relationships[0].types, ["CONTAINS"]);
}

#[test]
fn untyped_edge_is_the_wildcard() {
    let input = lower("MATCH (u:User {id: 1})-->(p:Project) RETURN p.name");
    assert_eq!(input.relationships[0].types, ["*"]);
}

#[test]
fn hop_ranges() {
    let input = lower("MATCH (u:User {id: 1})-[:MEMBER_OF*2]->(g:Group) RETURN g.name");
    assert_eq!(
        (
            input.relationships[0].hops.min,
            input.relationships[0].hops.max
        ),
        (2, 2)
    );
    let input = lower("MATCH (u:User {id: 1})-[:MEMBER_OF*..3]->(g:Group) RETURN g.name");
    assert_eq!(
        (
            input.relationships[0].hops.min,
            input.relationships[0].hops.max
        ),
        (1, 3)
    );
}

#[test]
fn limit_defaults_to_thirty() {
    assert_eq!(lower("MATCH (u:User {id: 1}) RETURN u").limit, 30);
    assert_eq!(lower("MATCH (u:User {id: 1}) RETURN u LIMIT 7").limit, 7);
}

#[test]
fn aggregation_groups_by_non_aggregate_items() {
    let input = lower(
        "MATCH (u:User {state: 'active'})-[:AUTHORED]->(n:Note)
         RETURN u.state, date_trunc('month', n.created_at) AS month, count(n) AS notes
         ORDER BY notes DESC",
    );
    assert_eq!(input.query_type, QueryType::Aggregation);
    assert!(matches!(
        &input.aggregation.group_by[0],
        InputGroupByKey::Property { node, property, truncate: None, alias: None } if node == "u" && property == "state"
    ));
    assert!(matches!(
        &input.aggregation.group_by[1],
        InputGroupByKey::Property { alias: Some(a), truncate: Some(_), .. } if a == "month"
    ));
    assert_eq!(input.aggregation.metrics[0].output_name(), "notes");
    let sort = input.aggregation.sort.as_ref().unwrap();
    assert_eq!(
        (sort.column.as_str(), sort.direction),
        ("notes", OrderDirection::Desc)
    );
}

#[test]
fn aggregation_sort_resolves_verbatim_items_and_derived_names() {
    let input = lower("MATCH (u:User {id: 1}) RETURN u.state, count(u) ORDER BY count(u)");
    assert_eq!(input.aggregation.sort.as_ref().unwrap().column, "count_u");
    let input = lower("MATCH (u:User {id: 1}) RETURN u.state, count(u) ORDER BY u.state");
    assert_eq!(input.aggregation.sort.as_ref().unwrap().column, "u_state");
}

#[test]
fn bare_node_beside_its_properties_selects_columns() {
    let input =
        lower("MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note) RETURN u, u.username, count(n)");
    assert!(
        matches!(&input.aggregation.group_by[..], [InputGroupByKey::Node { node, .. }] if node == "u")
    );
    assert_eq!(
        input.nodes[0].columns,
        Some(ColumnSelection::List(vec!["username".into()]))
    );
}

#[test]
fn neighbors_direction_is_relative_to_the_center() {
    for (statement, direction) in [
        ("MATCH (u:User {id: 1})-->(n) RETURN n", Direction::Outgoing),
        ("MATCH (u:User {id: 1})<--(n) RETURN n", Direction::Incoming),
        ("MATCH (n)-->(u:User {id: 1}) RETURN n", Direction::Incoming),
        (
            "MATCH (n)<-[e:AUTHORED|MEMBER_OF]-(u:User {id: 1}) RETURN u, n, e",
            Direction::Outgoing,
        ),
    ] {
        let input = lower(statement);
        assert_eq!(input.query_type, QueryType::Neighbors, "{statement}");
        assert_eq!(ids(&input), ["u"], "{statement}");
        assert_eq!(
            input.neighbors.as_ref().unwrap().direction,
            direction,
            "{statement}"
        );
    }
    let input = lower("MATCH (u:User {id: 1})-[:AUTHORED|MEMBER_OF]-(n) RETURN n");
    assert_eq!(
        input.neighbors.unwrap().rel_types,
        ["AUTHORED", "MEMBER_OF"]
    );
}

#[test]
fn shortest_path_left_arrow_swaps_endpoints() {
    let input = lower(
        "MATCH p = shortestPath((a:Project {id: 1})<-[:CONTAINS*..2]-(b:Project {id: 2})) RETURN p",
    );
    let path = input.path.unwrap();
    assert_eq!(
        (path.from.as_str(), path.to.as_str(), path.max_depth),
        ("b", "a", 2)
    );
    assert_eq!(path.rel_types, ["CONTAINS"]);
}

#[test]
fn parameters_substitute_values_lists_and_limit() {
    let params: Parameters = json!({"id": 7, "states": ["a", "b"], "n": 5})
        .as_object()
        .unwrap()
        .clone();
    let input = lower_with(
        "MATCH (u:User {id: $id}) WHERE u.state IN $states RETURN u.username LIMIT $n",
        params,
    );
    assert_eq!(input.nodes[0].node_ids, [7]);
    assert_eq!(
        input.nodes[0].filters["state"][0].value,
        Some(json!(["a", "b"]))
    );
    assert_eq!(input.limit, 5);
    assert!(
        reject("MATCH (u:User {id: $id}) RETURN u")
            .to_string()
            .contains("$id is not bound")
    );
}

#[test]
fn query_hash_ignores_layout_and_tracks_bindings() {
    let hash = |statement: &str, params: serde_json::Value| {
        lower_with(statement, params.as_object().unwrap().clone())
            .compiler
            .query_hash
    };
    let base = hash(
        "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10",
        json!({}),
    );
    assert_eq!(
        base,
        hash(
            "match (u:User {id: 1})\n  // comment\n  return u.username limit 10",
            json!({})
        )
    );
    assert_ne!(
        base,
        hash(
            "MATCH (u:User {id: 1}) RETURN u.username LIMIT 11",
            json!({})
        )
    );
    assert_eq!(
        base,
        hash(
            "MATCH (u:User {id: $id}) RETURN u.username LIMIT 10",
            json!({"id": 1})
        )
    );
    assert_ne!(
        hash(
            "MATCH (u:User {id: $id}) RETURN u.username",
            json!({"id": 1})
        ),
        hash(
            "MATCH (u:User {id: $id}) RETURN u.username",
            json!({"id": 2})
        )
    );
}

#[test]
fn rejections_name_the_limitation() {
    let cases = [
        ("CREATE (n:User) RETURN n", "read-only"),
        ("MATCH (u:User) WITH u RETURN u", "WITH/UNWIND"),
        (
            "MATCH (u:User) RETURN u UNION MATCH (p:Project) RETURN p",
            "Composite",
        ),
        (
            "OPTIONAL MATCH (u:User) RETURN u",
            "OPTIONAL MATCH is not supported",
        ),
        (
            "MATCH (u:User) MATCH (p:Project) RETURN u",
            "only one MATCH",
        ),
        (
            "MATCH (u:User) RETURN DISTINCT u",
            "DISTINCT is not supported",
        ),
        ("MATCH (u:User) RETURN u SKIP 5", "SKIP/OFFSET"),
        (
            "MATCH (u:User) RETURN u ORDER BY u.username NULLS LAST",
            "NULLS FIRST/LAST",
        ),
        (
            "MATCH (u:User) RETURN u ORDER BY u.username, u.state",
            "exactly one key",
        ),
        (
            "MATCH (u:User) WHERE NOT u.state = 'a' RETURN u",
            "NOT is not supported",
        ),
        (
            "MATCH (u:User) WHERE u.state = 'a' OR u.state = 'b' RETURN u",
            "OR/XOR",
        ),
        ("MATCH (u:User) WHERE u.state <> 'a' RETURN u", "not-equal"),
        ("MATCH (u:User) WHERE u.state =~ 'a.*' RETURN u", "regex"),
        ("MATCH (u:User) WHERE u.state = NULL RETURN u", "IS NULL"),
        ("MATCH (u:User) RETURN count(*)", "count(*)"),
        ("MATCH () RETURN 1", "needs a variable"),
        ("MATCH (u:User:Admin) RETURN u", "exactly one label"),
        ("MATCH p = (u:User)-->(n:Note) RETURN p", "shortestPath()"),
        (
            "MATCH (u:User)-[:AUTHORED]->{1,3}(n:Note) RETURN u",
            "GQL quantifier",
        ),
        ("MATCH (u:User)->(n:Note) RETURN u", "GQL abbreviations"),
        ("MATCH (u:User WHERE u.id = 1) RETURN u", "statement WHERE"),
        (
            "MATCH (u:User) WHERE u.created_at > DATE '2024-01-01' RETURN u",
            "GQL literal",
        ),
        (
            "MATCH allShortestPaths((a:Project {id: 1})-[:CONTAINS]->(b:Project {id: 2})) RETURN a",
            "allShortestPaths",
        ),
        (
            "MATCH (u:User) RETURN u.state, count(u) GROUP BY u.state",
            "GROUP BY",
        ),
        ("MATCH (u:user {id: 1}) RETURN u", "did you mean `User`"),
        ("MATCH (u:User {id: 1}) RETURN u.Username", "does not exist"),
        (
            "MATCH (u:User {nope: 1}) RETURN u",
            "unknown property `nope`",
        ),
        (
            "MATCH (u:User {id: 1})-[:AUTHORS]->(n:Note) RETURN u",
            "unknown relationship type",
        ),
        (
            "MATCH (u:User {id: 1}) RETURN u.username AS name",
            "AS is only supported in aggregations",
        ),
        (
            "MATCH (u:User {id: 1})-[:AUTHORED]-(n:Note) RETURN u, count(n)",
            "undirected",
        ),
        (
            "MATCH (u:User {id: 1})-[:AUTHORED]->(n)-[:AUTHORED]->(m:Note) RETURN u",
            "needs a label",
        ),
        (
            "MATCH (u:User {id: 1})-->(n {x: 1}) RETURN n",
            "discovered endpoint",
        ),
        (
            "MATCH (u:User {id: 1})-[:MEMBER_OF*]->(g:Group) RETURN g",
            "unbounded",
        ),
        (
            "MATCH (u:User {id: 1})-[:MEMBER_OF*0..2]->(g:Group) RETURN g",
            "start at 1",
        ),
        (
            "MATCH shortestPath((a:Project {id: 1})-->(b:Project {id: 2})) RETURN a",
            "needs a type",
        ),
        ("MATCH (u:User {id: 1}) RETURN u LIMIT 0", "out of range"),
        ("MATCH (`end`:User {id: 1}) RETURN end", "backticks"),
        ("MATCH (`bad-name`:User {id: 1}) RETURN u", "must match"),
        (
            "MATCH (u:User) WHERE u.state = u.username RETURN u",
            "comparing two properties",
        ),
        (
            "MATCH (u:User) WHERE toLower(u.state) = 'a' RETURN u",
            "not allowed in WHERE",
        ),
        ("MATCH (u:User) RETURN u {.username}", "Map projection"),
        (
            "MATCH (u:User) WHERE u.state IN ['a', ['b']] RETURN u",
            "nested",
        ),
    ];
    for (statement, expected) in cases {
        let err = reject(statement);
        assert!(err.is_client_safe(), "{statement}: {err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains(expected),
            "{statement}\nexpected {expected:?} in: {msg}"
        );
    }
}

#[test]
fn error_classes_match_the_json_frontend() {
    assert!(matches!(
        reject("MATCH (u:Nope {id: 1}) RETURN u"),
        QueryError::AllowlistRejected(_)
    ));
    assert!(matches!(
        reject("MATCH (u:User {id: 1}) RETURN v"),
        QueryError::ReferenceError(_)
    ));
    assert!(matches!(
        reject("MATCH (u:User RETURN u"),
        QueryError::Syntax(_)
    ));
    let list = (0..101)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    assert!(matches!(
        reject(&format!("MATCH (u:User) WHERE u.id IN [{list}] RETURN u")),
        QueryError::LimitExceeded(_)
    ));
}

#[test]
fn parser_caps() {
    let deep = format!(
        "MATCH (u:User) WHERE {}u.id = 1{} RETURN u",
        "(".repeat(MAX_NESTING_DEPTH + 1),
        ")".repeat(MAX_NESTING_DEPTH + 1)
    );
    let err = reject(&deep);
    assert!(
        matches!(err, QueryError::Syntax(_)) && err.to_string().contains("nest deeper"),
        "{err}"
    );

    let long = format!(
        "MATCH (u:User) RETURN u // {}",
        "x".repeat(MAX_STATEMENT_BYTES)
    );
    assert!(reject(&long).to_string().contains("maximum is"));
}

#[test]
fn syntax_errors_report_position_and_expectation() {
    let err = reject("MATCH (u:User) RETRUN u");
    let msg = err.to_string();
    assert!(msg.contains("line 1, column 16"), "{msg}");
    assert!(msg.contains("unexpected `RETRUN`"), "{msg}");
    assert!(msg.contains("RETURN"), "{msg}");
}
