use compiler::{Frontend, QueryError, compile, compile_local};

use super::setup::{embedded_ontology, test_ctx};

fn assert_budget(query: &str, count: usize) {
    let ontology = embedded_ontology();
    for result in [
        compile(query, Frontend::Gql, &ontology, &test_ctx()),
        compile_local(query, Frontend::Gql, &ontology),
    ] {
        if count == 16 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(QueryError::Validation(message))
                if message == "Query has 17 predicates; the maximum is 16 across the entire query. Remove redundant conditions or simplify the query."));
        }
    }
}

#[test]
fn predicate_budget_combines_filters_across_matches() {
    for count in [16, 17] {
        let comparisons = vec!["a.id = b.id"; count - 4].join(" AND ");
        let query = format!(
            "MATCH (a:Definition {{id: 1}})-[r:CALLS]->(b:Definition)
             WHERE b.name = 'callee' AND a.start_line <= a.end_line
             MATCH (a) WHERE r.source_id >= 1 AND {comparisons} RETURN a.id, b.id"
        );
        assert_budget(&query, count);
    }
}

#[test]
fn predicate_budget_preserves_id_list_and_range_counts() {
    for count in [16, 17] {
        let remaining = count - 11;
        let filters = [
            vec!["a.name = 'caller'"; 8],
            vec!["a.fqn = 'main.caller'"; remaining],
        ]
        .concat()
        .join(" AND ");
        let query = format!(
            "MATCH (a:Definition) WHERE a.id IN [1, 2] AND a.id >= 1 AND a.id <= 100
             AND {filters} RETURN a.id"
        );
        assert_budget(&query, count);
    }
}
