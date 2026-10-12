use super::*;
use crate::auth::claims::TraversalPathClaim;
use crate::proto::{ExecuteQueryRequest, execute_query_message};

#[tokio::test]
async fn predicate_limit_returns_recovery_guidance_over_the_query_stream() {
    let mut client = serve(test_service()).await;
    let comparisons = vec!["a.id = b.id"; 16].join(" AND ");
    let gql = format!(
        "MATCH (a:Definition {{id: 1}})-[:CALLS]->(b:Definition) WHERE {comparisons} RETURN a.id"
    );
    let json = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "a", "entity": "Definition", "node_ids": [1], "filters": {
            "name": vec![serde_json::json!({"eq": "caller"}); 8],
            "fqn": vec![serde_json::json!({"eq": "main.caller"}); 8]
        }}]
    })
    .to_string();
    for (query, language) in [(gql, QueryLanguage::Gql), (json, QueryLanguage::Json)] {
        let message = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Request(
                ExecuteQueryRequest {
                    query,
                    language: language as i32,
                    ..Default::default()
                },
            )),
        };
        let request = signed_request(
            tokio_stream::iter([message]),
            Claims {
                source_type: SourceType::Mcp,
                group_traversal_ids: vec![TraversalPathClaim {
                    path: TraversalPath::new_unchecked("1/"),
                    access_levels: vec![20],
                }],
                ..test_claims()
            },
        );
        let mut responses = client.execute_query(request).await.unwrap().into_inner();
        let response = responses.message().await.unwrap().unwrap();
        let Some(execute_query_message::Content::Error(error)) = response.content else {
            panic!("expected a query error, got {response:?}");
        };
        assert_eq!(error.code, "compile_error");
        assert_eq!(
            error.message,
            "schema violation: Query has 17 predicates; the maximum is 16 across the entire query. Remove redundant conditions or simplify the query."
        );
        assert!(responses.message().await.unwrap().is_none());
    }
}
