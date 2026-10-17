use super::*;
use crate::proto::execute_query_message::Content;
use crate::proto::execute_query_result::Content::ResultJson;
use crate::proto::{ExecuteQueryRequest, ExecuteQueryResult};

async fn schema_query(chunked_result: bool) -> Vec<Content> {
    let message = ExecuteQueryMessage {
        content: Some(Content::Request(ExecuteQueryRequest {
            query: "CALL db.schema()".into(),
            language: QueryLanguage::Gql as i32,
            chunked_result,
            ..Default::default()
        })),
    };
    let request = signed_request(tokio_stream::iter([message]), test_claims());
    let mut responses = serve(test_service())
        .await
        .execute_query(request)
        .await
        .unwrap()
        .into_inner();
    let mut contents = Vec::new();
    while let Some(message) = responses.message().await.unwrap() {
        contents.extend(message.content);
    }
    contents
}

#[tokio::test]
async fn chunked_result_sends_the_same_text_before_a_metadata_only_result() {
    let unchunked = schema_query(false).await;
    let [
        Content::Result(ExecuteQueryResult {
            content: Some(ResultJson(whole)),
            metadata,
        }),
    ] = unchunked.as_slice()
    else {
        panic!("expected one result message");
    };

    let mut contents = schema_query(true).await;
    let Some(Content::Result(last)) = contents.pop() else {
        panic!("expected a final result message");
    };
    let text: String = contents
        .into_iter()
        .map(|content| match content {
            Content::ResultChunk(chunk) => chunk.data,
            other => panic!("expected a result chunk, got {other:?}"),
        })
        .collect();

    assert_eq!(&text, whole);
    assert_eq!(last.content, None);
    assert_eq!(&last.metadata, metadata);
}
