use compiler::dag::Backend;
use query_engine::shared::dag::Event;

use super::{caller, run};

#[tokio::test]
async fn the_side_a_where_conjunct_bounds_reads_first() {
    let ran = run(
        "MATCH (mr:MergeRequest)<-[:DEVELOPED_IN]-(i:JiraIssue)<-[:MENTIONS_ISSUE]-(m:SlackMessage) WHERE i.key = 'ORB-1' AND m.text CONTAINS 'deploy' RETURN mr.iid, i.key, m.text LIMIT 10",
        &caller("1/9970/"),
        Vec::new(),
    )
    .await;
    let stages = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .expect("the query splits");
    assert_eq!(stages[0].backend, Backend::DuckDb);
    assert!(
        stages[0].logical[0].ends_with("-> [i]"),
        "{:?}",
        stages[0].logical
    );
}

#[tokio::test]
async fn a_condition_on_an_edge_that_crosses_connections_fails() {
    let ran = run(
        "MATCH (mr:MergeRequest)<-[e:DEVELOPED_IN]-(i:JiraIssue) WHERE mr.id <= 2002 AND e.target_id = 2000 RETURN mr.iid, i.key LIMIT 10",
        &caller("1/9970/"),
        Vec::new(),
    )
    .await;
    assert_eq!(
        ran.error(),
        "schema violation: a condition on e reads an edge that crosses connections, which no stage holds"
    );
}

#[tokio::test]
async fn a_step_needs_the_key_its_holder_stores_in_either_direction() {
    let split = "allowlist rejected: no one store of SlackMessage serves both a contains match on SlackMessage.text and a read of SlackMessage.merge_request_id";
    for query in [
        "MATCH (m:SlackMessage)-[:DISCUSSES]->(mr:MergeRequest) WHERE m.text CONTAINS 'deploy' RETURN mr.iid, m.text LIMIT 10",
        "MATCH (mr:MergeRequest)<-[:DISCUSSES]-(m:SlackMessage) WHERE m.text CONTAINS 'deploy' RETURN mr.iid, m.text LIMIT 10",
    ] {
        assert_eq!(
            run(query, &caller("1/9970/"), Vec::new()).await.error(),
            split
        );
    }
}

#[tokio::test]
async fn a_key_the_target_holds_leaves_the_source_on_the_store_its_needs_pick() {
    let ran = run(
        "MATCH (i:JiraIssue)-[:CITES]->(r:GitLabRef) WHERE i.description CONTAINS 'login' RETURN i.key, r.url LIMIT 10",
        &caller("1/9970/"),
        Vec::new(),
    )
    .await;
    let stages = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .expect("the query splits");
    let reads: Vec<(Backend, bool)> = stages
        .iter()
        .filter(|stage| stage.backend != Backend::Combine)
        .map(|stage| (stage.backend, stage.logical[0].ends_with("-> [i]")))
        .collect();
    assert!(reads.contains(&(Backend::Live, true)), "{reads:?}");
    assert!(reads.contains(&(Backend::DuckDb, false)), "{reads:?}");
}
