use std::collections::{BTreeMap, BTreeSet};

use integration_testkit::{TestContext, load_seed_sql};
use orbit_server_config::GrpcConfig;
use serde::Deserialize;
use serde_json::Value;

use super::query_client::QueryClient;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryScenario {
    seed: String,
    query: Value,
    max_response_bytes: usize,
    expect: Expectations,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    paginated: bool,
    nodes: BTreeMap<String, Vec<i64>>,
}

pub async fn run_scenarios(context: &TestContext, yaml: &str) {
    let scenarios: BTreeMap<String, QueryScenario> = orbit_utils::yaml::from_str(yaml).unwrap();
    assert!(!scenarios.is_empty(), "query scenarios must not be empty");

    for (name, scenario) in scenarios {
        eprintln!("--- query response: {name}");
        let database = context.fork(&format!("query_response_{name}")).await;
        load_seed_sql(&database, &scenario.seed).await;
        run_scenario(&database, scenario, &name).await;
    }
}

async fn run_scenario(context: &TestContext, scenario: QueryScenario, name: &str) {
    let mut expected_nodes: Vec<_> = scenario
        .expect
        .nodes
        .into_iter()
        .flat_map(|(entity, ids)| ids.into_iter().map(move |id| (entity.clone(), id)))
        .collect();
    expected_nodes.sort();

    let unrestricted = QueryClient::new(context, GrpcConfig::default().max_query_response_bytes);
    let mut full_page = unrestricted.query(&scenario.query).await;
    assert!(
        !full_page.response.pagination.as_ref().unwrap().has_more,
        "{name}: reference must contain all results"
    );
    assert!(
        full_page.response.edges.is_empty(),
        "{name}: response scenarios require node-only queries"
    );
    assert_eq!(
        full_page.encoded_bytes > scenario.max_response_bytes,
        scenario.expect.paginated,
        "{name}: fixture size must exercise the expected budget behavior"
    );

    let limited = QueryClient::new(context, scenario.max_response_bytes);
    let mut query = scenario.query;
    let mut page = limited.query(&query).await;
    assert_eq!(
        page.response.pagination.as_ref().unwrap().has_more,
        scenario.expect.paginated,
        "{name}: first-page pagination"
    );
    if scenario.expect.paginated {
        assert!(
            page.response.nodes.len() < full_page.response.nodes.len(),
            "{name}: first page must be smaller"
        );
    }

    let mut returned_nodes = Vec::new();
    let mut seen_cursors = BTreeSet::new();

    for page_number in 0..expected_nodes.len().max(1) {
        assert!(
            page.encoded_bytes <= scenario.max_response_bytes,
            "{name}: response exceeds byte budget"
        );
        assert!(
            !page.response.nodes.is_empty() || expected_nodes.is_empty(),
            "{name}: nonempty results must make progress"
        );
        returned_nodes.extend(page.response.nodes);

        let pagination = page.response.pagination.unwrap();
        assert_eq!(
            pagination.has_more,
            pagination.next_cursor.is_some(),
            "{name}: continuation cursor"
        );
        assert_eq!(
            pagination.truncated, pagination.has_more,
            "{name}: truncation metadata"
        );

        let Some(cursor) = pagination.next_cursor else {
            break;
        };
        assert!(
            page_number + 1 < expected_nodes.len(),
            "{name}: pagination must terminate"
        );
        assert!(
            seen_cursors.insert(cursor.clone()),
            "{name}: cursor must advance"
        );
        query["cursor"]["after"] = cursor.into();
        page = limited.query(&query).await;
    }

    returned_nodes.sort_by_key(|node| (node.entity_type.clone(), node.id));
    let identities: Vec<_> = returned_nodes
        .iter()
        .map(|node| (node.entity_type.clone(), node.id))
        .collect();
    assert_eq!(
        identities, expected_nodes,
        "{name}: every expected node must appear exactly once"
    );

    full_page
        .response
        .nodes
        .sort_by_key(|node| (node.entity_type.clone(), node.id));
    assert_eq!(
        serde_json::to_value(returned_nodes).unwrap(),
        serde_json::to_value(full_page.response.nodes).unwrap(),
        "{name}: pagination must preserve node properties"
    );
}
