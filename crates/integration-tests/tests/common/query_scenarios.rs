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
    pages: Vec<BTreeMap<String, Vec<i64>>>,
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
    let expected_pages = scenario.expect.pages;
    assert!(
        !expected_pages.is_empty(),
        "{name}: declare at least one expected page"
    );
    let expected_node_count: usize = expected_pages
        .iter()
        .flat_map(|page| page.values())
        .map(Vec::len)
        .sum();

    let mut full_query = scenario.query.clone();
    full_query["cursor"]["page_size"] = expected_node_count.max(1).into();
    let unrestricted = QueryClient::new(context, GrpcConfig::default().max_query_response_bytes);
    let mut full_page = unrestricted.query(&full_query).await;
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
        expected_pages.len() > 1,
        "{name}: fixture size must exercise the expected budget behavior"
    );

    let limited = QueryClient::new(context, scenario.max_response_bytes);
    let mut query = scenario.query;
    let mut page = limited.query(&query).await;

    let mut returned_nodes = Vec::new();
    let mut seen_cursors = BTreeSet::new();

    for (page_index, expected_nodes) in expected_pages.iter().enumerate() {
        assert!(
            page.encoded_bytes <= scenario.max_response_bytes,
            "{name}: response exceeds byte budget"
        );
        let mut actual_nodes: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for node in &page.response.nodes {
            actual_nodes
                .entry(node.entity_type.clone())
                .or_default()
                .push(node.id);
        }
        assert_eq!(
            &actual_nodes,
            expected_nodes,
            "{name}: page {} node IDs",
            page_index + 1
        );
        returned_nodes.extend(page.response.nodes);

        let pagination = page.response.pagination.unwrap();
        let more_pages_expected = page_index + 1 < expected_pages.len();
        assert_eq!(
            pagination.has_more, more_pages_expected,
            "{name}: page exhaustion"
        );
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
            seen_cursors.insert(cursor.clone()),
            "{name}: cursor must advance"
        );
        query["cursor"]["after"] = cursor.into();
        page = limited.query(&query).await;
    }

    returned_nodes.sort_by_key(|node| (node.entity_type.clone(), node.id));
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
