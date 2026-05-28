use std::time::Duration;

use gkg_analytics::{OrbitCommonContext, OrbitQueryContext, orbit_common, orbit_query};
use labkit_events::Tracker;
use serde_json::Value;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyDataSource, CopyTargetOptions, GenericImage, ImageExt};
use tokio::time::sleep;

const MICRO_IMAGE: &str = "snowplow/snowplow-micro";
const MICRO_TAG: &str = "2.1.2";
const MICRO_PORT: u16 = 9090;

const IGLU_CONFIG: &str = r#"{
  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-3",
  "data": {
    "cacheSize": 500,
    "repositories": [
      {
        "name": "Iglu Central",
        "priority": 0,
        "vendorPrefixes": ["com.snowplowanalytics"],
        "connection": {"http": {"uri": "http://iglucentral.com"}}
      },
      {
        "name": "GitLab",
        "priority": 5,
        "vendorPrefixes": ["com.gitlab"],
        "connection": {"http": {"uri": "https://gitlab-org.gitlab.io/iglu"}}
      }
    ]
  }
}"#;

struct Micro {
    _container: ContainerAsync<GenericImage>,
    base_url: String,
}

async fn start_micro() -> Micro {
    let container = GenericImage::new(MICRO_IMAGE, MICRO_TAG)
        .with_exposed_port(ContainerPort::Tcp(MICRO_PORT))
        .with_wait_for(WaitFor::message_on_stderr("started at http://"))
        .with_cmd(["--iglu", "/config/iglu.json"])
        .with_copy_to(
            CopyTargetOptions::new("/config/iglu.json"),
            CopyDataSource::Data(IGLU_CONFIG.as_bytes().to_vec()),
        )
        .start()
        .await
        .expect("snowplow-micro start");

    let host = container.get_host().await.expect("micro host");
    let port = container
        .get_host_port_ipv4(MICRO_PORT)
        .await
        .expect("micro port");
    let host = if host.to_string() == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    };
    let base_url = format!("http://{host}:{port}");
    Micro {
        _container: container,
        base_url,
    }
}

async fn micro_counts(http: &reqwest::Client, base_url: &str) -> (u64, u64) {
    let body: Value = http
        .get(format!("{base_url}/micro/all"))
        .send()
        .await
        .expect("GET /micro/all")
        .json()
        .await
        .expect("micro json");
    (
        body.get("good").and_then(Value::as_u64).unwrap_or(0),
        body.get("bad").and_then(Value::as_u64).unwrap_or(0),
    )
}

async fn micro_good_events(http: &reqwest::Client, base_url: &str) -> Vec<Value> {
    http.get(format!("{base_url}/micro/good"))
        .send()
        .await
        .expect("GET /micro/good")
        .json()
        .await
        .expect("good json")
}

fn init_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

#[tokio::test]
async fn snowplow_micro_receives_gkg_query_executed() {
    init_crypto_provider();
    let micro = start_micro().await;
    let http = reqwest::Client::new();

    let tracker = Tracker::builder(&micro.base_url, "gkg-analytics-it")
        .batch_size(1)
        .build()
        .expect("tracker build");

    let common = OrbitCommonContext::new(orbit_common::OrbitCommon {
        deployment_type: orbit_common::OrbitCommonDeploymentType::Com,
        environment: "staging".parse().expect("environment"),
        correlation_id: Some("corr-it-1".parse().expect("correlation_id")),
        instance_id: Some("inst-it".parse().expect("instance_id")),
        unique_instance_id: None,
        host_name: None,
        organization_id: Some(42),
        root_namespace_ids: Some(vec![99]),
        schema_version: None,
    });

    let query = OrbitQueryContext::new(orbit_query::OrbitQuery {
        source_type: orbit_query::OrbitQuerySourceType::Mcp,
        tool_name: Some("query_graph".parse().expect("tool_name")),
        coding_agent: None,
        queried_namespace_ids: None,
        root_namespace_id: Some(99),
        global_user_id: Some("guser-it".parse().expect("global_user_id")),
        session_id: Some("sess-it".parse().expect("session_id")),
        user_type: None,
        plan: None,
        is_gitlab_team_member: None,
    });

    let event = labkit_events::StructuredEvent::builder("gkg", "gkg_query_executed")
        .context(common)
        .context(query)
        .build()
        .expect("event build");

    let (good_before, bad_before) = micro_counts(&http, &micro.base_url).await;
    tracker.track_structured_event(event).expect("track");
    tracker.shutdown().await;

    let (mut good, mut bad) = (good_before, bad_before);
    for _ in 0..30 {
        (good, bad) = micro_counts(&http, &micro.base_url).await;
        if good > good_before || bad > bad_before {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        bad, bad_before,
        "event landed in bad bucket — schema validation failed"
    );
    assert_eq!(
        good,
        good_before + 1,
        "micro did not receive one good event (good={good}, bad={bad})"
    );

    let events = micro_good_events(&http, &micro.base_url).await;
    let last = events.last().expect("at least one good event");
    let event = &last["event"];
    assert_eq!(event["se_category"], "gkg");
    assert_eq!(event["se_action"], "gkg_query_executed");

    let context_schemas: Vec<String> = last["contexts"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        context_schemas
            .iter()
            .any(|s| s == gkg_analytics::ORBIT_COMMON_SCHEMA),
        "missing orbit_common context, contexts={context_schemas:?}"
    );
    assert!(
        context_schemas
            .iter()
            .any(|s| s == gkg_analytics::ORBIT_QUERY_SCHEMA),
        "missing orbit_query context, contexts={context_schemas:?}"
    );

    let context_data = event["contexts"]["data"]
        .as_array()
        .expect("contexts.data array");
    let common_data = context_data
        .iter()
        .find(|c| {
            c["schema"]
                .as_str()
                .is_some_and(|s| s.contains("orbit_common"))
        })
        .expect("orbit_common entity");
    assert_eq!(common_data["data"]["deployment_type"], ".com");
    assert_eq!(common_data["data"]["environment"], "staging");
    assert_eq!(common_data["data"]["organization_id"], 42);
}
