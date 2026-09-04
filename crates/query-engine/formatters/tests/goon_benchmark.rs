use std::hint::black_box;
use std::time::Instant;

use formatters::{GraphNode, GraphResponse, goon_encode};
use semver::Version;
use serde_json::{Map, Value};

fn representative_response(node_count: usize) -> GraphResponse {
    let nodes = (0..node_count)
        .map(|index| {
            let mut properties = Map::new();
            properties.insert("iid".into(), Value::from(index));
            properties.insert(
                "full_path".into(),
                Value::from(format!("gitlab-org/orbit/project-{index}")),
            );
            properties.insert(
                "title".into(),
                Value::from(format!(
                    "Improve representative query formatting for project {index} with clean text"
                )),
            );
            properties.insert(
                "description".into(),
                Value::from(format!(
                    "Node {index}: {} unicode café 日本語 and escaped newline\ncontinuation",
                    "representative property graph response text ".repeat(3)
                )),
            );
            properties.insert(
                "web_url".into(),
                Value::from(format!(
                    "https://gitlab.com/gitlab-org/orbit/project-{index}"
                )),
            );
            GraphNode {
                entity_type: "Project".into(),
                id: index as i64,
                properties,
            }
        })
        .collect();

    GraphResponse {
        format_version: "1.2.0".into(),
        query_type: "traversal".into(),
        nodes,
        edges: Vec::new(),
        columns: None,
        group_columns: None,
        rows: None,
        pagination: None,
    }
}

#[test]
#[ignore = "manual release-mode performance measurement"]
fn measure_goon_encoder() {
    let response = representative_response(10_000);
    let version = Version::new(1, 2, 0);
    let expected = goon_encode(&response, &version);
    let runs = std::env::var("GOON_BENCH_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);

    black_box(goon_encode(&response, &version));
    for run in 0..runs {
        let start = Instant::now();
        let output = black_box(goon_encode(black_box(&response), black_box(&version)));
        let elapsed = start.elapsed();
        assert_eq!(output, expected);
        println!(
            "goon_benchmark,{run},{},{:.6}",
            output.len(),
            elapsed.as_secs_f64()
        );
    }
}
