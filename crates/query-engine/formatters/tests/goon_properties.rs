use std::sync::LazyLock;

use proptest::prelude::*;
use semver::Version;
use serde_json::{Map, json};

use formatters::{GraphEdge, GraphNode, GraphResponse};

static FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| Version::new(1, 0, 0));

fn run(response: &GraphResponse) -> String {
    formatters::goon_encode(response, &FORMAT_VERSION, &[])
}

fn arb_node() -> impl Strategy<Value = GraphNode> {
    (
        prop_oneof![
            Just("User".to_string()),
            Just("MergeRequest".to_string()),
            Just("Project".to_string()),
            Just("Note".to_string()),
        ],
        any::<i32>().prop_map(|n| n as i64),
        prop::collection::vec(("[a-z_]{3,12}", any::<i32>()), 0..6),
    )
        .prop_map(|(entity_type, id, raw_props)| {
            let mut properties = Map::new();
            for (k, v) in raw_props {
                properties.insert(k, json!(v));
            }
            GraphNode {
                entity_type,
                id,
                properties,
            }
        })
}

fn arb_edge() -> impl Strategy<Value = GraphEdge> {
    (
        prop_oneof![
            Just("AUTHORED".to_string()),
            Just("IN_PROJECT".to_string()),
            Just("MEMBER_OF".to_string()),
            Just("CONTAINS".to_string()),
        ],
        prop_oneof![
            Just("User".to_string()),
            Just("MergeRequest".to_string()),
            Just("Project".to_string()),
        ],
        any::<i32>().prop_map(|n| n as i64),
        prop_oneof![
            Just("MergeRequest".to_string()),
            Just("Project".to_string()),
            Just("Group".to_string()),
        ],
        any::<i32>().prop_map(|n| n as i64),
    )
        .prop_map(|(edge_type, from, from_id, to, to_id)| GraphEdge {
            from,
            from_id,
            to,
            to_id,
            edge_type,
            depth: None,
            path_id: None,
            step: None,
        })
}

fn arb_response() -> impl Strategy<Value = GraphResponse> {
    (
        prop::collection::vec(arb_node(), 0..15),
        prop::collection::vec(arb_edge(), 0..30),
    )
        .prop_map(|(nodes, edges)| GraphResponse {
            format_version: "1.2.0".into(),
            query_type: "traversal".into(),
            nodes,
            edges,
            columns: None,
            group_columns: None,
            rows: None,
            pagination: None,
        })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..Default::default()
    })]

    #[test]
    fn shuffle_invariant(response in arb_response(), seed in any::<u64>()) {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        let canonical = run(&response);

        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut shuffled = GraphResponse {
            format_version: response.format_version.clone(),
            query_type: response.query_type.clone(),
            nodes: response.nodes.clone(),
            edges: response.edges.clone(),
            columns: response.columns.clone(),
            group_columns: response.group_columns.clone(),
            rows: response.rows.clone(),
            pagination: None,
        };
        shuffled.nodes.shuffle(&mut rng);
        shuffled.edges.shuffle(&mut rng);
        prop_assert_eq!(canonical, run(&shuffled));
    }

    #[test]
    fn no_unescaped_control_chars(response in arb_response()) {
        let out = run(&response);
        for line in out.lines() {
            prop_assert!(
                !line.contains('\r') && !line.contains('\t'),
                "line contains unescaped control char: {:?}", line
            );
        }
    }

    #[test]
    fn encoding_is_pure(response in arb_response()) {
        prop_assert_eq!(run(&response), run(&response));
    }

    #[test]
    fn output_starts_with_header(response in arb_response()) {
        let out = run(&response);
        prop_assert!(out.starts_with("@header\n"));
    }
}
