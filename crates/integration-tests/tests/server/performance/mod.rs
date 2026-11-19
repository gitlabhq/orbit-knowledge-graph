use std::collections::BTreeMap;
use std::path::Path;

use query_engine::compiler::{Frontend, SecurityContext, compile};
use serde::Deserialize;

const SCENARIOS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/performance/scenarios"
);
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PerformanceScenario {
    description: String,
    query: BTreeMap<String, String>,
}

#[test]
fn performance_queries() {
    let ontology = integration_testkit::load_ontology();
    let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let files = integration_testkit::scenario::yaml_files(Path::new(SCENARIOS));
    assert!(!files.is_empty());

    for file in files {
        let raw = std::fs::read_to_string(&file).unwrap();
        let scenario: PerformanceScenario = orbit_utils::yaml::from_str(&raw)
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
        assert_eq!(scenario.query.len(), 1, "{}", file.display());
        let query = scenario
            .query
            .get("json")
            .unwrap_or_else(|| panic!("{}: missing query.json", file.display()));
        compile(query, Frontend::JsonDsl, &ontology, &security).unwrap_or_else(|error| {
            panic!("{}: {}: {error}", file.display(), scenario.description)
        });
    }
}
