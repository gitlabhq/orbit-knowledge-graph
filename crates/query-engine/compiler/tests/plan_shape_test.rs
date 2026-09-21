use std::path::PathBuf;
use std::sync::Arc;

fn scenario_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/plan_shape")
}

fn load_scenarios() -> Vec<(String, serde_json::Value)> {
    let dir = scenario_dir();
    let mut scenarios = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("plan_shape dir") {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "yaml") {
            let content = std::fs::read_to_string(&path).unwrap();
            let doc: serde_json::Value =
                serde_saphyr::from_str(&content).unwrap_or_else(|e| panic!("parse {:?}: {e}", path));
            let name = doc["name"].as_str().unwrap_or("unnamed").to_string();
            scenarios.push((name, doc));
        }
    }
    scenarios.sort_by(|a, b| a.0.cmp(&b.0));
    scenarios
}

fn shape_matches(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    match (actual, expected) {
        (serde_json::Value::Object(a), serde_json::Value::Object(e)) => {
            e.iter().all(|(k, v)| a.get(k).is_some_and(|av| shape_matches(av, v)))
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(e)) => {
            a.len() == e.len()
                && a.iter().zip(e.iter()).all(|(av, ev)| shape_matches(av, ev))
        }
        _ => actual == expected,
    }
}

fn collect_tables(shape: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(t) = shape.get("table").and_then(|t| t.as_str()) {
        out.push(t.to_string());
    }
    for key in &["input", "left", "right", "body", "consumer"] {
        if let Some(child) = shape.get(*key) { out.extend(collect_tables(child)); }
    }
    if let Some(arms) = shape.get("arms").and_then(|a| a.as_array()) {
        for arm in arms { out.extend(collect_tables(arm)); }
    }
    out
}

fn security_ctx() -> compiler::types::SecurityContext {
    compiler::types::SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

#[test]
fn plan_shape_scenarios() {
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology"));
    let ctx = security_ctx();

    for (name, doc) in load_scenarios() {
        let json_str = doc["input"]["json"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: missing input.json"));

        // Compile fully to get the normalized Input, then re-run plan to get PhysOp.
        let compiled = compiler::compile(json_str, compiler::Frontend::JsonDsl, &ontology, &ctx)
            .unwrap_or_else(|e| panic!("{name}: compile failed: {e}"));

        let mut input = compiled.input.clone();
        let (_, phys_op) = compiler::passes::plan_v2::plan(&mut input, &ontology)
            .unwrap_or_else(|e| panic!("{name}: plan failed: {e}"));

        let actual = phys_op.shape();
        let expected = &doc["expected"];

        assert!(
            shape_matches(&actual, expected),
            "{name}: shape mismatch\nexpected:\n{}\nactual:\n{}",
            serde_json::to_string_pretty(expected).unwrap(),
            serde_json::to_string_pretty(&actual).unwrap(),
        );

        if let Some(absent) = doc.get("absent").and_then(|a| a.as_array()) {
            let tables = collect_tables(&actual);
            for item in absent {
                if let Some(t) = item.get("table").and_then(|t| t.as_str()) {
                    assert!(
                        !tables.contains(&t.to_string()),
                        "{name}: table '{t}' should be absent\nactual:\n{}",
                        serde_json::to_string_pretty(&actual).unwrap(),
                    );
                }
            }
        }
    }
}
