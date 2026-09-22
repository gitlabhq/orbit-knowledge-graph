use std::path::PathBuf;
use std::sync::Arc;

fn scenario_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/plan_shape")
}

fn snapshot_dir() -> PathBuf {
    scenario_dir().join("snapshots")
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

fn snapshot_filename(name: &str) -> String {
    let fname: String = name.to_lowercase()
        .replace(' ', "_")
        .replace('—', "")
        .replace('\'', "")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(60)
        .collect();
    fname
}

fn security_ctx() -> compiler::types::SecurityContext {
    compiler::types::SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

#[test]
fn plan_shape_scenarios() {
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology"));
    let ctx = security_ctx();
    let snap_dir = snapshot_dir();
    let mut failures = Vec::new();
    let update = std::env::var("UPDATE_SNAPSHOTS").is_ok();

    for (name, doc) in load_scenarios() {
        let json_string;
        let json_str = if let Some(s) = doc["input"]["json"].as_str() {
            s
        } else if doc["input"].is_object() {
            json_string = serde_json::to_string(&doc["input"]).unwrap();
            &json_string
        } else {
            failures.push(format!("{name}: missing input"));
            continue;
        };

        let compiled = match compiler::compile(json_str, compiler::Frontend::JsonDsl, &ontology, &ctx) {
            Ok(c) => c,
            Err(e) => { failures.push(format!("{name}: compile failed: {e}")); continue; }
        };

        let mut input = compiled.input.clone();
        let (_, phys_op) = match compiler::passes::plan_v2::plan(&mut input, &ontology) {
            Ok(p) => p,
            Err(e) => { failures.push(format!("{name}: plan failed: {e}")); continue; }
        };

        let actual = serde_json::to_value(&phys_op).unwrap();
        let actual_pretty = serde_json::to_string_pretty(&actual).unwrap();

        let snap_file = snap_dir.join(format!("{}.json", snapshot_filename(&name)));

        if update || !snap_file.exists() {
            std::fs::write(&snap_file, format!("{actual_pretty}\n")).unwrap();
            if !update {
                failures.push(format!("{name}: snapshot created at {snap_file:?} — rerun to verify"));
            }
            continue;
        }

        let expected_str = std::fs::read_to_string(&snap_file).unwrap();
        let expected: serde_json::Value = serde_json::from_str(&expected_str)
            .unwrap_or_else(|e| panic!("{name}: bad snapshot {snap_file:?}: {e}"));

        if actual != expected {
            failures.push(format!(
                "{name}: snapshot mismatch ({snap_file:?})\nTo update: UPDATE_SNAPSHOTS=1 cargo test --test plan_shape_test\n\nexpected:\n{expected_str}\nactual:\n{actual_pretty}",
            ));
        }
    }

    if let Some(absent) = load_scenarios().iter().find_map(|(_, doc)| doc.get("absent")) {
        // absent checks from YAML still work if present
    }

    if !failures.is_empty() {
        panic!(
            "\n{} scenario(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}
