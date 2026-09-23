mod format;
mod pattern;

use format::PlanScenario;
use query_engine::compiler::{self, Frontend};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn run_dir(root: &str) {
    let root = Path::new(root);
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology"));
    let security = compiler::types::SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    assert!(!files.is_empty(), "no plan scenarios under {}", root.display());

    let show = std::env::var("SHOW_PLANS").is_ok();
    let mut failures = Vec::new();
    for file in files {
        let raw = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let scenario: PlanScenario = serde_saphyr::from_str(&raw)
            .unwrap_or_else(|error| panic!("invalid {}: {error}", file.display()));
        scenario.validate();
        let compiled = match compiler::compile(
            &scenario.query(),
            Frontend::JsonDsl,
            &ontology,
            &security,
        ) {
            Ok(compiled) => compiled,
            Err(error) => {
                failures.push(format!("{}: compile failed: {error}", scenario.name));
                continue;
            }
        };
        let tree = pattern::parse(&compiled.plan);
        let plan = tree.render(0);
        if show {
            eprintln!("=== {} ===\n{plan}\n", scenario.name);
        }
        for expected in &scenario.expect {
            if !pattern::matches_anywhere(&pattern::parse(expected), &tree) {
                failures.push(format!(
                    "{}: expected pattern not found:\n{expected}\n\nplan:\n{plan}\n",
                    scenario.name
                ));
            }
        }
        for rejected in &scenario.reject {
            if pattern::matches_anywhere(&pattern::parse(rejected), &tree) {
                failures.push(format!(
                    "{}: rejected pattern found:\n{rejected}\n\nplan:\n{plan}\n",
                    scenario.name
                ));
            }
        }
        if let Some(expected) = &scenario.plan
            && expected.trim() != plan.trim()
        {
            failures.push(format!(
                "{}: plan snapshot differs; expected:\n{}\n\nactual:\n{plan}\n",
                scenario.name,
                expected.trim()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "\n{} scenario(s) failed:\n\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn discover(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("plan scenario directory") {
        let path = entry.expect("plan scenario entry").path();
        if path.is_dir() {
            discover(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "yaml") {
            files.push(path);
        }
    }
}
